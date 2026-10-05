#include "../support.hpp"
#include <array>
#include <atomic>
#include <barrier>
#include <boost/beast.hpp>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <laso/api/api.hpp>
#include <laso/artifacts/artifacts.hpp>
#include <laso/artifacts/server.hpp>
#include <laso/runtime/workspace.hpp>
#include <laso/storage/coordination.hpp>
#include <laso/storage/factory.hpp>
#include <laso/workers/worker.hpp>
#include <mutex>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>
#include <thread>

using namespace laso;
using namespace laso::test;

namespace {
struct ContinuationObservation {
  std::string input_tag;
  std::optional<std::string> received_state;
  std::optional<std::string> context_generation_id;
  Json context_payload = nullptr;
  Json context_turns = Json::array();
};

struct ContinuationFixtureState {
  std::mutex mutex;
  std::condition_variable condition;
  std::vector<ContinuationObservation> observed;
  std::string secret_prefix;
  bool reject_state = false;
  bool timeout = false;
  bool block_until_released = false;
  bool provider_entered = false;
  bool allow_provider_return = false;
  bool provider_returned = false;
};

class SessionContinuationFixture final : public ModelProvider {
public:
  SessionContinuationFixture(std::shared_ptr<ContinuationFixtureState> state,
                             ContinuationMode mode = ContinuationMode::Opaque)
      : state_(std::move(state)), mode_(mode) {}

  Task<ModelResponse> generate(const ModelRequest &request, ExecutionContext &context) override {
    context.check();
    const auto tag = request.input.value("tag", std::string{"untagged"});
    const auto previous = request.continuation
                              ? std::optional<std::string>{request.continuation->state}
                              : std::nullopt;
    bool reject = false;
    bool timeout = false;
    bool block = false;
    {
      std::lock_guard lock(state_->mutex);
      state_->observed.push_back(
          {tag, previous,
           request.session_context
               ? std::optional<std::string>{request.session_context->generation_id}
               : std::nullopt,
           request.session_context ? request.session_context->payload : Json(nullptr)});
      state_->observed.back().context_turns =
          request.session_context ? request.session_context->recent_turns : Json::array();
      reject = state_->reject_state;
      timeout = state_->timeout;
      block = state_->block_until_released;
    }
    if (block) {
      std::unique_lock lock(state_->mutex);
      state_->provider_entered = true;
      state_->condition.notify_all();
      state_->condition.wait(lock, [&] { return state_->allow_provider_return; });
      state_->provider_returned = true;
    }
    if (reject && previous)
      throw Error(ErrorCode::Provider, "Fixture rejected stored continuation");
    if (timeout)
      throw Error(ErrorCode::Timeout, "Fixture provider timed out");
    ModelResponse response{Json{{"tag", tag}, {"text", "fixture-result"}}, request.model,
                           "continuation-fixture"};
    if (mode_ == ContinuationMode::Opaque) {
      const auto next = previous ? *previous + ":" + tag : state_->secret_prefix + ":" + tag;
      response.continuation = OpaqueProviderContinuation{"continuation-fixture", "1", next};
    }
    co_return response;
  }

  ProviderHealth health() const override {
    return {true, "deterministic fixture"};
  }

  ProviderMetadata metadata() const override {
    ProviderMetadata result;
    result.name = "continuation-fixture";
    result.version = "1";
    result.capabilities.push_back("session-context");
    result.continuation_mode = mode_;
    return result;
  }

private:
  std::shared_ptr<ContinuationFixtureState> state_;
  ContinuationMode mode_;
};

class MarkerContextReducer final : public ContextReducer {
public:
  explicit MarkerContextReducer(bool fail = false, bool malformed = false,
                                Milliseconds delay = Milliseconds{0})
      : fail_(fail), malformed_(malformed), delay_(delay) {}
  ContextReductionResult reduce(const ContextReductionRequest &request,
                                std::stop_token cancellation) override {
    if (delay_.count() > 0)
      std::this_thread::sleep_for(delay_);
    if (cancellation.stop_requested() || std::chrono::steady_clock::now() >= request.deadline)
      throw Error(ErrorCode::Timeout, "test reducer deadline");
    if (fail_)
      throw Error(ErrorCode::Execution, "private reducer failure");
    return {"test.marker", "1", request.through_turn_sequence + (malformed_ ? 1 : 0),
            Json{{"reduced_through", request.through_turn_sequence}}};
  }

private:
  bool fail_;
  bool malformed_;
  Milliseconds delay_;
};

Config continuation_config(const std::filesystem::path &path) {
  auto options = config(path);
  options.models["session-model"] =
      ModelBinding{"continuation-fixture", "deterministic", Json::object()};
  return options;
}

std::string continuation_pipeline() {
  return single("type: agent\n    model: session-model\n    prompt: Continue the test session.");
}

void register_continuation_fixture(Service &service,
                                   const std::shared_ptr<ContinuationFixtureState> &state,
                                   ContinuationMode mode = ContinuationMode::Opaque) {
  service.provider_registry().add("continuation-fixture",
                                  std::make_shared<SessionContinuationFixture>(state, mode));
}

class DefaultLogCapture {
public:
  DefaultLogCapture()
      : previous_(spdlog::default_logger()), output_(std::make_shared<std::ostringstream>()) {
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(*output_);
    auto logger = std::make_shared<spdlog::logger>("session-test-capture", sink);
    logger->set_level(spdlog::level::trace);
    spdlog::set_default_logger(std::move(logger));
  }
  ~DefaultLogCapture() {
    spdlog::set_default_logger(std::move(previous_));
  }
  std::string str() const {
    return output_->str();
  }

private:
  std::shared_ptr<spdlog::logger> previous_;
  std::shared_ptr<std::ostringstream> output_;
};

class SessionSseTestClient {
public:
  SessionSseTestClient(std::uint16_t port, const std::string &session_id, std::uint64_t cursor)
      : stream_(io_) {
    stream_.expires_after(std::chrono::seconds(5));
    stream_.connect({asio::ip::make_address("127.0.0.1"), port});
    const auto request = "GET /api/v1/sessions/" + session_id +
                         "/events/stream HTTP/1.1\r\nHost: localhost\r\n"
                         "Accept: text/event-stream\r\nLast-Event-ID: " +
                         std::to_string(cursor) + "\r\n\r\n";
    asio::write(stream_, asio::buffer(request));
    asio::read_until(stream_, buffer_, "\r\n\r\n");
    std::istream headers(&buffer_);
    std::getline(headers, status_line_);
    for (std::string line; std::getline(headers, line) && line != "\r";) {
    }
  }

  std::pair<std::uint64_t, Json> next() {
    stream_.expires_after(std::chrono::seconds(5));
    asio::read_until(stream_, buffer_, "\n\n");
    std::istream frame(&buffer_);
    std::uint64_t sequence = 0;
    std::string data;
    for (std::string line; std::getline(frame, line);) {
      if (line.empty() || line == "\r")
        break;
      if (!line.empty() && line.back() == '\r')
        line.pop_back();
      if (line.rfind("id: ", 0) == 0)
        sequence = std::stoull(line.substr(4));
      else if (line.rfind("data: ", 0) == 0)
        data = line.substr(6);
    }
    if (sequence == 0 || data.empty())
      throw std::runtime_error("SSE stream returned a frame without an event");
    auto event = Json::parse(data);
    event["sequence"] = sequence;
    return {sequence, std::move(event)};
  }

  void close() {
    boost::system::error_code error;
    stream_.socket().shutdown(asio::ip::tcp::socket::shutdown_both, error);
    stream_.socket().close(error);
  }

  const std::string &status_line() const {
    return status_line_;
  }

private:
  asio::io_context io_;
  boost::beast::tcp_stream stream_;
  asio::streambuf buffer_;
  std::string status_line_;
};
} // namespace

TEST(Storage, PersistsAcrossConnections) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    {
      auto s = fixture.open(dir.path / "state.db");
      s->commit({{RecordKind::Pipeline, "example", "", {{"value", 42}}}});
    }
    {
      auto s = fixture.open(dir.path / "state.db");
      EXPECT_EQ(s->get(RecordKind::Pipeline, "example").at("value"), 42);
    }
  });
}
TEST(Storage, TransactionRollsBackWholeCheckpoint) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    std::vector<Record> batch{
        {RecordKind::Run, "first", "first", {{"valid", true}}},
        {RecordKind::Message, "large", "first", std::string(4 * 1024 * 1024 + 1, 'a')}};
    EXPECT_THROW(s->commit(batch), Error);
    EXPECT_THROW(s->get(RecordKind::Run, "first"), Error);
  });
}
TEST(Storage, ConformanceStoresAllRecordKinds) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    constexpr std::array kinds = {RecordKind::Pipeline,
                                  RecordKind::Run,
                                  RecordKind::Attempt,
                                  RecordKind::Message,
                                  RecordKind::Approval,
                                  RecordKind::Artifact,
                                  RecordKind::Event,
                                  RecordKind::Schedule,
                                  RecordKind::Trigger,
                                  RecordKind::ScheduleOccurrence,
                                  RecordKind::TriggerDelivery,
                                  RecordKind::EventSource,
                                  RecordKind::ExternalEventClaim,
                                  RecordKind::WorkerJob,
                                  RecordKind::WorkerInteraction,
                                  RecordKind::NodeWork,
                                  RecordKind::AgentSession,
                                  RecordKind::SessionTurn,
                                  RecordKind::SessionEvent,
                                  RecordKind::SessionContinuation};
    std::vector<Record> records;
    for (std::size_t i = 0; i < kinds.size(); ++i)
      records.push_back({kinds[i], "record-" + std::to_string(i), "run-1", {{"index", i}}});
    s->commit(records);
    for (std::size_t i = 0; i < kinds.size(); ++i) {
      EXPECT_EQ(s->get(kinds[i], "record-" + std::to_string(i)).at("index"), i);
      EXPECT_EQ(s->list(kinds[i], "run-1").size(), 1U);
    }
  });
}
TEST(Storage, ConformancePreservesOrderAcrossUpdates) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    s->commit({{RecordKind::Message, "first", "run-1", {{"value", 1}}},
               {RecordKind::Message, "second", "run-1", {{"value", 2}}}});
    s->commit({{RecordKind::Message, "first", "run-1", {{"value", 3}}}});
    const auto messages = s->list(RecordKind::Message, "run-1");
    ASSERT_EQ(messages.size(), 2U);
    EXPECT_EQ(messages[0].at("value"), 3);
    EXPECT_EQ(messages[1].at("value"), 2);
  });
}
TEST(Storage, ConformanceRejectsConflictingPipelineRevision) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    const Record original{RecordKind::Pipeline, "hello@1", "", {{"yaml", "one"}}};
    s->commit({original});
    EXPECT_NO_THROW(s->commit({original}));
    try {
      s->commit({{RecordKind::Pipeline, "hello@1", "", {{"yaml", "two"}}}});
      FAIL() << "conflicting pipeline revision should be rejected";
    } catch (const Error &error) {
      EXPECT_EQ(error.code, ErrorCode::Conflict);
    }
  });
}
TEST(Storage, ConformanceSupportsPagination) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    for (int i = 0; i < 3; ++i)
      s->commit({{RecordKind::Event, "event-" + std::to_string(i), "run-1", {{"index", i}}}});
    const auto page = s->list(RecordKind::Event, "run-1", 2, 1);
    ASSERT_EQ(page.size(), 2U);
    EXPECT_EQ(page[0].at("index"), 1);
    EXPECT_EQ(page[1].at("index"), 2);
  });
}
TEST(Storage, ConformancePaginatesBeyondOnePage) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    std::vector<Record> records;
    records.reserve(10001);
    for (unsigned i = 0; i < 10001; ++i)
      records.push_back({RecordKind::Event, "event-" + std::to_string(i), "run-1", {{"index", i}}});
    s->commit(records);
    const auto page = s->list(RecordKind::Event, "run-1", 1000, 10000);
    ASSERT_EQ(page.size(), 1U);
    EXPECT_EQ(page.front().at("index"), 10000U);
  });
}
TEST(Storage, ConformanceRejectsInvalidRecordInputs) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    try {
      s->commit({{static_cast<RecordKind>(99), "record", "run-1", Json::object()}});
      FAIL() << "invalid record kind should be rejected";
    } catch (const Error &error) {
      EXPECT_EQ(error.code, ErrorCode::Validation);
    }
    try {
      s->get(static_cast<RecordKind>(99), "record");
      FAIL() << "invalid get kind should be rejected";
    } catch (const Error &error) {
      EXPECT_EQ(error.code, ErrorCode::Validation);
    }
    try {
      s->list(static_cast<RecordKind>(99));
      FAIL() << "invalid list kind should be rejected";
    } catch (const Error &error) {
      EXPECT_EQ(error.code, ErrorCode::Validation);
    }
    EXPECT_THROW(s->commit({{RecordKind::Event, "", "run-1", Json::object()}}), Error);
    EXPECT_THROW(
        s->commit({{RecordKind::Event, "discarded", "run-1", Json(Json::value_t::discarded)}}),
        Error);
  });
}
TEST(Storage, ConformancePersistsStructuredOperationalRecords) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    const Json run = {{"id", "parent"},
                      {"state", "WaitingApproval"},
                      {"pipeline_id", "parent"},
                      {"pipeline_version", 2},
                      {"child_runs", Json::array({"child"})}};
    const Json child = {{"id", "child"},
                        {"parent_id", "parent"},
                        {"parent_node_id", "invoke"},
                        {"pipeline_id", "child"},
                        {"pipeline_version", 3}};
    const Json approval = {{"id", "approval"}, {"run_id", "child"}, {"decision", "pending"}};
    const Json artifact = {{"id", "artifact"}, {"run_id", "child"}, {"location", "local"}};
    const Json event = {{"id", "event"}, {"run_id", "child"}, {"type", "child.started"}};
    s->commit({{RecordKind::Run, "parent", "parent", run},
               {RecordKind::Run, "child", "parent", child},
               {RecordKind::Approval, "approval", "child", approval},
               {RecordKind::Artifact, "artifact", "child", artifact},
               {RecordKind::Event, "event", "child", event}});
    EXPECT_EQ(s->get(RecordKind::Run, "child"), child);
    EXPECT_EQ(s->list(RecordKind::Run, "parent").size(), 2U);
    EXPECT_EQ(s->get(RecordKind::Approval, "approval"), approval);
    EXPECT_EQ(s->get(RecordKind::Artifact, "artifact"), artifact);
    EXPECT_EQ(s->get(RecordKind::Event, "event"), event);
    auto decided = approval;
    decided["decision"] = "approved";
    s->commit({{RecordKind::Approval, "approval", "child", decided}});
    EXPECT_EQ(s->get(RecordKind::Approval, "approval").at("decision"), "approved");
    auto cancelled = run;
    cancelled["state"] = "Cancelled";
    s->commit({{RecordKind::Run, "parent", "parent", cancelled}});
    EXPECT_EQ(s->get(RecordKind::Run, "parent").at("state"), "Cancelled");
  });
}
TEST(Storage, ConformanceSerializesConcurrentCommits) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    std::atomic<bool> failed = false;
    std::vector<std::jthread> writers;
    for (unsigned writer = 0; writer < 4; ++writer) {
      writers.emplace_back([&, writer] {
        try {
          for (unsigned i = 0; i < 32; ++i) {
            const auto id = "concurrent-" + std::to_string(writer) + "-" + std::to_string(i);
            s->commit({{RecordKind::Event, id, "run-1", {{"writer", writer}, {"index", i}}}});
          }
        } catch (...) {
          failed = true;
        }
      });
    }
    writers.clear();
    EXPECT_FALSE(failed);
    EXPECT_EQ(s->list(RecordKind::Event, "run-1").size(), 128U);
  });
}
TEST(Storage, SessionTurnsAreOrderedIdempotentReplayableAndDurable) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    const auto path = dir.path / "sessions.db";
    const auto session_id = "session-" + uuid();
    {
      auto storage = fixture.open(path);
      AgentSession session;
      session.id = session_id;
      session.pipeline_id = "example@1";
      storage->commit({{RecordKind::AgentSession, session_id, session_id, Json(session)}});
      std::atomic<bool> failed = false;
      std::vector<std::jthread> writers;
      for (unsigned i = 0; i < 12; ++i)
        writers.emplace_back([&, i] {
          try {
            const auto key = "request-" + std::to_string(i);
            Json turn{{"idempotency_key", key}, {"input", {{"n", i}}}, {"state", "queued"}};
            Event event;
            event.type = "ignored-by-store";
            ASSERT_TRUE(storage->submit_session_turn(session_id, "turn-" + std::to_string(i), turn,
                                                     Json(event)));
          } catch (...) {
            failed = true;
          }
        });
      writers.clear();
      EXPECT_FALSE(failed);
      EXPECT_FALSE(storage->submit_session_turn(
          session_id, "turn-0",
          Json{{"idempotency_key", "request-0"}, {"input", {{"n", 0}}}, {"state", "queued"}},
          Json::object()));
      EXPECT_THROW(storage->submit_session_turn(
                       session_id, "turn-0",
                       Json{{"idempotency_key", "request-0"}, {"input", {{"n", 99}}}},
                       Json::object()),
                   Error);
      const auto all = storage->session_events(session_id, 0, 100);
      ASSERT_EQ(all.size(), 12U);
      for (std::size_t i = 0; i < all.size(); ++i)
        EXPECT_EQ(all[i].at("sequence").template get<std::uint64_t>(), i + 1);
      EXPECT_EQ(storage->session_events(session_id, 7, 100).size(), 5U);
      EXPECT_TRUE(storage->session_events(session_id, 12, 100).empty());
      EXPECT_TRUE(storage->close_agent_session(session_id, Json{{"type", "ignored"}}));
      EXPECT_FALSE(storage->close_agent_session(session_id, Json::object()));
      const auto closed_events = storage->session_events(session_id, 12, 100);
      ASSERT_EQ(closed_events.size(), 14U);
      EXPECT_EQ(closed_events.front().at("sequence"), 13U);
      EXPECT_EQ(closed_events.front().at("type"), "session.closing");
      for (std::size_t i = 1; i < 13; ++i)
        EXPECT_EQ(closed_events[i].at("type"), "turn.execution.cancelled");
      EXPECT_EQ(closed_events.back().at("sequence"), 26U);
      EXPECT_EQ(closed_events.back().at("type"), "session.closed");
    }
    auto reopened = fixture.open(path);
    EXPECT_EQ(reopened->session_events(session_id, 0, 100).size(), 26U);
  });
}
TEST(Storage, SessionDispatchClaimsAndRunBindingAreAtomic) {
  for_each_storage_fixture([](const auto &backend) {
    SCOPED_TRACE(backend.name);
    TemporaryDirectory dir;
    auto storage = backend.open(dir.path / "session-dispatch.db");
    AgentSession session;
    session.pipeline_id = "example@1";
    storage->commit({{RecordKind::AgentSession, session.id, session.id, Json(session)}});

    const auto first_id = "turn-first";
    const auto second_id = "turn-second";
    for (const auto &[id, key] :
         {std::pair{first_id, "key-first"}, std::pair{second_id, "key-second"}}) {
      Event accepted;
      accepted.type = "input.accepted";
      ASSERT_TRUE(storage->submit_session_turn(session.id, id,
                                               Json{{"idempotency_key", key},
                                                    {"input", {{"text", id}}},
                                                    {"pipeline_id", "example@1"},
                                                    {"state", "queued"}},
                                               Json(accepted)));
    }

    Event claimed_event;
    const auto claimed = storage->claim_next_session_turn(session.id, "instance-a", 0, timestamp(),
                                                          Json(claimed_event));
    ASSERT_TRUE(claimed);
    EXPECT_EQ(claimed->at("id"), first_id);
    EXPECT_EQ(claimed->at("state"), "claimed");
    const auto fence = claimed->at("dispatch_fencing_token").template get<std::uint64_t>();

    EXPECT_EQ(storage->list(RecordKind::SessionTurn, session.id).size(), 2U);
    EXPECT_EQ(storage->get(RecordKind::SessionTurn, first_id).at("id"), first_id);
    std::optional<Json> repeated_claim;
    EXPECT_NO_THROW(repeated_claim = storage->claim_next_session_turn(session.id, "instance-a", 0,
                                                                      timestamp(), Json::object()));
    ASSERT_TRUE(repeated_claim);
    EXPECT_EQ(repeated_claim->at("id"), first_id);
    EXPECT_EQ(storage->session_events(session.id, 0, 20).size(), 3U);

    laso::Run run;
    run.id = "run-for-first-turn";
    run.pipeline_id = "example";
    run.session_id = session.id;
    run.session_turn_id = first_id;
    Event run_event;
    run_event.run_id = run.id;
    run_event.type = "run.created";
    Event session_event;

    EXPECT_THROW(storage->bind_session_turn_run(session.id, first_id, Json(run), Json(run_event),
                                                "instance-b", 0, Json(session_event)),
                 Error);
    EXPECT_THROW(storage->get(RecordKind::Run, run.id), Error);

    const auto recovered =
        storage->claim_next_session_turn(session.id, "instance-b", 0, timestamp(), Json::object());
    ASSERT_TRUE(recovered);
    EXPECT_EQ(recovered->at("id"), first_id);
    const auto recovered_fence =
        recovered->at("dispatch_fencing_token").template get<std::uint64_t>();
    EXPECT_GT(recovered_fence, fence);
    const auto binding_fence = backend.name == "postgres" ? 0 : recovered_fence;
    EXPECT_THROW(storage->bind_session_turn_run(session.id, first_id, Json(run), Json(run_event),
                                                "instance-a", 0, Json::object()),
                 Error);
    EXPECT_TRUE(storage->bind_session_turn_run(session.id, first_id, Json(run), Json(run_event),
                                               "instance-b", binding_fence, Json(session_event)));
    EXPECT_FALSE(storage->bind_session_turn_run(session.id, first_id, Json(run), Json(run_event),
                                                "instance-a", 0, Json::object()));

    const auto stored_turn = storage->get(RecordKind::SessionTurn, first_id);
    EXPECT_EQ(stored_turn.at("state"), "running");
    EXPECT_EQ(stored_turn.at("run_id"), run.id);
    EXPECT_FALSE(stored_turn.contains("dispatch_owner"));
    EXPECT_FALSE(stored_turn.contains("dispatch_fencing_token"));
    EXPECT_EQ(storage->get(RecordKind::Run, run.id).at("session_turn_id"), first_id);
    const auto snapshot =
        storage->get(RecordKind::RunContextSnapshot, run.id).template get<RunContextSnapshot>();
    EXPECT_EQ(snapshot.session_id, session.id);
    EXPECT_EQ(snapshot.session_turn_id, first_id);
    EXPECT_EQ(snapshot.turn_sequence, 1U);
    EXPECT_EQ(snapshot.history_through_turn_sequence, 1U);

    const auto stored_session =
        storage->get(RecordKind::AgentSession, session.id).template get<AgentSession>();
    EXPECT_EQ(stored_session.active_turn_id, first_id);
    EXPECT_EQ(stored_session.active_run_id, run.id);

    std::optional<Json> next_claim;
    EXPECT_NO_THROW(next_claim = storage->claim_next_session_turn(session.id, "instance-a", 0,
                                                                  timestamp(), Json::object()));
    EXPECT_FALSE(next_claim);
    const auto events = storage->session_events(session.id, 0, 20);
    ASSERT_EQ(events.size(), 5U);
    EXPECT_EQ(events[0].at("type"), "input.accepted");
    EXPECT_EQ(events[1].at("type"), "input.accepted");
    EXPECT_EQ(events[2].at("type"), "turn.execution.claimed");
    EXPECT_EQ(events[3].at("type"), "turn.execution.claimed");
    EXPECT_EQ(events[4].at("type"), "turn.execution.started");
    for (std::size_t i = 0; i < events.size(); ++i)
      EXPECT_EQ(events[i].at("sequence").template get<std::uint64_t>(), i + 1);
  });
}
TEST(Storage, ContextGenerationsAndRunSnapshotsAreImmutableAndDurable) {
  for_each_storage_fixture([](const auto &backend) {
    SCOPED_TRACE(backend.name);
    TemporaryDirectory dir;
    const auto database = dir.path / "context-provenance.db";
    auto storage = backend.open(database);
    AgentSession session;
    session.pipeline_id = "example@1";
    storage->commit({{RecordKind::AgentSession, session.id, session.id, Json(session)}});

    const auto initial = storage->create_session_context_generation(
        session.id, 0, 0, "generation-key-0", "structured-context", "1",
        Json{{"state", "initial"}});
    EXPECT_EQ(initial.at("generation"), 1U);
    EXPECT_TRUE(initial.at("predecessor_id").template get<std::string>().empty());
    const auto retried = storage->create_session_context_generation(
        session.id, 0, 0, "generation-key-0", "structured-context", "1",
        Json{{"state", "initial"}});
    EXPECT_EQ(retried.at("id"), initial.at("id"));
    EXPECT_EQ(storage
                  ->get(RecordKind::SessionContextGeneration,
                        initial.at("id").template get<std::string>())
                  .at("generation"),
              1U);
    EXPECT_EQ(storage->list(RecordKind::SessionContextGeneration, session.id).size(), 1U);
    EXPECT_THROW(storage->create_session_context_generation(
                     session.id, 0, 0, "generation-key-other", "structured-context", "1",
                     Json{{"state", "stale"}}),
                 Error);
    EXPECT_THROW(storage->create_session_context_generation(session.id, 0, 1, "future-boundary",
                                                            "structured-context", "1",
                                                            Json{{"state", "invalid"}}),
                 Error);
    auto mutated = initial;
    mutated["payload"] = {{"state", "rewritten"}};
    EXPECT_THROW(
        storage->commit({{RecordKind::SessionContextGeneration,
                          initial.at("id").template get<std::string>(), session.id, mutated}}),
        Error);

    const std::string completed_turn_id = "completed-context-turn";
    storage->commit({{RecordKind::SessionTurn, completed_turn_id, session.id,
                      Json{{"id", completed_turn_id},
                           {"session_id", session.id},
                           {"sequence", 1},
                           {"state", "succeeded"},
                           {"input", {{"text", "prior"}}}}}});
    const auto next = storage->create_session_context_generation(
        session.id, 1, 1, "generation-key-1", "structured-context", "1",
        Json{{"state", "derived-through-one"}});
    EXPECT_EQ(next.at("generation"), 2U);
    EXPECT_EQ(next.at("predecessor_id"), initial.at("id"));
    EXPECT_THROW(storage->create_session_context_generation(session.id, 2, 0, "backward-boundary",
                                                            "structured-context", "1",
                                                            Json{{"state", "stale-history"}}),
                 Error);

    Event accepted;
    ASSERT_TRUE(storage->submit_session_turn(session.id, "context-turn-2",
                                             Json{{"idempotency_key", "turn-2"},
                                                  {"input", {{"text", "next"}}},
                                                  {"pipeline_id", "example@1"},
                                                  {"state", "queued"}},
                                             Json(accepted)));
    auto claimed =
        storage->claim_next_session_turn(session.id, "instance-a", 0, timestamp(), Json::object());
    ASSERT_TRUE(claimed);
    laso::Run run;
    run.id = "run-context-turn-2";
    run.pipeline_id = "example";
    run.session_id = session.id;
    run.session_turn_id = "context-turn-2";
    Event run_event;
    Event session_event;
    ASSERT_TRUE(storage->bind_session_turn_run(session.id, run.session_turn_id, Json(run),
                                               Json(run_event), "instance-a", 0,
                                               Json(session_event)));
    const auto snapshot =
        storage->get(RecordKind::RunContextSnapshot, run.id).template get<RunContextSnapshot>();
    EXPECT_EQ(snapshot.context_generation_id, next.at("id").template get<std::string>());
    EXPECT_EQ(snapshot.context_generation, 2U);
    EXPECT_EQ(snapshot.context_through_turn_sequence, 1U);
    EXPECT_EQ(snapshot.history_through_turn_sequence, 2U);
    EXPECT_EQ(snapshot.run_id, run.id);
    EXPECT_THROW(storage->commit(
                     {{RecordKind::RunContextSnapshot, run.id, run.id, Json{{"run_id", run.id}}}}),
                 Error);

    storage.reset();
    auto reopened = backend.open(database);
    const auto persisted_generation =
        reopened->get(RecordKind::SessionContextGeneration, next.at("id"));
    EXPECT_EQ(persisted_generation.at("payload").at("state"), "derived-through-one");
    const auto persisted_snapshot =
        reopened->get(RecordKind::RunContextSnapshot, run.id).template get<RunContextSnapshot>();
    EXPECT_EQ(persisted_snapshot.context_generation_id, next.at("id").template get<std::string>());
    EXPECT_EQ(persisted_snapshot.session_turn_id, run.session_turn_id);

    AgentSession concurrent_session;
    concurrent_session.pipeline_id = "example@1";
    reopened->commit({{RecordKind::AgentSession, concurrent_session.id, concurrent_session.id,
                       Json(concurrent_session)}});
    std::barrier start(3);
    std::atomic<unsigned> created = 0;
    std::atomic<unsigned> stale = 0;
    std::atomic<bool> unexpected = false;
    std::vector<std::jthread> writers;
    for (unsigned i = 0; i < 2; ++i)
      writers.emplace_back([&, i] {
        start.arrive_and_wait();
        try {
          (void)reopened->create_session_context_generation(
              concurrent_session.id, 0, 0, "concurrent-key-" + std::to_string(i),
              "structured-context", "1", Json{{"writer", i}});
          ++created;
        } catch (const Error &error) {
          if (error.code == ErrorCode::Conflict)
            ++stale;
          else
            unexpected = true;
        }
      });
    start.arrive_and_wait();
    writers.clear();
    EXPECT_EQ(created, 1U);
    EXPECT_EQ(stale, 1U);
    EXPECT_FALSE(unexpected);
    EXPECT_EQ(reopened->list(RecordKind::SessionContextGeneration, concurrent_session.id).size(),
              1U);
  });
}
TEST(Sessions, AcceptedTurnsExecuteDurablyInAcceptanceOrder) {
  TemporaryDirectory dir;
  std::string session_id;
  std::vector<std::string> accepted_ids;
  {
    asio::io_context io;
    Service service(io, config(dir.path));
    service.functions().add(
        "session_order",
        std::make_shared<Function>([](ExecutionContext &context, const Json &input) -> Task<Json> {
          if (input.value("value", std::string{}) == "A")
            co_await context.delay(Milliseconds{100});
          co_return input;
        }));
    const auto pipeline =
        service.register_pipeline(single("type: function\n    function: session_order"))
            .at("id")
            .get<std::string>();
    const auto session = service.create_session(pipeline);
    session_id = session.id;

    const auto first =
        service.submit_session_turn(session.id, "session-turn-a", Json{{"value", "A"}});
    const auto duplicate_running =
        service.submit_session_turn(session.id, "session-turn-a", Json{{"value", "A"}});
    const auto second =
        service.submit_session_turn(session.id, "session-turn-b", Json{{"value", "B"}});
    const auto third =
        service.submit_session_turn(session.id, "session-turn-c", Json{{"value", "C"}});
    EXPECT_EQ(first.at("state"), "queued");
    EXPECT_EQ(second.at("state"), "queued");
    EXPECT_EQ(third.at("state"), "queued");
    EXPECT_EQ(duplicate_running.at("id"), first.at("id"));
    accepted_ids = {first.at("id").get<std::string>(), second.at("id").get<std::string>(),
                    third.at("id").get<std::string>()};

    io.run();

    const auto turns = service.list(RecordKind::SessionTurn, session.id);
    ASSERT_EQ(turns.size(), 3U);
    for (std::size_t i = 0; i < turns.size(); ++i) {
      EXPECT_EQ(turns[i].at("id"), accepted_ids[i]);
      EXPECT_EQ(turns[i].at("sequence"), i + 1);
      EXPECT_EQ(turns[i].at("state"), "succeeded");
      EXPECT_EQ(turns[i].at("result").at("value"), std::string(1, static_cast<char>('A' + i)));
      const auto run_id = turns[i].at("run_id").get<std::string>();
      const auto run = service.get(RecordKind::Run, run_id).get<laso::Run>();
      EXPECT_EQ(run.state, RunState::Completed);
      EXPECT_EQ(run.session_id, session.id);
      EXPECT_EQ(run.session_turn_id, turns[i].at("id").get<std::string>());
    }
    const auto duplicate_completed =
        service.submit_session_turn(session.id, "session-turn-a", Json{{"value", "A"}});
    EXPECT_EQ(duplicate_completed.at("id"), accepted_ids.front());

    const auto events = service.session_events(session.id, 0, 100);
    std::vector<std::string> started;
    std::vector<std::string> completed_turns;
    std::size_t accepted = 0;
    for (const auto &event : events) {
      if (event.at("type") == "input.accepted")
        ++accepted;
      if (event.at("type") == "turn.execution.started")
        started.push_back(event.at("turn_id").get<std::string>());
      if (event.at("type") == "turn.execution.completed")
        completed_turns.push_back(event.at("turn_id").get<std::string>());
    }
    EXPECT_EQ(accepted, 3U);
    EXPECT_EQ(started, accepted_ids);
    EXPECT_EQ(completed_turns, accepted_ids);
  }

  asio::io_context restarted_io;
  Service restarted(restarted_io, config(dir.path));
  const auto turns = restarted.list(RecordKind::SessionTurn, session_id);
  ASSERT_EQ(turns.size(), accepted_ids.size());
  for (std::size_t i = 0; i < turns.size(); ++i) {
    EXPECT_EQ(turns[i].at("id"), accepted_ids[i]);
    EXPECT_EQ(turns[i].at("sequence"), i + 1);
    EXPECT_EQ(turns[i].at("state"), "succeeded");
    EXPECT_EQ(turns[i].at("result").at("value"), std::string(1, static_cast<char>('A' + i)));
  }
  const auto replay = restarted.session_events(session_id, 0, 100);
  std::vector<std::string> replayed_completions;
  for (const auto &event : replay)
    if (event.at("type") == "turn.execution.completed")
      replayed_completions.push_back(event.at("turn_id").get<std::string>());
  EXPECT_EQ(replayed_completions, accepted_ids);
}

TEST(Sessions, QueuedRunRecoversAfterServiceRestartWithoutDuplicateRun) {
  TemporaryDirectory dir;
  std::atomic<unsigned> executions{0};
  std::string session_id;
  std::string turn_id;
  std::string run_id;
  {
    asio::io_context io;
    Service service(io, config(dir.path));
    service.functions().add(
        "session_restart",
        std::make_shared<Function>([&](ExecutionContext &, const Json &input) -> Task<Json> {
          executions.fetch_add(1);
          co_return input;
        }));
    const auto pipeline =
        service.register_pipeline(single("type: function\n    function: session_restart"))
            .at("id")
            .get<std::string>();
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    bool interrupted_after_binding = false;
    service.runtime().set_session_test_hook([&](SessionTestPoint point) {
      if (!interrupted_after_binding && point == SessionTestPoint::AfterRunBinding) {
        interrupted_after_binding = true;
        throw std::runtime_error("injected session run binding interruption");
      }
    });
#endif
    const auto session = service.create_session(pipeline);
    session_id = session.id;
    const auto accepted =
        service.submit_session_turn(session.id, "restart-recovery", Json{{"value", "durable"}});
    turn_id = accepted.at("id").get<std::string>();
    const auto stored = service.get(RecordKind::SessionTurn, turn_id);
    run_id = stored.at("run_id").get<std::string>();
    EXPECT_EQ(stored.at("state"), "running");
    EXPECT_EQ(service.get(RecordKind::Run, run_id).at("state"), "Queued");
    EXPECT_EQ(executions.load(), 0U);
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    EXPECT_TRUE(interrupted_after_binding);
#endif
  }

  asio::io_context restarted_io;
  Service restarted(restarted_io, config(dir.path));
  restarted.functions().add(
      "session_restart",
      std::make_shared<Function>([&](ExecutionContext &, const Json &input) -> Task<Json> {
        executions.fetch_add(1);
        co_return input;
      }));
  restarted_io.run();

  const auto recovered = restarted.get(RecordKind::SessionTurn, turn_id);
  EXPECT_EQ(recovered.at("state"), "succeeded");
  EXPECT_EQ(recovered.at("run_id"), run_id);
  EXPECT_EQ(recovered.at("result").at("value"), "durable");
  EXPECT_EQ(executions.load(), 1U);
  const auto recovered_run = restarted.get(RecordKind::Run, run_id).get<laso::Run>();
  EXPECT_EQ(recovered_run.state, RunState::Completed);
  EXPECT_EQ(recovered_run.session_id, session_id);
  EXPECT_EQ(recovered_run.session_turn_id, turn_id);
  const auto events = restarted.session_events(session_id, 0, 100);
  std::size_t started = 0;
  std::size_t completed = 0;
  for (const auto &event : events) {
    started += event.at("type") == "turn.execution.started";
    completed += event.at("type") == "turn.execution.completed";
  }
  EXPECT_EQ(started, 1U);
  EXPECT_EQ(completed, 1U);
}

#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
TEST(Sessions, ClaimedTurnRecoversAfterInterruptionBeforeRunBinding) {
  TemporaryDirectory dir;
  std::atomic<unsigned> executions{0};
  std::string session_id;
  std::string turn_id;
  {
    asio::io_context io;
    Service service(io, config(dir.path));
    service.functions().add(
        "session_claim_restart",
        std::make_shared<Function>([&](ExecutionContext &, const Json &input) -> Task<Json> {
          executions.fetch_add(1);
          co_return input;
        }));
    const auto pipeline =
        service.register_pipeline(single("type: function\n    function: session_claim_restart"))
            .at("id")
            .get<std::string>();
    const auto session = service.create_session(pipeline);
    session_id = session.id;
    bool interrupted_after_claim = false;
    service.runtime().set_session_test_hook([&](SessionTestPoint point) {
      if (!interrupted_after_claim && point == SessionTestPoint::AfterClaim) {
        interrupted_after_claim = true;
        throw std::runtime_error("injected session claim interruption");
      }
    });
    const auto accepted =
        service.submit_session_turn(session.id, "claim-recovery", Json{{"value", "recover"}});
    turn_id = accepted.at("id").get<std::string>();
    EXPECT_TRUE(interrupted_after_claim);
    const auto claimed = service.get(RecordKind::SessionTurn, turn_id);
    EXPECT_EQ(claimed.at("state"), "claimed");
    EXPECT_TRUE(claimed.value("run_id", std::string{}).empty());
    EXPECT_EQ(executions.load(), 0U);
  }

  asio::io_context restarted_io;
  Service restarted(restarted_io, config(dir.path));
  restarted.functions().add(
      "session_claim_restart",
      std::make_shared<Function>([&](ExecutionContext &, const Json &input) -> Task<Json> {
        executions.fetch_add(1);
        co_return input;
      }));
  restarted.runtime().dispatch_session(session_id);
  restarted_io.run();

  const auto recovered = restarted.get(RecordKind::SessionTurn, turn_id);
  EXPECT_EQ(recovered.at("state"), "succeeded");
  EXPECT_FALSE(recovered.at("run_id").get<std::string>().empty());
  EXPECT_EQ(executions.load(), 1U);
  const auto events = restarted.session_events(session_id, 0, 100);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) ==
                                       "turn.execution.completed" &&
                                   event.value("turn_id", std::string{}) == turn_id;
                          }),
            1);
}
#endif

TEST(Sessions, DifferentSessionsExecuteConcurrently) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  std::atomic<unsigned> active{0};
  std::atomic<unsigned> maximum_active{0};
  service.functions().add(
      "session_overlap",
      std::make_shared<Function>([&](ExecutionContext &context, const Json &input) -> Task<Json> {
        const auto now_active = active.fetch_add(1) + 1;
        auto observed = maximum_active.load();
        while (observed < now_active &&
               !maximum_active.compare_exchange_weak(observed, now_active)) {
        }
        co_await context.delay(Milliseconds{100});
        active.fetch_sub(1);
        co_return input;
      }));
  const auto pipeline =
      service.register_pipeline(single("type: function\n    function: session_overlap"))
          .at("id")
          .get<std::string>();
  const auto session_x = service.create_session(pipeline);
  const auto session_y = service.create_session(pipeline);
  const auto turn_x = service.submit_session_turn(session_x.id, "overlap-x", Json{{"value", "X"}});
  const auto turn_y = service.submit_session_turn(session_y.id, "overlap-y", Json{{"value", "Y"}});
  io.run();

  EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_x.at("id").get<std::string>()).at("state"),
            "succeeded");
  EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_y.at("id").get<std::string>()).at("state"),
            "succeeded");
  EXPECT_GE(maximum_active.load(), 2U);
}

TEST(Sessions, OpaqueProviderContinuationSurvivesRestartAndIsSessionScoped) {
  TemporaryDirectory dir;
  auto trace = std::make_shared<ContinuationFixtureState>();
  trace->secret_prefix = "opaque-session-secret-" + uuid();
  DefaultLogCapture log_capture;
  std::string pipeline_id;
  std::string session_a;
  std::string session_b;
  std::string first_a_state;
  std::string first_b_state;
  std::string first_a_turn_id;
  std::string first_b_turn_id;
  {
    asio::io_context io;
    Service service(io, continuation_config(dir.path));
    register_continuation_fixture(service, trace);
    pipeline_id = service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
    session_a = service.create_session(pipeline_id).id;
    session_b = service.create_session(pipeline_id).id;
    const auto turn_a = service.submit_session_turn(session_a, "A-first", Json{{"tag", "A1"}});
    const auto turn_b = service.submit_session_turn(session_b, "B-first", Json{{"tag", "B1"}});
    first_a_turn_id = turn_a.at("id").get<std::string>();
    first_b_turn_id = turn_b.at("id").get<std::string>();
    io.run();
    EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_a.at("id").get<std::string>()).at("state"),
              "succeeded");
    EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_b.at("id").get<std::string>()).at("state"),
              "succeeded");
    const auto stored_a = service.list(RecordKind::SessionContinuation, session_a);
    const auto stored_b = service.list(RecordKind::SessionContinuation, session_b);
    ASSERT_EQ(stored_a.size(), 1U);
    ASSERT_EQ(stored_b.size(), 1U);
    first_a_state = stored_a.front().at("state").get<std::string>();
    first_b_state = stored_b.front().at("state").get<std::string>();
    EXPECT_EQ(first_a_state, trace->secret_prefix + ":A1");
    EXPECT_EQ(first_b_state, trace->secret_prefix + ":B1");
  }

  {
    asio::io_context io;
    Service service(io, continuation_config(dir.path));
    register_continuation_fixture(service, trace);
    LocalDevelopmentIdentity identity;
    Api api(service, identity);
    const auto turn_a = service.submit_session_turn(session_a, "A-second", Json{{"tag", "A2"}});
    const auto turn_b = service.submit_session_turn(session_b, "B-second", Json{{"tag", "B2"}});
    io.run();
    EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_a.at("id").get<std::string>()).at("state"),
              "succeeded");
    EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_b.at("id").get<std::string>()).at("state"),
              "succeeded");

    std::map<std::string, std::optional<std::string>> received;
    {
      std::lock_guard lock(trace->mutex);
      for (const auto &observation : trace->observed)
        received[observation.input_tag] = observation.received_state;
    }
    ASSERT_TRUE(received.contains("A2"));
    ASSERT_TRUE(received.contains("B2"));
    EXPECT_EQ(received.at("A2"), first_a_state);
    EXPECT_EQ(received.at("B2"), first_b_state);

    const auto authoritative_a =
        service.list(RecordKind::SessionContinuation, session_a).front().at("state");
    const auto duplicate_completed_a =
        service.submit_session_turn(session_a, "A-first", Json{{"tag", "A1"}});
    EXPECT_EQ(duplicate_completed_a.at("id"), first_a_turn_id);
    EXPECT_EQ(service.list(RecordKind::SessionContinuation, session_a).front().at("state"),
              authoritative_a);

    Json public_views = Json::array();
    for (const auto &id : {session_a, session_b}) {
      public_views.push_back(api.handle("GET", "/api/v1/sessions/" + id, "").body);
      public_views.push_back(api.handle("GET", "/api/v1/sessions/" + id + "/turns", "").body);
      public_views.push_back(
          api.handle("GET", "/api/v1/sessions/" + id + "/events?after=0", "").body);
      const auto turns = service.list(RecordKind::SessionTurn, id);
      ASSERT_EQ(turns.size(), 2U);
      const auto run_id = turns.back().at("run_id").get<std::string>();
      public_views.push_back(api.handle("GET", "/api/v1/runs/" + run_id, "").body);
    }
    EXPECT_EQ(public_views.dump().find(trace->secret_prefix), std::string::npos);

    const auto events = service.session_events(session_a, 0, 100);
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [&](const Json &event) {
                              return event.value("type", std::string{}) == "input.accepted" &&
                                     event.value("turn_id", std::string{}) == first_a_turn_id;
                            }),
              1);
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [&](const Json &event) {
                              return event.value("type", std::string{}) ==
                                         "turn.execution.completed" &&
                                     event.value("turn_id", std::string{}) == first_a_turn_id;
                            }),
              1);
    {
      std::lock_guard lock(trace->mutex);
      EXPECT_EQ(
          std::count_if(trace->observed.begin(), trace->observed.end(),
                        [](const ContinuationObservation &item) { return item.input_tag == "A1"; }),
          1);
    }
    ASSERT_FALSE(events.empty());
    HttpServer server(io, api, "127.0.0.1", 0);
    server.start();
    io.restart();
    std::jthread server_thread([&] { io.run(); });
    asio::io_context peer_io;
    boost::beast::tcp_stream client(peer_io);
    client.expires_after(std::chrono::seconds(5));
    client.connect({asio::ip::make_address("127.0.0.1"), server.port()});
    const auto request = "GET /api/v1/sessions/" + session_a +
                         "/events/stream HTTP/1.1\r\nHost: localhost\r\n"
                         "Accept: text/event-stream\r\nLast-Event-ID: 0\r\n\r\n";
    asio::write(client, asio::buffer(request));
    asio::streambuf response_buffer;
    asio::read_until(client, response_buffer, "\r\n\r\n");
    {
      std::istream headers(&response_buffer);
      std::string line;
      while (std::getline(headers, line) && line != "\r") {
      }
    }
    std::string streamed_frames;
    for (std::size_t i = 0; i < events.size(); ++i) {
      client.expires_after(std::chrono::seconds(5));
      asio::read_until(client, response_buffer, "\n\n");
      std::istream frame(&response_buffer);
      for (std::string line; std::getline(frame, line) && !line.empty() && line != "\r";) {
        streamed_frames += line;
        streamed_frames.push_back('\n');
      }
      streamed_frames.push_back('\n');
    }
    EXPECT_EQ(streamed_frames.find(trace->secret_prefix), std::string::npos);
    EXPECT_EQ(log_capture.str().find(trace->secret_prefix), std::string::npos);
    boost::system::error_code ec;
    client.socket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    client.socket().close(ec);
    server.stop();
    server_thread.join();
  }
}

TEST(Sessions, PostgresOpaqueContinuationSurvivesRestartAndIsSessionScoped) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_session_continuation_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};

  TemporaryDirectory dir;
  auto options = continuation_config(dir.path);
  options.postgres_dsn = dsn;
  options.postgres_schema = schema;
  options.validate();
  auto trace = std::make_shared<ContinuationFixtureState>();
  trace->secret_prefix = "postgres-opaque-session-secret-" + uuid();
  DefaultLogCapture log_capture;
  std::string session_a;
  std::string session_b;
  std::string continuation_a;
  std::string continuation_b;
  std::string first_a_turn_id;
  {
    asio::io_context io;
    Service service(io, options);
    register_continuation_fixture(service, trace);
    const auto pipeline =
        service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
    session_a = service.create_session(pipeline).id;
    session_b = service.create_session(pipeline).id;
    const auto first_a = service.submit_session_turn(session_a, "postgres-A1", Json{{"tag", "A1"}});
    const auto first_b = service.submit_session_turn(session_b, "postgres-B1", Json{{"tag", "B1"}});
    first_a_turn_id = first_a.at("id").get<std::string>();
    io.run();
    EXPECT_EQ(service.get(RecordKind::SessionTurn, first_a.at("id").get<std::string>()).at("state"),
              "succeeded");
    EXPECT_EQ(service.get(RecordKind::SessionTurn, first_b.at("id").get<std::string>()).at("state"),
              "succeeded");
    const auto stored_a = service.list(RecordKind::SessionContinuation, session_a);
    const auto stored_b = service.list(RecordKind::SessionContinuation, session_b);
    ASSERT_EQ(stored_a.size(), 1U);
    ASSERT_EQ(stored_b.size(), 1U);
    continuation_a = stored_a.front().at("state").get<std::string>();
    continuation_b = stored_b.front().at("state").get<std::string>();
  }

  {
    asio::io_context io;
    Service service(io, options);
    register_continuation_fixture(service, trace);
    const auto second_a =
        service.submit_session_turn(session_a, "postgres-A2", Json{{"tag", "A2"}});
    const auto second_b =
        service.submit_session_turn(session_b, "postgres-B2", Json{{"tag", "B2"}});
    io.run();
    EXPECT_EQ(
        service.get(RecordKind::SessionTurn, second_a.at("id").get<std::string>()).at("state"),
        "succeeded");
    EXPECT_EQ(
        service.get(RecordKind::SessionTurn, second_b.at("id").get<std::string>()).at("state"),
        "succeeded");
    std::map<std::string, std::optional<std::string>> received;
    {
      std::lock_guard lock(trace->mutex);
      for (const auto &observation : trace->observed)
        received[observation.input_tag] = observation.received_state;
    }
    ASSERT_TRUE(received.contains("A2"));
    ASSERT_TRUE(received.contains("B2"));
    EXPECT_EQ(received.at("A2"), continuation_a);
    EXPECT_EQ(received.at("B2"), continuation_b);

    const auto authoritative_a =
        service.list(RecordKind::SessionContinuation, session_a).front().at("state");
    const auto duplicate_completed_a =
        service.submit_session_turn(session_a, "postgres-A1", Json{{"tag", "A1"}});
    EXPECT_EQ(duplicate_completed_a.at("id"), first_a_turn_id);
    EXPECT_EQ(service.list(RecordKind::SessionContinuation, session_a).front().at("state"),
              authoritative_a);
    const auto events_a = service.session_events(session_a, 0, 100);
    EXPECT_EQ(std::count_if(events_a.begin(), events_a.end(),
                            [&](const Json &event) {
                              return event.value("type", std::string{}) == "input.accepted" &&
                                     event.value("turn_id", std::string{}) == first_a_turn_id;
                            }),
              1);
    EXPECT_EQ(std::count_if(events_a.begin(), events_a.end(),
                            [&](const Json &event) {
                              return event.value("type", std::string{}) ==
                                         "turn.execution.completed" &&
                                     event.value("turn_id", std::string{}) == first_a_turn_id;
                            }),
              1);
    {
      std::lock_guard lock(trace->mutex);
      EXPECT_EQ(
          std::count_if(trace->observed.begin(), trace->observed.end(),
                        [](const ContinuationObservation &item) { return item.input_tag == "A1"; }),
          1);
    }

    LocalDevelopmentIdentity identity;
    Api api(service, identity);
    for (const auto &id : {session_a, session_b}) {
      const auto public_session = api.handle("GET", "/api/v1/sessions/" + id, "").body;
      const auto public_turns = api.handle("GET", "/api/v1/sessions/" + id + "/turns", "").body;
      const auto public_events =
          api.handle("GET", "/api/v1/sessions/" + id + "/events?after=0", "").body;
      EXPECT_EQ(public_session.dump().find(trace->secret_prefix), std::string::npos);
      EXPECT_EQ(public_turns.dump().find(trace->secret_prefix), std::string::npos);
      EXPECT_EQ(public_events.dump().find(trace->secret_prefix), std::string::npos);
    }
    EXPECT_EQ(log_capture.str().find(trace->secret_prefix), std::string::npos);
  }
}

TEST(Sessions, InvalidAndTimedOutContinuationDoNotAdvanceState) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  trace->secret_prefix = "opaque-failure-secret-" + uuid();
  DefaultLogCapture log_capture;
  Service service(io, continuation_config(dir.path));
  register_continuation_fixture(service, trace);
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  const auto pipeline =
      service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const auto first = service.submit_session_turn(session.id, "first", Json{{"tag", "first"}});
  io.run();
  const auto first_state = service.list(RecordKind::SessionContinuation, session.id)
                               .front()
                               .at("state")
                               .get<std::string>();

  trace->reject_state = true;
  io.restart();
  const auto invalid = service.submit_session_turn(session.id, "invalid", Json{{"tag", "invalid"}});
  io.run();
  EXPECT_EQ(service.get(RecordKind::SessionTurn, invalid.at("id").get<std::string>()).at("state"),
            "failed");
  EXPECT_EQ(service.list(RecordKind::SessionContinuation, session.id).front().at("state"),
            first_state);

  trace->reject_state = false;
  trace->timeout = true;
  io.restart();
  const auto timed_out =
      service.submit_session_turn(session.id, "timeout", Json{{"tag", "timeout"}});
  io.run();
  EXPECT_EQ(service.get(RecordKind::SessionTurn, timed_out.at("id").get<std::string>()).at("state"),
            "failed");
  EXPECT_EQ(service.list(RecordKind::SessionContinuation, session.id).front().at("state"),
            first_state);

  const auto turns = api.handle("GET", "/api/v1/sessions/" + session.id + "/turns", "").body;
  const auto events =
      api.handle("GET", "/api/v1/sessions/" + session.id + "/events?after=0", "").body;
  Json public_errors = Json::array();
  for (const auto &failed_turn : {invalid, timed_out}) {
    const auto turn = service.get(RecordKind::SessionTurn, failed_turn.at("id").get<std::string>());
    const auto run_id = turn.value("run_id", std::string{});
    if (!run_id.empty())
      public_errors.push_back(api.handle("GET", "/api/v1/runs/" + run_id, "").body);
  }
  EXPECT_EQ(turns.dump().find(trace->secret_prefix), std::string::npos);
  EXPECT_EQ(events.dump().find(trace->secret_prefix), std::string::npos);
  EXPECT_EQ(public_errors.dump().find(trace->secret_prefix), std::string::npos);
  EXPECT_EQ(log_capture.str().find(trace->secret_prefix), std::string::npos);
  EXPECT_EQ(api.handle("GET", "/api/v1/sessions/" + session.id + "/turns", "").status, 200U);
  EXPECT_EQ(service.get(RecordKind::SessionTurn, first.at("id").get<std::string>()).at("state"),
            "succeeded");
}

TEST(Sessions, ContextGenerationIsPassedAndRunSnapshotDoesNotChange) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  trace->secret_prefix = "snapshot-private-state";
  Service service(io, continuation_config(dir.path));
  register_continuation_fixture(service, trace);
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  const auto pipeline =
      service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const Json first_payload{{"memory", "provider-neutral context"}};
  const Json generation_request{{"expected_generation", 0},
                                {"through_turn_sequence", 0},
                                {"idempotency_key", "context-generation-0"},
                                {"representation_kind", "structured-context"},
                                {"representation_version", "1"},
                                {"payload", first_payload}};
  const auto generation_response = api.handle(
      "POST", "/api/v1/sessions/" + session.id + "/context/generations", generation_request.dump());
  ASSERT_EQ(generation_response.status, 201U);
  EXPECT_FALSE(generation_response.body.contains("payload"));
  const auto retried_generation = api.handle(
      "POST", "/api/v1/sessions/" + session.id + "/context/generations", generation_request.dump());
  ASSERT_EQ(retried_generation.status, 201U);
  EXPECT_EQ(retried_generation.body.at("id"), generation_response.body.at("id"));
  auto conflicting_generation_request = generation_request;
  conflicting_generation_request["payload"] = {{"memory", "different"}};
  EXPECT_EQ(api.handle("POST", "/api/v1/sessions/" + session.id + "/context/generations",
                       conflicting_generation_request.dump())
                .status,
            409U);
  const auto first_generation = generation_response.body;
  EXPECT_EQ(
      service
          .get(RecordKind::SessionContextGeneration, first_generation.at("id").get<std::string>())
          .at("generation"),
      1U);
  const auto first = service.submit_session_turn(session.id, "turn-1", Json{{"tag", "first"}});
  io.run();
  const auto first_turn = service.get(RecordKind::SessionTurn, first.at("id").get<std::string>());
  const auto first_run_id = first_turn.at("run_id").get<std::string>();
  const auto first_snapshot = service.get(RecordKind::RunContextSnapshot, first_run_id);
  EXPECT_EQ(first_snapshot.at("context_generation_id"), first_generation.at("id"));
  EXPECT_EQ(first_snapshot.at("context_through_turn_sequence"), 0U);
  {
    std::lock_guard lock(trace->mutex);
    ASSERT_EQ(trace->observed.size(), 1U);
    EXPECT_EQ(trace->observed[0].context_generation_id,
              first_generation.at("id").get<std::string>());
    EXPECT_EQ(trace->observed[0].context_payload, first_payload);
    EXPECT_TRUE(trace->observed[0].context_turns.empty());
  }

  io.restart();
  const auto second = service.submit_session_turn(session.id, "turn-2", Json{{"tag", "second"}});
  io.run();
  const auto second_turn = service.get(RecordKind::SessionTurn, second.at("id").get<std::string>());
  const auto second_run_id = second_turn.at("run_id").get<std::string>();
  const auto second_snapshot = service.get(RecordKind::RunContextSnapshot, second_run_id);
  {
    std::lock_guard lock(trace->mutex);
    ASSERT_EQ(trace->observed.size(), 2U);
    ASSERT_EQ(trace->observed[1].context_turns.size(), 1U);
    EXPECT_EQ(trace->observed[1].context_turns[0].at("id"), first_turn.at("id"));
    EXPECT_EQ(trace->observed[1].context_turns[0].at("sequence"), 1U);
  }
  ASSERT_EQ(second_snapshot.at("provider_continuations").size(), 1U);
  EXPECT_EQ(second_snapshot.at("provider_continuations")[0].at("state"),
            "snapshot-private-state:first");
  const auto second_generation = service.create_session_context_generation(
      session.id, 1, 2, "context-generation-1", "structured-context", "1",
      Json{{"memory", "through turn two"}});
  EXPECT_EQ(second_generation.at("generation"), 2U);
  EXPECT_EQ(service.get(RecordKind::RunContextSnapshot, second_run_id).at("context_generation_id"),
            first_generation.at("id"));
  EXPECT_NE(service.list(RecordKind::SessionContinuation, session.id).front().at("state"),
            second_snapshot.at("provider_continuations")[0].at("state"));

  const auto session_context = api.handle("GET", "/api/v1/sessions/" + session.id + "/context", "");
  ASSERT_EQ(session_context.status, 200U);
  EXPECT_EQ(session_context.body.at("current_generation").at("id"), second_generation.at("id"));
  EXPECT_FALSE(session_context.body.at("current_generation").contains("payload"));
  const auto run_context = api.handle("GET", "/api/v1/runs/" + second_run_id + "/context", "");
  ASSERT_EQ(run_context.status, 200U);
  EXPECT_EQ(run_context.body.at("context_generation_id"), first_generation.at("id"));
  EXPECT_EQ(run_context.body.dump().find("snapshot-private-state"), std::string::npos);
}

TEST(Sessions, AutomaticReductionCreatesGenerationBeforeRunBinding) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  auto options = continuation_config(dir.path);
  options.session_context_reduction_enabled = true;
  options.session_context_reducer = "recent-turns";
  options.session_context_reduction_threshold_bytes = 700;
  options.session_context_reduction_target_bytes = 600;
  options.session_context_reduction_max_input_bytes = 4096;
  Service service(io, options);
  register_continuation_fixture(service, trace);
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  const auto advertised_capabilities =
      api.handle("GET", "/api/v1/version", "").body.at("capabilities");
  ASSERT_TRUE(std::find(advertised_capabilities.begin(), advertised_capabilities.end(),
                        Json("sessions.context_reduction")) != advertised_capabilities.end());
  const auto pipeline =
      service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const auto first = service.submit_session_turn(session.id, "auto-reduce-first", Json::object());
  io.run();
  io.restart();
  const auto second = service.submit_session_turn(session.id, "auto-reduce-second", Json::object());
  io.run();
  EXPECT_TRUE(service.list(RecordKind::SessionContextGeneration, session.id).empty());
  io.restart();
  const auto third = service.submit_session_turn(session.id, "auto-reduce-third", Json::object());
  io.run();
  const auto first_turn = service.get(RecordKind::SessionTurn, first.at("id").get<std::string>());
  const auto second_turn = service.get(RecordKind::SessionTurn, second.at("id").get<std::string>());
  const auto third_turn = service.get(RecordKind::SessionTurn, third.at("id").get<std::string>());
  ASSERT_EQ(third_turn.at("state"), "succeeded");
  const auto generations = service.list(RecordKind::SessionContextGeneration, session.id);
  ASSERT_EQ(generations.size(), 1U);
  EXPECT_EQ(generations[0].at("through_turn_sequence"), 2U);
  EXPECT_EQ(generations[0].at("payload").at("format"), "laso.recent-turns");
  ASSERT_EQ(generations[0].at("payload").at("turns").size(), 1U);
  EXPECT_EQ(generations[0].at("payload").at("turns")[0].at("sequence"), 2U);
  const auto snapshot =
      service.get(RecordKind::RunContextSnapshot, third_turn.at("run_id").get<std::string>());
  EXPECT_EQ(snapshot.at("context_generation_id"), generations[0].at("id"));
  EXPECT_EQ(first_turn.at("state"), "succeeded");
  EXPECT_EQ(second_turn.at("state"), "succeeded");
  EXPECT_EQ(service.list(RecordKind::SessionTurn, session.id).size(), 3U);
  io.restart();
  const auto fourth = service.submit_session_turn(session.id, "auto-reduce-fourth", Json::object());
  io.run();
  const auto fourth_turn = service.get(RecordKind::SessionTurn, fourth.at("id").get<std::string>());
  ASSERT_EQ(fourth_turn.at("state"), "succeeded");
  const auto advanced_generations = service.list(RecordKind::SessionContextGeneration, session.id);
  ASSERT_EQ(advanced_generations.size(), 2U);
  EXPECT_EQ(advanced_generations[1].at("generation"), 2U);
  EXPECT_EQ(advanced_generations[1].at("predecessor_id"), advanced_generations[0].at("id"));
  EXPECT_EQ(advanced_generations[1].at("through_turn_sequence"), 3U);
  const auto fourth_snapshot =
      service.get(RecordKind::RunContextSnapshot, fourth_turn.at("run_id").get<std::string>());
  EXPECT_EQ(fourth_snapshot.at("context_generation_id"), advanced_generations[1].at("id"));
  EXPECT_EQ(service.list(RecordKind::SessionTurn, session.id).size(), 4U);
  {
    std::lock_guard lock(trace->mutex);
    ASSERT_EQ(trace->observed.size(), 4U);
    EXPECT_EQ(trace->observed[2].context_generation_id, generations[0].at("id").get<std::string>());
    EXPECT_TRUE(trace->observed[2].context_turns.empty());
    EXPECT_EQ(trace->observed[2].context_payload, generations[0].at("payload"));
    EXPECT_EQ(trace->observed[3].context_generation_id,
              advanced_generations[1].at("id").get<std::string>());
    EXPECT_EQ(trace->observed[3].context_payload, advanced_generations[1].at("payload"));
  }
}

TEST(Sessions, AutomaticReductionFailureMalformedAndTimedOutResultsFailClosed) {
  for (const auto scenario : {0, 1, 2}) {
    TemporaryDirectory dir;
    asio::io_context io;
    auto options = config(dir.path);
    options.session_context_reduction_enabled = true;
    options.session_context_reducer = "test-marker";
    options.session_context_reduction_threshold_bytes = 100;
    options.session_context_reduction_target_bytes = 64;
    options.session_context_reduction_max_input_bytes = 4096;
    if (scenario == 2)
      options.session_context_reduction_timeout_ms = 1;
    Service service(io, options);
    service.context_reducer_registry().add(
        "test-marker",
        std::make_shared<MarkerContextReducer>(scenario == 0, scenario == 1,
                                               scenario == 2 ? Milliseconds{10} : Milliseconds{0}));
    service.functions().add(
        "session_reduction_probe",
        std::make_shared<Function>(
            [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; }));
    const auto pipeline =
        service.register_pipeline(single("type: function\n    function: session_reduction_probe"))
            .at("id")
            .get<std::string>();
    const auto session = service.create_session(pipeline);
    const auto first = service.submit_session_turn(session.id, "fail-closed-first", Json::object());
    io.run();
    io.restart();
    const auto second =
        service.submit_session_turn(session.id, "fail-closed-second", Json::object());
    const auto second_turn =
        service.get(RecordKind::SessionTurn, second.at("id").get<std::string>());
    EXPECT_EQ(second_turn.at("state"), "claimed");
    EXPECT_TRUE(second_turn.value("run_id", std::string{}).empty());
    EXPECT_TRUE(service.list(RecordKind::SessionContextGeneration, session.id).empty());
    EXPECT_EQ(service.list(RecordKind::Run).size(), 1U);
    EXPECT_EQ(service.get(RecordKind::SessionTurn, first.at("id").get<std::string>()).at("state"),
              "succeeded");
  }
}

TEST(Sessions, RunContextSnapshotPreservesRunLocalContinuationProgress) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  trace->secret_prefix = "run-local-state-" + uuid();
  Service service(io, continuation_config(dir.path));
  register_continuation_fixture(service, trace);
  const auto pipeline = service
                            .register_pipeline(R"(laso: "1"
name: repeated-provider
version: 1
nodes:
  first:
    type: agent
    model: session-model
    prompt: First provider step.
  second:
    type: agent
    model: session-model
    prompt: Second provider step.
edges:
  - {from: input, to: first}
  - {from: first, to: second}
  - {from: second, to: output}
)")
                            .at("id")
                            .get<std::string>();
  const auto session = service.create_session(pipeline);
  (void)service.create_session_context_generation(session.id, 0, 0, "run-context-1",
                                                  "structured-context", "1", Json{{"x", 1}});
  (void)service.submit_session_turn(session.id, "two-provider-steps", Json{{"tag", "same-run"}});
  io.run();
  const auto turns = service.list(RecordKind::SessionTurn, session.id);
  ASSERT_EQ(turns.size(), 1U);
  ASSERT_EQ(turns.front().at("state"), "succeeded");
  const auto snapshot =
      service.get(RecordKind::RunContextSnapshot, turns.front().at("run_id").get<std::string>());
  EXPECT_TRUE(snapshot.at("provider_continuations").empty());
  std::lock_guard lock(trace->mutex);
  ASSERT_EQ(trace->observed.size(), 2U);
  EXPECT_FALSE(trace->observed[0].received_state.has_value());
  ASSERT_TRUE(trace->observed[1].received_state.has_value());
  EXPECT_EQ(*trace->observed[1].received_state, trace->secret_prefix + ":same-run");
}

TEST(Sessions, CloseAfterProviderCallDoesNotAdvanceContinuation) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  trace->secret_prefix = "cancelled-provider-state-" + uuid();
  Service service(io, continuation_config(dir.path));
  register_continuation_fixture(service, trace);
  const auto pipeline =
      service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const auto first =
      service.submit_session_turn(session.id, "before-close", Json{{"tag", "accepted-state"}});
  io.run();
  const auto first_turn_id = first.at("id").get<std::string>();
  ASSERT_EQ(service.get(RecordKind::SessionTurn, first_turn_id).at("state"), "succeeded");
  const auto previous_continuation =
      service.list(RecordKind::SessionContinuation, session.id).front().at("state");

  {
    std::lock_guard lock(trace->mutex);
    trace->block_until_released = true;
    trace->provider_entered = false;
    trace->allow_provider_return = false;
    trace->provider_returned = false;
  }
  io.restart();
  const auto accepted =
      service.submit_session_turn(session.id, "close-during-provider", Json{{"tag", "blocked"}});
  const auto turn_id = accepted.at("id").get<std::string>();
  std::jthread service_thread([&] { io.run(); });

  bool provider_entered = false;
  {
    std::unique_lock lock(trace->mutex);
    provider_entered = trace->condition.wait_for(lock, std::chrono::seconds(5),
                                                 [&] { return trace->provider_entered; });
  }
  if (!provider_entered) {
    {
      std::lock_guard lock(trace->mutex);
      trace->allow_provider_return = true;
    }
    trace->condition.notify_all();
    service.shutdown();
    io.stop();
    service_thread.join();
    FAIL() << "Continuation fixture did not reach its provider barrier";
    return;
  }

  bool close_succeeded = true;
  try {
    service.close_session(session.id);
  } catch (...) {
    close_succeeded = false;
  }
  {
    std::lock_guard lock(trace->mutex);
    trace->allow_provider_return = true;
  }
  trace->condition.notify_all();

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  std::string state;
  while (std::chrono::steady_clock::now() < deadline) {
    state = service.get(RecordKind::SessionTurn, turn_id).value("state", "");
    if (state == "cancelled" || state == "failed" || state == "succeeded")
      break;
    std::this_thread::sleep_for(Milliseconds{5});
  }
  service.shutdown();
  io.stop();
  service_thread.join();

  EXPECT_TRUE(close_succeeded);
  EXPECT_TRUE(trace->provider_returned);
  EXPECT_EQ(state, "cancelled");
  EXPECT_EQ(service.get(RecordKind::SessionTurn, first_turn_id).at("state"), "succeeded");
  EXPECT_EQ(service.agent_session(session.id).state, "closed");
  const auto continuations = service.list(RecordKind::SessionContinuation, session.id);
  ASSERT_EQ(continuations.size(), 1U);
  EXPECT_EQ(continuations.front().at("state"), previous_continuation);
  const auto events = service.session_events(session.id, 0, 100);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) ==
                                       "turn.execution.cancelled" &&
                                   event.value("turn_id", std::string{}) == turn_id;
                          }),
            1);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) ==
                                       "turn.execution.completed" &&
                                   event.value("turn_id", std::string{}) == first_turn_id;
                          }),
            1);
  EXPECT_EQ(Json(events).dump().find(trace->secret_prefix), std::string::npos);
}

TEST(Sessions, UnsupportedContinuationProviderFailsClosed) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  Service service(io, continuation_config(dir.path));
  register_continuation_fixture(service, trace, ContinuationMode::Unsupported);
  const auto pipeline =
      service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const auto accepted =
      service.submit_session_turn(session.id, "unsupported", Json{{"tag", "unsupported"}});
  io.run();
  EXPECT_EQ(service.get(RecordKind::SessionTurn, accepted.at("id").get<std::string>()).at("state"),
            "failed");
  EXPECT_TRUE(trace->observed.empty());
  EXPECT_TRUE(service.list(RecordKind::SessionContinuation, session.id).empty());
  EXPECT_EQ(service.providers().at(0).at("continuation_mode"), "unsupported");
}

TEST(Sessions, OpaqueProviderInParallelBranchFailsClosed) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  Service service(io, continuation_config(dir.path));
  register_continuation_fixture(service, trace);
  const auto yaml = R"(laso: "1"
name: session-parallel-provider
version: 1
nodes:
  fork: {type: parallel, join: join}
  branch_a: {type: agent, model: session-model, prompt: Continue safely.}
  branch_b: {type: function, function: identity}
  join: {type: join}
edges:
  - {from: input, to: fork}
  - {from: fork, to: branch_a}
  - {from: fork, to: branch_b}
  - {from: branch_a, to: join}
  - {from: branch_b, to: join}
  - {from: join, to: output}
)";
  const auto pipeline = service.register_pipeline(yaml).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const auto accepted =
      service.submit_session_turn(session.id, "parallel-provider", Json{{"value", "test"}});
  io.run();
  EXPECT_EQ(service.get(RecordKind::SessionTurn, accepted.at("id").get<std::string>()).at("state"),
            "failed");
  EXPECT_TRUE(trace->observed.empty());
  EXPECT_TRUE(service.list(RecordKind::SessionContinuation, session.id).empty());
}

TEST(Sessions, DeclaredStatelessProviderDoesNotInventContinuation) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto trace = std::make_shared<ContinuationFixtureState>();
  Service service(io, continuation_config(dir.path));
  register_continuation_fixture(service, trace, ContinuationMode::Stateless);
  const auto pipeline =
      service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const auto first = service.submit_session_turn(session.id, "stateless-1", Json{{"tag", "one"}});
  const auto second = service.submit_session_turn(session.id, "stateless-2", Json{{"tag", "two"}});
  io.run();
  EXPECT_EQ(service.get(RecordKind::SessionTurn, first.at("id").get<std::string>()).at("state"),
            "succeeded");
  EXPECT_EQ(service.get(RecordKind::SessionTurn, second.at("id").get<std::string>()).at("state"),
            "succeeded");
  EXPECT_TRUE(service.list(RecordKind::SessionContinuation, session.id).empty());
  ASSERT_EQ(trace->observed.size(), 2U);
  EXPECT_FALSE(trace->observed[0].received_state);
  EXPECT_FALSE(trace->observed[1].received_state);
}

TEST(Sessions, QueuedSessionsDispatchWhenRunCapacityFrees) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto options = config(dir.path);
  options.max_runs = 1;
  Service service(io, options);
  std::mutex order_mutex;
  std::vector<std::string> execution_order;
  service.functions().add(
      "session_capacity",
      std::make_shared<Function>([&](ExecutionContext &context, const Json &input) -> Task<Json> {
        const auto value = input.at("value").get<std::string>();
        if (value == "A")
          co_await context.delay(Milliseconds{100});
        {
          std::lock_guard lock(order_mutex);
          execution_order.push_back(value);
        }
        co_return input;
      }));
  const auto pipeline =
      service.register_pipeline(single("type: function\n    function: session_capacity"))
          .at("id")
          .get<std::string>();
  const auto session_a = service.create_session(pipeline);
  const auto session_b = service.create_session(pipeline);
  const auto turn_a = service.submit_session_turn(session_a.id, "capacity-a", Json{{"value", "A"}});
  const auto turn_b = service.submit_session_turn(session_b.id, "capacity-b", Json{{"value", "B"}});
  io.run();

  EXPECT_EQ(execution_order, (std::vector<std::string>{"A", "B"}));
  EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_a.at("id").get<std::string>()).at("state"),
            "succeeded");
  EXPECT_EQ(service.get(RecordKind::SessionTurn, turn_b.at("id").get<std::string>()).at("state"),
            "succeeded");
}

TEST(Sessions, PostgresSingleOwnerCompletesAcceptedTurn) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_session_exec_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};

  TemporaryDirectory dir;
  Config options = config(dir.path);
  options.postgres_dsn = dsn;
  options.postgres_schema = schema;
  asio::io_context io;
  Service service(io, options);
  const auto pipeline = service.register_pipeline(single()).at("id").get<std::string>();
  const auto session = service.create_session(pipeline);
  const auto accepted =
      service.submit_session_turn(session.id, "postgres-session-turn", Json{{"value", "durable"}});
  io.run();

  const auto turn_id = accepted.at("id").get<std::string>();
  const auto turn = service.get(RecordKind::SessionTurn, turn_id);
  EXPECT_EQ(turn.at("state"), "succeeded");
  EXPECT_TRUE(turn.contains("run_id"));
  EXPECT_EQ(
      service.get(RecordKind::Run, turn.at("run_id").get<std::string>()).get<laso::Run>().state,
      RunState::Completed);
  const auto events = service.session_events(session.id, 0, 100);
  bool saw_completion = false;
  for (const auto &event : events)
    if (event.at("type") == "turn.execution.completed" && event.at("turn_id") == turn_id)
      saw_completion = true;
  EXPECT_TRUE(saw_completion);
}

TEST(Sessions, PostgresMultiInstanceDispatchesQueuedTurnOnStartup) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_session_startup_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};

  TemporaryDirectory directory;
  auto options = config(directory.path);
  options.postgres_dsn = dsn;
  options.postgres_schema = schema;
  options.execution_mode = "multi_instance";
  options.coordination_lease_ttl_ms = 1000;
  options.coordination_heartbeat_interval_ms = 100;
  options.validate();

  const auto function = std::make_shared<Function>(
      [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; });
  AgentSession session;
  std::string pipeline_id;
  {
    asio::io_context seed_io;
    Service seed(seed_io, options);
    seed.functions().add("session_startup_recovery", function);
    pipeline_id =
        seed.register_pipeline(single("type: function\n    function: session_startup_recovery"))
            .at("id")
            .get<std::string>();
    session = seed.create_session(pipeline_id);
    seed.shutdown();
  }

  // Model a process exit after the acceptance transaction commits but before
  // its immediate in-process dispatch is durably reflected in turn state.
  StorageOptions storage_options;
  storage_options.postgres_dsn = dsn;
  storage_options.postgres_schema = schema;
  storage_options.allow_multiple_processes = true;
  auto storage = create_storage(storage_options);
  const auto turn_id = session.id + "-turn-startup-recovery";
  Json queued_turn{{"idempotency_key", "startup-recovery"},
                   {"input", {{"value", "durable"}}},
                   {"state", "queued"},
                   {"accepted_at", timestamp()},
                   {"pipeline_id", pipeline_id}};
  ASSERT_TRUE(storage->submit_session_turn(session.id, turn_id, queued_turn, Json::object()));
  storage.reset();

  asio::io_context recovery_io;
  Service recovery(recovery_io, options);
  recovery.functions().add("session_startup_recovery", function);
  std::jthread recovery_thread([&] { recovery_io.run(); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  std::string state;
  while (std::chrono::steady_clock::now() < deadline) {
    state = recovery.get(RecordKind::SessionTurn, turn_id).value("state", "");
    if (state == "succeeded" || state == "failed" || state == "cancelled")
      break;
    std::this_thread::sleep_for(Milliseconds{10});
  }
  recovery.shutdown();
  recovery_io.stop();
  recovery_thread.join();

  const auto completed = recovery.get(RecordKind::SessionTurn, turn_id);
  EXPECT_EQ(state, "succeeded");
  EXPECT_EQ(completed.at("state"), "succeeded");
  const auto run_id = completed.value("run_id", std::string{});
  ASSERT_FALSE(run_id.empty());
  const auto run = recovery.get(RecordKind::Run, run_id).get<laso::Run>();
  EXPECT_EQ(run.session_turn_id, turn_id);
  EXPECT_EQ(run.state, RunState::Completed);
  EXPECT_EQ(run.message.payload.value("value", std::string{}), "durable");
  const auto events = recovery.session_events(session.id, 0, 100);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) ==
                                       "turn.execution.completed" &&
                                   event.value("turn_id", std::string{}) == turn_id;
                          }),
            1);
}

TEST(Sessions, PostgresClaimInterruptionIsRecoveredByAnotherInstance) {
#if defined(LASO_ENABLE_SESSION_TEST_HOOKS)
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_claim_recovery_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};

  TemporaryDirectory directory;
  auto options = config(directory.path);
  options.postgres_dsn = dsn;
  options.postgres_schema = schema;
  options.execution_mode = "multi_instance";
  options.coordination_lease_ttl_ms = 1000;
  options.coordination_heartbeat_interval_ms = 100;
  options.validate();

  const auto function = std::make_shared<Function>(
      [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; });
  AgentSession session;
  std::string pipeline_id;
  {
    asio::io_context io;
    Service seed(io, options);
    seed.functions().add("session_claim_recovery", function);
    pipeline_id =
        seed.register_pipeline(single("type: function\n    function: session_claim_recovery"))
            .at("id")
            .get<std::string>();
    session = seed.create_session(pipeline_id);
    seed.shutdown();
  }

  StorageOptions storage_options;
  storage_options.postgres_dsn = dsn;
  storage_options.postgres_schema = schema;
  storage_options.allow_multiple_processes = true;
  auto storage = create_storage(storage_options);
  const auto turn_id = session.id + "-turn-claim-recovery";
  Json queued_turn{{"idempotency_key", "claim-recovery"},
                   {"input", {{"value", "recovered"}}},
                   {"state", "queued"},
                   {"accepted_at", timestamp()},
                   {"pipeline_id", pipeline_id}};
  ASSERT_TRUE(storage->submit_session_turn(session.id, turn_id, queued_turn, Json::object()));
  storage.reset();

  {
    asio::io_context old_io;
    Service old_owner(old_io, options);
    old_owner.functions().add("session_claim_recovery", function);
    bool interrupted = false;
    old_owner.runtime().set_session_test_hook([&](SessionTestPoint point) {
      if (!interrupted && point == SessionTestPoint::AfterClaim) {
        interrupted = true;
        throw std::runtime_error("injected owner interruption after claim");
      }
    });
    EXPECT_THROW(old_owner.runtime().dispatch_session(session.id), std::runtime_error);
    EXPECT_TRUE(interrupted);
    const auto claimed = old_owner.get(RecordKind::SessionTurn, turn_id);
    EXPECT_EQ(claimed.at("state"), "claimed");
    EXPECT_EQ(claimed.value("dispatch_attempt", 0U), 1U);
    EXPECT_TRUE(claimed.value("run_id", std::string{}).empty());
    old_owner.shutdown();
    old_io.stop();
  }

  asio::io_context recovery_io;
  Service recovery(recovery_io, options);
  recovery.functions().add("session_claim_recovery", function);
  std::jthread recovery_thread([&] { recovery_io.run(); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  std::string state;
  while (std::chrono::steady_clock::now() < deadline) {
    state = recovery.get(RecordKind::SessionTurn, turn_id).value("state", "");
    if (state == "succeeded" || state == "failed" || state == "cancelled")
      break;
    std::this_thread::sleep_for(Milliseconds{10});
  }
  recovery.shutdown();
  recovery_io.stop();
  recovery_thread.join();

  const auto completed = recovery.get(RecordKind::SessionTurn, turn_id);
  EXPECT_EQ(state, "succeeded");
  EXPECT_EQ(completed.at("state"), "succeeded");
  EXPECT_GE(completed.value("dispatch_attempt", 0U), 2U);
  const auto run_id = completed.value("run_id", std::string{});
  ASSERT_FALSE(run_id.empty());
  EXPECT_EQ(recovery.get(RecordKind::Run, run_id).get<laso::Run>().session_turn_id, turn_id);
  const auto events = recovery.session_events(session.id, 0, 100);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) == "turn.execution.claimed" &&
                                   event.value("turn_id", std::string{}) == turn_id;
                          }),
            2);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) ==
                                       "turn.execution.completed" &&
                                   event.value("turn_id", std::string{}) == turn_id;
                          }),
            1);
}

#endif

TEST(Sessions, PostgresTwoInstancesFenceDispatchAndPreserveSessionOrdering) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_session_claims_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};

  TemporaryDirectory dir;
  auto options = config(dir.path);
  options.postgres_dsn = dsn;
  options.postgres_schema = schema;
  options.execution_mode = "multi_instance";
  options.session_context_reduction_enabled = true;
  options.session_context_reduction_threshold_bytes = 800;
  options.session_context_reduction_target_bytes = 700;
  options.session_context_reduction_max_input_bytes = 4096;
  options.validate();

  std::atomic<unsigned> active{0};
  std::atomic<unsigned> maximum_active{0};
  std::mutex order_mutex;
  std::vector<std::string> order;
  auto function = std::make_shared<Function>([&](ExecutionContext &context,
                                                 const Json &input) -> Task<Json> {
    const auto now_active = active.fetch_add(1) + 1;
    auto observed = maximum_active.load();
    while (observed < now_active && !maximum_active.compare_exchange_weak(observed, now_active)) {
    }
    const auto tag = input.at("tag").get<std::string>();
    if (tag == "X-A" || tag == "Y-1")
      co_await context.delay(Milliseconds{100});
    {
      std::lock_guard lock(order_mutex);
      order.push_back(tag);
    }
    active.fetch_sub(1);
    co_return Json{{"ok", true}};
  });

  std::string pipeline;
  AgentSession session_x;
  AgentSession session_y;
  {
    asio::io_context bootstrap_io;
    Service bootstrap(bootstrap_io, options);
    bootstrap.functions().add("session_claim_order", function);
    pipeline =
        bootstrap.register_pipeline(single("type: function\n    function: session_claim_order"))
            .at("id")
            .get<std::string>();
    session_x = bootstrap.create_session(pipeline);
    session_y = bootstrap.create_session(pipeline);
    bootstrap.shutdown();
  }

  StorageOptions storage_options;
  storage_options.postgres_dsn = options.postgres_dsn;
  storage_options.postgres_schema = options.postgres_schema;
  storage_options.allow_multiple_processes = true;
  auto storage = create_storage(storage_options);
  const auto enqueue = [&](const AgentSession &session, const std::string &key,
                           const std::string &tag) {
    const auto turn_id = session.id + "-turn-" + key;
    Json turn{{"idempotency_key", key},
              {"input", {{"tag", tag}}},
              {"state", "queued"},
              {"accepted_at", timestamp()},
              {"pipeline_id", pipeline}};
    EXPECT_TRUE(storage->submit_session_turn(session.id, turn_id, turn, Json::object()));
    return turn_id;
  };
  const auto x_a = enqueue(session_x, "pg-x-a", "X-A");
  const auto x_b = enqueue(session_x, "pg-x-b", "X-B");
  const auto x_c = enqueue(session_x, "pg-x-c", "X-C");
  const auto y_1 = enqueue(session_y, "pg-y-1", "Y-1");
  storage.reset();

  asio::io_context first_io;
  asio::io_context second_io;
  Service first(first_io, options);
  first.functions().add("session_claim_order", function);
  Service second(second_io, options);
  second.functions().add("session_claim_order", function);
  std::barrier startup(3);
  std::jthread first_thread([&] {
    startup.arrive_and_wait();
    first_io.run();
  });
  std::jthread second_thread([&] {
    startup.arrive_and_wait();
    second_io.run();
  });
  startup.arrive_and_wait();

  const std::vector<std::pair<std::string, Json>> submitted{{session_x.id, Json{{"id", x_a}}},
                                                            {session_x.id, Json{{"id", x_b}}},
                                                            {session_x.id, Json{{"id", x_c}}},
                                                            {session_y.id, Json{{"id", y_1}}}};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  bool all_terminal = false;
  while (std::chrono::steady_clock::now() < deadline) {
    all_terminal = true;
    for (const auto &[session_id, response] : submitted) {
      (void)session_id;
      const auto turn_id = response.at("id").get<std::string>();
      const auto state = first.get(RecordKind::SessionTurn, turn_id).value("state", "");
      if (state != "succeeded" && state != "failed" && state != "cancelled")
        all_terminal = false;
    }
    if (all_terminal)
      break;
    std::this_thread::sleep_for(Milliseconds{10});
  }
  first.shutdown();
  second.shutdown();
  first_io.stop();
  second_io.stop();
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(all_terminal) << "PostgreSQL-backed session turns did not reach terminal state";

  std::set<std::string> run_ids;
  std::vector<std::string> x_order;
  for (const auto &[session_id, response] : submitted) {
    const auto turn = first.get(RecordKind::SessionTurn, response.at("id").get<std::string>());
    EXPECT_EQ(turn.at("state"), "succeeded");
    EXPECT_EQ(turn.at("sequence"), session_id == session_x.id ? x_order.size() + 1 : 1U);
    ASSERT_TRUE(turn.contains("run_id"));
    run_ids.insert(turn.at("run_id").get<std::string>());
    if (session_id == session_x.id)
      x_order.push_back(turn.at("input").at("tag").get<std::string>());
  }
  EXPECT_EQ(run_ids.size(), 4U);
  std::vector<std::string> observed_x_order;
  {
    std::lock_guard lock(order_mutex);
    for (const auto &tag : order)
      if (tag.starts_with("X-"))
        observed_x_order.push_back(tag);
  }
  EXPECT_EQ(observed_x_order, (std::vector<std::string>{"X-A", "X-B", "X-C"}));
  EXPECT_GE(maximum_active.load(), 2U);
  const auto generations = first.list(RecordKind::SessionContextGeneration, session_x.id);
  ASSERT_EQ(generations.size(), 1U);
  EXPECT_EQ(generations[0].at("through_turn_sequence"), 2U);
  const auto third_turn = first.get(RecordKind::SessionTurn, x_c);
  const auto third_snapshot =
      first.get(RecordKind::RunContextSnapshot, third_turn.at("run_id").get<std::string>());
  EXPECT_EQ(third_snapshot.at("context_generation_id"), generations[0].at("id"));
  EXPECT_EQ(first.list(RecordKind::SessionTurn, session_x.id).size(), 3U);
  for (const auto &[session_id, response] : submitted) {
    const auto events = first.session_events(session_id, 0, 100);
    const auto turn_id = response.at("id").get<std::string>();
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [&](const Json &event) {
                              return event.value("type", std::string{}) ==
                                         "turn.execution.claimed" &&
                                     event.value("turn_id", std::string{}) == turn_id;
                            }),
              1);
  }
}
TEST(Sessions, PostgresTwoInstancesDeduplicateConcurrentSubmissions) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_session_idempotency_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};

  TemporaryDirectory directory;
  auto options = config(directory.path);
  options.postgres_dsn = dsn;
  options.postgres_schema = schema;
  options.execution_mode = "multi_instance";
  options.coordination_lease_ttl_ms = 1000;
  options.coordination_heartbeat_interval_ms = 100;
  options.validate();

  std::mutex mutex;
  std::condition_variable condition;
  bool provider_entered = false;
  bool release_provider = false;
  std::atomic<unsigned> calls{0};
  std::atomic<bool> provider_hold_timed_out{false};
  auto function =
      std::make_shared<Function>([&](ExecutionContext &context, const Json &input) -> Task<Json> {
        calls.fetch_add(1);
        {
          std::lock_guard lock(mutex);
          provider_entered = true;
        }
        condition.notify_all();
        // Suspend the provider coroutine instead of blocking the service's
        // io_context while the test observes idempotent retries.
        const auto hold_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (;;) {
          {
            std::lock_guard lock(mutex);
            if (release_provider)
              break;
          }
          if (std::chrono::steady_clock::now() >= hold_deadline) {
            provider_hold_timed_out.store(true);
            break;
          }
          co_await context.delay(Milliseconds{10});
        }
        context.check();
        co_return input;
      });

  AgentSession session;
  std::string pipeline_id;
  {
    asio::io_context bootstrap_io;
    Service bootstrap(bootstrap_io, options);
    bootstrap.functions().add("session_pg_idempotency", function);
    pipeline_id =
        bootstrap.register_pipeline(single("type: function\n    function: session_pg_idempotency"))
            .at("id")
            .get<std::string>();
    session = bootstrap.create_session(pipeline_id);
    bootstrap.shutdown();
  }

  asio::io_context first_io;
  asio::io_context second_io;
  Service first(first_io, options);
  first.functions().add("session_pg_idempotency", function);
  Service second(second_io, options);
  second.functions().add("session_pg_idempotency", function);

  const Json input{{"tag", "one-logical-turn"}};
  std::barrier submit_start(3);
  Json response_a;
  Json response_b;
  std::atomic<bool> submit_error_a{false};
  std::atomic<bool> submit_error_b{false};
  std::jthread submitter_a([&] {
    submit_start.arrive_and_wait();
    try {
      response_a = first.submit_session_turn(session.id, "same-idempotency-key", input);
    } catch (...) {
      submit_error_a = true;
    }
  });
  std::jthread submitter_b([&] {
    submit_start.arrive_and_wait();
    try {
      response_b = second.submit_session_turn(session.id, "same-idempotency-key", input);
    } catch (...) {
      submit_error_b = true;
    }
  });
  submit_start.arrive_and_wait();
  submitter_a.join();
  submitter_b.join();
  ASSERT_FALSE(submit_error_a);
  ASSERT_FALSE(submit_error_b);
  ASSERT_EQ(response_a.at("id"), response_b.at("id"));
  ASSERT_EQ(response_a.at("sequence"), response_b.at("sequence"));

  EXPECT_THROW(second.submit_session_turn(session.id, "same-idempotency-key",
                                          Json{{"tag", "conflicting-input"}}),
               Error);

  std::jthread first_thread([&] { first_io.run(); });
  std::jthread second_thread([&] { second_io.run(); });
  // The 5 s entry bound spans five 1 s lease periods; the 100 ms heartbeat
  // renews ownership throughout. The 10 s completion bound stays finite.
  bool entered = false;
  {
    std::unique_lock lock(mutex);
    entered = condition.wait_for(lock, std::chrono::seconds(5), [&] { return provider_entered; });
  }
  std::cerr << "[session-pg-idempotency] provider barrier entered=" << entered << std::endl;
  if (!entered) {
    const auto state = first.get(RecordKind::SessionTurn, response_a.at("id").get<std::string>())
                           .value("state", "<missing>");
    {
      std::lock_guard lock(mutex);
      release_provider = true;
    }
    condition.notify_all();
    first.shutdown();
    second.shutdown();
    first_io.stop();
    second_io.stop();
    first_thread.join();
    second_thread.join();
    FAIL() << "Neither PostgreSQL service executed the committed turn; state=" << state
           << ", provider calls=" << calls.load();
    return;
  }

  std::cerr << "[session-pg-idempotency] running retry begin" << std::endl;
  const auto running_retry_started = std::chrono::steady_clock::now();
  const auto duplicate_running =
      second.submit_session_turn(session.id, "same-idempotency-key", input);
  const auto running_retry_elapsed = std::chrono::steady_clock::now() - running_retry_started;
  std::cerr << "[session-pg-idempotency] running retry returned in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(running_retry_elapsed).count()
            << " ms; provider hold expired=" << provider_hold_timed_out.load() << std::endl;
  EXPECT_FALSE(provider_hold_timed_out.load())
      << "provider watchdog released the test before the running retry returned";
  EXPECT_EQ(duplicate_running.at("id"), response_a.at("id"));
  EXPECT_EQ(
      first.get(RecordKind::SessionTurn, response_a.at("id").get<std::string>()).value("state", ""),
      "running");

  std::cerr << "[session-pg-idempotency] conflicting retry begin" << std::endl;
  const auto conflict_retry_started = std::chrono::steady_clock::now();
  EXPECT_THROW(second.submit_session_turn(session.id, "same-idempotency-key",
                                          Json{{"tag", "conflicting-input"}}),
               Error);
  const auto conflict_retry_elapsed = std::chrono::steady_clock::now() - conflict_retry_started;
  std::cerr << "[session-pg-idempotency] conflicting retry returned in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(conflict_retry_elapsed).count()
            << " ms" << std::endl;
  EXPECT_FALSE(provider_hold_timed_out.load())
      << "provider watchdog released the test before conflicting reuse returned";
  EXPECT_EQ(
      first.get(RecordKind::SessionTurn, response_a.at("id").get<std::string>()).value("state", ""),
      "running");
  EXPECT_EQ(calls.load(), 1U);

  {
    std::lock_guard lock(mutex);
    release_provider = true;
  }
  condition.notify_all();

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  bool terminal = false;
  std::string last_state;
  while (std::chrono::steady_clock::now() < deadline) {
    last_state = first.get(RecordKind::SessionTurn, response_a.at("id").get<std::string>())
                     .value("state", "<missing>");
    if (last_state == "succeeded" || last_state == "failed" || last_state == "cancelled") {
      terminal = true;
      break;
    }
    std::this_thread::sleep_for(Milliseconds{10});
  }
  first.shutdown();
  second.shutdown();
  first_io.stop();
  second_io.stop();
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(terminal) << "the shared PostgreSQL turn did not reach a terminal state; last state="
                        << last_state << ", provider calls=" << calls.load();

  const auto completed = first.get(RecordKind::SessionTurn, response_a.at("id").get<std::string>());
  EXPECT_EQ(completed.at("state"), "succeeded");
  EXPECT_EQ(completed.at("id"), response_b.at("id"));
  const auto run_id = completed.at("run_id").get<std::string>();
  ASSERT_FALSE(run_id.empty());
  const auto run = first.get(RecordKind::Run, run_id).get<laso::Run>();
  EXPECT_EQ(run.session_turn_id, completed.at("id").get<std::string>());
  EXPECT_GT(run.fencing_token, 0U);
  EXPECT_EQ(calls.load(), 1U);

  const auto duplicate_completed =
      second.submit_session_turn(session.id, "same-idempotency-key", input);
  EXPECT_EQ(duplicate_completed.at("id"), completed.at("id"));
  EXPECT_EQ(duplicate_completed.at("run_id"), completed.at("run_id"));
  const auto events = first.session_events(session.id, 0, 100);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) == "input.accepted" &&
                                   event.value("turn_id", std::string{}) ==
                                       completed.at("id").get<std::string>();
                          }),
            1);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) == "turn.execution.claimed" &&
                                   event.value("turn_id", std::string{}) ==
                                       completed.at("id").get<std::string>();
                          }),
            1);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) == "turn.execution.started" &&
                                   event.value("turn_id", std::string{}) ==
                                       completed.at("id").get<std::string>();
                          }),
            1);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) ==
                                       "turn.execution.completed" &&
                                   event.value("turn_id", std::string{}) ==
                                       completed.at("id").get<std::string>();
                          }),
            1);
  for (std::size_t i = 0; i < events.size(); ++i)
    EXPECT_EQ(events[i].at("sequence"), i + 1);
}
TEST(Sessions, PostgresStaleCompletionCannotReplaceContinuation) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_stale_completion_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};

  CoordinationOptions coordination_options;
  coordination_options.postgres_dsn = dsn;
  coordination_options.postgres_schema = schema;
  auto owner_a = create_coordination(coordination_options, "session-owner-a");
  auto owner_b = create_coordination(coordination_options, "session-owner-b");
  StorageOptions storage_options;
  storage_options.postgres_dsn = dsn;
  storage_options.postgres_schema = schema;
  storage_options.allow_multiple_processes = true;
  auto storage = create_storage(storage_options);

  AgentSession session;
  session.pipeline_id = "session-fixture";
  storage->commit({{RecordKind::AgentSession, session.id, session.id, Json(session)}});
  const auto turn_id = session.id + "-turn-stale";
  const Json turn{{"idempotency_key", "stale-completion"},
                  {"input", {{"tag", "continuation"}}},
                  {"state", "queued"},
                  {"pipeline_id", session.pipeline_id}};
  ASSERT_TRUE(storage->submit_session_turn(session.id, turn_id, turn, Json::object()));

  auto session_lease = owner_a->acquire("session:" + session.id, 5000);
  ASSERT_TRUE(session_lease);
  const auto claimed = storage->claim_next_session_turn(session.id, session_lease->owner_instance,
                                                        session_lease->fencing_token,
                                                        session_lease->expires_at, Json::object());
  ASSERT_TRUE(claimed);
  laso::Run running;
  running.id = "run-session-stale-completion";
  running.pipeline_id = session.pipeline_id;
  running.session_id = session.id;
  running.session_turn_id = turn_id;
  ASSERT_TRUE(storage->bind_session_turn_run(session.id, turn_id, Json(running), Json::object(),
                                             session_lease->owner_instance,
                                             session_lease->fencing_token, Json::object()));
  ASSERT_TRUE(owner_a->release(*session_lease));

  const auto run_resource = "run:" + running.id;
  auto old_lease = owner_a->acquire(run_resource, 5000);
  ASSERT_TRUE(old_lease);
  const auto candidate_id = running.id + "-continuation-fixture";
  const auto make_candidate = [&](const std::string &value, const std::string &scope) {
    return Record{RecordKind::SessionContinuation,
                  candidate_id,
                  running.id,
                  {{"scope", scope},
                   {"session_id", session.id},
                   {"run_id", running.id},
                   {"turn_id", turn_id},
                   {"provider_id", "fixture"},
                   {"provider_version", "1"},
                   {"state", value}}};
  };
  const auto old_candidate = make_candidate("continuation-old", "candidate");
  storage->commit_owned({old_candidate}, run_resource, old_lease->owner_instance,
                        old_lease->fencing_token);
  ASSERT_TRUE(owner_a->release(*old_lease));

  auto current_lease = owner_b->acquire(run_resource, 5000);
  ASSERT_TRUE(current_lease);
  EXPECT_GT(current_lease->fencing_token, old_lease->fencing_token);
  const auto current_candidate = make_candidate("continuation-current", "candidate");
  storage->commit_owned({current_candidate}, run_resource, current_lease->owner_instance,
                        current_lease->fencing_token);

  laso::Run completed = running;
  completed.state = RunState::Completed;
  completed.message.payload = {{"text", "current-result"}};
  completed.owner_instance_id = current_lease->owner_instance;
  completed.fencing_token = current_lease->fencing_token;
  Event run_completed;
  run_completed.run_id = completed.id;
  run_completed.type = "run.completed";
  auto promoted = make_candidate("continuation-current", "current");
  promoted.id = session.id + "-continuation-fixture";
  promoted.run_id = session.id;
  promoted.value["source_run_id"] = completed.id;
  auto discarded = make_candidate("", "discarded");
  storage->commit_session_run(
      {{RecordKind::Run, completed.id, completed.id, Json(completed)},
       {RecordKind::Event, run_completed.id, completed.id, Json(run_completed)},
       promoted,
       discarded},
      current_lease->owner_instance, current_lease->fencing_token, Json::object());

  EXPECT_THROW(storage->commit_owned({make_candidate("continuation-stale", "candidate")},
                                     run_resource, old_lease->owner_instance,
                                     old_lease->fencing_token),
               Error);
  laso::Run stale = completed;
  stale.message.payload = {{"text", "stale-result"}};
  stale.owner_instance_id = old_lease->owner_instance;
  stale.fencing_token = old_lease->fencing_token;
  Event stale_event;
  stale_event.run_id = stale.id;
  stale_event.type = "run.completed";
  auto stale_continuation = make_candidate("continuation-stale", "current");
  stale_continuation.id = session.id + "-continuation-fixture";
  stale_continuation.run_id = session.id;
  EXPECT_THROW(storage->commit_session_run(
                   {{RecordKind::Run, stale.id, stale.id, Json(stale)},
                    {RecordKind::Event, stale_event.id, stale.id, Json(stale_event)},
                    stale_continuation},
                   old_lease->owner_instance, old_lease->fencing_token, Json::object()),
               Error);

  const auto stored_turn = storage->get(RecordKind::SessionTurn, turn_id);
  EXPECT_EQ(stored_turn.at("state"), "succeeded");
  EXPECT_EQ(stored_turn.at("result").at("text"), "current-result");
  const auto continuations = storage->list(RecordKind::SessionContinuation, session.id);
  ASSERT_EQ(continuations.size(), 1U);
  EXPECT_EQ(continuations.front().at("state"), "continuation-current");
  const auto events = storage->session_events(session.id, 0, 100);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [](const Json &event) {
                            return event.value("type", std::string{}) == "turn.execution.completed";
                          }),
            1);
}

TEST(Storage, SessionCloseRacesInputAcceptanceTransactionally) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto storage = fixture.open(dir.path / "session-close-race.db");
    AgentSession session;
    storage->commit({{RecordKind::AgentSession, session.id, session.id, Json(session)}});

    std::barrier start(3);
    std::atomic<unsigned> accepted{0};
    std::atomic<unsigned> conflicts{0};
    std::atomic<unsigned> closed{0};
    std::atomic<bool> unexpected_error{false};
    std::jthread submitter([&] {
      start.arrive_and_wait();
      try {
        accepted = storage->submit_session_turn(session.id, "close-race-request",
                                                Json{{"idempotency_key", "close-race-request"},
                                                     {"input", {{"value", 1}}},
                                                     {"state", "queued"}},
                                                Json::object());
      } catch (const Error &error) {
        if (error.code == ErrorCode::Conflict)
          conflicts = 1;
        else
          unexpected_error = true;
      } catch (...) {
        unexpected_error = true;
      }
    });
    std::jthread closer([&] {
      start.arrive_and_wait();
      try {
        closed = storage->close_agent_session(session.id, Json::object());
      } catch (...) {
        unexpected_error = true;
      }
    });
    start.arrive_and_wait();
    submitter.join();
    closer.join();

    EXPECT_FALSE(unexpected_error);
    EXPECT_EQ(accepted + conflicts, 1U);
    EXPECT_EQ(closed, 1U);
    const auto events = storage->session_events(session.id, 0, 10);
    ASSERT_EQ(events.size(), accepted ? 4U : 2U);
    if (accepted) {
      EXPECT_EQ(events[0].at("type"), "input.accepted");
      EXPECT_EQ(events[0].at("sequence"), 1U);
      EXPECT_EQ(events[1].at("type"), "session.closing");
      EXPECT_EQ(events[1].at("sequence"), 2U);
      EXPECT_EQ(events[2].at("type"), "turn.execution.cancelled");
      EXPECT_EQ(events[2].at("sequence"), 3U);
      EXPECT_EQ(events[3].at("type"), "session.closed");
      EXPECT_EQ(events[3].at("sequence"), 4U);
      EXPECT_EQ(storage->list(RecordKind::SessionTurn, session.id).size(), 1U);
      EXPECT_EQ(storage->list(RecordKind::SessionTurn, session.id).front().at("state"),
                "cancelled");
    } else {
      EXPECT_EQ(events[0].at("type"), "session.closing");
      EXPECT_EQ(events[0].at("sequence"), 1U);
      EXPECT_EQ(events[1].at("type"), "session.closed");
      EXPECT_EQ(events[1].at("sequence"), 2U);
      EXPECT_TRUE(storage->list(RecordKind::SessionTurn, session.id).empty());
    }
  });
}
TEST(Storage, SessionCloseRacesTurnClaimTransactionally) {
  for_each_storage_fixture([](const auto &backend) {
    TemporaryDirectory dir;
    auto storage = backend.open(dir.path / "session-close-claim-race.db");
    AgentSession session;
    storage->commit({{RecordKind::AgentSession, session.id, session.id, Json(session)}});
    const auto turn_id = session.id + "-close-claim";
    const Json turn{
        {"idempotency_key", "close-claim"}, {"input", {{"value", "work"}}}, {"state", "queued"}};
    ASSERT_TRUE(storage->submit_session_turn(session.id, turn_id, turn, Json::object()));

    std::barrier start(3);
    std::optional<Json> claimed;
    std::atomic<bool> close_won{false};
    std::atomic<bool> unexpected_error{false};
    std::jthread claimer([&] {
      start.arrive_and_wait();
      try {
        claimed = storage->claim_next_session_turn(session.id, "close-claim-racer", 0, "",
                                                   Json::object());
      } catch (...) {
        unexpected_error = true;
      }
    });
    std::jthread closer([&] {
      start.arrive_and_wait();
      try {
        close_won = storage->close_agent_session(session.id, Json::object());
      } catch (...) {
        unexpected_error = true;
      }
    });
    start.arrive_and_wait();
    claimer.join();
    closer.join();

    EXPECT_FALSE(unexpected_error);
    EXPECT_TRUE(close_won);
    const auto stored_turn = storage->get(RecordKind::SessionTurn, turn_id);
    EXPECT_EQ(stored_turn.at("state"), "cancelled");
    EXPECT_EQ(storage->get(RecordKind::AgentSession, session.id).at("state"), "closed");
    const auto events = storage->session_events(session.id, 0, 10);
    ASSERT_FALSE(events.empty());
    for (std::size_t i = 0; i < events.size(); ++i)
      EXPECT_EQ(events[i].at("sequence"), i + 1);
    EXPECT_EQ(events.front().at("type"), "input.accepted");
    EXPECT_EQ(events.back().at("type"), "session.closed");
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [](const Json &event) {
                              return event.value("type", std::string{}) == "turn.execution.claimed";
                            }),
              claimed ? 1 : 0);
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [](const Json &event) {
                              return event.value("type", std::string{}) ==
                                     "turn.execution.cancelled";
                            }),
              1);
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [](const Json &event) {
                              return event.value("type", std::string{}) == "session.closed";
                            }),
              1);
    if (claimed) {
      EXPECT_EQ(events.size(), 5U);
      EXPECT_EQ(claimed->at("id"), turn_id);
      EXPECT_EQ(events[1].at("type"), "turn.execution.claimed");
      EXPECT_EQ(events[2].at("type"), "session.closing");
      EXPECT_EQ(events[3].at("type"), "turn.execution.cancelled");
      EXPECT_EQ(events[4].at("type"), "session.closed");
    } else {
      EXPECT_EQ(events.size(), 4U);
      EXPECT_EQ(events[1].at("type"), "session.closing");
      EXPECT_EQ(events[2].at("type"), "turn.execution.cancelled");
      EXPECT_EQ(events[3].at("type"), "session.closed");
    }
  });
}
TEST(Storage, IndependentSessionReadersSeeTheSameCommittedJournal) {
  TemporaryDirectory dir;
  auto writer = make_storage(dir.path / "shared-session.db");
  auto first_reader = make_storage(dir.path / "shared-session.db");
  auto second_reader = make_storage(dir.path / "shared-session.db");
  AgentSession session;
  writer->commit({{RecordKind::AgentSession, session.id, session.id, Json(session)}});
  const Json turn{{"idempotency_key", "input-1"}, {"input", {{"value", 1}}}};
  Event event;
  ASSERT_TRUE(writer->submit_session_turn(session.id, "turn-1", turn, Json(event)));
  const auto first = first_reader->session_events(session.id, 0, 10);
  const auto second = second_reader->session_events(session.id, 0, 10);
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(first, second);
  EXPECT_EQ(second.front().at("sequence"), 1U);
}
TEST(Storage, ConformanceClaimsDurableOccurrenceOnce) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    const Record first{RecordKind::ScheduleOccurrence,
                       "schedule|due",
                       "",
                       {{"status", "claimed"}, {"attempt", 1}}};
    EXPECT_TRUE(s->claim(first));
    EXPECT_FALSE(s->claim(
        {RecordKind::ScheduleOccurrence, first.id, "", {{"status", "claimed"}, {"attempt", 2}}}));
    EXPECT_EQ(s->get(RecordKind::ScheduleOccurrence, first.id).at("attempt"), 1);
    EXPECT_TRUE(
        s->claim({RecordKind::TriggerDelivery, "trigger|event", "", {{"status", "claimed"}}}));
  });
}
TEST(Storage, ConformanceSerializesConcurrentClaims) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    std::atomic<unsigned> winners = 0;
    std::vector<std::jthread> claimers;
    for (unsigned i = 0; i < 8; ++i) {
      claimers.emplace_back([&, i] {
        if (s->claim({RecordKind::ScheduleOccurrence, "same-occurrence", "", {{"claimer", i}}}))
          ++winners;
      });
    }
    claimers.clear();
    EXPECT_EQ(winners, 1U);
  });
}
TEST(Storage, ConformanceAtomicallyClaimsExternalEventAndDeduplicates) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    const Record claim{
        RecordKind::ExternalEventClaim,
        "external-event:source:event-1",
        "",
        {{"source_id", "source"}, {"external_event_id", "event-1"}, {"event_id", "event-record"}}};
    const Record event{RecordKind::Event, "event-record", "", {{"type", "example.created"}}};
    EXPECT_TRUE(s->claim(claim, {event}));
    EXPECT_FALSE(s->claim(claim, {{RecordKind::Event, "other-event", "", Json::object()}}));
    EXPECT_EQ(s->list(RecordKind::Event).size(), 1U);
    EXPECT_EQ(s->get(RecordKind::ExternalEventClaim, claim.id).at("event_id"), "event-record");
  });
}
TEST(Storage, ConformanceSerializesConcurrentExternalEventClaims) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    std::atomic<unsigned> winners = 0;
    std::vector<std::jthread> claimers;
    for (unsigned i = 0; i < 8; ++i) {
      claimers.emplace_back([&, i] {
        const auto event_id = "external-event-" + std::to_string(i);
        if (s->claim({RecordKind::ExternalEventClaim,
                      "external-event:source:same",
                      "",
                      {{"event_id", event_id}}},
                     {{RecordKind::Event, event_id, "", {{"type", "example.created"}}}}))
          ++winners;
      });
    }
    claimers.clear();
    EXPECT_EQ(winners, 1U);
    EXPECT_EQ(s->list(RecordKind::Event).size(), 1U);
  });
}

TEST(Storage, ConformancePersistsWorkerJobLifecycleAndRejectsInvalidUpdates) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    WorkerJob job;
    job.id = "worker-job-1";
    job.worker_id = "offline";
    job.run_id = "run-1";
    job.node_id = "work";
    job.idempotency_key = "run-1:work:1";
    s->commit({{RecordKind::WorkerJob, job.id, job.run_id, Json(job)}});

    job.state = WorkerJobState::Submitting;
    s->commit({{RecordKind::WorkerJob, job.id, job.run_id, Json(job)}});
    job.state = WorkerJobState::Queued;
    s->commit({{RecordKind::WorkerJob, job.id, job.run_id, Json(job)}});
    job.state = WorkerJobState::Running;
    job.external_job_id = "external-1";
    s->commit({{RecordKind::WorkerJob, job.id, job.run_id, Json(job)}});
    job.state = WorkerJobState::Completed;
    job.result = Json{{"ok", true}};
    job.completed_at = timestamp();
    s->commit({{RecordKind::WorkerJob, job.id, job.run_id, Json(job)}});
    EXPECT_EQ(s->get(RecordKind::WorkerJob, job.id).template get<WorkerJob>().result.at("ok"),
              true);

    auto late = job;
    late.state = WorkerJobState::Failed;
    EXPECT_THROW(s->commit({{RecordKind::WorkerJob, late.id, late.run_id, Json(late)}}), Error);

    WorkerJob invalid;
    invalid.id = "worker-job-invalid";
    invalid.worker_id = "offline";
    invalid.run_id = "run-1";
    invalid.node_id = "work";
    invalid.idempotency_key = "run-1:work:2";
    invalid.state = WorkerJobState::Created;
    s->commit({{RecordKind::WorkerJob, invalid.id, invalid.run_id, Json(invalid)}});
    invalid.state = WorkerJobState::Completed;
    EXPECT_THROW(s->commit({{RecordKind::WorkerJob, invalid.id, invalid.run_id, Json(invalid)}}),
                 Error);

    WorkerJob retry = invalid;
    retry.id = "worker-job-retry";
    retry.state = WorkerJobState::Created;
    retry.attempt = 2;
    retry.idempotency_key = "run-1:work:2";
    s->commit({{RecordKind::WorkerJob, retry.id, retry.run_id, Json(retry)}});
    EXPECT_EQ(s->list(RecordKind::WorkerJob, "run-1").size(), 3U);
  });
}

TEST(Storage, ConformanceClaimsWorkerJobIdentityOnce) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    WorkerJob job;
    job.id = "worker-claim";
    job.worker_id = "offline";
    job.run_id = "run-claim";
    job.node_id = "work";
    job.idempotency_key = "run-claim:work:1";
    EXPECT_TRUE(s->claim({RecordKind::WorkerJob, job.id, job.run_id, Json(job)}));
    EXPECT_FALSE(s->claim({RecordKind::WorkerJob, job.id, job.run_id, Json(job)}));
    EXPECT_EQ(s->list(RecordKind::WorkerJob, job.run_id).size(), 1U);
  });
}

TEST(Workspace, ManifestStagesOnlyBoundedRelativeFilesWithIntegrity) {
  TemporaryDirectory directory;
  std::filesystem::create_directories(directory.path / "src");
  std::ofstream(directory.path / "src" / "main.cpp") << "int main() { return 0; }\n";
  const auto manifest = workspace_manifest(directory.path);
  ASSERT_EQ(manifest.at("files").size(), 1U);
  EXPECT_EQ(manifest.at("files").front().at("path"), "src/main.cpp");
  const auto staged =
      stage_workspace(manifest, directory.path / "staging", "run", "work", "attempt");
  EXPECT_EQ(read_document(staged / "src" / "main.cpp"), "int main() { return 0; }\n");

  auto traversal = manifest;
  traversal["files"][0]["path"] = "../outside";
  EXPECT_THROW(validate_workspace_manifest(traversal), Error);
  auto corrupted = manifest;
  corrupted["files"][0]["data"][0] = static_cast<unsigned char>('X');
  EXPECT_THROW(validate_workspace_manifest(corrupted), Error);
}

TEST(Storage, ConformancePersistsAndFencesNodeWork) {
  for_each_storage_fixture([](const auto &fixture) {
    TemporaryDirectory dir;
    auto s = fixture.open(dir.path / "state.db");
    NodeWork work;
    work.id = "node-work-1";
    work.run_id = "run-1";
    work.group_id = "group-1";
    work.node_id = "branch";
    work.join = "join";
    work.required_worker_id = "codex";
    work.required_capability = "coding";
    work.token = {"branch", Message{}, {{"group-1", "join", 1, 0}}};
    s->commit({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}});
    const auto queued = s->get(RecordKind::NodeWork, work.id).template get<NodeWork>();
    EXPECT_EQ(queued.required_worker_id, "codex");
    EXPECT_EQ(queued.required_capability, "coding");
    work.state = NodeWorkState::Running;
    work.attempt = 1;
    s->commit({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}});
    work.state = NodeWorkState::Completed;
    work.result = Message{};
    s->commit({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}});
    EXPECT_EQ(s->get(RecordKind::NodeWork, work.id).template get<NodeWork>().state,
              NodeWorkState::Completed);
    auto duplicate = work;
    duplicate.updated_at = timestamp();
    EXPECT_NO_THROW(
        s->commit({{RecordKind::NodeWork, duplicate.id, duplicate.run_id, Json(duplicate)}}));
    auto conflicting = duplicate;
    conflicting.result = Message{};
    conflicting.result->payload = Json{{"different", true}};
    EXPECT_THROW(
        s->commit({{RecordKind::NodeWork, conflicting.id, conflicting.run_id, Json(conflicting)}}),
        Error);
    auto stale = work;
    stale.state = NodeWorkState::Running;
    EXPECT_THROW(s->commit({{RecordKind::NodeWork, stale.id, stale.run_id, Json(stale)}}), Error);
  });
}

TEST(Storage, PostgresRejectsSecondOwner) {
  if (!std::getenv("LASO_TEST_POSTGRES_DSN"))
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  for (const auto &fixture : storage_fixtures()) {
    struct FixtureCleanup {
      const StorageFixture &fixture;
      ~FixtureCleanup() {
        if (fixture.cleanup)
          fixture.cleanup();
      }
    } fixture_cleanup{fixture};
    TemporaryDirectory dir;
    auto first = fixture.open(dir.path / "state.db");
    try {
      auto second = fixture.open(dir.path / "state.db");
      (void)second;
      ADD_FAILURE() << "a second PostgreSQL storage owner was accepted";
    } catch (const Error &error) {
      EXPECT_EQ(error.code, ErrorCode::Conflict);
      EXPECT_STREQ(error.what(), "PostgreSQL database is owned by another LASO process");
    }
  }
}
TEST(Storage, PostgresOwnerCanBeReacquiredImmediatelyAfterStorageDestruction) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  TemporaryDirectory dir;
  StorageOptions options;
  options.postgres_dsn = dsn;
  options.postgres_schema = schema_for(dir.path / "owner-lifecycle");

  for (int attempt = 0; attempt < 25; ++attempt) {
    {
      auto owner = create_storage(options);
      try {
        auto competing = create_storage(options);
        (void)competing;
        ADD_FAILURE() << "a second PostgreSQL storage owner was accepted";
      } catch (const Error &error) {
        EXPECT_EQ(error.code, ErrorCode::Conflict);
      }
    }
    std::unique_ptr<Storage> replacement;
    EXPECT_NO_THROW(replacement = create_storage(options));
    ASSERT_TRUE(replacement);
  }
}
TEST(Storage, PostgresUpgradesSchemaSevenToCurrent) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_upgrade_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  const std::vector<std::string> prior_tables = {"pipelines",
                                                 "runs",
                                                 "attempts",
                                                 "messages",
                                                 "approvals",
                                                 "artifacts",
                                                 "events",
                                                 "schedules",
                                                 "triggers",
                                                 "schedule_occurrences",
                                                 "trigger_deliveries",
                                                 "event_sources",
                                                 "external_event_claims",
                                                 "worker_jobs",
                                                 "worker_interactions"};
  try {
    pqxx::connection connection(dsn);
    pqxx::work transaction(connection);
    transaction.exec("CREATE SCHEMA \"" + schema + "\"");
    transaction.exec("SET search_path TO \"" + schema + "\", public");
    transaction.exec("CREATE TABLE laso_schema_migrations (version INTEGER PRIMARY KEY, "
                     "applied_at TIMESTAMPTZ NOT NULL DEFAULT now())");
    for (int version = 1; version <= 7; ++version)
      transaction.exec("INSERT INTO laso_schema_migrations(version) VALUES (" +
                       std::to_string(version) + ")");
    for (const auto &name : prior_tables)
      transaction.exec(
          "CREATE TABLE \"" + name +
          "\" (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, "
          "body TEXT NOT NULL, sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
    transaction.exec("CREATE TABLE laso_coordination_leases (resource_key TEXT PRIMARY KEY, "
                     "owner_instance TEXT NOT NULL, fencing_token BIGINT NOT NULL, "
                     "acquired_at TIMESTAMPTZ NOT NULL, heartbeat_at TIMESTAMPTZ NOT NULL, "
                     "expires_at TIMESTAMPTZ NOT NULL)");
    transaction.exec(
        "CREATE TABLE laso_instances (instance_id TEXT PRIMARY KEY, "
        "started_at TIMESTAMPTZ NOT NULL, last_heartbeat_at TIMESTAMPTZ NOT NULL, "
        "software_version TEXT NOT NULL, capabilities TEXT NOT NULL, state TEXT NOT NULL)");
    transaction.commit();

    StorageOptions options;
    options.postgres_dsn = dsn;
    options.postgres_schema = schema;
    options.allow_multiple_processes = true;
    {
      auto storage = create_storage(options);
      EXPECT_TRUE(storage);
    }

    pqxx::connection verify_connection(dsn);
    pqxx::read_transaction verify(verify_connection);
    verify.exec("SET search_path TO \"" + schema + "\", public");
    EXPECT_EQ(verify.exec1("SELECT MAX(version) FROM laso_schema_migrations")[0].as<int>(), 11);
    EXPECT_STREQ(verify.exec1("SELECT to_regclass('node_work')")[0].c_str(), "node_work");
    EXPECT_STREQ(verify.exec1("SELECT to_regclass('agent_sessions')")[0].c_str(), "agent_sessions");
    EXPECT_STREQ(verify.exec1("SELECT to_regclass('session_context_generations')")[0].c_str(),
                 "session_context_generations");
    EXPECT_STREQ(verify.exec1("SELECT to_regclass('run_context_snapshots')")[0].c_str(),
                 "run_context_snapshots");
  } catch (...) {
    pqxx::connection cleanup_connection(dsn);
    pqxx::work cleanup(cleanup_connection);
    cleanup.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
    cleanup.commit();
    throw;
  }
  pqxx::connection cleanup_connection(dsn);
  pqxx::work cleanup(cleanup_connection);
  cleanup.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
  cleanup.commit();
}
TEST(Storage, PostgresRunsAndRecoversNormalRuntime) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  const auto dsn_copy = std::string(dsn);
  auto schema = "laso_runtime_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  TemporaryDirectory dir;
  Config c = config(dir.path);
  c.postgres_dsn = dsn_copy;
  c.postgres_schema = schema;
  c.validate();
  std::string run_id;
  try {
    {
      asio::io_context io;
      Service service(io, c);
      service.register_pipeline(fixture("hello-pipeline"));
      const auto parent = service.register_pipeline(fixture("subpipeline"));
      run_id = service.start(parent.at("id").get<std::string>(), Json{{"value", 42}});
      io.run();
      const auto run = service.get(RecordKind::Run, run_id).get<laso::Run>();
      EXPECT_EQ(run.state, RunState::Completed);
      EXPECT_EQ(run.child_runs.size(), 1U);
      EXPECT_EQ(service.get(RecordKind::Run, run.child_runs.front()).at("pipeline_version"), 1U);
    }
    {
      asio::io_context io;
      Service reopened(io, c);
      const auto run = reopened.get(RecordKind::Run, run_id).get<laso::Run>();
      EXPECT_EQ(run.state, RunState::Completed);
      EXPECT_EQ(reopened.run_view(run_id).at("children").size(), 1U);
    }
  } catch (...) {
    pqxx::connection connection(dsn_copy);
    pqxx::work transaction(connection);
    transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
    transaction.commit();
    throw;
  }
  pqxx::connection connection(dsn_copy);
  pqxx::work transaction(connection);
  transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
  transaction.commit();
}
TEST(Artifacts, IgnoresUntrustedNamesForPath) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  LocalArtifactStore artifacts(dir.path / "artifacts", *storage);
  Artifact a;
  a.name = "../../outside";
  a.run_id = "../../outside";
  std::string data = "example";
  auto saved = artifacts.put(a, std::as_bytes(std::span(data.data(), data.size())));
  EXPECT_TRUE(saved.location.empty());
  ASSERT_TRUE(saved.object_id.starts_with("sha256:"));
  EXPECT_TRUE(artifacts.exists(saved.object_id));
  EXPECT_NO_THROW(artifacts.verify(saved.object_id, saved.sha256, saved.size));
  EXPECT_EQ(storage->get(RecordKind::Artifact, saved.id).at("name"), "../../outside");
}
TEST(Artifacts, ContentAddressedObjectsStreamAndMaterialize) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  ArtifactStoreLimits limits;
  limits.max_object_bytes = 2 * 1024 * 1024;
  limits.max_temp_bytes = 4 * 1024 * 1024;
  LocalArtifactStore artifacts(dir.path / "artifacts", *storage, limits);
  const auto source = dir.path / "source.bin";
  {
    std::ofstream stream(source, std::ios::binary);
    for (unsigned i = 0; i < 128; ++i) {
      const std::string block(8192, static_cast<char>(i));
      stream.write(block.data(), block.size());
    }
  }
  Artifact metadata;
  metadata.run_id = "run";
  metadata.node_id = "node";
  metadata.name = "result.bin";
  const auto saved = artifacts.put_file(metadata, source);
  EXPECT_EQ(saved.size, std::filesystem::file_size(source));
  EXPECT_TRUE(artifacts.exists(saved.object_id));
  EXPECT_NO_THROW(artifacts.verify(saved.object_id, saved.sha256, saved.size));
  const auto destination = dir.path / "materialized" / "result.bin";
  EXPECT_NO_THROW(artifacts.materialize(saved.object_id, destination, saved.sha256, saved.size));
  EXPECT_EQ(std::filesystem::file_size(destination), std::filesystem::file_size(source));
  EXPECT_EQ(read_document(destination), read_document(source));
  const auto duplicate = artifacts.put_file(metadata, source);
  EXPECT_EQ(duplicate.object_id, saved.object_id);
  const auto report = artifacts.integrity();
  EXPECT_EQ(report.invalid, 0U);
  EXPECT_GE(report.verified, 1U);
}
TEST(Artifacts, GarbageCollectionHandlesFreshStore) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  LocalArtifactStore artifacts(dir.path / "artifacts", *storage);
  const auto report = artifacts.collect_garbage(true);
  EXPECT_EQ(report.at("dry_run"), true);
  EXPECT_EQ(report.at("live_objects"), 0U);
  EXPECT_EQ(report.at("removed"), 0U);
}
TEST(Artifacts, ObjectBackedWorkspaceStagesWithIntegrity) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  LocalArtifactStore artifacts(dir.path / "artifacts", *storage);
  std::filesystem::create_directories(dir.path / "workspace" / "src");
  std::ofstream(dir.path / "workspace" / "src" / "main.cpp") << "int main() { return 0; }\n";
  const auto manifest =
      workspace_manifest(dir.path / "workspace", artifacts, WorkspaceManifestLimits{}, "run-1",
                         "work-1", "attempt-1", "worker-1", 7);
  ASSERT_EQ(manifest.at("version"), 2);
  EXPECT_EQ(manifest.at("provenance").at("run_id"), "run-1");
  EXPECT_EQ(manifest.at("provenance").at("node_work_id"), "work-1");
  EXPECT_EQ(manifest.at("provenance").at("attempt_id"), "attempt-1");
  EXPECT_EQ(manifest.at("provenance").at("worker_id"), "worker-1");
  EXPECT_EQ(manifest.at("provenance").at("fencing_token"), 7);
  const auto staged = stage_workspace(manifest, dir.path / "staging", "run", "work", "attempt",
                                      WorkspaceManifestLimits{}, &artifacts);
  EXPECT_EQ(read_document(staged / "src" / "main.cpp"), "int main() { return 0; }\n");
  auto corrupted = manifest;
  corrupted["files"][0]["sha256"] = std::string(64, '0');
  EXPECT_THROW(stage_workspace(corrupted, dir.path / "staging", "run", "work", "bad",
                               WorkspaceManifestLimits{}, &artifacts),
               Error);
}
TEST(Artifacts, InlineWorkspaceManifestUsesStandardSha256) {
  TemporaryDirectory dir;
  std::ofstream(dir.path / "input.txt") << "abc";
  const auto manifest = workspace_manifest(dir.path);
  ASSERT_EQ(manifest.at("files").size(), 1U);
  EXPECT_EQ(manifest.at("files")[0].at("sha256"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_EQ(manifest.at("files")[0].at("size"), 3U);
  EXPECT_EQ(sha256_file(dir.path / "input.txt").first,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  const std::array<unsigned char, 3> bytes{'a', 'b', 'c'};
  EXPECT_EQ(sha256_bytes(bytes), sha256_file(dir.path / "input.txt").first);
}
TEST(Artifacts, AuthenticatedRemoteGatewayStreamsAndMaterializesLargeObject) {
  TemporaryDirectory dir;
  auto owner_storage = make_storage(dir.path / "owner.db");
  auto worker_storage = make_storage(dir.path / "worker.db");
  LocalArtifactStore owner_store(dir.path / "owner-artifacts", *owner_storage,
                                 {64U * 1024U * 1024U, 128U * 1024U * 1024U, 3600});
  asio::io_context gateway_io;
  ArtifactHttpServer gateway(gateway_io, owner_store, "127.0.0.1", 0, "synthetic-artifact-token",
                             64U * 1024U * 1024U);
  gateway.start();
  RemoteArtifactStore remote("http://127.0.0.1:" + std::to_string(gateway.port()),
                             "synthetic-artifact-token", dir.path / "worker-cache", *worker_storage,
                             {64U * 1024U * 1024U, 128U * 1024U * 1024U, 3600});
  const auto source = dir.path / "large.bin";
  {
    std::ofstream output(source, std::ios::binary);
    for (std::size_t index = 0; index < 16U * 1024U * 1024U; ++index)
      output.put(static_cast<char>((index * 31U + 7U) & 0xffU));
  }
  Artifact metadata;
  metadata.run_id = "run-remote";
  metadata.node_id = "node-remote";
  metadata.name = "large.bin";
  metadata.media_type = "application/octet-stream";
  metadata.metadata = Json{{"purpose", "remote-test"}};
  const auto saved = remote.put_file(metadata, source);
  EXPECT_TRUE(owner_store.exists(saved.object_id));
  EXPECT_NO_THROW(remote.verify(saved.object_id, saved.sha256, saved.size));
  const auto materialized = dir.path / "materialized.bin";
  EXPECT_NO_THROW(remote.materialize(saved.object_id, materialized, saved.sha256, saved.size));
  EXPECT_EQ(sha256_file(materialized), sha256_file(source));

  RemoteArtifactStore unauthorized("http://127.0.0.1:" + std::to_string(gateway.port()),
                                   "wrong-token", dir.path / "unauthorized-cache", *worker_storage,
                                   {64U * 1024U * 1024U, 128U * 1024U * 1024U, 3600});
  EXPECT_THROW(unauthorized.verify(saved.object_id), Error);
  gateway.stop();
}
TEST(Plugins, DiscoversLoadsInvokesAndUnloadsExample) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.plugin_dirs = {LASO_PLUGIN_DIR};
  Service s(io, c);
  ASSERT_EQ(s.plugins().size(), 3U);
  EXPECT_TRUE(s.plugins().at(0).at("loaded").get<bool>());
  auto r = execute(s, io, fixture("native-plugin"), {{"echo", 123}});
  EXPECT_EQ(r.state, RunState::Completed);
  EXPECT_EQ(r.message.payload.at("echo"), 123);
}
TEST(Plugins, ModelProviderIsDiscoverableAndServicesAgentPipeline) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.plugin_dirs = {LASO_PLUGIN_DIR};
  c.models["plugin-model"] = {"example-model", "offline-example"};
  Service s(io, c);
  const auto providers = s.providers();
  const auto found = std::find_if(providers.begin(), providers.end(), [](const Json &provider) {
    return provider.at("name") == "example-model" &&
           provider.at("plugin") == "example-model-provider" && provider.at("healthy") == true;
  });
  ASSERT_NE(found, providers.end());
  auto r = execute(s, io, R"(laso: "1"
name: plugin-model
version: 1
nodes:
  generate:
    type: agent
    model: plugin-model
    prompt: Produce a deterministic offline response.
edges:
  - {from: input, to: generate}
  - {from: generate, to: output}
)",
                   Json{{"request", "example"}});
  EXPECT_EQ(r.state, RunState::Completed);
  EXPECT_EQ(r.message.payload.at("text"), "Offline plugin model response");
  EXPECT_TRUE(r.message.payload.at("reviewed"));
  const auto session_pipeline = s.register_pipeline(R"(laso: "1"
name: plugin-session-context
version: 1
nodes:
  generate:
    type: agent
    model: plugin-model
    prompt: Continue the session.
edges:
  - {from: input, to: generate}
  - {from: generate, to: output}
)")
                                    .at("id")
                                    .get<std::string>();
  const auto session = s.create_session(session_pipeline);
  (void)s.create_session_context_generation(session.id, 0, 0, "plugin-context-1",
                                            "structured-context", "1", Json{{"key", "value"}});
  io.restart();
  const auto turn = s.submit_session_turn(session.id, "plugin-session-turn", Json{{"input", "go"}});
  io.run();
  const auto completed = s.get(RecordKind::SessionTurn, turn.at("id").get<std::string>());
  ASSERT_EQ(completed.at("state"), "succeeded");
  const auto session_run = s.get(RecordKind::Run, completed.at("run_id").get<std::string>());
  EXPECT_TRUE(session_run.at("message").at("payload").at("session_context_received"));
  const auto provenance = std::find_if(
      r.message.provenance.begin(), r.message.provenance.end(), [](const ProvenanceRecord &item) {
        return item.provider == "example-model" && item.model == "offline-example";
      });
  EXPECT_NE(provenance, r.message.provenance.end());
}
TEST(Plugins, RejectsIncompatibleABI) {
  ToolRegistry r;
  ProviderRegistry providers;
  PluginLoader loader(r, providers);
  loader.discover({LASO_BAD_PLUGIN_DIR});
  ASSERT_EQ(loader.plugins().size(), 1U);
  EXPECT_FALSE(loader.plugins().front().loaded);
  EXPECT_EQ(loader.plugins().front().abi, 999U);
  EXPECT_TRUE(r.names().empty());
}
TEST(Plugins, RejectsNonLibraryFile) {
  TemporaryDirectory dir;
  std::ofstream(dir.path / "bad.so") << "not a library";
  ToolRegistry r;
  ProviderRegistry providers;
  PluginLoader loader(r, providers);
  loader.discover({dir.path});
  ASSERT_EQ(loader.plugins().size(), 1U);
  EXPECT_FALSE(loader.plugins().front().loaded);
}
TEST(Plugins, NoImplicitDirectories) {
  ToolRegistry r;
  ProviderRegistry providers;
  PluginLoader loader(r, providers);
  loader.discover({});
  EXPECT_TRUE(loader.plugins().empty());
}
TEST(Plugins, MissingConfiguredDirectoryFailsClearly) {
  ToolRegistry tools;
  ProviderRegistry providers;
  PluginLoader loader(tools, providers);
  try {
    loader.discover({std::filesystem::temp_directory_path() / "laso-no-such-plugin-dir"});
    FAIL() << "missing plugin directory should fail configuration";
  } catch (const Error &error) {
    EXPECT_EQ(error.code, ErrorCode::Configuration);
    EXPECT_STREQ(error.what(), "Configured plugin directory is unavailable");
  }
}
TEST(Plugins, SymlinksNotLoaded) {
  TemporaryDirectory dir;
  std::filesystem::create_symlink(
      std::filesystem::path(LASO_PLUGIN_DIR) / "liblaso_example_tool.so", dir.path / "redirect.so");
  ToolRegistry r;
  ProviderRegistry providers;
  PluginLoader loader(r, providers);
  loader.discover({dir.path});
  EXPECT_TRUE(loader.plugins().empty());
}
TEST(Api, HealthAndVersion) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  LocalDevelopmentIdentity identity;
  Api api(s, identity);
  EXPECT_EQ(api.handle("GET", "/api/v1/health", "").status, 200U);
  const auto version = api.handle("GET", "/api/v1/version", "");
  EXPECT_EQ(version.body.at("version"), "0.1.0-rc.1");
  EXPECT_EQ(version.body.at("capabilities"),
            (Json{"sessions.durable", "sessions.ordered_turns", "sessions.sequential_execution",
                  "sessions.event_replay", "sessions.sse", "sessions.context_generations",
                  "sessions.run_context_snapshots"}));
}
TEST(Api, RegistersAndCreatesRun) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  LocalDevelopmentIdentity identity;
  Api api(s, identity);
  EXPECT_EQ(
      api.handle("POST", "/api/v1/pipelines", Json{{"yaml", fixture("hello-pipeline")}}.dump())
          .status,
      201U);
  auto response = api.handle("POST", "/api/v1/pipelines/hello/runs", "{}");
  ASSERT_EQ(response.status, 202U);
  io.run();
  auto id = response.body.at("id").get<std::string>();
  EXPECT_EQ(api.handle("GET", "/api/v1/runs/" + id, "").body.at("state"), "Completed");
}
TEST(Api, PersistentSessionTurnsCanBeRetriedAndReplayed) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  ASSERT_EQ(api.handle("POST", "/api/v1/pipelines", Json{{"yaml", single()}}.dump()).status, 201U);
  auto created = api.handle("POST", "/api/v1/sessions", R"({"pipeline_id":"test@1"})");
  ASSERT_EQ(created.status, 201U);
  const auto id = created.body.at("id").get<std::string>();
  const auto listed = api.handle("GET", "/api/v1/sessions?limit=50&offset=0", "");
  ASSERT_EQ(listed.status, 200U);
  ASSERT_EQ(listed.body.size(), 1U);
  EXPECT_EQ(listed.body.front().at("id"), id);
  EXPECT_FALSE(listed.body.front().contains("next_sequence"));
  const auto inspected = api.handle("GET", "/api/v1/sessions/" + id, "").body;
  EXPECT_EQ(inspected.at("state"), "open");
  EXPECT_FALSE(inspected.contains("backend_state"));
  const auto first = Json{{"idempotency_key", "request-a"}, {"input", {{"text", "first"}}}};
  const auto first_response = api.handle("POST", "/api/v1/sessions/" + id + "/turns", first.dump());
  ASSERT_EQ(first_response.status, 202U);
  const auto retry_response = api.handle("POST", "/api/v1/sessions/" + id + "/turns", first.dump());
  ASSERT_EQ(retry_response.status, 202U);
  EXPECT_EQ(retry_response.body.at("id"), first_response.body.at("id"));
  EXPECT_EQ(retry_response.body.at("idempotency_key"), first_response.body.at("idempotency_key"));
  EXPECT_EQ(retry_response.body.at("sequence"), first_response.body.at("sequence"));
  auto conflicting = first;
  conflicting["input"]["text"] = "different input";
  EXPECT_EQ(api.handle("POST", "/api/v1/sessions/" + id + "/turns", conflicting.dump()).status,
            409U);
  const auto second = Json{{"idempotency_key", "request-b"}, {"input", {{"text", "second"}}}};
  const auto second_response =
      api.handle("POST", "/api/v1/sessions/" + id + "/turns", second.dump());
  ASSERT_EQ(second_response.status, 202U);
  const auto turns = api.handle("GET", "/api/v1/sessions/" + id + "/turns", "").body;
  ASSERT_EQ(turns.size(), 2U);
  for (const auto &turn : turns) {
    EXPECT_FALSE(turn.contains("dispatch_owner"));
    EXPECT_FALSE(turn.contains("dispatch_fencing_token"));
    EXPECT_FALSE(turn.contains("dispatch_expires_at"));
    EXPECT_FALSE(turn.contains("dispatch_attempt"));
  }
  EXPECT_EQ(turns[0].at("id"), first_response.body.at("id"));
  EXPECT_EQ(turns[0].at("sequence"), 1U);
  EXPECT_EQ(turns[1].at("id"), second_response.body.at("id"));
  EXPECT_EQ(turns[1].at("sequence"), 2U);
  const auto journal = api.handle("GET", "/api/v1/sessions/" + id + "/events?after=0", "").body;
  ASSERT_EQ(journal.size(), 4U);
  std::vector<std::string> accepted_turn_ids;
  for (std::size_t i = 0; i < journal.size(); ++i) {
    EXPECT_EQ(journal[i].at("sequence"), i + 1);
    if (journal[i].at("type") == "input.accepted")
      accepted_turn_ids.push_back(journal[i].at("turn_id").get<std::string>());
  }
  EXPECT_EQ(accepted_turn_ids,
            (std::vector<std::string>{first_response.body.at("id").get<std::string>(),
                                      second_response.body.at("id").get<std::string>()}));
  const auto events = api.handle("GET", "/api/v1/sessions/" + id + "/events?after=1", "");
  ASSERT_EQ(events.status, 200U);
  ASSERT_EQ(events.body.size(), 3U);
  EXPECT_EQ(events.body[0].at("sequence"), 2U);
  EXPECT_EQ(events.body[0].at("type"), "turn.execution.claimed");
  EXPECT_EQ(events.body.back().at("type"), "input.accepted");
  EXPECT_EQ(events.body.back().at("turn_id"), second_response.body.at("id"));
  EXPECT_EQ(events.body.back().at("payload").at("input").at("text"), "second");
  const auto after_two = api.handle("GET", "/api/v1/sessions/" + id + "/events?after=2", "");
  ASSERT_EQ(after_two.body.size(), 2U);
  EXPECT_EQ(after_two.body[0].at("type"), "turn.execution.started");
  EXPECT_EQ(after_two.body[1].at("type"), "input.accepted");
  EXPECT_EQ(
      api.handle("GET", "/api/v1/sessions/" + id + "/events?after=18446744073709551615", "").status,
      400U);
  const auto run_cancelled_by_close = service.agent_session(id).active_run_id;
  ASSERT_FALSE(run_cancelled_by_close.empty());
  ASSERT_EQ(api.handle("POST", "/api/v1/sessions/" + id + "/close", "{}").status, 202U);
  io.run();
  const auto cancelled_run = api.handle("GET", "/api/v1/runs/" + run_cancelled_by_close, "");
  ASSERT_EQ(cancelled_run.status, 200U);
  EXPECT_EQ(cancelled_run.body.at("state"), "Cancelled");
  const auto closed = api.handle("GET", "/api/v1/sessions/" + id + "/events?after=2", "");
  ASSERT_EQ(closed.body.size(), 7U);
  EXPECT_EQ(closed.body.back().at("type"), "session.closed");
  bool saw_close_request = false;
  bool saw_queued_cancellation = false;
  for (const auto &event : closed.body) {
    saw_close_request |= event.at("type") == "turn.execution.cancel_requested";
    saw_queued_cancellation |= event.at("type") == "turn.execution.cancelled";
  }
  EXPECT_TRUE(saw_close_request);
  EXPECT_TRUE(saw_queued_cancellation);
  EXPECT_EQ(api.handle("POST", "/api/v1/sessions/" + id + "/turns", first.dump()).status, 202U);
  const auto third = Json{{"idempotency_key", "request-c"}, {"input", {{"text", "third"}}}};
  EXPECT_EQ(api.handle("POST", "/api/v1/sessions/" + id + "/turns", third.dump()).status, 409U);
}
TEST(Api, PersistentSessionsSurviveServiceRestart) {
  TemporaryDirectory dir;
  std::string open_session_id;
  std::string closed_session_id;
  {
    asio::io_context io;
    Service service(io, config(dir.path));
    LocalDevelopmentIdentity identity;
    Api api(service, identity);
    ASSERT_EQ(api.handle("POST", "/api/v1/pipelines", Json{{"yaml", single()}}.dump()).status,
              201U);
    const auto open_session = api.handle("POST", "/api/v1/sessions", R"({"pipeline_id":"test@1"})");
    ASSERT_EQ(open_session.status, 201U);
    open_session_id = open_session.body.at("id").get<std::string>();
    const auto closed_session =
        api.handle("POST", "/api/v1/sessions", R"({"pipeline_id":"test@1"})");
    ASSERT_EQ(closed_session.status, 201U);
    closed_session_id = closed_session.body.at("id").get<std::string>();
    ASSERT_EQ(api.handle("POST", "/api/v1/sessions/" + open_session_id + "/turns",
                         R"({"idempotency_key":"open-1","input":{"text":"one"}})")
                  .status,
              202U);
    ASSERT_EQ(api.handle("POST", "/api/v1/sessions/" + closed_session_id + "/turns",
                         R"({"idempotency_key":"closed-1","input":{"text":"one"}})")
                  .status,
              202U);
    const auto session_before_close = service.agent_session(closed_session_id);
    EXPECT_FALSE(session_before_close.active_run_id.empty());
    ASSERT_EQ(api.handle("POST", "/api/v1/sessions/" + closed_session_id + "/close", "{}").status,
              202U);
    EXPECT_EQ(service.agent_session(closed_session_id).state, "closed");
  }
  {
    asio::io_context io;
    Service restarted(io, config(dir.path));
    LocalDevelopmentIdentity identity;
    Api api(restarted, identity);
    const auto open = api.handle("GET", "/api/v1/sessions/" + open_session_id, "");
    ASSERT_EQ(open.status, 200U);
    EXPECT_EQ(open.body.at("state"), "open");
    const auto listed = api.handle("GET", "/api/v1/sessions?limit=50&offset=0", "");
    ASSERT_EQ(listed.status, 200U);
    bool found_open = false;
    for (const auto &session : listed.body)
      found_open |= session.at("id") == open_session_id;
    EXPECT_TRUE(found_open);
    const auto turns = api.handle("GET", "/api/v1/sessions/" + open_session_id + "/turns", "");
    ASSERT_EQ(turns.status, 200U);
    ASSERT_EQ(turns.body.size(), 1U);
    EXPECT_EQ(turns.body.front().at("sequence"), 1U);
    EXPECT_EQ(turns.body.front().at("input").at("text"), "one");
    const auto open_events =
        api.handle("GET", "/api/v1/sessions/" + open_session_id + "/events?after=0", "");
    ASSERT_EQ(open_events.body.size(), 3U);
    EXPECT_EQ(open_events.body[0].at("sequence"), 1U);
    EXPECT_EQ(open_events.body[0].at("type"), "input.accepted");
    EXPECT_EQ(open_events.body.back().at("type"), "turn.execution.started");
    const auto next = api.handle("POST", "/api/v1/sessions/" + open_session_id + "/turns",
                                 R"({"idempotency_key":"open-2","input":{"text":"two"}})");
    ASSERT_EQ(next.status, 202U);
    EXPECT_EQ(next.body.at("sequence"), 2U);

    const auto closed = api.handle("GET", "/api/v1/sessions/" + closed_session_id, "");
    ASSERT_EQ(closed.status, 200U);
    EXPECT_EQ(closed.body.at("state"), "closed");
    const auto closed_events =
        api.handle("GET", "/api/v1/sessions/" + closed_session_id + "/events?after=0", "");
    ASSERT_EQ(closed_events.body.size(), 7U);
    for (std::size_t i = 0; i < closed_events.body.size(); ++i)
      EXPECT_EQ(closed_events.body[i].at("sequence"), i + 1);
    EXPECT_EQ(closed_events.body[3].at("type"), "session.closing");
    EXPECT_EQ(closed_events.body[4].at("type"), "turn.execution.cancel_requested");
    EXPECT_EQ(closed_events.body[5].at("type"), "turn.execution.cancelled");
    EXPECT_EQ(closed_events.body[6].at("type"), "session.closed");
    EXPECT_EQ(api.handle("POST", "/api/v1/sessions/" + closed_session_id + "/turns",
                         R"({"idempotency_key":"closed-2","input":{"text":"two"}})")
                  .status,
              409U);
  }
}
TEST(Api, SessionSseTwoClientsReplayExecutionAcrossRestart) {
  struct StreamCleanup {
    HttpServer &server;
    Service &service;
    std::shared_ptr<ContinuationFixtureState> provider;
    bool stopped = false;
    void stop() {
      if (stopped)
        return;
      if (provider) {
        {
          std::lock_guard lock(provider->mutex);
          provider->allow_provider_return = true;
        }
        provider->condition.notify_all();
      }
      server.stop();
      service.shutdown();
      stopped = true;
    }
    ~StreamCleanup() {
      stop();
    }
  };

  TemporaryDirectory dir;
  auto trace = std::make_shared<ContinuationFixtureState>();
  trace->secret_prefix = "sse-replay-canary-" + uuid();
  {
    std::lock_guard lock(trace->mutex);
    trace->block_until_released = true;
  }

  std::string session_id;
  std::string turn_id;
  std::vector<std::pair<std::uint64_t, Json>> first_client_prefix;
  std::vector<std::pair<std::uint64_t, Json>> second_client_prefix;
  {
    asio::io_context io;
    Service service(io, continuation_config(dir.path));
    register_continuation_fixture(service, trace);
    const auto pipeline =
        service.register_pipeline(continuation_pipeline()).at("id").get<std::string>();
    const auto session = service.create_session(pipeline);
    session_id = session.id;
    LocalDevelopmentIdentity identity;
    Api api(service, identity);
    HttpServer server(io, api, "127.0.0.1", 0);
    server.start();
    std::jthread server_thread_a([&] { io.run(); });
    std::jthread server_thread_b([&] { io.run(); });
    StreamCleanup cleanup{server, service, trace};

    SessionSseTestClient client_a(server.port(), session_id, 0);
    SessionSseTestClient client_b(server.port(), session_id, 0);
    EXPECT_NE(client_a.status_line().find("200 OK"), std::string::npos);
    EXPECT_NE(client_b.status_line().find("200 OK"), std::string::npos);
    const auto accepted = service.submit_session_turn(session_id, "two-client-replay",
                                                      Json{{"tag", "two-client-replay"}});
    turn_id = accepted.at("id").get<std::string>();

    bool provider_entered = false;
    {
      std::unique_lock lock(trace->mutex);
      provider_entered = trace->condition.wait_for(lock, std::chrono::seconds(5),
                                                   [&] { return trace->provider_entered; });
    }
    if (!provider_entered) {
      ADD_FAILURE() << "deterministic provider did not reach the active-turn barrier";
      client_a.close();
      client_b.close();
      cleanup.stop();
      server_thread_a.join();
      server_thread_b.join();
      return;
    }

    std::string stream_error;
    try {
      for (int i = 0; i < 3; ++i) {
        first_client_prefix.push_back(client_a.next());
        second_client_prefix.push_back(client_b.next());
      }
    } catch (const std::exception &error) {
      stream_error = error.what();
    }
    const std::vector<std::string> expected{"input.accepted", "turn.execution.claimed",
                                            "turn.execution.started"};
    if (first_client_prefix.size() == expected.size() &&
        second_client_prefix.size() == expected.size()) {
      for (std::size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(first_client_prefix[i].first, i + 1);
        EXPECT_EQ(second_client_prefix[i].first, i + 1);
        EXPECT_EQ(first_client_prefix[i].second.at("type"), expected[i]);
        EXPECT_EQ(second_client_prefix[i].second.at("type"), expected[i]);
        EXPECT_EQ(first_client_prefix[i].second, second_client_prefix[i].second);
      }
    }

    client_a.close();
    client_b.close();
    {
      std::lock_guard lock(trace->mutex);
      trace->allow_provider_return = true;
    }
    trace->condition.notify_all();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    bool succeeded = false;
    std::string final_state;
    while (std::chrono::steady_clock::now() < deadline) {
      final_state = service.get(RecordKind::SessionTurn, turn_id).value("state", "<missing>");
      if (final_state == "succeeded" || final_state == "failed" || final_state == "cancelled") {
        succeeded = final_state == "succeeded";
        break;
      }
      std::this_thread::sleep_for(Milliseconds{10});
    }
    EXPECT_TRUE(succeeded) << "session turn failed to complete before restart; final state="
                           << final_state;
    EXPECT_TRUE(stream_error.empty()) << "SSE client failed: " << stream_error;
    cleanup.stop();
    server_thread_a.join();
    server_thread_b.join();
  }

  {
    asio::io_context io;
    Service restarted(io, continuation_config(dir.path));
    register_continuation_fixture(restarted, trace);
    LocalDevelopmentIdentity identity;
    Api api(restarted, identity);
    HttpServer server(io, api, "127.0.0.1", 0);
    server.start();
    std::jthread server_thread_a([&] { io.run(); });
    std::jthread server_thread_b([&] { io.run(); });
    StreamCleanup cleanup{server, restarted, {}};

    SessionSseTestClient client_a(server.port(), session_id, 3);
    SessionSseTestClient client_b(server.port(), session_id, 3);
    EXPECT_NE(client_a.status_line().find("200 OK"), std::string::npos);
    EXPECT_NE(client_b.status_line().find("200 OK"), std::string::npos);
    const auto completed_a = client_a.next();
    const auto completed_b = client_b.next();
    EXPECT_EQ(completed_a.first, 4U);
    EXPECT_EQ(completed_b.first, 4U);
    EXPECT_EQ(completed_a.second.at("type"), "turn.execution.completed");
    EXPECT_EQ(completed_b.second.at("type"), "turn.execution.completed");
    EXPECT_EQ(completed_a.second, completed_b.second);
    EXPECT_EQ(completed_a.second.at("turn_id"), turn_id);
    const auto replay = restarted.session_events(session_id, 3, 10);
    EXPECT_EQ(replay.size(), 1U);
    if (!replay.empty()) {
      EXPECT_EQ(replay.front().at("type"), "turn.execution.completed");
      EXPECT_EQ(replay.front().at("turn_id"), turn_id);
    }
    EXPECT_EQ(completed_a.second.dump().find(trace->secret_prefix), std::string::npos);

    client_a.close();
    client_b.close();
    cleanup.stop();
    server_thread_a.join();
    server_thread_b.join();
  }
}

TEST(Api, SessionSseDeliversCommittedJournalEvents) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  service.register_pipeline(fixture("hello-pipeline"));
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  const auto session = service.create_session("hello@1");
  HttpServer server(io, api, "127.0.0.1", 0);
  server.start();
  std::jthread server_thread([&] { io.run(); });
  asio::io_context peer_io;
  boost::beast::tcp_stream client(peer_io);
  client.expires_after(std::chrono::seconds(5));
  client.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  const auto request =
      "GET /api/v1/sessions/" + session.id +
      "/events/stream HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n";
  asio::write(client, asio::buffer(request));
  asio::streambuf response_buffer;
  const auto header_bytes = asio::read_until(client, response_buffer, "\r\n\r\n");
  std::istream headers(&response_buffer);
  std::string status_line;
  std::getline(headers, status_line);
  EXPECT_NE(status_line.find("200 OK"), std::string::npos);
  bool event_content_type = false;
  for (std::string line; std::getline(headers, line) && line != "\r";)
    event_content_type =
        event_content_type || line.find("Content-Type: text/event-stream") != std::string::npos;
  EXPECT_TRUE(event_content_type);
  (void)header_bytes;
  const auto posted = api.handle("POST", "/api/v1/sessions/" + session.id + "/turns",
                                 R"({"idempotency_key":"sse-input","input":{"text":"hello"}})");
  EXPECT_EQ(posted.status, 202U);
  client.expires_after(std::chrono::seconds(5));
  asio::read_until(client, response_buffer, "\n\n");
  std::istream frame_stream(&response_buffer);
  std::string frame((std::istreambuf_iterator<char>(frame_stream)), {});
  EXPECT_NE(frame.find("id: 1\ndata: "), std::string::npos);
  boost::system::error_code ec;
  client.socket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  client.socket().close(ec);
  server.stop();
  server_thread.join();
}
TEST(Api, SessionSseResumesAfterCursorAndEndsAfterClose) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  service.register_pipeline(fixture("hello-pipeline"));
  const auto session = service.create_session("hello@1");
  const auto first = service.submit_session_turn(session.id, "sse-one", Json{{"text", "one"}});
  const auto second = service.submit_session_turn(session.id, "sse-two", Json{{"text", "two"}});
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  HttpServer server(io, api, "127.0.0.1", 0);
  server.start();
  std::jthread server_thread([&] { io.run(); });

  auto read_until_event = [&](std::uint64_t cursor, auto before_frame, auto target,
                              bool expect_stream_end) {
    asio::io_context peer_io;
    boost::beast::tcp_stream client(peer_io);
    client.expires_after(std::chrono::seconds(5));
    client.connect({asio::ip::make_address("127.0.0.1"), server.port()});
    const auto request = "GET /api/v1/sessions/" + session.id +
                         "/events/stream HTTP/1.1\r\nHost: localhost\r\nAccept: "
                         "text/event-stream\r\nLast-Event-ID: " +
                         std::to_string(cursor) + "\r\n\r\n";
    asio::write(client, asio::buffer(request));
    asio::streambuf response_buffer;
    asio::read_until(client, response_buffer, "\r\n\r\n");
    {
      std::istream headers(&response_buffer);
      std::string header_line;
      while (std::getline(headers, header_line) && header_line != "\r") {
      }
    }
    before_frame();

    std::vector<Json> events;
    while (true) {
      client.expires_after(std::chrono::seconds(5));
      asio::read_until(client, response_buffer, "\n\n");
      std::istream frame_stream(&response_buffer);
      std::string line;
      std::uint64_t sequence = 0;
      std::string data;
      while (std::getline(frame_stream, line) && !line.empty() && line != "\r") {
        if (!line.empty() && line.back() == '\r')
          line.pop_back();
        if (line.rfind("id: ", 0) == 0)
          sequence = std::stoull(line.substr(4));
        else if (line.rfind("data: ", 0) == 0)
          data = line.substr(6);
      }
      EXPECT_NE(sequence, 0U);
      EXPECT_FALSE(data.empty());
      if (sequence == 0 || data.empty())
        return events;
      auto event = Json::parse(data);
      event["sequence"] = sequence;
      events.push_back(std::move(event));
      if (target(events.back()))
        break;
    }
    if (expect_stream_end) {
      client.expires_after(std::chrono::seconds(2));
      char byte{};
      boost::system::error_code ec;
      (void)client.read_some(asio::buffer(&byte, 1), ec);
      EXPECT_TRUE(ec == asio::error::eof || ec == asio::error::connection_reset);
    }
    boost::system::error_code ec;
    client.socket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    client.socket().close(ec);
    return events;
  };

  const auto resumed = read_until_event(
      1, [] {},
      [&](const Json &event) {
        return event.value("type", std::string{}) == "input.accepted" &&
               event.value("turn_id", std::string{}) == second.at("id").get<std::string>();
      },
      false);
  std::uint64_t cursor = 1;
  for (const auto &event : resumed) {
    const auto sequence = event.at("sequence").get<std::uint64_t>();
    EXPECT_GT(sequence, cursor);
    cursor = sequence;
  }
  ASSERT_FALSE(resumed.empty());
  EXPECT_EQ(resumed.back().at("type"), "input.accepted");
  EXPECT_EQ(resumed.back().at("payload").at("input").at("text"), "two");
  EXPECT_NE(first.at("id"), second.at("id"));

  const auto third = service.submit_session_turn(session.id, "sse-three", Json{{"text", "three"}});
  const auto reconnected = read_until_event(
      cursor, [] {},
      [&](const Json &event) {
        return event.value("type", std::string{}) == "input.accepted" &&
               event.value("turn_id", std::string{}) == third.at("id").get<std::string>();
      },
      false);
  for (const auto &event : reconnected) {
    const auto sequence = event.at("sequence").get<std::uint64_t>();
    EXPECT_GT(sequence, cursor);
    cursor = sequence;
  }
  ASSERT_FALSE(reconnected.empty());
  EXPECT_EQ(reconnected.back().at("type"), "input.accepted");
  EXPECT_EQ(reconnected.back().at("payload").at("input").at("text"), "three");

  const auto closed = read_until_event(
      cursor, [&] { service.close_session(session.id); },
      [](const Json &event) { return event.value("type", std::string{}) == "session.closed"; },
      true);
  for (const auto &event : closed) {
    const auto sequence = event.at("sequence").get<std::uint64_t>();
    EXPECT_GT(sequence, cursor);
    cursor = sequence;
  }
  ASSERT_FALSE(closed.empty());
  EXPECT_EQ(closed.back().at("type"), "session.closed");

  server.stop();
  server_thread.join();
}
TEST(Api, SessionSseAdmissionRejectsAndReconnectsWithReplayCursor) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  service.register_pipeline(fixture("hello-pipeline"));
  const auto session = service.create_session("hello@1");
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  HttpServer server(io, api, "127.0.0.1", 0,
                    HttpServerOptions{1, std::chrono::seconds(1), std::chrono::seconds(10)});
  server.start();
  std::jthread server_thread([&] { io.run(); });
  auto open = [&](auto &socket, std::uint64_t cursor) {
    socket.connect({asio::ip::make_address("127.0.0.1"), server.port()});
    const auto request = "GET /api/v1/sessions/" + session.id +
                         "/events/stream HTTP/1.1\r\nHost: localhost\r\nAccept: "
                         "text/event-stream\r\nLast-Event-ID: " +
                         std::to_string(cursor) + "\r\n\r\n";
    asio::write(socket, asio::buffer(request));
  };
  auto read_header = [](auto &socket, asio::streambuf &buffer) {
    asio::read_until(socket, buffer, "\r\n\r\n");
    return std::string(asio::buffers_begin(buffer.data()), asio::buffers_end(buffer.data()));
  };
  asio::io_context first_io;
  boost::beast::tcp_stream first(first_io);
  first.expires_after(std::chrono::seconds(5));
  open(first, 0);
  asio::streambuf first_buffer;
  const auto first_response = read_header(first, first_buffer);
  EXPECT_NE(first_response.find("200 OK"), std::string::npos);
  ASSERT_EQ(server.metrics().active_session_streams, 1U);
  EXPECT_EQ(server.metrics().session_stream_limit, 1U);

  asio::io_context rejected_io;
  boost::beast::tcp_stream rejected(rejected_io);
  rejected.expires_after(std::chrono::seconds(5));
  open(rejected, 0);
  asio::streambuf rejected_buffer;
  const auto rejected_response = read_header(rejected, rejected_buffer);
  const auto headers_end = rejected_response.find("\r\n\r\n");
  const auto rejected_headers = rejected_response.substr(0, headers_end);
  EXPECT_NE(rejected_headers.find("429 Too Many Requests"), std::string::npos);
  EXPECT_NE(rejected_headers.find("Retry-After: 1"), std::string::npos);
  EXPECT_EQ(rejected_headers.find("text/event-stream"), std::string::npos);
  EXPECT_EQ(server.metrics().active_session_streams, 1U);
  EXPECT_EQ(server.metrics().rejected_session_streams, 1U);
  EXPECT_TRUE(service.session_events(session.id, 0, 10).empty());

  const auto accepted = service.submit_session_turn(session.id, "sse-admission-replay",
                                                    Json{{"text", "replay after admission"}});
  ASSERT_EQ(accepted.at("sequence"), 1U);
  boost::system::error_code ec;
  first.socket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  first.socket().close(ec);
  for (int i = 0; i < 200 && server.metrics().active_session_streams != 0; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_EQ(server.metrics().active_session_streams, 0U);

  asio::io_context resumed_io;
  boost::beast::tcp_stream resumed(resumed_io);
  resumed.expires_after(std::chrono::seconds(5));
  open(resumed, 0);
  asio::streambuf resumed_buffer;
  const auto resumed_headers = read_header(resumed, resumed_buffer);
  ASSERT_NE(resumed_headers.find("200 OK"), std::string::npos);
  resumed.expires_after(std::chrono::seconds(2));
  asio::read_until(resumed, resumed_buffer, "\n\n");
  const std::string frame(asio::buffers_begin(resumed_buffer.data()),
                          asio::buffers_end(resumed_buffer.data()));
  EXPECT_NE(frame.find("id: 1\ndata: "), std::string::npos);
  EXPECT_NE(frame.find("replay after admission"), std::string::npos);
  resumed.socket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  resumed.socket().close(ec);
  server.stop();
  server_thread.join();
  EXPECT_EQ(server.metrics().active_session_streams, 0U);
  EXPECT_EQ(server.metrics().accepted_session_streams, 2U);
  EXPECT_EQ(server.metrics().closed_session_streams, 2U);
}
TEST(Api, SessionSseSlotReleasesPromptlyOnIdleClientDisconnect) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  service.register_pipeline(fixture("hello-pipeline"));
  const auto session = service.create_session("hello@1");
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  HttpServer server(io, api, "127.0.0.1", 0,
                    HttpServerOptions{1, std::chrono::seconds(15), std::chrono::minutes(30)});
  server.start();
  std::jthread server_thread([&] { io.run(); });
  asio::io_context peer_io;
  boost::beast::tcp_stream client(peer_io);
  client.expires_after(std::chrono::seconds(5));
  client.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  const auto request = "GET /api/v1/sessions/" + session.id +
                       "/events/stream HTTP/1.1\r\nHost: localhost\r\nAccept: "
                       "text/event-stream\r\n\r\n";
  asio::write(client, asio::buffer(request));
  asio::streambuf response;
  asio::read_until(client, response, "\r\n\r\n");
  ASSERT_EQ(server.metrics().active_session_streams, 1U);
  boost::system::error_code ec;
  client.socket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  client.socket().close(ec);
  for (int i = 0; i < 100 && server.metrics().active_session_streams != 0; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_EQ(server.metrics().active_session_streams, 0U);
  EXPECT_EQ(server.metrics().closed_session_streams, 1U);
  server.stop();
  server_thread.join();
}
TEST(Api, SessionSseSlotIsReleasedAfterConfiguredLifetimeAndShutdown) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  service.register_pipeline(fixture("hello-pipeline"));
  const auto session = service.create_session("hello@1");
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  HttpServer server(io, api, "127.0.0.1", 0,
                    HttpServerOptions{1, std::chrono::seconds(1), std::chrono::seconds(1)});
  server.start();
  std::jthread server_thread([&] { io.run(); });
  asio::io_context peer_io;
  boost::beast::tcp_stream client(peer_io);
  client.expires_after(std::chrono::seconds(3));
  client.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  const auto request = "GET /api/v1/sessions/" + session.id +
                       "/events/stream HTTP/1.1\r\nHost: localhost\r\nAccept: "
                       "text/event-stream\r\n\r\n";
  asio::write(client, asio::buffer(request));
  asio::streambuf buffer;
  asio::read_until(client, buffer, "\r\n\r\n");
  ASSERT_EQ(server.metrics().active_session_streams, 1U);
  client.expires_after(std::chrono::seconds(3));
  char byte{};
  boost::system::error_code read_error;
  (void)client.read_some(asio::buffer(&byte, 1), read_error);
  EXPECT_TRUE(read_error == asio::error::eof || read_error == asio::error::connection_reset);
  for (int i = 0; i < 100 && server.metrics().active_session_streams != 0; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_EQ(server.metrics().active_session_streams, 0U);
  EXPECT_EQ(server.metrics().rejected_session_streams, 0U);
  asio::io_context shutdown_io;
  boost::beast::tcp_stream active(shutdown_io);
  active.expires_after(std::chrono::seconds(3));
  active.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  const auto active_request = "GET /api/v1/sessions/" + session.id +
                              "/events/stream HTTP/1.1\r\nHost: localhost\r\nAccept: "
                              "text/event-stream\r\n\r\n";
  asio::write(active, asio::buffer(active_request));
  asio::streambuf active_buffer;
  asio::read_until(active, active_buffer, "\r\n\r\n");
  ASSERT_EQ(server.metrics().active_session_streams, 1U);
  server.stop();
  server_thread.join();
  EXPECT_EQ(server.metrics().active_session_streams, 0U);
  EXPECT_EQ(server.metrics().accepted_session_streams, 2U);
  EXPECT_EQ(server.metrics().closed_session_streams, 2U);
}
TEST(Api, PostgresSessionSseObservesEventsFromAnotherInstance) {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  ASSERT_NE(dsn, nullptr);
  auto schema = "laso_session_sse_" + uuid();
  std::replace(schema.begin(), schema.end(), '-', '_');
  struct SchemaCleanup {
    std::string dsn;
    std::string schema;
    ~SchemaCleanup() {
      try {
        pqxx::connection connection(dsn);
        pqxx::work transaction(connection);
        transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
        transaction.commit();
      } catch (...) {
      }
    }
  } cleanup{dsn, schema};
  TemporaryDirectory dir;
  Config c = config(dir.path);
  c.postgres_dsn = dsn;
  c.postgres_schema = schema;
  c.execution_mode = "multi_instance";
  c.validate();
  asio::io_context writer_io;
  asio::io_context observer_io;
  {
    Service writer(writer_io, c);
    writer.register_pipeline(fixture("hello-pipeline"));
    Service observer(observer_io, c);
    LocalDevelopmentIdentity identity;
    Api api(observer, identity);
    const auto session = writer.create_session("hello@1");
    const auto first = writer.submit_session_turn(session.id, "replayed-before-connect-one",
                                                  Json{{"text", "replay-one"}});
    const auto replayed = writer.submit_session_turn(session.id, "replayed-before-connect-two",
                                                     Json{{"text", "replay-two"}});
    EXPECT_EQ(first.at("sequence"), 1U);
    EXPECT_EQ(replayed.at("sequence"), 2U);
    HttpServer server(observer_io, api, "127.0.0.1", 0);
    server.start();
    std::jthread server_thread([&] { observer_io.run(); });
    struct ServerCleanup {
      HttpServer &server;
      Service &service;
      std::jthread &thread;
      ~ServerCleanup() {
        server.stop();
        service.shutdown();
        if (thread.joinable())
          thread.join();
      }
    } server_cleanup{server, observer, server_thread};
    asio::io_context peer_io;
    boost::beast::tcp_stream client(peer_io);
    client.expires_after(std::chrono::seconds(5));
    client.connect({asio::ip::make_address("127.0.0.1"), server.port()});
    const auto existing_events = writer.session_events(session.id, 0, 100);
    const auto first_accept =
        std::find_if(existing_events.begin(), existing_events.end(), [&](const Json &event) {
          return event.value("type", std::string{}) == "input.accepted" &&
                 event.value("turn_id", std::string{}) == first.at("id").get<std::string>();
        });
    ASSERT_NE(first_accept, existing_events.end());
    auto cursor = first_accept->at("sequence").get<std::uint64_t>();
    const auto request = "GET /api/v1/sessions/" + session.id +
                         "/events/stream HTTP/1.1\r\nHost: localhost\r\nAccept: "
                         "text/event-stream\r\nLast-Event-ID: " +
                         std::to_string(cursor) + "\r\n\r\n";
    asio::write(client, asio::buffer(request));
    asio::streambuf response_buffer;
    (void)asio::read_until(client, response_buffer, "\r\n\r\n");
    {
      std::istream headers(&response_buffer);
      std::string line;
      while (std::getline(headers, line) && line != "\r") {
      }
    }
    const auto read_frame = [&]() {
      client.expires_after(std::chrono::seconds(5));
      asio::read_until(client, response_buffer, "\n\n");
      std::istream frame_stream(&response_buffer);
      std::uint64_t sequence = 0;
      std::string frame;
      for (std::string line; std::getline(frame_stream, line);) {
        if (line.empty() || line == "\r")
          break;
        if (line.rfind("id: ", 0) == 0)
          sequence = std::stoull(line.substr(4));
        frame += line + "\n";
      }
      return std::pair{sequence, frame};
    };
    const auto is_accepted_turn = [](const std::string &frame, const std::string &turn_id) {
      return frame.find("\"type\":\"input.accepted\"") != std::string::npos &&
             frame.find("\"turn_id\":\"" + turn_id + "\"") != std::string::npos;
    };
    bool replayed_second = false;
    for (std::size_t i = 0; i < 16 && !replayed_second; ++i) {
      const auto [sequence, frame] = read_frame();
      EXPECT_EQ(sequence, cursor + 1);
      cursor = sequence;
      EXPECT_EQ(frame.find("replay-one"), std::string::npos);
      replayed_second = is_accepted_turn(frame, replayed.at("id").get<std::string>());
      if (replayed_second) {
        EXPECT_NE(frame.find("replay-two"), std::string::npos);
      }
    }
    EXPECT_TRUE(replayed_second);
    const auto posted = writer.submit_session_turn(session.id, "writer-instance-input",
                                                   Json{{"text", "from writer"}});
    EXPECT_EQ(posted.at("sequence"), 3U);
    bool observed_live = false;
    for (std::size_t i = 0; i < 16 && !observed_live; ++i) {
      const auto [sequence, frame] = read_frame();
      EXPECT_EQ(sequence, cursor + 1);
      cursor = sequence;
      observed_live = is_accepted_turn(frame, posted.at("id").get<std::string>());
    }
    EXPECT_TRUE(observed_live);
    boost::system::error_code ec;
    client.socket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    client.socket().close(ec);
  }
}
TEST(Api, RunMetadataIsPreservedForWorkerContext) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  LocalDevelopmentIdentity identity;
  Api api(s, identity);
  ASSERT_EQ(
      api.handle("POST", "/api/v1/pipelines", Json{{"yaml", fixture("hello-pipeline")}}.dump())
          .status,
      201U);
  const auto response = api.handle(
      "POST", "/api/v1/pipelines/hello/runs",
      Json{{"input", Json{{"value", 42}}},
           {"metadata", Json{{"classification", "public"}, {"project_dir", "/tmp/example"}}}}
          .dump());
  ASSERT_EQ(response.status, 202U);
  io.run();
  const auto run =
      api.handle("GET", "/api/v1/runs/" + response.body.at("id").get<std::string>(), "").body;
  EXPECT_EQ(run.at("message").at("metadata").at("classification"), "public");
  EXPECT_EQ(run.at("message").at("metadata").at("project_dir"), "/tmp/example");
}
TEST(Api, RejectsMalformedAndOversizedRequests) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  LocalDevelopmentIdentity identity;
  Api api(s, identity);
  EXPECT_EQ(api.handle("POST", "/api/v1/pipelines", "{").status, 400U);
  EXPECT_EQ(api.handle("POST", "/api/v1/pipelines", std::string(1024 * 1024 + 1, 'a')).status,
            413U);
}
TEST(Api, AuthenticationBoundaryApplies) {
  struct Denied : IdentityProvider {
    Actor authenticate(const std::string &) const override {
      return {};
    }
    bool authorize(const AuthorizationContext &) const override {
      return false;
    }
  } identity;
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  Api api(s, identity);
  EXPECT_EQ(api.handle("GET", "/api/v1/runs", "").status, 403U);
}
TEST(Api, ServesRealHTTPHealth) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  LocalDevelopmentIdentity identity;
  Api api(s, identity);
  HttpServer server(io, api, "127.0.0.1", 0);
  auto port = server.port();
  server.start();
  std::jthread worker([&] { io.run(); });
  asio::io_context peer_io;
  boost::beast::tcp_stream stream(peer_io);
  stream.expires_after(std::chrono::seconds(5));
  stream.connect({asio::ip::make_address("127.0.0.1"), port});
  boost::beast::http::request<boost::beast::http::empty_body> request{boost::beast::http::verb::get,
                                                                      "/api/v1/health", 11};
  request.set(boost::beast::http::field::host, "localhost");
  boost::beast::http::write(stream, request);
  boost::beast::flat_buffer buffer;
  boost::beast::http::response<boost::beast::http::string_body> response;
  boost::beast::http::read(stream, buffer, response);
  EXPECT_EQ(response.result_int(), 200U);
  EXPECT_EQ(Json::parse(response.body()).at("status"), "ok");
  server.stop();
  worker.join();
}
TEST(Scheduler, FiniteIntervalSchedule) {
  asio::io_context io;
  unsigned count = 0;
  LocalScheduler scheduler(io, [&](const ScheduledPipeline &) { ++count; });
  ScheduledPipeline s;
  s.max_firings = 3;
  s.interval = Milliseconds{1};
  scheduler.schedule(s);
  io.run();
  EXPECT_EQ(count, 3U);
}
TEST(Scheduler, StopBeforeExecutorStarts) {
  asio::io_context io;
  unsigned count = 0;
  LocalScheduler scheduler(io, [&](const ScheduledPipeline &) { ++count; });
  scheduler.schedule({});
  scheduler.stop();
  io.run();
  EXPECT_EQ(count, 0U);
}
TEST(Storage, InterruptedRunBecomesInspectablePausedCheckpoint) {
  TemporaryDirectory dir;
  auto c = config(dir.path);
  laso::Run r;
  r.state = RunState::Running;
  r.pipeline_id = "hello";
  r.definition = fixture("hello-pipeline");
  {
    auto s = make_storage(c.data_dir / "service");
    s->commit({{RecordKind::Run, r.id, r.id, Json(r)}});
  }
  asio::io_context io;
  Service s(io, c);
  EXPECT_EQ(s.get(RecordKind::Run, r.id).get<laso::Run>().state, RunState::Paused);
}
TEST(Storage, PersistedCancellationSurvivesRestart) {
  TemporaryDirectory dir;
  auto c = config(dir.path);
  laso::Run r;
  r.state = RunState::Running;
  r.cancellation_requested = true;
  {
    auto s = make_storage(c.data_dir / "service");
    s->commit({{RecordKind::Run, r.id, r.id, Json(r)}});
  }
  asio::io_context io;
  Service s(io, c);
  EXPECT_EQ(s.get(RecordKind::Run, r.id).get<laso::Run>().state, RunState::Cancelled);
}
TEST(Storage, RecoveryDoesNotRewriteTerminalRuns) {
  TemporaryDirectory dir;
  auto c = config(dir.path);
  laso::Run r;
  r.state = RunState::Completed;
  r.cancellation_requested = true;
  r.pipeline_id = "completed";
  r.definition = fixture("hello-pipeline");
  {
    auto storage = make_storage(c.data_dir / "service");
    storage->commit({{RecordKind::Run, r.id, r.id, Json(r)}});
  }
  asio::io_context io;
  Service service(io, c);
  EXPECT_EQ(service.get(RecordKind::Run, r.id).get<laso::Run>().state, RunState::Completed);
}
TEST(Storage, RecoveryScansAttemptsBeyondOnePage) {
  TemporaryDirectory dir;
  auto c = config(dir.path);
  laso::Run r;
  r.state = RunState::Running;
  r.pipeline_id = "recovery";
  r.definition = fixture("hello-pipeline");
  std::vector<Record> records{{RecordKind::Run, r.id, r.id, Json(r)}};
  std::string interrupted;
  for (unsigned i = 0; i < 10001; ++i) {
    NodeExecution attempt;
    attempt.run_id = r.id;
    attempt.node_id = "action";
    attempt.state = i == 10000 ? NodeState::Running : NodeState::Completed;
    if (i == 10000)
      interrupted = attempt.id;
    records.push_back({RecordKind::Attempt, attempt.id, r.id, Json(attempt)});
  }
  {
    auto storage = make_storage(c.data_dir / "service");
    storage->commit(records);
  }
  asio::io_context io;
  Service service(io, c);
  EXPECT_EQ(service.get(RecordKind::Attempt, interrupted).at("state"), "Failed");
}
TEST(Api, PaginationBounds) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  LocalDevelopmentIdentity identity;
  Api api(s, identity);
  EXPECT_EQ(api.handle("GET", "/api/v1/runs?limit=1&offset=0", "").status, 200U);
  EXPECT_EQ(api.handle("GET", "/api/v1/runs?limit=10000", "").status, 400U);
}
