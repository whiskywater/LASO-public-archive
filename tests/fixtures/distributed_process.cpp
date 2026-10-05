#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <laso/application/service.hpp>
#include <laso/runtime/executor.hpp>
#include <string>
#include <thread>
#include <utility>

using namespace laso;

namespace {
const char *required(const char *name) {
  const auto *value = std::getenv(name);
  return value && *value ? value : nullptr;
}
std::string setting(const char *name, std::string fallback = {}) {
  const auto *value = required(name);
  return value ? value : std::move(fallback);
}
void touch(const std::filesystem::path &path, const std::string &contents = "entered\n") {
  std::filesystem::create_directories(path.parent_path());
  std::ofstream(path, std::ios::trunc) << contents;
}
bool wait_for_file(const std::filesystem::path &path, Milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::filesystem::exists(path))
      return true;
    std::this_thread::sleep_for(Milliseconds{10});
  }
  return std::filesystem::exists(path);
}

class PhysicalContinuationProvider final : public ModelProvider {
public:
  Task<ModelResponse> generate(const ModelRequest &request, ExecutionContext &context) override {
    context.check();
    const auto entered = required("LASO_DISTRIBUTED_TEST_PROVIDER_ENTERED");
    if (entered)
      touch(entered);
    const auto release = required("LASO_DISTRIBUTED_TEST_PROVIDER_RELEASE");
    if (release) {
      const auto timeout =
          std::stoull(setting("LASO_DISTRIBUTED_TEST_BARRIER_TIMEOUT_MS", "30000"));
      const auto deadline =
          std::chrono::steady_clock::now() + Milliseconds{static_cast<std::int64_t>(timeout)};
      while (!std::filesystem::exists(release)) {
        context.check();
        if (std::chrono::steady_clock::now() >= deadline) {
          if (entered)
            touch(std::string(entered) + ".timeout", "provider barrier timed out\n");
          throw Error(ErrorCode::Timeout, "Test provider barrier timed out");
        }
        co_await context.delay(Milliseconds{10});
      }
    }

    const auto expected = setting("LASO_DISTRIBUTED_TEST_EXPECTED_CONTINUATION");
    Json output{
        {"worker_result", setting("LASO_DISTRIBUTED_TEST_PROVIDER_RESULT", "fixture-result")}};
    if (!expected.empty())
      output["continuation_match"] =
          request.continuation && request.continuation->state == expected;
    ModelResponse response{std::move(output), request.model, "physical-fixture"};
    response.continuation = OpaqueProviderContinuation{
        "physical-fixture", "1",
        setting("LASO_DISTRIBUTED_TEST_PROVIDER_CONTINUATION", "fixture-continuation")};
    co_return response;
  }

  ProviderHealth health() const override {
    return {true, "deterministic fixture"};
  }

  ProviderMetadata metadata() const override {
    ProviderMetadata result;
    result.name = "physical-fixture";
    result.version = "1";
    result.timeout = Milliseconds{180000};
    result.continuation_mode = ContinuationMode::Opaque;
    return result;
  }
};

const std::string session_pipeline = R"yaml(
laso: '1'
name: physical-session-recovery
version: 1
nodes:
  input: {type: input}
  model:
    type: agent
    model: physical-model
    prompt: Return the deterministic fixture response.
    timeout_ms: 180000
  output: {type: output}
edges:
  - {from: input, to: model}
  - {from: model, to: output}
)yaml";

Config test_config() {
  Config config;
  config.storage_backend = "postgres";
  config.postgres_dsn =
      required("LASO_DISTRIBUTED_TEST_DSN") ? required("LASO_DISTRIBUTED_TEST_DSN") : "";
  config.postgres_schema =
      required("LASO_DISTRIBUTED_TEST_SCHEMA") ? required("LASO_DISTRIBUTED_TEST_SCHEMA") : "";
  config.execution_mode = "multi_instance";
  config.postgres_pool_acquisition_timeout_ms =
      std::stoull(setting("LASO_DISTRIBUTED_TEST_PG_CONNECT_TIMEOUT_MS", "15000"));
  config.models["physical-model"] =
      ModelBinding{setting("LASO_DISTRIBUTED_TEST_PROVIDER_NAME", "physical-fixture"),
                   "deterministic", Json::object()};
  config.max_runs = 1;
  config.max_nodes = 1;
  config.max_nodes_per_run = 1;
  config.workers =
      static_cast<unsigned>(std::stoul(setting("LASO_DISTRIBUTED_TEST_IO_THREADS", "1")));
  config.coordination_lease_ttl_ms =
      std::stoull(setting("LASO_DISTRIBUTED_TEST_COORDINATION_LEASE_TTL_MS", "1000"));
  config.coordination_heartbeat_interval_ms =
      std::stoull(setting("LASO_DISTRIBUTED_TEST_COORDINATION_HEARTBEAT_INTERVAL_MS", "100"));
  config.max_pending_runs = 16;
  config.data_dir =
      setting("LASO_DISTRIBUTED_TEST_DATA_DIR").empty()
          ? std::filesystem::temp_directory_path() / ("laso-distributed-fixture-" + uuid())
          : std::filesystem::path(setting("LASO_DISTRIBUTED_TEST_DATA_DIR"));
  config.validate();
  std::filesystem::create_directories(config.data_dir);
  return config;
}

std::unique_ptr<Storage> test_storage(const Config &config) {
  StorageOptions options;
  options.backend = config.storage_backend;
  options.postgres_dsn = config.postgres_dsn;
  options.postgres_schema = config.postgres_schema;
  options.postgres_pool_acquisition_timeout_ms = config.postgres_pool_acquisition_timeout_ms;
  options.allow_multiple_processes = true;
  return create_storage(options);
}

int seed_case(const Config &config) {
  asio::io_context io;
  Service service(io, config);
  service.provider_registry().add("physical-fixture",
                                  std::make_shared<PhysicalContinuationProvider>());
  const auto pipeline_id = service.register_pipeline(session_pipeline).at("id").get<std::string>();
  const auto session = service.create_session(pipeline_id);
  auto storage = test_storage(config);
  const auto turn_id = session.id + "-turn-" + uuid();
  const Json turn{{"idempotency_key", uuid()},
                  {"input", {{"fixture_case", setting("LASO_DISTRIBUTED_TEST_CASE", "recovery")}}},
                  {"state", "queued"},
                  {"accepted_at", timestamp()},
                  {"pipeline_id", pipeline_id}};
  Event accepted;
  accepted.run_id = session.id;
  accepted.type = "input.accepted";
  if (!storage->submit_session_turn(session.id, turn_id, turn, Json(accepted)))
    throw Error(ErrorCode::Storage, "Could not seed the test session turn");
  service.shutdown();
  std::cout
      << Json{{"session_id", session.id}, {"turn_id", turn_id}, {"pipeline_id", pipeline_id}}.dump()
      << '\n';
  return 0;
}

int submit_turn(const Config &config) {
  const auto *session_id = required("LASO_DISTRIBUTED_TEST_SESSION_ID");
  if (!session_id)
    return 2;
  auto storage = test_storage(config);
  const auto session = storage->get(RecordKind::AgentSession, session_id);
  const auto turn_id = std::string(session_id) + "-turn-" + uuid();
  const Json turn{{"idempotency_key", uuid()},
                  {"input", {{"fixture_case", setting("LASO_DISTRIBUTED_TEST_CASE", "next-turn")}}},
                  {"state", "queued"},
                  {"accepted_at", timestamp()},
                  {"pipeline_id", session.at("pipeline_id")}};
  Event accepted;
  accepted.run_id = session_id;
  accepted.type = "input.accepted";
  if (!storage->submit_session_turn(session_id, turn_id, turn, Json(accepted)))
    throw Error(ErrorCode::Storage, "Could not submit the test session turn");
  std::cout << Json{{"turn_id", turn_id}}.dump() << '\n';
  return 0;
}

int inspect(const Config &config) {
  const auto *session_id = required("LASO_DISTRIBUTED_TEST_SESSION_ID");
  const auto *turn_id = required("LASO_DISTRIBUTED_TEST_TURN_ID");
  if (!session_id || !turn_id)
    return 2;
  auto storage = test_storage(config);
  const auto session = storage->get(RecordKind::AgentSession, session_id);
  const auto turn = storage->get(RecordKind::SessionTurn, turn_id);
  Json snapshot{{"observed_at", timestamp()},
                {"session",
                 {{"state", session.value("state", std::string{})},
                  {"active_turn_id", session.value("active_turn_id", std::string{})},
                  {"active_run_id", session.value("active_run_id", std::string{})}}},
                {"turn",
                 {{"state", turn.value("state", std::string{})},
                  {"sequence", turn.value("sequence", std::uint64_t{0})},
                  {"run_id", turn.value("run_id", std::string{})},
                  {"cancellation_requested", turn.value("cancellation_requested", false)}}}};
  const auto result = turn.value("result", Json::object());
  if (result.is_object() && result.contains("worker_result"))
    snapshot["turn"]["worker_result"] = result.at("worker_result");
  if (result.is_object() && result.contains("continuation_match"))
    snapshot["turn"]["continuation_match"] = result.at("continuation_match");

  const auto run_id = turn.value("run_id", std::string{});
  if (!run_id.empty()) {
    const auto run = storage->get(RecordKind::Run, run_id);
    CoordinationOptions coordination_options;
    coordination_options.postgres_dsn = config.postgres_dsn;
    coordination_options.postgres_schema = config.postgres_schema;
    coordination_options.pool_acquisition_timeout_ms = config.postgres_pool_acquisition_timeout_ms;
    auto coordination = create_coordination(coordination_options, "physical-inspector-" + uuid());
    const auto lease = coordination->inspect("run:" + run_id);
    if (lease)
      snapshot["lease"] = {{"owner_instance_id", lease->owner_instance},
                           {"fencing_token", lease->fencing_token},
                           {"expires_at", lease->expires_at},
                           {"active", lease->active}};
    snapshot["run"] = {{"state", run.value("state", std::string{})},
                       {"owner_instance_id", run.value("owner_instance_id", std::string{})},
                       {"fencing_token", run.value("fencing_token", std::uint64_t{0})},
                       {"lease_expires_at", run.value("lease_expires_at", std::string{})},
                       {"cancellation_requested", run.value("cancellation_requested", false)},
                       {"error", run.value("error", std::string{})}};
    Json attempts = Json::array();
    for (const auto &attempt : storage->list(RecordKind::Attempt, run_id, 1000, 0))
      attempts.push_back({{"id", attempt.value("id", std::string{})},
                          {"node_id", attempt.value("node_id", std::string{})},
                          {"state", attempt.value("state", std::string{})},
                          {"attempt", attempt.value("attempt", 0U)}});
    snapshot["attempts"] = std::move(attempts);
    const auto runs = storage->list(RecordKind::Run, "", 10000, 0);
    snapshot["associated_run_count"] =
        std::count_if(runs.begin(), runs.end(), [&](const Json &candidate) {
          return candidate.value("session_turn_id", std::string{}) == turn_id;
        });
  }

  Json events = Json::array();
  for (const auto &event : storage->session_events(session_id, 0, 1000))
    events.push_back({{"sequence", event.value("sequence", std::uint64_t{0})},
                      {"type", event.value("type", std::string{})},
                      {"turn_id", event.value("turn_id", std::string{})}});
  snapshot["events"] = std::move(events);

  Json continuations = Json::array();
  const auto expected = setting("LASO_DISTRIBUTED_TEST_EXPECTED_CONTINUATION");
  for (const auto &record : storage->list(RecordKind::SessionContinuation, session_id, 1000, 0)) {
    Json safe{{"scope", record.value("scope", std::string{})},
              {"provider_id", record.value("provider_id", std::string{})},
              {"provider_version", record.value("provider_version", std::string{})},
              {"source_run_id", record.value("source_run_id", std::string{})},
              {"has_state", !record.value("state", std::string{}).empty()}};
    if (!expected.empty())
      safe["matches_expected"] = record.value("state", std::string{}) == expected;
    continuations.push_back(std::move(safe));
  }
  snapshot["continuations"] = std::move(continuations);
  std::cout << snapshot.dump() << '\n';
  return 0;
}

#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
std::string test_point_name(SessionTestPoint point) {
  switch (point) {
  case SessionTestPoint::AfterClaim:
    return "after-claim";
  case SessionTestPoint::BeforeRunClaim:
    return "before-run-claim";
  case SessionTestPoint::BeforeRunBinding:
    return "before-run-binding";
  case SessionTestPoint::AfterRunBinding:
    return "after-run-binding";
  case SessionTestPoint::BeforeNodeCheckpointCommit:
    return "before-node-checkpoint";
  case SessionTestPoint::NodeCheckpointRejected:
    return "node-checkpoint-rejected";
  case SessionTestPoint::BeforeCompletionCommit:
    return "before-completion-commit";
  case SessionTestPoint::AfterCompletionCommit:
    return "after-completion-commit";
  }
  return "unknown";
}
bool selected_point(const std::string &points, const std::string &name) {
  std::size_t start = 0;
  while (start <= points.size()) {
    const auto end = points.find(',', start);
    const auto item =
        points.substr(start, end == std::string::npos ? std::string::npos : end - start);
    if (item == name)
      return true;
    if (end == std::string::npos)
      break;
    start = end + 1;
  }
  return false;
}
#endif
} // namespace

int main() {
  const auto *dsn = required("LASO_DISTRIBUTED_TEST_DSN");
  const auto *schema = required("LASO_DISTRIBUTED_TEST_SCHEMA");
  if (!dsn || !schema)
    return 2;
  try {
    auto config = test_config();
    if (const auto *worker_host = required("LASO_DISTRIBUTED_TEST_WORKER_HOST")) {
      ProcessWorkerConfig worker;
      worker.executable = worker_host;
      worker.args = {"--mode", "delay-ms", "--delay-ms", "200"};
      worker.startup_timeout_ms = 1000;
      worker.request_timeout_ms = 100;
      config.process_workers.emplace("process", std::move(worker));
      config.validate();
    }
    const auto mode = setting("LASO_DISTRIBUTED_TEST_MODE", "run");
    if (mode == "seed")
      return seed_case(config);
    if (mode == "inspect")
      return inspect(config);
    if (mode == "submit")
      return submit_turn(config);

    const auto *run_id = required("LASO_DISTRIBUTED_TEST_RUN_ID");
    const auto *session_id = required("LASO_DISTRIBUTED_TEST_SESSION_ID");
    const auto *turn_id = required("LASO_DISTRIBUTED_TEST_TURN_ID");
    const bool session_mode = session_id && turn_id;
    if (!session_mode && !run_id)
      return 2;

    const auto hold_ms = std::stoll(setting("LASO_DISTRIBUTED_TEST_HOLD_MS", "10000"));
    const auto delay = Milliseconds{hold_ms};
    Executor executor(config.workers);
    Service service(executor.context(), config);
    service.provider_registry().add("physical-fixture",
                                    std::make_shared<PhysicalContinuationProvider>());
    const auto hold = std::make_shared<Function>(
        [delay](ExecutionContext &context, const Json &input) -> Task<Json> {
          if (const auto *marker = required("LASO_DISTRIBUTED_TEST_MARKER"))
            touch(marker);
          if (const auto *release = required("LASO_DISTRIBUTED_TEST_RELEASE")) {
            const auto timeout =
                std::stoull(setting("LASO_DISTRIBUTED_TEST_BARRIER_TIMEOUT_MS", "30000"));
            const auto deadline =
                std::chrono::steady_clock::now() + Milliseconds{static_cast<std::int64_t>(timeout)};
            while (!std::filesystem::exists(release)) {
              context.check();
              if (std::chrono::steady_clock::now() >= deadline)
                throw Error(ErrorCode::Timeout, "Test function barrier timed out");
              co_await context.delay(Milliseconds{10});
            }
          } else {
            co_await context.delay(delay);
          }
          co_return input;
        });
    service.functions().add("distributed_hold", hold);
    service.functions().add("session_process_hold", hold);
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    const auto exit_point = setting("LASO_DISTRIBUTED_TEST_SESSION_EXIT_AT");
    const auto hold_points = setting("LASO_DISTRIBUTED_TEST_HOLD_POINTS");
    const auto barrier_dir = std::filesystem::path(setting("LASO_DISTRIBUTED_TEST_BARRIER_DIR"));
    const auto barrier_timeout = Milliseconds{static_cast<std::int64_t>(
        std::stoull(setting("LASO_DISTRIBUTED_TEST_BARRIER_TIMEOUT_MS", "30000")))};
    service.runtime().set_session_test_hook(
        [exit_point, hold_points, barrier_dir, barrier_timeout](SessionTestPoint point) {
          const auto name = test_point_name(point);
          if (!exit_point.empty() && exit_point == name)
            std::_Exit(86);
          if (selected_point(hold_points, name)) {
            if (barrier_dir.empty())
              std::_Exit(88);
            const auto entered = barrier_dir / (name + ".entered");
            const auto release = barrier_dir / (name + ".release");
            touch(entered);
            if (!wait_for_file(release, barrier_timeout)) {
              touch(barrier_dir / (name + ".timeout"),
                    "test barrier timed out; checkpoint not reached\n");
              std::_Exit(87);
            }
          }
        });
#endif
    executor.start();
    const auto external_control = setting("LASO_DISTRIBUTED_TEST_EXTERNAL_CONTROL") == "1";
    const auto stop_marker = setting("LASO_DISTRIBUTED_TEST_STOP_MARKER");
    const auto stop_file = std::filesystem::path(stop_marker);
    for (unsigned attempt = 0; external_control || attempt < 1200; ++attempt) {
      bool terminal_state = false;
      bool successful = false;
      if (session_mode) {
        const auto state =
            service.get(RecordKind::SessionTurn, turn_id).value("state", std::string{});
        terminal_state = state == "succeeded" || state == "failed" || state == "cancelled";
        successful = state == "succeeded";
      } else {
        const auto run = service.get(RecordKind::Run, run_id).get<Run>();
        terminal_state = laso::terminal(run.state);
        successful = run.state == RunState::Completed;
      }
      if (terminal_state && !external_control) {
        service.shutdown();
        executor.join();
        return successful ? 0 : 1;
      }
      if (external_control && std::filesystem::exists(stop_file)) {
        if (!terminal_state) {
          touch(stop_file.string() + ".unsafe",
                "refusing graceful fixture teardown before terminal state\n");
          std::_Exit(87);
        }
        service.shutdown();
        executor.join();
        return successful ? 0 : 1;
      }
      std::this_thread::sleep_for(Milliseconds{10});
    }
    touch(setting("LASO_DISTRIBUTED_TEST_TIMEOUT_MARKER",
                  (config.data_dir / "fixture-timeout").string()),
          "fixture timed out; exiting without graceful cancellation\n");
    std::_Exit(124);
  } catch (const std::exception &error) {
    if (const auto *path = required("LASO_DISTRIBUTED_TEST_ERROR_FILE")) {
      std::string message = error.what();
      for (const auto *name :
           {"LASO_DISTRIBUTED_TEST_DSN", "LASO_DISTRIBUTED_TEST_PROVIDER_CONTINUATION",
            "LASO_DISTRIBUTED_TEST_EXPECTED_CONTINUATION"}) {
        if (const auto *secret = required(name)) {
          std::size_t offset = 0;
          const std::string value = secret;
          while ((offset = message.find(value, offset)) != std::string::npos) {
            message.replace(offset, value.size(), "<redacted>");
            offset += sizeof("<redacted>") - 1;
          }
        }
      }
      touch(path, message + "\n");
    }
    return 3;
  } catch (...) {
    if (const auto *path = required("LASO_DISTRIBUTED_TEST_ERROR_FILE"))
      touch(path, "unknown fixture exception\n");
    return 3;
  }
}
