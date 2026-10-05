#include "../support.hpp"
#include <condition_variable>
#include <fstream>
#include <laso/api/api.hpp>
#include <laso/application/event_ingress.hpp>
#include <laso/plugins/loader.hpp>
#include <thread>

using namespace laso;
using namespace laso::test;

namespace {
class BlockingSubscriber final : public EventSubscriber {
public:
  void receive(const Event &) override {
    std::unique_lock lock(mutex);
    entered = true;
    ready.notify_all();
    release.wait(lock, [this] { return released; });
  }
  void wait_until_entered() {
    std::unique_lock lock(mutex);
    ready.wait(lock, [this] { return entered; });
  }
  void allow() {
    std::lock_guard lock(mutex);
    released = true;
    release.notify_all();
  }

private:
  std::mutex mutex;
  std::condition_variable ready, release;
  bool entered = false, released = false;
};
std::string source_pipeline() {
  return R"(laso: "1"
name: ingress-test
version: 1
nodes:
  copy:
    type: function
    function: identity
edges:
  - {from: input, to: copy}
  - {from: copy, to: output}
)";
}
} // namespace

TEST(EventSources, PluginLoadsAndReportsHealth) {
  ToolRegistry tools;
  ProviderRegistry providers;
  PluginLoader loader(tools, providers,
                      [](const auto &, const auto &, const auto &, const auto &, const auto &) {
                        return IngressResult{IngressStatus::Accepted, "event-1", "accepted"};
                      });
  loader.discover({LASO_EVENT_PLUGIN_DIR},
                  {{"offline", EventSourceConfig{"example-event-source", "example-event", "", false,
                                                 Json::object()}}});
  ASSERT_EQ(loader.event_sources().size(), 1U);
  EXPECT_EQ(loader.event_source("offline").at("status"), "disabled");
  EXPECT_EQ(loader.event_source("offline").at("capabilities").at(0), "deterministic");
  loader.start_event_sources();
  EXPECT_EQ(loader.event_source("offline").at("status"), "disabled");
  loader.set_event_source_enabled("offline", true);
  EXPECT_EQ(loader.event_source("offline").at("status"), "healthy");
  EXPECT_EQ(loader.event_source("offline").at("accepted"), 1U);
  loader.stop_event_sources();
  EXPECT_EQ(loader.event_source("offline").at("status"), "stopped");
}

TEST(EventSources, LifecycleFailuresAreIsolated) {
  ToolRegistry tools;
  ProviderRegistry providers;
  PluginLoader start_failure(tools, providers);
  start_failure.discover({LASO_EVENT_BAD_PLUGIN_DIR},
                         {{"failing", EventSourceConfig{"lifecycle-failure-source",
                                                        "lifecycle-failure",
                                                        "",
                                                        true,
                                                        {{"start_failure", true}}}}});
  start_failure.start_event_sources();
  EXPECT_EQ(start_failure.event_source("failing").at("status"), "failed");

  PluginLoader stop_failure(tools, providers);
  stop_failure.discover(
      {LASO_EVENT_BAD_PLUGIN_DIR},
      {{"failing-stop",
        EventSourceConfig{
            "lifecycle-failure-source", "lifecycle-failure", "", true, {{"stop_failure", true}}}}});
  stop_failure.start_event_sources();
  EXPECT_EQ(stop_failure.event_source("failing-stop").at("status"), "healthy");
  stop_failure.stop_event_sources();
  EXPECT_EQ(stop_failure.event_source("failing-stop").at("status"), "failed");
}

TEST(EventSources, EventReachesDurableTriggerAndPipeline) {
  TemporaryDirectory dir;
  std::ofstream(dir.path / "event.schema.json")
      << R"({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}})";
  asio::io_context io;
  Config c = config(dir.path);
  c.plugin_dirs = {LASO_EVENT_PLUGIN_DIR};
  c.schema_roots = {dir.path};
  c.event_sources.emplace("offline", EventSourceConfig{"example-event-source", "example-event",
                                                       "event.schema.json", false, Json::object()});
  Service service(io, c);
  service.register_pipeline(source_pipeline());
  service.create_trigger({{"name", "offline-trigger"},
                          {"pipeline", "ingress-test@1"},
                          {"event", "example.item.created"},
                          {"match", {{"source", "offline-example"}}}});
  service.set_event_source_enabled("offline", true);
  io.run();
  const auto events = service.list(RecordKind::Event);
  const auto found = std::find_if(events.begin(), events.end(), [](const Json &event) {
    return event.value("source_id", std::string{}) == "offline" &&
           event.value("external_event_id", std::string{}) == "example-1";
  });
  ASSERT_NE(found, events.end());
  const auto runs = service.list(RecordKind::Run);
  ASSERT_EQ(runs.size(), 1U);
  EXPECT_EQ(runs.front().at("initiation_type"), "event");
  EXPECT_EQ(runs.front().at("pipeline_version"), 1U);
  EXPECT_EQ(runs.front().at("event_id"), found->at("id"));
}

TEST(EventSources, DuplicateExternalEventSurvivesRestart) {
  TemporaryDirectory dir;
  SchemaValidator schemas({dir.path});
  auto storage = make_storage(dir.path / "state.db");
  InProcessEventBus events;
  EventIngress ingress(*storage, events, schemas);
  const auto event = R"({"type":"example.created","external_id":"same","payload":{"x":1}})";
  EXPECT_EQ(ingress.submit("source", "plugin", "component", "", event).status,
            IngressStatus::Accepted);
  EXPECT_EQ(ingress.submit("source", "plugin", "component", "", event).status,
            IngressStatus::Duplicate);
  ingress.stop();
  storage.reset();
  auto reopened = make_storage(dir.path / "state.db");
  InProcessEventBus reopened_events;
  EventIngress reopened_ingress(*reopened, reopened_events, schemas);
  const auto result = reopened_ingress.submit("source", "plugin", "component", "", event);
  EXPECT_EQ(result.status, IngressStatus::Duplicate);
  EXPECT_EQ(reopened->list(RecordKind::Event).size(), 1U);
}

TEST(EventSources, EnabledStateSurvivesServiceRestart) {
  TemporaryDirectory dir;
  Config c = config(dir.path);
  c.plugin_dirs = {LASO_EVENT_PLUGIN_DIR};
  c.event_sources.emplace("offline", EventSourceConfig{"example-event-source", "example-event", "",
                                                       false, Json::object()});
  {
    asio::io_context io;
    Service service(io, c);
    service.set_event_source_enabled("offline", true);
    EXPECT_TRUE(service.event_source("offline").at("enabled"));
  }
  {
    asio::io_context io;
    Service service(io, c);
    const auto source = service.event_source("offline");
    EXPECT_TRUE(source.at("enabled"));
    EXPECT_EQ(source.at("status"), "healthy");
    EXPECT_EQ(service.list(RecordKind::Event).size(), 1U);
  }
}

TEST(EventSources, InvalidEventSchemaIsRejected) {
  TemporaryDirectory dir;
  const auto schema = dir.path / "event.schema.json";
  std::ofstream(schema)
      << R"({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}})";
  SchemaValidator schemas({dir.path});
  auto storage = make_storage(dir.path / "state.db");
  InProcessEventBus events;
  EventIngress ingress(*storage, events, schemas);
  EXPECT_EQ(ingress
                .submit("source", "plugin", "component", "event.schema.json",
                        R"({"type":"example","payload":{"value":"wrong"}})")
                .status,
            IngressStatus::Rejected);
  EXPECT_EQ(storage->list(RecordKind::Event).size(), 0U);
  EXPECT_EQ(ingress
                .submit("source", "plugin", "component", "event.schema.json",
                        R"({"type":"example","payload":{"value":1}})")
                .status,
            IngressStatus::Accepted);
  ingress.stop();
}

TEST(EventSources, DeclaredSchemaMustBePresentAndValidAtStartup) {
  TemporaryDirectory dir;
  Config missing = config(dir.path);
  missing.plugin_dirs = {LASO_EVENT_PLUGIN_DIR};
  missing.schema_roots = {dir.path};
  missing.event_sources.emplace("offline",
                                EventSourceConfig{"example-event-source", "example-event",
                                                  "missing.json", false, Json::object()});
  asio::io_context missing_io;
  EXPECT_THROW(Service(missing_io, missing), Error);

  std::ofstream(dir.path / "malformed.json") << "{not-json";
  Config malformed = missing;
  malformed.event_sources.at("offline").schema = "malformed.json";
  asio::io_context malformed_io;
  EXPECT_THROW(Service(malformed_io, malformed), Error);
}

TEST(EventSources, IngressBackpressureAndShutdownAreBounded) {
  TemporaryDirectory dir;
  SchemaValidator schemas({dir.path});
  auto storage = make_storage(dir.path / "state.db");
  InProcessEventBus events;
  auto blocker = std::make_shared<BlockingSubscriber>();
  events.subscribe(blocker);
  EventIngress ingress(*storage, events, schemas, 4, 1, 1);
  IngressResult first;
  std::jthread sender([&] {
    first = ingress.submit("source", "plugin", "component", "",
                           R"({"type":"example.created","payload":{}})");
  });
  blocker->wait_until_entered();
  EXPECT_EQ(
      ingress
          .submit("source", "plugin", "component", "", R"({"type":"example.created","payload":{}})")
          .status,
      IngressStatus::Backpressured);
  blocker->allow();
  sender.join();
  EXPECT_EQ(first.status, IngressStatus::Accepted);
  ingress.stop();
  EXPECT_EQ(
      ingress
          .submit("source", "plugin", "component", "", R"({"type":"example.created","payload":{}})")
          .status,
      IngressStatus::Stopped);
}

TEST(Api, EventSourceInspectionAndDisable) {
  TemporaryDirectory dir;
  asio::io_context io;
  Config c = config(dir.path);
  c.plugin_dirs = {LASO_EVENT_PLUGIN_DIR};
  c.event_sources.emplace("offline", EventSourceConfig{"example-event-source", "example-event", "",
                                                       false, Json::object()});
  Service service(io, c);
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  EXPECT_EQ(api.handle("GET", "/api/v1/event-sources", "").status, 200U);
  EXPECT_EQ(api.handle("GET", "/api/v1/event-sources/offline", "").status, 200U);
  EXPECT_EQ(api.handle("POST", "/api/v1/event-sources/offline/enable", "").status, 202U);
  EXPECT_EQ(api.handle("POST", "/api/v1/event-sources/offline/disable", "").status, 202U);
}
