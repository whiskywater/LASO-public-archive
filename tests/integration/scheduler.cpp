#include "../support.hpp"
#include <laso/api/api.hpp>
#include <laso/scheduler/scheduler.hpp>

using namespace laso;
using namespace laso::test;

namespace {
ScheduleDefinition schedule(const std::string &id, const std::string &type,
                            const std::string &due) {
  ScheduleDefinition result;
  result.id = id;
  result.name = id;
  result.pipeline_id = "pipeline";
  result.type = type;
  result.at = type == "one_time" ? due : "";
  result.interval_ms = type == "interval" ? "1000" : "";
  result.cron = type == "cron" ? "*/5 * * * *" : "";
  result.next_due_at = due;
  result.created_at = due;
  result.updated_at = due;
  return result;
}
} // namespace

TEST(Scheduler, DurableTimeTypesAndPinnedOrigin) {
  for_each_storage_backend([](const auto &backend) {
    TemporaryDirectory dir;
    auto storage = backend.open(dir.path / "state.db");
    const auto initial = parse_utc_timestamp("2026-01-01T00:00:00.000Z");
    auto clock = std::make_shared<TestClock>(initial);
    std::vector<LaunchRequest> launches;
    asio::io_context io;
    LocalScheduler scheduler(
        io, *storage,
        [&](const LaunchRequest &request) {
          launches.push_back(request);
          return "run-" + std::to_string(launches.size());
        },
        {}, clock);

    scheduler.create_schedule(schedule("once", "one_time", "2026-01-01T00:00:00.000Z"));
    scheduler.process_due();
    ASSERT_EQ(launches.size(), 1U);
    EXPECT_EQ(launches.front().origin.at("initiation_type"), "schedule");
    EXPECT_EQ(launches.front().origin.at("schedule_occurrence_id"),
              "once|2026-01-01T00:00:00.000Z");

    auto interval = schedule("interval", "interval", "2026-01-01T00:00:00.000Z");
    scheduler.create_schedule(interval);
    scheduler.process_due();
    ASSERT_EQ(launches.size(), 2U);
    clock->advance(Milliseconds{1000});
    scheduler.process_due();
    EXPECT_EQ(launches.size(), 3U);

    auto cron = schedule("cron", "cron", "2026-01-01T00:05:00.000Z");
    scheduler.create_schedule(cron);
    clock->set(parse_utc_timestamp("2026-01-01T00:05:00.000Z"));
    scheduler.process_due();
    EXPECT_EQ(launches.size(), 4U);
  });
}

TEST(Scheduler, MisfireOverlapAndOccurrenceDeduplication) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  const auto initial = parse_utc_timestamp("2026-01-01T01:00:00.000Z");
  auto clock = std::make_shared<TestClock>(initial);
  unsigned launches = 0;
  asio::io_context io;
  LocalScheduler scheduler(
      io, *storage, [&](const LaunchRequest &) { return "run-" + std::to_string(++launches); }, {},
      clock);

  auto skip = schedule("skip", "one_time", "2026-01-01T00:00:00.000Z");
  skip.misfire_policy = "SKIP";
  scheduler.create_schedule(skip);
  scheduler.process_due();
  EXPECT_EQ(launches, 0U);
  EXPECT_EQ(
      storage->get(RecordKind::ScheduleOccurrence, "skip|2026-01-01T00:00:00.000Z").at("status"),
      "skipped");

  auto run_once = schedule("catchup", "one_time", "2026-01-01T00:00:00.000Z");
  run_once.misfire_policy = "RUN_ONCE";
  scheduler.create_schedule(run_once);
  scheduler.process_due();
  EXPECT_EQ(launches, 1U);
  scheduler.process_due();
  EXPECT_EQ(launches, 1U);

  auto active = schedule("active", "one_time", "2026-01-01T01:00:00.000Z");
  active.overlap_policy = "SKIP";
  laso::Run active_run;
  active_run.id = "active-run";
  active_run.pipeline_id = "pipeline";
  active_run.schedule_id = "active";
  active_run.state = RunState::Running;
  storage->commit({{RecordKind::Run, active_run.id, active_run.id, Json(active_run)}});
  scheduler.create_schedule(active);
  scheduler.process_due();
  EXPECT_EQ(launches, 1U);
  EXPECT_EQ(
      storage->get(RecordKind::ScheduleOccurrence, "active|2026-01-01T01:00:00.000Z").at("status"),
      "skipped");

  auto queue = schedule("queue", "one_time", "2026-01-01T01:00:00.000Z");
  queue.overlap_policy = "QUEUE_ONE";
  laso::Run queue_run = active_run;
  queue_run.id = "queue-run";
  queue_run.schedule_id = "queue";
  storage->commit({{RecordKind::Run, queue_run.id, queue_run.id, Json(queue_run)}});
  scheduler.create_schedule(queue);
  scheduler.process_due();
  EXPECT_EQ(launches, 1U);
  auto finished = queue_run;
  finished.state = RunState::Completed;
  storage->commit({{RecordKind::Run, finished.id, finished.id, Json(finished)}});
  scheduler.process_due();
  EXPECT_EQ(launches, 2U);
}

TEST(Scheduler, EventTriggersAreDurableMatchedAndDeduplicated) {
  for_each_storage_backend([](const auto &backend) {
    TemporaryDirectory dir;
    auto storage = backend.open(dir.path / "state.db");
    auto clock = std::make_shared<TestClock>(parse_utc_timestamp("2026-01-01T00:00:00.000Z"));
    std::vector<LaunchRequest> launches;
    asio::io_context io;
    LocalScheduler scheduler(
        io, *storage,
        [&](const LaunchRequest &request) {
          launches.push_back(request);
          return "event-run";
        },
        {}, clock, 128, 2);
    TriggerDefinition trigger;
    trigger.id = "trigger-1";
    trigger.name = "artifact trigger";
    trigger.pipeline_id = "pipeline";
    trigger.event_type = "artifact.created";
    trigger.match = {{"source", "import"}};
    trigger.created_at = "2025-12-31T23:59:00.000Z";
    scheduler.create_trigger(trigger);
    scheduler.start();
    Event event;
    event.id = "event-1";
    event.type = "artifact.created";
    event.time = "2026-01-01T00:00:00.000Z";
    event.metadata = {{"source", "import"}};
    auto subscriber = scheduler.event_subscriber();
    subscriber->receive(event);
    subscriber->receive(event);
    io.run();
    ASSERT_EQ(launches.size(), 1U);
    EXPECT_EQ(launches.front().input.at("event").at("id"), "event-1");
    EXPECT_EQ(storage->get(RecordKind::TriggerDelivery, "trigger-1|event-1").at("status"),
              "started");

    Event unrelated = event;
    unrelated.id = "event-2";
    unrelated.metadata["source"] = "other";
    subscriber->receive(unrelated);
    io.restart();
    io.run();
    EXPECT_EQ(launches.size(), 1U);
  });
}

TEST(Scheduler, EventTriggerDepthIsBounded) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  auto clock = std::make_shared<TestClock>(parse_utc_timestamp("2026-01-01T00:00:00.000Z"));
  asio::io_context io;
  LocalScheduler scheduler(
      io, *storage, [&](const LaunchRequest &) { return "run"; }, {}, clock, 128, 1);
  TriggerDefinition trigger;
  trigger.id = "depth-trigger";
  trigger.name = "depth";
  trigger.pipeline_id = "pipeline";
  trigger.event_type = "loop";
  trigger.created_at = "2025-12-31T23:59:00.000Z";
  scheduler.create_trigger(trigger);
  scheduler.start();
  Event event;
  event.id = "deep-event";
  event.type = "loop";
  event.time = "2026-01-01T00:00:00.000Z";
  event.trigger_depth = 1;
  scheduler.event_subscriber()->receive(event);
  io.run();
  EXPECT_EQ(storage->list(RecordKind::TriggerDelivery).size(), 0U);
}

TEST(Scheduler, ServiceLaunchUsesNormalRuntimeAndPersistsOrigin) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  const auto yaml = single();
  service.register_pipeline(yaml);
  const auto now = parse_utc_timestamp(timestamp());
  const auto due = format_utc_timestamp(now - Milliseconds{2000});
  auto schedule_record = service.create_schedule({{"name", "normal-run"},
                                                  {"pipeline", "test@1"},
                                                  {"type", "one_time"},
                                                  {"at", due},
                                                  {"misfire_policy", "RUN_ONCE"}});
  dynamic_cast<LocalScheduler &>(service.scheduler()).process_due();
  io.restart();
  io.run();
  const auto runs = service.list(RecordKind::Run);
  ASSERT_EQ(runs.size(), 1U);
  const auto run = runs.front().get<laso::Run>();
  EXPECT_EQ(run.state, RunState::Completed);
  EXPECT_EQ(run.initiation_type, "schedule");
  EXPECT_EQ(run.schedule_id, schedule_record.at("id").get<std::string>());
  EXPECT_EQ(run.pipeline_version, 1U);
}

TEST(Scheduler, ApiCrudForSchedulesAndTriggers) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  service.register_pipeline(single());
  LocalDevelopmentIdentity identity;
  Api api(service, identity);
  const auto due = format_utc_timestamp(parse_utc_timestamp(timestamp()) + Milliseconds{60000});
  const auto schedule_body = Json{{"name", "api-schedule"},
                                  {"pipeline", "test@1"},
                                  {"type", "one_time"},
                                  {"at", due},
                                  {"misfire_policy", "RUN_ONCE"}};
  auto response = api.handle("POST", "/api/v1/schedules", schedule_body.dump());
  ASSERT_EQ(response.status, 201U);
  const auto schedule_id = response.body.at("id").get<std::string>();
  EXPECT_EQ(api.handle("GET", "/api/v1/schedules/" + schedule_id, "").status, 200U);
  EXPECT_EQ(
      api.handle("PATCH", "/api/v1/schedules/" + schedule_id, R"({"overlap_policy":"QUEUE_ONE"})")
          .status,
      200U);
  EXPECT_EQ(api.handle("POST", "/api/v1/schedules/" + schedule_id + "/disable", "{}").status, 202U);
  EXPECT_EQ(api.handle("POST", "/api/v1/schedules/" + schedule_id + "/enable", "{}").status, 202U);
  const auto trigger_body = Json{{"name", "api-trigger"},
                                 {"pipeline", "test@1"},
                                 {"event", "artifact.created"},
                                 {"match", Json{{"source", "api"}}}};
  response = api.handle("POST", "/api/v1/triggers", trigger_body.dump());
  ASSERT_EQ(response.status, 201U);
  const auto trigger_id = response.body.at("id").get<std::string>();
  EXPECT_EQ(api.handle("GET", "/api/v1/triggers", "").status, 200U);
  EXPECT_EQ(api.handle("DELETE", "/api/v1/triggers/" + trigger_id, "{}").status, 202U);
  EXPECT_EQ(api.handle("DELETE", "/api/v1/schedules/" + schedule_id, "{}").status, 202U);
}

TEST(Scheduler, ScheduleAndTriggerRecoveryAfterRestart) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  const auto initial = parse_utc_timestamp("2026-01-01T00:00:00.000Z");
  auto clock = std::make_shared<TestClock>(initial);
  asio::io_context io;
  unsigned launches = 0;
  auto durable = schedule("restart", "one_time", "2026-01-01T00:00:00.000Z");
  durable.misfire_policy = "RUN_ONCE";
  {
    LocalScheduler first(
        io, *storage, [&](const LaunchRequest &) { return "run-" + std::to_string(++launches); },
        {}, clock);
    first.create_schedule(durable);
  }
  {
    LocalScheduler second(
        io, *storage, [&](const LaunchRequest &) { return "run-" + std::to_string(++launches); },
        {}, clock);
    second.process_due();
    second.process_due();
  }
  EXPECT_EQ(launches, 1U);

  TriggerDefinition trigger;
  trigger.id = "restart-trigger";
  trigger.name = "restart trigger";
  trigger.pipeline_id = "pipeline";
  trigger.event_type = "restart.event";
  trigger.created_at = "2025-12-31T23:59:00.000Z";
  storage->commit({{RecordKind::Trigger, trigger.id, "", Json(trigger)}});
  Event event;
  event.id = "restart-event";
  event.type = trigger.event_type;
  event.time = "2026-01-01T00:00:00.000Z";
  storage->commit({{RecordKind::Event, event.id, "", Json(event)}});
  {
    LocalScheduler recovered(
        io, *storage, [&](const LaunchRequest &) { return "run-" + std::to_string(++launches); },
        {}, clock);
    recovered.start();
    io.run();
  }
  EXPECT_EQ(launches, 2U);
  EXPECT_EQ(storage->get(RecordKind::TriggerDelivery, "restart-trigger|restart-event").at("status"),
            "started");
}

TEST(Scheduler, CapacityLeavesBoundedDurablePendingWork) {
  TemporaryDirectory dir;
  auto storage = make_storage(dir.path / "state.db");
  auto clock = std::make_shared<TestClock>(parse_utc_timestamp("2026-01-01T00:00:00.000Z"));
  bool capacity = true;
  unsigned launches = 0;
  asio::io_context io;
  LocalScheduler scheduler(
      io, *storage,
      [&](const LaunchRequest &) -> std::string {
        if (capacity)
          throw Error(ErrorCode::Capacity, "full");
        ++launches;
        return "run";
      },
      {}, clock);
  scheduler.create_schedule(schedule("capacity", "one_time", "2026-01-01T00:00:00.000Z"));
  scheduler.process_due();
  EXPECT_EQ(
      storage->get(RecordKind::ScheduleOccurrence, "capacity|2026-01-01T00:00:00.000Z")["status"],
      "pending");
  capacity = false;
  scheduler.process_due();
  EXPECT_EQ(launches, 1U);
}
