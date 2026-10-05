#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include <laso/core/async.hpp>
#include <laso/events/events.hpp>
#include <laso/storage/storage.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace laso {
using WallTime = std::chrono::system_clock::time_point;

class Clock {
public:
  virtual ~Clock() = default;
  virtual WallTime now() const = 0;
};
class SystemClock final : public Clock {
public:
  WallTime now() const override;
};
class TestClock final : public Clock {
public:
  explicit TestClock(WallTime initial);
  WallTime now() const override;
  void set(WallTime value);
  void advance(Milliseconds amount);

private:
  mutable std::mutex mutex_;
  WallTime now_;
};
WallTime parse_utc_timestamp(const std::string &value);
std::string format_utc_timestamp(WallTime value);

struct ScheduleDefinition {
  std::string id = uuid(), name, pipeline_id;
  unsigned pipeline_version = 1;
  std::string type, at, interval_ms, cron;
  Json input = Json::object();
  bool enabled = true, deleted = false, queued = false;
  std::string created_at = timestamp(), updated_at = created_at, next_due_at, last_due_at,
              last_started_at, last_completed_run_id, queued_due_at;
  std::string misfire_policy = "SKIP", overlap_policy = "ALLOW";
  unsigned missed_count = 0;
};
void to_json(Json &, const ScheduleDefinition &);
void from_json(const Json &, ScheduleDefinition &);

struct TriggerDefinition {
  std::string id = uuid(), name, pipeline_id, event_type;
  unsigned pipeline_version = 1;
  Json match = Json::object();
  bool enabled = true, deleted = false;
  std::string created_at = timestamp(), updated_at = created_at;
};
void to_json(Json &, const TriggerDefinition &);
void from_json(const Json &, TriggerDefinition &);

ScheduleDefinition parse_schedule_spec(const Json &spec);
TriggerDefinition parse_trigger_spec(const Json &spec);
void validate_schedule(const ScheduleDefinition &schedule);
void validate_trigger(const TriggerDefinition &trigger);
std::string next_schedule_due(const ScheduleDefinition &schedule, const std::string &after);

struct LaunchRequest {
  std::string pipeline_id;
  unsigned pipeline_version = 1;
  Json input = Json::object();
  Json origin = Json::object();
};

// Legacy transient timers remain available for source compatibility.  The
// durable constructor below is the application scheduler used by Service.
struct ScheduledPipeline {
  std::string id = uuid(), pipeline_id;
  Milliseconds delay{0}, interval{0};
  unsigned max_firings = 1;
  Json input = Json::object();
};
class Scheduler {
public:
  virtual ~Scheduler() = default;
  virtual void schedule(ScheduledPipeline) = 0;
  virtual void stop() = 0;
};
class LocalScheduler final : public Scheduler {
public:
  LocalScheduler(asio::io_context &io, std::function<void(const ScheduledPipeline &)> dispatch);
  LocalScheduler(asio::io_context &, Storage &, std::function<std::string(const LaunchRequest &)>,
                 std::function<void(const Event &)> emit = {},
                 std::shared_ptr<Clock> clock = std::make_shared<SystemClock>(),
                 unsigned max_pending_launches = 128, unsigned max_trigger_depth = 16,
                 unsigned max_event_deliveries = 1024);
  ~LocalScheduler() noexcept override;
  void schedule(ScheduledPipeline) override;
  void stop() override;

  ScheduleDefinition create_schedule(ScheduleDefinition);
  ScheduleDefinition update_schedule(ScheduleDefinition);
  void set_schedule_enabled(const std::string &id, bool enabled);
  void delete_schedule(const std::string &id);
  std::shared_ptr<EventSubscriber> event_subscriber();
  TriggerDefinition create_trigger(TriggerDefinition);
  TriggerDefinition update_trigger(TriggerDefinition);
  void set_trigger_enabled(const std::string &id, bool enabled);
  void delete_trigger(const std::string &id);
  void start();
  // Synchronously evaluates due durable work.  This is also the deterministic
  // entry point used by scheduler tests with TestClock.
  void process_due();

private:
  struct Subscriber;
  asio::strand<asio::io_context::executor_type> strand_;
  std::function<void(const ScheduledPipeline &)> legacy_dispatch_;
  Storage *storage_ = nullptr;
  std::function<std::string(const LaunchRequest &)> dispatch_;
  std::function<void(const Event &)> emit_;
  std::shared_ptr<Clock> clock_;
  std::shared_ptr<asio::steady_timer> wake_timer_;
  unsigned max_pending_launches_ = 128, max_trigger_depth_ = 16, max_event_deliveries_ = 1024;
  std::mutex mutex_;
  std::atomic<bool> stopped_{false};
  std::atomic<bool> started_{false};
  std::atomic<std::size_t> pending_events_{0};
  std::map<std::string, std::shared_ptr<asio::steady_timer>> timers_;

  void wake();
  void arm();
  void replay_events();
  void replay_pending_deliveries();
  void receive_event(const Event &event);
  void handle_event(Event event);
  void process_trigger(const TriggerDefinition &, const Event &);
  void emit_event(const Event &event);
  bool active_schedule_run(const std::string &schedule_id) const;
};
} // namespace laso
