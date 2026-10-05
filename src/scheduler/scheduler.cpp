#include <algorithm>
#include <array>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <laso/core/config.hpp>
#include <laso/scheduler/scheduler.hpp>
#include <limits>
#include <regex>
#include <set>
#include <sstream>

namespace laso {
namespace {
constexpr auto max_schedule_input = std::size_t{1} * 1024 * 1024;
constexpr auto max_match_bytes = std::size_t{16} * 1024;
constexpr auto max_lateness = std::chrono::seconds{1};

std::vector<std::string> split(const std::string &value, char delimiter) {
  std::vector<std::string> result;
  std::size_t begin = 0;
  while (begin <= value.size()) {
    const auto end = value.find(delimiter, begin);
    result.push_back(value.substr(begin, end == std::string::npos ? end : end - begin));
    if (end == std::string::npos)
      break;
    begin = end + 1;
  }
  return result;
}

unsigned number(const std::string &value, unsigned minimum, unsigned maximum) {
  if (value.empty() || value.size() > 6 ||
      !std::all_of(value.begin(), value.end(),
                   [](const unsigned char character) { return std::isdigit(character) != 0; }))
    throw Error(ErrorCode::Validation, "Invalid numeric schedule field");
  try {
    const auto parsed = std::stoul(value);
    if (parsed < minimum || parsed > maximum)
      throw Error(ErrorCode::Validation, "Schedule field is outside its allowed range");
    return static_cast<unsigned>(parsed);
  } catch (const Error &) {
    throw;
  } catch (...) {
    throw Error(ErrorCode::Validation, "Invalid numeric schedule field");
  }
}

struct CronField {
  std::vector<bool> values;
  bool wildcard = false;
};

CronField cron_field(const std::string &text, unsigned minimum, unsigned maximum,
                     bool sunday_alias) {
  if (text.empty() || text.size() > 64)
    throw Error(ErrorCode::Validation, "Invalid cron field");
  CronField result{std::vector<bool>(maximum + 1, false), false};
  for (const auto &part : split(text, ',')) {
    if (part.empty())
      throw Error(ErrorCode::Validation, "Invalid cron list");
    auto pieces = split(part, '/');
    if (pieces.size() > 2 || pieces[0].empty() || (pieces.size() == 2 && pieces[1].empty()))
      throw Error(ErrorCode::Validation, "Invalid cron step");
    unsigned step = 1;
    if (pieces.size() == 2)
      step = number(pieces[1], 1, maximum - minimum + 1);
    unsigned first = minimum, last = maximum;
    if (pieces[0] != "*") {
      const auto range = split(pieces[0], '-');
      if (range.size() > 2 || range[0].empty())
        throw Error(ErrorCode::Validation, "Invalid cron range");
      first = number(range[0], minimum, maximum);
      last = range.size() == 1 ? first : number(range[1], minimum, maximum);
      if (first > last)
        throw Error(ErrorCode::Validation, "Cron range is reversed");
    } else {
      result.wildcard = true;
    }
    for (unsigned value = first; value <= last; value += step) {
      auto normalized = value;
      if (sunday_alias && normalized == 7)
        normalized = 0;
      result.values[normalized] = true;
      if (last - value < step)
        break;
    }
  }
  return result;
}

struct Cron {
  CronField minute, hour, day, month, weekday;
};

Cron parse_cron(const std::string &expression) {
  const auto fields = split(expression, ' ');
  std::vector<std::string> nonempty;
  for (const auto &field : fields)
    if (!field.empty())
      nonempty.push_back(field);
  if (nonempty.size() != 5)
    throw Error(ErrorCode::Validation, "Cron must use exactly five UTC fields");
  return {cron_field(nonempty[0], 0, 59, false), cron_field(nonempty[1], 0, 23, false),
          cron_field(nonempty[2], 1, 31, false), cron_field(nonempty[3], 1, 12, false),
          cron_field(nonempty[4], 0, 7, true)};
}

bool cron_match(const Cron &cron, const std::tm &tm) {
  if (!cron.minute.values[static_cast<unsigned>(tm.tm_min)] ||
      !cron.hour.values[static_cast<unsigned>(tm.tm_hour)] ||
      !cron.month.values[static_cast<unsigned>(tm.tm_mon + 1)])
    return false;
  const bool day_match = cron.day.values[static_cast<unsigned>(tm.tm_mday)];
  const bool weekday_match = cron.weekday.values[static_cast<unsigned>(tm.tm_wday)];
  if (!cron.day.wildcard && !cron.weekday.wildcard)
    return day_match || weekday_match;
  return day_match && weekday_match;
}

std::string cron_next(const std::string &expression, const std::string &after) {
  const auto cron = parse_cron(expression);
  auto candidate = std::chrono::time_point_cast<std::chrono::minutes>(parse_utc_timestamp(after));
  candidate += std::chrono::minutes{1};
  constexpr std::size_t search_limit = static_cast<std::size_t>(5) * 366U * 24U * 60U;
  for (std::size_t i = 0; i < search_limit; ++i, candidate += std::chrono::minutes{1}) {
    const auto time = std::chrono::system_clock::to_time_t(candidate);
    std::tm utc{};
    if (gmtime_r(&time, &utc) == nullptr)
      throw Error(ErrorCode::Validation, "Cannot evaluate cron timestamp");
    if (cron_match(cron, utc))
      return format_utc_timestamp(candidate);
  }
  throw Error(ErrorCode::Validation, "Cron expression has no occurrence in its search bound");
}

std::int64_t interval_value(const ScheduleDefinition &schedule) {
  if (schedule.interval_ms.empty())
    throw Error(ErrorCode::Validation, "Interval schedule requires interval_ms");
  try {
    std::size_t end = 0;
    const auto value = std::stoll(schedule.interval_ms, &end);
    constexpr auto max_interval = std::int64_t{365} * 24 * 60 * 60 * 1000;
    if (end != schedule.interval_ms.size() || value <= 0 || value > max_interval)
      throw std::out_of_range("interval");
    return value;
  } catch (...) {
    throw Error(ErrorCode::Validation, "Interval must be a positive bounded millisecond duration");
  }
}

std::string pipeline_id_from_spec(const Json &spec) {
  auto result = spec.value("pipeline", spec.value("pipeline_id", std::string{}));
  if (result.empty())
    throw Error(ErrorCode::Validation, "Trigger or schedule pipeline is required");
  return result;
}

bool is_due(const std::string &due, WallTime now) {
  return !due.empty() && parse_utc_timestamp(due) <= now;
}

bool is_late(const std::string &due, WallTime now) {
  return due.empty() || now - parse_utc_timestamp(due) > max_lateness;
}

Json record_value(const std::string &status, const ScheduleDefinition &schedule,
                  const std::string &due, const std::string &run_id = {}) {
  return {{"id", schedule.id + "|" + due},
          {"schedule_id", schedule.id},
          {"due_at", due},
          {"status", status},
          {"run_id", run_id},
          {"updated_at", schedule.updated_at}};
}

Event scheduler_event(const std::string &type, const ScheduleDefinition &schedule,
                      const std::string &due = {}, const Json &extra = Json::object()) {
  Event event;
  event.type = type;
  event.pipeline_id = schedule.pipeline_id;
  event.metadata = {{"schedule_id", schedule.id},
                    {"pipeline_version", schedule.pipeline_version},
                    {"due_at", due}};
  for (const auto &[key, value] : extra.items())
    event.metadata[key] = value;
  return event;
}

Event trigger_event(const std::string &type, const TriggerDefinition &trigger, const Event &source,
                    const Json &extra = Json::object()) {
  Event event;
  event.type = type;
  event.pipeline_id = trigger.pipeline_id;
  if (!source.id.empty()) {
    event.causation_id = source.id;
    event.root_event_id = source.root_event_id.empty() ? source.id : source.root_event_id;
    event.trigger_depth = source.trigger_depth;
  }
  event.metadata = {
      {"trigger_id", trigger.id},
      {"event_id", source.id},
      {"root_event_id", source.root_event_id.empty() ? source.id : source.root_event_id}};
  for (const auto &[key, value] : extra.items())
    event.metadata[key] = value;
  return event;
}

} // namespace

WallTime SystemClock::now() const {
  return std::chrono::system_clock::now();
}
TestClock::TestClock(WallTime initial) : now_(initial) {}
WallTime TestClock::now() const {
  std::lock_guard lock(mutex_);
  return now_;
}
void TestClock::set(WallTime value) {
  std::lock_guard lock(mutex_);
  now_ = value;
}
void TestClock::advance(Milliseconds amount) {
  if (amount < Milliseconds{0})
    throw Error(ErrorCode::Validation, "Test clock cannot move backwards");
  std::lock_guard lock(mutex_);
  now_ += amount;
}

WallTime parse_utc_timestamp(const std::string &value) {
  static const std::regex pattern("^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):"
                                  "([0-9]{2})\\.([0-9]{3})Z$");
  std::smatch match;
  if (!std::regex_match(value, match, pattern))
    throw Error(ErrorCode::Validation, "Timestamp must be an absolute UTC ISO-8601 value");
  std::tm utc{};
  try {
    utc.tm_year = std::stoi(match[1]) - 1900;
    utc.tm_mon = std::stoi(match[2]) - 1;
    utc.tm_mday = std::stoi(match[3]);
    utc.tm_hour = std::stoi(match[4]);
    utc.tm_min = std::stoi(match[5]);
    utc.tm_sec = std::stoi(match[6]);
    const auto milliseconds = std::stoi(match[7]);
    const auto seconds = timegm(&utc);
    if (seconds < 0)
      throw std::out_of_range("timestamp");
    std::tm roundtrip{};
    if (gmtime_r(&seconds, &roundtrip) == nullptr || roundtrip.tm_year != utc.tm_year ||
        roundtrip.tm_mon != utc.tm_mon || roundtrip.tm_mday != utc.tm_mday ||
        roundtrip.tm_hour != utc.tm_hour || roundtrip.tm_min != utc.tm_min ||
        roundtrip.tm_sec != utc.tm_sec)
      throw std::out_of_range("timestamp");
    return std::chrono::system_clock::from_time_t(seconds) + Milliseconds{milliseconds};
  } catch (...) {
    throw Error(ErrorCode::Validation, "Invalid UTC timestamp");
  }
}

std::string format_utc_timestamp(WallTime value) {
  const auto milliseconds =
      std::chrono::duration_cast<Milliseconds>(value.time_since_epoch()).count();
  const auto seconds = milliseconds / 1000;
  auto remainder = milliseconds % 1000;
  if (remainder < 0)
    remainder += 1000;
  const auto time = static_cast<std::time_t>(seconds);
  std::tm utc{};
  if (gmtime_r(&time, &utc) == nullptr)
    throw Error(ErrorCode::Validation, "Cannot format UTC timestamp");
  std::ostringstream out;
  out << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(3) << std::setfill('0')
      << remainder << 'Z';
  return out.str();
}

void to_json(Json &j, const ScheduleDefinition &s) {
  j = {{"id", s.id},
       {"name", s.name},
       {"pipeline_id", s.pipeline_id},
       {"pipeline_version", s.pipeline_version},
       {"type", s.type},
       {"at", s.at},
       {"interval_ms", s.interval_ms},
       {"cron", s.cron},
       {"input", s.input},
       {"enabled", s.enabled},
       {"deleted", s.deleted},
       {"queued", s.queued},
       {"created_at", s.created_at},
       {"updated_at", s.updated_at},
       {"next_due_at", s.next_due_at},
       {"last_due_at", s.last_due_at},
       {"last_started_at", s.last_started_at},
       {"last_completed_run_id", s.last_completed_run_id},
       {"queued_due_at", s.queued_due_at},
       {"misfire_policy", s.misfire_policy},
       {"overlap_policy", s.overlap_policy},
       {"missed_count", s.missed_count}};
}
void from_json(const Json &j, ScheduleDefinition &s) {
  s.id = j.value("id", uuid());
  s.name = j.value("name", std::string{});
  s.pipeline_id = j.value("pipeline_id", std::string{});
  s.pipeline_version = j.value("pipeline_version", 1U);
  s.type = j.value("type", std::string{});
  s.at = j.value("at", std::string{});
  s.interval_ms = j.value("interval_ms", std::string{});
  s.cron = j.value("cron", std::string{});
  s.input = j.value("input", Json::object());
  s.enabled = j.value("enabled", true);
  s.deleted = j.value("deleted", false);
  s.queued = j.value("queued", false);
  s.created_at = j.value("created_at", timestamp());
  s.updated_at = j.value("updated_at", s.created_at);
  s.next_due_at = j.value("next_due_at", std::string{});
  s.last_due_at = j.value("last_due_at", std::string{});
  s.last_started_at = j.value("last_started_at", std::string{});
  s.last_completed_run_id = j.value("last_completed_run_id", std::string{});
  s.queued_due_at = j.value("queued_due_at", std::string{});
  s.misfire_policy = j.value("misfire_policy", std::string{"SKIP"});
  s.overlap_policy = j.value("overlap_policy", std::string{"ALLOW"});
  s.missed_count = j.value("missed_count", 0U);
}
void to_json(Json &j, const TriggerDefinition &t) {
  j = {{"id", t.id},
       {"name", t.name},
       {"pipeline_id", t.pipeline_id},
       {"pipeline_version", t.pipeline_version},
       {"event_type", t.event_type},
       {"match", t.match},
       {"enabled", t.enabled},
       {"deleted", t.deleted},
       {"created_at", t.created_at},
       {"updated_at", t.updated_at}};
}
void from_json(const Json &j, TriggerDefinition &t) {
  t.id = j.value("id", uuid());
  t.name = j.value("name", std::string{});
  t.pipeline_id = j.value("pipeline_id", std::string{});
  t.pipeline_version = j.value("pipeline_version", 1U);
  t.event_type = j.value("event_type", std::string{});
  t.match = j.value("match", Json::object());
  t.enabled = j.value("enabled", true);
  t.deleted = j.value("deleted", false);
  t.created_at = j.value("created_at", timestamp());
  t.updated_at = j.value("updated_at", t.created_at);
}

ScheduleDefinition parse_schedule_spec(const Json &spec) {
  if (!spec.is_object())
    throw Error(ErrorCode::Validation, "Schedule must be a JSON object");
  ScheduleDefinition s;
  s.id = spec.value("id", uuid());
  s.name = spec.value("name", std::string{});
  s.pipeline_id = pipeline_id_from_spec(spec);
  s.pipeline_version = spec.value("pipeline_version", spec.value("version", 1U));
  s.type = spec.value("type", std::string{});
  s.at = spec.value("at", std::string{});
  s.cron = spec.value("cron", std::string{});
  if (spec.contains("interval_ms")) {
    if (spec["interval_ms"].is_number_unsigned() || spec["interval_ms"].is_number_integer())
      s.interval_ms = std::to_string(spec["interval_ms"].get<std::int64_t>());
    else
      s.interval_ms = spec["interval_ms"].get<std::string>();
  } else if (spec.contains("interval")) {
    s.interval_ms = spec["interval"].is_string()
                        ? spec["interval"].get<std::string>()
                        : std::to_string(spec["interval"].get<std::int64_t>());
  }
  s.input = spec.value("input", Json::object());
  s.enabled = spec.value("enabled", true);
  s.misfire_policy = spec.value("misfire_policy", std::string{"SKIP"});
  s.overlap_policy = spec.value("overlap_policy", std::string{"ALLOW"});
  s.next_due_at = spec.value("next_due_at", spec.value("start_at", std::string{}));
  validate_schedule(s);
  return s;
}

TriggerDefinition parse_trigger_spec(const Json &spec) {
  if (!spec.is_object())
    throw Error(ErrorCode::Validation, "Trigger must be a JSON object");
  TriggerDefinition t;
  t.id = spec.value("id", uuid());
  t.name = spec.value("name", std::string{});
  t.pipeline_id = pipeline_id_from_spec(spec);
  t.pipeline_version = spec.value("pipeline_version", spec.value("version", 1U));
  t.event_type = spec.value("event", spec.value("event_type", std::string{}));
  t.match = spec.value("match", Json::object());
  t.enabled = spec.value("enabled", true);
  validate_trigger(t);
  return t;
}

void validate_schedule(const ScheduleDefinition &s) {
  if (s.id.empty() || s.id.size() > 128 || s.name.empty() || s.name.size() > 128 ||
      s.pipeline_id.empty() || s.pipeline_id.size() > 128 || s.pipeline_version == 0 ||
      s.input.dump().size() > max_schedule_input)
    throw Error(ErrorCode::Validation, "Invalid schedule identity or input");
  if (s.type != "one_time" && s.type != "interval" && s.type != "cron")
    throw Error(ErrorCode::Validation, "Unsupported schedule type");
  if (s.type == "one_time") {
    if (s.at.empty())
      throw Error(ErrorCode::Validation, "One-time schedule requires at");
    (void)parse_utc_timestamp(s.at);
  } else if (s.type == "interval") {
    (void)interval_value(s);
    if (!s.at.empty())
      (void)parse_utc_timestamp(s.at);
  } else {
    (void)parse_cron(s.cron);
  }
  if (!s.next_due_at.empty())
    (void)parse_utc_timestamp(s.next_due_at);
  if (!s.queued_due_at.empty())
    (void)parse_utc_timestamp(s.queued_due_at);
  if (s.misfire_policy != "SKIP" && s.misfire_policy != "RUN_ONCE")
    throw Error(ErrorCode::Validation, "Unsupported misfire policy");
  if (s.overlap_policy != "ALLOW" && s.overlap_policy != "SKIP" && s.overlap_policy != "QUEUE_ONE")
    throw Error(ErrorCode::Validation, "Unsupported overlap policy");
}

void validate_trigger(const TriggerDefinition &t) {
  if (t.id.empty() || t.id.size() > 128 || t.name.empty() || t.name.size() > 128 ||
      t.pipeline_id.empty() || t.pipeline_id.size() > 128 || t.pipeline_version == 0 ||
      t.event_type.empty() || t.event_type.size() > 128 ||
      !std::regex_match(t.event_type, std::regex("[A-Za-z0-9_.-]+")) || !t.match.is_object() ||
      t.match.size() > 32 || t.match.dump().size() > max_match_bytes)
    throw Error(ErrorCode::Validation, "Invalid event trigger definition");
  for (const auto &[key, value] : t.match.items())
    if (key.empty() || key.size() > 128 ||
        !(value.is_string() || value.is_boolean() || value.is_number() || value.is_null()))
      throw Error(ErrorCode::Validation, "Event trigger filters must use scalar values");
}

std::string next_schedule_due(const ScheduleDefinition &s, const std::string &after) {
  validate_schedule(s);
  const auto base = after.empty() ? format_utc_timestamp(SystemClock{}.now()) : after;
  if (s.type == "one_time")
    return {};
  if (s.type == "cron")
    return cron_next(s.cron, base);
  const auto value = interval_value(s);
  const auto next = parse_utc_timestamp(base) + Milliseconds{value};
  return format_utc_timestamp(next);
}

struct LocalScheduler::Subscriber final : EventSubscriber {
  explicit Subscriber(LocalScheduler &owner) : owner(owner) {}
  void receive(const Event &event) override {
    owner.receive_event(event);
  }
  LocalScheduler &owner;
};

LocalScheduler::LocalScheduler(asio::io_context &io,
                               std::function<void(const ScheduledPipeline &)> dispatch)
    : strand_(asio::make_strand(io)), legacy_dispatch_(std::move(dispatch)) {}

LocalScheduler::LocalScheduler(asio::io_context &io, Storage &storage,
                               std::function<std::string(const LaunchRequest &)> dispatch,
                               std::function<void(const Event &)> emit,
                               std::shared_ptr<Clock> clock, unsigned max_pending_launches,
                               unsigned max_trigger_depth, unsigned max_event_deliveries)
    : strand_(asio::make_strand(io)), storage_(&storage), dispatch_(std::move(dispatch)),
      emit_(std::move(emit)), clock_(std::move(clock)),
      wake_timer_(std::make_shared<asio::steady_timer>(strand_)),
      max_pending_launches_(max_pending_launches), max_trigger_depth_(max_trigger_depth),
      max_event_deliveries_(max_event_deliveries) {
  if (!clock_ || max_pending_launches_ == 0 || max_event_deliveries_ == 0 ||
      max_trigger_depth_ == 0)
    throw Error(ErrorCode::Configuration, "Invalid scheduler configuration");
}
LocalScheduler::~LocalScheduler() noexcept {
  try {
    stop();
  } catch (...) {
    // Destruction cannot report an asynchronous shutdown failure.
  }
}

void LocalScheduler::schedule(ScheduledPipeline s) {
  std::lock_guard lock(mutex_);
  if (stopped_)
    throw Error(ErrorCode::Conflict, "Scheduler is stopped");
  if (s.max_firings == 0 || s.max_firings > 100000 || s.delay < Milliseconds{0} ||
      s.interval < Milliseconds{0} || (s.max_firings > 1 && s.interval < Milliseconds{1}) ||
      timers_.size() >= 128)
    throw Error(ErrorCode::Configuration, "Invalid schedule or scheduler capacity exceeded");
  auto timer = std::make_shared<asio::steady_timer>(strand_);
  if (!timers_.emplace(s.id, timer).second)
    throw Error(ErrorCode::Conflict, "Duplicate schedule");
  asio::co_spawn(
      strand_,
      [this, s, timer]() -> Task<void> {
        for (unsigned i = 0; i < s.max_firings; ++i) {
          if (stopped_)
            break;
          timer->expires_after(i == 0 ? s.delay : s.interval);
          boost::system::error_code ec;
          co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
          if (ec || stopped_)
            break;
          try {
            legacy_dispatch_(s);
          } catch (...) {
            log_diagnostic("schedule.dispatch_failed", {{"schedule_id", s.id}});
          }
        }
        std::lock_guard guard(mutex_);
        timers_.erase(s.id);
      },
      asio::detached);
}

void LocalScheduler::stop() {
  if (stopped_.exchange(true))
    return;
  std::lock_guard lock(mutex_);
  for (auto &[id, timer] : timers_) {
    (void)id;
    asio::post(strand_, [timer] { timer->cancel(); });
  }
  if (wake_timer_)
    asio::post(strand_, [timer = wake_timer_] { timer->cancel(); });
}

ScheduleDefinition LocalScheduler::create_schedule(ScheduleDefinition schedule) {
  if (!storage_)
    throw Error(ErrorCode::Configuration, "Durable scheduler storage is unavailable");
  validate_schedule(schedule);
  std::lock_guard lock(mutex_);
  try {
    (void)storage_->get(RecordKind::Schedule, schedule.id);
    throw Error(ErrorCode::Conflict, "Schedule already exists");
  } catch (const Error &error) {
    if (error.code != ErrorCode::NotFound)
      throw;
  }
  const auto event = scheduler_event("schedule.created", schedule, schedule.next_due_at);
  storage_->commit({{RecordKind::Schedule, schedule.id, "", Json(schedule)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
  return schedule;
}

ScheduleDefinition LocalScheduler::update_schedule(ScheduleDefinition schedule) {
  if (!storage_)
    throw Error(ErrorCode::Configuration, "Durable scheduler storage is unavailable");
  validate_schedule(schedule);
  std::lock_guard lock(mutex_);
  const auto old = storage_->get(RecordKind::Schedule, schedule.id).get<ScheduleDefinition>();
  schedule.created_at = old.created_at;
  schedule.updated_at = format_utc_timestamp(clock_->now());
  const auto event = scheduler_event("schedule.updated", schedule, schedule.next_due_at);
  storage_->commit({{RecordKind::Schedule, schedule.id, "", Json(schedule)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
  return schedule;
}

void LocalScheduler::set_schedule_enabled(const std::string &id, bool enabled) {
  std::lock_guard lock(mutex_);
  auto schedule = storage_->get(RecordKind::Schedule, id).get<ScheduleDefinition>();
  if (schedule.deleted && enabled)
    throw Error(ErrorCode::Conflict, "Deleted schedule cannot be enabled");
  if (enabled && schedule.next_due_at.empty()) {
    if (schedule.type == "one_time")
      schedule.next_due_at = schedule.at;
    else
      schedule.next_due_at = next_schedule_due(schedule, format_utc_timestamp(clock_->now()));
  }
  schedule.enabled = enabled;
  schedule.updated_at = format_utc_timestamp(clock_->now());
  const auto event = scheduler_event(enabled ? "schedule.enabled" : "schedule.disabled", schedule);
  storage_->commit({{RecordKind::Schedule, id, "", Json(schedule)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
}

void LocalScheduler::delete_schedule(const std::string &id) {
  std::lock_guard lock(mutex_);
  auto schedule = storage_->get(RecordKind::Schedule, id).get<ScheduleDefinition>();
  schedule.enabled = false;
  schedule.deleted = true;
  schedule.updated_at = format_utc_timestamp(clock_->now());
  const auto event = scheduler_event("schedule.deleted", schedule);
  storage_->commit({{RecordKind::Schedule, id, "", Json(schedule)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
}

TriggerDefinition LocalScheduler::create_trigger(TriggerDefinition trigger) {
  if (!storage_)
    throw Error(ErrorCode::Configuration, "Durable scheduler storage is unavailable");
  validate_trigger(trigger);
  std::lock_guard lock(mutex_);
  try {
    (void)storage_->get(RecordKind::Trigger, trigger.id);
    throw Error(ErrorCode::Conflict, "Trigger already exists");
  } catch (const Error &error) {
    if (error.code != ErrorCode::NotFound)
      throw;
  }
  const auto event = trigger_event("trigger.created", trigger, Event{});
  storage_->commit({{RecordKind::Trigger, trigger.id, "", Json(trigger)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
  return trigger;
}

TriggerDefinition LocalScheduler::update_trigger(TriggerDefinition trigger) {
  validate_trigger(trigger);
  std::lock_guard lock(mutex_);
  const auto old = storage_->get(RecordKind::Trigger, trigger.id).get<TriggerDefinition>();
  trigger.created_at = old.created_at;
  trigger.updated_at = format_utc_timestamp(clock_->now());
  const auto event = trigger_event("trigger.updated", trigger, Event{});
  storage_->commit({{RecordKind::Trigger, trigger.id, "", Json(trigger)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
  return trigger;
}

void LocalScheduler::set_trigger_enabled(const std::string &id, bool enabled) {
  std::lock_guard lock(mutex_);
  auto trigger = storage_->get(RecordKind::Trigger, id).get<TriggerDefinition>();
  if (trigger.deleted && enabled)
    throw Error(ErrorCode::Conflict, "Deleted trigger cannot be enabled");
  trigger.enabled = enabled;
  trigger.updated_at = format_utc_timestamp(clock_->now());
  const auto event =
      trigger_event(enabled ? "trigger.enabled" : "trigger.disabled", trigger, Event{});
  storage_->commit({{RecordKind::Trigger, id, "", Json(trigger)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
}

void LocalScheduler::delete_trigger(const std::string &id) {
  std::lock_guard lock(mutex_);
  auto trigger = storage_->get(RecordKind::Trigger, id).get<TriggerDefinition>();
  trigger.enabled = false;
  trigger.deleted = true;
  trigger.updated_at = format_utc_timestamp(clock_->now());
  const auto event = trigger_event("trigger.deleted", trigger, Event{});
  storage_->commit({{RecordKind::Trigger, id, "", Json(trigger)},
                    {RecordKind::Event, event.id, "", Json(event)}});
  emit_event(event);
  wake();
}

std::shared_ptr<EventSubscriber> LocalScheduler::event_subscriber() {
  return std::make_shared<Subscriber>(*this);
}

void LocalScheduler::start() {
  if (!storage_)
    return;
  std::lock_guard lock(mutex_);
  if (started_ || stopped_)
    return;
  started_ = true;
  asio::post(strand_, [this] {
    replay_events();
    process_due();
    arm();
  });
}

void LocalScheduler::wake() {
  if (!storage_ || stopped_ || !started_)
    return;
  asio::post(strand_, [this] {
    if (wake_timer_)
      wake_timer_->cancel();
    arm();
  });
}

void LocalScheduler::arm() {
  if (!storage_ || stopped_ || !wake_timer_)
    return;
  replay_pending_deliveries();
  const auto schedules = storage_->list(RecordKind::Schedule, "", max_pending_launches_ + 1, 0);
  WallTime earliest{};
  bool found = false;
  for (const auto &record : schedules) {
    const auto schedule = record.get<ScheduleDefinition>();
    if (schedule.deleted || !schedule.enabled || schedule.next_due_at.empty())
      continue;
    const auto due = parse_utc_timestamp(schedule.next_due_at);
    if (!found || due < earliest) {
      earliest = due;
      found = true;
    }
  }
  bool pending_delivery = false;
  for (const auto &record :
       storage_->list(RecordKind::TriggerDelivery, "", max_event_deliveries_, 0))
    if (record.value("status", std::string{}) == "pending") {
      pending_delivery = true;
      break;
    }
  bool pending_schedule = false;
  for (const auto &record :
       storage_->list(RecordKind::ScheduleOccurrence, "", max_pending_launches_, 0))
    if (record.value("status", std::string{}) == "pending") {
      pending_schedule = true;
      break;
    }
  if (!found && !pending_delivery && !pending_schedule) {
    wake_timer_->cancel();
    return;
  }
  auto delay = pending_delivery || pending_schedule ? Milliseconds{1000}
               : earliest <= clock_->now()
                   ? Milliseconds{1}
                   : std::chrono::duration_cast<Milliseconds>(earliest - clock_->now());
  wake_timer_->expires_after(std::max(delay, Milliseconds{1}));
  asio::co_spawn(
      strand_,
      [this]() -> Task<void> {
        boost::system::error_code ec;
        co_await wake_timer_->async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (!ec && !stopped_) {
          process_due();
          replay_pending_deliveries();
          arm();
        }
      },
      asio::detached);
}

void LocalScheduler::replay_pending_deliveries() {
  if (!storage_ || stopped_)
    return;
  std::lock_guard lock(mutex_);
  for (const auto &record :
       storage_->list(RecordKind::TriggerDelivery, "", max_event_deliveries_, 0)) {
    if (record.value("status", std::string{}) != "pending")
      continue;
    try {
      const auto trigger =
          storage_->get(RecordKind::Trigger, record.value("trigger_id", std::string{}))
              .get<TriggerDefinition>();
      const auto event =
          storage_->get(RecordKind::Event, record.value("event_id", std::string{})).get<Event>();
      process_trigger(trigger, event);
    } catch (const Error &) {
      log_diagnostic("trigger.pending_replay_failed");
    }
  }
}

void LocalScheduler::replay_events() {
  if (!storage_ || stopped_)
    return;
  for (const auto &record : storage_->list(RecordKind::Event, "", max_event_deliveries_, 0)) {
    try {
      receive_event(record.get<Event>());
    } catch (const Error &) {
      log_diagnostic("trigger.replay_rejected");
    }
  }
}

void LocalScheduler::receive_event(const Event &event) {
  if (stopped_ || !started_)
    return;
  const auto previous = pending_events_.fetch_add(1);
  if (previous >= max_event_deliveries_) {
    pending_events_.fetch_sub(1);
    return;
  }
  asio::post(strand_, [this, event] { handle_event(event); });
}

void LocalScheduler::emit_event(const Event &event) {
  if (emit_)
    emit_(event);
  else
    log_event(event);
}

bool LocalScheduler::active_schedule_run(const std::string &schedule_id) const {
  for (const auto &record : storage_->list(RecordKind::Run, "", 10000, 0)) {
    const auto run = record.get<Run>();
    if (run.schedule_id == schedule_id && !terminal(run.state))
      return true;
  }
  return false;
}

void LocalScheduler::process_due() {
  if (!storage_ || stopped_)
    return;
  std::lock_guard lock(mutex_);
  if (stopped_)
    return;
  const auto now = clock_->now();
  std::size_t launches = 0;
  for (const auto &record :
       storage_->list(RecordKind::Schedule, "", max_pending_launches_ + 1, 0)) {
    if (launches >= max_pending_launches_)
      break;
    auto schedule = record.get<ScheduleDefinition>();
    if (schedule.deleted || (!schedule.enabled && !schedule.queued))
      continue;
    const bool queued = schedule.queued;
    const auto due = queued ? schedule.queued_due_at : schedule.next_due_at;
    if (!queued && !is_due(due, now))
      continue;
    if (due.empty())
      continue;
    const auto occurrence_id = schedule.id + "|" + due;
    Json existing;
    bool claimed = false;
    try {
      existing = storage_->get(RecordKind::ScheduleOccurrence, occurrence_id);
      const auto status = existing.value("status", std::string{});
      if (status == "started" || status == "skipped" || status == "failed")
        continue;
      if (status == "claimed") {
        bool recovered = false;
        for (const auto &run_record : storage_->list(RecordKind::Run, "", 10000, 0)) {
          const auto run = run_record.get<Run>();
          if (run.schedule_occurrence_id == occurrence_id) {
            auto updated = existing;
            updated["status"] = "started";
            updated["run_id"] = run.id;
            updated["updated_at"] = format_utc_timestamp(now);
            storage_->commit({{RecordKind::ScheduleOccurrence, occurrence_id, "", updated}});
            recovered = true;
            break;
          }
        }
        if (recovered)
          continue;
        existing["status"] = "pending";
      }
    } catch (const Error &error) {
      if (error.code != ErrorCode::NotFound)
        throw;
    }
    if (existing.is_null()) {
      claimed = storage_->claim({RecordKind::ScheduleOccurrence, occurrence_id, "",
                                 record_value("claimed", schedule, due)});
      if (!claimed)
        continue;
    } else if (existing.value("status", std::string{}) == "pending" ||
               existing.value("status", std::string{}) == "queued") {
      claimed = true;
    }
    if (!claimed)
      continue;

    const bool late = is_late(due, now);
    if (late && schedule.misfire_policy == "SKIP") {
      schedule.last_due_at = due;
      schedule.missed_count++;
      if (schedule.type == "one_time") {
        schedule.enabled = false;
        schedule.next_due_at.clear();
      } else {
        schedule.next_due_at = next_schedule_due(schedule, due);
        for (unsigned i = 0; i < 10000 && is_due(schedule.next_due_at, now); ++i) {
          ++schedule.missed_count;
          schedule.next_due_at = next_schedule_due(schedule, schedule.next_due_at);
        }
      }
      schedule.updated_at = format_utc_timestamp(now);
      auto event =
          scheduler_event("schedule.misfired", schedule, due,
                          {{"policy", schedule.misfire_policy}, {"missed", schedule.missed_count}});
      auto occurrence = record_value("skipped", schedule, due);
      storage_->commit({{RecordKind::Schedule, schedule.id, "", Json(schedule)},
                        {RecordKind::ScheduleOccurrence, occurrence_id, "", occurrence},
                        {RecordKind::Event, event.id, "", Json(event)}});
      emit_event(event);
      continue;
    }

    if (!queued && active_schedule_run(schedule.id)) {
      if (schedule.overlap_policy == "SKIP") {
        schedule.last_due_at = due;
        schedule.next_due_at = schedule.type == "one_time" ? "" : next_schedule_due(schedule, due);
        if (schedule.type == "one_time")
          schedule.enabled = false;
        schedule.updated_at = format_utc_timestamp(now);
        const auto event =
            scheduler_event("schedule.skipped", schedule, due, {{"reason", "overlap"}});
        storage_->commit({{RecordKind::Schedule, schedule.id, "", Json(schedule)},
                          {RecordKind::ScheduleOccurrence, occurrence_id, "",
                           record_value("skipped", schedule, due)},
                          {RecordKind::Event, event.id, "", Json(event)}});
        emit_event(event);
        continue;
      }
      if (schedule.overlap_policy == "QUEUE_ONE") {
        schedule.queued = true;
        schedule.queued_due_at = due;
        schedule.last_due_at = due;
        schedule.next_due_at = schedule.type == "one_time" ? "" : next_schedule_due(schedule, due);
        if (schedule.type == "one_time")
          schedule.enabled = false;
        schedule.updated_at = format_utc_timestamp(now);
        const auto event =
            scheduler_event("schedule.skipped", schedule, due, {{"reason", "overlap_queued"}});
        storage_->commit({{RecordKind::Schedule, schedule.id, "", Json(schedule)},
                          {RecordKind::ScheduleOccurrence, occurrence_id, "",
                           record_value("queued", schedule, due)},
                          {RecordKind::Event, event.id, "", Json(event)}});
        emit_event(event);
        continue;
      }
    }

    LaunchRequest request;
    request.pipeline_id = schedule.pipeline_id;
    request.pipeline_version = schedule.pipeline_version;
    request.input = schedule.input;
    request.origin = {{"initiation_type", "schedule"},
                      {"schedule_id", schedule.id},
                      {"schedule_occurrence_id", occurrence_id},
                      {"due_at", due}};
    try {
      const auto run_id = dispatch_(request);
      schedule.queued = false;
      schedule.queued_due_at.clear();
      schedule.last_due_at = due;
      schedule.last_started_at = format_utc_timestamp(now);
      schedule.next_due_at = schedule.type == "one_time" ? "" : next_schedule_due(schedule, due);
      if (schedule.type == "one_time")
        schedule.enabled = false;
      schedule.updated_at = format_utc_timestamp(now);
      const auto event =
          scheduler_event("schedule.run_created", schedule, due, {{"run_id", run_id}});
      storage_->commit({{RecordKind::Schedule, schedule.id, "", Json(schedule)},
                        {RecordKind::ScheduleOccurrence, occurrence_id, "",
                         record_value("started", schedule, due, run_id)},
                        {RecordKind::Event, event.id, "", Json(event)}});
      emit_event(event);
      ++launches;
    } catch (const Error &error) {
      auto occurrence =
          record_value(error.code == ErrorCode::Capacity ? "pending" : "failed", schedule, due);
      storage_->commit({{RecordKind::ScheduleOccurrence, occurrence_id, "", occurrence}});
      log_diagnostic("schedule.launch_failed",
                     {{"schedule_id", schedule.id}, {"code", static_cast<int>(error.code)}});
      if (error.code != ErrorCode::Capacity)
        ++launches;
    }
  }
}

void LocalScheduler::handle_event(Event event) {
  pending_events_.fetch_sub(1);
  {
    std::lock_guard lock(mutex_);
    if (stopped_)
      return;
    for (const auto &record : storage_->list(RecordKind::Trigger, "", max_event_deliveries_, 0)) {
      try {
        process_trigger(record.get<TriggerDefinition>(), event);
      } catch (const Error &) {
        log_diagnostic("trigger.processing_failed", {{"event_id", event.id}});
      }
    }
    // A terminal scheduled run may release a QUEUE_ONE occurrence.
    if (event.type == "run.completed" || event.type == "run.failed" ||
        event.type == "run.cancelled" || event.type == "run.timed_out") {
      try {
        const auto run = storage_->get(RecordKind::Run, event.run_id).get<Run>();
        if (!run.schedule_id.empty()) {
          auto schedule =
              storage_->get(RecordKind::Schedule, run.schedule_id).get<ScheduleDefinition>();
          schedule.last_completed_run_id = run.id;
          if (schedule.queued && !active_schedule_run(schedule.id)) {
            schedule.next_due_at = schedule.queued_due_at;
            schedule.queued = false;
            schedule.queued_due_at.clear();
          }
          schedule.updated_at = format_utc_timestamp(clock_->now());
          storage_->commit({{RecordKind::Schedule, schedule.id, "", Json(schedule)}});
          wake();
        }
      } catch (const Error &) {
        log_diagnostic("schedule.completion_reconciliation_failed", {{"run_id", event.run_id}});
      }
    }
  }
  arm();
}

void LocalScheduler::process_trigger(const TriggerDefinition &trigger, const Event &source) {
  if (!trigger.enabled || trigger.deleted || source.id.empty() ||
      source.time < trigger.created_at || trigger.event_type != source.type)
    return;
  for (const auto &[key, expected] : trigger.match.items()) {
    const auto dotted = split(key, '.');
    const Json *actual = &source.metadata;
    for (const auto &part : dotted) {
      if (!actual->is_object() || !actual->contains(part)) {
        actual = nullptr;
        break;
      }
      actual = &actual->at(part);
    }
    if (!actual || *actual != expected)
      return;
  }
  if (source.trigger_depth >= max_trigger_depth_) {
    const auto event = trigger_event("trigger.skipped", trigger, source, {{"reason", "depth"}});
    emit_event(event);
    return;
  }
  const auto delivery_id = trigger.id + "|" + source.id;
  Json delivery;
  bool claimed = false;
  try {
    delivery = storage_->get(RecordKind::TriggerDelivery, delivery_id);
    const auto status = delivery.value("status", std::string{});
    if (status == "started" || status == "skipped" || status == "failed")
      return;
    claimed = status == "pending" || status == "claimed";
  } catch (const Error &error) {
    if (error.code != ErrorCode::NotFound)
      throw;
  }
  if (!claimed)
    delivery = {{"id", delivery_id},
                {"trigger_id", trigger.id},
                {"event_id", source.id},
                {"status", "claimed"},
                {"updated_at", format_utc_timestamp(clock_->now())}};
  if (!claimed)
    claimed = storage_->claim({RecordKind::TriggerDelivery, delivery_id, "", delivery});
  if (!claimed)
    return;
  LaunchRequest request;
  request.pipeline_id = trigger.pipeline_id;
  request.pipeline_version = trigger.pipeline_version;
  request.input = {{"event", Json(source)}};
  request.origin = {
      {"initiation_type", "event"},
      {"trigger_id", trigger.id},
      {"event_id", source.id},
      {"root_event_id", source.root_event_id.empty() ? source.id : source.root_event_id},
      {"trigger_depth", source.trigger_depth + 1U}};
  try {
    const auto run_id = dispatch_(request);
    delivery["status"] = "started";
    delivery["run_id"] = run_id;
    delivery["updated_at"] = format_utc_timestamp(clock_->now());
    const auto event = trigger_event("trigger.run_created", trigger, source, {{"run_id", run_id}});
    storage_->commit({{RecordKind::TriggerDelivery, delivery_id, "", delivery},
                      {RecordKind::Event, event.id, "", Json(event)}});
    emit_event(event);
  } catch (const Error &error) {
    delivery["status"] = error.code == ErrorCode::Capacity ? "pending" : "failed";
    delivery["updated_at"] = format_utc_timestamp(clock_->now());
    storage_->commit({{RecordKind::TriggerDelivery, delivery_id, "", delivery}});
    log_diagnostic("trigger.launch_failed", {{"trigger_id", trigger.id}, {"event_id", source.id}});
  }
}
} // namespace laso
