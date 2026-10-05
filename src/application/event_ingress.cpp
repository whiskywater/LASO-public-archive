#include <algorithm>
#include <laso/application/event_ingress.hpp>
#include <laso/scheduler/scheduler.hpp>
#include <regex>

namespace laso {
namespace {
constexpr auto max_event_bytes = std::size_t{1024} * 1024;
constexpr auto max_metadata_bytes = std::size_t{64} * 1024;
constexpr auto max_type_bytes = std::size_t{128};
constexpr auto max_identifier_bytes = std::size_t{256};

bool identifier(const std::string &value, std::size_t maximum) {
  return !value.empty() && value.size() <= maximum &&
         std::regex_match(value, std::regex("[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}"));
}
IngressResult rejected(std::string message) {
  if (message.size() > 512)
    message.resize(512);
  return {IngressStatus::Rejected, {}, std::move(message)};
}
} // namespace

EventIngress::EventIngress(Storage &storage, EventBus &events, SchemaValidator &schemas,
                           unsigned max_trigger_depth, std::size_t max_pending,
                           std::size_t max_per_source)
    : storage_(storage), events_(events), schemas_(schemas), max_pending_(max_pending),
      max_per_source_(max_per_source) {
  if (max_trigger_depth == 0 || max_pending == 0 || max_per_source == 0)
    throw Error(ErrorCode::Configuration, "Invalid event ingress limits");
}

IngressResult EventIngress::submit(const std::string &source_id, const std::string &plugin_name,
                                   const std::string &component_name,
                                   const std::string &schema_reference,
                                   const std::string &event_json) {
  {
    std::lock_guard lock(mutex_);
    if (!accepting_)
      return {IngressStatus::Stopped, {}, "Event ingress is stopped"};
    auto &source_count = in_flight_[source_id];
    if (pending_ >= max_pending_ || source_count >= max_per_source_) {
      ++backpressured_;
      return {IngressStatus::Backpressured, {}, "Event ingress capacity is exhausted"};
    }
    ++pending_;
    ++source_count;
  }
  struct Release final {
    EventIngress &owner;
    std::string source;
    ~Release() {
      std::lock_guard lock(owner.mutex_);
      --owner.pending_;
      auto it = owner.in_flight_.find(source);
      if (it != owner.in_flight_.end() && --it->second == 0)
        owner.in_flight_.erase(it);
      owner.drained_.notify_all();
    }
  } release{*this, source_id};

  try {
    if (!identifier(source_id, max_identifier_bytes) ||
        !identifier(plugin_name, max_identifier_bytes) ||
        !identifier(component_name, max_identifier_bytes))
      return rejected("Event source identity is invalid");
    if (event_json.size() > max_event_bytes)
      return rejected("Event payload exceeds ingress size limit");
    auto input = Json::parse(event_json, nullptr, false);
    if (input.is_discarded() || !input.is_object())
      return rejected("Event must be a JSON object");
    for (const auto &key : {"source_id", "source", "source_plugin", "source_component"})
      if (input.contains(key))
        return rejected("Event source identity is assigned by LASO");
    if (!input.contains("type") || !input.at("type").is_string())
      return rejected("Event type is required");
    const auto type = input.at("type").get<std::string>();
    if (type.empty() || type.size() > max_type_bytes ||
        !std::regex_match(type, std::regex("[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")))
      return rejected("Event type is invalid");
    const auto external_id = input.value("external_id", std::string{});
    if (external_id.size() > max_identifier_bytes)
      return rejected("External event id exceeds the size limit");
    const auto occurred_at = input.value("occurred_at", timestamp());
    (void)parse_utc_timestamp(occurred_at);
    const auto payload = input.value("payload", Json::object());
    const auto metadata = input.value("metadata", Json::object());
    if (!metadata.is_object() || metadata.dump().size() > max_metadata_bytes)
      return rejected("Event metadata is invalid or too large");
    if (payload.dump().size() > max_event_bytes)
      return rejected("Event payload exceeds ingress size limit");
    const auto causation = input.value("causation_id", std::string{});
    const auto root = input.value("root_event_id", std::string{});
    if (causation.size() > max_identifier_bytes || root.size() > max_identifier_bytes)
      return rejected("Event causation metadata exceeds the size limit");
    if (input.contains("trigger_depth") &&
        !((input.at("trigger_depth").is_number_unsigned() && input.at("trigger_depth") == 0U) ||
          (input.at("trigger_depth").is_number_integer() && input.at("trigger_depth") == 0)))
      return rejected("Event trigger depth is assigned by LASO");
    if (!schema_reference.empty())
      schemas_.validate(schema_reference, payload, source_id, "event");

    Event event;
    event.type = type;
    event.time = occurred_at;
    event.occurred_at = occurred_at;
    event.ingested_at = timestamp();
    event.source_id = source_id;
    event.source_plugin = plugin_name;
    event.source_component = component_name;
    event.external_event_id = external_id;
    event.causation_id = causation;
    event.root_event_id = root;
    event.payload = payload;
    event.metadata = metadata;
    event.metadata["source_id"] = source_id;
    event.metadata["source_plugin"] = plugin_name;
    event.metadata["source_component"] = component_name;
    if (event.metadata.dump().size() > max_metadata_bytes)
      return rejected("Event metadata exceeds the size limit");
    if (event.root_event_id.empty())
      event.root_event_id = event.id;

    if (!external_id.empty()) {
      // Prefix the source length so source/external-ID pairs cannot collide
      // merely because either value contains the delimiter.
      const auto claim_id = "external-event:" + std::to_string(source_id.size()) + ":" + source_id +
                            ":" + external_id;
      const Json claim{{"id", claim_id},
                       {"source_id", source_id},
                       {"external_event_id", external_id},
                       {"event_id", event.id},
                       {"accepted_at", event.ingested_at}};
      if (!storage_.claim({RecordKind::ExternalEventClaim, claim_id, "", claim},
                          {{RecordKind::Event, event.id, "", Json(event)}})) {
        const auto existing = storage_.get(RecordKind::ExternalEventClaim, claim_id);
        {
          std::lock_guard lock(mutex_);
          ++deduplicated_;
        }
        return {IngressStatus::Duplicate, existing.value("event_id", std::string{}),
                "External event was already accepted"};
      }
    } else {
      storage_.commit({{RecordKind::Event, event.id, "", Json(event)}});
    }
    {
      std::lock_guard lock(mutex_);
      ++accepted_;
    }
    events_.publish(event);
    return {IngressStatus::Accepted, event.id, "Event accepted"};
  } catch (const Error &error) {
    {
      std::lock_guard lock(mutex_);
      ++rejected_;
    }
    // Validation diagnostics are deliberately bounded and do not contain the
    // submitted payload. Storage and implementation details are not exposed
    // to native plugins or API callers.
    if (error.code == ErrorCode::Validation)
      return rejected(error.what());
    return rejected("Event ingestion failed");
  } catch (const Json::exception &) {
    std::lock_guard lock(mutex_);
    ++rejected_;
    return rejected("Event JSON is invalid");
  } catch (...) {
    std::lock_guard lock(mutex_);
    ++rejected_;
    return rejected("Event ingestion failed");
  }
}

void EventIngress::stop() noexcept {
  std::unique_lock lock(mutex_);
  accepting_ = false;
  drained_.wait(lock, [this] { return pending_ == 0; });
}

Json EventIngress::stats() const {
  std::lock_guard lock(mutex_);
  return {{"accepting", accepting_},       {"pending", pending_},
          {"accepted", accepted_},         {"rejected", rejected_},
          {"deduplicated", deduplicated_}, {"backpressured", backpressured_}};
}
} // namespace laso
