#pragma once
#include <condition_variable>
#include <laso/events/events.hpp>
#include <laso/schema/validator.hpp>
#include <laso/storage/storage.hpp>
#include <mutex>
#include <unordered_map>

namespace laso {
class EventIngress final {
public:
  EventIngress(Storage &, EventBus &, SchemaValidator &, unsigned max_trigger_depth = 16,
               std::size_t max_pending = 128, std::size_t max_per_source = 32);
  ~EventIngress() = default;

  // Thread-safe and synchronous through durable acceptance. The supplied
  // identity is assigned by the host; it is not read from the plugin JSON.
  IngressResult submit(const std::string &source_id, const std::string &plugin_name,
                       const std::string &component_name, const std::string &schema_reference,
                       const std::string &event_json);
  void stop() noexcept;
  Json stats() const;

private:
  Storage &storage_;
  EventBus &events_;
  SchemaValidator &schemas_;
  std::size_t max_pending_, max_per_source_;
  mutable std::mutex mutex_;
  std::condition_variable drained_;
  std::unordered_map<std::string, std::size_t> in_flight_;
  std::size_t pending_ = 0;
  bool accepting_ = true;
  std::uint64_t accepted_ = 0, rejected_ = 0, deduplicated_ = 0, backpressured_ = 0;
};
} // namespace laso
