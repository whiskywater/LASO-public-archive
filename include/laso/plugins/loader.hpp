#pragma once
#include <filesystem>
#include <functional>
#include <laso/core/config.hpp>
#include <laso/events/events.hpp>
#include <laso/providers/provider.hpp>
#include <laso/tools/tool.hpp>
#include <laso/workers/worker.hpp>
#include <memory>
#include <optional>
#include <set>

namespace laso {
struct PluginInfo {
  std::string path, name, version, error;
  unsigned abi = 0;
  bool loaded = false;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(PluginInfo, path, name, version, error, abi, loaded)
struct EventSourceInfo {
  std::string id, plugin, plugin_version, component, event_schema, configuration_identity,
      status = "disabled";
  std::vector<std::string> capabilities;
  bool enabled = false, healthy = false;
  std::uint64_t accepted = 0, rejected = 0, deduplicated = 0, backpressured = 0, malformed = 0;
};
inline void to_json(Json &j, const EventSourceInfo &info) {
  j = {{"id", info.id},
       {"plugin", info.plugin},
       {"plugin_version", info.plugin_version},
       {"component", info.component},
       {"event_schema", info.event_schema},
       {"configuration_identity", info.configuration_identity},
       {"status", info.status},
       {"capabilities", info.capabilities},
       {"enabled", info.enabled},
       {"healthy", info.healthy},
       {"accepted", info.accepted},
       {"rejected", info.rejected},
       {"deduplicated", info.deduplicated},
       {"backpressured", info.backpressured},
       {"malformed", info.malformed}};
}
inline void from_json(const Json &j, EventSourceInfo &info) {
  info.id = j.value("id", std::string{});
  info.plugin = j.value("plugin", std::string{});
  info.plugin_version = j.value("plugin_version", std::string{});
  info.component = j.value("component", std::string{});
  info.event_schema = j.value("event_schema", std::string{});
  info.configuration_identity = j.value("configuration_identity", std::string{});
  info.status = j.value("status", std::string{"disabled"});
  info.capabilities = j.value("capabilities", std::vector<std::string>{});
  info.enabled = j.value("enabled", false);
  info.healthy = j.value("healthy", false);
  info.accepted = j.value("accepted", std::uint64_t{0});
  info.rejected = j.value("rejected", std::uint64_t{0});
  info.deduplicated = j.value("deduplicated", std::uint64_t{0});
  info.backpressured = j.value("backpressured", std::uint64_t{0});
  info.malformed = j.value("malformed", std::uint64_t{0});
}
class PluginLoader {
public:
  using EventSubmitter =
      std::function<IngressResult(const std::string &, const std::string &, const std::string &,
                                  const std::string &, const std::string &)>;
  using EventStatePersist = std::function<void(const EventSourceInfo &)>;
  using EventStateLoad = std::function<std::optional<EventSourceInfo>(const std::string &)>;
  PluginLoader(ToolRegistry &tools, ProviderRegistry &providers,
               EventSubmitter event_submitter = {}, EventStatePersist event_state_persist = {},
               EventStateLoad event_state_load = {})
      : tools_(tools), providers_(providers), event_submitter_(std::move(event_submitter)),
        event_state_persist_(std::move(event_state_persist)),
        event_state_load_(std::move(event_state_load)) {}
  PluginLoader(ToolRegistry &tools, ProviderRegistry &providers, WorkerRegistry &workers,
               EventSubmitter event_submitter = {}, EventStatePersist event_state_persist = {},
               EventStateLoad event_state_load = {})
      : tools_(tools), providers_(providers), workers_(&workers),
        event_submitter_(std::move(event_submitter)),
        event_state_persist_(std::move(event_state_persist)),
        event_state_load_(std::move(event_state_load)) {}
  ~PluginLoader() noexcept;
  // Call during startup, before workers or consumers access the registries.
  void discover(const std::vector<std::filesystem::path> &configured_directories,
                const std::map<std::string, EventSourceConfig> &event_sources = {},
                const std::map<std::string, WorkerConfig> &worker_plugins = {});
  void start_event_sources();
  void stop_event_sources() noexcept;
  void start_workers();
  void stop_workers() noexcept;
  void set_event_source_enabled(const std::string &id, bool enabled);
  const std::vector<PluginInfo> &plugins() const {
    return plugins_;
  }
  Json event_sources() const;
  Json event_source(const std::string &id) const;
  Json workers() const;
  Json worker(const std::string &id) const;

private:
  struct EventSource;
  ToolRegistry &tools_;
  ProviderRegistry &providers_;
  WorkerRegistry *workers_ = nullptr;
  EventSubmitter event_submitter_;
  EventStatePersist event_state_persist_;
  EventStateLoad event_state_load_;
  std::vector<PluginInfo> plugins_;
  std::vector<std::shared_ptr<void>> libraries_;
  std::vector<std::shared_ptr<EventSource>> event_sources_;
  std::vector<std::shared_ptr<WorkerTransport>> worker_adapters_;
  std::map<std::string, EventSourceConfig> event_configs_;
  std::map<std::string, WorkerConfig> worker_configs_;
  std::set<std::string> matched_event_configs_;
  std::set<std::string> matched_worker_configs_;
  mutable std::mutex lifecycle_mutex_;
  bool event_sources_started_ = false;
  bool workers_started_ = false;
  void load(const std::filesystem::path &);
};
} // namespace laso
