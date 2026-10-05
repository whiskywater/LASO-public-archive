#pragma once

#include <cstddef>
#include <cstdint>
#include <laso/core/types.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace laso {
struct CoordinationOptions {
  std::string backend = "postgres";
  std::string postgres_dsn;
  std::string postgres_schema = "public";
  std::size_t pool_min_connections = 1;
  std::size_t pool_max_connections = 4;
  std::uint64_t pool_acquisition_timeout_ms = 1000;
  std::uint64_t instance_stale_after_ms = 90000;
};

struct LeaseRecord {
  std::string resource_key;
  std::string owner_instance;
  std::uint64_t fencing_token = 0;
  std::string acquired_at;
  std::string heartbeat_at;
  std::string expires_at;
  bool active = false;
};

struct CoordinationDiagnostics {
  std::size_t pool_size = 0;
  std::size_t pool_in_use = 0;
  std::uint64_t pool_acquisition_timeouts = 0;
  std::uint64_t pool_replacements = 0;
  std::uint64_t lease_acquisition_failures = 0;
  std::uint64_t renewal_failures = 0;
  std::uint64_t fencing_rejections = 0;
};
struct InstanceRecord {
  std::string instance_id, started_at, last_heartbeat_at, software_version, capabilities, state;
};
inline void to_json(Json &j, const InstanceRecord &i) {
  j = {{"instance_id", i.instance_id},
       {"started_at", i.started_at},
       {"last_heartbeat_at", i.last_heartbeat_at},
       {"software_version", i.software_version},
       {"capabilities", i.capabilities},
       {"state", i.state}};
}
inline void from_json(const Json &j, InstanceRecord &i) {
  i.instance_id = j.value("instance_id", std::string{});
  i.started_at = j.value("started_at", std::string{});
  i.last_heartbeat_at = j.value("last_heartbeat_at", std::string{});
  i.software_version = j.value("software_version", std::string{});
  i.capabilities = j.value("capabilities", std::string{});
  i.state = j.value("state", std::string{});
}

class Coordination {
public:
  virtual ~Coordination() = default;
  virtual std::optional<LeaseRecord> acquire(const std::string &resource_key,
                                             std::uint64_t ttl_ms) = 0;
  virtual bool renew(LeaseRecord &, std::uint64_t ttl_ms) = 0;
  virtual bool release(const LeaseRecord &) = 0;
  virtual std::optional<LeaseRecord> inspect(const std::string &resource_key) const = 0;
  // Throws Conflict if the lease no longer owns the current fencing token.
  // Protected writes must include the same token predicate in their own SQL.
  virtual void require_current(const LeaseRecord &) = 0;
  virtual CoordinationDiagnostics diagnostics() const = 0;
  virtual void register_instance(const std::string &software_version,
                                 const std::string &capabilities) = 0;
  virtual bool heartbeat_instance(const std::string &state) = 0;
  virtual void set_instance_state(const std::string &state) = 0;
  virtual std::vector<InstanceRecord> list_instances(std::uint64_t stale_after_ms = 0) const = 0;
};

std::unique_ptr<Coordination> create_coordination(const CoordinationOptions &,
                                                  const std::string &owner_instance);

// A fresh opaque identifier for one running LASO service instance. It is not
// derived from host, user, network, or hardware identity.
inline std::string generate_service_instance_id() {
  return uuid();
}
} // namespace laso
