#include <atomic>
#include <laso/storage/coordination.hpp>
#include <laso/storage/postgres_pool.hpp>
#include <memory>
#include <version>

#ifdef __cpp_lib_source_location
#undef __cpp_lib_source_location
#endif
#include <pqxx/pqxx>

namespace laso {
namespace {
void validate_resource(const std::string &resource) {
  if (resource.empty() || resource.size() > 256)
    throw Error(ErrorCode::Validation, "Invalid coordination resource key");
}

void validate_ttl(std::uint64_t ttl_ms) {
  if (ttl_ms == 0 || ttl_ms > 86400000)
    throw Error(ErrorCode::Validation, "Invalid coordination lease duration");
}

void validate_instance_state(const std::string &state) {
  if (state != "STARTING" && state != "ACTIVE" && state != "DRAINING" && state != "STOPPED" &&
      state != "STALE")
    throw Error(ErrorCode::Validation, "Invalid LASO instance state");
}

LeaseRecord read_lease(const pqxx::row &row) {
  LeaseRecord lease;
  lease.resource_key = row[0].c_str();
  lease.owner_instance = row[1].c_str();
  lease.fencing_token = row[2].as<std::uint64_t>();
  lease.acquired_at = row[3].c_str();
  lease.heartbeat_at = row[4].c_str();
  lease.expires_at = row[5].c_str();
  lease.active = row.size() < 7 || row[6].as<bool>();
  return lease;
}

[[noreturn]] void translate_sql(const pqxx::sql_error &error) {
  if (error.sqlstate() == "23505")
    throw Error(ErrorCode::Conflict, "Coordination uniqueness conflict");
  if (error.sqlstate() == "40001" || error.sqlstate() == "40P01")
    throw Error(ErrorCode::Storage, "Coordination transaction could not be completed");
  throw Error(ErrorCode::Storage, "Coordination database operation failed");
}

[[noreturn]] void translate_connection() {
  throw Error(ErrorCode::Storage, "Coordination database connection failed");
}

class PostgresCoordination final : public Coordination {
public:
  PostgresCoordination(const CoordinationOptions &options, std::string owner)
      : owner_(std::move(owner)), pool_(options.postgres_dsn, options.postgres_schema,
                                        {options.pool_min_connections, options.pool_max_connections,
                                         options.pool_acquisition_timeout_ms}) {
    if (owner_.empty() || owner_.size() > 128)
      throw Error(ErrorCode::Validation, "Invalid coordination owner instance");
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      tx.exec("CREATE TABLE IF NOT EXISTS laso_coordination_leases ("
              "resource_key TEXT PRIMARY KEY, owner_instance TEXT NOT NULL, "
              "fencing_token BIGINT NOT NULL, acquired_at TIMESTAMPTZ NOT NULL, "
              "heartbeat_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL)");
      tx.exec("CREATE INDEX IF NOT EXISTS laso_coordination_leases_expiry "
              "ON laso_coordination_leases (expires_at)");
      tx.exec("CREATE TABLE IF NOT EXISTS laso_instances ("
              "instance_id TEXT PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL, "
              "last_heartbeat_at TIMESTAMPTZ NOT NULL, software_version TEXT NOT NULL, "
              "capabilities TEXT NOT NULL, state TEXT NOT NULL)");
      tx.exec("CREATE INDEX IF NOT EXISTS laso_instances_heartbeat "
              "ON laso_instances (last_heartbeat_at)");
      tx.commit();
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Coordination schema initialization failed");
    }
  }

  std::optional<LeaseRecord> acquire(const std::string &resource, std::uint64_t ttl_ms) override {
    validate_resource(resource);
    validate_ttl(ttl_ms);
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "INSERT INTO laso_coordination_leases "
          "(resource_key, owner_instance, fencing_token, acquired_at, heartbeat_at, expires_at) "
          "VALUES ($1, $2, 1, clock_timestamp(), clock_timestamp(), "
          "clock_timestamp() + ($3::double precision * interval '1 millisecond')) "
          "ON CONFLICT (resource_key) DO UPDATE SET owner_instance = EXCLUDED.owner_instance, "
          "fencing_token = laso_coordination_leases.fencing_token + 1, "
          "acquired_at = EXCLUDED.acquired_at, heartbeat_at = EXCLUDED.heartbeat_at, "
          "expires_at = EXCLUDED.expires_at "
          "WHERE laso_coordination_leases.expires_at <= clock_timestamp() "
          "RETURNING resource_key, owner_instance, fencing_token, acquired_at, heartbeat_at, "
          "expires_at, expires_at > clock_timestamp()",
          resource, owner_, ttl_ms);
      tx.commit();
      if (result.empty()) {
        ++acquisition_failures_;
        return std::nullopt;
      }
      return read_lease(result.front());
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Coordination acquisition failed");
    }
  }

  bool renew(LeaseRecord &lease_record, std::uint64_t ttl_ms) override {
    validate_resource(lease_record.resource_key);
    validate_ttl(ttl_ms);
    if (lease_record.owner_instance != owner_) {
      ++renewal_failures_;
      return false;
    }
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "UPDATE laso_coordination_leases SET heartbeat_at = clock_timestamp(), "
          "expires_at = clock_timestamp() + ($4::double precision * interval '1 millisecond') "
          "WHERE resource_key = $1 AND owner_instance = $2 AND fencing_token = $3 "
          "AND expires_at > clock_timestamp() "
          "RETURNING resource_key, owner_instance, fencing_token, acquired_at, heartbeat_at, "
          "expires_at, expires_at > clock_timestamp()",
          lease_record.resource_key, lease_record.owner_instance, lease_record.fencing_token,
          ttl_ms);
      tx.commit();
      if (result.empty()) {
        ++renewal_failures_;
        return false;
      }
      lease_record = read_lease(result.front());
      return true;
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Coordination renewal failed");
    }
  }

  bool release(const LeaseRecord &lease_record) override {
    validate_resource(lease_record.resource_key);
    if (lease_record.owner_instance != owner_)
      return false;
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "UPDATE laso_coordination_leases SET expires_at = clock_timestamp() "
          "WHERE resource_key = $1 AND owner_instance = $2 AND fencing_token = $3 "
          "AND expires_at > clock_timestamp() RETURNING resource_key",
          lease_record.resource_key, lease_record.owner_instance, lease_record.fencing_token);
      tx.commit();
      return !result.empty();
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Coordination release failed");
    }
  }

  std::optional<LeaseRecord> inspect(const std::string &resource) const override {
    validate_resource(resource);
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "SELECT resource_key, owner_instance, fencing_token, acquired_at, heartbeat_at, "
          "expires_at, expires_at > clock_timestamp() FROM laso_coordination_leases "
          "WHERE resource_key = $1",
          resource);
      tx.commit();
      if (result.empty())
        return std::nullopt;
      return read_lease(result.front());
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Coordination inspection failed");
    }
  }

  void require_current(const LeaseRecord &lease_record) override {
    validate_resource(lease_record.resource_key);
    if (lease_record.owner_instance != owner_) {
      ++fencing_rejections_;
      throw Error(ErrorCode::Conflict, "Coordination fencing token belongs to another owner");
    }
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "SELECT 1 FROM laso_coordination_leases WHERE resource_key = $1 AND owner_instance = $2 "
          "AND fencing_token = $3 AND expires_at > clock_timestamp()",
          lease_record.resource_key, lease_record.owner_instance, lease_record.fencing_token);
      tx.commit();
      if (result.empty()) {
        ++fencing_rejections_;
        throw Error(ErrorCode::Conflict, "Stale coordination fencing token");
      }
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Coordination fencing check failed");
    }
  }

  CoordinationDiagnostics diagnostics() const override {
    const auto pool = pool_.diagnostics();
    CoordinationDiagnostics result{pool.size,
                                   pool.in_use,
                                   pool.acquisition_timeouts,
                                   pool.replacements,
                                   acquisition_failures_.load(),
                                   renewal_failures_.load(),
                                   fencing_rejections_.load()};
    return result;
  }

  void register_instance(const std::string &software_version,
                         const std::string &capabilities) override {
    if (software_version.size() > 128 || capabilities.size() > 4096)
      throw Error(ErrorCode::Validation, "LASO instance metadata exceeds limits");
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      tx.exec_params(
          "INSERT INTO laso_instances (instance_id, started_at, last_heartbeat_at, "
          "software_version, capabilities, state) VALUES ($1, clock_timestamp(), "
          "clock_timestamp(), $2, $3, 'ACTIVE') ON CONFLICT (instance_id) DO UPDATE SET "
          "started_at = EXCLUDED.started_at, last_heartbeat_at = EXCLUDED.last_heartbeat_at, "
          "software_version = EXCLUDED.software_version, capabilities = EXCLUDED.capabilities, "
          "state = 'ACTIVE'",
          owner_, software_version, capabilities);
      tx.commit();
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Instance registration failed");
    }
  }

  bool heartbeat_instance(const std::string &state) override {
    validate_instance_state(state);
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "UPDATE laso_instances SET last_heartbeat_at = clock_timestamp(), state = $2 "
          "WHERE instance_id = $1 RETURNING instance_id",
          owner_, state);
      tx.commit();
      return !result.empty();
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Instance heartbeat failed");
    }
  }

  void set_instance_state(const std::string &state) override {
    validate_instance_state(state);
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "UPDATE laso_instances SET last_heartbeat_at = clock_timestamp(), state = $2 "
          "WHERE instance_id = $1 RETURNING instance_id",
          owner_, state);
      if (result.empty())
        throw Error(ErrorCode::NotFound, "LASO instance is not registered");
      tx.commit();
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Instance state update failed");
    }
  }

  std::vector<InstanceRecord> list_instances(std::uint64_t stale_after_ms) const override {
    if (stale_after_ms > 86400000)
      throw Error(ErrorCode::Validation, "Invalid instance stale interval");
    auto lease = pool_.acquire();
    try {
      pqxx::work tx(lease.connection());
      const auto result = tx.exec_params(
          "SELECT instance_id, started_at, last_heartbeat_at, software_version, capabilities, "
          "CASE WHEN $1::bigint > 0 AND last_heartbeat_at + ($1::double precision * "
          "interval '1 millisecond') <= clock_timestamp() THEN 'STALE' ELSE state END "
          "FROM laso_instances ORDER BY instance_id",
          stale_after_ms);
      std::vector<InstanceRecord> instances;
      instances.reserve(result.size());
      for (const auto &row : result)
        instances.push_back({row[0].c_str(), row[1].c_str(), row[2].c_str(), row[3].c_str(),
                             row[4].c_str(), row[5].c_str()});
      tx.commit();
      return instances;
    } catch (const pqxx::sql_error &error) {
      translate_sql(error);
    } catch (const pqxx::broken_connection &) {
      lease.mark_broken();
      translate_connection();
    } catch (const Error &) {
      throw;
    } catch (const std::exception &) {
      throw Error(ErrorCode::Storage, "Instance listing failed");
    }
  }

private:
  std::string owner_;
  mutable PostgresConnectionPool pool_;
  std::atomic<std::uint64_t> acquisition_failures_{0}, renewal_failures_{0}, fencing_rejections_{0};
};
} // namespace

std::unique_ptr<Coordination> create_coordination(const CoordinationOptions &options,
                                                  const std::string &owner_instance) {
  if (options.backend != "postgres")
    throw Error(ErrorCode::Configuration, options.backend == "sqlite"
                                              ? "Coordination is not available for SQLite"
                                              : "Unsupported coordination backend");
  return std::make_unique<PostgresCoordination>(options, owner_instance);
}
} // namespace laso
