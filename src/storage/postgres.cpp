#include <array>
#include <cctype>
#include <laso/storage/postgres.hpp>
#include <laso/workers/worker.hpp>
#include <version>
// Ubuntu's libpqxx 7.8 package is built without std::source_location support,
// while a C++20 consumer sees that library feature in <version>.  Keep the
// exception ABI aligned with the packaged library for this implementation TU.
#ifdef __cpp_lib_source_location
#undef __cpp_lib_source_location
#endif
#include <pqxx/pqxx>

namespace laso {
namespace {
constexpr std::array<const char *, 16> table_names = {"pipelines",
                                                      "runs",
                                                      "attempts",
                                                      "messages",
                                                      "approvals",
                                                      "artifacts",
                                                      "events",
                                                      "schedules",
                                                      "triggers",
                                                      "schedule_occurrences",
                                                      "trigger_deliveries",
                                                      "event_sources",
                                                      "external_event_claims",
                                                      "worker_jobs",
                                                      "worker_interactions",
                                                      "node_work"};

const char *table(RecordKind kind) {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= table_names.size())
    throw Error(ErrorCode::Validation, "Unknown record kind");
  return table_names[index];
}

void validate_schema(const std::string &schema) {
  if (schema.empty() || schema.size() > 63 ||
      !std::isalpha(static_cast<unsigned char>(schema.front())))
    throw Error(ErrorCode::Configuration, "Invalid PostgreSQL schema");
  for (const auto ch : schema)
    if (!std::isalnum(static_cast<unsigned char>(ch)) && ch != '_')
      throw Error(ErrorCode::Configuration, "Invalid PostgreSQL schema");
}

std::string quoted_schema(const std::string &schema) {
  validate_schema(schema);
  return '"' + schema + '"';
}

[[noreturn]] void translate_sql_error(const pqxx::sql_error &error) {
  if (error.sqlstate() == "23505")
    throw Error(ErrorCode::Conflict, "PostgreSQL uniqueness conflict");
  if (error.sqlstate() == "40001")
    throw Error(ErrorCode::Storage, "PostgreSQL transaction serialization failure");
  if (error.sqlstate() == "40P01")
    throw Error(ErrorCode::Storage, "PostgreSQL transaction deadlock");
  throw Error(ErrorCode::Storage, "PostgreSQL operation failed");
}

[[noreturn]] void translate_connection_error() {
  throw Error(ErrorCode::Storage, "Cannot connect to PostgreSQL");
}

Json parse_body(const pqxx::row &row) {
  try {
    return Json::parse(row[0].c_str());
  } catch (const Json::exception &) {
    throw Error(ErrorCode::Storage, "Invalid stored record");
  }
}

std::string serialize(const Json &value) {
  try {
    const auto body = value.dump();
    if (body.size() > std::size_t{4} * 1024 * 1024)
      throw Error(ErrorCode::Storage, "Stored record exceeds limit");
    return body;
  } catch (const Error &) {
    throw;
  } catch (const Json::exception &) {
    throw Error(ErrorCode::Validation, "Record contains invalid JSON");
  }
}

void write_records(pqxx::work &tx, const std::vector<Record> &records) {
  for (const auto &record : records) {
    if (record.id.empty())
      throw Error(ErrorCode::Validation, "Record id is empty");
    const auto body = serialize(record.value);
    if (record.kind == RecordKind::Pipeline) {
      const auto existing = tx.exec_params("SELECT body FROM pipelines WHERE id = $1", record.id);
      if (!existing.empty() && existing.front()[0].c_str() != body)
        throw Error(ErrorCode::Conflict, "Pipeline revision is immutable");
    }
    if (record.kind == RecordKind::WorkerJob) {
      const auto existing = tx.exec_params("SELECT body FROM worker_jobs WHERE id = $1", record.id);
      if (!existing.empty()) {
        auto old_job = Json::parse(existing.front()[0].c_str()).get<WorkerJob>();
        auto new_job = record.value.get<WorkerJob>();
        if (old_job.state != new_job.state &&
            !valid_worker_job_transition(old_job.state, new_job.state))
          throw Error(ErrorCode::Conflict, "Invalid worker job state transition");
      }
    }
    if (record.kind == RecordKind::NodeWork) {
      const auto existing = tx.exec_params("SELECT body FROM node_work WHERE id = $1", record.id);
      if (!existing.empty()) {
        const auto old_work = Json::parse(existing.front()[0].c_str()).get<NodeWork>();
        const auto new_work = record.value.get<NodeWork>();
        if (terminal(old_work.state)) {
          if (equivalent_terminal_node_work(old_work, new_work))
            continue;
          throw Error(ErrorCode::Conflict, "Terminal node work is immutable");
        }
        if (!valid_node_work_transition(old_work.state, new_work.state))
          throw Error(ErrorCode::Conflict, "Invalid node work state transition");
      }
    }
    tx.exec_params("INSERT INTO " + std::string(table(record.kind)) +
                       " (id, run_id, body) VALUES ($1, $2, $3) "
                       "ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, "
                       "run_id = EXCLUDED.run_id",
                   record.id, record.run_id, body);
  }
}
} // namespace

struct PostgresStorage::Impl {
  std::unique_ptr<pqxx::connection> owner;
  std::unique_ptr<PostgresConnectionPool> pool;
};

PostgresStorage::PostgresStorage(const std::string &dsn, const std::string &schema,
                                 PostgresPoolOptions pool_options, bool allow_multiple_processes) {
  if (dsn.empty())
    throw Error(ErrorCode::Configuration, "PostgreSQL DSN is required");
  validate_schema(schema);
  try {
    auto candidate = std::make_unique<Impl>();
    candidate->owner = std::make_unique<pqxx::connection>(dsn);
    pqxx::work tx(*candidate->owner);
    if (allow_multiple_processes) {
      tx.exec("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || "
              "':laso-schema-migration', 0))");
    } else {
      const auto lock =
          tx.exec_params("SELECT pg_try_advisory_lock(hashtextextended(current_database() || "
                         "':laso-service-ownership', 0))");
      if (lock.empty() || !lock.front()[0].as<bool>())
        throw Error(ErrorCode::Conflict, "PostgreSQL database is owned by another LASO process");
    }

    const auto schema_name = quoted_schema(schema);
    tx.exec("CREATE SCHEMA IF NOT EXISTS " + schema_name);
    tx.exec("SET search_path TO " + schema_name + ", public");
    tx.exec("CREATE TABLE IF NOT EXISTS laso_schema_migrations ("
            "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())");
    const auto version = tx.exec("SELECT COALESCE(MAX(version), 0) FROM laso_schema_migrations")
                             .front()[0]
                             .as<int>();
    if (version > 8)
      throw Error(ErrorCode::Storage, "Unsupported PostgreSQL database schema version");
    if (version == 0) {
      for (const auto name : table_names) {
        tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
                " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
                "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
                "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
        tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + name +
                " (run_id, sequence)");
      }
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (1)");
    }
    if (version < 2) {
      for (std::size_t i = 7; i < table_names.size(); ++i) {
        const auto name = table_names[i];
        tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
                " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
                "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
                "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
        tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + name +
                " (run_id, sequence)");
      }
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (2)");
    }
    if (version < 3) {
      for (std::size_t i = 11; i < table_names.size(); ++i) {
        const auto name = table_names[i];
        tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
                " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
                "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
                "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
        tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + name +
                " (run_id, sequence)");
      }
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (3)");
    }
    if (version < 4) {
      for (std::size_t i = 13; i < table_names.size(); ++i) {
        const auto name = table_names[i];
        tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
                " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
                "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
                "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
        tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + name +
                " (run_id, sequence)");
      }
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (4)");
    }
    if (version < 5) {
      const auto name = table_names[14];
      tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
              " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
              "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
              "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
      tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + name +
              " (run_id, sequence)");
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (5)");
    }
    if (version < 6) {
      tx.exec("CREATE TABLE IF NOT EXISTS laso_coordination_leases ("
              "resource_key TEXT PRIMARY KEY, owner_instance TEXT NOT NULL, "
              "fencing_token BIGINT NOT NULL, acquired_at TIMESTAMPTZ NOT NULL, "
              "heartbeat_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL)");
      tx.exec("CREATE INDEX IF NOT EXISTS laso_coordination_leases_expiry "
              "ON laso_coordination_leases (expires_at)");
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (6)");
    }
    if (version < 7) {
      tx.exec("CREATE TABLE IF NOT EXISTS laso_instances ("
              "instance_id TEXT PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL, "
              "last_heartbeat_at TIMESTAMPTZ NOT NULL, software_version TEXT NOT NULL, "
              "capabilities TEXT NOT NULL, state TEXT NOT NULL)");
      tx.exec("CREATE INDEX IF NOT EXISTS laso_instances_heartbeat "
              "ON laso_instances (last_heartbeat_at)");
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (7)");
    }
    if (version < 8) {
      const auto name = table_names[15];
      tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
              " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
              "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
              "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
      tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + name +
              " (run_id, sequence)");
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (8)");
    }
    tx.commit();
    candidate->pool = std::make_unique<PostgresConnectionPool>(dsn, schema, pool_options);
    impl_ = std::move(candidate);
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL initialization failed");
  }
}

PostgresStorage::~PostgresStorage() = default;

void PostgresStorage::commit(const std::vector<Record> &records) {
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    write_records(tx, records);
    tx.commit();
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL write failed");
  }
}

void PostgresStorage::commit_owned(const std::vector<Record> &records,
                                   const std::string &resource_key,
                                   const std::string &owner_instance, std::uint64_t fencing_token) {
  if (resource_key.empty() || owner_instance.empty() || fencing_token == 0)
    throw Error(ErrorCode::Validation, "Invalid run ownership proof");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto lease =
        tx.exec_params("SELECT 1 FROM laso_coordination_leases WHERE resource_key = $1 "
                       "AND owner_instance = $2 AND fencing_token = $3 AND expires_at > "
                       "clock_timestamp() FOR UPDATE",
                       resource_key, owner_instance, fencing_token);
    if (lease.empty())
      throw Error(ErrorCode::Conflict, "Run ownership is no longer valid");
    write_records(tx, records);
    tx.commit();
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL owned write failed");
  }
}

void PostgresStorage::request_cancellation(const std::string &run_id) {
  if (run_id.empty())
    throw Error(ErrorCode::Validation, "Run id is empty");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto result = tx.exec_params("SELECT body FROM runs WHERE id = $1 FOR UPDATE", run_id);
    if (result.empty())
      throw Error(ErrorCode::NotFound, "Run not found");
    auto run = Json::parse(result.front()[0].c_str()).get<Run>();
    if (!terminal(run.state)) {
      run.cancellation_requested = true;
      run.updated_at = timestamp();
      tx.exec_params("UPDATE runs SET body = $2 WHERE id = $1", run_id, serialize(Json(run)));
    }
    tx.commit();
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL cancellation write failed");
  }
}
bool PostgresStorage::claim(const Record &record, const std::vector<Record> &associated) {
  if (record.id.empty())
    throw Error(ErrorCode::Validation, "Record id is empty");
  if (record.kind != RecordKind::ScheduleOccurrence && record.kind != RecordKind::TriggerDelivery &&
      record.kind != RecordKind::ExternalEventClaim && record.kind != RecordKind::WorkerJob &&
      record.kind != RecordKind::NodeWork)
    throw Error(ErrorCode::Validation, "Record kind cannot be claimed");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto body = serialize(record.value);
    const auto result =
        tx.exec_params("INSERT INTO " + std::string(table(record.kind)) +
                           " (id, run_id, body) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
                       record.id, record.run_id, body);
    const bool inserted = result.affected_rows() == 1;
    if (inserted) {
      for (const auto &related : associated) {
        if (related.id.empty())
          throw Error(ErrorCode::Validation, "Associated record id is empty");
        tx.exec_params("INSERT INTO " + std::string(table(related.kind)) +
                           " (id, run_id, body) VALUES ($1, $2, $3)",
                       related.id, related.run_id, serialize(related.value));
      }
    }
    tx.commit();
    return inserted;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL claim failed");
  }
}

Json PostgresStorage::get(RecordKind kind, const std::string &id) const {
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto result =
        tx.exec_params("SELECT body FROM " + std::string(table(kind)) + " WHERE id = $1", id);
    if (result.empty())
      throw Error(ErrorCode::NotFound, "Record not found");
    auto value = parse_body(result.front());
    tx.commit();
    return value;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL read failed");
  }
}

std::vector<Json> PostgresStorage::list(RecordKind kind, const std::string &run_id,
                                        std::size_t limit, std::size_t offset) const {
  if (limit > 10000 || offset > 100000000)
    throw Error(ErrorCode::Validation, "Pagination limit exceeded");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto sql = "SELECT body FROM " + std::string(table(kind)) +
                     (run_id.empty() ? "" : " WHERE run_id = $1") +
                     (run_id.empty() ? " ORDER BY sequence LIMIT $1 OFFSET $2"
                                     : " ORDER BY sequence LIMIT $2 OFFSET $3");
    pqxx::result result =
        run_id.empty()
            ? tx.exec_params(sql, static_cast<long long>(limit), static_cast<long long>(offset))
            : tx.exec_params(sql, run_id, static_cast<long long>(limit),
                             static_cast<long long>(offset));
    std::vector<Json> values;
    values.reserve(result.size());
    for (const auto &row : result)
      values.push_back(parse_body(row));
    tx.commit();
    return values;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL list failed");
  }
}
} // namespace laso
