#include <array>
#include <cctype>
#include <laso/storage/postgres.hpp>
#include <laso/workers/worker.hpp>
#include <limits>
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
constexpr std::array<const char *, 20> table_names = {"pipelines",
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
                                                      "node_work",
                                                      "agent_sessions",
                                                      "session_turns",
                                                      "session_events",
                                                      "session_continuations"};

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
    if (version > 10)
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
    if (version < 9) {
      for (std::size_t i = 16; i < 19; ++i) {
        const auto name = table_names[i];
        tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
                " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
                "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
                "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
        tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + name +
                " (run_id, sequence)");
      }
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (9)");
    }
    if (version < 10) {
      const auto name = table_names[19];
      tx.exec("CREATE TABLE IF NOT EXISTS " + std::string(name) +
              " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
              "CHECK (jsonb_typeof(body::jsonb) IS NOT NULL), "
              "sequence BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL)");
      tx.exec("CREATE INDEX IF NOT EXISTS " + std::string(name) + "_run ON " + std::string(name) +
              " (run_id, sequence)");
      tx.exec("INSERT INTO laso_schema_migrations(version) VALUES (10)");
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
      record.kind != RecordKind::NodeWork && record.kind != RecordKind::SessionTurn)
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

bool PostgresStorage::submit_session_turn(const std::string &session_id, const std::string &turn_id,
                                          const Json &turn, Json event) {
  if (session_id.empty() || session_id.size() > 128 || turn_id.empty() || turn_id.size() > 256 ||
      !turn.is_object() || !turn.contains("input") || !turn.contains("idempotency_key") ||
      turn.dump().size() > 1024 * 1024 || event.dump().size() > 2 * 1024 * 1024)
    throw Error(ErrorCode::Validation, "Invalid session turn");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto rows =
        tx.exec_params("SELECT body FROM agent_sessions WHERE id=$1 FOR UPDATE", session_id);
    if (rows.empty())
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse_body(rows.front()).get<AgentSession>();
    const auto prior_rows = tx.exec_params("SELECT body FROM session_turns WHERE id=$1", turn_id);
    if (!prior_rows.empty()) {
      const auto prior = parse_body(prior_rows.front());
      if (prior.value("session_id", std::string{}) != session_id ||
          prior.value("idempotency_key", std::string{}) !=
              turn.value("idempotency_key", std::string{}) ||
          prior.value("input", Json()) != turn.value("input", Json()))
        throw Error(ErrorCode::Conflict, "Session turn idempotency key was reused");
      tx.commit();
      return false;
    }
    if (session.state != "open")
      throw Error(ErrorCode::Conflict, "Agent session is closed");
    const auto event_sequence = session.next_sequence++;
    if (event_sequence > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
      throw Error(ErrorCode::Capacity, "Agent session sequence is exhausted");
    const auto max_turn_sequence = tx.exec_params(
        "SELECT COALESCE(MAX(sequence),0) FROM session_turns WHERE run_id=$1", session_id);
    const auto last_turn_sequence = max_turn_sequence.front()[0].as<std::uint64_t>();
    if (last_turn_sequence >= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
      throw Error(ErrorCode::Capacity, "Agent session turn sequence is exhausted");
    const auto turn_sequence = last_turn_sequence + 1;
    session.updated_at = timestamp();
    event["id"] = event.value("id", uuid());
    event["session_id"] = session_id;
    event["sequence"] = event_sequence;
    event["type"] = "input.accepted";
    event["turn_id"] = turn_id;
    event["payload"] = {{"input", turn.at("input")}};
    if (event.dump().size() > 2 * 1024 * 1024)
      throw Error(ErrorCode::Validation, "Session event exceeds limit");
    auto stored_turn = turn;
    stored_turn["id"] = turn_id;
    stored_turn["session_id"] = session_id;
    stored_turn["sequence"] = turn_sequence;
    tx.exec_params("INSERT INTO session_turns(id,run_id,body,sequence) VALUES($1,$2,$3,$4)",
                   turn_id, session_id, serialize(stored_turn), turn_sequence);
    tx.exec_params("INSERT INTO session_events(id,run_id,body,sequence) VALUES($1,$2,$3,$4)",
                   event.at("id").get<std::string>(), session_id, serialize(event), event_sequence);
    tx.exec_params("UPDATE agent_sessions SET body=$2 WHERE id=$1", session_id,
                   serialize(Json(session)));
    tx.commit();
    return true;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL session turn failed");
  }
}

std::optional<Json> PostgresStorage::claim_next_session_turn(const std::string &session_id,
                                                             const std::string &owner_instance,
                                                             std::uint64_t fencing_token,
                                                             const std::string &lease_expires_at,
                                                             Json event) {
  if (session_id.empty() || owner_instance.empty() ||
      (fencing_token != 0 && lease_expires_at.empty()))
    throw Error(ErrorCode::Validation, "Invalid session dispatch ownership");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    if (fencing_token > 0) {
      const auto resource = "session:" + session_id;
      const auto lease =
          tx.exec_params("SELECT 1 FROM laso_coordination_leases WHERE resource_key=$1 "
                         "AND owner_instance=$2 AND fencing_token=$3 "
                         "AND expires_at > clock_timestamp() FOR UPDATE",
                         resource, owner_instance, fencing_token);
      if (lease.empty())
        throw Error(ErrorCode::Conflict, "Session dispatch lease is stale");
    }
    const auto sessions =
        tx.exec_params("SELECT body FROM agent_sessions WHERE id=$1 FOR UPDATE", session_id);
    if (sessions.empty())
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse_body(sessions.front()).get<AgentSession>();
    const auto claim_fence = fencing_token > 0 ? fencing_token : session.dispatch_generation + 1;
    if (session.state != "open") {
      tx.commit();
      return std::nullopt;
    }

    Json turn;
    bool found = false;
    if (!session.active_turn_id.empty()) {
      const auto active = tx.exec_params("SELECT body FROM session_turns WHERE id=$1 FOR UPDATE",
                                         session.active_turn_id);
      if (active.empty())
        throw Error(ErrorCode::Storage, "Active session turn is missing");
      turn = parse_body(active.front());
      const auto prior_owner = turn.value("dispatch_owner", std::string{});
      const auto prior_fence = turn.value("dispatch_fencing_token", std::uint64_t{0});
      if (turn.value("state", std::string{}) != "claimed" ||
          !turn.value("run_id", std::string{}).empty()) {
        tx.commit();
        return std::nullopt;
      }
      if (prior_owner == owner_instance &&
          prior_fence == (fencing_token > 0 ? fencing_token : session.dispatch_generation)) {
        tx.commit();
        return turn;
      }
      if (fencing_token > 0 && fencing_token <= prior_fence) {
        tx.commit();
        return std::nullopt;
      }
      found = true;
    } else {
      const auto queued =
          tx.exec_params("SELECT body FROM session_turns WHERE run_id=$1 "
                         "AND body::jsonb->>'state'='queued' ORDER BY sequence LIMIT 1 FOR UPDATE",
                         session_id);
      if (!queued.empty()) {
        turn = parse_body(queued.front());
        found = true;
      }
      if (!found) {
        tx.commit();
        return std::nullopt;
      }
      session.active_turn_id = turn.at("id").get<std::string>();
    }

    const auto turn_id = turn.at("id").get<std::string>();
    turn["state"] = "claimed";
    turn["dispatch_owner"] = owner_instance;
    turn["dispatch_fencing_token"] = claim_fence;
    turn["dispatch_expires_at"] = lease_expires_at;
    turn["dispatch_attempt"] = turn.value("dispatch_attempt", 0U) + 1U;
    session.dispatch_generation++;
    session.updated_at = timestamp();
    const auto sequence = session.next_sequence++;
    event["id"] = event.value("id", uuid());
    event["session_id"] = session_id;
    event["sequence"] = sequence;
    event["turn_id"] = turn_id;
    event["type"] = "turn.execution.claimed";
    event["payload"] = {{"state", "claimed"}, {"dispatch_attempt", turn.at("dispatch_attempt")}};
    tx.exec_params("UPDATE session_turns SET body=$2 WHERE id=$1", turn_id, serialize(turn));
    tx.exec_params("INSERT INTO session_events(id,run_id,body,sequence) "
                   "VALUES($1,$2,$3,$4)",
                   event.at("id").get<std::string>(), session_id, serialize(event), sequence);
    tx.exec_params("UPDATE agent_sessions SET body=$2 WHERE id=$1", session_id,
                   serialize(Json(session)));
    tx.commit();
    return turn;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL session claim failed");
  }
}

bool PostgresStorage::bind_session_turn_run(const std::string &session_id,
                                            const std::string &turn_id, Json run, Json run_event,
                                            const std::string &owner_instance,
                                            std::uint64_t fencing_token, Json session_event) {
  if (session_id.empty() || turn_id.empty() || owner_instance.empty() || !run.is_object() ||
      !run_event.is_object() || run.value("session_id", std::string{}) != session_id ||
      run.value("session_turn_id", std::string{}) != turn_id)
    throw Error(ErrorCode::Validation, "Invalid session run binding");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    if (fencing_token > 0) {
      const auto resource = "session:" + session_id;
      const auto lease =
          tx.exec_params("SELECT 1 FROM laso_coordination_leases WHERE resource_key=$1 "
                         "AND owner_instance=$2 AND fencing_token=$3 "
                         "AND expires_at > clock_timestamp() FOR UPDATE",
                         resource, owner_instance, fencing_token);
      if (lease.empty())
        throw Error(ErrorCode::Conflict, "Session dispatch lease is stale");
    }
    const auto sessions =
        tx.exec_params("SELECT body FROM agent_sessions WHERE id=$1 FOR UPDATE", session_id);
    if (sessions.empty())
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse_body(sessions.front()).get<AgentSession>();
    const auto turns =
        tx.exec_params("SELECT body FROM session_turns WHERE id=$1 FOR UPDATE", turn_id);
    if (turns.empty())
      throw Error(ErrorCode::NotFound, "Session turn not found");
    auto turn = parse_body(turns.front());
    const auto expected_claim_fence =
        fencing_token > 0 ? fencing_token : session.dispatch_generation;
    if (turn.value("session_id", std::string{}) != session_id)
      throw Error(ErrorCode::Conflict, "Session turn belongs to another session");
    if (turn.value("state", std::string{}) == "running" &&
        turn.value("run_id", std::string{}) == run.at("id").get<std::string>()) {
      tx.commit();
      return false;
    }
    if (session.state != "open" || session.active_turn_id != turn_id ||
        turn.value("state", std::string{}) != "claimed" ||
        turn.value("dispatch_owner", std::string{}) != owner_instance ||
        turn.value("dispatch_fencing_token", std::uint64_t{0}) != expected_claim_fence ||
        !turn.value("run_id", std::string{}).empty())
      throw Error(ErrorCode::Conflict, "Session turn claim is no longer current");

    const auto run_id = run.at("id").get<std::string>();
    turn["state"] = "running";
    turn["run_id"] = run_id;
    turn["started_at"] = timestamp();
    turn.erase("dispatch_owner");
    turn.erase("dispatch_fencing_token");
    turn.erase("dispatch_expires_at");
    session.active_run_id = run_id;
    session.updated_at = timestamp();
    const auto sequence = session.next_sequence++;
    session_event["id"] = session_event.value("id", uuid());
    session_event["session_id"] = session_id;
    session_event["sequence"] = sequence;
    session_event["turn_id"] = turn_id;
    session_event["run_id"] = run_id;
    session_event["type"] = "turn.execution.started";
    session_event["payload"] = {{"state", "running"}, {"run_id", run_id}};
    const auto run_event_id = run_event.value("id", uuid());
    run_event["id"] = run_event_id;
    run_event["run_id"] = run_id;
    run["id"] = run_id;
    write_records(tx, {{RecordKind::Run, run_id, run_id, std::move(run)},
                       {RecordKind::Event, run_event_id, run_id, std::move(run_event)}});
    tx.exec_params("UPDATE session_turns SET body=$2 WHERE id=$1", turn_id, serialize(turn));
    tx.exec_params("INSERT INTO session_events(id,run_id,body,sequence) "
                   "VALUES($1,$2,$3,$4)",
                   session_event.at("id").get<std::string>(), session_id, serialize(session_event),
                   sequence);
    tx.exec_params("UPDATE agent_sessions SET body=$2 WHERE id=$1", session_id,
                   serialize(Json(session)));
    tx.commit();
    return true;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL session run binding failed");
  }
}

void PostgresStorage::commit_session_run(const std::vector<Record> &records,
                                         const std::string &owner_instance,
                                         std::uint64_t fencing_token, Json session_event) {
  if ((fencing_token == 0 && !owner_instance.empty()) ||
      (fencing_token > 0 && owner_instance.empty()))
    throw Error(ErrorCode::Validation, "Invalid session run ownership proof");
  const auto run_record = std::find_if(records.begin(), records.end(), [](const Record &record) {
    return record.kind == RecordKind::Run;
  });
  if (run_record == records.end())
    throw Error(ErrorCode::Validation, "Session completion has no run record");
  const auto run = run_record->value.get<Run>();
  if (run.session_id.empty() || run.session_turn_id.empty() || !terminal(run.state))
    throw Error(ErrorCode::Validation, "Invalid session completion");
  const std::string terminal_state = run.state == RunState::Completed   ? "succeeded"
                                     : run.state == RunState::Cancelled ? "cancelled"
                                                                        : "failed";

  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    if (fencing_token > 0) {
      const auto lease =
          tx.exec_params("SELECT 1 FROM laso_coordination_leases WHERE resource_key=$1 "
                         "AND owner_instance=$2 AND fencing_token=$3 "
                         "AND expires_at > clock_timestamp() FOR UPDATE",
                         "run:" + run.id, owner_instance, fencing_token);
      if (lease.empty())
        throw Error(ErrorCode::Conflict, "Run ownership is no longer valid");
    }
    const auto sessions =
        tx.exec_params("SELECT body FROM agent_sessions WHERE id=$1 FOR UPDATE", run.session_id);
    if (sessions.empty())
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse_body(sessions.front()).get<AgentSession>();
    const auto turns = tx.exec_params("SELECT body FROM session_turns WHERE id=$1 FOR UPDATE",
                                      run.session_turn_id);
    if (turns.empty())
      throw Error(ErrorCode::NotFound, "Session turn not found");
    auto turn = parse_body(turns.front());
    const auto prior_state = turn.value("state", std::string{});
    if (prior_state == terminal_state && turn.value("run_id", std::string{}) == run.id) {
      tx.commit();
      return;
    }
    if (session.active_run_id != run.id || session.active_turn_id != run.session_turn_id ||
        turn.value("session_id", std::string{}) != run.session_id ||
        turn.value("run_id", std::string{}) != run.id || prior_state != "running")
      throw Error(ErrorCode::Conflict, "Session completion is no longer current");

    turn["state"] = terminal_state;
    turn["completed_at"] = timestamp();
    turn["result"] = run.message.payload;
    if (terminal_state == "failed")
      turn["error"] = "execution_failed";
    else if (terminal_state == "cancelled")
      turn["error"] = "execution_cancelled";
    turn.erase("dispatch_owner");
    turn.erase("dispatch_fencing_token");
    turn.erase("dispatch_expires_at");
    session.active_turn_id.clear();
    session.active_run_id.clear();
    session.updated_at = timestamp();

    const auto sequence = session.next_sequence++;
    session_event["id"] = session_event.value("id", uuid());
    session_event["session_id"] = run.session_id;
    session_event["sequence"] = sequence;
    session_event["turn_id"] = run.session_turn_id;
    session_event["run_id"] = run.id;
    session_event["type"] = terminal_state == "succeeded"   ? "turn.execution.completed"
                            : terminal_state == "cancelled" ? "turn.execution.cancelled"
                                                            : "turn.execution.failed";
    session_event["payload"] = {{"state", terminal_state}, {"run_id", run.id}};
    if (terminal_state == "succeeded") {
      session_event["payload"]["result"] = run.message.payload;
    } else {
      session_event["payload"]["error"] = turn.at("error");
    }
    if (session_event.dump().size() > 2 * 1024 * 1024) {
      session_event["payload"].erase("result");
      session_event["payload"]["result_in_run"] = true;
    }

    write_records(tx, records);
    tx.exec_params("UPDATE session_turns SET body=$2 WHERE id=$1", run.session_turn_id,
                   serialize(turn));
    tx.exec_params("INSERT INTO session_events(id,run_id,body,sequence) VALUES($1,$2,$3,$4)",
                   session_event.at("id").get<std::string>(), run.session_id,
                   serialize(session_event), sequence);
    if (session.state == "closing") {
      session.state = "closed";
      session.updated_at = timestamp();
      const auto closed_sequence = session.next_sequence++;
      if (closed_sequence > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
        throw Error(ErrorCode::Capacity, "Agent session sequence is exhausted");
      Json closed_event{{"id", uuid()},
                        {"session_id", run.session_id},
                        {"sequence", closed_sequence},
                        {"type", "session.closed"},
                        {"payload", {{"state", "closed"}}}};
      tx.exec_params("INSERT INTO session_events(id,run_id,body,sequence) VALUES($1,$2,$3,$4)",
                     closed_event.at("id").get<std::string>(), run.session_id,
                     serialize(closed_event), closed_sequence);
    }
    tx.exec_params("UPDATE agent_sessions SET body=$2 WHERE id=$1", run.session_id,
                   serialize(Json(session)));
    tx.commit();
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL session completion failed");
  }
}

bool PostgresStorage::close_agent_session(const std::string &session_id, Json event) {
  if (session_id.empty() || session_id.size() > 128)
    throw Error(ErrorCode::Validation, "Invalid agent session id");
  (void)event;
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto rows =
        tx.exec_params("SELECT body FROM agent_sessions WHERE id=$1 FOR UPDATE", session_id);
    if (rows.empty())
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse_body(rows.front()).get<AgentSession>();
    if (session.state == "closed") {
      tx.commit();
      return false;
    }

    auto append_event = [&](const std::string &type, const std::string &turn_id,
                            const std::string &run_id, Json payload) {
      if (session.next_sequence >
          static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
        throw Error(ErrorCode::Capacity, "Agent session sequence is exhausted");
      const auto sequence = session.next_sequence++;
      Json record{{"id", uuid()},
                  {"session_id", session_id},
                  {"sequence", sequence},
                  {"type", type},
                  {"payload", std::move(payload)}};
      if (!turn_id.empty())
        record["turn_id"] = turn_id;
      if (!run_id.empty())
        record["run_id"] = run_id;
      tx.exec_params("INSERT INTO session_events(id,run_id,body,sequence) VALUES($1,$2,$3,$4)",
                     record.at("id").get<std::string>(), session_id, serialize(record), sequence);
    };
    auto persist_turn = [&](Json turn) {
      const auto turn_id = turn.at("id").get<std::string>();
      turn["state"] = "cancelled";
      turn["error"] = "session_closed";
      turn["completed_at"] = timestamp();
      turn.erase("dispatch_owner");
      turn.erase("dispatch_fencing_token");
      turn.erase("dispatch_expires_at");
      tx.exec_params("UPDATE session_turns SET body=$2 WHERE id=$1", turn_id, serialize(turn));
      append_event("turn.execution.cancelled", turn_id, "",
                   {{"state", "cancelled"}, {"reason", "session_closed"}});
    };

    const bool has_active_run = !session.active_run_id.empty();
    const bool first_close = session.state != "closing";
    if (first_close) {
      session.state = has_active_run ? "closing" : "closed";
      session.updated_at = timestamp();
      append_event("session.closing", "", "", {{"state", "closing"}});
      if (has_active_run) {
        if (!session.active_turn_id.empty()) {
          const auto active = tx.exec_params(
              "SELECT body FROM session_turns WHERE id=$1 FOR UPDATE", session.active_turn_id);
          if (active.empty())
            throw Error(ErrorCode::Storage, "Active session turn is missing");
          auto turn = parse_body(active.front());
          turn["cancellation_requested"] = true;
          tx.exec_params("UPDATE session_turns SET body=$2 WHERE id=$1", session.active_turn_id,
                         serialize(turn));
          append_event("turn.execution.cancel_requested", session.active_turn_id,
                       session.active_run_id, {{"state", "cancellation_requested"}});
        }
      } else if (!session.active_turn_id.empty()) {
        const auto active = tx.exec_params("SELECT body FROM session_turns WHERE id=$1 FOR UPDATE",
                                           session.active_turn_id);
        if (!active.empty()) {
          auto turn = parse_body(active.front());
          const auto state = turn.value("state", std::string{});
          if (state == "claimed" || state == "queued")
            persist_turn(std::move(turn));
        }
        session.active_turn_id.clear();
      }

      const auto queued = tx.exec_params(
          "SELECT body FROM session_turns WHERE run_id=$1 ORDER BY sequence FOR UPDATE",
          session_id);
      for (const auto &row : queued) {
        auto turn = parse_body(row);
        const auto state = turn.value("state", std::string{});
        if (state == "queued" || (!has_active_run && state == "claimed"))
          persist_turn(std::move(turn));
      }
      if (!has_active_run) {
        session.active_run_id.clear();
        append_event("session.closed", "", "", {{"state", "closed"}});
      }
    }
    session.updated_at = timestamp();
    tx.exec_params("UPDATE agent_sessions SET body=$2 WHERE id=$1", session_id,
                   serialize(Json(session)));
    tx.commit();
    return true;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL session close failed");
  }
}
std::vector<Json> PostgresStorage::session_events(const std::string &session_id,
                                                  std::uint64_t after, std::size_t limit) const {
  if (session_id.empty() || session_id.size() > 128 || limit == 0 || limit > 1000 ||
      after > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    throw Error(ErrorCode::Validation, "Invalid session event page");
  PostgresConnectionPool::Lease connection;
  try {
    connection = impl_->pool->acquire();
    pqxx::work tx(connection.connection());
    const auto rows = tx.exec_params("SELECT body FROM session_events WHERE run_id=$1 AND "
                                     "sequence>$2 ORDER BY sequence LIMIT $3",
                                     session_id, after, limit);
    std::vector<Json> result;
    result.reserve(rows.size());
    for (const auto &row : rows)
      result.push_back(parse_body(row));
    tx.commit();
    return result;
  } catch (const Error &) {
    throw;
  } catch (const pqxx::sql_error &error) {
    translate_sql_error(error);
  } catch (const pqxx::broken_connection &) {
    connection.mark_broken();
    translate_connection_error();
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL session event read failed");
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
