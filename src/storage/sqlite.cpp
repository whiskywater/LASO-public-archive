#include <array>
#include <laso/storage/sqlite.hpp>
#include <laso/workers/worker.hpp>
#include <limits>
#include <mutex>
#include <sqlite3.h>

namespace laso {
namespace {
using Statement = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;
std::string table(RecordKind kind) {
  static constexpr std::array names = {"pipelines",
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
  const auto index = static_cast<std::size_t>(kind);
  if (index >= names.size())
    throw Error(ErrorCode::Validation, "Unknown record kind");
  return names[index];
}
void exec(sqlite3 *db, const std::string &sql) {
  if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK)
    throw Error(ErrorCode::Storage, "SQLite operation failed");
}
Statement prepare(sqlite3 *db, const std::string &sql) {
  sqlite3_stmt *raw = nullptr;
  if (sqlite3_prepare_v2(db, sql.c_str(), -1, &raw, nullptr) != SQLITE_OK)
    throw Error(ErrorCode::Storage, "SQLite statement failed");
  return {raw, sqlite3_finalize};
}
void bind(sqlite3_stmt *s, int index, const std::string &value) {
  const auto result =
      sqlite3_bind_text(s, index, value.c_str(), static_cast<int>(value.size()), SQLITE_TRANSIENT);
  if (result != SQLITE_OK)
    throw Error(ErrorCode::Storage, "SQLite binding failed");
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
Json parse(sqlite3_stmt *s) {
  const auto *data = sqlite3_column_text(s, 0);
  if (!data)
    throw Error(ErrorCode::Storage, "Missing SQLite record");
  try {
    return Json::parse(reinterpret_cast<const char *>(data));
  } catch (const Json::exception &) {
    throw Error(ErrorCode::Storage, "Invalid stored record");
  }
}
} // namespace
struct SQLiteStorage::Impl {
  std::unique_ptr<sqlite3, decltype(&sqlite3_close_v2)> db{nullptr, sqlite3_close_v2};
  mutable std::mutex mutex;
};
SQLiteStorage::SQLiteStorage(const std::filesystem::path &path) : impl_(std::make_unique<Impl>()) {
  if (path.has_parent_path())
    std::filesystem::create_directories(path.parent_path());
  sqlite3 *raw = nullptr;
  const auto result =
      sqlite3_open_v2(path.c_str(), &raw,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);
  impl_->db.reset(raw);
  if (result != SQLITE_OK)
    throw Error(ErrorCode::Storage, "Cannot open SQLite database");
  sqlite3_busy_timeout(raw, 5000);
  exec(raw, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;");
  auto version_stmt = prepare(raw, "PRAGMA user_version");
  if (sqlite3_step(version_stmt.get()) != SQLITE_ROW ||
      sqlite3_column_int(version_stmt.get(), 0) > 7)
    throw Error(ErrorCode::Storage, "Unsupported database schema version");
  version_stmt.reset();
  exec(raw, "BEGIN IMMEDIATE");
  try {
    for (std::size_t i = 0; i < 20; ++i) {
      auto name = table(static_cast<RecordKind>(i));
      exec(raw, "CREATE TABLE IF NOT EXISTS " + name +
                    " (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, body TEXT NOT NULL "
                    "CHECK(json_valid(body)), sequence INTEGER NOT NULL)");
      exec(raw, "CREATE INDEX IF NOT EXISTS " + name + "_run ON " + name + "(run_id,sequence)");
    }
    exec(raw, "PRAGMA user_version=7; COMMIT");
  } catch (...) {
    sqlite3_exec(raw, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}
SQLiteStorage::~SQLiteStorage() = default;
void SQLiteStorage::commit(const std::vector<Record> &records) {
  std::lock_guard lock(impl_->mutex);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    for (const auto &r : records) {
      if (r.id.empty())
        throw Error(ErrorCode::Validation, "Record id is empty");
      auto name = table(r.kind);
      auto s = prepare(
          db,
          "INSERT INTO " + name +
              "(id,run_id,body,sequence) VALUES(?,?,?,(SELECT COALESCE(MAX(sequence),0)+1 FROM " +
              name + ")) ON CONFLICT(id) DO UPDATE SET body=excluded.body,run_id=excluded.run_id");
      const auto body = serialize(r.value);
      if (r.kind == RecordKind::Pipeline) {
        auto existing = prepare(db, "SELECT body FROM pipelines WHERE id=?");
        bind(existing.get(), 1, r.id);
        if (sqlite3_step(existing.get()) == SQLITE_ROW) {
          const auto *stored = sqlite3_column_text(existing.get(), 0);
          if (!stored || body != reinterpret_cast<const char *>(stored))
            throw Error(ErrorCode::Conflict, "Pipeline revision is immutable");
        }
      }
      if (r.kind == RecordKind::WorkerJob) {
        auto existing = prepare(db, "SELECT body FROM worker_jobs WHERE id=?");
        bind(existing.get(), 1, r.id);
        if (sqlite3_step(existing.get()) == SQLITE_ROW) {
          const auto *stored = sqlite3_column_text(existing.get(), 0);
          if (!stored)
            throw Error(ErrorCode::Storage, "Missing worker job state");
          auto old_job = Json::parse(reinterpret_cast<const char *>(stored)).get<WorkerJob>();
          auto new_job = r.value.get<WorkerJob>();
          if (old_job.state != new_job.state &&
              !valid_worker_job_transition(old_job.state, new_job.state))
            throw Error(ErrorCode::Conflict, "Invalid worker job state transition");
        }
      }
      if (r.kind == RecordKind::NodeWork) {
        auto existing = prepare(db, "SELECT body FROM node_work WHERE id=?");
        bind(existing.get(), 1, r.id);
        if (sqlite3_step(existing.get()) == SQLITE_ROW) {
          const auto *stored = sqlite3_column_text(existing.get(), 0);
          if (!stored)
            throw Error(ErrorCode::Storage, "Missing node work state");
          const auto old_work = Json::parse(reinterpret_cast<const char *>(stored)).get<NodeWork>();
          const auto new_work = r.value.get<NodeWork>();
          if (terminal(old_work.state)) {
            if (equivalent_terminal_node_work(old_work, new_work))
              continue;
            throw Error(ErrorCode::Conflict, "Terminal node work is immutable");
          }
          if (!valid_node_work_transition(old_work.state, new_work.state))
            throw Error(ErrorCode::Conflict, "Invalid node work state transition");
        }
      }
      bind(s.get(), 1, r.id);
      bind(s.get(), 2, r.run_id);
      bind(s.get(), 3, body);
      if (sqlite3_step(s.get()) != SQLITE_DONE)
        throw Error(ErrorCode::Storage, "SQLite checkpoint failed");
    }
    exec(db, "COMMIT");
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}
void SQLiteStorage::commit_owned(const std::vector<Record> &records, const std::string &,
                                 const std::string &, std::uint64_t) {
  commit(records);
}
void SQLiteStorage::request_cancellation(const std::string &run_id) {
  if (run_id.empty())
    throw Error(ErrorCode::Validation, "Run id is empty");
  std::lock_guard lock(impl_->mutex);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    auto s = prepare(db, "SELECT body FROM runs WHERE id=?");
    bind(s.get(), 1, run_id);
    if (sqlite3_step(s.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Run not found");
    auto run = parse(s.get()).get<Run>();
    if (!terminal(run.state)) {
      run.cancellation_requested = true;
      run.updated_at = timestamp();
      auto update = prepare(db, "UPDATE runs SET body=? WHERE id=?");
      bind(update.get(), 1, serialize(Json(run)));
      bind(update.get(), 2, run_id);
      if (sqlite3_step(update.get()) != SQLITE_DONE)
        throw Error(ErrorCode::Storage, "SQLite cancellation write failed");
    }
    exec(db, "COMMIT");
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}
bool SQLiteStorage::claim(const Record &record, const std::vector<Record> &associated) {
  std::lock_guard lock(impl_->mutex);
  if (record.id.empty())
    throw Error(ErrorCode::Validation, "Record id is empty");
  if (record.kind != RecordKind::ScheduleOccurrence && record.kind != RecordKind::TriggerDelivery &&
      record.kind != RecordKind::ExternalEventClaim && record.kind != RecordKind::WorkerJob &&
      record.kind != RecordKind::NodeWork && record.kind != RecordKind::SessionTurn)
    throw Error(ErrorCode::Validation, "Record kind cannot be claimed");
  const auto name = table(record.kind);
  const auto body = serialize(record.value);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    auto statement = prepare(db, "INSERT INTO " + name +
                                     "(id,run_id,body,sequence) VALUES(?,?,?,(SELECT "
                                     "COALESCE(MAX(sequence),0)+1 FROM " +
                                     name + ")) ON CONFLICT(id) DO NOTHING");
    bind(statement.get(), 1, record.id);
    bind(statement.get(), 2, record.run_id);
    bind(statement.get(), 3, body);
    if (sqlite3_step(statement.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite claim failed");
    const bool inserted = sqlite3_changes(db) == 1;
    if (inserted) {
      for (const auto &related : associated) {
        if (related.id.empty())
          throw Error(ErrorCode::Validation, "Associated record id is empty");
        const auto related_name = table(related.kind);
        auto related_statement = prepare(
            db,
            "INSERT INTO " + related_name +
                "(id,run_id,body,sequence) VALUES(?,?,?,(SELECT COALESCE(MAX(sequence),0)+1 FROM " +
                related_name + "))");
        bind(related_statement.get(), 1, related.id);
        bind(related_statement.get(), 2, related.run_id);
        const auto related_body = serialize(related.value);
        bind(related_statement.get(), 3, related_body);
        if (sqlite3_step(related_statement.get()) != SQLITE_DONE)
          throw Error(ErrorCode::Storage, "SQLite associated claim failed");
      }
    }
    exec(db, "COMMIT");
    return inserted;
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}
bool SQLiteStorage::submit_session_turn(const std::string &session_id, const std::string &turn_id,
                                        const Json &turn, Json event) {
  if (session_id.empty() || session_id.size() > 128 || turn_id.empty() || turn_id.size() > 256 ||
      !turn.is_object() || !turn.contains("input") || !turn.contains("idempotency_key") ||
      turn.dump().size() > 1024 * 1024 || event.dump().size() > 2 * 1024 * 1024)
    throw Error(ErrorCode::Validation, "Invalid session turn");
  std::lock_guard lock(impl_->mutex);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    auto session_stmt = prepare(db, "SELECT body FROM agent_sessions WHERE id=?");
    bind(session_stmt.get(), 1, session_id);
    if (sqlite3_step(session_stmt.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse(session_stmt.get()).get<AgentSession>();
    auto existing = prepare(db, "SELECT body FROM session_turns WHERE id=?");
    bind(existing.get(), 1, turn_id);
    if (sqlite3_step(existing.get()) == SQLITE_ROW) {
      const auto prior = parse(existing.get());
      if (prior.value("session_id", std::string{}) != session_id ||
          prior.value("idempotency_key", std::string{}) !=
              turn.value("idempotency_key", std::string{}) ||
          prior.value("input", Json()) != turn.value("input", Json()))
        throw Error(ErrorCode::Conflict, "Session turn idempotency key was reused");
      exec(db, "COMMIT");
      return false;
    }
    if (session.state != "open")
      throw Error(ErrorCode::Conflict, "Agent session is closed");
    const auto event_sequence = session.next_sequence++;
    if (event_sequence > static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
      throw Error(ErrorCode::Capacity, "Agent session sequence is exhausted");
    auto max_turn_sequence =
        prepare(db, "SELECT COALESCE(MAX(sequence),0) FROM session_turns WHERE run_id=?");
    bind(max_turn_sequence.get(), 1, session_id);
    if (sqlite3_step(max_turn_sequence.get()) != SQLITE_ROW)
      throw Error(ErrorCode::Storage, "SQLite session turn sequence lookup failed");
    const auto last_turn_sequence =
        static_cast<std::uint64_t>(sqlite3_column_int64(max_turn_sequence.get(), 0));
    if (last_turn_sequence >= static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
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
    auto turn_record = turn;
    turn_record["id"] = turn_id;
    turn_record["session_id"] = session_id;
    turn_record["sequence"] = turn_sequence;
    auto turn_stmt =
        prepare(db, "INSERT INTO session_turns(id,run_id,body,sequence) VALUES(?,?,?,?)");
    bind(turn_stmt.get(), 1, turn_id);
    bind(turn_stmt.get(), 2, session_id);
    const auto turn_body = serialize(turn_record);
    bind(turn_stmt.get(), 3, turn_body);
    if (sqlite3_bind_int64(turn_stmt.get(), 4, static_cast<sqlite3_int64>(turn_sequence)) !=
        SQLITE_OK)
      throw Error(ErrorCode::Storage, "SQLite session sequence bind failed");
    if (sqlite3_step(turn_stmt.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session turn insert failed");
    auto event_stmt =
        prepare(db, "INSERT INTO session_events(id,run_id,body,sequence) VALUES(?,?,?,?)");
    const auto event_id = event.at("id").get<std::string>();
    bind(event_stmt.get(), 1, event_id);
    bind(event_stmt.get(), 2, session_id);
    const auto event_body = serialize(event);
    bind(event_stmt.get(), 3, event_body);
    if (sqlite3_bind_int64(event_stmt.get(), 4, static_cast<sqlite3_int64>(event_sequence)) !=
        SQLITE_OK)
      throw Error(ErrorCode::Storage, "SQLite session event sequence bind failed");
    if (sqlite3_step(event_stmt.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session event insert failed");
    auto update = prepare(db, "UPDATE agent_sessions SET body=? WHERE id=?");
    const auto session_body = serialize(Json(session));
    bind(update.get(), 1, session_body);
    bind(update.get(), 2, session_id);
    if (sqlite3_step(update.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "Session update failed");
    exec(db, "COMMIT");
    return true;
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}
std::optional<Json> SQLiteStorage::claim_next_session_turn(const std::string &session_id,
                                                           const std::string &owner_instance,
                                                           std::uint64_t,
                                                           const std::string &lease_expires_at,
                                                           Json event) {
  if (session_id.empty() || owner_instance.empty())
    throw Error(ErrorCode::Validation, "Invalid session dispatch ownership");
  std::lock_guard lock(impl_->mutex);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    auto session_stmt = prepare(db, "SELECT body FROM agent_sessions WHERE id=?");
    bind(session_stmt.get(), 1, session_id);
    if (sqlite3_step(session_stmt.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse(session_stmt.get()).get<AgentSession>();
    if (session.state != "open") {
      exec(db, "COMMIT");
      return std::nullopt;
    }

    Json turn;
    bool found = false;
    if (!session.active_turn_id.empty()) {
      auto active = prepare(db, "SELECT body FROM session_turns WHERE run_id=? ORDER BY sequence");
      bind(active.get(), 1, session_id);
      while (sqlite3_step(active.get()) == SQLITE_ROW) {
        auto candidate = parse(active.get());
        if (candidate.value("id", std::string{}) == session.active_turn_id) {
          turn = std::move(candidate);
          found = true;
          break;
        }
      }
      if (!found)
        throw Error(ErrorCode::Storage, "Active session turn is missing");
      if (turn.value("state", std::string{}) != "claimed" ||
          !turn.value("run_id", std::string{}).empty()) {
        exec(db, "COMMIT");
        return std::nullopt;
      }
      if (turn.value("dispatch_owner", std::string{}) == owner_instance &&
          turn.value("dispatch_fencing_token", std::uint64_t{0}) == session.dispatch_generation) {
        exec(db, "COMMIT");
        return turn;
      }
    } else {
      auto queued = prepare(db, "SELECT body FROM session_turns WHERE run_id=? ORDER BY sequence");
      bind(queued.get(), 1, session_id);
      while (sqlite3_step(queued.get()) == SQLITE_ROW) {
        auto candidate = parse(queued.get());
        if (candidate.value("state", std::string{}) == "queued") {
          turn = std::move(candidate);
          found = true;
          break;
        }
      }
      if (!found) {
        exec(db, "COMMIT");
        return std::nullopt;
      }
      session.active_turn_id = turn.at("id").get<std::string>();
    }

    ++session.dispatch_generation;
    const auto turn_id = turn.at("id").get<std::string>();
    turn["state"] = "claimed";
    turn["dispatch_owner"] = owner_instance;
    turn["dispatch_fencing_token"] = session.dispatch_generation;
    turn["dispatch_expires_at"] = lease_expires_at;
    turn["dispatch_attempt"] = turn.value("dispatch_attempt", 0U) + 1U;
    session.updated_at = timestamp();
    const auto sequence = session.next_sequence++;
    event["id"] = event.value("id", uuid());
    event["session_id"] = session_id;
    event["sequence"] = sequence;
    event["turn_id"] = turn_id;
    event["type"] = "turn.execution.claimed";
    event["payload"] = {{"state", "claimed"}, {"dispatch_attempt", turn.at("dispatch_attempt")}};
    auto update_turn = prepare(db, "UPDATE session_turns SET body=? WHERE id=?");
    const auto claimed_body = serialize(turn);
    if (sqlite3_bind_text(update_turn.get(), 1, claimed_body.c_str(),
                          static_cast<int>(claimed_body.size()), SQLITE_TRANSIENT) != SQLITE_OK)
      throw Error(ErrorCode::Storage, "SQLite session turn body bind failed");
    bind(update_turn.get(), 2, turn_id);
    if (sqlite3_step(update_turn.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage,
                  std::string("SQLite session turn update failed: ") + sqlite3_errmsg(db));
    auto insert_event =
        prepare(db, "INSERT INTO session_events(id,run_id,body,sequence) VALUES(?,?,?,?)");
    bind(insert_event.get(), 1, event.at("id").get<std::string>());
    bind(insert_event.get(), 2, session_id);
    const auto event_body = serialize(event);
    bind(insert_event.get(), 3, event_body);
    if (sqlite3_bind_int64(insert_event.get(), 4, static_cast<sqlite3_int64>(sequence)) !=
            SQLITE_OK ||
        sqlite3_step(insert_event.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session claim event failed");
    auto update_session = prepare(db, "UPDATE agent_sessions SET body=? WHERE id=?");
    const auto session_body = serialize(Json(session));
    bind(update_session.get(), 1, session_body);
    bind(update_session.get(), 2, session_id);
    if (sqlite3_step(update_session.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage,
                  std::string("SQLite session claim update failed: ") + sqlite3_errmsg(db));
    exec(db, "COMMIT");
    return turn;
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}

bool SQLiteStorage::bind_session_turn_run(const std::string &session_id, const std::string &turn_id,
                                          Json run, Json run_event,
                                          const std::string &owner_instance,
                                          std::uint64_t fencing_token, Json session_event) {
  if (session_id.empty() || turn_id.empty() || owner_instance.empty() || !run.is_object() ||
      !run_event.is_object() || run.value("session_id", std::string{}) != session_id ||
      run.value("session_turn_id", std::string{}) != turn_id)
    throw Error(ErrorCode::Validation, "Invalid session run binding");
  std::lock_guard lock(impl_->mutex);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    auto session_stmt = prepare(db, "SELECT body FROM agent_sessions WHERE id=?");
    bind(session_stmt.get(), 1, session_id);
    if (sqlite3_step(session_stmt.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse(session_stmt.get()).get<AgentSession>();
    auto turn_stmt = prepare(db, "SELECT body FROM session_turns WHERE id=?");
    bind(turn_stmt.get(), 1, turn_id);
    if (sqlite3_step(turn_stmt.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Session turn not found");
    auto turn = parse(turn_stmt.get());
    if (turn.value("session_id", std::string{}) != session_id)
      throw Error(ErrorCode::Conflict, "Session turn belongs to another session");
    const auto run_id = run.at("id").get<std::string>();
    if (turn.value("state", std::string{}) == "running" &&
        turn.value("run_id", std::string{}) == run_id) {
      exec(db, "COMMIT");
      return false;
    }
    if (session.state != "open" || session.active_turn_id != turn_id ||
        turn.value("state", std::string{}) != "claimed" ||
        turn.value("dispatch_owner", std::string{}) != owner_instance ||
        turn.value("dispatch_fencing_token", std::uint64_t{0}) !=
            (fencing_token > 0 ? fencing_token : session.dispatch_generation) ||
        !turn.value("run_id", std::string{}).empty())
      throw Error(ErrorCode::Conflict, "Session turn claim is no longer current");

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
    run_event["id"] = run_event.value("id", uuid());
    run_event["run_id"] = run_id;

    auto write = [&](RecordKind kind, const std::string &id, const std::string &record_run_id,
                     const Json &body) {
      const auto name = table(kind);
      auto stmt = prepare(
          db,
          "INSERT INTO " + name +
              "(id,run_id,body,sequence) VALUES(?,?,?,(SELECT COALESCE(MAX(sequence),0)+1 FROM " +
              name + ")) ON CONFLICT(id) DO UPDATE SET run_id=excluded.run_id,body=excluded.body");
      bind(stmt.get(), 1, id);
      bind(stmt.get(), 2, record_run_id);
      const auto serialized_body = serialize(body);
      bind(stmt.get(), 3, serialized_body);
      if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        throw Error(ErrorCode::Storage, "SQLite session run insert failed");
    };
    write(RecordKind::Run, run_id, run_id, run);
    write(RecordKind::Event, run_event.at("id").get<std::string>(), run_id, run_event);
    auto update_turn = prepare(db, "UPDATE session_turns SET body=? WHERE id=?");
    const auto turn_body = serialize(turn);
    bind(update_turn.get(), 1, turn_body);
    bind(update_turn.get(), 2, turn_id);
    if (sqlite3_step(update_turn.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session turn binding failed");
    auto insert_event =
        prepare(db, "INSERT INTO session_events(id,run_id,body,sequence) VALUES(?,?,?,?)");
    bind(insert_event.get(), 1, session_event.at("id").get<std::string>());
    bind(insert_event.get(), 2, session_id);
    const auto session_event_body = serialize(session_event);
    bind(insert_event.get(), 3, session_event_body);
    if (sqlite3_bind_int64(insert_event.get(), 4, static_cast<sqlite3_int64>(sequence)) !=
            SQLITE_OK ||
        sqlite3_step(insert_event.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session start event failed");
    auto update_session = prepare(db, "UPDATE agent_sessions SET body=? WHERE id=?");
    const auto session_body = serialize(Json(session));
    bind(update_session.get(), 1, session_body);
    bind(update_session.get(), 2, session_id);
    if (sqlite3_step(update_session.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session run binding update failed");
    exec(db, "COMMIT");
    return true;
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}

void SQLiteStorage::commit_session_run(const std::vector<Record> &records, const std::string &,
                                       std::uint64_t, Json session_event) {
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
  std::lock_guard lock(impl_->mutex);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    auto session_stmt = prepare(db, "SELECT body FROM agent_sessions WHERE id=?");
    bind(session_stmt.get(), 1, run.session_id);
    if (sqlite3_step(session_stmt.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse(session_stmt.get()).get<AgentSession>();
    auto turn_stmt = prepare(db, "SELECT body FROM session_turns WHERE id=?");
    bind(turn_stmt.get(), 1, run.session_turn_id);
    if (sqlite3_step(turn_stmt.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Session turn not found");
    auto turn = parse(turn_stmt.get());
    const auto prior_state = turn.value("state", std::string{});
    if (prior_state == terminal_state && turn.value("run_id", std::string{}) == run.id) {
      exec(db, "COMMIT");
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

    for (const auto &record : records) {
      if (record.id.empty())
        throw Error(ErrorCode::Validation, "Record id is empty");
      const auto name = table(record.kind);
      auto stmt = prepare(
          db,
          "INSERT INTO " + name +
              "(id,run_id,body,sequence) VALUES(?,?,?,(SELECT COALESCE(MAX(sequence),0)+1 FROM " +
              name + ")) ON CONFLICT(id) DO UPDATE SET run_id=excluded.run_id,body=excluded.body");
      bind(stmt.get(), 1, record.id);
      bind(stmt.get(), 2, record.run_id);
      const auto body = serialize(record.value);
      bind(stmt.get(), 3, body);
      if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        throw Error(ErrorCode::Storage, "SQLite session completion write failed");
    }
    auto turn_update = prepare(db, "UPDATE session_turns SET body=? WHERE id=?");
    const auto turn_body = serialize(turn);
    bind(turn_update.get(), 1, turn_body);
    bind(turn_update.get(), 2, run.session_turn_id);
    if (sqlite3_step(turn_update.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session turn completion failed");
    auto event_insert =
        prepare(db, "INSERT INTO session_events(id,run_id,body,sequence) VALUES(?,?,?,?)");
    bind(event_insert.get(), 1, session_event.at("id").get<std::string>());
    bind(event_insert.get(), 2, run.session_id);
    const auto event_body = serialize(session_event);
    bind(event_insert.get(), 3, event_body);
    if (sqlite3_bind_int64(event_insert.get(), 4, static_cast<sqlite3_int64>(sequence)) !=
            SQLITE_OK ||
        sqlite3_step(event_insert.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session completion event failed");
    if (session.state == "closing") {
      session.state = "closed";
      session.updated_at = timestamp();
      if (session.next_sequence >
          static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
        throw Error(ErrorCode::Capacity, "Agent session sequence is exhausted");
      const auto closed_sequence = session.next_sequence++;
      Json closed_event{{"id", uuid()},
                        {"session_id", run.session_id},
                        {"sequence", closed_sequence},
                        {"type", "session.closed"},
                        {"payload", {{"state", "closed"}}}};
      auto closed_insert =
          prepare(db, "INSERT INTO session_events(id,run_id,body,sequence) VALUES(?,?,?,?)");
      bind(closed_insert.get(), 1, closed_event.at("id").get<std::string>());
      bind(closed_insert.get(), 2, run.session_id);
      const auto closed_body = serialize(closed_event);
      bind(closed_insert.get(), 3, closed_body);
      if (sqlite3_bind_int64(closed_insert.get(), 4, static_cast<sqlite3_int64>(closed_sequence)) !=
              SQLITE_OK ||
          sqlite3_step(closed_insert.get()) != SQLITE_DONE)
        throw Error(ErrorCode::Storage, "SQLite session close completion event failed");
    }
    auto session_update = prepare(db, "UPDATE agent_sessions SET body=? WHERE id=?");
    const auto session_body = serialize(Json(session));
    bind(session_update.get(), 1, session_body);
    bind(session_update.get(), 2, run.session_id);
    if (sqlite3_step(session_update.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session completion state failed");
    exec(db, "COMMIT");
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}

bool SQLiteStorage::close_agent_session(const std::string &session_id, Json event) {
  if (session_id.empty() || session_id.size() > 128)
    throw Error(ErrorCode::Validation, "Invalid agent session id");
  (void)event;
  std::lock_guard lock(impl_->mutex);
  auto *db = impl_->db.get();
  exec(db, "BEGIN IMMEDIATE");
  try {
    auto query = prepare(db, "SELECT body FROM agent_sessions WHERE id=?");
    bind(query.get(), 1, session_id);
    if (sqlite3_step(query.get()) != SQLITE_ROW)
      throw Error(ErrorCode::NotFound, "Agent session not found");
    auto session = parse(query.get()).get<AgentSession>();
    if (session.state == "closed") {
      exec(db, "COMMIT");
      return false;
    }

    auto append_event = [&](const std::string &type, const std::string &turn_id,
                            const std::string &run_id, Json payload) {
      if (session.next_sequence >
          static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
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
      auto insert =
          prepare(db, "INSERT INTO session_events(id,run_id,body,sequence) VALUES(?,?,?,?)");
      bind(insert.get(), 1, record.at("id").get<std::string>());
      bind(insert.get(), 2, session_id);
      const auto body = serialize(record);
      bind(insert.get(), 3, body);
      if (sqlite3_bind_int64(insert.get(), 4, static_cast<sqlite3_int64>(sequence)) != SQLITE_OK ||
          sqlite3_step(insert.get()) != SQLITE_DONE)
        throw Error(ErrorCode::Storage, "SQLite session close event insert failed");
    };
    auto persist_turn = [&](Json turn) {
      const auto turn_id = turn.at("id").get<std::string>();
      turn["state"] = "cancelled";
      turn["error"] = "session_closed";
      turn["completed_at"] = timestamp();
      turn.erase("dispatch_owner");
      turn.erase("dispatch_fencing_token");
      turn.erase("dispatch_expires_at");
      auto update = prepare(db, "UPDATE session_turns SET body=? WHERE id=?");
      const auto body = serialize(turn);
      bind(update.get(), 1, body);
      bind(update.get(), 2, turn_id);
      if (sqlite3_step(update.get()) != SQLITE_DONE)
        throw Error(ErrorCode::Storage, "SQLite session turn cancellation failed");
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
          auto active =
              prepare(db, "SELECT body FROM session_turns WHERE run_id=? ORDER BY sequence");
          bind(active.get(), 1, session_id);
          Json turn;
          bool found = false;
          while (sqlite3_step(active.get()) == SQLITE_ROW) {
            auto candidate = parse(active.get());
            if (candidate.value("id", std::string{}) == session.active_turn_id) {
              turn = std::move(candidate);
              found = true;
              break;
            }
          }
          if (!found)
            throw Error(ErrorCode::Storage, "Active session turn is missing");
          turn["cancellation_requested"] = true;
          auto update = prepare(db, "UPDATE session_turns SET body=? WHERE id=?");
          const auto body = serialize(turn);
          bind(update.get(), 1, body);
          bind(update.get(), 2, session.active_turn_id);
          if (sqlite3_step(update.get()) != SQLITE_DONE)
            throw Error(ErrorCode::Storage, "SQLite active session cancellation request failed");
          append_event("turn.execution.cancel_requested", session.active_turn_id,
                       session.active_run_id, {{"state", "cancellation_requested"}});
        }
      } else if (!session.active_turn_id.empty()) {
        auto active = prepare(db, "SELECT body FROM session_turns WHERE id=?");
        bind(active.get(), 1, session.active_turn_id);
        if (sqlite3_step(active.get()) == SQLITE_ROW) {
          auto turn = parse(active.get());
          const auto state = turn.value("state", std::string{});
          if (state == "claimed" || state == "queued")
            persist_turn(std::move(turn));
        }
        session.active_turn_id.clear();
      }

      auto queued = prepare(db, "SELECT body FROM session_turns WHERE run_id=? ORDER BY sequence");
      bind(queued.get(), 1, session_id);
      while (sqlite3_step(queued.get()) == SQLITE_ROW) {
        auto turn = parse(queued.get());
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
    auto update = prepare(db, "UPDATE agent_sessions SET body=? WHERE id=?");
    const auto session_body = serialize(Json(session));
    bind(update.get(), 1, session_body);
    bind(update.get(), 2, session_id);
    if (sqlite3_step(update.get()) != SQLITE_DONE)
      throw Error(ErrorCode::Storage, "SQLite session close failed");
    exec(db, "COMMIT");
    return true;
  } catch (...) {
    sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
    throw;
  }
}
std::vector<Json> SQLiteStorage::session_events(const std::string &session_id, std::uint64_t after,
                                                std::size_t limit) const {
  if (session_id.empty() || session_id.size() > 128 || limit == 0 || limit > 1000 ||
      after > static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max()))
    throw Error(ErrorCode::Validation, "Invalid session event page");
  std::lock_guard lock(impl_->mutex);
  auto stmt =
      prepare(impl_->db.get(), "SELECT body FROM session_events WHERE run_id=? AND sequence>? "
                               "ORDER BY sequence LIMIT ?");
  bind(stmt.get(), 1, session_id);
  sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(after));
  sqlite3_bind_int64(stmt.get(), 3, static_cast<sqlite3_int64>(limit));
  std::vector<Json> result;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW)
    result.push_back(parse(stmt.get()));
  return result;
}
Json SQLiteStorage::get(RecordKind kind, const std::string &id) const {
  std::lock_guard lock(impl_->mutex);
  auto s = prepare(impl_->db.get(), "SELECT body FROM " + table(kind) + " WHERE id=?");
  bind(s.get(), 1, id);
  const auto rc = sqlite3_step(s.get());
  if (rc == SQLITE_DONE)
    throw Error(ErrorCode::NotFound, "Record not found");
  if (rc != SQLITE_ROW)
    throw Error(ErrorCode::Storage, "SQLite read failed");
  return parse(s.get());
}
std::vector<Json> SQLiteStorage::list(RecordKind kind, const std::string &run_id, std::size_t limit,
                                      std::size_t offset) const {
  if (limit > 10000 || offset > 100000000)
    throw Error(ErrorCode::Validation, "Pagination limit exceeded");
  std::lock_guard lock(impl_->mutex);
  auto s = prepare(impl_->db.get(), "SELECT body FROM " + table(kind) +
                                        (run_id.empty() ? "" : " WHERE run_id=?") +
                                        " ORDER BY sequence LIMIT ? OFFSET ?");
  int index = 1;
  if (!run_id.empty())
    bind(s.get(), index++, run_id);
  sqlite3_bind_int64(s.get(), index++, static_cast<sqlite3_int64>(limit));
  sqlite3_bind_int64(s.get(), index, static_cast<sqlite3_int64>(offset));
  std::vector<Json> result;
  int rc;
  while ((rc = sqlite3_step(s.get())) == SQLITE_ROW)
    result.push_back(parse(s.get()));
  if (rc != SQLITE_DONE)
    throw Error(ErrorCode::Storage, "SQLite list failed");
  return result;
}
} // namespace laso
