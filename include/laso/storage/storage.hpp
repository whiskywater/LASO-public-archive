#pragma once
#include <filesystem>
#include <laso/core/types.hpp>
#include <memory>
#include <string>
#include <vector>

namespace laso {
enum class RecordKind {
  Pipeline,
  Run,
  Attempt,
  Message,
  Approval,
  Artifact,
  Event,
  Schedule,
  Trigger,
  ScheduleOccurrence,
  TriggerDelivery,
  EventSource,
  ExternalEventClaim,
  WorkerJob,
  WorkerInteraction,
  NodeWork,
  AgentSession,
  SessionTurn,
  SessionEvent,
  SessionContinuation
};
struct Record {
  RecordKind kind;
  std::string id, run_id;
  Json value;
};
class Storage {
public:
  virtual ~Storage() = default;
  // An entire checkpoint commits atomically, or none of it does.
  virtual void commit(const std::vector<Record> &records) = 0;
  // Atomically commits records only while the PostgreSQL run lease is current.
  // SQLite implements this as its ordinary serialized commit because it is a
  // single-instance backend and cannot provide distributed fencing.
  virtual void commit_owned(const std::vector<Record> &records, const std::string &resource_key,
                            const std::string &owner_instance, std::uint64_t fencing_token) = 0;
  // Control-plane cancellation is intentionally owner-independent and durable.
  virtual void request_cancellation(const std::string &run_id) = 0;
  // Atomically inserts a durable claim.  An existing id is never overwritten.
  // This is used for schedule occurrences, event-trigger deliveries, and
  // external event identities. Associated records are inserted in the same
  // transaction only when the claim is new.
  virtual bool claim(const Record &record, const std::vector<Record> &associated = {}) = 0;
  // Session inputs and events use a per-session sequence assigned while the
  // session row is locked. The database remains authoritative across service
  // instances; event delivery may poll this journal.
  virtual bool submit_session_turn(const std::string &session_id, const std::string &turn_id,
                                   const Json &turn, Json event) {
    (void)session_id;
    (void)turn_id;
    (void)turn;
    (void)event;
    throw Error(ErrorCode::Configuration, "Storage backend does not support agent sessions");
  }
  virtual std::optional<Json> claim_next_session_turn(const std::string &session_id,
                                                      const std::string &owner_instance,
                                                      std::uint64_t fencing_token,
                                                      const std::string &lease_expires_at,
                                                      Json event) {
    (void)session_id;
    (void)owner_instance;
    (void)fencing_token;
    (void)lease_expires_at;
    (void)event;
    throw Error(ErrorCode::Configuration, "Storage backend does not support session execution");
  }
  virtual bool bind_session_turn_run(const std::string &session_id, const std::string &turn_id,
                                     Json run, Json run_event, const std::string &owner_instance,
                                     std::uint64_t fencing_token, Json session_event) {
    (void)session_id;
    (void)turn_id;
    (void)run;
    (void)run_event;
    (void)owner_instance;
    (void)fencing_token;
    (void)session_event;
    throw Error(ErrorCode::Configuration, "Storage backend does not support session execution");
  }
  virtual void commit_session_run(const std::vector<Record> &records,
                                  const std::string &owner_instance, std::uint64_t fencing_token,
                                  Json session_event) {
    (void)records;
    (void)owner_instance;
    (void)fencing_token;
    (void)session_event;
    throw Error(ErrorCode::Configuration, "Storage backend does not support session execution");
  }
  virtual bool close_agent_session(const std::string &session_id, Json event) {
    (void)session_id;
    (void)event;
    throw Error(ErrorCode::Configuration, "Storage backend does not support agent sessions");
  }
  virtual std::vector<Json> session_events(const std::string &session_id, std::uint64_t after,
                                           std::size_t limit) const {
    (void)session_id;
    (void)after;
    (void)limit;
    throw Error(ErrorCode::Configuration, "Storage backend does not support agent sessions");
  }
  virtual Json get(RecordKind kind, const std::string &id) const = 0;
  virtual std::vector<Json> list(RecordKind kind, const std::string &run_id = "",
                                 std::size_t limit = 1000, std::size_t offset = 0) const = 0;
};
// Kept in this long-standing public header for source compatibility. New code
// may include <laso/storage/sqlite.hpp> when it needs the concrete adapter.
class SQLiteStorage final : public Storage {
public:
  explicit SQLiteStorage(const std::filesystem::path &path);
  ~SQLiteStorage() override;
  SQLiteStorage(const SQLiteStorage &) = delete;
  SQLiteStorage &operator=(const SQLiteStorage &) = delete;
  void commit(const std::vector<Record> &) override;
  void commit_owned(const std::vector<Record> &, const std::string &, const std::string &,
                    std::uint64_t) override;
  void request_cancellation(const std::string &) override;
  bool claim(const Record &, const std::vector<Record> &associated = {}) override;
  bool submit_session_turn(const std::string &, const std::string &, const Json &, Json) override;
  std::optional<Json> claim_next_session_turn(const std::string &, const std::string &,
                                              std::uint64_t, const std::string &, Json) override;
  bool bind_session_turn_run(const std::string &, const std::string &, Json, Json,
                             const std::string &, std::uint64_t, Json) override;
  void commit_session_run(const std::vector<Record> &, const std::string &, std::uint64_t,
                          Json) override;
  bool close_agent_session(const std::string &, Json) override;
  std::vector<Json> session_events(const std::string &, std::uint64_t, std::size_t) const override;
  Json get(RecordKind, const std::string &) const override;
  std::vector<Json> list(RecordKind, const std::string &run_id = "", std::size_t limit = 1000,
                         std::size_t offset = 0) const override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace laso
