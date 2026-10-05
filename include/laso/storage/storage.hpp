#pragma once
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
  SessionContinuation,
  SessionContextGeneration,
  RunContextSnapshot
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
  virtual Json create_session_context_generation(const std::string &session_id,
                                                 std::uint64_t expected_generation,
                                                 std::uint64_t through_turn_sequence,
                                                 const std::string &idempotency_key,
                                                 const std::string &representation_kind,
                                                 const std::string &representation_version,
                                                 const Json &payload) {
    (void)session_id;
    (void)expected_generation;
    (void)through_turn_sequence;
    (void)idempotency_key;
    (void)representation_kind;
    (void)representation_version;
    (void)payload;
    throw Error(ErrorCode::Configuration, "Storage backend does not support session context");
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
  virtual std::optional<Json>
  latest_session_context_generation(const std::string &session_id) const {
    (void)session_id;
    throw Error(ErrorCode::Configuration, "Storage backend does not support session context");
  }
  virtual std::vector<Json> session_turns_between(const std::string &session_id,
                                                  std::uint64_t after, std::uint64_t through,
                                                  std::size_t limit) const {
    (void)session_id;
    (void)after;
    (void)through;
    (void)limit;
    throw Error(ErrorCode::Configuration, "Storage backend does not support session history");
  }
  virtual Json get(RecordKind kind, const std::string &id) const = 0;
  virtual std::vector<Json> list(RecordKind kind, const std::string &run_id = "",
                                 std::size_t limit = 1000, std::size_t offset = 0) const = 0;
};

} // namespace laso
