#pragma once
#include <chrono>
#include <map>
#include <nlohmann/json.hpp>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace laso {
// Opaque provider state is held only by the runtime/session persistence path.
// It intentionally has no JSON serialization overload.
struct OpaqueProviderContinuation {
  std::string provider_id;
  std::string provider_version;
  std::string state;
};
using Json = nlohmann::json;
using Milliseconds = std::chrono::milliseconds;
inline constexpr auto version = "0.1.0-rc.1";
std::string uuid();
std::string timestamp();
enum class ErrorCode {
  Configuration,
  Validation,
  Plugin,
  Storage,
  Provider,
  Tool,
  Policy,
  Execution,
  Timeout,
  Cancellation,
  NotFound,
  Conflict,
  Capacity
};
class Error : public std::runtime_error {
public:
  Error(ErrorCode code, const std::string &safe_message, Json details = Json::object())
      : std::runtime_error(safe_message), code(code), details(std::move(details)) {}
  ErrorCode code;
  Json details;
};
enum class RunState {
  Queued,
  Starting,
  Running,
  WaitingTool,
  WaitingModel,
  WaitingWorker,
  WaitingApproval,
  Retrying,
  Paused,
  Completed,
  Failed,
  Cancelled,
  TimedOut
};
NLOHMANN_JSON_SERIALIZE_ENUM(RunState, {{RunState::Queued, "Queued"},
                                        {RunState::Starting, "Starting"},
                                        {RunState::Running, "Running"},
                                        {RunState::WaitingTool, "WaitingTool"},
                                        {RunState::WaitingModel, "WaitingModel"},
                                        {RunState::WaitingWorker, "WaitingWorker"},
                                        {RunState::WaitingApproval, "WaitingApproval"},
                                        {RunState::Retrying, "Retrying"},
                                        {RunState::Paused, "Paused"},
                                        {RunState::Completed, "Completed"},
                                        {RunState::Failed, "Failed"},
                                        {RunState::Cancelled, "Cancelled"},
                                        {RunState::TimedOut, "TimedOut"}})
bool terminal(RunState state);
bool valid_transition(RunState from, RunState to);
enum class NodeState { Running, Completed, Failed, WaitingApproval, Cancelled, TimedOut };
NLOHMANN_JSON_SERIALIZE_ENUM(NodeState, {{NodeState::Running, "Running"},
                                         {NodeState::Completed, "Completed"},
                                         {NodeState::Failed, "Failed"},
                                         {NodeState::WaitingApproval, "WaitingApproval"},
                                         {NodeState::Cancelled, "Cancelled"},
                                         {NodeState::TimedOut, "TimedOut"}})
struct ProvenanceRecord {
  std::string node, tool, model, provider, artifact, parent_message, validation, time, worker;
};
inline void to_json(Json &j, const ProvenanceRecord &p) {
  j = {{"node", p.node},
       {"tool", p.tool},
       {"model", p.model},
       {"provider", p.provider},
       {"artifact", p.artifact},
       {"parent_message", p.parent_message},
       {"validation", p.validation},
       {"time", p.time},
       {"worker", p.worker}};
}
inline void from_json(const Json &j, ProvenanceRecord &p) {
  p.node = j.value("node", std::string{});
  p.tool = j.value("tool", std::string{});
  p.model = j.value("model", std::string{});
  p.provider = j.value("provider", std::string{});
  p.artifact = j.value("artifact", std::string{});
  p.parent_message = j.value("parent_message", std::string{});
  p.validation = j.value("validation", std::string{});
  p.time = j.value("time", std::string{});
  p.worker = j.value("worker", std::string{});
}
struct Message {
  std::string id = uuid(), run_id, pipeline_id, node_id, type = "laso.data", time = timestamp();
  Json payload = Json::object(), metadata = Json::object();
  std::vector<ProvenanceRecord> provenance;
  // A missing confidence is serialized as JSON null.
  std::optional<double> confidence;
};
void to_json(Json &json, const Message &message);
void from_json(const Json &json, Message &message);
struct RetryPolicy {
  unsigned max_attempts = 1;
  Milliseconds delay{0};
};
struct TimeoutPolicy {
  Milliseconds timeout{30000};
};
struct NodeDefinition {
  std::string id, type, binding, prompt, field, condition, reason, join, input_schema,
      output_schema, schema, task_type, capability, instructions;
  Json value = nullptr;
  RetryPolicy retry;
  TimeoutPolicy timeout;
  unsigned max_iterations = 0;
};
struct EdgeDefinition {
  std::string from, to, condition;
  unsigned max_iterations = 0;
};
struct PipelineDefinition {
  std::string name, source;
  std::string input_schema, output_schema;
  std::map<std::string, std::string> resolved_subpipelines;
  unsigned schema_version = 1, version = 1, max_steps = 1000;
  TimeoutPolicy timeout{Milliseconds{300000}};
  std::map<std::string, NodeDefinition> nodes;
  std::vector<EdgeDefinition> edges;
};
struct BranchFrame {
  std::string group, join;
  unsigned width = 0, index = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BranchFrame, group, join, width, index)
struct ExecutionToken {
  std::string node_id;
  Message message;
  std::vector<BranchFrame> frames;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ExecutionToken, node_id, message, frames)
struct JoinCheckpoint {
  std::map<unsigned, Message> messages;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(JoinCheckpoint, messages)
struct Run {
  std::string id = uuid(), pipeline_id, definition, active_node = "input", created_at = timestamp(),
              updated_at = created_at, actor = "local", error, parent_id, parent_node_id,
              parent_message_id, child_id, child_pipeline_id, initiation_type = "manual",
              session_id, session_turn_id, schedule_id, schedule_occurrence_id, due_at, trigger_id,
              event_id, root_event_id, owner_instance_id, lease_expires_at, claimed_at,
              last_renewed_at, pending_parallel_group, pending_parallel_join;
  unsigned pipeline_version = 1, child_pipeline_version = 0, subpipeline_depth = 0;
  unsigned trigger_depth = 0;
  std::uint64_t fencing_token = 0;
  RunState state = RunState::Queued;
  bool cancellation_requested = false;
  Message message;
  std::map<std::string, unsigned> edge_visits, node_visits;
  std::vector<BranchFrame> frames;
  std::vector<ExecutionToken> ready;
  std::map<std::string, JoinCheckpoint> joins;
  // A nested pipeline returns to this durable parent; empty for root runs.
  std::vector<std::string> child_runs;
  std::map<std::string, std::string> resolved_subpipelines;
  std::string prepared_join;
  std::string provider, model, tool, plugin;
  std::string worker, worker_job_id;
  unsigned steps = 0;
};
inline void to_json(Json &j, const Run &r) {
  j = {{"id", r.id},
       {"pipeline_id", r.pipeline_id},
       {"pipeline_version", r.pipeline_version},
       {"definition", r.definition},
       {"active_node", r.active_node},
       {"created_at", r.created_at},
       {"updated_at", r.updated_at},
       {"actor", r.actor},
       {"error", r.error},
       {"parent_id", r.parent_id},
       {"parent_node_id", r.parent_node_id},
       {"parent_message_id", r.parent_message_id},
       {"session_id", r.session_id},
       {"session_turn_id", r.session_turn_id},
       {"child_id", r.child_id},
       {"child_pipeline_id", r.child_pipeline_id},
       {"child_pipeline_version", r.child_pipeline_version},
       {"subpipeline_depth", r.subpipeline_depth},
       {"initiation_type", r.initiation_type},
       {"schedule_id", r.schedule_id},
       {"schedule_occurrence_id", r.schedule_occurrence_id},
       {"due_at", r.due_at},
       {"trigger_id", r.trigger_id},
       {"event_id", r.event_id},
       {"root_event_id", r.root_event_id},
       {"owner_instance_id", r.owner_instance_id},
       {"fencing_token", r.fencing_token},
       {"lease_expires_at", r.lease_expires_at},
       {"claimed_at", r.claimed_at},
       {"last_renewed_at", r.last_renewed_at},
       {"pending_parallel_group", r.pending_parallel_group},
       {"pending_parallel_join", r.pending_parallel_join},
       {"trigger_depth", r.trigger_depth},
       {"child_runs", r.child_runs},
       {"resolved_subpipelines", r.resolved_subpipelines},
       {"state", r.state},
       {"cancellation_requested", r.cancellation_requested},
       {"message", r.message},
       {"edge_visits", r.edge_visits},
       {"node_visits", r.node_visits},
       {"frames", r.frames},
       {"ready", r.ready},
       {"joins", r.joins},
       {"prepared_join", r.prepared_join},
       {"provider", r.provider},
       {"model", r.model},
       {"tool", r.tool},
       {"plugin", r.plugin},
       {"worker", r.worker},
       {"worker_job_id", r.worker_job_id},
       {"steps", r.steps}};
}
inline void from_json(const Json &j, Run &r) {
  j.at("id").get_to(r.id);
  j.at("pipeline_id").get_to(r.pipeline_id);
  r.pipeline_version = j.value("pipeline_version", 1U);
  j.at("definition").get_to(r.definition);
  j.at("active_node").get_to(r.active_node);
  j.at("created_at").get_to(r.created_at);
  j.at("updated_at").get_to(r.updated_at);
  j.at("actor").get_to(r.actor);
  j.at("error").get_to(r.error);
  j.at("parent_id").get_to(r.parent_id);
  r.parent_node_id = j.value("parent_node_id", std::string{});
  r.parent_message_id = j.value("parent_message_id", std::string{});
  r.session_id = j.value("session_id", std::string{});
  r.session_turn_id = j.value("session_turn_id", std::string{});
  j.at("child_id").get_to(r.child_id);
  r.child_pipeline_id = j.value("child_pipeline_id", std::string{});
  r.child_pipeline_version = j.value("child_pipeline_version", 0U);
  r.subpipeline_depth = j.value("subpipeline_depth", 0U);
  r.initiation_type = j.value("initiation_type", std::string{"manual"});
  r.schedule_id = j.value("schedule_id", std::string{});
  r.schedule_occurrence_id = j.value("schedule_occurrence_id", std::string{});
  r.due_at = j.value("due_at", std::string{});
  r.trigger_id = j.value("trigger_id", std::string{});
  r.event_id = j.value("event_id", std::string{});
  r.root_event_id = j.value("root_event_id", std::string{});
  r.owner_instance_id = j.value("owner_instance_id", std::string{});
  r.fencing_token = j.value("fencing_token", 0ULL);
  r.lease_expires_at = j.value("lease_expires_at", std::string{});
  r.claimed_at = j.value("claimed_at", std::string{});
  r.last_renewed_at = j.value("last_renewed_at", std::string{});
  r.pending_parallel_group = j.value("pending_parallel_group", std::string{});
  r.pending_parallel_join = j.value("pending_parallel_join", std::string{});
  r.trigger_depth = j.value("trigger_depth", 0U);
  r.child_runs = j.value("child_runs", std::vector<std::string>{});
  r.resolved_subpipelines = j.value("resolved_subpipelines", std::map<std::string, std::string>{});
  j.at("state").get_to(r.state);
  j.at("cancellation_requested").get_to(r.cancellation_requested);
  j.at("message").get_to(r.message);
  j.at("edge_visits").get_to(r.edge_visits);
  j.at("node_visits").get_to(r.node_visits);
  j.at("frames").get_to(r.frames);
  j.at("ready").get_to(r.ready);
  j.at("joins").get_to(r.joins);
  j.at("prepared_join").get_to(r.prepared_join);
  j.at("provider").get_to(r.provider);
  j.at("model").get_to(r.model);
  j.at("tool").get_to(r.tool);
  j.at("plugin").get_to(r.plugin);
  r.worker = j.value("worker", std::string{});
  r.worker_job_id = j.value("worker_job_id", std::string{});
  j.at("steps").get_to(r.steps);
}
struct NodeExecution {
  std::string id = uuid(), run_id, node_id, started_at = timestamp(), finished_at, error,
              child_run_id, child_pipeline_id;
  std::string worker_job_id, worker_id, external_job_id;
  unsigned child_pipeline_version = 0;
  unsigned attempt = 1;
  NodeState state = NodeState::Running;
  double duration_ms = 0;
};
inline void to_json(Json &j, const NodeExecution &a) {
  j = {{"id", a.id},
       {"run_id", a.run_id},
       {"node_id", a.node_id},
       {"started_at", a.started_at},
       {"finished_at", a.finished_at},
       {"error", a.error},
       {"child_run_id", a.child_run_id},
       {"child_pipeline_id", a.child_pipeline_id},
       {"child_pipeline_version", a.child_pipeline_version},
       {"worker_job_id", a.worker_job_id},
       {"worker_id", a.worker_id},
       {"external_job_id", a.external_job_id},
       {"attempt", a.attempt},
       {"state", a.state},
       {"duration_ms", a.duration_ms}};
}
inline void from_json(const Json &j, NodeExecution &a) {
  j.at("id").get_to(a.id);
  j.at("run_id").get_to(a.run_id);
  j.at("node_id").get_to(a.node_id);
  j.at("started_at").get_to(a.started_at);
  j.at("finished_at").get_to(a.finished_at);
  j.at("error").get_to(a.error);
  a.child_run_id = j.value("child_run_id", std::string{});
  a.child_pipeline_id = j.value("child_pipeline_id", std::string{});
  a.child_pipeline_version = j.value("child_pipeline_version", 0U);
  a.worker_job_id = j.value("worker_job_id", std::string{});
  a.worker_id = j.value("worker_id", std::string{});
  a.external_job_id = j.value("external_job_id", std::string{});
  j.at("attempt").get_to(a.attempt);
  j.at("state").get_to(a.state);
  j.at("duration_ms").get_to(a.duration_ms);
}
enum class NodeWorkState { Queued, Running, Completed, Failed, Cancelled };
NLOHMANN_JSON_SERIALIZE_ENUM(NodeWorkState, {{NodeWorkState::Queued, "Queued"},
                                             {NodeWorkState::Running, "Running"},
                                             {NodeWorkState::Completed, "Completed"},
                                             {NodeWorkState::Failed, "Failed"},
                                             {NodeWorkState::Cancelled, "Cancelled"}})
inline bool terminal(NodeWorkState state) {
  return state == NodeWorkState::Completed || state == NodeWorkState::Failed ||
         state == NodeWorkState::Cancelled;
}
inline bool valid_node_work_transition(NodeWorkState from, NodeWorkState to) {
  if (from == to)
    return true;
  if (from == NodeWorkState::Queued && to == NodeWorkState::Running)
    return true;
  if (from == NodeWorkState::Queued && to == NodeWorkState::Cancelled)
    return true;
  // A fenced recovery pass may requeue a running attempt after its durable
  // worker job has reached a terminal state.  The old owner is fenced by the
  // same commit, so it cannot subsequently publish a result for this work.
  if (from == NodeWorkState::Running && to == NodeWorkState::Queued)
    return true;
  if (from == NodeWorkState::Running &&
      (to == NodeWorkState::Completed || to == NodeWorkState::Failed ||
       to == NodeWorkState::Cancelled))
    return true;
  return false;
}
struct NodeWork {
  std::string id = uuid(), run_id, group_id, node_id, join, created_at = timestamp(),
              updated_at = created_at, owner_instance_id, lease_expires_at, claimed_at,
              last_renewed_at, attempt_id, error, required_worker_id, required_capability;
  unsigned index = 0, attempt = 0, steps = 0;
  std::uint64_t fencing_token = 0;
  NodeWorkState state = NodeWorkState::Queued;
  ExecutionToken token;
  std::optional<Message> result;
};
inline void to_json(Json &j, const NodeWork &w) {
  j = {{"id", w.id},
       {"run_id", w.run_id},
       {"group_id", w.group_id},
       {"node_id", w.node_id},
       {"join", w.join},
       {"created_at", w.created_at},
       {"updated_at", w.updated_at},
       {"owner_instance_id", w.owner_instance_id},
       {"lease_expires_at", w.lease_expires_at},
       {"claimed_at", w.claimed_at},
       {"last_renewed_at", w.last_renewed_at},
       {"attempt_id", w.attempt_id},
       {"error", w.error},
       {"index", w.index},
       {"attempt", w.attempt},
       {"steps", w.steps},
       {"fencing_token", w.fencing_token},
       {"state", w.state},
       {"token", w.token},
       {"required_worker_id", w.required_worker_id},
       {"required_capability", w.required_capability}};
  if (w.result)
    j["result"] = *w.result;
}
inline void from_json(const Json &j, NodeWork &w) {
  w.id = j.value("id", uuid());
  w.run_id = j.value("run_id", std::string{});
  w.group_id = j.value("group_id", std::string{});
  w.node_id = j.value("node_id", std::string{});
  w.join = j.value("join", std::string{});
  w.created_at = j.value("created_at", timestamp());
  w.updated_at = j.value("updated_at", timestamp());
  w.owner_instance_id = j.value("owner_instance_id", std::string{});
  w.lease_expires_at = j.value("lease_expires_at", std::string{});
  w.claimed_at = j.value("claimed_at", std::string{});
  w.last_renewed_at = j.value("last_renewed_at", std::string{});
  w.attempt_id = j.value("attempt_id", std::string{});
  w.error = j.value("error", std::string{});
  w.index = j.value("index", 0U);
  w.attempt = j.value("attempt", 0U);
  w.steps = j.value("steps", 0U);
  w.fencing_token = j.value("fencing_token", 0ULL);
  w.state = j.value("state", NodeWorkState::Queued);
  w.required_worker_id = j.value("required_worker_id", std::string{});
  w.required_capability = j.value("required_capability", std::string{});
  j.at("token").get_to(w.token);
  if (j.contains("result") && !j.at("result").is_null())
    w.result = j.at("result").get<Message>();
  else
    w.result.reset();
}
inline bool equivalent_terminal_node_work(const NodeWork &left, const NodeWork &right) {
  const auto same_result =
      (!left.result && !right.result) ||
      (left.result && right.result && Json(*left.result) == Json(*right.result));
  if (left.id != right.id || left.run_id != right.run_id || left.group_id != right.group_id ||
      left.node_id != right.node_id || left.join != right.join ||
      left.required_worker_id != right.required_worker_id ||
      left.required_capability != right.required_capability ||
      left.owner_instance_id != right.owner_instance_id || left.state != right.state ||
      left.attempt != right.attempt || left.attempt_id != right.attempt_id ||
      left.fencing_token != right.fencing_token || left.error != right.error ||
      left.steps != right.steps || Json(left.token) != Json(right.token) || !same_result)
    return false;
  return left.state == NodeWorkState::Completed || left.state == NodeWorkState::Failed ||
         left.state == NodeWorkState::Cancelled;
}
struct Approval {
  std::string id = uuid(), run_id, node_id, reason, action, created_at = timestamp(),
              decision = "pending", decided_at, actor, comment;
  unsigned visit = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Approval, id, run_id, node_id, reason, action, created_at,
                                   decision, decided_at, actor, comment, visit)
struct Event {
  std::string id = uuid(), run_id, pipeline_id, node_id, type, time = timestamp();
  std::string causation_id, root_event_id;
  unsigned trigger_depth = 0;
  std::string source_id, source_plugin, source_component, external_event_id, occurred_at,
      ingested_at;
  Json payload = Json::object();
  Json metadata = Json::object();
};
// Durable identity for a sequence of agent inputs. Provider state is kept out
// of the public session record.
struct AgentSession {
  std::string id = uuid(), pipeline_id, state = "open", created_at = timestamp(),
              updated_at = created_at, active_turn_id, active_run_id;
  std::uint64_t dispatch_generation = 0;
  std::uint64_t next_sequence = 1;
};
inline void to_json(Json &j, const AgentSession &s) {
  j = {{"id", s.id},
       {"pipeline_id", s.pipeline_id},
       {"state", s.state},
       {"created_at", s.created_at},
       {"updated_at", s.updated_at},
       {"next_sequence", s.next_sequence},
       {"active_turn_id", s.active_turn_id},
       {"active_run_id", s.active_run_id},
       {"dispatch_generation", s.dispatch_generation}};
}
inline void from_json(const Json &j, AgentSession &s) {
  s.id = j.value("id", uuid());
  s.pipeline_id = j.value("pipeline_id", std::string{});
  s.state = j.value("state", std::string{"open"});
  s.created_at = j.value("created_at", timestamp());
  s.updated_at = j.value("updated_at", s.created_at);
  s.next_sequence = j.value("next_sequence", std::uint64_t{1});
  s.active_turn_id = j.value("active_turn_id", std::string{});
  s.active_run_id = j.value("active_run_id", std::string{});
  s.dispatch_generation = j.value("dispatch_generation", std::uint64_t{0});
}
inline void to_json(Json &j, const Event &e) {
  j = {{"id", e.id},
       {"run_id", e.run_id},
       {"pipeline_id", e.pipeline_id},
       {"node_id", e.node_id},
       {"type", e.type},
       {"time", e.time},
       {"causation_id", e.causation_id},
       {"root_event_id", e.root_event_id},
       {"trigger_depth", e.trigger_depth},
       {"source_id", e.source_id},
       {"source_plugin", e.source_plugin},
       {"source_component", e.source_component},
       {"external_event_id", e.external_event_id},
       {"occurred_at", e.occurred_at},
       {"ingested_at", e.ingested_at},
       {"payload", e.payload},
       {"metadata", e.metadata}};
}
inline void from_json(const Json &j, Event &e) {
  e.id = j.value("id", uuid());
  e.run_id = j.value("run_id", std::string{});
  e.pipeline_id = j.value("pipeline_id", std::string{});
  e.node_id = j.value("node_id", std::string{});
  e.type = j.value("type", std::string{});
  e.time = j.value("time", timestamp());
  e.causation_id = j.value("causation_id", std::string{});
  e.root_event_id = j.value("root_event_id", std::string{});
  e.trigger_depth = j.value("trigger_depth", 0U);
  e.source_id = j.value("source_id", std::string{});
  e.source_plugin = j.value("source_plugin", std::string{});
  e.source_component = j.value("source_component", std::string{});
  e.external_event_id = j.value("external_event_id", std::string{});
  e.occurred_at = j.value("occurred_at", std::string{});
  e.ingested_at = j.value("ingested_at", std::string{});
  e.payload = j.value("payload", Json::object());
  e.metadata = j.value("metadata", Json::object());
}
struct Artifact {
  std::string id = uuid(), run_id, node_id, name, media_type, location, object_id, sha256,
              created_at = timestamp();
  std::uint64_t size = 0;
  Json metadata = Json::object();
};
inline void to_json(Json &j, const Artifact &a) {
  j = {{"id", a.id},
       {"run_id", a.run_id},
       {"node_id", a.node_id},
       {"name", a.name},
       {"media_type", a.media_type},
       {"location", a.location},
       {"object_id", a.object_id},
       {"sha256", a.sha256},
       {"size", a.size},
       {"created_at", a.created_at},
       {"metadata", a.metadata}};
}
inline void from_json(const Json &j, Artifact &a) {
  a.id = j.value("id", uuid());
  a.run_id = j.value("run_id", std::string{});
  a.node_id = j.value("node_id", std::string{});
  a.name = j.value("name", std::string{});
  a.media_type = j.value("media_type", std::string{});
  a.location = j.value("location", std::string{});
  a.object_id = j.value("object_id", std::string{});
  a.sha256 = j.value("sha256", std::string{});
  a.size = j.value("size", std::uint64_t{0});
  a.created_at = j.value("created_at", timestamp());
  a.metadata = j.value("metadata", Json::object());
}
} // namespace laso
