#include <cmath>
#include <laso/workers/process_protocol.hpp>
#include <laso/workers/worker.hpp>
#include <stdexcept>

namespace laso {
bool worker_job_terminal(WorkerJobState state) {
  return state == WorkerJobState::Completed || state == WorkerJobState::Failed ||
         state == WorkerJobState::Cancelled || state == WorkerJobState::TimedOut;
}

bool valid_worker_job_transition(WorkerJobState from, WorkerJobState to) {
  if (worker_job_terminal(from))
    return false;
  switch (from) {
  case WorkerJobState::Created:
    return to == WorkerJobState::Submitting || to == WorkerJobState::Failed ||
           to == WorkerJobState::Cancelled || to == WorkerJobState::Unknown;
  case WorkerJobState::Submitting:
    return to == WorkerJobState::Queued || to == WorkerJobState::Running ||
           to == WorkerJobState::Completed || to == WorkerJobState::Failed ||
           to == WorkerJobState::Cancelled || to == WorkerJobState::TimedOut ||
           to == WorkerJobState::Unknown;
  case WorkerJobState::Queued:
  case WorkerJobState::Running:
  case WorkerJobState::Waiting:
  case WorkerJobState::Unknown:
    return to == WorkerJobState::Running || to == WorkerJobState::Waiting ||
           to == WorkerJobState::Completed || to == WorkerJobState::Failed ||
           to == WorkerJobState::Cancelled || to == WorkerJobState::TimedOut ||
           to == WorkerJobState::Unknown;
  default:
    return false;
  }
}

void to_json(Json &j, const WorkerUsage &usage) {
  j = Json::object();
  if (usage.queue_duration_ms)
    j["queue_duration_ms"] = *usage.queue_duration_ms;
  if (usage.wall_duration_ms)
    j["wall_duration_ms"] = *usage.wall_duration_ms;
  if (usage.input_tokens)
    j["input_tokens"] = *usage.input_tokens;
  if (usage.output_tokens)
    j["output_tokens"] = *usage.output_tokens;
  if (usage.total_tokens)
    j["total_tokens"] = *usage.total_tokens;
  if (usage.tool_calls)
    j["tool_calls"] = *usage.tool_calls;
  if (usage.action_count)
    j["action_count"] = *usage.action_count;
  if (usage.cost_units)
    j["cost_units"] = *usage.cost_units;
  if (!usage.provider.empty())
    j["provider"] = usage.provider;
  if (!usage.model.empty())
    j["model"] = usage.model;
  if (!usage.executor.empty())
    j["executor"] = usage.executor;
  if (!usage.metadata.empty())
    j["metadata"] = usage.metadata;
}

void from_json(const Json &j, WorkerUsage &usage) {
  if (!j.is_object())
    throw Error(ErrorCode::Validation, "Worker usage must be an object");
  auto optional_uint = [&](const char *name) -> std::optional<std::uint64_t> {
    if (!j.contains(name) || j.at(name).is_null())
      return std::nullopt;
    return j.at(name).get<std::uint64_t>();
  };
  usage.queue_duration_ms = optional_uint("queue_duration_ms");
  usage.wall_duration_ms = optional_uint("wall_duration_ms");
  usage.input_tokens = optional_uint("input_tokens");
  usage.output_tokens = optional_uint("output_tokens");
  usage.total_tokens = optional_uint("total_tokens");
  usage.tool_calls = optional_uint("tool_calls");
  usage.action_count = optional_uint("action_count");
  if (j.contains("cost_units") && !j.at("cost_units").is_null()) {
    usage.cost_units = j.at("cost_units").get<double>();
    if (!std::isfinite(*usage.cost_units) || *usage.cost_units < 0)
      throw Error(ErrorCode::Validation, "Worker cost_units must be finite and non-negative");
  } else {
    usage.cost_units = std::nullopt;
  }
  usage.provider = j.value("provider", std::string{});
  usage.model = j.value("model", std::string{});
  usage.executor = j.value("executor", std::string{});
  usage.metadata = j.value("metadata", Json::object());
  if (!usage.metadata.is_object())
    throw Error(ErrorCode::Validation, "Worker usage metadata must be an object");
}

void to_json(Json &j, const WorkerMetadata &m) {
  j = {{"id", m.id},
       {"name", m.name},
       {"version", m.version},
       {"description", m.description},
       {"plugin", m.plugin},
       {"event_schema", m.event_schema},
       {"event_source_id", m.event_source_id},
       {"status", m.status},
       {"capabilities", m.capabilities},
       {"local", m.local},
       {"remote", m.remote},
       {"healthy", m.healthy},
       {"enabled", m.enabled},
       {"supports_recovery", m.supports_recovery},
       {"supports_cancellation", m.supports_cancellation}};
}

void from_json(const Json &j, WorkerMetadata &m) {
  m.id = j.value("id", std::string{});
  m.name = j.value("name", std::string{});
  m.version = j.value("version", std::string{});
  m.description = j.value("description", std::string{});
  m.plugin = j.value("plugin", std::string{});
  m.event_schema = j.value("event_schema", std::string{});
  m.event_source_id = j.value("event_source_id", std::string{});
  m.status = j.value("status", std::string{"disabled"});
  m.capabilities = j.value("capabilities", std::vector<std::string>{});
  m.local = j.value("local", true);
  m.remote = j.value("remote", false);
  m.healthy = j.value("healthy", false);
  m.enabled = j.value("enabled", false);
  m.supports_recovery = j.value("supports_recovery", false);
  m.supports_cancellation = j.value("supports_cancellation", false);
}

void to_json(Json &j, const WorkerJob &job) {
  j = {{"id", job.id},
       {"worker_id", job.worker_id},
       {"run_id", job.run_id},
       {"node_id", job.node_id},
       {"idempotency_key", job.idempotency_key},
       {"external_job_id", job.external_job_id},
       {"attempt", job.attempt},
       {"status", job.state},
       {"failure_kind", job.failure_kind},
       {"submitted_at", job.submitted_at},
       {"started_at", job.started_at},
       {"completed_at", job.completed_at},
       {"error", job.error},
       {"cancellation_error", job.cancellation_error},
       {"cancellation_requested", job.cancellation_requested},
       {"cancellation_acknowledged", job.cancellation_acknowledged},
       {"request_metadata", job.request_metadata},
       {"result", job.result},
       {"result_metadata", job.result_metadata},
       {"artifacts", job.artifacts},
       {"usage", job.usage}};
}

void from_json(const Json &j, WorkerJob &job) {
  j.at("id").get_to(job.id);
  j.at("worker_id").get_to(job.worker_id);
  j.at("run_id").get_to(job.run_id);
  j.at("node_id").get_to(job.node_id);
  job.idempotency_key = j.value("idempotency_key", std::string{});
  job.external_job_id = j.value("external_job_id", std::string{});
  job.attempt = j.value("attempt", 1U);
  j.at("status").get_to(job.state);
  job.failure_kind = j.value("failure_kind", WorkerFailureKind::None);
  job.submitted_at = j.value("submitted_at", std::string{});
  job.started_at = j.value("started_at", std::string{});
  job.completed_at = j.value("completed_at", std::string{});
  job.error = j.value("error", std::string{});
  job.cancellation_error = j.value("cancellation_error", std::string{});
  job.cancellation_requested = j.value("cancellation_requested", false);
  job.cancellation_acknowledged = j.value("cancellation_acknowledged", false);
  job.request_metadata = j.value("request_metadata", Json::object());
  job.result = j.value("result", Json::object());
  job.result_metadata = j.value("result_metadata", Json::object());
  job.artifacts = j.value("artifacts", std::vector<Json>{});
  if (j.contains("usage"))
    job.usage = j.at("usage").get<WorkerUsage>();
}

void to_json(Json &j, const WorkerInteractionRequest &request) {
  j = {{"protocol_version", process_protocol::version},
       {"request_id", request.request_id},
       {"worker_job_id", request.worker_job_id},
       {"worker_id", request.worker_id},
       {"external_job_id", request.external_job_id},
       {"session_id", request.session_id},
       {"request_type", request.type},
       {"title", request.title},
       {"summary", request.summary},
       {"payload", request.payload},
       {"created_at", request.created_at},
       {"deadline", request.deadline},
       {"risk", request.risk},
       {"category", request.category}};
}

void from_json(const Json &j, WorkerInteractionRequest &request) {
  request.request_id = j.value("request_id", std::string{});
  request.worker_job_id = j.value("worker_job_id", std::string{});
  request.worker_id = j.value("worker_id", std::string{});
  request.external_job_id = j.value("external_job_id", std::string{});
  request.session_id = j.value("session_id", std::string{});
  const auto type = j.value("request_type", std::string{"question"});
  if (type == "approval")
    request.type = WorkerInteractionType::Approval;
  else if (type == "permission")
    request.type = WorkerInteractionType::Permission;
  else if (type == "question")
    request.type = WorkerInteractionType::Question;
  else
    throw std::invalid_argument("unknown worker interaction type");
  request.title = j.value("title", std::string{});
  request.summary = j.value("summary", std::string{});
  request.payload = j.value("payload", Json::object());
  request.created_at = j.value("created_at", std::string{});
  request.deadline = j.value("deadline", std::string{});
  request.risk = j.value("risk", std::string{});
  request.category = j.value("category", std::string{});
}

void to_json(Json &j, const WorkerInteractionResponse &response) {
  j = {{"request_id", response.request_id},
       {"decision", response.state},
       {"payload", response.payload},
       {"reason", response.reason}};
}

void from_json(const Json &j, WorkerInteractionResponse &response) {
  response.request_id = j.value("request_id", std::string{});
  const auto decision = j.value("decision", std::string{"denied"});
  if (decision == "approved")
    response.state = WorkerInteractionState::Approved;
  else if (decision == "denied")
    response.state = WorkerInteractionState::Denied;
  else if (decision == "answered")
    response.state = WorkerInteractionState::Answered;
  else if (decision == "cancelled")
    response.state = WorkerInteractionState::Cancelled;
  else if (decision == "expired")
    response.state = WorkerInteractionState::Expired;
  else
    throw std::invalid_argument("unknown worker interaction decision");
  response.payload = j.value("payload", Json::object());
  response.reason = j.value("reason", std::string{});
}

void to_json(Json &j, const WorkerInteraction &interaction) {
  j = {{"id", interaction.id},
       {"worker_job_id", interaction.worker_job_id},
       {"worker_id", interaction.worker_id},
       {"run_id", interaction.run_id},
       {"external_job_id", interaction.external_job_id},
       {"session_id", interaction.session_id},
       {"request_type", interaction.type},
       {"state", interaction.state},
       {"title", interaction.title},
       {"summary", interaction.summary},
       {"payload", interaction.payload},
       {"response", interaction.response},
       {"created_at", interaction.created_at},
       {"deadline", interaction.deadline},
       {"risk", interaction.risk},
       {"category", interaction.category},
       {"reason", interaction.reason},
       {"actor", interaction.actor},
       {"decided_at", interaction.decided_at}};
}

void from_json(const Json &j, WorkerInteraction &interaction) {
  j.at("id").get_to(interaction.id);
  interaction.worker_job_id = j.value("worker_job_id", std::string{});
  interaction.worker_id = j.value("worker_id", std::string{});
  interaction.run_id = j.value("run_id", std::string{});
  interaction.external_job_id = j.value("external_job_id", std::string{});
  interaction.session_id = j.value("session_id", std::string{});
  const auto type = j.value("request_type", std::string{"question"});
  if (type == "approval")
    interaction.type = WorkerInteractionType::Approval;
  else if (type == "permission")
    interaction.type = WorkerInteractionType::Permission;
  else if (type == "question")
    interaction.type = WorkerInteractionType::Question;
  else
    throw std::invalid_argument("unknown worker interaction type");
  const auto state = j.value("state", std::string{"pending"});
  if (state == "pending")
    interaction.state = WorkerInteractionState::Pending;
  else if (state == "approved")
    interaction.state = WorkerInteractionState::Approved;
  else if (state == "denied")
    interaction.state = WorkerInteractionState::Denied;
  else if (state == "answered")
    interaction.state = WorkerInteractionState::Answered;
  else if (state == "cancelled")
    interaction.state = WorkerInteractionState::Cancelled;
  else if (state == "expired")
    interaction.state = WorkerInteractionState::Expired;
  else
    throw std::invalid_argument("unknown worker interaction state");
  interaction.title = j.value("title", std::string{});
  interaction.summary = j.value("summary", std::string{});
  interaction.payload = j.value("payload", Json::object());
  interaction.response = j.value("response", Json::object());
  interaction.created_at = j.value("created_at", std::string{});
  interaction.deadline = j.value("deadline", std::string{});
  interaction.risk = j.value("risk", std::string{});
  interaction.category = j.value("category", std::string{});
  interaction.reason = j.value("reason", std::string{});
  interaction.actor = j.value("actor", std::string{});
  interaction.decided_at = j.value("decided_at", std::string{});
}
} // namespace laso
