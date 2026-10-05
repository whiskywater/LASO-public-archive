#include <algorithm>
#include <laso/core/config.hpp>
#include <laso/nodes/node.hpp>

namespace laso {
Task<NodeResult> FunctionNode::execute(ExecutionContext &c, const Message &input) {
  c.check();
  auto message = input;
  message.payload = co_await (*function_)(c, input.payload);
  c.check();
  co_return NodeResult{message, {}};
}
Task<NodeResult> AgentNode::execute(ExecutionContext &c, const Message &input) {
  auto permit = co_await limiter_.acquire(c);
  ModelRequest request{binding_.model, prompt_, input.payload, binding_.options};
  auto response = co_await provider_->generate(request, c);
  c.check();
  auto message = input;
  message.payload = std::move(response.output);
  message.provenance.push_back({c.node_id, "", response.model, response.provider, "", input.id,
                                "untrusted", timestamp(), ""});
  co_return NodeResult{message, {}};
}
Task<NodeResult> ToolNode::execute(ExecutionContext &c, const Message &input) {
  auto permit = co_await limiter_.acquire(c);
  ToolContext context{c};
  ToolRequest request{input.payload};
  auto result = co_await tool_->invoke(request, context);
  c.check();
  auto message = input;
  message.payload = std::move(result.output);
  message.provenance.push_back(
      {c.node_id, tool_->metadata().name, "", "", "", input.id, "", timestamp(), ""});
  co_return NodeResult{message, {}};
}
Task<NodeResult> WorkerNode::execute(ExecutionContext &c, const Message &input) {
  c.check();
  if (!manager_)
    throw Error(ErrorCode::Execution, "Worker manager is unavailable");
  WorkerRequest request;
  request.worker_id = worker_id_;
  request.capability = capability_;
  request.task_type = task_type_;
  request.instructions = instructions_;
  request.idempotency_key = c.run_id + ":" + c.node_id + ":" + std::to_string(c.attempt);
  if (!c.distributed_attempt_id.empty())
    request.idempotency_key += ":" + c.distributed_attempt_id;
  request.deadline = timestamp();
  request.run_id = c.run_id;
  request.node_id = c.node_id;
  request.attempt = c.attempt;
  const auto remaining =
      std::chrono::duration_cast<Milliseconds>(c.deadline - std::chrono::steady_clock::now());
  request.timeout_ms = static_cast<std::uint64_t>(std::max<std::int64_t>(remaining.count(), 1));
  request.input = input.payload;
  request.output_schema =
      output_schema_.empty() ? Json::object() : Json{{"reference", output_schema_}};
  request.metadata = input.metadata;
  if (!c.distributed_work_id.empty())
    request.metadata["node_work_id"] = c.distributed_work_id;
  if (!c.distributed_attempt_id.empty())
    request.metadata["node_work_attempt_id"] = c.distributed_attempt_id;
  WorkerJob current;
  try {
    // Process-backed workers may block while their provider obtains an
    // external handle.  Keep that wait off the LASO execution/lease loop so
    // cancellation, renewal, and ownership fencing remain live.  The
    // durable id is known before submission, which also lets cancellation
    // interrupt an in-flight local process-group submission.
    current.id = manager_->job_id_for(request.idempotency_key);
    current = manager_->submit_async(request);
    if (c.worker_job_started)
      c.worker_job_started(current.id);
    for (;;) {
      current = manager_->refresh(current.id);
      if (current.state == WorkerJobState::Completed) {
        auto message = input;
        message.payload = current.result;
        message.metadata["worker_id"] = current.worker_id;
        message.metadata["worker_job_id"] = current.id;
        message.metadata["external_job_id"] = current.external_job_id;
        message.metadata["worker_result_metadata"] = current.result_metadata;
        message.metadata["worker_usage"] = current.usage;
        message.metadata["worker_failure_kind"] = current.failure_kind;
        if (!current.artifacts.empty())
          message.metadata["worker_artifacts"] = current.artifacts;
        message.provenance.push_back(
            {c.node_id, "", "", "", "", input.id, "", timestamp(), current.worker_id});
        co_return NodeResult{std::move(message), {}};
      }
      if (current.state == WorkerJobState::Failed)
        log_diagnostic("worker.node_terminal", {{"worker_job_id", current.id},
                                                {"state", current.state},
                                                {"worker_id", current.worker_id}});
      if (current.state == WorkerJobState::Failed)
        throw Error(ErrorCode::Execution,
                    current.error.empty() ? "Worker job failed" : current.error,
                    {{"worker_job_id", current.id},
                     {"worker_id", current.worker_id},
                     {"external_job_id", current.external_job_id}});
      if (current.state == WorkerJobState::Cancelled)
        log_diagnostic("worker.node_terminal", {{"worker_job_id", current.id},
                                                {"state", current.state},
                                                {"worker_id", current.worker_id}});
      if (current.state == WorkerJobState::Cancelled)
        throw Error(ErrorCode::Cancellation, "Worker job was cancelled",
                    {{"worker_job_id", current.id},
                     {"worker_id", current.worker_id},
                     {"external_job_id", current.external_job_id}});
      if (current.state == WorkerJobState::TimedOut)
        log_diagnostic("worker.node_terminal", {{"worker_job_id", current.id},
                                                {"state", current.state},
                                                {"worker_id", current.worker_id}});
      if (current.state == WorkerJobState::TimedOut)
        throw Error(ErrorCode::Timeout, "Worker job exceeded its deadline",
                    {{"worker_job_id", current.id},
                     {"worker_id", current.worker_id},
                     {"external_job_id", current.external_job_id}});
      if (current.state == WorkerJobState::Unknown)
        throw Error(ErrorCode::Execution, "Worker job recovery state is unknown",
                    {{"worker_job_id", current.id},
                     {"worker_id", current.worker_id},
                     {"external_job_id", current.external_job_id}});
      c.check();
      co_await c.delay(Milliseconds{10});
    }
  } catch (const Error &error) {
    log_diagnostic("worker.node_error", {{"worker_job_id", current.id},
                                         {"error_code", error.code},
                                         {"worker_id", current.worker_id}});
    if (error.code == ErrorCode::Cancellation || error.code == ErrorCode::Timeout) {
      try {
        if (!current.id.empty())
          manager_->cancel(current.id,
                           error.code == ErrorCode::Timeout ? WorkerJobState::TimedOut
                                                            : WorkerJobState::Cancelled,
                           error.what());
        log_diagnostic("worker.node_cancellation_processed",
                       {{"worker_job_id", current.id}, {"worker_id", current.worker_id}});
      } catch (...) { // NOLINT(bugprone-empty-catch): preserve the original cancellation error.
        log_diagnostic("worker.node_cancellation_failed",
                       {{"worker_job_id", current.id}, {"worker_id", current.worker_id}});
      }
    }
    throw;
  }
}
Task<NodeResult> RouterNode::execute(ExecutionContext &c, const Message &input) {
  c.check();
  bool accepted = input.payload.is_object() && input.payload.contains(definition_.field) &&
                  input.payload.at(definition_.field) == definition_.value;
  auto message = input;
  if (type() == "validator")
    message.provenance.push_back(
        {c.node_id, "", "", "", "", input.id, accepted ? "accepted" : "rejected", timestamp(), ""});
  co_return NodeResult{message, accepted ? "accepted" : "rejected"};
}
Task<NodeResult> ValidatorNode::execute(ExecutionContext &c, const Message &input) {
  if (definition_.schema.empty())
    co_return co_await RouterNode::execute(c, input);
  c.check();
  schemas_->validate(definition_.schema, input.payload, c.node_id, "validator");
  auto message = input;
  message.provenance.push_back({c.node_id, "", "", "", "", input.id, "validated", timestamp(), ""});
  co_return NodeResult{std::move(message), "accepted"};
}
void register_functions(FunctionRegistry &r) {
  r.add("identity",
        std::make_shared<Function>([](ExecutionContext &c, const Json &input) -> Task<Json> {
          c.check();
          co_return input;
        }));
  r.add("hello",
        std::make_shared<Function>([](ExecutionContext &c, const Json &input) -> Task<Json> {
          c.check();
          co_return Json{{"greeting", "Hello from LASO"}, {"input", input}};
        }));
}
} // namespace laso
