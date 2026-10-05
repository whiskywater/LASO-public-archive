#include <algorithm>
#include <laso/pipeline/parser.hpp>
#include <laso/runtime/runtime.hpp>

namespace laso {
namespace {
struct SessionContinuationCandidates {
  std::mutex mutex;
  std::map<std::string, OpaqueProviderContinuation> by_provider;
};

std::string session_continuation_id(const std::string &session_id, const std::string &provider_id) {
  return "current:" + std::to_string(session_id.size()) + ":" + session_id + ":" + provider_id;
}

std::string run_continuation_candidate_id(const std::string &run_id,
                                          const std::string &provider_id) {
  return "candidate:" + std::to_string(run_id.size()) + ":" + run_id + ":" + provider_id;
}

Json object_workspace_manifest(const Json &manifest, ArtifactStore &store,
                               const std::string &run_id, const std::string &node_id) {
  if (manifest.value("version", 1U) != 1U)
    return manifest;
  WorkspaceManifestLimits limits;
  validate_workspace_manifest(manifest, limits);
  Json result{{"version", 2}, {"storage", "content-addressed"}, {"files", Json::array()}};
  for (const auto &entry : manifest.at("files")) {
    const auto data = entry.at("data").get<std::vector<unsigned char>>();
    std::vector<std::byte> bytes;
    bytes.reserve(data.size());
    for (const auto value : data)
      bytes.push_back(static_cast<std::byte>(value));
    Artifact artifact;
    artifact.run_id = run_id;
    artifact.node_id = node_id;
    artifact.name = entry.at("path").get<std::string>();
    artifact.media_type = "application/octet-stream";
    artifact.metadata = Json{{"purpose", "workspace-input"}};
    const auto saved = store.put(std::move(artifact), bytes);
    result["files"].push_back({{"path", entry.at("path")},
                               {"object_id", saved.object_id},
                               {"sha256", saved.sha256},
                               {"size", saved.size}});
  }
  validate_workspace_manifest(result, limits);
  return result;
}

bool worker_requirement(const PipelineDefinition &pipeline, const ExecutionToken &token,
                        std::string &worker_id, std::string &capability) {
  std::vector<std::string> pending{token.node_id};
  std::set<std::string> visited;
  bool found = false;
  while (!pending.empty()) {
    const auto node_id = std::move(pending.back());
    pending.pop_back();
    if (!visited.insert(node_id).second)
      continue;
    const auto &node = pipeline.nodes.at(node_id);
    if (node.type == "join")
      continue;
    if (node.type == "worker") {
      if (found && (worker_id != node.binding || capability != node.capability))
        return false;
      worker_id = node.binding;
      capability = node.capability;
      found = true;
    }
    for (const auto &edge : pipeline.edges)
      if (edge.from == node_id)
        pending.push_back(edge.to);
  }
  return true;
}

void remove_ephemeral_workspace_paths(Json &value) {
  if (value.is_array()) {
    for (auto &item : value)
      remove_ephemeral_workspace_paths(item);
    return;
  }
  if (!value.is_object())
    return;
  for (const auto &key : {"project_dir", "workspace_path", "cwd"})
    value.erase(key);
  for (auto &[key, item] : value.items()) {
    (void)key;
    remove_ephemeral_workspace_paths(item);
  }
}

std::string validation_detail(const Error &error) {
  if (error.code != ErrorCode::Validation || !error.details.is_object())
    return {};
  auto bounded = [](const Json &value, std::size_t maximum) {
    if (!value.is_string())
      return std::string{};
    auto result = value.get<std::string>();
    if (result.size() > maximum)
      result.resize(maximum);
    return result;
  };
  std::string result = error.what();
  const auto schema = bounded(error.details.value("schema", Json{}), 512);
  const auto direction = bounded(error.details.value("direction", Json{}), 32);
  const auto instance = bounded(error.details.value("instance_path", Json{}), 1024);
  const auto schema_path = bounded(error.details.value("schema_path", Json{}), 1024);
  const auto message = bounded(error.details.value("message", Json{}), 512);
  if (!schema.empty())
    result += " (schema=" + schema;
  else
    result += " (";
  if (!direction.empty())
    result += ", direction=" + direction;
  if (!instance.empty())
    result += ", instance_path=" + instance;
  if (!schema_path.empty())
    result += ", schema_path=" + schema_path;
  if (!message.empty())
    result += ", message=" + message;
  result += ")";
  return result;
}
} // namespace

struct Runtime::ParallelState {
  ParallelState(std::size_t width, unsigned initial_steps) : outputs(width), steps(initial_steps) {}
  std::mutex mutex;
  std::vector<std::optional<Message>> outputs;
  std::size_t completed = 0;
  unsigned steps = 0;
  bool failed = false;
  std::string error;
  ErrorCode code = ErrorCode::Execution;
  std::stop_source stop;
};

Task<void> Runtime::execute_branch(const PipelineDefinition &pipeline, ExecutionToken token,
                                   std::shared_ptr<ParallelState> state,
                                   std::shared_ptr<AsyncLimiter> run_nodes,
                                   std::chrono::steady_clock::time_point pipeline_deadline,
                                   unsigned subpipeline_depth,
                                   std::optional<LeaseRecord> work_lease, std::string work_id,
                                   std::string work_attempt_id, std::string session_id) {
  auto persist = [&](const std::vector<Record> &records) {
    if (work_lease) {
      NodeWork work;
      work.id = work_id;
      deps_.storage.commit_owned(records, "node:" + work_id, work_lease->owner_instance,
                                 work_lease->fencing_token);
    } else {
      deps_.storage.commit(records);
    }
  };
  try {
    Run branch;
    branch.id = token.message.run_id;
    branch.pipeline_id = token.message.pipeline_id;
    branch.definition = pipeline.source;
    branch.active_node = token.node_id;
    branch.message = std::move(token.message);
    branch.frames = std::move(token.frames);
    branch.actor = "parallel";
    branch.subpipeline_depth = subpipeline_depth;
    branch.session_id = std::move(session_id);
    for (;;) {
      const auto &definition = pipeline.nodes.at(branch.active_node);
      if (definition.type == "join") {
        const auto index =
            work_id.empty() && !branch.frames.empty() ? branch.frames.back().index : 0U;
        std::lock_guard lock(state->mutex);
        if (index >= state->outputs.size() || state->outputs[index].has_value())
          throw Error(ErrorCode::Execution, "Parallel branch arrived twice");
        state->outputs[index] = std::move(branch.message);
        ++state->completed;
        co_return;
      }
      {
        std::lock_guard lock(state->mutex);
        if (state->steps >= pipeline.max_steps)
          throw Error(ErrorCode::Execution, "Pipeline step limit exceeded");
        ++state->steps;
      }
      ExecutionContext context{branch.id, branch.pipeline_id, definition.id,
                               state->stop.get_token(), pipeline_deadline};
      context.session_id = branch.session_id;
      context.distributed_work_id = work_id;
      context.distributed_attempt_id = work_attempt_id;
      context.check();
      const auto policy = permission(definition, branch);
      if (policy.decision == PolicyDecision::Deny)
        throw Error(ErrorCode::Policy, "Operation denied by policy");
      if (policy.decision == PolicyDecision::RequireApproval || definition.type == "approval")
        throw Error(ErrorCode::Policy, "Approval is not supported inside a concurrent branch");
      context.visit = branch.node_visits[definition.id] + 1;
      auto node = make_node(definition);
      bool succeeded = false;
      for (unsigned attempt_number = 1; attempt_number <= definition.retry.max_attempts;
           ++attempt_number) {
        const auto started = std::chrono::steady_clock::now();
        context.attempt = attempt_number;
        context.deadline = std::min(pipeline_deadline,
                                    std::chrono::steady_clock::now() + definition.timeout.timeout);
        if (definition.type == "tool")
          context.deadline = std::min(context.deadline,
                                      std::chrono::steady_clock::now() +
                                          deps_.tools.get(definition.binding)->metadata().timeout);
        if (definition.type == "agent") {
          const auto binding = config_.models.find(definition.binding);
          if (binding == config_.models.end())
            throw Error(ErrorCode::Provider, "Logical model not configured");
          context.deadline =
              std::min(context.deadline,
                       std::chrono::steady_clock::now() +
                           deps_.providers.get(binding->second.provider)->metadata().timeout);
        }
        bool retry = false;
        NodeExecution attempt;
        attempt.run_id = branch.id;
        attempt.node_id = definition.id;
        if (attempt_number == 1 && !work_attempt_id.empty())
          attempt.id = work_attempt_id;
        attempt.attempt = attempt_number;
        persist({{RecordKind::Attempt, attempt.id, branch.id, Json(attempt)}});
        context.worker_job_started = [&](const std::string &worker_job_id) {
          attempt.worker_job_id = worker_job_id;
          persist({{RecordKind::Attempt, attempt.id, branch.id, Json(attempt)}});
        };
        try {
          NodeResult result;
          if (definition.type == "subpipeline") {
            deps_.schemas.validate(definition.input_schema, branch.message.payload, definition.id,
                                   "input");
            if (subpipeline_depth >= config_.max_subpipeline_depth)
              throw Error(ErrorCode::Execution, "Subpipeline nesting limit exceeded");
            const auto resolved = pipeline.resolved_subpipelines.contains(definition.id)
                                      ? pipeline.resolved_subpipelines.at(definition.id)
                                      : definition.binding;
            if (!deps_.resolve_pipeline)
              throw Error(ErrorCode::Execution, "Pipeline resolver is unavailable");
            auto child_pipeline = deps_.resolve_pipeline(resolved);
            const auto child_id =
                run(child_pipeline, branch.message.payload, branch.actor, branch.id, definition.id,
                    subpipeline_depth + 1, branch.message.id);
            attempt.child_run_id = child_id;
            attempt.child_pipeline_id = child_pipeline.name;
            attempt.child_pipeline_version = child_pipeline.version;
            for (;;) {
              context.check();
              const auto child = deps_.storage.get(RecordKind::Run, child_id).get<Run>();
              if (terminal(child.state)) {
                if (child.state != RunState::Completed)
                  throw Error(ErrorCode::Execution, "Subpipeline did not complete successfully");
                result.message = child.message;
                break;
              }
              co_await context.delay(Milliseconds{5});
            }
          } else {
            auto global_slot = co_await nodes_.acquire(context);
            auto run_slot = co_await run_nodes->acquire(context);
            deps_.schemas.validate(definition.input_schema, branch.message.payload, definition.id,
                                   "input");
            result = co_await node->execute(context, branch.message);
          }
          context.check();
          deps_.schemas.validate(definition.output_schema, result.message.payload, definition.id,
                                 "output");
          if (definition.type == "output")
            deps_.schemas.validate(pipeline.output_schema, result.message.payload, pipeline.name,
                                   "pipeline_output");
          attempt.state = NodeState::Completed;
          attempt.finished_at = timestamp();
          attempt.duration_ms =
              std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started)
                  .count();
          ++branch.node_visits[definition.id];
          auto parent = branch.message.id;
          branch.message = std::move(result.message);
          branch.message.id = uuid();
          branch.message.run_id = branch.id;
          branch.message.pipeline_id = branch.pipeline_id;
          branch.message.node_id = definition.id;
          branch.message.time = timestamp();
          branch.message.provenance.push_back(
              {definition.id, "", "", "", "", parent, "", timestamp(), ""});
          persist({{RecordKind::Attempt, attempt.id, branch.id, Json(attempt)},
                   {RecordKind::Message, branch.message.id, branch.id, Json(branch.message)}});
          std::vector<const EdgeDefinition *> edges;
          for (const auto &edge : pipeline.edges)
            if (edge.from == definition.id &&
                (edge.condition.empty() || edge.condition == result.condition))
              edges.push_back(&edge);
          if (edges.size() != 1)
            throw Error(ErrorCode::Execution, "Parallel branch has ambiguous routing");
          const auto &edge = *edges.front();
          const auto key = edge.from + "->" + edge.to;
          if (edge.max_iterations && ++branch.edge_visits[key] > edge.max_iterations)
            throw Error(ErrorCode::Execution, "Edge iteration limit exceeded");
          branch.active_node = edge.to;
          succeeded = true;
          break;
        } catch (const Error &error) {
          attempt.state = error.code == ErrorCode::Cancellation ? NodeState::Cancelled
                          : error.code == ErrorCode::Timeout    ? NodeState::TimedOut
                                                                : NodeState::Failed;
          const auto detail = validation_detail(error);
          attempt.error = attempt.state == NodeState::Cancelled  ? "Node cancelled"
                          : attempt.state == NodeState::TimedOut ? "Node deadline exceeded"
                          : detail.empty()                       ? "Node execution failed"
                                                                 : detail;
          attempt.finished_at = timestamp();
          attempt.duration_ms =
              std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started)
                  .count();
          persist({{RecordKind::Attempt, attempt.id, branch.id, Json(attempt)}});
          if (attempt.state == NodeState::Cancelled ||
              attempt_number == definition.retry.max_attempts)
            throw;
          retry = true;
        }
        if (retry)
          co_await context.delay(definition.retry.delay);
      }
      if (!succeeded)
        throw Error(ErrorCode::Execution, "Parallel branch did not complete");
    }
  } catch (const Error &error) {
    std::lock_guard lock(state->mutex);
    if (!state->failed) {
      state->failed = true;
      state->error = error.what();
      state->code = error.code;
    }
    ++state->completed;
    state->stop.request_stop();
  } catch (...) {
    std::lock_guard lock(state->mutex);
    if (!state->failed) {
      state->failed = true;
      state->error = "Parallel branch failed";
    }
    ++state->completed;
    state->stop.request_stop();
  }
}

Task<void> Runtime::execute_distributed_work(NodeWork work, LeaseRecord work_lease,
                                             std::optional<LeaseRecord> global_slot,
                                             std::optional<LeaseRecord> run_slot,
                                             std::stop_token stop) {
  (void)global_slot;
  (void)run_slot;
  try {
    const auto run = deps_.storage.get(RecordKind::Run, work.run_id).get<Run>();
    if (run.cancellation_requested || terminal(run.state)) {
      auto cancelled = deps_.storage.get(RecordKind::NodeWork, work.id).get<NodeWork>();
      cancelled.state = NodeWorkState::Cancelled;
      cancelled.error = "Parent run cancellation requested";
      cancelled.updated_at = timestamp();
      commit_node_owned({{RecordKind::NodeWork, cancelled.id, cancelled.run_id, Json(cancelled)}},
                        cancelled, work_lease);
      co_return;
    }
    auto extensions = deps_.nodes.names();
    auto pipeline = parse_pipeline(run.definition, {extensions.begin(), extensions.end()});
    pipeline.resolved_subpipelines = run.resolved_subpipelines;
    auto branch_state = std::make_shared<ParallelState>(1, work.steps);
    auto run_nodes = std::make_shared<AsyncLimiter>(config_.max_nodes_per_run);
    std::stop_callback parent_stop(stop, [branch_state] { branch_state->stop.request_stop(); });
    auto token = std::move(work.token);
    std::optional<std::filesystem::path> staged_workspace;
    if (!work.required_worker_id.empty() || !work.required_capability.empty()) {
      token.message.metadata.erase("project_dir");
      if (token.message.metadata.contains("workspace_manifest")) {
        staged_workspace = stage_workspace(
            token.message.metadata.at("workspace_manifest"), deps_.workspace_root, work.run_id,
            work.id, work.attempt_id, WorkspaceManifestLimits{}, deps_.artifacts);
        token.message.metadata.erase("workspace_manifest");
        token.message.metadata["project_dir"] = staged_workspace->string();
      }
    }
    co_await execute_branch(pipeline, std::move(token), branch_state, run_nodes,
                            std::chrono::steady_clock::now() + pipeline.timeout.timeout,
                            run.subpipeline_depth, work_lease, work.id, work.attempt_id,
                            run.session_id);
    NodeWork completed = deps_.storage.get(RecordKind::NodeWork, work.id).get<NodeWork>();
    const auto current_run = deps_.storage.get(RecordKind::Run, work.run_id).get<Run>();
    if (current_run.cancellation_requested || terminal(current_run.state)) {
      completed.state = NodeWorkState::Cancelled;
      completed.error = "Parent run cancellation requested";
      completed.updated_at = timestamp();
      commit_node_owned({{RecordKind::NodeWork, completed.id, completed.run_id, Json(completed)}},
                        completed, work_lease);
      co_return;
    }
    {
      std::lock_guard lock(branch_state->mutex);
      if (branch_state->failed)
        throw Error(branch_state->code, branch_state->error);
      if (!branch_state->outputs.front())
        throw Error(ErrorCode::Execution, "Distributed node work produced no output");
      completed.result = *branch_state->outputs.front();
      completed.steps = branch_state->steps;
    }
    if (staged_workspace)
      completed.result->metadata["workspace_result_manifest"] =
          deps_.artifacts
              ? workspace_manifest(*staged_workspace, *deps_.artifacts, WorkspaceManifestLimits{},
                                   work.run_id, work.id, work.attempt_id, work_lease.owner_instance,
                                   work_lease.fencing_token)
              : workspace_manifest(*staged_workspace);
    if (!work.required_worker_id.empty() || !work.required_capability.empty())
      remove_ephemeral_workspace_paths(completed.result->metadata);
    completed.state = NodeWorkState::Completed;
    completed.owner_instance_id = work_lease.owner_instance;
    completed.fencing_token = work_lease.fencing_token;
    completed.updated_at = timestamp();
    completed.error.clear();
    commit_node_owned({{RecordKind::NodeWork, completed.id, completed.run_id, Json(completed)}},
                      completed, work_lease);
  } catch (const Error &error) {
    if (error.code == ErrorCode::Storage) {
      // A transient database failure is not a provider or workload failure.
      // Leave the durable NodeWork attempt recoverable; the lease loop and a
      // replacement owner will reconcile it after the current fence expires.
      log_diagnostic("runtime.distributed_node_deferred_after_storage_error",
                     {{"node_work_id", work.id}, {"fencing_token", work_lease.fencing_token}});
      co_return;
    }
    if (error.code == ErrorCode::Conflict)
      log_diagnostic("runtime.distributed_node_commit_rejected",
                     {{"node_work_id", work.id},
                      {"fencing_token", work_lease.fencing_token},
                      {"owner_instance", work_lease.owner_instance}});
    try {
      auto failed = deps_.storage.get(RecordKind::NodeWork, work.id).get<NodeWork>();
      failed.state =
          error.code == ErrorCode::Cancellation ? NodeWorkState::Cancelled : NodeWorkState::Failed;
      failed.error = error.what();
      failed.updated_at = timestamp();
      failed.owner_instance_id = work_lease.owner_instance;
      failed.fencing_token = work_lease.fencing_token;
      commit_node_owned({{RecordKind::NodeWork, failed.id, failed.run_id, Json(failed)}}, failed,
                        work_lease);
    } catch (const Error &) {
      // A lost fencing token is expected when another instance has taken over.
    }
  } catch (...) {
    try {
      auto failed = deps_.storage.get(RecordKind::NodeWork, work.id).get<NodeWork>();
      failed.state = NodeWorkState::Failed;
      failed.error = "Distributed node work failed";
      failed.updated_at = timestamp();
      failed.owner_instance_id = work_lease.owner_instance;
      failed.fencing_token = work_lease.fencing_token;
      commit_node_owned({{RecordKind::NodeWork, failed.id, failed.run_id, Json(failed)}}, failed,
                        work_lease);
    } catch (const Error &) {
    }
  }
  co_return;
}

Task<bool> Runtime::execute_parallel(Run &run, const PipelineDefinition &pipeline,
                                     std::shared_ptr<AsyncLimiter> run_nodes,
                                     std::stop_token parent,
                                     std::chrono::steady_clock::time_point pipeline_deadline,
                                     unsigned subpipeline_depth) {
  std::vector<ExecutionToken> tokens;
  tokens.swap(run.ready);
  auto distributable = [&](const ExecutionToken &token) {
    std::vector<std::string> pending{token.node_id};
    std::set<std::string> visited;
    while (!pending.empty()) {
      const auto node_id = std::move(pending.back());
      pending.pop_back();
      if (!visited.insert(node_id).second)
        continue;
      const auto &node = pipeline.nodes.at(node_id);
      if (node.type == "join")
        continue;
      if (node.type != "function" && node.type != "validator" && node.type != "router" &&
          node.type != "worker")
        return false;
      for (const auto &edge : pipeline.edges)
        if (edge.from == node_id)
          pending.push_back(edge.to);
    }
    return true;
  };
  if (deps_.coordination && std::all_of(tokens.begin(), tokens.end(), distributable)) {
    const auto group = uuid();
    const auto join = pipeline.nodes.at(run.active_node).join;
    std::vector<Record> records;
    records.reserve(tokens.size());
    for (std::size_t index = 0; index < tokens.size(); ++index) {
      NodeWork work;
      work.id = group + ":" + std::to_string(index);
      work.run_id = run.id;
      work.group_id = group;
      work.node_id = tokens[index].node_id;
      work.join = join;
      work.index = static_cast<unsigned>(index);
      work.token = std::move(tokens[index]);
      work.steps = run.steps;
      if (deps_.artifacts && work.token.message.metadata.contains("workspace_manifest"))
        work.token.message.metadata["workspace_manifest"] =
            object_workspace_manifest(work.token.message.metadata.at("workspace_manifest"),
                                      *deps_.artifacts, run.id, work.id);
      if (!worker_requirement(pipeline, work.token, work.required_worker_id,
                              work.required_capability))
        throw Error(ErrorCode::Execution,
                    "Distributed branch requires incompatible worker capabilities");
      records.push_back({RecordKind::NodeWork, work.id, run.id, Json(work)});
    }
    run.pending_parallel_group = group;
    run.pending_parallel_join = join;
    run.state = RunState::Paused;
    checkpoint(run, "parallel.waiting", std::move(records));
    co_return true;
  }
  auto state = std::make_shared<ParallelState>(tokens.size(), run.steps);
  std::stop_callback parent_stop(parent, [state] { state->stop.request_stop(); });
  for (auto &token : tokens)
    asio::co_spawn(io_,
                   execute_branch(pipeline, std::move(token), state, run_nodes, pipeline_deadline,
                                  subpipeline_depth, std::nullopt, {}, {}, run.session_id),
                   asio::detached);
  while (true) {
    {
      std::lock_guard lock(state->mutex);
      if (state->completed == state->outputs.size()) {
        run.steps = state->steps;
        if (state->failed)
          throw Error(state->code, state->error);
        break;
      }
    }
    asio::steady_timer timer(co_await asio::this_coro::executor, Milliseconds{2});
    co_await timer.async_wait(asio::use_awaitable);
  }
  run.message.payload = Json::array();
  for (const auto &output : state->outputs) {
    if (!output)
      throw Error(ErrorCode::Execution, "Parallel branch output missing");
    run.message.payload.push_back(output->payload);
    run.message.provenance.insert(run.message.provenance.end(), output->provenance.begin(),
                                  output->provenance.end());
  }
  run.frames.clear();
  run.active_node = pipeline.nodes.at(run.active_node).join;
  run.prepared_join = run.active_node;
  co_return false;
}

Task<void> Runtime::execute(Run r, std::stop_token stop) {
  auto extensions = deps_.nodes.names();
  auto pipeline = parse_pipeline(r.definition, {extensions.begin(), extensions.end()});
  pipeline.resolved_subpipelines = r.resolved_subpipelines;
  const auto deadline = std::chrono::steady_clock::now() + pipeline.timeout.timeout;
  auto run_nodes = std::make_shared<AsyncLimiter>(config_.max_nodes_per_run);
  auto continuation_candidates = std::make_shared<SessionContinuationCandidates>();
  bool attempt_recorded = false;
  try {
    {
      std::lock_guard lock(mutex_);
      const auto current = deps_.storage.get(RecordKind::Run, r.id).get<Run>();
      if (terminal(current.state))
        co_return;
      r = current;
      if (r.cancellation_requested) {
        transition(r, RunState::Cancelled, "run.cancelled");
        co_return;
      }
      transition(r, RunState::Starting, "run.starting");
      transition(r, RunState::Running, "run.started");
      reconcile_distributed_parallel(r, pipeline);
    }
    while (true) {
      attempt_recorded = false;
      ExecutionContext context{r.id, r.pipeline_id, r.active_node, stop, deadline};
      if (!r.session_id.empty()) {
        context.session_id = r.session_id;
        const auto session_id = r.session_id;
        const auto run_id = r.id;
        context.load_provider_continuation =
            [this, session_id,
             run_id](const std::string &provider_id) -> std::optional<OpaqueProviderContinuation> {
          const auto read =
              [this, &session_id, &provider_id](
                  const std::string &id,
                  const std::string &scope) -> std::optional<OpaqueProviderContinuation> {
            try {
              const auto value = deps_.storage.get(RecordKind::SessionContinuation, id);
              if (value.value("scope", std::string{}) != scope ||
                  value.value("session_id", std::string{}) != session_id ||
                  value.value("provider_id", std::string{}) != provider_id)
                throw Error(ErrorCode::Storage, "Stored provider continuation is invalid");
              const auto state = value.value("state", std::string{});
              if (state.empty() || state.size() > 64 * 1024)
                throw Error(ErrorCode::Storage, "Stored provider continuation is invalid");
              return OpaqueProviderContinuation{
                  provider_id, value.value("provider_version", std::string{}), state};
            } catch (const Error &error) {
              if (error.code == ErrorCode::NotFound)
                return std::nullopt;
              throw;
            }
          };
          if (auto candidate =
                  read(run_continuation_candidate_id(run_id, provider_id), "candidate"))
            return candidate;
          return read(session_continuation_id(session_id, provider_id), "current");
        };
        context.stage_provider_continuation = [continuation_candidates](
                                                  OpaqueProviderContinuation continuation) {
          if (continuation.provider_id.empty() || continuation.provider_id.size() > 256 ||
              continuation.provider_version.empty() || continuation.provider_version.size() > 128 ||
              continuation.state.empty() || continuation.state.size() > 64 * 1024)
            throw Error(ErrorCode::Provider, "Provider returned invalid continuation state");
          std::lock_guard lock(continuation_candidates->mutex);
          continuation_candidates->by_provider[continuation.provider_id] = std::move(continuation);
        };
      }
      context.check();
      const auto &definition = pipeline.nodes.at(r.active_node);
      r.provider.clear();
      r.model.clear();
      r.tool.clear();
      r.plugin.clear();
      if (definition.type == "agent" && config_.models.contains(definition.binding)) {
        const auto &binding = config_.models.at(definition.binding);
        r.provider = binding.provider;
        r.model = binding.model;
      } else if (definition.type == "tool") {
        r.tool = definition.binding;
        r.plugin = deps_.tools.get(definition.binding)->metadata().plugin;
      } else if (definition.type == "worker" && deps_.workers) {
        r.worker = deps_.workers->resolve_worker(definition.binding, definition.capability);
      }
      context.deadline =
          std::min(deadline, std::chrono::steady_clock::now() + definition.timeout.timeout);
      if (!prepare_join(r, definition))
        continue;
      if (r.steps >= pipeline.max_steps)
        throw Error(ErrorCode::Execution, "Pipeline step limit exceeded");
      context.visit =
          (r.node_visits.contains(definition.id) ? r.node_visits.at(definition.id) : 0) + 1;
      const auto policy = permission(definition, r);
      if (policy.decision == PolicyDecision::Deny)
        throw Error(ErrorCode::Policy, "Operation denied by policy");
      if ((definition.type == "approval" || policy.decision == PolicyDecision::RequireApproval) &&
          !approved(r)) {
        std::lock_guard lock(mutex_);
        context.check();
        wait_approval(r, definition,
                      definition.type == "approval" ? definition.reason : policy.reason);
        co_return;
      }
      auto node = make_node(definition);
      bool completed = false;
      for (unsigned attempt_number = 1; attempt_number <= definition.retry.max_attempts;
           ++attempt_number) {
        context.attempt = attempt_number;
        context.deadline =
            std::min(deadline, std::chrono::steady_clock::now() + definition.timeout.timeout);
        if (definition.type == "tool")
          context.deadline = std::min(context.deadline,
                                      std::chrono::steady_clock::now() +
                                          deps_.tools.get(definition.binding)->metadata().timeout);
        if (definition.type == "agent")
          context.deadline =
              std::min(context.deadline,
                       std::chrono::steady_clock::now() +
                           deps_.providers.get(config_.models.at(definition.binding).provider)
                               ->metadata()
                               .timeout);
        if (definition.type == "worker")
          transition(r, RunState::WaitingWorker, "worker.submitting");
        NodeExecution attempt;
        attempt.run_id = r.id;
        attempt.node_id = definition.id;
        attempt.attempt = attempt_number;
        auto started = std::chrono::steady_clock::now();
        checkpoint(r, "node.started", {{RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
        attempt_recorded = true;
        if (definition.type == "tool")
          transition(r, RunState::WaitingTool, "tool.called");
        if (definition.type == "agent")
          transition(r, RunState::WaitingModel, "model.called");
        std::optional<ErrorCode> failure;
        std::string failure_detail;
        NodeResult result;
        bool child_result = false;
        try {
          if (definition.type == "subpipeline") {
            deps_.schemas.validate(definition.input_schema, r.message.payload, definition.id,
                                   "input");
            bool child_active = false;
            if (!r.child_id.empty()) {
              const auto existing_child = deps_.storage.get(RecordKind::Run, r.child_id).get<Run>();
              if (existing_child.state == RunState::Completed) {
                result.message = existing_child.message;
                child_result = true;
                attempt.child_run_id = existing_child.id;
                attempt.child_pipeline_id = existing_child.pipeline_id;
                attempt.child_pipeline_version = existing_child.pipeline_version;
              } else if (terminal(existing_child.state)) {
                // Retries create a new immutable child run rather than
                // mutating the failed child execution.
                r.child_id.clear();
              } else {
                child_active = true;
                attempt.child_run_id = existing_child.id;
                attempt.child_pipeline_id = existing_child.pipeline_id;
                attempt.child_pipeline_version = existing_child.pipeline_version;
              }
            }
            if (!child_result && !child_active) {
              if (r.subpipeline_depth >= config_.max_subpipeline_depth)
                throw Error(ErrorCode::Execution, "Subpipeline nesting limit exceeded");
              const auto resolved = pipeline.resolved_subpipelines.contains(definition.id)
                                        ? pipeline.resolved_subpipelines.at(definition.id)
                                        : definition.binding;
              if (!deps_.resolve_pipeline)
                throw Error(ErrorCode::Execution, "Pipeline resolver is unavailable");
              auto child_pipeline = deps_.resolve_pipeline(resolved);
              Json child_origin = {{"initiation_type", r.initiation_type},
                                   {"trigger_depth", r.trigger_depth},
                                   {"root_event_id", r.root_event_id},
                                   {"trigger_id", r.trigger_id},
                                   {"event_id", r.event_id}};
              const auto child_id =
                  run(child_pipeline, r.message.payload, r.actor, r.id, definition.id,
                      r.subpipeline_depth + 1, r.message.id, std::move(child_origin));
              r.child_id = child_id;
              r.child_pipeline_id = child_pipeline.name;
              r.child_pipeline_version = child_pipeline.version;
              r.child_runs.push_back(child_id);
              attempt.child_run_id = child_id;
              attempt.child_pipeline_id = child_pipeline.name;
              attempt.child_pipeline_version = child_pipeline.version;
              checkpoint(r, "subpipeline.started",
                         {{RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
            }
            if (!child_result) {
              if (deps_.coordination) {
                attempt.state = NodeState::WaitingApproval;
                checkpoint(r, "subpipeline.waiting",
                           {{RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
                std::lock_guard lock(mutex_);
                transition(r, RunState::Paused, "subpipeline.waiting");
                co_return;
              }
              for (;;) {
                context.check();
                const auto child = deps_.storage.get(RecordKind::Run, r.child_id).get<Run>();
                if (terminal(child.state)) {
                  if (child.state != RunState::Completed)
                    throw Error(ErrorCode::Execution, "Subpipeline did not complete successfully",
                                {{"child_run_id", child.id},
                                 {"child_pipeline", child.pipeline_id},
                                 {"child_pipeline_version", child.pipeline_version}});
                  result.message = child.message;
                  child_result = true;
                  break;
                }
                if (child.state == RunState::WaitingApproval || child.state == RunState::Paused) {
                  attempt.state = NodeState::WaitingApproval;
                  checkpoint(r, "subpipeline.waiting",
                             {{RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
                  std::lock_guard lock(mutex_);
                  transition(r, RunState::Paused, "subpipeline.waiting");
                  co_return;
                }
                co_await context.delay(Milliseconds{5});
              }
            }
          } else {
            auto global_slot = co_await nodes_.acquire(context);
            auto run_slot = co_await run_nodes->acquire(context);
            deps_.schemas.validate(definition.input_schema, r.message.payload, definition.id,
                                   "input");
            result = co_await node->execute(context, r.message);
          }
          context.check();
          deps_.schemas.validate(definition.output_schema, result.message.payload, definition.id,
                                 "output");
          if (definition.type == "output")
            deps_.schemas.validate(pipeline.output_schema, result.message.payload, pipeline.name,
                                   "pipeline_output");
          if (result.message.payload.dump().size() > max_document_bytes)
            throw Error(ErrorCode::Execution, "Node result exceeds 1 MiB");
        } catch (const Error &e) {
          failure = e.code;
          if (definition.type == "worker") {
            attempt.worker_job_id = e.details.value("worker_job_id", std::string{});
            attempt.worker_id = e.details.value("worker_id", std::string{});
            attempt.external_job_id = e.details.value("external_job_id", std::string{});
          }
          if (e.code == ErrorCode::Validation || definition.type == "subpipeline")
            failure_detail = validation_detail(e);
          if (definition.type == "worker" && failure_detail.empty())
            failure_detail = e.what();
          if (failure_detail.empty() && definition.type == "subpipeline")
            failure_detail = e.what();
        } catch (...) {
          failure = ErrorCode::Execution;
        }
        attempt.finished_at = timestamp();
        attempt.duration_ms =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started)
                .count();
        if (r.state == RunState::WaitingTool || r.state == RunState::WaitingModel ||
            r.state == RunState::WaitingWorker)
          transition(r, RunState::Running,
                     failure ? (definition.type == "tool"     ? "tool.failed"
                                : definition.type == "worker" ? "worker.failed"
                                                              : "model.failed")
                             : (definition.type == "tool"     ? "tool.completed"
                                : definition.type == "worker" ? "worker.completed"
                                                              : "model.completed"));
        if (failure) {
          attempt.state = *failure == ErrorCode::Timeout        ? NodeState::TimedOut
                          : *failure == ErrorCode::Cancellation ? NodeState::Cancelled
                                                                : NodeState::Failed;
          attempt.error = *failure == ErrorCode::Timeout        ? "Node deadline exceeded"
                          : *failure == ErrorCode::Cancellation ? "Node cancelled"
                          : failure_detail.empty()              ? "Node execution failed"
                                                                : failure_detail;
          checkpoint(r, "node.failed", {{RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
          if (*failure == ErrorCode::Cancellation || *failure == ErrorCode::Timeout ||
              attempt_number == definition.retry.max_attempts)
            throw Error(*failure, attempt.error);
          transition(r, RunState::Retrying, "node.retrying");
          context.deadline = deadline;
          co_await context.delay(definition.retry.delay);
          transition(r, RunState::Running, "run.running");
          continue;
        }
        attempt.state = NodeState::Completed;
        if (definition.type == "worker") {
          attempt.worker_job_id = result.message.metadata.value("worker_job_id", std::string{});
          attempt.worker_id = result.message.metadata.value("worker_id", std::string{});
          attempt.external_job_id = result.message.metadata.value("external_job_id", std::string{});
          r.worker_job_id = attempt.worker_job_id;
        }
        ++r.steps;
        ++r.node_visits[definition.id];
        auto parent_message = r.message.id;
        r.message = std::move(result.message);
        r.message.id = uuid();
        r.message.run_id = r.id;
        r.message.pipeline_id = r.pipeline_id;
        r.message.node_id = definition.id;
        r.message.time = timestamp();
        r.message.provenance.push_back(
            {definition.id, "", "", "", "", parent_message, "", timestamp(), ""});
        if (r.message.provenance.size() > 256)
          r.message.provenance.erase(r.message.provenance.begin(),
                                     r.message.provenance.end() - 256);
        std::vector<Record> records{{RecordKind::Attempt, attempt.id, r.id, Json(attempt)},
                                    {RecordKind::Message, r.message.id, r.id, Json(r.message)}};
        if (!r.session_id.empty() && definition.type == "agent") {
          std::lock_guard lock(continuation_candidates->mutex);
          for (const auto &[provider_id, continuation] : continuation_candidates->by_provider) {
            Json candidate{{"scope", "candidate"},
                           {"session_id", r.session_id},
                           {"run_id", r.id},
                           {"turn_id", r.session_turn_id},
                           {"provider_id", provider_id},
                           {"provider_version", continuation.provider_version},
                           {"state", continuation.state}};
            records.push_back({RecordKind::SessionContinuation,
                               run_continuation_candidate_id(r.id, provider_id), r.id,
                               std::move(candidate)});
          }
          continuation_candidates->by_provider.clear();
        }
        if (definition.type == "subpipeline")
          r.child_id.clear();
        bool continuing = false;
        try {
          if (definition.type == "parallel") {
            bool approval_branch = false;
            for (const auto &edge : pipeline.edges)
              if (edge.from == definition.id && pipeline.nodes.at(edge.to).type == "approval")
                approval_branch = true;
            continuing = advance(r, pipeline, definition, result.condition);
            if (approval_branch) {
              // Approval branches need the existing durable pause/resume protocol.
              continuing = next_ready(r);
            } else {
              if (co_await execute_parallel(r, pipeline, run_nodes, stop, deadline,
                                            r.subpipeline_depth)) {
                checkpoint(r, "node.completed", std::move(records));
                co_return;
              }
              result.message = r.message;
              for (auto &record : records)
                if (record.kind == RecordKind::Message && record.id == r.message.id) {
                  record.value = Json(r.message);
                  break;
                }
            }
          } else {
            continuing = advance(r, pipeline, definition, result.condition);
          }
        } catch (...) {
          // Keep the completed node's result and final attempt even when routing fails.
          checkpoint(r, "node.completed", std::move(records));
          throw;
        }
        if (!continuing) {
          transition(r, RunState::Completed, "run.completed", std::move(records));
          co_return;
        }
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
        if (!r.session_id.empty())
          session_test_point(SessionTestPoint::BeforeNodeCheckpointCommit);
#endif
        checkpoint(r, "node.completed", std::move(records));
        completed = true;
        break;
      }
      if (!completed)
        throw Error(ErrorCode::Execution, "Node did not complete");
    }
  } catch (const Error &e) {
    std::lock_guard lock(mutex_);
    if (!attempt_recorded) {
      NodeExecution attempt;
      attempt.run_id = r.id;
      attempt.node_id = r.active_node;
      attempt.state = e.code == ErrorCode::Timeout        ? NodeState::TimedOut
                      : e.code == ErrorCode::Cancellation ? NodeState::Cancelled
                                                          : NodeState::Failed;
      attempt.finished_at = timestamp();
      attempt.error = "Node preparation failed";
      checkpoint(r, "node.failed", {{RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
    }
    if (!r.child_id.empty()) {
      auto child = deps_.storage.get(RecordKind::Run, r.child_id).get<Run>();
      if (!terminal(child.state))
        cancel(child.id);
    }
    r.error = e.code == ErrorCode::Timeout        ? "Execution deadline exceeded"
              : e.code == ErrorCode::Cancellation ? "Execution cancelled"
                                                  : "Pipeline execution failed";
    if (!terminal(r.state))
      transition(r,
                 e.code == ErrorCode::Timeout        ? RunState::TimedOut
                 : e.code == ErrorCode::Cancellation ? RunState::Cancelled
                                                     : RunState::Failed,
                 e.code == ErrorCode::Timeout        ? "run.timed_out"
                 : e.code == ErrorCode::Cancellation ? "run.cancelled"
                                                     : "run.failed");
  } catch (...) {
    std::lock_guard lock(mutex_);
    if (!attempt_recorded) {
      NodeExecution attempt;
      attempt.run_id = r.id;
      attempt.node_id = r.active_node;
      attempt.state = NodeState::Failed;
      attempt.finished_at = timestamp();
      attempt.error = "Node preparation failed";
      checkpoint(r, "node.failed", {{RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
    }
    if (!r.child_id.empty()) {
      auto child = deps_.storage.get(RecordKind::Run, r.child_id).get<Run>();
      if (!terminal(child.state))
        cancel(child.id);
    }
    r.error = "Unexpected pipeline failure";
    if (!terminal(r.state))
      transition(r, RunState::Failed, "run.failed");
  }
}
} // namespace laso
