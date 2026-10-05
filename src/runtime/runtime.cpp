#include <algorithm>
#include <laso/pipeline/parser.hpp>
#include <laso/runtime/runtime.hpp>
#include <limits>

namespace laso {
namespace {
constexpr std::size_t max_message_metadata_bytes = std::size_t{64} * 1024;

std::string session_continuation_id(const std::string &session_id, const std::string &provider_id) {
  return "current:" + std::to_string(session_id.size()) + ":" + session_id + ":" + provider_id;
}

std::string run_continuation_candidate_id(const std::string &run_id,
                                          const std::string &provider_id) {
  return "candidate:" + std::to_string(run_id.size()) + ":" + run_id + ":" + provider_id;
}

std::vector<Json> list_all(const Storage &storage, RecordKind kind, const std::string &run_id) {
  constexpr std::size_t page_size = 10000;
  std::vector<Json> records;
  for (std::size_t offset = 0;;) {
    auto page = storage.list(kind, run_id, page_size, offset);
    records.insert(records.end(), page.begin(), page.end());
    if (page.size() < page_size)
      return records;
    if (offset > std::numeric_limits<std::size_t>::max() - page_size)
      throw Error(ErrorCode::Storage, "Record history is too large to inspect");
    offset += page_size;
  }
}

Json capability_advertisement(const std::shared_ptr<WorkerManager> &workers) {
  Json capabilities{{"protocol_version", 1}, {"features", Json::array({"run-claims", "fencing"})}};
  if (workers) {
    capabilities["features"].push_back("worker-claims");
    capabilities["workers"] = workers->distributed_capabilities().value("workloads", Json::array());
  }
  if (capabilities.dump().size() > 4096)
    throw Error(ErrorCode::Configuration, "Instance capability advertisement is too large");
  return capabilities;
}
} // namespace
Runtime::Runtime(asio::io_context &io, Config config, RuntimeDependencies dependencies)
    : io_(io), config_(std::move(config)), deps_(dependencies), nodes_(config_.max_nodes),
      models_(config_.max_models), tools_(config_.max_tools),
      claim_timer_(std::make_shared<asio::steady_timer>(io)),
      lease_timer_(std::make_shared<asio::steady_timer>(io)),
      session_timer_(std::make_shared<asio::steady_timer>(io)) {}
Runtime::~Runtime() = default; // Owner must drain the executor before destruction.
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
void Runtime::set_session_test_hook(std::function<void(SessionTestPoint)> hook) {
  std::lock_guard lock(session_test_hook_mutex_);
  session_test_hook_ = std::move(hook);
}
void Runtime::session_test_point(SessionTestPoint point) {
  std::function<void(SessionTestPoint)> hook;
  {
    std::lock_guard lock(session_test_hook_mutex_);
    hook = session_test_hook_;
  }
  if (hook)
    hook(point);
}
#endif
void Runtime::start_distributed() {
  if (distributed_started_)
    return;
  distributed_started_ = true;
  if (!deps_.coordination) {
    dispatch_sessions();
    return;
  }
  deps_.coordination->register_instance(version, capability_advertisement(deps_.workers).dump());
  asio::co_spawn(io_, supervise_claim_loop(), asio::detached);
  asio::co_spawn(io_, supervise_lease_loop(), asio::detached);
  asio::co_spawn(io_, supervise_session_loop(), asio::detached);
}
Task<void> Runtime::supervise_claim_loop() {
  for (;;) {
    try {
      co_await claim_loop();
      co_return;
    } catch (...) {
      log_diagnostic("runtime.claim_loop_failed", {{"instance_id", deps_.instance_id}});
    }
    {
      std::lock_guard lock(mutex_);
      if (stopping_)
        co_return;
    }
    asio::steady_timer retry(io_);
    retry.expires_after(Milliseconds{100});
    boost::system::error_code error;
    co_await retry.async_wait(asio::redirect_error(asio::use_awaitable, error));
    if (error)
      co_return;
  }
}
Task<void> Runtime::supervise_lease_loop() {
  for (;;) {
    try {
      co_await lease_loop();
      co_return;
    } catch (...) {
      log_diagnostic("runtime.lease_loop_failed", {{"instance_id", deps_.instance_id}});
    }
    {
      std::lock_guard lock(mutex_);
      if (stopping_)
        co_return;
    }
    asio::steady_timer retry(io_);
    retry.expires_after(Milliseconds{100});
    boost::system::error_code error;
    co_await retry.async_wait(asio::redirect_error(asio::use_awaitable, error));
    if (error)
      co_return;
  }
}
Task<void> Runtime::supervise_session_loop() {
  for (;;) {
    try {
      co_await session_loop();
      co_return;
    } catch (...) {
      log_diagnostic("runtime.session_loop_failed");
    }
    {
      std::lock_guard lock(mutex_);
      if (stopping_)
        co_return;
    }
    asio::steady_timer retry(io_);
    retry.expires_after(Milliseconds{100});
    boost::system::error_code error;
    co_await retry.async_wait(asio::redirect_error(asio::use_awaitable, error));
    if (error)
      co_return;
  }
}

Task<void> Runtime::session_loop() {
  for (;;) {
    {
      std::lock_guard lock(mutex_);
      if (stopping_)
        co_return;
    }
    // Recover accepted turns that were durably queued before this instance
    // started. The normal submission path dispatches immediately; this initial
    // sweep closes the restart window before the periodic fallback begins.
    dispatch_sessions();

    // Submissions and terminal runs dispatch immediately. Keep this periodic
    // sweep as a recovery fallback without repeatedly scanning every session
    // while the queue is idle.
    session_timer_->expires_after(Milliseconds{1000});
    boost::system::error_code error;
    co_await session_timer_->async_wait(asio::redirect_error(asio::use_awaitable, error));
    if (error)
      co_return;
    {
      std::lock_guard lock(mutex_);
      if (stopping_)
        co_return;
    }
    dispatch_sessions();
  }
}

void Runtime::dispatch_sessions() {
  for (const auto &record : list_all(deps_.storage, RecordKind::AgentSession, "")) {
    try {
      dispatch_session(record.at("id").get<std::string>());
    } catch (const Error &error) {
      if (error.code != ErrorCode::Conflict && error.code != ErrorCode::Capacity)
        log_diagnostic("runtime.session_dispatch_failed");
    } catch (...) {
      log_diagnostic("runtime.session_dispatch_failed");
    }
  }
}

void Runtime::dispatch_session(const std::string &session_id) {
  if (session_id.empty())
    return;
  {
    std::lock_guard lock(mutex_);
    if (stopping_)
      return;
  }
  AgentSession session;
  try {
    session = deps_.storage.get(RecordKind::AgentSession, session_id).get<AgentSession>();
  } catch (const Error &error) {
    if (error.code == ErrorCode::NotFound)
      return;
    throw;
  }
  if (session.state == "closing" && !session.active_run_id.empty()) {
    try {
      cancel(session.active_run_id);
    } catch (const Error &error) {
      if (error.code != ErrorCode::Conflict && error.code != ErrorCode::NotFound)
        throw;
    }
    return;
  }
  if (!session.active_run_id.empty()) {
    if (!deps_.coordination) {
      const auto active = deps_.storage.get(RecordKind::Run, session.active_run_id).get<Run>();
      if (active.state == RunState::Queued || active.state == RunState::Paused) {
        try {
          resume(active.id);
        } catch (const Error &error) {
          if (error.code != ErrorCode::Conflict && error.code != ErrorCode::Capacity)
            throw;
        }
      }
    }
    return;
  }
  if (session.state != "open")
    return;

  std::optional<LeaseRecord> lease;
  std::string owner = deps_.instance_id;
  std::uint64_t fence = 0;
  std::string expires_at = timestamp();
  if (deps_.coordination) {
    lease = deps_.coordination->acquire("session:" + session_id, config_.coordination_lease_ttl_ms);
    if (!lease)
      return;
    owner = lease->owner_instance;
    fence = lease->fencing_token;
    expires_at = lease->expires_at;
  }
  auto release = [&] {
    if (lease && deps_.coordination) {
      try {
        deps_.coordination->release(*lease);
      } catch (const Error &) {
      }
      lease.reset();
    }
  };
  try {
    Event claim_event;
    const auto turn = deps_.storage.claim_next_session_turn(session_id, owner, fence, expires_at,
                                                            Json(claim_event));
    if (!turn) {
      release();
      return;
    }
    const auto turn_fence = turn->value("dispatch_fencing_token", std::uint64_t{0});
    if (deps_.coordination && turn_fence != fence)
      throw Error(ErrorCode::Conflict, "Session claim fencing token changed");
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    session_test_point(SessionTestPoint::AfterClaim);
#endif
    const auto pipeline_id = turn->value("pipeline_id", session.pipeline_id);
    const auto pipeline = deps_.resolve_pipeline(pipeline_id);
    run(pipeline, turn->at("input"), "session", "", "", 0, "", Json::object(), Json::object(),
        session_id, turn->at("id").get<std::string>(), owner, fence);
  } catch (...) {
    release();
    throw;
  }
  release();
}

void Runtime::checkpoint(Run &r, const std::string &type, std::vector<Record> records) {
  std::lock_guard lock(mutex_);
  if (!r.session_id.empty() && terminal(r.state)) {
    std::map<std::string, Json> candidates;
    const auto collect = [&](const Json &candidate) {
      if (candidate.value("scope", std::string{}) != "candidate")
        return;
      if (candidate.value("session_id", std::string{}) != r.session_id ||
          candidate.value("run_id", std::string{}) != r.id ||
          candidate.value("turn_id", std::string{}) != r.session_turn_id)
        throw Error(ErrorCode::Storage, "Stored provider continuation is invalid");
      const auto provider_id = candidate.value("provider_id", std::string{});
      const auto provider_version = candidate.value("provider_version", std::string{});
      const auto state = candidate.value("state", std::string{});
      if (provider_id.empty() || provider_version.empty() || state.empty() ||
          state.size() > 64 * 1024)
        throw Error(ErrorCode::Storage, "Stored provider continuation is invalid");
      candidates[provider_id] = candidate;
    };
    for (const auto &candidate :
         deps_.storage.list(RecordKind::SessionContinuation, r.id, 10000, 0))
      collect(candidate);
    for (const auto &record : records)
      if (record.kind == RecordKind::SessionContinuation)
        collect(record.value);
    records.erase(std::remove_if(records.begin(), records.end(),
                                 [](const Record &record) {
                                   return record.kind == RecordKind::SessionContinuation;
                                 }),
                  records.end());
    for (auto &[provider_id, candidate] : candidates) {
      if (r.state == RunState::Completed) {
        Json current = candidate;
        current["scope"] = "current";
        current["run_id"] = r.session_id;
        current["source_run_id"] = r.id;
        records.push_back({RecordKind::SessionContinuation,
                           session_continuation_id(r.session_id, provider_id), r.session_id,
                           std::move(current)});
      }
      candidate["scope"] = "discarded";
      candidate["state"] = "";
      records.push_back({RecordKind::SessionContinuation,
                         run_continuation_candidate_id(r.id, provider_id), r.id,
                         std::move(candidate)});
    }
  }
  try {
    const auto stored = deps_.storage.get(RecordKind::Run, r.id).get<Run>();
    r.cancellation_requested = r.cancellation_requested || stored.cancellation_requested;
  } catch (const Error &error) {
    if (error.code != ErrorCode::NotFound)
      throw;
  }
  r.updated_at = timestamp();
  Event event;
  event.run_id = r.id;
  event.pipeline_id = r.pipeline_id;
  event.node_id = r.active_node;
  event.type = r.state == RunState::Cancelled && type == "run.completed" ? "run.cancelled" : type;
  event.metadata["state"] = r.state;
  event.metadata["provider"] = r.provider;
  event.metadata["model"] = r.model;
  event.metadata["tool"] = r.tool;
  event.metadata["plugin"] = r.plugin;
  event.metadata["pipeline_version"] = r.pipeline_version;
  event.metadata["parent_run_id"] = r.parent_id;
  event.metadata["parent_node_id"] = r.parent_node_id;
  event.metadata["child_run_id"] = r.child_id;
  event.metadata["child_pipeline_id"] = r.child_pipeline_id;
  event.metadata["child_pipeline_version"] = r.child_pipeline_version;
  event.metadata["initiation_type"] = r.initiation_type;
  event.metadata["schedule_id"] = r.schedule_id;
  event.metadata["schedule_occurrence_id"] = r.schedule_occurrence_id;
  event.metadata["due_at"] = r.due_at;
  event.metadata["trigger_id"] = r.trigger_id;
  event.metadata["event_id"] = r.event_id;
  event.metadata["root_event_id"] = r.root_event_id;
  event.metadata["trigger_depth"] = r.trigger_depth;
  event.causation_id = r.id;
  event.root_event_id = r.root_event_id;
  event.trigger_depth = r.trigger_depth;
  for (const auto &record : records)
    if (record.kind == RecordKind::Attempt)
      event.node_id = record.value.at("node_id").get<std::string>();
  records.push_back({RecordKind::Run, r.id, r.id, Json(r)});
  records.push_back({RecordKind::Event, event.id, r.id, Json(event)});
  try {
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    if (!r.session_id.empty() && terminal(r.state))
      session_test_point(SessionTestPoint::BeforeCompletionCommit);
#endif
    if (!r.session_id.empty() && terminal(r.state)) {
      Event session_event;
      deps_.storage.commit_session_run(records, r.owner_instance_id, r.fencing_token,
                                       Json(session_event));
    } else if (deps_.coordination && !r.owner_instance_id.empty()) {
      deps_.storage.commit_owned(records, "run:" + r.id, r.owner_instance_id, r.fencing_token);
    } else {
      deps_.storage.commit(records);
    }
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    if (!r.session_id.empty() && terminal(r.state))
      session_test_point(SessionTestPoint::AfterCompletionCommit);
#endif
  } catch (const Error &error) {
    if (deps_.coordination && error.code == ErrorCode::Conflict) {
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
      if (!r.session_id.empty())
        session_test_point(SessionTestPoint::NodeCheckpointRejected);
#endif
      auto active = active_.find(r.id);
      if (active != active_.end()) {
        active->second.ownership_lost = true;
        active->second.stop.request_stop();
      }
    }
    throw;
  }
  deps_.events.publish(event);
  log_event(event);
}
void Runtime::commit_node_owned(const std::vector<Record> &records, const NodeWork &work,
                                const LeaseRecord &lease) {
  deps_.storage.commit_owned(records, "node:" + work.id, lease.owner_instance, lease.fencing_token);
}
bool Runtime::distributed_parallel_ready(const Run &r) const {
  if (r.pending_parallel_group.empty())
    return false;
  const auto works = deps_.storage.list(RecordKind::NodeWork, r.id, 10000, 0);
  bool found = false;
  for (const auto &value : works) {
    const auto work = value.get<NodeWork>();
    if (work.group_id != r.pending_parallel_group)
      continue;
    found = true;
    if (work.state == NodeWorkState::Queued || work.state == NodeWorkState::Running)
      return false;
  }
  return found;
}
void Runtime::reconcile_distributed_parallel(Run &r, const PipelineDefinition &) {
  if (r.pending_parallel_group.empty())
    return;
  std::vector<NodeWork> works;
  for (const auto &value : deps_.storage.list(RecordKind::NodeWork, r.id, 10000, 0)) {
    auto work = value.get<NodeWork>();
    if (work.group_id == r.pending_parallel_group)
      works.push_back(std::move(work));
  }
  if (works.empty())
    throw Error(ErrorCode::Execution, "Distributed parallel work is missing");
  std::sort(works.begin(), works.end(),
            [](const NodeWork &left, const NodeWork &right) { return left.index < right.index; });
  for (const auto &work : works)
    if (work.state != NodeWorkState::Completed || !work.result)
      throw Error(ErrorCode::Execution,
                  work.error.empty() ? "Distributed parallel branch failed" : work.error);
  r.message.payload = Json::array();
  Json workspace_results = Json::array();
  r.steps = 0;
  for (const auto &work : works) {
    r.message.payload.push_back(work.result->payload);
    r.message.provenance.insert(r.message.provenance.end(), work.result->provenance.begin(),
                                work.result->provenance.end());
    if (work.result->metadata.contains("workspace_result_manifest")) {
      const auto &manifest = work.result->metadata.at("workspace_result_manifest");
      validate_workspace_manifest(manifest);
      if (manifest.value("version", 1U) == 2U) {
        if (!deps_.artifacts)
          throw Error(ErrorCode::Storage, "Object-backed workspace requires an artifact store");
        const auto provenance = manifest.value("provenance", Json::object());
        if (!provenance.is_object() || provenance.value("run_id", std::string{}) != work.run_id ||
            provenance.value("node_work_id", std::string{}) != work.id ||
            provenance.value("attempt_id", std::string{}) != work.attempt_id ||
            provenance.value("fencing_token", std::uint64_t{0}) != work.fencing_token)
          throw Error(ErrorCode::Conflict,
                      "Workspace artifact provenance does not match its fence");
        for (const auto &entry : manifest.at("files"))
          deps_.artifacts->verify(entry.at("object_id").get<std::string>(),
                                  entry.at("sha256").get<std::string>(),
                                  entry.at("size").get<std::uint64_t>());
      }
      workspace_results.push_back(
          {{"work_id", work.id}, {"attempt_id", work.attempt_id}, {"manifest", manifest}});
    }
    r.steps = std::max(r.steps, work.steps);
  }
  if (!workspace_results.empty())
    r.message.metadata["workspace_result_manifests"] = std::move(workspace_results);
  r.frames.clear();
  r.active_node = r.pending_parallel_join;
  r.prepared_join = r.active_node;
  r.pending_parallel_group.clear();
  r.pending_parallel_join.clear();
}
void Runtime::reconcile_terminal_worker(const NodeWork &observed,
                                        const LeaseRecord &observed_lease) {
  if (!deps_.workers || observed.state != NodeWorkState::Running)
    return;
  Run run;
  try {
    run = deps_.storage.get(RecordKind::Run, observed.run_id).get<Run>();
  } catch (const Error &) {
    return;
  }

  NodeExecution attempt;
  bool found = false;
  try {
    for (const auto &record : deps_.storage.list(RecordKind::Attempt, observed.run_id, 10000, 0)) {
      const auto candidate = record.get<NodeExecution>();
      if (candidate.node_id != observed.node_id || candidate.worker_job_id.empty())
        continue;
      if (candidate.id == observed.attempt_id) {
        attempt = candidate;
        found = true;
        break;
      }
      if (observed.attempt_id.empty() && candidate.state == NodeState::Running) {
        attempt = candidate;
        found = true;
      }
    }
  } catch (const Error &) {
    return;
  }
  if (!found)
    return;

  WorkerJob job;
  try {
    job = deps_.workers->job(attempt.worker_job_id);
  } catch (const Error &) {
    return;
  }
  if (!worker_job_terminal(job.state) || job.state == WorkerJobState::Completed)
    return;

  NodeWork current;
  try {
    current = deps_.storage.get(RecordKind::NodeWork, observed.id).get<NodeWork>();
  } catch (const Error &) {
    return;
  }
  if (current.state != NodeWorkState::Running || current.attempt_id != observed.attempt_id)
    return;
  {
    // The active owner still has an execution coroutine that can observe the
    // terminal worker job and apply the normal retry policy.  Requeueing here
    // would race that unwind and could create a second retry for one logical
    // attempt.  Durable reconciliation remains the recovery path once the
    // owner is gone or has lost the in-memory node entry.
    std::lock_guard lock(mutex_);
    if (active_nodes_.contains(current.id))
      return;
  }

  attempt.state = job.state == WorkerJobState::Cancelled  ? NodeState::Cancelled
                  : job.state == WorkerJobState::TimedOut ? NodeState::TimedOut
                                                          : NodeState::Failed;
  attempt.error = job.error.empty()
                      ? (attempt.state == NodeState::TimedOut    ? "Worker job timed out"
                         : attempt.state == NodeState::Cancelled ? "Worker job cancelled"
                                                                 : "Worker job failed")
                      : job.error;
  attempt.finished_at = timestamp();

  bool retry = false;
  try {
    auto extensions = deps_.nodes.names();
    const auto pipeline = parse_pipeline(run.definition, {extensions.begin(), extensions.end()});
    const auto node = pipeline.nodes.find(current.node_id);
    retry = node != pipeline.nodes.end() && current.attempt < node->second.retry.max_attempts &&
            attempt.state != NodeState::Cancelled && !run.cancellation_requested &&
            !terminal(run.state);
  } catch (const Error &) {
    retry = false;
  }

  if (attempt.state == NodeState::Cancelled || run.cancellation_requested || terminal(run.state)) {
    current.state = NodeWorkState::Cancelled;
    current.error = "Worker cancellation reconciled";
  } else if (retry) {
    current.state = NodeWorkState::Queued;
    current.error = "Worker attempt reconciled for retry";
    current.owner_instance_id.clear();
    current.lease_expires_at.clear();
    current.claimed_at.clear();
    current.last_renewed_at.clear();
    current.attempt_id.clear();
    current.fencing_token = 0;
  } else {
    current.state = NodeWorkState::Failed;
    current.error = attempt.error;
  }
  current.updated_at = timestamp();

  const auto records = [&] {
    return std::vector<Record>{{RecordKind::Attempt, attempt.id, attempt.run_id, Json(attempt)},
                               {RecordKind::NodeWork, current.id, current.run_id, Json(current)}};
  };
  std::optional<LeaseRecord> replacement_lease;
  const LeaseRecord *commit_lease = &observed_lease;
  try {
    try {
      deps_.storage.commit_owned(records(), "node:" + current.id, observed_lease.owner_instance,
                                 observed_lease.fencing_token);
    } catch (const Error &error) {
      if (error.code != ErrorCode::Conflict || !deps_.coordination)
        throw;
      // The provider can time out just after the original node lease expires.
      // Reacquire the same resource before reconciling so recovery remains a
      // fenced write instead of weakening ownership checks.
      replacement_lease =
          deps_.coordination->acquire("node:" + current.id, config_.coordination_lease_ttl_ms);
      if (!replacement_lease)
        return;
      current = deps_.storage.get(RecordKind::NodeWork, current.id).get<NodeWork>();
      if (current.state != NodeWorkState::Running || current.attempt_id != observed.attempt_id) {
        deps_.coordination->release(*replacement_lease);
        replacement_lease.reset();
        return;
      }
      // Reapply the decision to the freshly read record before the fenced
      // takeover commit, preserving any state that changed while acquiring.
      if (attempt.state == NodeState::Cancelled || run.cancellation_requested ||
          terminal(run.state)) {
        current.state = NodeWorkState::Cancelled;
        current.error = "Worker cancellation reconciled";
      } else if (retry) {
        current.state = NodeWorkState::Queued;
        current.error = "Worker attempt reconciled for retry";
        current.owner_instance_id.clear();
        current.lease_expires_at.clear();
        current.claimed_at.clear();
        current.last_renewed_at.clear();
        current.attempt_id.clear();
        current.fencing_token = 0;
      } else {
        current.state = NodeWorkState::Failed;
        current.error = attempt.error;
      }
      current.updated_at = timestamp();
      deps_.storage.commit_owned(records(), "node:" + current.id, replacement_lease->owner_instance,
                                 replacement_lease->fencing_token);
      commit_lease = &*replacement_lease;
    }
    log_diagnostic("runtime.worker_terminal_reconciled", {{"node_work_id", current.id},
                                                          {"attempt_id", attempt.id},
                                                          {"worker_job_id", job.id},
                                                          {"worker_state", job.state},
                                                          {"node_work_state", current.state},
                                                          {"retry", retry}});
    std::optional<LeaseRecord> old_global_slot;
    std::optional<LeaseRecord> old_run_slot;
    std::optional<LeaseRecord> old_work_lease;
    {
      std::lock_guard lock(mutex_);
      if (const auto active = active_nodes_.find(current.id); active != active_nodes_.end()) {
        active->second.stop.request_stop();
        old_global_slot = active->second.global_slot;
        old_run_slot = active->second.run_slot;
        old_work_lease = active->second.work_lease;
        // The worker job is already terminal.  Do not let the unwinding
        // coroutine hold a scheduler slot and delay the replacement claim.
        // Its later commit is still fenced by the old work lease.
        active_nodes_.erase(active);
      }
    }
    for (const auto &lease : {old_run_slot, old_global_slot, old_work_lease})
      if (lease) {
        try {
          deps_.coordination->release(*lease);
        } catch (const Error &) {
        }
      }
    try {
      deps_.coordination->release(*commit_lease);
    } catch (const Error &) {
    }
  } catch (const Error &error) {
    if (replacement_lease) {
      try {
        deps_.coordination->release(*replacement_lease);
      } catch (const Error &) {
      }
    }
    if (error.code != ErrorCode::Conflict)
      log_diagnostic("runtime.worker_terminal_reconcile_failed", {{"node_work_id", current.id}});
  }
}
void Runtime::transition(Run &r, RunState state, const std::string &event,
                         std::vector<Record> records) {
  if (!valid_transition(r.state, state))
    throw Error(ErrorCode::Execution, "Invalid run state transition");
  r.state = state;
  checkpoint(r, event, std::move(records));
}
std::string Runtime::run(const PipelineDefinition &p, Json input, std::string actor,
                         std::string parent_id, std::string parent_node_id,
                         unsigned subpipeline_depth, std::string parent_message_id, Json origin,
                         Json message_metadata, std::string session_id, std::string session_turn_id,
                         std::string session_owner, std::uint64_t session_fencing_token) {
  std::lock_guard lock(mutex_);
  if (stopping_ || (!deps_.coordination && active_.size() >= config_.max_runs))
    throw Error(ErrorCode::Capacity, "Concurrent run limit reached");
  if (deps_.coordination) {
    std::size_t pending = 0;
    for (std::size_t offset = 0;;) {
      const auto page = deps_.storage.list(RecordKind::Run, "", 1000, offset);
      for (const auto &record : page)
        if (record.get<Run>().state == RunState::Queued && ++pending >= config_.max_pending_runs)
          throw Error(ErrorCode::Capacity, "Distributed run queue is full");
      if (page.size() < 1000)
        break;
      if (offset > std::numeric_limits<std::size_t>::max() - 1000)
        throw Error(ErrorCode::Storage, "Distributed run queue is too large to inspect");
      offset += 1000;
    }
  }
  if (input.dump().size() > max_document_bytes)
    throw Error(ErrorCode::Validation, "Run input exceeds 1 MiB");
  if (!message_metadata.is_object() || message_metadata.dump().size() > max_message_metadata_bytes)
    throw Error(ErrorCode::Validation, "Run metadata is invalid or exceeds 64 KiB");
  // Do not execute a caller-modified graph that differs from the durable source.
  auto names = deps_.nodes.names();
  auto checked = parse_pipeline(p.source, {names.begin(), names.end()});
  checked.resolved_subpipelines = p.resolved_subpipelines;
  if (subpipeline_depth > config_.max_subpipeline_depth)
    throw Error(ErrorCode::Execution, "Subpipeline nesting limit exceeded");
  deps_.schemas.validate(checked.input_schema, input, checked.name, "pipeline_input");
  Run r;
  r.pipeline_id = checked.name;
  r.pipeline_version = checked.version;
  r.definition = checked.source;
  r.actor = std::move(actor);
  r.session_id = std::move(session_id);
  r.session_turn_id = std::move(session_turn_id);
  r.parent_id = std::move(parent_id);
  r.parent_node_id = std::move(parent_node_id);
  r.parent_message_id = std::move(parent_message_id);
  r.subpipeline_depth = subpipeline_depth;
  r.initiation_type = origin.value("initiation_type", std::string{"manual"});
  r.schedule_id = origin.value("schedule_id", std::string{});
  r.schedule_occurrence_id = origin.value("schedule_occurrence_id", std::string{});
  r.due_at = origin.value("due_at", std::string{});
  r.trigger_id = origin.value("trigger_id", std::string{});
  r.event_id = origin.value("event_id", std::string{});
  r.root_event_id = origin.value("root_event_id", std::string{});
  r.trigger_depth = origin.value("trigger_depth", 0U);
  r.resolved_subpipelines = checked.resolved_subpipelines;
  r.message.run_id = r.id;
  r.message.pipeline_id = r.pipeline_id;
  r.message.node_id = "input";
  r.message.payload = std::move(input);
  r.message.metadata = std::move(message_metadata);
  if (!r.parent_message_id.empty())
    r.message.provenance.push_back(
        {r.parent_node_id, "", "", "", "", r.parent_message_id, "", timestamp(), ""});
  if (r.session_id.empty()) {
    checkpoint(r, "run.created");
  } else {
    Event event;
    event.run_id = r.id;
    event.pipeline_id = r.pipeline_id;
    event.node_id = "input";
    event.type = "run.created";
    event.metadata["state"] = r.state;
    event.metadata["pipeline_version"] = r.pipeline_version;
    event.metadata["initiation_type"] = r.initiation_type;
    event.causation_id = r.id;
    event.root_event_id = r.root_event_id;
    event.trigger_depth = r.trigger_depth;
    Event session_event;
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    session_test_point(SessionTestPoint::BeforeRunBinding);
#endif
    if (!deps_.storage.bind_session_turn_run(r.session_id, r.session_turn_id, Json(r), Json(event),
                                             session_owner, session_fencing_token,
                                             Json(session_event)))
      throw Error(ErrorCode::Conflict, "Session run is already bound");
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
    session_test_point(SessionTestPoint::AfterRunBinding);
#endif
    deps_.events.publish(event);
    log_event(event);
  }
  auto id = r.id;
  if (!deps_.coordination)
    schedule(std::move(r));
  return id;
}
void Runtime::schedule(Run r, std::optional<LeaseRecord> lease) {
  if (active_.contains(r.id))
    throw Error(ErrorCode::Conflict, "Run already executing");
  if (stopping_ || active_.size() >= config_.max_runs)
    throw Error(ErrorCode::Capacity, "Concurrent run limit reached");
  if (deps_.coordination && !lease)
    throw Error(ErrorCode::Conflict, "Distributed run is missing an ownership lease");
  auto id = r.id;
  ActiveRun active;
  active.lease = std::move(lease);
  auto [it, inserted] = active_.emplace(id, std::move(active));
  (void)inserted;
  auto stop = it->second.stop.get_token();
  asio::co_spawn(io_, execute(std::move(r), stop), [this, id](const std::exception_ptr &error) {
    std::unique_lock lock(mutex_);
    bool ownership_lost = false;
    std::optional<LeaseRecord> lease;
    if (const auto active = active_.find(id); active != active_.end()) {
      ownership_lost = active->second.ownership_lost;
      lease = active->second.lease;
    }
    active_.erase(id);
    try {
      auto ended = deps_.storage.get(RecordKind::Run, id).get<Run>();
      if (error && !ownership_lost && !terminal(ended.state) &&
          ended.state != RunState::WaitingApproval && ended.state != RunState::Paused) {
        ended.error = "Unhandled executor failure";
        transition(ended, RunState::Failed, "run.failed");
      }
      finish_parent(ended);
      if (ended.state == RunState::Paused && !ended.child_id.empty() && !stopping_) {
        auto child = deps_.storage.get(RecordKind::Run, ended.child_id).get<Run>();
        if (terminal(child.state))
          resume(ended.id);
      }
      if (lease && deps_.coordination)
        deps_.coordination->release(*lease);
      if (!stopping_ && terminal(ended.state) && !ended.session_id.empty()) {
        lock.unlock();
        dispatch_sessions();
      }
    } catch (...) {
      log_diagnostic("runtime.persistence_or_resume_failure", {{"run_id", id}});
    }
  });
}
Task<void> Runtime::claim_loop() {
  for (;;) {
    claim_timer_->expires_after(Milliseconds{100});
    boost::system::error_code wait_error;
    co_await claim_timer_->async_wait(asio::redirect_error(asio::use_awaitable, wait_error));
    if (wait_error)
      co_return;
    {
      std::lock_guard lock(mutex_);
      if (stopping_)
        co_return;
      if (active_.size() >= config_.max_runs)
        continue;
    }
    std::vector<Json> candidates;
    try {
      candidates = deps_.storage.list(RecordKind::Run, "", config_.claim_batch_size, 0);
    } catch (const Error &) {
      continue;
    }
    for (const auto &value : candidates) {
      Run run;
      try {
        run = value.get<Run>();
      } catch (const Json::exception &) {
        continue;
      }
      if (terminal(run.state) || run.state == RunState::WaitingApproval)
        continue;
      if (run.state == RunState::Paused && !run.pending_parallel_group.empty()) {
        try {
          if (!distributed_parallel_ready(run))
            continue;
        } catch (const Error &) {
          continue;
        }
      } else if (run.state == RunState::Paused && !run.child_id.empty()) {
        try {
          const auto child = deps_.storage.get(RecordKind::Run, run.child_id).get<Run>();
          if (!terminal(child.state))
            continue;
        } catch (const Error &) {
          continue;
        }
      }
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
      session_test_point(SessionTestPoint::BeforeRunClaim);
#endif
      {
        std::lock_guard lock(mutex_);
        if (stopping_ || active_.size() >= config_.max_runs || active_.contains(run.id))
          break;
      }
      std::optional<LeaseRecord> lease;
      try {
        lease = deps_.coordination->acquire("run:" + run.id, config_.coordination_lease_ttl_ms);
      } catch (const Error &) {
        continue;
      }
      if (!lease)
        continue;
      try {
        run = deps_.storage.get(RecordKind::Run, run.id).get<Run>();
        if (terminal(run.state) || run.state == RunState::WaitingApproval) {
          deps_.coordination->release(*lease);
          continue;
        }
        // The first readiness check happens before the run lease is acquired.
        // A remote worker may claim a NodeWork branch between that check and
        // this reread.  Revalidate from the lease-protected snapshot so an
        // owner cannot resume a paused distributed join while a branch is
        // still queued/running.
        if (run.state == RunState::Paused) {
          if (!run.pending_parallel_group.empty() && !distributed_parallel_ready(run)) {
            deps_.coordination->release(*lease);
            continue;
          }
          if (run.pending_parallel_group.empty() && !run.child_id.empty()) {
            const auto child = deps_.storage.get(RecordKind::Run, run.child_id).get<Run>();
            if (!terminal(child.state)) {
              deps_.coordination->release(*lease);
              continue;
            }
          }
        }
        // A takeover replays from the durable node checkpoint.  This avoids
        // attempting to continue an in-flight coroutine that died with the
        // previous process.
        run.state = RunState::Queued;
        run.owner_instance_id = lease->owner_instance;
        run.fencing_token = lease->fencing_token;
        run.claimed_at = lease->acquired_at;
        run.last_renewed_at = lease->heartbeat_at;
        run.lease_expires_at = lease->expires_at;
        deps_.storage.commit_owned({{RecordKind::Run, run.id, run.id, Json(run)}},
                                   lease->resource_key, lease->owner_instance,
                                   lease->fencing_token);
        std::lock_guard lock(mutex_);
        if (stopping_ || active_.size() >= config_.max_runs) {
          deps_.coordination->release(*lease);
          continue;
        }
        schedule(std::move(run), std::move(lease));
      } catch (const Error &) {
        if (lease) {
          try {
            deps_.coordination->release(*lease);
          } catch (const Error &) {
          }
        }
      }
    }
    std::vector<Json> node_candidates;
    try {
      node_candidates = deps_.storage.list(RecordKind::NodeWork, "", config_.claim_batch_size, 0);
    } catch (const Error &) {
      continue;
    }
    for (const auto &value : node_candidates) {
      NodeWork work;
      try {
        work = value.get<NodeWork>();
        const auto run = deps_.storage.get(RecordKind::Run, work.run_id).get<Run>();
        const auto cancellation_cleanup = run.cancellation_requested && terminal(run.state) &&
                                          work.state == NodeWorkState::Running;
        if (!cancellation_cleanup && deps_.workers &&
            !deps_.workers->can_execute(work.required_worker_id, work.required_capability))
          continue;
        if ((!cancellation_cleanup && (terminal(run.state) || run.cancellation_requested)) ||
            run.pending_parallel_group != work.group_id)
          continue;
      } catch (const Error &) {
        continue;
      }
      {
        std::lock_guard lock(mutex_);
        if (stopping_ || active_nodes_.size() >= config_.max_nodes ||
            active_nodes_.contains(work.id))
          break;
      }
      std::optional<LeaseRecord> work_lease;
      std::optional<LeaseRecord> global_slot;
      std::optional<LeaseRecord> run_slot;
      try {
        work_lease =
            deps_.coordination->acquire("node:" + work.id, config_.coordination_lease_ttl_ms);
        if (!work_lease)
          continue;
        for (unsigned slot = 0; slot < config_.max_nodes && !global_slot; ++slot)
          global_slot = deps_.coordination->acquire("node-slot:" + std::to_string(slot),
                                                    config_.coordination_lease_ttl_ms);
        if (!global_slot) {
          deps_.coordination->release(*work_lease);
          continue;
        }
        for (unsigned slot = 0; slot < config_.max_nodes_per_run && !run_slot; ++slot)
          run_slot = deps_.coordination->acquire("run-node-slot:" + work.run_id + ":" +
                                                     std::to_string(slot),
                                                 config_.coordination_lease_ttl_ms);
        if (!run_slot) {
          deps_.coordination->release(*global_slot);
          deps_.coordination->release(*work_lease);
          continue;
        }
        work = deps_.storage.get(RecordKind::NodeWork, work.id).get<NodeWork>();
        if (work.state == NodeWorkState::Completed || work.state == NodeWorkState::Failed ||
            work.state == NodeWorkState::Cancelled) {
          deps_.coordination->release(*run_slot);
          deps_.coordination->release(*global_slot);
          deps_.coordination->release(*work_lease);
          continue;
        }
        std::vector<Record> claim_records;
        if (work.state == NodeWorkState::Running && !work.attempt_id.empty()) {
          try {
            auto previous =
                deps_.storage.get(RecordKind::Attempt, work.attempt_id).get<NodeExecution>();
            if (previous.state == NodeState::Running) {
              previous.state = NodeState::Failed;
              previous.error = "Distributed node work lease expired before attempt completed";
              previous.finished_at = timestamp();
              claim_records.push_back(
                  {RecordKind::Attempt, previous.id, previous.run_id, Json(previous)});
            }
          } catch (const Error &error) {
            if (error.code != ErrorCode::NotFound)
              throw;
          }
        }
        work.state = NodeWorkState::Running;
        ++work.attempt;
        work.attempt_id = uuid();
        work.owner_instance_id = work_lease->owner_instance;
        work.fencing_token = work_lease->fencing_token;
        work.claimed_at = work_lease->acquired_at;
        work.last_renewed_at = work_lease->heartbeat_at;
        work.lease_expires_at = work_lease->expires_at;
        work.updated_at = timestamp();
        claim_records.push_back({RecordKind::NodeWork, work.id, work.run_id, Json(work)});
        commit_node_owned(claim_records, work, *work_lease);
        log_diagnostic("runtime.distributed_node_claimed",
                       {{"node_work_id", work.id},
                        {"attempt", work.attempt},
                        {"attempt_id", work.attempt_id},
                        {"required_worker_id", work.required_worker_id},
                        {"fencing_token", work.fencing_token}});
        std::lock_guard lock(mutex_);
        if (stopping_ || active_nodes_.size() >= config_.max_nodes) {
          deps_.coordination->release(*run_slot);
          deps_.coordination->release(*global_slot);
          deps_.coordination->release(*work_lease);
          continue;
        }
        ActiveNode active;
        active.work_lease = *work_lease;
        active.global_slot = *global_slot;
        active.run_slot = *run_slot;
        auto [it, inserted] = active_nodes_.emplace(work.id, std::move(active));
        if (!inserted)
          throw Error(ErrorCode::Conflict, "Node work is already executing");
        auto stop = it->second.stop.get_token();
        const auto id = work.id;
        asio::co_spawn(
            io_,
            execute_distributed_work(std::move(work), *work_lease, *global_slot, *run_slot, stop),
            [this, id](const std::exception_ptr &error) {
              std::lock_guard lock(mutex_);
              std::optional<LeaseRecord> work_lease;
              std::optional<LeaseRecord> global_slot;
              std::optional<LeaseRecord> run_slot;
              if (const auto active = active_nodes_.find(id); active != active_nodes_.end()) {
                work_lease = active->second.work_lease;
                global_slot = active->second.global_slot;
                run_slot = active->second.run_slot;
                active_nodes_.erase(active);
              }
              if (error)
                log_diagnostic("runtime.distributed_node_failure", {{"node_work_id", id}});
              if (run_slot)
                try {
                  deps_.coordination->release(*run_slot);
                } catch (const Error &) {
                }
              if (global_slot)
                try {
                  deps_.coordination->release(*global_slot);
                } catch (const Error &) {
                }
              if (work_lease)
                try {
                  deps_.coordination->release(*work_lease);
                } catch (const Error &) {
                }
            });
      } catch (const Error &) {
        if (run_slot)
          try {
            deps_.coordination->release(*run_slot);
          } catch (const Error &) {
          }
        if (global_slot)
          try {
            deps_.coordination->release(*global_slot);
          } catch (const Error &) {
          }
        if (work_lease)
          try {
            deps_.coordination->release(*work_lease);
          } catch (const Error &) {
          }
      }
    }
  }
}
Task<void> Runtime::lease_loop() {
  for (;;) {
    lease_timer_->expires_after(Milliseconds{config_.coordination_heartbeat_interval_ms});
    boost::system::error_code wait_error;
    co_await lease_timer_->async_wait(asio::redirect_error(asio::use_awaitable, wait_error));
    if (wait_error)
      co_return;
    struct NodeLeases {
      std::string id;
      LeaseRecord work;
      std::optional<LeaseRecord> global;
      std::optional<LeaseRecord> run;
    };
    std::vector<std::pair<std::string, LeaseRecord>> leases;
    std::vector<NodeLeases> node_leases;
    {
      std::lock_guard lock(mutex_);
      if (stopping_)
        co_return;
      for (const auto &[id, active] : active_)
        if (active.lease)
          leases.emplace_back(id, *active.lease);
      for (const auto &[id, active] : active_nodes_)
        node_leases.push_back({id, active.work_lease, active.global_slot, active.run_slot});
    }
    try {
      deps_.coordination->register_instance(version,
                                            capability_advertisement(deps_.workers).dump());
      if (!deps_.coordination->heartbeat_instance("ACTIVE"))
        log_diagnostic("runtime.instance_heartbeat_missing", {{"instance_id", deps_.instance_id}});
    } catch (const Error &) {
      log_diagnostic("runtime.instance_heartbeat_failed", {{"instance_id", deps_.instance_id}});
    }
    for (auto &[id, lease] : leases) {
      try {
        if (!deps_.coordination->renew(lease, config_.coordination_lease_ttl_ms)) {
          std::lock_guard lock(mutex_);
          if (const auto active = active_.find(id); active != active_.end()) {
            active->second.ownership_lost = true;
            active->second.stop.request_stop();
          }
          continue;
        }
        std::lock_guard lock(mutex_);
        if (const auto active = active_.find(id); active != active_.end() && active->second.lease)
          *active->second.lease = lease;
      } catch (const Error &error) {
        // A transient database failure is not proof that this instance lost
        // ownership. Keep the database-authoritative lease snapshot and try
        // again before its expiry; an explicit fence loss still stops the run.
        if (error.code == ErrorCode::Storage) {
          log_diagnostic("runtime.run_lease_renew_deferred", {{"run_id", id}});
          continue;
        }
        std::lock_guard lock(mutex_);
        if (const auto active = active_.find(id); active != active_.end()) {
          active->second.ownership_lost = true;
          active->second.stop.request_stop();
        }
      }
    }
    for (auto &node : node_leases) {
      try {
        const auto observed = deps_.storage.get(RecordKind::NodeWork, node.id).get<NodeWork>();
        reconcile_terminal_worker(observed, node.work);
      } catch (const Error &) {
      }
      try {
        const auto run =
            deps_.storage
                .get(RecordKind::Run,
                     deps_.storage.get(RecordKind::NodeWork, node.id).get<NodeWork>().run_id)
                .get<Run>();
        if (run.cancellation_requested || terminal(run.state)) {
          std::lock_guard lock(mutex_);
          if (const auto active = active_nodes_.find(node.id); active != active_nodes_.end())
            active->second.stop.request_stop();
        }
      } catch (const Error &error) {
        if (error.code == ErrorCode::Storage) {
          log_diagnostic("runtime.distributed_node_state_read_deferred",
                         {{"node_work_id", node.id}});
        } else {
          std::lock_guard lock(mutex_);
          if (const auto active = active_nodes_.find(node.id); active != active_nodes_.end()) {
            active->second.ownership_lost = true;
            active->second.stop.request_stop();
          }
        }
      }
      bool valid = true;
      for (auto *lease :
           {&node.work, node.global ? &*node.global : nullptr, node.run ? &*node.run : nullptr}) {
        if (!lease)
          continue;
        try {
          if (!deps_.coordination->renew(*lease, config_.coordination_lease_ttl_ms)) {
            log_diagnostic("runtime.distributed_node_lease_renew_failed",
                           {{"node_work_id", node.id}, {"resource", lease->resource_key}});
            valid = false;
          }
        } catch (const Error &error) {
          if (error.code == ErrorCode::Storage) {
            // Keep the last database-authoritative lease snapshot. A
            // transient outage must not be treated as an immediate fence loss;
            // the next heartbeat either renews it or observes expiry.
            log_diagnostic("runtime.distributed_node_lease_renew_deferred",
                           {{"node_work_id", node.id}, {"resource", lease->resource_key}});
            continue;
          }
          log_diagnostic("runtime.distributed_node_lease_renew_error",
                         {{"node_work_id", node.id}, {"resource", lease->resource_key}});
          valid = false;
        }
      }
      if (!valid) {
        log_diagnostic("runtime.distributed_node_lease_lost",
                       {{"node_work_id", node.id}, {"fencing_token", node.work.fencing_token}});
        std::lock_guard lock(mutex_);
        if (const auto active = active_nodes_.find(node.id); active != active_nodes_.end()) {
          active->second.ownership_lost = true;
          active->second.stop.request_stop();
        }
        continue;
      }
      std::lock_guard lock(mutex_);
      if (const auto active = active_nodes_.find(node.id); active != active_nodes_.end()) {
        active->second.work_lease = node.work;
        if (node.global)
          active->second.global_slot = node.global;
        if (node.run)
          active->second.run_slot = node.run;
      }
    }
  }
}
void Runtime::resume(const std::string &id) {
  std::lock_guard lock(mutex_);
  auto r = deps_.storage.get(RecordKind::Run, id).get<Run>();
  if (active_.contains(id))
    throw Error(ErrorCode::Conflict, "Run is still leaving its checkpoint; retry shortly");
  if (r.state != RunState::WaitingApproval && r.state != RunState::Paused &&
      r.state != RunState::Queued)
    throw Error(ErrorCode::Conflict, "Run cannot resume from this state");
  if (r.state == RunState::WaitingApproval && !approved(r))
    throw Error(ErrorCode::Policy, "Approval is pending");
  if (r.cancellation_requested)
    throw Error(ErrorCode::Conflict, "Run cancellation has already been requested");
  if (stopping_ || (!deps_.coordination && active_.size() >= config_.max_runs))
    throw Error(ErrorCode::Capacity, "Concurrent run limit reached");
  if (r.state != RunState::Queued)
    transition(r, RunState::Queued, "run.resumed");
  if (!deps_.coordination)
    schedule(std::move(r));
}
void Runtime::cancel(const std::string &id) {
  std::set<std::string> visited;
  std::vector<std::string> worker_jobs;
  {
    std::lock_guard lock(mutex_);
    cancel_locked(id, visited, worker_jobs);
  }
  // Adapter callbacks may emit events synchronously.  Never invoke native
  // worker code while the runtime mutex is held, or an event-triggered
  // callback could re-enter Runtime and deadlock.
  if (deps_.workers)
    for (const auto &worker_job_id : worker_jobs) {
      try {
        deps_.workers->cancel(worker_job_id, WorkerJobState::Cancelled,
                              "LASO run cancellation requested");
      } catch (const Error &) {
        log_diagnostic("worker.cancellation_persist_failed",
                       {{"worker_job_id", worker_job_id}, {"run_id", id}});
      }
    }
}
void Runtime::cancel_locked(const std::string &id, std::set<std::string> &visited,
                            std::vector<std::string> &worker_jobs) {
  if (!visited.insert(id).second)
    throw Error(ErrorCode::Conflict, "Run cancellation cycle detected");
  auto r = deps_.storage.get(RecordKind::Run, id).get<Run>();
  if (terminal(r.state))
    throw Error(ErrorCode::Conflict, "Run is already terminal");
  if (deps_.coordination) {
    deps_.storage.request_cancellation(id);
    for (const auto &record : deps_.storage.list(RecordKind::NodeWork, id, 10000, 0)) {
      auto work = record.get<NodeWork>();
      if (work.state == NodeWorkState::Queued) {
        work.state = NodeWorkState::Cancelled;
        work.error = "Parent run cancellation requested";
        work.updated_at = timestamp();
        deps_.storage.commit({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}});
      } else if (work.state == NodeWorkState::Running) {
        if (const auto active = active_nodes_.find(work.id); active != active_nodes_.end())
          active->second.stop.request_stop();
      }
    }
    std::set<std::string> children;
    if (!r.child_id.empty())
      children.insert(r.child_id);
    for (const auto &record : list_all(deps_.storage, RecordKind::Run, "")) {
      const auto child = record.get<Run>();
      if (child.parent_id == id)
        children.insert(child.id);
    }
    for (const auto &child_id : children) {
      auto child = deps_.storage.get(RecordKind::Run, child_id).get<Run>();
      if (!terminal(child.state))
        cancel_locked(child.id, visited, worker_jobs);
    }
    if (deps_.workers) {
      for (const auto &record : deps_.storage.list(RecordKind::WorkerJob, id, 10000, 0)) {
        const auto worker_job = record.get<WorkerJob>();
        if (!worker_job_terminal(worker_job.state))
          worker_jobs.push_back(worker_job.id);
      }
    }
    if (const auto found = active_.find(id); found != active_.end()) {
      found->second.stop.request_stop();
      if (r.state == RunState::WaitingApproval || r.state == RunState::Paused ||
          r.state == RunState::Queued)
        transition(r, RunState::Cancelled, "run.cancelled");
    } else if (r.state == RunState::WaitingApproval || r.state == RunState::Paused ||
               r.state == RunState::Queued) {
      r.cancellation_requested = true;
      if (r.session_id.empty()) {
        r.state = RunState::Cancelled;
        r.updated_at = timestamp();
        deps_.storage.commit({{RecordKind::Run, r.id, r.id, Json(r)}});
      } else {
        auto lease = deps_.coordination->acquire("run:" + id, config_.coordination_lease_ttl_ms);
        if (!lease)
          return;
        r.owner_instance_id = lease->owner_instance;
        r.fencing_token = lease->fencing_token;
        try {
          transition(r, RunState::Cancelled, "run.cancelled");
        } catch (...) {
          try {
            deps_.coordination->release(*lease);
          } catch (const Error &) {
          }
          throw;
        }
        try {
          deps_.coordination->release(*lease);
        } catch (const Error &) {
        }
      }
    }
    return;
  }
  r.cancellation_requested = true;
  std::vector<Record> cancellation_records;
  for (const auto &record : list_all(deps_.storage, RecordKind::Approval, id)) {
    auto approval = record.get<Approval>();
    if (approval.decision == "pending") {
      approval.decision = "cancelled";
      approval.decided_at = timestamp();
      cancellation_records.push_back({RecordKind::Approval, approval.id, id, Json(approval)});
    }
  }
  checkpoint(r, "run.cancellation_requested", std::move(cancellation_records));
  std::set<std::string> children;
  if (!r.child_id.empty())
    children.insert(r.child_id);
  // A parent can have several active branch children.  The relationship is
  // persisted on each child, so cancellation does not depend on a single
  // mutable pointer in the parent checkpoint.
  for (const auto &record : list_all(deps_.storage, RecordKind::Run, "")) {
    const auto child = record.get<Run>();
    if (child.parent_id == id)
      children.insert(child.id);
  }
  for (const auto &child_id : children) {
    auto child = deps_.storage.get(RecordKind::Run, child_id).get<Run>();
    if (!terminal(child.state))
      cancel_locked(child.id, visited, worker_jobs);
  }
  if (deps_.workers) {
    for (const auto &record : deps_.storage.list(RecordKind::WorkerJob, id, 10000, 0)) {
      const auto worker_job = record.get<WorkerJob>();
      if (!worker_job_terminal(worker_job.state))
        worker_jobs.push_back(worker_job.id);
    }
  }
  auto found = active_.find(id);
  if (found != active_.end()) {
    found->second.stop.request_stop();
    if (r.state == RunState::WaitingApproval || r.state == RunState::Paused ||
        r.state == RunState::Queued)
      transition(r, RunState::Cancelled, "run.cancelled");
  } else
    transition(r, RunState::Cancelled, "run.cancelled");
}
void Runtime::shutdown() {
  std::lock_guard lock(mutex_);
  stopping_ = true;
  claim_timer_->cancel();
  lease_timer_->cancel();
  session_timer_->cancel();
  if (deps_.coordination && distributed_started_) {
    try {
      deps_.coordination->set_instance_state("DRAINING");
    } catch (const Error &) {
    }
  }
  for (auto &[id, source] : active_) {
    (void)id;
    source.stop.request_stop();
  }
  for (auto &[id, source] : active_nodes_) {
    (void)id;
    source.stop.request_stop();
  }
}
bool Runtime::idle() const {
  std::lock_guard lock(mutex_);
  return active_.empty();
}
bool Runtime::approved(const Run &r) const {
  for (const auto &item : list_all(deps_.storage, RecordKind::Approval, r.id)) {
    auto a = item.get<Approval>();
    auto found = r.node_visits.find(r.active_node);
    auto visit = found == r.node_visits.end() ? 0 : found->second;
    if (a.node_id == r.active_node && a.visit == visit && a.decision == "approved")
      return true;
  }
  return false;
}
void Runtime::wait_approval(Run &r, const NodeDefinition &node, const std::string &reason) {
  Approval a;
  a.run_id = r.id;
  a.node_id = node.id;
  a.reason = reason;
  a.action = node.type + ":" + node.binding;
  a.visit = r.node_visits[node.id];
  NodeExecution attempt;
  attempt.run_id = r.id;
  attempt.node_id = node.id;
  attempt.state = NodeState::WaitingApproval;
  transition(r, RunState::WaitingApproval, "approval.requested",
             {{RecordKind::Approval, a.id, r.id, Json(a)},
              {RecordKind::Attempt, attempt.id, r.id, Json(attempt)}});
}
void Runtime::decide(const std::string &id, bool approve, const std::string &actor,
                     const std::string &comment) {
  std::lock_guard lock(mutex_);
  if (actor.size() > 128 || comment.size() > 2048)
    throw Error(ErrorCode::Validation, "Approval annotation exceeds limit");
  auto a = deps_.storage.get(RecordKind::Approval, id).get<Approval>();
  auto r = deps_.storage.get(RecordKind::Run, a.run_id).get<Run>();
  if (a.decision != "pending" || r.state != RunState::WaitingApproval || r.active_node != a.node_id)
    throw Error(ErrorCode::Conflict, "Approval is no longer pending");
  if (active_.contains(r.id))
    throw Error(ErrorCode::Conflict, "Approval checkpoint is settling; retry shortly");
  if (approve && (stopping_ || (!deps_.coordination && active_.size() >= config_.max_runs)))
    throw Error(ErrorCode::Capacity, "Concurrent run limit reached");
  a.decision = approve ? "approved" : "rejected";
  a.decided_at = timestamp();
  a.actor = actor;
  a.comment = comment;
  std::vector<Record> records{{RecordKind::Approval, a.id, r.id, Json(a)}};
  for (const auto &item : list_all(deps_.storage, RecordKind::Attempt, r.id)) {
    auto attempt = item.get<NodeExecution>();
    if (attempt.node_id == a.node_id && attempt.state == NodeState::WaitingApproval) {
      attempt.state = approve ? NodeState::Completed : NodeState::Failed;
      attempt.finished_at = timestamp();
      records.push_back({RecordKind::Attempt, attempt.id, r.id, Json(attempt)});
    }
  }
  if (deps_.coordination) {
    // Approval is a control-plane decision and may be made by an instance
    // other than the one that was executing the run.  The paused executor
    // has already persisted its checkpoint and will release its lease when
    // it returns, so the decision must not require that executor's fencing
    // proof.  Clearing the stale execution ownership also makes the queued
    // run claimable by any healthy instance.
    r.owner_instance_id.clear();
    r.lease_expires_at.clear();
    r.claimed_at.clear();
    r.last_renewed_at.clear();
    r.fencing_token = 0;
  }
  if (approve) {
    // Decision and resumable queue checkpoint are one transaction.
    transition(r, RunState::Queued, "approval.approved", std::move(records));
    if (!deps_.coordination)
      schedule(std::move(r));
  } else {
    r.error = "Human approval rejected";
    transition(r, RunState::Failed, "approval.rejected", std::move(records));
    finish_parent(r);
  }
}
void Runtime::finish_parent(const Run &child) {
  if (child.parent_id.empty() || !terminal(child.state) || stopping_)
    return;
  auto parent = deps_.storage.get(RecordKind::Run, child.parent_id).get<Run>();
  if (parent.state == RunState::Paused && !active_.contains(parent.id)) {
    if (deps_.coordination) {
      parent.state = RunState::Queued;
      parent.updated_at = timestamp();
      deps_.storage.commit({{RecordKind::Run, parent.id, parent.id, Json(parent)}});
    } else {
      resume(parent.id);
    }
  }
}
} // namespace laso
