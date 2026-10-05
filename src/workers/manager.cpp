#include <algorithm>
#include <cctype>
#include <cmath>
#include <laso/core/config.hpp>
#include <laso/workers/manager.hpp>
#include <laso/workers/process_protocol.hpp>
#include <limits>
#include <set>
#include <sstream>

namespace laso {
namespace {
constexpr auto max_job_id_bytes = std::size_t{128};
constexpr auto max_external_id_bytes = std::size_t{512};
constexpr auto max_metadata_bytes = std::size_t{64} * 1024;
constexpr auto max_result_bytes = std::size_t{1024} * 1024;
constexpr auto max_artifact_metadata_bytes = std::size_t{64} * 1024;
constexpr auto max_usage_metadata_bytes = std::size_t{64} * 1024;
constexpr auto max_usage_identity_bytes = std::size_t{128};
constexpr std::uint64_t max_usage_metric = 1000000000000000ULL;

bool bounded_identifier(const std::string &value, std::size_t maximum) {
  return !value.empty() && value.size() <= maximum &&
         std::all_of(value.begin(), value.end(), [](unsigned char c) {
           return std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == ':';
         });
}

std::string bounded_error(const std::string &value) {
  if (value.size() <= 512)
    return value;
  return value.substr(0, 512);
}

WorkerJobState event_state(const std::string &type) {
  if (type == "worker.job.started")
    return WorkerJobState::Running;
  if (type == "worker.job.completed")
    return WorkerJobState::Completed;
  if (type == "worker.job.failed")
    return WorkerJobState::Failed;
  if (type == "worker.job.cancelled")
    return WorkerJobState::Cancelled;
  return WorkerJobState::Unknown;
}
} // namespace

WorkerManager::WorkerManager(Storage &storage, WorkerRegistry &registry, unsigned max_active,
                             unsigned max_per_worker, unsigned max_artifacts,
                             std::uint64_t max_wall_time_ms, std::uint64_t max_tokens_per_run,
                             double max_cost_units_per_run)
    : storage_(storage), registry_(registry), max_active_(max_active),
      max_per_worker_(max_per_worker), max_artifacts_(max_artifacts),
      max_wall_time_ms_(max_wall_time_ms), max_tokens_per_run_(max_tokens_per_run),
      max_cost_units_per_run_(max_cost_units_per_run) {
  if (max_active_ == 0 || max_active_ > 4096 || max_per_worker_ == 0 ||
      max_per_worker_ > max_active_ || max_artifacts_ == 0 || max_artifacts_ > 64 ||
      max_wall_time_ms_ > max_usage_metric || max_tokens_per_run_ > max_usage_metric ||
      !std::isfinite(max_cost_units_per_run_) || max_cost_units_per_run_ < 0 ||
      max_cost_units_per_run_ > static_cast<double>(max_usage_metric))
    throw Error(ErrorCode::Configuration, "Invalid worker limits");
}

WorkerManager::WorkerManager(Storage &storage, WorkerRegistry &registry, Policy &policy,
                             unsigned max_active, unsigned max_per_worker, unsigned max_artifacts,
                             std::uint64_t max_wall_time_ms, std::uint64_t max_tokens_per_run,
                             double max_cost_units_per_run)
    : WorkerManager(storage, registry, max_active, max_per_worker, max_artifacts, max_wall_time_ms,
                    max_tokens_per_run, max_cost_units_per_run) {
  policy_ = &policy;
}

WorkerJob WorkerManager::job(const std::string &id) const {
  if (!bounded_identifier(id, max_job_id_bytes))
    throw Error(ErrorCode::Validation, "Invalid worker job id");
  return storage_.get(RecordKind::WorkerJob, id).get<WorkerJob>();
}

std::string WorkerManager::job_id_for(const std::string &idempotency_key) const {
  if (idempotency_key.empty() || idempotency_key.size() > 512)
    throw Error(ErrorCode::Validation, "Invalid worker idempotency key");
  return durable_job_id(idempotency_key);
}

WorkerJob WorkerManager::refresh(const std::string &id) {
  auto value = job(id);
  if (worker_job_terminal(value.state) || value.external_job_id.empty())
    return value;
  return reconcile(std::move(value), true);
}

std::vector<Json> WorkerManager::jobs(const std::string &run_id, std::size_t limit,
                                      std::size_t offset) const {
  return storage_.list(RecordKind::WorkerJob, run_id, limit, offset);
}

Json WorkerManager::workers() const {
  Json result = Json::array();
  for (const auto &name : registry_.names())
    result.push_back(registry_.get(name)->metadata());
  return result;
}

Json WorkerManager::worker(const std::string &id) const {
  return registry_.get(id)->metadata();
}

bool WorkerManager::can_execute(const std::string &worker_id, const std::string &capability) const {
  if (worker_id.empty() && capability.empty())
    return true;
  for (const auto &candidate : registry_.names()) {
    if (!worker_id.empty() && candidate != worker_id)
      continue;
    const auto metadata = registry_.get(candidate)->metadata();
    const auto recoverable = metadata.supports_recovery && metadata.status == "failed";
    if (!metadata.enabled || (!metadata.healthy && !recoverable) ||
        (metadata.status != "healthy" && !recoverable))
      continue;
    if (!capability.empty() && std::find(metadata.capabilities.begin(), metadata.capabilities.end(),
                                         capability) == metadata.capabilities.end())
      continue;
    return true;
  }
  return false;
}

Json WorkerManager::distributed_capabilities() const {
  Json result{{"protocol_version", 1}, {"workloads", Json::array()}};
  for (const auto &candidate : registry_.names()) {
    const auto metadata = registry_.get(candidate)->metadata();
    const auto recoverable = metadata.supports_recovery && metadata.status == "failed";
    if (!metadata.enabled || (!metadata.healthy && !recoverable) ||
        (metadata.status != "healthy" && !recoverable))
      continue;
    result["workloads"].push_back({{"worker_id", metadata.id},
                                   {"capabilities", metadata.capabilities},
                                   {"supports_recovery", metadata.supports_recovery},
                                   {"supports_cancellation", metadata.supports_cancellation},
                                   {"workspace_transport", true}});
  }
  if (result.dump().size() > 4096)
    throw Error(ErrorCode::Configuration, "Worker capability advertisement is too large");
  return result;
}

std::string WorkerManager::resolve_worker(const std::string &worker_id,
                                          const std::string &capability) const {
  if (!worker_id.empty()) {
    (void)registry_.get(worker_id);
    return worker_id;
  }
  if (capability.empty())
    throw Error(ErrorCode::Validation, "Worker id or capability is required");
  std::string result;
  for (const auto &candidate : registry_.names()) {
    const auto metadata = registry_.get(candidate)->metadata();
    if (std::find(metadata.capabilities.begin(), metadata.capabilities.end(), capability) ==
        metadata.capabilities.end())
      continue;
    if (!result.empty())
      throw Error(ErrorCode::Conflict, "Worker capability is ambiguous");
    result = candidate;
  }
  if (result.empty())
    throw Error(ErrorCode::NotFound, "No worker provides the requested capability");
  return result;
}

WorkerJobState WorkerManager::parse_state(const Json &value, WorkerJobState fallback) {
  try {
    return value.get<WorkerJobState>();
  } catch (...) {
    return fallback;
  }
}

std::string WorkerManager::durable_job_id(const std::string &idempotency_key) {
  // The identifier is a bounded storage key, not a security primitive.  The
  // full idempotency key remains in the job record so a hash collision is
  // detected and rejected rather than silently reusing another job.
  std::uint64_t hash = 1469598103934665603ULL;
  for (const auto byte : idempotency_key) {
    hash ^= static_cast<unsigned char>(byte);
    hash *= 1099511628211ULL;
  }
  std::ostringstream out;
  out << "worker-" << std::hex << hash;
  return out.str();
}

void WorkerManager::merge_usage(WorkerUsage &target, const WorkerUsage &incoming) {
  if (incoming.queue_duration_ms)
    target.queue_duration_ms = incoming.queue_duration_ms;
  if (incoming.wall_duration_ms)
    target.wall_duration_ms = incoming.wall_duration_ms;
  if (incoming.input_tokens)
    target.input_tokens = incoming.input_tokens;
  if (incoming.output_tokens)
    target.output_tokens = incoming.output_tokens;
  if (incoming.total_tokens)
    target.total_tokens = incoming.total_tokens;
  if (incoming.tool_calls)
    target.tool_calls = incoming.tool_calls;
  if (incoming.action_count)
    target.action_count = incoming.action_count;
  if (incoming.cost_units)
    target.cost_units = incoming.cost_units;
  if (!incoming.provider.empty())
    target.provider = incoming.provider;
  if (!incoming.model.empty())
    target.model = incoming.model;
  if (!incoming.executor.empty())
    target.executor = incoming.executor;
  if (incoming.metadata.is_object())
    target.metadata.update(incoming.metadata);
  if (!target.total_tokens && target.input_tokens && target.output_tokens &&
      *target.input_tokens <= std::numeric_limits<std::uint64_t>::max() - *target.output_tokens)
    target.total_tokens = *target.input_tokens + *target.output_tokens;
}

std::optional<std::uint64_t> WorkerManager::total_tokens(const WorkerUsage &usage) {
  if (usage.total_tokens)
    return usage.total_tokens;
  if (usage.input_tokens && usage.output_tokens &&
      *usage.input_tokens <= std::numeric_limits<std::uint64_t>::max() - *usage.output_tokens)
    return *usage.input_tokens + *usage.output_tokens;
  return std::nullopt;
}

std::string WorkerManager::budget_violation(const WorkerJob &value) const {
  const auto metric_exceeds = [](const auto &metric) {
    return metric && *metric > max_usage_metric;
  };
  if (metric_exceeds(value.usage.queue_duration_ms) ||
      metric_exceeds(value.usage.wall_duration_ms) || metric_exceeds(value.usage.input_tokens) ||
      metric_exceeds(value.usage.output_tokens) || metric_exceeds(value.usage.total_tokens) ||
      metric_exceeds(value.usage.tool_calls) || metric_exceeds(value.usage.action_count))
    return "reported worker usage exceeds the representable limit";
  if (value.usage.metadata.dump().size() > max_usage_metadata_bytes)
    return "worker usage metadata exceeds the limit";
  if (value.usage.provider.size() > max_usage_identity_bytes ||
      value.usage.model.size() > max_usage_identity_bytes ||
      value.usage.executor.size() > max_usage_identity_bytes)
    return "worker usage identity exceeds the limit";
  if (value.usage.cost_units &&
      (!std::isfinite(*value.usage.cost_units) || *value.usage.cost_units < 0 ||
       *value.usage.cost_units > static_cast<double>(max_usage_metric)))
    return "reported worker cost_units is invalid";
  if (max_wall_time_ms_ != 0 && value.usage.wall_duration_ms &&
      *value.usage.wall_duration_ms > max_wall_time_ms_)
    return "worker wall-time budget exceeded";

  std::uint64_t prior_tokens = 0;
  double prior_cost = 0;
  if (max_tokens_per_run_ != 0 || max_cost_units_per_run_ != 0) {
    for (const auto &record : storage_.list(RecordKind::WorkerJob, value.run_id, 10000, 0)) {
      const auto prior = record.get<WorkerJob>();
      if (prior.id == value.id)
        continue;
      if (const auto tokens = total_tokens(prior.usage)) {
        if (prior_tokens > std::numeric_limits<std::uint64_t>::max() - *tokens)
          return "worker token budget exceeded";
        prior_tokens += *tokens;
      }
      if (prior.usage.cost_units)
        prior_cost += *prior.usage.cost_units;
    }
  }
  if (max_tokens_per_run_ != 0)
    if (const auto tokens = total_tokens(value.usage);
        tokens &&
        (prior_tokens > max_tokens_per_run_ || *tokens > max_tokens_per_run_ - prior_tokens))
      return "worker token budget exceeded";
  if (max_cost_units_per_run_ != 0 && value.usage.cost_units &&
      prior_cost + *value.usage.cost_units > max_cost_units_per_run_)
    return "worker cost_units budget exceeded";
  return {};
}

void WorkerManager::persist(WorkerJob &value) {
  if (value.error.size() > 512)
    value.error.resize(512);
  if (value.cancellation_error.size() > 512)
    value.cancellation_error.resize(512);
  std::size_t artifact_bytes = 0;
  for (const auto &artifact : value.artifacts) {
    const auto size = artifact.dump().size();
    if (size > max_artifact_metadata_bytes || artifact_bytes > max_artifact_metadata_bytes - size)
      throw Error(ErrorCode::Validation, "Worker artifact metadata exceeds limit");
    artifact_bytes += size;
  }
  if (value.result.dump().size() > max_result_bytes ||
      value.result_metadata.dump().size() > max_metadata_bytes ||
      value.artifacts.size() > max_artifacts_)
    throw Error(ErrorCode::Validation, "Worker result exceeds limit");
  if (value.usage.metadata.dump().size() > max_usage_metadata_bytes)
    throw Error(ErrorCode::Validation, "Worker usage metadata exceeds limit");
  storage_.commit({{RecordKind::WorkerJob, value.id, value.run_id, Json(value)}});
}

void WorkerManager::retire_superseded_distributed_jobs_locked() {
  for (const auto &record : storage_.list(RecordKind::WorkerJob, "", 10000, 0)) {
    auto value = record.get<WorkerJob>();
    if (worker_job_terminal(value.state))
      continue;
    const auto work_id = value.request_metadata.value("node_work_id", std::string{});
    const auto attempt_id = value.request_metadata.value("node_work_attempt_id", std::string{});
    if (work_id.empty() || attempt_id.empty())
      continue;
    try {
      const auto work = storage_.get(RecordKind::NodeWork, work_id).get<NodeWork>();
      if (work.state == NodeWorkState::Running && work.attempt_id == attempt_id)
        continue;
      value.state = WorkerJobState::Failed;
      value.failure_kind = WorkerFailureKind::Transport;
      value.error = "Worker attempt was superseded or lost its node lease";
      value.completed_at = timestamp();
      persist(value);
    } catch (const Error &) {
      // A concurrently recovered node may disappear between the list and the
      // lookup.  Leave that job for the normal durable recovery path.
    }
  }
}

WorkerJob WorkerManager::reconcile(WorkerJob value, bool fail_transport) {
  // An asynchronous submission remains in Submitting until the provider
  // handle is available.  Do not call adapter->status() with an empty handle:
  // process-backed transports serialize status with submit, so doing so would
  // block the runtime/lease loop behind provider startup or execution.
  if (worker_job_terminal(value.state) || value.external_job_id.empty())
    return value;
  auto adapter = registry_.get(value.worker_id);
  if (!adapter->metadata().supports_recovery)
    return value;
  try {
    const auto status = adapter->status(value.external_job_id);
    if (status.state == WorkerJobState::Unknown)
      return value;
    std::lock_guard state_lock(state_mutex_);
    value = job(value.id);
    if (worker_job_terminal(value.state))
      return value;
    if (!valid_worker_job_transition(value.state, status.state) && value.state != status.state)
      return value;
    if (status.state != value.state)
      value.state = status.state;
    if (!status.result.is_null() && status.result.dump().size() <= max_result_bytes)
      value.result = status.result;
    if (status.metadata.is_object() && status.metadata.dump().size() <= max_metadata_bytes)
      value.result_metadata = status.metadata;
    merge_usage(value.usage, status.usage);
    if (!status.artifacts.empty() && status.artifacts.size() <= max_artifacts_)
      value.artifacts = status.artifacts;
    if (!status.error.empty())
      value.error = bounded_error(status.error);
    if (const auto violation = budget_violation(value); !violation.empty()) {
      value.state = WorkerJobState::Failed;
      value.failure_kind = WorkerFailureKind::Budget;
      value.error = violation;
    } else if (value.state == WorkerJobState::Failed &&
               value.failure_kind == WorkerFailureKind::None) {
      value.failure_kind = WorkerFailureKind::Job;
    }
    if (worker_job_terminal(value.state) && value.completed_at.empty())
      value.completed_at = timestamp();
    persist(value);
  } catch (const WorkerTransportError &error) {
    if (fail_transport) {
      std::lock_guard state_lock(state_mutex_);
      value = job(value.id);
      if (!worker_job_terminal(value.state)) {
        value.state = WorkerJobState::Failed;
        value.failure_kind = WorkerFailureKind::Transport;
        value.error = bounded_error(error.what());
        value.completed_at = timestamp();
        persist(value);
      }
    }
  } catch (const Error &) { // NOLINT(bugprone-empty-catch): recovery is best effort.
    // Recovery is deliberately conservative. An adapter failure does not
    // invent a terminal result or submit a second external task.
  } catch (...) { // NOLINT(bugprone-empty-catch): contain native adapter failures.
    // Native adapter exceptions are contained by the loader; this is an
    // additional safety boundary around recovery.
  }
  return value;
}

WorkerJob WorkerManager::submit(const WorkerRequest &request) {
  std::lock_guard submit_lock(submit_mutex_);
  if (stopped_)
    throw Error(ErrorCode::Conflict, "Worker manager is stopped");
  if (request.idempotency_key.empty() || request.idempotency_key.size() > 512 ||
      request.task_type.size() > 128 || request.capability.size() > 128 ||
      request.instructions.size() > max_result_bytes ||
      request.input.dump().size() > max_result_bytes ||
      request.metadata.dump().size() > max_metadata_bytes ||
      request.artifact_ids.size() > max_artifacts_)
    throw Error(ErrorCode::Validation, "Worker request exceeds limit");
  for (const auto &artifact : request.artifact_ids)
    if (!bounded_identifier(artifact, 128))
      throw Error(ErrorCode::Validation, "Invalid worker artifact reference");

  auto worker_id = resolve_worker(request.worker_id, request.capability);
  auto adapter = registry_.get(worker_id);
  const auto durable_id = durable_job_id(request.idempotency_key);
  auto existing_job = [&]() -> std::optional<WorkerJob> {
    std::lock_guard state_lock(state_mutex_);
    try {
      auto existing = job(durable_id);
      if (existing.idempotency_key != request.idempotency_key)
        throw Error(ErrorCode::Conflict, "Worker idempotency key collision");
      if (existing.worker_id != worker_id)
        throw Error(ErrorCode::Conflict, "Worker idempotency key belongs to another worker");
      return existing;
    } catch (const Error &error) {
      if (error.code == ErrorCode::NotFound)
        return std::nullopt;
      throw;
    }
  };
  auto existing = existing_job();
  if (existing.has_value()) {
    if (!worker_id.empty() && existing->worker_id != worker_id)
      throw Error(ErrorCode::Conflict, "Worker idempotency key belongs to another worker");
    if (worker_job_terminal(existing->state))
      return *existing;
  }

  auto metadata = adapter->metadata();
  if (metadata.supports_recovery && !metadata.healthy && metadata.status != "disabled" &&
      metadata.status != "unavailable") {
    try {
      // A supervised transport may have torn down its process group while a
      // previous submission was timing out.  Re-establish ownership before
      // checking availability for a retry; start() is serialized by the
      // transport and is idempotent for healthy adapters.
      adapter->start();
      metadata = adapter->metadata();
    } catch (...) {
    }
  }
  const auto recoverable = metadata.supports_recovery && metadata.status == "failed";
  if (!metadata.enabled || metadata.status == "disabled" || metadata.status == "unavailable" ||
      (!metadata.healthy && !recoverable))
    throw Error(ErrorCode::Capacity, "Worker is unavailable");

  if (existing.has_value()) {
    if (!existing->external_job_id.empty())
      return reconcile(std::move(*existing), false);
    if (!metadata.supports_recovery) {
      std::lock_guard state_lock(state_mutex_);
      existing = job(durable_id);
      existing->state = WorkerJobState::Unknown;
      existing->error = "External submission outcome is unknown; adapter recovery is unavailable";
      persist(*existing);
      return *existing;
    }
    auto retry_request = request;
    retry_request.job_id = existing->id;
    try {
      const auto submission = adapter->submit(retry_request);
      std::lock_guard state_lock(state_mutex_);
      existing = job(durable_id);
      if (worker_job_terminal(existing->state))
        return *existing;
      existing->external_job_id = submission.external_job_id;
      existing->state = submission.state;
      existing->result_metadata = submission.metadata;
      if (!submission.result.is_null())
        existing->result = submission.result;
      if (!submission.artifacts.empty())
        existing->artifacts = submission.artifacts;
      if (!submission.error.empty())
        existing->error = bounded_error(submission.error);
      merge_usage(existing->usage, submission.usage);
      if (existing->external_job_id.empty() ||
          existing->external_job_id.size() > max_external_id_bytes)
        throw WorkerTransportError("Worker returned an invalid external job id");
      if (worker_job_terminal(existing->state))
        existing->completed_at = timestamp();
      if (const auto violation = budget_violation(*existing); !violation.empty()) {
        existing->state = WorkerJobState::Failed;
        existing->failure_kind = WorkerFailureKind::Budget;
        existing->error = violation;
        existing->completed_at = timestamp();
      } else if (existing->state == WorkerJobState::Failed) {
        existing->failure_kind = WorkerFailureKind::Job;
      }
      persist(*existing);
      return *existing;
    } catch (const WorkerTransportError &error) {
      {
        std::lock_guard state_lock(state_mutex_);
        existing = job(durable_id);
        if (!worker_job_terminal(existing->state)) {
          if (!existing->cancellation_requested) {
            existing->state = error.timed_out ? WorkerJobState::TimedOut : WorkerJobState::Failed;
            existing->failure_kind = WorkerFailureKind::Transport;
            existing->error = bounded_error(error.what());
            existing->completed_at = timestamp();
            persist(*existing);
          }
        }
      }
      if (existing->cancellation_requested && !worker_job_terminal(existing->state)) {
        std::unique_lock wait_lock(state_mutex_);
        state_changed_.wait_for(wait_lock, std::chrono::seconds(1), [&] {
          try {
            *existing = job(durable_id);
            return worker_job_terminal(existing->state);
          } catch (...) {
            return false;
          }
        });
      }
      return *existing;
    } catch (...) {
      std::lock_guard state_lock(state_mutex_);
      existing = job(durable_id);
      if (!worker_job_terminal(existing->state)) {
        existing->state = WorkerJobState::Failed;
        existing->failure_kind = WorkerFailureKind::Job;
        existing->error = "Worker retry submission failed";
        existing->completed_at = timestamp();
        persist(*existing);
      }
      return *existing;
    }
  }

  WorkerJob created;
  {
    std::lock_guard state_lock(state_mutex_);
    retire_superseded_distributed_jobs_locked();
    std::size_t active = 0, worker_active = 0;
    for (const auto &record : storage_.list(RecordKind::WorkerJob, "", 10000, 0)) {
      const auto stored = record.get<WorkerJob>();
      if (worker_job_terminal(stored.state))
        continue;
      ++active;
      if (stored.worker_id == worker_id)
        ++worker_active;
    }
    if (active >= max_active_)
      throw Error(ErrorCode::Capacity, "Worker job capacity is exhausted");
    if (worker_active >= max_per_worker_)
      throw Error(ErrorCode::Capacity, "Worker-specific job capacity is exhausted");

    created.id = durable_id;
    created.worker_id = worker_id;
    created.run_id = request.run_id;
    created.node_id = request.node_id;
    created.attempt = request.attempt;
    created.idempotency_key = request.idempotency_key;
    created.request_metadata = {{"task_type", request.task_type},
                                {"capability", request.capability},
                                {"deadline", request.deadline},
                                {"artifact_ids", request.artifact_ids}};
    if (request.metadata.is_object()) {
      for (const auto &key : {"classification", "node_work_id", "node_work_attempt_id"})
        if (request.metadata.contains(key))
          created.request_metadata[key] = request.metadata.at(key);
    }
    if (!storage_.claim({RecordKind::WorkerJob, created.id, created.run_id, Json(created)}))
      return job(durable_id);
    created.state = WorkerJobState::Submitting;
    persist(created);
  }

  try {
    auto outbound = request;
    outbound.job_id = created.id;
    const auto submission = adapter->submit(outbound);
    if (submission.external_job_id.empty() ||
        submission.external_job_id.size() > max_external_id_bytes)
      throw WorkerTransportError("Worker returned an invalid external job id");

    std::lock_guard state_lock(state_mutex_);
    const auto observed = job(created.id);
    if (worker_job_terminal(observed.state))
      return observed;
    created = observed;
    created.external_job_id = submission.external_job_id;
    created.state = submission.state;
    created.result_metadata = submission.metadata;
    if (!submission.result.is_null())
      created.result = submission.result;
    if (!submission.artifacts.empty())
      created.artifacts = submission.artifacts;
    if (!submission.error.empty())
      created.error = bounded_error(submission.error);
    merge_usage(created.usage, submission.usage);
    if (worker_job_terminal(created.state))
      created.completed_at = timestamp();
    if (const auto violation = budget_violation(created); !violation.empty()) {
      created.state = WorkerJobState::Failed;
      created.failure_kind = WorkerFailureKind::Budget;
      created.error = violation;
      created.completed_at = timestamp();
    } else if (created.state == WorkerJobState::Failed) {
      created.failure_kind = WorkerFailureKind::Job;
    }
    persist(created);
  } catch (const WorkerTransportError &error) {
    {
      std::lock_guard state_lock(state_mutex_);
      created = job(created.id);
      if (!worker_job_terminal(created.state)) {
        if (!created.cancellation_requested) {
          created.state = error.timed_out ? WorkerJobState::TimedOut : WorkerJobState::Failed;
          created.failure_kind = WorkerFailureKind::Transport;
          created.error = bounded_error(error.what());
          created.completed_at = timestamp();
          log_diagnostic("worker.transport_failed",
                         {{"worker_job_id", created.id}, {"worker_id", created.worker_id}});
          persist(created);
        }
      }
    }
    if (created.cancellation_requested && !worker_job_terminal(created.state)) {
      std::unique_lock wait_lock(state_mutex_);
      state_changed_.wait_for(wait_lock, std::chrono::seconds(1), [&] {
        try {
          created = job(created.id);
          return worker_job_terminal(created.state);
        } catch (...) {
          return false;
        }
      });
    }
  } catch (const Error &error) {
    if (error.code == ErrorCode::Storage) {
      // Leave the durable submission recoverable. The runtime lease/recovery
      // path will fence this attempt if the outage outlives its lease.
      log_diagnostic("worker.submission_deferred_after_storage_error",
                     {{"worker_job_id", created.id}, {"worker_id", created.worker_id}});
      throw;
    }
    std::lock_guard state_lock(state_mutex_);
    created = job(created.id);
    if (!worker_job_terminal(created.state)) {
      created.state = WorkerJobState::Failed;
      created.failure_kind = WorkerFailureKind::Job;
      created.error = bounded_error(error.code == ErrorCode::Plugin ? error.what()
                                                                    : "Worker submission failed");
      created.completed_at = timestamp();
      log_diagnostic("worker.submission_failed", {{"worker_job_id", created.id},
                                                  {"worker_id", created.worker_id},
                                                  {"reason", created.error}});
      persist(created);
    }
  } catch (...) {
    std::lock_guard state_lock(state_mutex_);
    created = job(created.id);
    if (!worker_job_terminal(created.state)) {
      created.state = WorkerJobState::Failed;
      created.failure_kind = WorkerFailureKind::Job;
      created.error = "Worker submission failed";
      created.completed_at = timestamp();
      log_diagnostic("worker.submission_failed",
                     {{"worker_job_id", created.id}, {"worker_id", created.worker_id}});
      persist(created);
    }
  }
  return job(created.id);
}

WorkerJob WorkerManager::submit_async(const WorkerRequest &request) {
  if (stopped_)
    throw Error(ErrorCode::Conflict, "Worker manager is stopped");
  if (request.idempotency_key.empty() || request.idempotency_key.size() > 512 ||
      request.task_type.size() > 128 || request.capability.size() > 128 ||
      request.instructions.size() > max_result_bytes ||
      request.input.dump().size() > max_result_bytes ||
      request.metadata.dump().size() > max_metadata_bytes ||
      request.artifact_ids.size() > max_artifacts_)
    throw Error(ErrorCode::Validation, "Worker request exceeds limit");
  for (const auto &artifact : request.artifact_ids)
    if (!bounded_identifier(artifact, 128))
      throw Error(ErrorCode::Validation, "Invalid worker artifact reference");

  std::lock_guard submit_lock(submit_mutex_);
  const auto worker_id = resolve_worker(request.worker_id, request.capability);
  const auto adapter = registry_.get(worker_id);
  auto metadata = adapter->metadata();
  if (metadata.supports_recovery && !metadata.healthy && metadata.status != "disabled" &&
      metadata.status != "unavailable") {
    try {
      adapter->start();
      metadata = adapter->metadata();
    } catch (...) {
    }
  }
  const auto recoverable = metadata.supports_recovery && metadata.status == "failed";
  if (!metadata.enabled || metadata.status == "disabled" || metadata.status == "unavailable" ||
      (!metadata.healthy && !recoverable))
    throw Error(ErrorCode::Capacity, "Worker is unavailable");

  const auto durable_id = durable_job_id(request.idempotency_key);
  WorkerJob created;
  {
    std::lock_guard state_lock(state_mutex_);
    retire_superseded_distributed_jobs_locked();
    try {
      created = job(durable_id);
      if (created.idempotency_key != request.idempotency_key)
        throw Error(ErrorCode::Conflict, "Worker idempotency key collision");
      if (created.worker_id != worker_id)
        throw Error(ErrorCode::Conflict, "Worker idempotency key belongs to another worker");
      if (worker_job_terminal(created.state) || !created.external_job_id.empty())
        return created;
    } catch (const Error &error) {
      if (error.code != ErrorCode::NotFound)
        throw;
      std::size_t active = 0, worker_active = 0;
      for (const auto &record : storage_.list(RecordKind::WorkerJob, "", 10000, 0)) {
        const auto stored = record.get<WorkerJob>();
        if (worker_job_terminal(stored.state))
          continue;
        ++active;
        if (stored.worker_id == worker_id)
          ++worker_active;
      }
      if (active >= max_active_)
        throw Error(ErrorCode::Capacity, "Worker job capacity is exhausted");
      if (worker_active >= max_per_worker_)
        throw Error(ErrorCode::Capacity, "Worker-specific job capacity is exhausted");

      created.id = durable_id;
      created.worker_id = worker_id;
      created.run_id = request.run_id;
      created.node_id = request.node_id;
      created.attempt = request.attempt;
      created.idempotency_key = request.idempotency_key;
      created.request_metadata = {{"task_type", request.task_type},
                                  {"capability", request.capability},
                                  {"deadline", request.deadline},
                                  {"artifact_ids", request.artifact_ids}};
      if (request.metadata.is_object()) {
        for (const auto &key : {"classification", "node_work_id", "node_work_attempt_id"})
          if (request.metadata.contains(key))
            created.request_metadata[key] = request.metadata.at(key);
      }
      if (!storage_.claim({RecordKind::WorkerJob, created.id, created.run_id, Json(created)}))
        return job(durable_id);
      created.state = WorkerJobState::Submitting;
      persist(created);
    }
  }

  {
    std::lock_guard async_lock(async_mutex_);
    if (!async_submissions_.insert(created.id).second)
      return created;
    async_threads_.emplace_back([this, request, id = created.id](std::stop_token) {
      try {
        (void)submit(request);
      } catch (const Error &error) {
        if (error.code == ErrorCode::Storage) {
          log_diagnostic("worker.submission_deferred_after_storage_error",
                         {{"worker_job_id", id}, {"worker_id", request.worker_id}});
        } else {
          try {
            std::lock_guard state_lock(state_mutex_);
            auto failed = job(id);
            if (!worker_job_terminal(failed.state)) {
              failed.state = WorkerJobState::Failed;
              failed.failure_kind = WorkerFailureKind::Job;
              failed.error = bounded_error(error.what());
              failed.completed_at = timestamp();
              persist(failed);
            }
          } catch (...) {
          }
        }
      } catch (...) {
        try {
          std::lock_guard state_lock(state_mutex_);
          auto failed = job(id);
          if (!worker_job_terminal(failed.state)) {
            failed.state = WorkerJobState::Failed;
            failed.failure_kind = WorkerFailureKind::Job;
            failed.error = "Worker submission failed";
            failed.completed_at = timestamp();
            persist(failed);
          }
        } catch (...) {
        }
      }
      std::lock_guard async_lock(async_mutex_);
      async_submissions_.erase(id);
    });
  }
  return created;
}

void WorkerManager::cancel(const std::string &id, WorkerJobState requested_state,
                           const std::string &reason) {
  std::string worker_id;
  std::string external_job_id;
  bool pending_submission = false;
  {
    std::lock_guard state_lock(state_mutex_);
    auto value = job(id);
    if (worker_job_terminal(value.state))
      return;
    value.cancellation_requested = true;
    value.cancellation_error = bounded_error(reason);
    if (value.external_job_id.empty()) {
      pending_submission = true;
      worker_id = value.worker_id;
      persist(value);
    } else {
      worker_id = value.worker_id;
      external_job_id = value.external_job_id;
      persist(value);
    }
  }

  bool acknowledged = false;
  std::string cancellation_error;
  try {
    const auto adapter = registry_.get(worker_id);
    if (pending_submission) {
      acknowledged = adapter->cancel_pending(id);
      if (acknowledged) {
        // cancel_pending may terminate a process group while the transport's
        // submit call still owns its mutex.  Restarting inline would block the
        // LASO execution/lease loop behind that in-flight call.  Queue the
        // bounded restart instead; the next submission also performs the
        // normal transport health check before it uses the adapter.
        std::lock_guard async_lock(async_mutex_);
        async_threads_.emplace_back([adapter](std::stop_token) {
          try {
            adapter->start();
          } catch (...) {
          }
        });
      }
    } else {
      acknowledged = adapter->cancel(external_job_id);
    }
  } catch (...) {
    cancellation_error =
        pending_submission ? "Worker pending cancellation failed" : "Worker cancellation failed";
  }

  std::lock_guard state_lock(state_mutex_);
  auto value = job(id);
  if (worker_job_terminal(value.state))
    return;
  {
    std::lock_guard lock(interaction_mutex_);
    for (const auto &record : storage_.list(RecordKind::WorkerInteraction, "", 10000, 0)) {
      auto interaction = record.get<WorkerInteraction>();
      if (interaction.worker_job_id == value.id &&
          interaction.state == WorkerInteractionState::Pending) {
        interaction.state = WorkerInteractionState::Cancelled;
        interaction.reason = bounded_error(reason);
        interaction.decided_at = timestamp();
        interaction.response = Json{{"decision", "cancelled"}, {"reason", interaction.reason}};
        storage_.commit({{RecordKind::WorkerInteraction, interaction.id, interaction.run_id,
                          Json(interaction)}});
      }
    }
  }
  interaction_changed_.notify_all();
  if (requested_state == WorkerJobState::TimedOut) {
    if (acknowledged) {
      value.cancellation_acknowledged = true;
      value.state = WorkerJobState::TimedOut;
      value.completed_at = timestamp();
    } else {
      value.cancellation_error = cancellation_error.empty()
                                     ? "Worker did not acknowledge timeout cancellation"
                                     : cancellation_error;
    }
  } else if (acknowledged) {
    value.cancellation_acknowledged = true;
    value.state = WorkerJobState::Cancelled;
    value.completed_at = timestamp();
  } else {
    value.cancellation_error =
        cancellation_error.empty() ? "Worker did not acknowledge cancellation" : cancellation_error;
  }
  persist(value);
  state_changed_.notify_all();
}

namespace {
bool interaction_terminal(WorkerInteractionState state) {
  return state != WorkerInteractionState::Pending;
}

WorkerInteractionResponse interaction_response(const WorkerInteraction &interaction) {
  WorkerInteractionResponse result;
  result.request_id = interaction.id;
  result.state = interaction.state;
  result.payload = interaction.response.value("payload", Json::object());
  result.reason = interaction.reason;
  return result;
}
} // namespace

WorkerInteractionResponse
WorkerManager::handle_interaction(const WorkerInteractionRequest &request) {
  if (stopped_)
    throw WorkerTransportError("LASO worker interaction service is stopped");
  if (request.request_id.empty() ||
      request.request_id.size() > process_protocol::max_interaction_id_bytes ||
      request.worker_job_id.empty() ||
      request.worker_job_id.size() > process_protocol::max_interaction_id_bytes ||
      request.worker_id.size() > process_protocol::max_interaction_id_bytes ||
      request.external_job_id.size() > process_protocol::max_interaction_id_bytes ||
      request.session_id.size() > process_protocol::max_interaction_id_bytes ||
      request.title.size() > process_protocol::max_interaction_text_bytes ||
      request.summary.size() > process_protocol::max_interaction_text_bytes ||
      request.created_at.size() > 64 || request.deadline.size() > 64 || request.risk.size() > 128 ||
      request.category.size() > 128 || !request.payload.is_object() ||
      request.payload.dump().size() > process_protocol::max_interaction_payload_bytes)
    throw WorkerTransportError("Worker interaction request exceeds its limits");

  std::unique_lock lock(interaction_mutex_);
  WorkerInteraction interaction;
  bool existing = true;
  try {
    interaction =
        storage_.get(RecordKind::WorkerInteraction, request.request_id).get<WorkerInteraction>();
    if (interaction.worker_job_id != request.worker_job_id || interaction.type != request.type)
      throw WorkerTransportError("Worker interaction id was reused with different content");
  } catch (const Error &error) {
    if (error.code != ErrorCode::NotFound)
      throw;
    existing = false;
  }
  if (!existing) {
    interaction.id = request.request_id;
    interaction.worker_job_id = request.worker_job_id;
    interaction.worker_id = request.worker_id;
    try {
      interaction.run_id =
          storage_.get(RecordKind::WorkerJob, request.worker_job_id).value("run_id", std::string{});
    } catch (const Error &error) {
      if (error.code != ErrorCode::NotFound)
        throw;
    }
    interaction.external_job_id = request.external_job_id;
    interaction.session_id = request.session_id;
    interaction.type = request.type;
    interaction.title = request.title;
    interaction.summary = request.summary;
    interaction.payload = request.payload;
    interaction.created_at = request.created_at.empty() ? timestamp() : request.created_at;
    interaction.deadline = request.deadline;
    interaction.risk = request.risk;
    interaction.category = request.category;

    PolicyResult policy_result{PolicyDecision::RequireApproval, "Human decision required"};
    if (request.type == WorkerInteractionType::Question) {
      policy_result = {PolicyDecision::RequireApproval, "Human answer required"};
    } else if (policy_) {
      const auto resource = request.payload.value("resource", std::string{"worker.permission"});
      policy_result = policy_->evaluate(
          {"", "", resource, request.payload.value("classification", std::string{"public"}),
           request.payload.value("network", false), request.payload.value("remote", false),
           request.type == WorkerInteractionType::Approval});
    }
    if (policy_result.decision == PolicyDecision::Allow) {
      interaction.state = request.type == WorkerInteractionType::Question
                              ? WorkerInteractionState::Answered
                              : WorkerInteractionState::Approved;
      interaction.reason = policy_result.reason;
      interaction.response = Json{{"decision", interaction.state}, {"reason", interaction.reason}};
      interaction.decided_at = timestamp();
    } else if (policy_result.decision == PolicyDecision::Deny) {
      interaction.state = WorkerInteractionState::Denied;
      interaction.reason = policy_result.reason;
      interaction.response = Json{{"decision", "denied"}, {"reason", interaction.reason}};
      interaction.decided_at = timestamp();
    }
    storage_.commit(
        {{RecordKind::WorkerInteraction, interaction.id, interaction.run_id, Json(interaction)}});
  }
  if (interaction_terminal(interaction.state))
    return interaction_response(interaction);

  // timestamp() is fixed-width UTC, so this comparison is valid for values
  // produced by LASO and avoids silently waiting past an already-expired
  // request. The in-memory wait remains bounded even for malformed clocks.
  if (!interaction.deadline.empty() && interaction.deadline <= timestamp()) {
    interaction.state = WorkerInteractionState::Expired;
    interaction.reason = "Worker interaction deadline expired";
    interaction.response = Json{{"decision", "expired"}, {"reason", interaction.reason}};
    interaction.decided_at = timestamp();
    storage_.commit(
        {{RecordKind::WorkerInteraction, interaction.id, interaction.run_id, Json(interaction)}});
    return interaction_response(interaction);
  }

  const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::minutes(5);
  while (!interaction_changed_.wait_until(lock, wait_deadline, [&] {
    try {
      interaction =
          storage_.get(RecordKind::WorkerInteraction, request.request_id).get<WorkerInteraction>();
      return interaction_terminal(interaction.state);
    } catch (...) {
      return false;
    }
  })) {
    interaction =
        storage_.get(RecordKind::WorkerInteraction, request.request_id).get<WorkerInteraction>();
    if (!interaction_terminal(interaction.state)) {
      interaction.state = WorkerInteractionState::Expired;
      interaction.reason = "Worker interaction deadline expired";
      interaction.response = Json{{"decision", "expired"}, {"reason", interaction.reason}};
      interaction.decided_at = timestamp();
      storage_.commit(
          {{RecordKind::WorkerInteraction, interaction.id, interaction.run_id, Json(interaction)}});
    }
    break;
  }
  return interaction_response(interaction);
}

std::vector<Json> WorkerManager::worker_interactions(const std::string &run_id, std::size_t limit,
                                                     std::size_t offset) const {
  return storage_.list(RecordKind::WorkerInteraction, run_id, limit, offset);
}

Json WorkerManager::worker_interaction(const std::string &id) const {
  return storage_.get(RecordKind::WorkerInteraction, id);
}

void WorkerManager::resolve_interaction(const std::string &id, WorkerInteractionState state,
                                        const Json &payload, const std::string &actor,
                                        const std::string &reason) {
  const auto supported =
      state == WorkerInteractionState::Approved || state == WorkerInteractionState::Denied ||
      state == WorkerInteractionState::Answered || state == WorkerInteractionState::Cancelled ||
      state == WorkerInteractionState::Expired;
  if (id.empty() || actor.size() > 128 || reason.size() > 2048 || !payload.is_object() ||
      payload.dump().size() > process_protocol::max_interaction_payload_bytes || !supported)
    throw Error(ErrorCode::Validation, "Invalid worker interaction decision");
  std::lock_guard lock(interaction_mutex_);
  auto interaction = storage_.get(RecordKind::WorkerInteraction, id).get<WorkerInteraction>();
  if (interaction.state != WorkerInteractionState::Pending)
    throw Error(ErrorCode::Conflict, "Worker interaction is no longer pending");
  if (interaction.type != WorkerInteractionType::Question &&
      state == WorkerInteractionState::Answered)
    throw Error(ErrorCode::Validation, "Only questions accept an answer");
  if (interaction.type == WorkerInteractionType::Question &&
      (state == WorkerInteractionState::Approved || state == WorkerInteractionState::Denied))
    throw Error(ErrorCode::Validation, "Questions require an answer or cancellation");
  interaction.state = state;
  interaction.response = Json{{"decision", state}, {"payload", payload}, {"reason", reason}};
  interaction.reason = reason;
  interaction.actor = actor;
  interaction.decided_at = timestamp();
  storage_.commit(
      {{RecordKind::WorkerInteraction, interaction.id, interaction.run_id, Json(interaction)}});
  interaction_changed_.notify_all();
}

void WorkerManager::apply_event(const Event &event) {
  if (stopped_ || event.type.rfind("worker.job.", 0) != 0 || !event.payload.is_object())
    return;
  std::lock_guard state_lock(state_mutex_);
  const auto job_id = event.payload.value("job_id", std::string{});
  if (!bounded_identifier(job_id, max_job_id_bytes))
    return;
  WorkerJob value;
  try {
    value = job(job_id);
    const auto metadata = registry_.get(value.worker_id)->metadata();
    if (event.source_id != metadata.event_source_id)
      return;
    const auto external = event.payload.value("external_job_id", std::string{});
    if (!external.empty()) {
      if (!bounded_identifier(external, max_external_id_bytes))
        return;
      if (!value.external_job_id.empty() && external != value.external_job_id)
        return;
      if (value.external_job_id.empty())
        value.external_job_id = external;
    }
    if (worker_job_terminal(value.state))
      return;
    if (event.type == "worker.job.progress") {
      const auto progress = event.payload.value("progress", Json::object());
      if (progress.dump().size() <= max_metadata_bytes)
        value.result_metadata["progress"] = progress;
      if (event.payload.contains("usage"))
        merge_usage(value.usage, event.payload.at("usage").get<WorkerUsage>());
      if (const auto violation = budget_violation(value); !violation.empty()) {
        value.state = WorkerJobState::Failed;
        value.failure_kind = WorkerFailureKind::Budget;
        value.error = violation;
        value.completed_at = timestamp();
      }
      persist(value);
      return;
    }
    const auto next = event_state(event.type);
    if (next == WorkerJobState::Unknown ||
        (next != value.state && !valid_worker_job_transition(value.state, next)))
      return;
    value.state = next;
    if (next == WorkerJobState::Running && value.started_at.empty())
      value.started_at = timestamp();
    if (next == WorkerJobState::Completed) {
      value.result = event.payload.value("result", event.payload.value("output", Json::object()));
      value.result_metadata = event.payload.value("metadata", Json::object());
      value.artifacts = event.payload.value("artifacts", std::vector<Json>{});
      if (event.payload.contains("usage"))
        merge_usage(value.usage, event.payload.at("usage").get<WorkerUsage>());
    } else if (next == WorkerJobState::Failed) {
      value.error = bounded_error(event.payload.value("error", std::string{"Worker failed"}));
      value.failure_kind = WorkerFailureKind::Job;
    } else if (next == WorkerJobState::Cancelled) {
      value.cancellation_acknowledged = true;
    }
    if (const auto violation = budget_violation(value); !violation.empty()) {
      value.state = WorkerJobState::Failed;
      value.failure_kind = WorkerFailureKind::Budget;
      value.error = violation;
    }
    if (worker_job_terminal(value.state))
      value.completed_at = timestamp();
    persist(value);
  } catch (const Error &) {
    log_diagnostic("worker.event_rejected", {{"event_id", event.id}, {"job_id", job_id}});
  } catch (...) {
    log_diagnostic("worker.event_processing_failed", {{"event_id", event.id}});
  }
}

void WorkerManager::receive(const Event &event) {
  try {
    apply_event(event);
  } catch (...) {
    log_diagnostic("worker.event_callback_failed", {{"event_id", event.id}});
  }
}

void WorkerManager::stop() noexcept {
  stopped_ = true;
}
} // namespace laso
