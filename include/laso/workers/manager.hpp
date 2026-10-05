#pragma once
#include <atomic>
#include <condition_variable>
#include <laso/events/events.hpp>
#include <laso/policies/policy.hpp>
#include <laso/storage/storage.hpp>
#include <laso/workers/worker.hpp>
#include <mutex>
#include <set>
#include <thread>

namespace laso {
class WorkerManager final : public EventSubscriber {
public:
  WorkerManager(Storage &, WorkerRegistry &, unsigned max_active = 32, unsigned max_per_worker = 16,
                unsigned max_artifacts = 16, std::uint64_t max_wall_time_ms = 0,
                std::uint64_t max_tokens_per_run = 0, double max_cost_units_per_run = 0.0);
  WorkerManager(Storage &, WorkerRegistry &, Policy &, unsigned max_active = 32,
                unsigned max_per_worker = 16, unsigned max_artifacts = 16,
                std::uint64_t max_wall_time_ms = 0, std::uint64_t max_tokens_per_run = 0,
                double max_cost_units_per_run = 0.0);
  WorkerJob submit(const WorkerRequest &);
  // Begin provider submission without blocking the caller's execution loop.
  // The durable job is created before the adapter is invoked; callers can
  // refresh/cancel it immediately, including while a local process transport
  // is still obtaining its external handle.
  WorkerJob submit_async(const WorkerRequest &);
  std::string job_id_for(const std::string &idempotency_key) const;
  WorkerJob job(const std::string &) const;
  // Refresh an active job through a recovery-capable transport.  This is used
  // by WorkerNode for transports that do not emit asynchronous status events.
  WorkerJob refresh(const std::string &);
  std::vector<Json> jobs(const std::string &run_id = "", std::size_t limit = 1000,
                         std::size_t offset = 0) const;
  Json workers() const;
  Json worker(const std::string &) const;
  bool can_execute(const std::string &worker_id, const std::string &capability) const;
  Json distributed_capabilities() const;
  std::string resolve_worker(const std::string &worker_id, const std::string &capability) const;
  void cancel(const std::string &, WorkerJobState requested_state, const std::string &reason);
  WorkerInteractionResponse handle_interaction(const WorkerInteractionRequest &);
  std::vector<Json> worker_interactions(const std::string &run_id = "", std::size_t limit = 1000,
                                        std::size_t offset = 0) const;
  Json worker_interaction(const std::string &id) const;
  void resolve_interaction(const std::string &, WorkerInteractionState, const Json &payload,
                           const std::string &actor, const std::string &reason);
  void receive(const Event &) override;
  void stop() noexcept;

private:
  Storage &storage_;
  WorkerRegistry &registry_;
  unsigned max_active_, max_per_worker_, max_artifacts_;
  std::uint64_t max_wall_time_ms_, max_tokens_per_run_;
  double max_cost_units_per_run_;
  Policy *policy_ = nullptr;
  std::atomic<bool> stopped_{false};
  mutable std::mutex submit_mutex_;
  mutable std::mutex async_mutex_;
  std::set<std::string> async_submissions_;
  std::vector<std::jthread> async_threads_;
  mutable std::mutex state_mutex_;
  std::condition_variable state_changed_;
  mutable std::mutex interaction_mutex_;
  std::condition_variable interaction_changed_;
  void apply_event(const Event &);
  WorkerJob reconcile(WorkerJob, bool fail_transport);
  void retire_superseded_distributed_jobs_locked();
  void persist(WorkerJob &);
  std::string budget_violation(const WorkerJob &) const;
  static void merge_usage(WorkerUsage &, const WorkerUsage &);
  static std::optional<std::uint64_t> total_tokens(const WorkerUsage &);
  static std::string durable_job_id(const std::string &idempotency_key);
  static WorkerJobState parse_state(const Json &,
                                    WorkerJobState fallback = WorkerJobState::Unknown);
};
} // namespace laso
