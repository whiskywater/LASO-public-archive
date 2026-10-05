#pragma once
#include <functional>
#include <laso/core/config.hpp>
#include <laso/events/events.hpp>
#include <laso/nodes/node.hpp>
#include <laso/runtime/workspace.hpp>
#include <laso/schema/validator.hpp>
#include <laso/storage/coordination.hpp>
#include <laso/storage/storage.hpp>
#include <laso/workers/manager.hpp>
#include <memory>
#include <mutex>
#include <set>

namespace laso {
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
enum class SessionTestPoint {
  AfterClaim,
  BeforeRunClaim,
  BeforeRunBinding,
  AfterRunBinding,
  BeforeNodeCheckpointCommit,
  NodeCheckpointRejected,
  BeforeCompletionCommit,
  AfterCompletionCommit
};
#endif
struct RuntimeDependencies {
  Storage &storage;
  EventBus &events;
  ProviderRegistry &providers;
  ToolRegistry &tools;
  FunctionRegistry &functions;
  NodeRegistry &nodes;
  Policy &policy;
  SchemaValidator &schemas;
  std::shared_ptr<WorkerManager> workers;
  std::function<PipelineDefinition(const std::string &)> resolve_pipeline;
  Coordination *coordination = nullptr;
  ArtifactStore *artifacts = nullptr;
  std::string instance_id;
  std::filesystem::path workspace_root;
};
class Runtime {
public:
  Runtime(asio::io_context &io, Config config, RuntimeDependencies dependencies);
  ~Runtime();
  // Returns immediately. Execution is scheduled on the bounded Asio executor.
  std::string run(const PipelineDefinition &, Json input = Json::object(),
                  std::string actor = "local", std::string parent_id = "",
                  std::string parent_node_id = "", unsigned subpipeline_depth = 0,
                  std::string parent_message_id = "", Json origin = Json::object(),
                  Json message_metadata = Json::object(), std::string session_id = {},
                  std::string session_turn_id = {}, std::string session_owner = {},
                  std::uint64_t session_fencing_token = 0);
  void resume(const std::string &id);
  void cancel(const std::string &id);
  void decide(const std::string &approval_id, bool approve, const std::string &actor,
              const std::string &comment);
  void shutdown();
  bool idle() const;
  void start_distributed();
  void dispatch_session(const std::string &session_id);
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
  // Deterministic fault injection is available only in explicit test builds.
  void set_session_test_hook(std::function<void(SessionTestPoint)> hook);
#endif

private:
  struct ParallelState;
  asio::io_context &io_;
  Config config_;
  RuntimeDependencies deps_;
  AsyncLimiter nodes_, models_, tools_;
  mutable std::recursive_mutex mutex_;
  struct ActiveRun {
    std::stop_source stop;
    std::optional<LeaseRecord> lease;
    bool ownership_lost = false;
  };
  struct ActiveNode {
    std::stop_source stop;
    LeaseRecord work_lease;
    std::optional<LeaseRecord> global_slot;
    std::optional<LeaseRecord> run_slot;
    bool ownership_lost = false;
  };
  std::map<std::string, ActiveRun> active_;
  std::map<std::string, ActiveNode> active_nodes_;
  bool stopping_ = false;
  bool distributed_started_ = false;
#ifdef LASO_ENABLE_SESSION_TEST_HOOKS
  std::mutex session_test_hook_mutex_;
  std::function<void(SessionTestPoint)> session_test_hook_;
  void session_test_point(SessionTestPoint point);
#endif
  std::shared_ptr<asio::steady_timer> claim_timer_, lease_timer_, session_timer_;
  Task<void> claim_loop();
  Task<void> lease_loop();
  Task<void> supervise_claim_loop();
  Task<void> supervise_lease_loop();
  Task<void> session_loop();
  Task<void> supervise_session_loop();
  void dispatch_sessions();
  Task<void> execute_distributed_work(NodeWork work, LeaseRecord work_lease,
                                      std::optional<LeaseRecord> global_slot,
                                      std::optional<LeaseRecord> run_slot, std::stop_token stop);
  Task<void> execute(Run run, std::stop_token stop);
  Task<void> execute_branch(const PipelineDefinition &, ExecutionToken,
                            std::shared_ptr<ParallelState>, std::shared_ptr<AsyncLimiter>,
                            std::chrono::steady_clock::time_point, unsigned,
                            std::optional<LeaseRecord> = std::nullopt, std::string = {},
                            std::string = {}, std::string = {});
  Task<bool> execute_parallel(Run &, const PipelineDefinition &, std::shared_ptr<AsyncLimiter>,
                              std::stop_token, std::chrono::steady_clock::time_point, unsigned);
  void schedule(Run run, std::optional<LeaseRecord> lease = std::nullopt);
  void cancel_locked(const std::string &, std::set<std::string> &, std::vector<std::string> &);
  void transition(Run &, RunState, const std::string &event, std::vector<Record> records = {});
  void checkpoint(Run &, const std::string &event, std::vector<Record> records = {});
  std::unique_ptr<Node> make_node(const NodeDefinition &);
  PolicyResult permission(const NodeDefinition &, const Run &) const;
  bool approved(const Run &) const;
  void wait_approval(Run &, const NodeDefinition &, const std::string &reason);
  void reconcile_distributed_parallel(Run &, const PipelineDefinition &);
  void reconcile_terminal_worker(const NodeWork &, const LeaseRecord &);
  bool distributed_parallel_ready(const Run &) const;
  void commit_node_owned(const std::vector<Record> &, const NodeWork &, const LeaseRecord &);
  bool advance(Run &, const PipelineDefinition &, const NodeDefinition &,
               const std::string &condition);
  bool prepare_join(Run &, const NodeDefinition &);
  bool next_ready(Run &);
  void finish_parent(const Run &);
};
} // namespace laso
