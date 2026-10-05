#pragma once
#include <laso/application/event_ingress.hpp>
#include <laso/artifacts/artifacts.hpp>
#include <laso/plugins/loader.hpp>
#include <laso/runtime/runtime.hpp>
#include <laso/scheduler/scheduler.hpp>
#include <laso/schema/validator.hpp>
#include <laso/security/security.hpp>
#include <laso/storage/coordination.hpp>
#include <laso/storage/factory.hpp>
#include <laso/workers/process_transport.hpp>

namespace laso {
class Service {
public:
  Service(asio::io_context &, Config);
  ~Service() noexcept;
  const Config &config() const {
    return config_;
  }
  const std::string &instance_id() const {
    return instance_id_;
  }
  Json register_pipeline(const std::string &yaml);
  std::string start(const std::string &name_or_path, const Json &input = Json::object(),
                    const std::string &actor = "local", bool allow_file = false,
                    Json origin = Json::object(), Json message_metadata = Json::object());
  AgentSession create_session(const std::string &pipeline_id);
  AgentSession agent_session(const std::string &id) const;
  void close_session(const std::string &id);
  Json submit_session_turn(const std::string &id, const std::string &idempotency_key,
                           const Json &input);
  std::vector<Json> session_events(const std::string &id, std::uint64_t after,
                                   std::size_t limit) const;
  Json create_schedule(const Json &spec);
  Json update_schedule(const std::string &id, const Json &spec);
  void set_schedule_enabled(const std::string &id, bool enabled);
  void delete_schedule(const std::string &id);
  Json create_trigger(const Json &spec);
  Json update_trigger(const std::string &id, const Json &spec);
  void set_trigger_enabled(const std::string &id, bool enabled);
  void delete_trigger(const std::string &id);
  Json run_view(const std::string &id) const;
  // Operator-facing views omit message payloads, prompts, artifact locations,
  // and other potentially sensitive content while preserving durable lineage.
  std::vector<Json> inspect_runs() const;
  Json inspect_run(const std::string &id) const;
  std::vector<Json> inspect_node_works(const std::string &run_id = "") const;
  Json inspect_node_work(const std::string &id) const;
  std::vector<Json> inspect_artifacts(const std::string &run_id = "") const;
  Json artifact_integrity() const;
  Json artifact_gc(bool dry_run, std::uint64_t grace_seconds = 0);
  std::vector<Json> inspect_worker_jobs(const std::string &run_id = "") const;
  Json inspect_worker_job(const std::string &id) const;
  Json get(RecordKind kind, const std::string &id) const {
    return storage_->get(kind, id);
  }
  std::vector<Json> list(RecordKind kind, const std::string &run_id = "", std::size_t limit = 1000,
                         std::size_t offset = 0) const {
    return storage_->list(kind, run_id, limit, offset);
  }
  Json providers() const;
  Json tools() const;
  Json plugins() const;
  Json event_sources() const;
  Json event_source(const std::string &id) const;
  void set_event_source_enabled(const std::string &id, bool enabled);
  Json workers() const;
  Json worker(const std::string &id) const;
  std::vector<Json> worker_jobs(const std::string &run_id = "", std::size_t limit = 1000,
                                std::size_t offset = 0) const;
  Json worker_job(const std::string &id) const;
  void cancel_worker_job(const std::string &id);
  std::vector<Json> worker_interactions(const std::string &run_id = "", std::size_t limit = 1000,
                                        std::size_t offset = 0) const;
  Json worker_interaction(const std::string &id) const;
  void resolve_worker_interaction(const std::string &, WorkerInteractionState, const Json &,
                                  const std::string &, const std::string &);
  Runtime &runtime() {
    return runtime_;
  }
  FunctionRegistry &functions() {
    return functions_;
  }
  ToolRegistry &tool_registry() {
    return tools_;
  }
  ProviderRegistry &provider_registry() {
    return providers_;
  }
  NodeRegistry &node_registry() {
    return nodes_;
  }
  EventBus &event_bus() {
    return events_;
  }
  ArtifactStore &artifacts() {
    return *artifacts_;
  }
  EventIngress &event_ingress() {
    return ingress_;
  }
  Scheduler &scheduler() {
    return scheduler_;
  }
  void shutdown();
  Json instances() const;

private:
  Config config_;
  std::string instance_id_;
  std::unique_ptr<ProcessLease> lease_;
  std::unique_ptr<Storage> storage_;
  std::unique_ptr<Coordination> coordination_;
  InProcessEventBus events_;
  ProviderRegistry providers_;
  ToolRegistry tools_;
  WorkerRegistry worker_registry_;
  FunctionRegistry functions_;
  NodeRegistry nodes_;
  PolicyEngine policy_;
  SchemaValidator schemas_;
  EventIngress ingress_;
  std::shared_ptr<WorkerManager> worker_manager_;
  PluginLoader plugins_;
  std::vector<std::shared_ptr<ProcessWorkerTransport>> process_workers_;
  std::unique_ptr<ArtifactStore> artifacts_;
  Runtime runtime_;
  LocalScheduler scheduler_;
  bool shutdown_ = false;
  Json pipeline_record(const std::string &reference) const;
  PipelineDefinition resolve_pipeline(const std::string &reference) const;
  void recover_history();
};
} // namespace laso
