#include <chrono>
#include <cstdlib>
#include <laso/application/service.hpp>
#include <laso/runtime/executor.hpp>
#include <thread>

using namespace laso;

namespace {
const char *required(const char *name) {
  const auto *value = std::getenv(name);
  return value && *value ? value : nullptr;
}
} // namespace

int main() {
  const auto *dsn = required("LASO_DISTRIBUTED_TEST_DSN");
  const auto *schema = required("LASO_DISTRIBUTED_TEST_SCHEMA");
  const auto *run_id = required("LASO_DISTRIBUTED_TEST_RUN_ID");
  if (!dsn || !schema || !run_id)
    return 2;
  try {
    const auto hold_ms = required("LASO_DISTRIBUTED_TEST_HOLD_MS");
    const auto delay = Milliseconds{hold_ms ? std::stoll(hold_ms) : 10000LL};
    Config config;
    config.storage_backend = "postgres";
    config.postgres_dsn = dsn;
    config.postgres_schema = schema;
    config.execution_mode = "multi_instance";
    config.max_runs = 1;
    config.max_nodes =
        required("LASO_DISTRIBUTED_TEST_MAX_NODES")
            ? static_cast<unsigned>(std::stoul(required("LASO_DISTRIBUTED_TEST_MAX_NODES")))
            : 1;
    config.max_nodes_per_run = config.max_nodes;
    config.coordination_lease_ttl_ms = 1000;
    config.coordination_heartbeat_interval_ms = 100;
    config.max_pending_runs = 16;
    if (const auto *worker_host = required("LASO_DISTRIBUTED_TEST_WORKER_HOST")) {
      ProcessWorkerConfig worker;
      worker.executable = worker_host;
      worker.args = {"--mode", "delay-ms", "--delay-ms", "200"};
      worker.startup_timeout_ms = 1000;
      worker.request_timeout_ms = 100;
      config.process_workers.emplace("process", std::move(worker));
    }
    config.validate();
    Executor executor(config.workers);
    Service service(executor.context(), config);
    service.functions().add("distributed_hold",
                            std::make_shared<Function>([delay](ExecutionContext &context,
                                                               const Json &input) -> Task<Json> {
                              co_await context.delay(delay);
                              co_return input;
                            }));
    if (required("LASO_DISTRIBUTED_TEST_WORKER_HOST")) {
      service.register_pipeline(R"yaml(
laso: '1'
name: process-worker-timeout
version: 1
timeout_ms: 5000
nodes:
  input: {type: input}
  fork: {type: parallel, join: join}
  timeout: {type: worker, worker: process, task_type: deterministic, max_attempts: 2,
            timeout_ms: 2000, retry_delay_ms: 10}
  stable: {type: function, function: identity}
  join: {type: join}
  output: {type: output}
edges:
  - {from: input, to: fork}
  - {from: fork, to: timeout}
  - {from: fork, to: stable}
  - {from: timeout, to: join}
  - {from: stable, to: join}
  - {from: join, to: output}
)yaml");
    }
    executor.start();
    for (unsigned i = 0; i < 1200; ++i) {
      const auto run = service.get(RecordKind::Run, run_id).get<Run>();
      if (terminal(run.state)) {
        service.shutdown();
        executor.join();
        return run.state == RunState::Completed ? 0 : 1;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    service.shutdown();
    executor.join();
    return 1;
  } catch (...) {
    return 3;
  }
}
