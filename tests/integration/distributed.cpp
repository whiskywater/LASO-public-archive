#include "../support.hpp"
#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <laso/runtime/workspace.hpp>
#include <set>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace laso;
using namespace laso::test;

#if defined(LASO_HAS_POSTGRES)
namespace {
std::string test_dsn() {
  const auto *value = std::getenv("LASO_TEST_POSTGRES_DSN");
  return value && *value ? value : std::string{};
}

void clear_distributed_fixture_environment() {
  for (const auto *name : {"LASO_DISTRIBUTED_TEST_MODE",
                           "LASO_DISTRIBUTED_TEST_DSN",
                           "LASO_DISTRIBUTED_TEST_SCHEMA",
                           "LASO_DISTRIBUTED_TEST_RUN_ID",
                           "LASO_DISTRIBUTED_TEST_SESSION_ID",
                           "LASO_DISTRIBUTED_TEST_TURN_ID",
                           "LASO_DISTRIBUTED_TEST_HOLD_MS",
                           "LASO_DISTRIBUTED_TEST_MAX_NODES",
                           "LASO_DISTRIBUTED_TEST_WORKER_HOST",
                           "LASO_DISTRIBUTED_TEST_EXTERNAL_CONTROL",
                           "LASO_DISTRIBUTED_TEST_STOP_MARKER",
                           "LASO_DISTRIBUTED_TEST_HOLD_POINTS",
                           "LASO_DISTRIBUTED_TEST_BARRIER_DIR",
                           "LASO_DISTRIBUTED_TEST_BARRIER_TIMEOUT_MS",
                           "LASO_DISTRIBUTED_TEST_SESSION_EXIT_AT",
                           "LASO_DISTRIBUTED_TEST_MARKER",
                           "LASO_DISTRIBUTED_TEST_RELEASE",
                           "LASO_DISTRIBUTED_TEST_PROVIDER_ENTERED",
                           "LASO_DISTRIBUTED_TEST_PROVIDER_RELEASE",
                           "LASO_DISTRIBUTED_TEST_PROVIDER_RESULT",
                           "LASO_DISTRIBUTED_TEST_PROVIDER_CONTINUATION",
                           "LASO_DISTRIBUTED_TEST_EXPECTED_CONTINUATION",
                           "LASO_DISTRIBUTED_TEST_IO_THREADS",
                           "LASO_DISTRIBUTED_TEST_COORDINATION_LEASE_TTL_MS",
                           "LASO_DISTRIBUTED_TEST_COORDINATION_HEARTBEAT_INTERVAL_MS",
                           "LASO_DISTRIBUTED_TEST_PG_CONNECT_TIMEOUT_MS",
                           "LASO_DISTRIBUTED_TEST_DATA_DIR",
                           "LASO_DISTRIBUTED_TEST_CASE",
                           "LASO_DISTRIBUTED_TEST_TIMEOUT_MARKER",
                           "LASO_DISTRIBUTED_TEST_ERROR_FILE"})
    unsetenv(name);
}

struct IsolatedSchema {
  std::string dsn = test_dsn();
  std::string schema = "distributed_" + uuid();
  IsolatedSchema() {
    std::replace(schema.begin(), schema.end(), '-', '_');
  }
  ~IsolatedSchema() {
    if (dsn.empty())
      return;
    try {
      pqxx::connection connection(dsn);
      pqxx::work transaction(connection);
      transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
      transaction.commit();
    } catch (...) {
    }
  }
};
} // namespace

TEST(DistributedExecution, TwoServicesSharePostgresAndOneCompletesQueuedRun) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  TemporaryDirectory first_data;
  TemporaryDirectory second_data;
  Config first = config(first_data.path);
  first.storage_backend = "postgres";
  first.postgres_dsn = database.dsn;
  first.postgres_schema = database.schema;
  first.execution_mode = "multi_instance";
  first.max_runs = 1;
  first.validate();
  auto second = first;
  second.data_dir = second_data.path;
  second.db_path.clear();

  Executor first_executor(first.workers), second_executor(second.workers);
  Service first_service(first_executor.context(), first);
  Service second_service(second_executor.context(), second);
  const auto pipeline = R"yaml(
laso: '1'
name: distributed
version: 1
nodes:
  input:
    type: input
  transform:
    type: function
    function: identity
  output:
    type: output
edges:
  - from: input
    to: transform
  - from: transform
    to: output
)yaml";
  first_service.register_pipeline(pipeline);
  first_executor.start();
  second_executor.start();
  const auto run_id = first_service.start("distributed@1", Json{{"value", "shared"}});
  laso::Run result;
  for (unsigned attempt = 0; attempt < 200; ++attempt) {
    result = first_service.get(RecordKind::Run, run_id).get<laso::Run>();
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  first_service.shutdown();
  second_service.shutdown();
  first_executor.join();
  second_executor.join();
  EXPECT_EQ(result.state, RunState::Completed);
  EXPECT_FALSE(result.owner_instance_id.empty());
  EXPECT_EQ(result.pipeline_version, 1U);
  EXPECT_EQ(result.message.payload, Json({{"value", "shared"}}));
}

TEST(DistributedExecution, ShortLeaseRenewsDuringLongAsyncWork) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  TemporaryDirectory first_data;
  TemporaryDirectory second_data;
  Config first = config(first_data.path);
  first.storage_backend = "postgres";
  first.postgres_dsn = database.dsn;
  first.postgres_schema = database.schema;
  first.execution_mode = "multi_instance";
  first.max_runs = 2;
  first.max_nodes = 4;
  first.max_nodes_per_run = 2;
  first.coordination_lease_ttl_ms = 3000;
  first.coordination_heartbeat_interval_ms = 500;
  first.validate();
  auto second = first;
  second.data_dir = second_data.path;
  second.db_path.clear();

  Executor first_executor(first.workers), second_executor(second.workers);
  Service first_service(first_executor.context(), first);
  Service second_service(second_executor.context(), second);
  const auto probe_origin = std::chrono::steady_clock::now();
  std::atomic<std::int64_t> action_started_ms{-1};
  std::atomic<std::int64_t> action_finished_ms{-1};
  auto hold =
      std::make_shared<Function>([&](ExecutionContext &context, const Json &input) -> Task<Json> {
        action_started_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - probe_origin)
                                    .count(),
                                std::memory_order_relaxed);
        co_await context.delay(Milliseconds{7000});
        action_finished_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now() - probe_origin)
                                     .count(),
                                 std::memory_order_relaxed);
        co_return input;
      });
  first_service.functions().add("distributed_short_lease_hold", hold);
  second_service.functions().add("distributed_short_lease_hold", hold);
  first_service.register_pipeline(R"yaml(
laso: '1'
name: distributed-short-lease
version: 1
nodes:
  input: {type: input}
  fork: {type: parallel, join: join}
  hold: {type: function, function: distributed_short_lease_hold}
  quick: {type: function, function: identity}
  join: {type: join}
  output: {type: output}
edges:
  - {from: input, to: fork}
  - {from: fork, to: hold}
  - {from: fork, to: quick}
  - {from: hold, to: join}
  - {from: quick, to: join}
  - {from: join, to: output}
)yaml");

  first_executor.start();
  second_executor.start();
  const auto run_id = first_service.start("distributed-short-lease@1", Json{{"value", "held"}});
  const auto started = std::chrono::steady_clock::now();
  std::vector<std::chrono::steady_clock::time_point> observed_renewals;
  std::string observed_work_id;
  std::string previous_heartbeat;
  auto claim_observed_at = std::chrono::steady_clock::time_point{};
  std::int64_t initial_expiry_margin_ms = -1;
  laso::Run result;
  pqxx::connection monitor(database.dsn);
  const auto deadline = started + std::chrono::seconds(20);
  while (std::chrono::steady_clock::now() < deadline) {
    result = first_service.get(RecordKind::Run, run_id).get<laso::Run>();
    for (const auto &value : first_service.list(RecordKind::NodeWork, run_id)) {
      const auto work = value.get<NodeWork>();
      if (work.state != NodeWorkState::Running || work.node_id != "hold")
        continue;
      observed_work_id = work.id;
      pqxx::read_transaction transaction(monitor);
      const auto row = transaction.exec_params(
          "SELECT heartbeat_at::text, floor(extract(epoch from (expires_at - "
          "clock_timestamp())) * 1000)::bigint FROM \"" +
              database.schema + "\".laso_coordination_leases WHERE resource_key = $1",
          "node:" + work.id);
      if (!row.empty()) {
        const auto heartbeat = row.front()[0].as<std::string>();
        if (previous_heartbeat.empty()) {
          previous_heartbeat = heartbeat;
          claim_observed_at = std::chrono::steady_clock::now();
          initial_expiry_margin_ms = row.front()[1].as<std::int64_t>();
        } else if (heartbeat != previous_heartbeat) {
          observed_renewals.push_back(std::chrono::steady_clock::now());
          previous_heartbeat = heartbeat;
        }
      }
    }
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  const auto elapsed = std::chrono::steady_clock::now() - started;
  const auto work = first_service.list(RecordKind::NodeWork, run_id);
  first_service.shutdown();
  second_service.shutdown();
  first_executor.join();
  second_executor.join();

  ASSERT_EQ(result.state, RunState::Completed) << result.error;
  ASSERT_EQ(work.size(), 2U);
  for (const auto &value : work) {
    const auto node_work = value.get<NodeWork>();
    EXPECT_EQ(node_work.state, NodeWorkState::Completed) << node_work.error;
    EXPECT_EQ(node_work.attempt, 1U) << "work was reclaimed despite the live owner";
  }
  EXPECT_GT(elapsed, std::chrono::seconds(3));
  ASSERT_GE(observed_renewals.size(), 4U) << "no sustained database lease renewals observed";
  for (std::size_t index = 1; index < observed_renewals.size(); ++index)
    EXPECT_LT(observed_renewals[index] - observed_renewals[index - 1],
              std::chrono::milliseconds(2500));
  EXPECT_FALSE(observed_work_id.empty());
  ASSERT_NE(claim_observed_at, std::chrono::steady_clock::time_point{});
  std::chrono::milliseconds max_renewal_gap{};
  for (std::size_t index = 1; index < observed_renewals.size(); ++index)
    max_renewal_gap =
        std::max(max_renewal_gap, std::chrono::duration_cast<std::chrono::milliseconds>(
                                      observed_renewals[index] - observed_renewals[index - 1]));
  std::cout
      << "[lease-probe] claim_observed_ms="
      << std::chrono::duration_cast<std::chrono::milliseconds>(claim_observed_at - started).count()
      << " initial_expiry_margin_ms=" << initial_expiry_margin_ms
      << " action_start_ms=" << action_started_ms.load(std::memory_order_relaxed)
      << " action_end_ms=" << action_finished_ms.load(std::memory_order_relaxed)
      << " completion_elapsed_ms="
      << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
      << " observed_renewals=" << observed_renewals.size()
      << " max_observed_renewal_gap_ms=" << max_renewal_gap.count() << '\n';
}

TEST(DistributedExecution, SubpipelineReleasesParentOwnershipWhileChildRuns) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  TemporaryDirectory first_data;
  TemporaryDirectory second_data;
  Config first = config(first_data.path);
  first.storage_backend = "postgres";
  first.postgres_dsn = database.dsn;
  first.postgres_schema = database.schema;
  first.execution_mode = "multi_instance";
  first.max_runs = 1;
  first.validate();
  auto second = first;
  second.data_dir = second_data.path;
  second.db_path.clear();

  Executor first_executor(first.workers), second_executor(second.workers);
  Service first_service(first_executor.context(), first);
  Service second_service(second_executor.context(), second);
  const auto child_pipeline = R"yaml(
laso: '1'
name: child
version: 1
nodes:
  input:
    type: input
  transform:
    type: function
    function: identity
  output:
    type: output
edges:
  - from: input
    to: transform
  - from: transform
    to: output
)yaml";
  const auto parent_pipeline = R"yaml(
laso: '1'
name: parent
version: 1
nodes:
  input:
    type: input
  child:
    type: subpipeline
    pipeline: child@1
  output:
    type: output
edges:
  - from: input
    to: child
  - from: child
    to: output
)yaml";
  first_service.register_pipeline(child_pipeline);
  first_service.register_pipeline(parent_pipeline);
  first_executor.start();
  second_executor.start();
  const auto run_id = first_service.start("parent@1", Json{{"value", "nested"}});
  laso::Run result;
  for (unsigned attempt = 0; attempt < 400; ++attempt) {
    result = first_service.get(RecordKind::Run, run_id).get<laso::Run>();
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  first_service.shutdown();
  second_service.shutdown();
  first_executor.join();
  second_executor.join();
  ASSERT_EQ(result.state, RunState::Completed);
  ASSERT_EQ(result.child_runs.size(), 1U);
  const auto child = first_service.get(RecordKind::Run, result.child_runs.front()).get<laso::Run>();
  EXPECT_EQ(child.state, RunState::Completed);
  EXPECT_EQ(result.message.payload, Json({{"value", "nested"}}));
}

TEST(DistributedExecution, ApprovalReleasesRunForAnyInstanceToResume) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  TemporaryDirectory first_data;
  TemporaryDirectory second_data;
  Config first = config(first_data.path);
  first.storage_backend = "postgres";
  first.postgres_dsn = database.dsn;
  first.postgres_schema = database.schema;
  first.execution_mode = "multi_instance";
  first.max_runs = 1;
  first.validate();
  auto second = first;
  second.data_dir = second_data.path;
  second.db_path.clear();

  Executor first_executor(first.workers), second_executor(second.workers);
  Service first_service(first_executor.context(), first);
  Service second_service(second_executor.context(), second);
  const auto pipeline = R"yaml(
laso: '1'
name: distributed-approval
version: 1
nodes:
  input: {type: input}
  gate: {type: approval, reason: distributed review}
  action: {type: function, function: identity}
  output: {type: output}
edges:
  - {from: input, to: gate}
  - {from: gate, to: action}
  - {from: action, to: output}
)yaml";
  first_service.register_pipeline(pipeline);
  first_executor.start();
  second_executor.start();
  const auto run_id = first_service.start("distributed-approval@1", Json{{"value", "review"}});
  laso::Run result;
  for (unsigned attempt = 0; attempt < 300; ++attempt) {
    result = first_service.get(RecordKind::Run, run_id).get<laso::Run>();
    if (result.state == RunState::WaitingApproval)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(result.state, RunState::WaitingApproval);
  for (unsigned attempt = 0; attempt < 300 && !first_service.runtime().idle(); ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_TRUE(first_service.runtime().idle()) << "approval checkpoint did not settle";
  const auto approvals = first_service.list(RecordKind::Approval, run_id);
  ASSERT_EQ(approvals.size(), 1U);
  first_service.runtime().decide(approvals.front().get<Approval>().id, true, "tester", "");
  for (unsigned attempt = 0; attempt < 400; ++attempt) {
    result = first_service.get(RecordKind::Run, run_id).get<laso::Run>();
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  first_service.shutdown();
  second_service.shutdown();
  first_executor.join();
  second_executor.join();
  ASSERT_EQ(result.state, RunState::Completed) << result.error;
  EXPECT_EQ(result.message.payload, Json({{"value", "review"}}));
}

TEST(DistributedExecution, ParallelBranchesUseDurableNodeWork) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  TemporaryDirectory first_data;
  TemporaryDirectory second_data;
  Config first = config(first_data.path);
  first.storage_backend = "postgres";
  first.postgres_dsn = database.dsn;
  first.postgres_schema = database.schema;
  first.execution_mode = "multi_instance";
  first.max_runs = 2;
  first.max_nodes = 4;
  first.max_nodes_per_run = 2;
  first.coordination_lease_ttl_ms = 1000;
  first.coordination_heartbeat_interval_ms = 100;
  first.validate();
  auto second = first;
  second.data_dir = second_data.path;
  second.db_path.clear();

  Executor first_executor(first.workers), second_executor(second.workers);
  Service first_service(first_executor.context(), first);
  Service second_service(second_executor.context(), second);
  const auto pipeline = R"yaml(
laso: '1'
name: distributed-parallel
version: 1
nodes:
  input:
    type: input
  fork:
    type: parallel
    join: join
  left:
    type: function
    function: identity
  right:
    type: function
    function: identity
  join:
    type: join
  output:
    type: output
edges:
  - {from: input, to: fork}
  - {from: fork, to: left}
  - {from: fork, to: right}
  - {from: left, to: join}
  - {from: right, to: join}
  - {from: join, to: output}
)yaml";
  first_service.register_pipeline(pipeline);
  first_executor.start();
  second_executor.start();
  const auto run_id = first_service.start("distributed-parallel@1", Json{{"value", "shared"}});
  laso::Run result;
  for (unsigned attempt = 0; attempt < 400; ++attempt) {
    result = first_service.get(RecordKind::Run, run_id).get<laso::Run>();
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const auto work = first_service.list(RecordKind::NodeWork, run_id);
  const auto attempts = first_service.list(RecordKind::Attempt, run_id);
  first_service.shutdown();
  second_service.shutdown();
  first_executor.join();
  second_executor.join();
  std::string work_error;
  for (const auto &item : work)
    work_error += item.dump() + "\n";
  ASSERT_EQ(result.state, RunState::Completed)
      << result.error << " pending=" << result.pending_parallel_group << " work=" << work_error;
  ASSERT_EQ(work.size(), 2U);
  EXPECT_EQ(work[0].get<NodeWork>().state, NodeWorkState::Completed);
  EXPECT_EQ(work[1].get<NodeWork>().state, NodeWorkState::Completed);
  bool fork_completed = false;
  for (const auto &value : attempts)
    if (value.get<NodeExecution>().node_id == "fork" &&
        value.get<NodeExecution>().state == NodeState::Completed)
      fork_completed = true;
  EXPECT_TRUE(fork_completed);
  EXPECT_EQ(result.message.payload,
            Json::array({Json{{"value", "shared"}}, Json{{"value", "shared"}}}));
}

TEST(DistributedExecution, DistributedBranchRetriesKeepDistinctAttempts) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  TemporaryDirectory first_data;
  TemporaryDirectory second_data;
  Config first = config(first_data.path);
  first.storage_backend = "postgres";
  first.postgres_dsn = database.dsn;
  first.postgres_schema = database.schema;
  first.execution_mode = "multi_instance";
  first.max_runs = 1;
  first.max_nodes = 2;
  first.max_nodes_per_run = 2;
  first.coordination_lease_ttl_ms = 1000;
  first.coordination_heartbeat_interval_ms = 100;
  first.validate();
  auto second = first;
  second.data_dir = second_data.path;
  second.db_path.clear();

  Executor first_executor(first.workers), second_executor(second.workers);
  Service first_service(first_executor.context(), first);
  Service second_service(second_executor.context(), second);
  auto flaky =
      std::make_shared<Function>([](ExecutionContext &context, const Json &input) -> Task<Json> {
        if (context.attempt == 1)
          throw Error(ErrorCode::Execution, "synthetic retry");
        co_return input;
      });
  first_service.functions().add("distributed_flaky", flaky);
  second_service.functions().add("distributed_flaky", flaky);
  const auto pipeline = R"yaml(
laso: '1'
name: distributed-retry
version: 1
nodes:
  input: {type: input}
  fork: {type: parallel, join: join}
  flaky:
    type: function
    function: distributed_flaky
    max_attempts: 2
  stable: {type: function, function: identity}
  join: {type: join}
  output: {type: output}
edges:
  - {from: input, to: fork}
  - {from: fork, to: flaky}
  - {from: fork, to: stable}
  - {from: flaky, to: join}
  - {from: stable, to: join}
  - {from: join, to: output}
)yaml";
  first_service.register_pipeline(pipeline);
  first_executor.start();
  second_executor.start();
  const auto run_id = first_service.start("distributed-retry@1", Json{{"value", "retry"}});
  laso::Run result;
  for (unsigned attempt = 0; attempt < 500; ++attempt) {
    result = first_service.get(RecordKind::Run, run_id).get<laso::Run>();
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const auto attempts = first_service.list(RecordKind::Attempt, run_id);
  first_service.shutdown();
  second_service.shutdown();
  first_executor.join();
  second_executor.join();
  ASSERT_EQ(result.state, RunState::Completed) << result.error;
  unsigned flaky_attempts = 0;
  std::set<std::string> attempt_ids;
  bool failed = false;
  bool completed = false;
  for (const auto &value : attempts) {
    const auto attempt = value.get<NodeExecution>();
    if (attempt.node_id == "flaky") {
      ++flaky_attempts;
      attempt_ids.insert(attempt.id);
      failed = failed || attempt.state == NodeState::Failed;
      completed = completed || attempt.state == NodeState::Completed;
    }
  }
  EXPECT_EQ(flaky_attempts, 2U);
  EXPECT_EQ(attempt_ids.size(), 2U);
  EXPECT_TRUE(failed);
  EXPECT_TRUE(completed);
}

TEST(DistributedExecution, ProcessCrashAllowsNodeWorkTakeoverAndRunRecovery) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
#if !defined(LASO_DISTRIBUTED_PROCESS)
  GTEST_SKIP() << "distributed process fixture is not built";
#else
  TemporaryDirectory directory;
  Config configuration = config(directory.path);
  configuration.storage_backend = "postgres";
  configuration.postgres_dsn = database.dsn;
  configuration.postgres_schema = database.schema;
  configuration.execution_mode = "multi_instance";
  configuration.max_runs = 1;
  configuration.max_nodes = 1;
  configuration.max_nodes_per_run = 1;
  configuration.coordination_lease_ttl_ms = 1000;
  configuration.coordination_heartbeat_interval_ms = 100;
  configuration.validate();

  asio::io_context io;
  Service seed(io, configuration);
  const auto pipeline = R"yaml(
laso: '1'
name: process-recovery
version: 1
nodes:
  input: {type: input}
  fork: {type: parallel, join: join}
  left: {type: function, function: distributed_hold}
  right: {type: function, function: identity}
  join: {type: join}
  output: {type: output}
edges:
  - {from: input, to: fork}
  - {from: fork, to: left}
  - {from: fork, to: right}
  - {from: left, to: join}
  - {from: right, to: join}
  - {from: join, to: output}
)yaml";
  seed.register_pipeline(pipeline);
  const auto run_id = seed.start("process-recovery@1", Json{{"value", "crash-safe"}});
  seed.shutdown();

  const auto start_process = [&](const char *hold_ms) {
    const auto child = fork();
    if (child == -1) {
      ADD_FAILURE() << "fork failed";
      return pid_t{-1};
    }
    if (child == 0) {
      clear_distributed_fixture_environment();
      setenv("LASO_DISTRIBUTED_TEST_DSN", database.dsn.c_str(), 1);
      setenv("LASO_DISTRIBUTED_TEST_SCHEMA", database.schema.c_str(), 1);
      setenv("LASO_DISTRIBUTED_TEST_RUN_ID", run_id.c_str(), 1);
      setenv("LASO_DISTRIBUTED_TEST_HOLD_MS", hold_ms, 1);
      execl(LASO_DISTRIBUTED_PROCESS, LASO_DISTRIBUTED_PROCESS, nullptr);
      _exit(127);
    }
    return child;
  };
  const auto owner = start_process("10000");
  ASSERT_NE(owner, -1);
  std::string old_owner;
  std::uint64_t old_token = 0;
  std::string old_attempt_id;
  std::string work_id;
  bool old_attempt_persisted = false;
  for (unsigned i = 0; i < 300; ++i) {
    for (const auto &value : seed.list(RecordKind::NodeWork, run_id)) {
      const auto work = value.get<NodeWork>();
      if (work.state == NodeWorkState::Running) {
        work_id = work.id;
        old_owner = work.owner_instance_id;
        old_token = work.fencing_token;
        old_attempt_id = work.attempt_id;
      }
    }
    if (!old_attempt_id.empty()) {
      try {
        (void)seed.get(RecordKind::Attempt, old_attempt_id).get<NodeExecution>();
        old_attempt_persisted = true;
      } catch (const Error &) {
      }
    }
    if (!work_id.empty() && old_attempt_persisted)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  ASSERT_FALSE(work_id.empty());
  ASSERT_TRUE(old_attempt_persisted);
  ASSERT_EQ(kill(owner, SIGKILL), 0);
  int owner_status = 0;
  ASSERT_EQ(waitpid(owner, &owner_status, 0), owner);
  ASSERT_TRUE(WIFSIGNALED(owner_status));
  const auto recovery = start_process("0");
  laso::Run result;
  for (unsigned i = 0; i < 600; ++i) {
    result = seed.get(RecordKind::Run, run_id).get<laso::Run>();
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  int recovery_status = 0;
  ASSERT_EQ(waitpid(recovery, &recovery_status, 0), recovery);
  EXPECT_TRUE(WIFEXITED(recovery_status));
  ASSERT_EQ(result.state, RunState::Completed) << result.error;
  const auto interrupted_attempt =
      seed.get(RecordKind::Attempt, old_attempt_id).get<NodeExecution>();
  EXPECT_EQ(interrupted_attempt.state, NodeState::Failed);
  EXPECT_EQ(interrupted_attempt.error,
            "Distributed node work lease expired before attempt completed");
  EXPECT_FALSE(interrupted_attempt.finished_at.empty());
  const auto works = seed.list(RecordKind::NodeWork, run_id);
  ASSERT_EQ(works.size(), 2U);
  bool retaken = false;
  for (const auto &value : works) {
    const auto work = value.get<NodeWork>();
    EXPECT_EQ(work.state, NodeWorkState::Completed);
    if (work.id == work_id) {
      retaken = work.attempt >= 2 && work.fencing_token > old_token &&
                work.owner_instance_id != old_owner && !work.attempt_id.empty() &&
                work.attempt_id != old_attempt_id;
    }
  }
  EXPECT_TRUE(retaken);
#endif
}

TEST(DistributedExecution, SessionTurnRecoversAfterOwnerProcessDiesDuringExecution) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
#if !defined(LASO_DISTRIBUTED_PROCESS)
  GTEST_SKIP() << "distributed process fixture is not built";
#else
  TemporaryDirectory directory;
  Config configuration = config(directory.path);
  configuration.storage_backend = "postgres";
  configuration.postgres_dsn = database.dsn;
  configuration.postgres_schema = database.schema;
  configuration.execution_mode = "multi_instance";
  configuration.max_runs = 1;
  configuration.coordination_lease_ttl_ms = 1000;
  configuration.coordination_heartbeat_interval_ms = 100;
  configuration.validate();

  const auto pipeline = R"yaml(
laso: '1'
name: session-owner-process-recovery
version: 1
nodes:
  input: {type: input}
  hold: {type: function, function: session_process_hold}
  output: {type: output}
edges:
  - {from: input, to: hold}
  - {from: hold, to: output}
)yaml";
  AgentSession session;
  std::string pipeline_id;
  {
    asio::io_context io;
    Service seed(io, configuration);
    seed.functions().add(
        "session_process_hold",
        std::make_shared<Function>(
            [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; }));
    pipeline_id = seed.register_pipeline(pipeline).at("id").get<std::string>();
    session = seed.create_session(pipeline_id);
    seed.shutdown();
  }

  StorageOptions storage_options;
  storage_options.backend = "postgres";
  storage_options.postgres_dsn = database.dsn;
  storage_options.postgres_schema = database.schema;
  storage_options.allow_multiple_processes = true;
  auto storage = create_storage(storage_options);
  const auto turn_id = session.id + "-turn-owner-process-crash";
  const Json turn{{"idempotency_key", "owner-process-crash"},
                  {"input", {{"value", "survives-owner-death"}}},
                  {"state", "queued"},
                  {"accepted_at", timestamp()},
                  {"pipeline_id", pipeline_id}};
  ASSERT_TRUE(storage->submit_session_turn(session.id, turn_id, turn, Json::object()));

  const auto marker = (directory.path / "provider-started.marker").string();
  const auto owner = fork();
  ASSERT_NE(owner, -1);
  if (owner == 0) {
    clear_distributed_fixture_environment();
    setenv("LASO_DISTRIBUTED_TEST_DSN", database.dsn.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_SCHEMA", database.schema.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_SESSION_ID", session.id.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_TURN_ID", turn_id.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_HOLD_MS", "10000", 1);
    setenv("LASO_DISTRIBUTED_TEST_MARKER", marker.c_str(), 1);
    execl(LASO_DISTRIBUTED_PROCESS, LASO_DISTRIBUTED_PROCESS, nullptr);
    _exit(127);
  }

  bool execution_started = false;
  bool owner_exited_early = false;
  int owner_status = 0;
  for (unsigned i = 0; i < 600; ++i) {
    const auto observed = storage->get(RecordKind::SessionTurn, turn_id);
    const auto run_id = observed.value("run_id", std::string{});
    if (!run_id.empty() && observed.value("state", std::string{}) == "running" &&
        std::filesystem::exists(marker) &&
        storage->get(RecordKind::Run, run_id).get<laso::Run>().state == RunState::Running) {
      execution_started = true;
      break;
    }
    if (waitpid(owner, &owner_status, WNOHANG) == owner) {
      owner_exited_early = true;
      break;
    }
    std::this_thread::sleep_for(Milliseconds{10});
  }
  if (!execution_started) {
    if (!owner_exited_early) {
      kill(owner, SIGKILL);
      waitpid(owner, &owner_status, 0);
    }
    ADD_FAILURE() << "session worker did not enter the provider fixture before the deadline";
    return;
  }
  ASSERT_EQ(kill(owner, SIGKILL), 0);
  ASSERT_EQ(waitpid(owner, &owner_status, 0), owner);
  ASSERT_TRUE(WIFSIGNALED(owner_status));
  EXPECT_EQ(WTERMSIG(owner_status), SIGKILL);

  asio::io_context recovery_io;
  Service recovery(recovery_io, configuration);
  recovery.functions().add(
      "session_process_hold",
      std::make_shared<Function>([](ExecutionContext &context, const Json &input) -> Task<Json> {
        co_await context.delay(Milliseconds{5000});
        co_return input;
      }));
  std::jthread recovery_thread([&] { recovery_io.run(); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  std::string state;
  while (std::chrono::steady_clock::now() < deadline) {
    state = storage->get(RecordKind::SessionTurn, turn_id).value("state", "");
    if (state == "succeeded" || state == "failed" || state == "cancelled")
      break;
    std::this_thread::sleep_for(Milliseconds{10});
  }
  recovery.shutdown();
  recovery_io.stop();
  recovery_thread.join();

  const auto completed = storage->get(RecordKind::SessionTurn, turn_id);
  EXPECT_EQ(state, "succeeded");
  EXPECT_EQ(completed.at("state"), "succeeded");
  EXPECT_EQ(completed.at("result").at("value"), "survives-owner-death");
  const auto run_id = completed.value("run_id", std::string{});
  ASSERT_FALSE(run_id.empty());
  const auto run = storage->get(RecordKind::Run, run_id).get<laso::Run>();
  EXPECT_EQ(run.state, RunState::Completed);
  EXPECT_EQ(run.session_turn_id, turn_id);
  const auto runs = storage->list(RecordKind::Run, "", 10000, 0);
  EXPECT_EQ(std::count_if(runs.begin(), runs.end(),
                          [&](const Json &record) {
                            return record.value("session_turn_id", std::string{}) == turn_id;
                          }),
            1);
  const auto events = storage->session_events(session.id, 0, 100);
  EXPECT_EQ(std::count_if(events.begin(), events.end(),
                          [&](const Json &event) {
                            return event.value("type", std::string{}) ==
                                       "turn.execution.completed" &&
                                   event.value("turn_id", std::string{}) == turn_id;
                          }),
            1);
#endif
}

TEST(DistributedExecution, SessionTurnRecoversAfterHardWorkerProcessDeath) {
#if defined(LASO_HAS_POSTGRES) && defined(LASO_DISTRIBUTED_PROCESS)
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  TemporaryDirectory directory;
  Config configuration = config(directory.path);
  configuration.storage_backend = "postgres";
  configuration.postgres_dsn = database.dsn;
  configuration.postgres_schema = database.schema;
  configuration.execution_mode = "multi_instance";
  configuration.coordination_lease_ttl_ms = 1000;
  configuration.coordination_heartbeat_interval_ms = 100;
  configuration.validate();

  const auto pipeline = R"yaml(
laso: '1'
name: session-hard-worker-recovery
version: 1
nodes:
  input: {type: input}
  work:
    type: function
    function: session_process_hold
    timeout_ms: 60000
  output: {type: output}
edges:
  - {from: input, to: work}
  - {from: work, to: output}
)yaml";
  AgentSession session;
  std::string pipeline_id;
  {
    asio::io_context io;
    Service seed(io, configuration);
    seed.functions().add(
        "session_process_hold",
        std::make_shared<Function>(
            [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; }));
    pipeline_id = seed.register_pipeline(pipeline).at("id").get<std::string>();
    session = seed.create_session(pipeline_id);
    seed.shutdown();
  }

  StorageOptions storage_options;
  storage_options.backend = "postgres";
  storage_options.postgres_dsn = database.dsn;
  storage_options.postgres_schema = database.schema;
  storage_options.allow_multiple_processes = true;
  auto storage = create_storage(storage_options);
  const auto first_turn = session.id + "-turn-hard-worker-death";
  const auto second_turn = session.id + "-turn-after-hard-worker-death";
  const auto submit = [&](const std::string &turn_id, const std::string &key,
                          const std::string &value) {
    return storage->submit_session_turn(session.id, turn_id,
                                        Json{{"idempotency_key", key},
                                             {"input", {{"value", value}}},
                                             {"state", "queued"},
                                             {"accepted_at", timestamp()},
                                             {"pipeline_id", pipeline_id}},
                                        Json::object());
  };
  ASSERT_TRUE(submit(first_turn, "hard-worker-death-first", "survives-hard-worker-death"));
  ASSERT_TRUE(submit(second_turn, "hard-worker-death-second", "next-turn-progresses"));

  struct ChildProcess {
    pid_t pid = -1;
    ~ChildProcess() {
      if (pid > 0) {
        (void)kill(pid, SIGKILL);
        int status = 0;
        (void)waitpid(pid, &status, 0);
      }
    }
    bool poll_exit(int &status) {
      if (pid <= 0)
        return true;
      if (waitpid(pid, &status, WNOHANG) == pid) {
        pid = -1;
        return true;
      }
      return false;
    }
  };
  ChildProcess worker;
  const auto marker = directory.path / "worker-in-function.marker";
  const auto release = directory.path / "worker-in-function.release";
  const auto child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    clear_distributed_fixture_environment();
    setenv("LASO_DISTRIBUTED_TEST_DSN", database.dsn.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_SCHEMA", database.schema.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_SESSION_ID", session.id.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_TURN_ID", first_turn.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_HOLD_MS", "0", 1);
    setenv("LASO_DISTRIBUTED_TEST_IO_THREADS", "3", 1);
    setenv("LASO_DISTRIBUTED_TEST_COORDINATION_LEASE_TTL_MS", "1000", 1);
    setenv("LASO_DISTRIBUTED_TEST_COORDINATION_HEARTBEAT_INTERVAL_MS", "100", 1);
    setenv("LASO_DISTRIBUTED_TEST_BARRIER_TIMEOUT_MS", "60000", 1);
    setenv("LASO_DISTRIBUTED_TEST_MARKER", marker.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_RELEASE", release.c_str(), 1);
    execl(LASO_DISTRIBUTED_PROCESS, LASO_DISTRIBUTED_PROCESS, nullptr);
    _exit(127);
  }
  worker.pid = child;

  bool entered = false;
  int worker_status = 0;
  const auto entered_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{15};
  while (std::chrono::steady_clock::now() < entered_deadline) {
    if (std::filesystem::exists(marker)) {
      entered = true;
      break;
    }
    if (worker.poll_exit(worker_status))
      break;
    std::this_thread::sleep_for(Milliseconds{10});
  }
  ASSERT_TRUE(entered) << "worker did not enter the active function before the deadline";
  const auto running = storage->get(RecordKind::SessionTurn, first_turn);
  ASSERT_EQ(running.value("state", std::string{}), "running");
  ASSERT_FALSE(running.value("cancellation_requested", false));
  const auto run_id = running.value("run_id", std::string{});
  ASSERT_FALSE(run_id.empty());
  const auto run = storage->get(RecordKind::Run, run_id).get<laso::Run>();
  ASSERT_FALSE(terminal(run.state));
  CoordinationOptions coordination_options;
  coordination_options.postgres_dsn = database.dsn;
  coordination_options.postgres_schema = database.schema;
  auto observer = create_coordination(coordination_options, "hard-worker-death-observer-" + uuid());
  const auto lease = observer->inspect("run:" + run_id);
  ASSERT_TRUE(lease);
  ASSERT_TRUE(lease->active);
  EXPECT_EQ(lease->owner_instance, run.owner_instance_id);
  EXPECT_EQ(lease->fencing_token, run.fencing_token);

  asio::io_context recovery_io;
  Service recovery(recovery_io, configuration);
  recovery.functions().add(
      "session_process_hold",
      std::make_shared<Function>(
          [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; }));
  std::jthread recovery_thread([&] { recovery_io.run(); });

  ASSERT_EQ(kill(worker.pid, SIGKILL), 0);
  ASSERT_EQ(waitpid(worker.pid, &worker_status, 0), worker.pid);
  worker.pid = -1;
  ASSERT_TRUE(WIFSIGNALED(worker_status));
  ASSERT_EQ(WTERMSIG(worker_status), SIGKILL);

  const auto recovery_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{15};
  std::string first_state;
  std::string second_state;
  while (std::chrono::steady_clock::now() < recovery_deadline) {
    first_state = storage->get(RecordKind::SessionTurn, first_turn).value("state", std::string{});
    second_state = storage->get(RecordKind::SessionTurn, second_turn).value("state", std::string{});
    if (first_state == "succeeded" && second_state == "succeeded")
      break;
    std::this_thread::sleep_for(Milliseconds{10});
  }
  recovery.shutdown();
  recovery_io.stop();
  recovery_thread.join();

  EXPECT_EQ(first_state, "succeeded") << "hard worker death must not become implicit cancellation";
  EXPECT_EQ(second_state, "succeeded") << "the next queued turn must progress after recovery";
  const auto recovered = storage->get(RecordKind::SessionTurn, first_turn);
  EXPECT_FALSE(recovered.value("cancellation_requested", false));
  EXPECT_EQ(recovered.at("result").at("value"), "survives-hard-worker-death");
  for (const auto &turn_id : {first_turn, second_turn}) {
    const auto current = storage->get(RecordKind::SessionTurn, turn_id);
    const auto runs = storage->list(RecordKind::Run, "", 10000, 0);
    EXPECT_EQ(std::count_if(runs.begin(), runs.end(),
                            [&](const Json &record) {
                              return record.value("session_turn_id", std::string{}) == turn_id;
                            }),
              1);
    const auto events = storage->session_events(session.id, 0, 100);
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [&](const Json &event) {
                              return event.value("type", std::string{}) ==
                                         "turn.execution.completed" &&
                                     event.value("turn_id", std::string{}) == turn_id;
                            }),
              1);
    EXPECT_FALSE(current.value("cancellation_requested", false));
  }
#else
  GTEST_SKIP() << "PostgreSQL process recovery fixture is not enabled";
#endif
}

TEST(DistributedExecution, SessionProcessCrashBoundariesRecoverDurably) {
#if defined(LASO_HAS_POSTGRES) && defined(LASO_ENABLE_SESSION_TEST_HOOKS) &&                       \
    defined(LASO_DISTRIBUTED_PROCESS)
  if (test_dsn().empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  const auto pipeline = R"yaml(
laso: '1'
name: session-process-crash-boundary
version: 1
nodes:
  input: {type: input}
  work: {type: function, function: session_process_hold}
  output: {type: output}
edges:
  - {from: input, to: work}
  - {from: work, to: output}
)yaml";
  const std::vector<std::string> boundaries{"after-claim", "before-run-binding",
                                            "after-run-binding", "before-completion-commit",
                                            "after-completion-commit"};
  for (const auto &boundary : boundaries) {
    IsolatedSchema database;
    TemporaryDirectory directory;
    Config configuration = config(directory.path);
    configuration.storage_backend = "postgres";
    configuration.postgres_dsn = database.dsn;
    configuration.postgres_schema = database.schema;
    configuration.execution_mode = "multi_instance";
    configuration.coordination_lease_ttl_ms = 1000;
    configuration.coordination_heartbeat_interval_ms = 100;
    configuration.validate();

    AgentSession session;
    std::string pipeline_id;
    {
      asio::io_context io;
      Service seed(io, configuration);
      seed.functions().add(
          "session_process_hold",
          std::make_shared<Function>(
              [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; }));
      pipeline_id = seed.register_pipeline(pipeline).at("id").get<std::string>();
      session = seed.create_session(pipeline_id);
      seed.shutdown();
    }

    StorageOptions storage_options;
    storage_options.backend = "postgres";
    storage_options.postgres_dsn = database.dsn;
    storage_options.postgres_schema = database.schema;
    storage_options.allow_multiple_processes = true;
    auto storage = create_storage(storage_options);
    const auto turn_id = session.id + "-turn-" + boundary;
    const Json turn{{"idempotency_key", boundary},
                    {"input", {{"value", boundary}}},
                    {"state", "queued"},
                    {"accepted_at", timestamp()},
                    {"pipeline_id", pipeline_id}};
    ASSERT_TRUE(storage->submit_session_turn(session.id, turn_id, turn, Json::object()))
        << "boundary=" << boundary;

    const auto child = fork();
    ASSERT_NE(child, -1) << "boundary=" << boundary;
    if (child == 0) {
      clear_distributed_fixture_environment();
      setenv("LASO_DISTRIBUTED_TEST_DSN", database.dsn.c_str(), 1);
      setenv("LASO_DISTRIBUTED_TEST_SCHEMA", database.schema.c_str(), 1);
      setenv("LASO_DISTRIBUTED_TEST_SESSION_ID", session.id.c_str(), 1);
      setenv("LASO_DISTRIBUTED_TEST_TURN_ID", turn_id.c_str(), 1);
      setenv("LASO_DISTRIBUTED_TEST_HOLD_MS", "0", 1);
      setenv("LASO_DISTRIBUTED_TEST_SESSION_EXIT_AT", boundary.c_str(), 1);
      execl(LASO_DISTRIBUTED_PROCESS, LASO_DISTRIBUTED_PROCESS, nullptr);
      _exit(127);
    }

    bool exited = false;
    int child_status = 0;
    for (unsigned attempt = 0; attempt < 600; ++attempt) {
      if (waitpid(child, &child_status, WNOHANG) == child) {
        exited = true;
        break;
      }
      std::this_thread::sleep_for(Milliseconds{10});
    }
    if (!exited) {
      kill(child, SIGKILL);
      waitpid(child, &child_status, 0);
      ADD_FAILURE() << "fixture did not reach crash boundary " << boundary;
      continue;
    }
    ASSERT_TRUE(WIFEXITED(child_status)) << "boundary=" << boundary;
    ASSERT_EQ(WEXITSTATUS(child_status), 86) << "boundary=" << boundary;

    const auto interrupted = storage->get(RecordKind::SessionTurn, turn_id);
    const auto prior_run_id = interrupted.value("run_id", std::string{});
    if (boundary == "after-claim" || boundary == "before-run-binding") {
      EXPECT_EQ(interrupted.at("state"), "claimed") << "boundary=" << boundary;
      EXPECT_TRUE(prior_run_id.empty()) << "boundary=" << boundary;
    } else if (boundary == "after-run-binding" || boundary == "before-completion-commit") {
      EXPECT_EQ(interrupted.at("state"), "running") << "boundary=" << boundary;
      EXPECT_FALSE(prior_run_id.empty()) << "boundary=" << boundary;
    } else {
      EXPECT_EQ(interrupted.at("state"), "succeeded") << "boundary=" << boundary;
      EXPECT_FALSE(prior_run_id.empty()) << "boundary=" << boundary;
    }

    asio::io_context recovery_io;
    Service recovery(recovery_io, configuration);
    recovery.functions().add(
        "session_process_hold",
        std::make_shared<Function>(
            [](ExecutionContext &, const Json &input) -> Task<Json> { co_return input; }));
    std::jthread recovery_thread([&] { recovery_io.run(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    std::string state;
    while (std::chrono::steady_clock::now() < deadline) {
      state = storage->get(RecordKind::SessionTurn, turn_id).value("state", "");
      if (state == "succeeded" || state == "failed" || state == "cancelled")
        break;
      std::this_thread::sleep_for(Milliseconds{10});
    }
    recovery.shutdown();
    recovery_io.stop();
    recovery_thread.join();

    const auto completed = storage->get(RecordKind::SessionTurn, turn_id);
    EXPECT_EQ(state, "succeeded") << "boundary=" << boundary;
    EXPECT_EQ(completed.at("state"), "succeeded") << "boundary=" << boundary;
    EXPECT_EQ(completed.at("result").at("value"), boundary) << "boundary=" << boundary;
    const auto run_id = completed.value("run_id", std::string{});
    ASSERT_FALSE(run_id.empty()) << "boundary=" << boundary;
    const auto associated_runs = storage->list(RecordKind::Run, "", 10000, 0);
    EXPECT_EQ(std::count_if(associated_runs.begin(), associated_runs.end(),
                            [&](const Json &record) {
                              return record.value("session_turn_id", std::string{}) == turn_id;
                            }),
              1)
        << "boundary=" << boundary;
    const auto events = storage->session_events(session.id, 0, 100);
    EXPECT_EQ(std::count_if(events.begin(), events.end(),
                            [&](const Json &event) {
                              return event.value("type", std::string{}) ==
                                         "turn.execution.completed" &&
                                     event.value("turn_id", std::string{}) == turn_id;
                            }),
              1)
        << "boundary=" << boundary;
  }
#else
  GTEST_SKIP() << "PostgreSQL session crash hooks and process fixture are not enabled";
#endif
}

TEST(DistributedExecution, StaleNodeCompletionIsRejectedByFencing) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
#if defined(LASO_HAS_POSTGRES)
  StorageOptions storage_options;
  storage_options.backend = "postgres";
  storage_options.postgres_dsn = database.dsn;
  storage_options.postgres_schema = database.schema;
  storage_options.allow_multiple_processes = true;
  auto storage = create_storage(storage_options);
  CoordinationOptions coordination_options;
  coordination_options.postgres_dsn = database.dsn;
  coordination_options.postgres_schema = database.schema;
  auto first = create_coordination(coordination_options, "stale-first");
  auto second = create_coordination(coordination_options, "stale-second");
  NodeWork work;
  work.id = "stale-node-work";
  work.run_id = "run-1";
  work.group_id = "group-1";
  work.node_id = "branch";
  work.join = "join";
  work.token = {"branch", Message{}, {{"group-1", "join", 1, 0}}};
  storage->commit({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}});
  const auto old = first->acquire("node:" + work.id, 100);
  ASSERT_TRUE(old);
  work.state = NodeWorkState::Running;
  work.attempt = 1;
  storage->commit_owned({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}},
                        "node:" + work.id, old->owner_instance, old->fencing_token);
  std::this_thread::sleep_for(std::chrono::milliseconds{180});
  const auto current = second->acquire("node:" + work.id, 5000);
  ASSERT_TRUE(current);
  EXPECT_GT(current->fencing_token, old->fencing_token);
  TemporaryDirectory artifact_directory;
  LocalArtifactStore artifact_store(artifact_directory.path / "artifacts", *storage);
  const auto workspace_root = artifact_directory.path / "workspace";
  std::filesystem::create_directories(workspace_root);
  std::ofstream(workspace_root / "result.txt") << "stale result\n";
  Artifact artifact_metadata;
  artifact_metadata.run_id = work.run_id;
  artifact_metadata.node_id = work.node_id;
  artifact_metadata.name = "result.txt";
  const auto uploaded = artifact_store.put_file(artifact_metadata, workspace_root / "result.txt");
  const auto stale_manifest =
      workspace_manifest(workspace_root, artifact_store, WorkspaceManifestLimits{}, work.run_id,
                         work.id, "stale-attempt", old->owner_instance, old->fencing_token);
  work.result = Message{};
  work.result->metadata["workspace_result_manifest"] = stale_manifest;
  EXPECT_TRUE(artifact_store.exists(uploaded.object_id));
  work.state = NodeWorkState::Completed;
  EXPECT_THROW(storage->commit_owned({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}},
                                     "node:" + work.id, old->owner_instance, old->fencing_token),
               Error);
  storage->commit_owned({{RecordKind::NodeWork, work.id, work.run_id, Json(work)}},
                        "node:" + work.id, current->owner_instance, current->fencing_token);
  EXPECT_EQ(storage->get(RecordKind::NodeWork, work.id).get<NodeWork>().state,
            NodeWorkState::Completed);
#else
  GTEST_SKIP() << "PostgreSQL backend is not enabled";
#endif
}

TEST(DistributedExecution, CancellationPropagatesToRemoteNodeWork) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
#if defined(LASO_DISTRIBUTED_PROCESS)
  TemporaryDirectory directory;
  Config configuration = config(directory.path);
  configuration.storage_backend = "postgres";
  configuration.postgres_dsn = database.dsn;
  configuration.postgres_schema = database.schema;
  configuration.execution_mode = "multi_instance";
  configuration.max_runs = 1;
  configuration.max_nodes = 1;
  configuration.max_nodes_per_run = 1;
  configuration.coordination_lease_ttl_ms = 1000;
  configuration.coordination_heartbeat_interval_ms = 100;
  configuration.validate();
  asio::io_context io;
  Service controller(io, configuration);
  const auto pipeline = R"yaml(
laso: '1'
name: process-cancel
version: 1
nodes:
  input: {type: input}
  fork: {type: parallel, join: join}
  left: {type: function, function: distributed_hold}
  right: {type: function, function: identity}
  join: {type: join}
  output: {type: output}
edges:
  - {from: input, to: fork}
  - {from: fork, to: left}
  - {from: fork, to: right}
  - {from: left, to: join}
  - {from: right, to: join}
  - {from: join, to: output}
)yaml";
  controller.register_pipeline(pipeline);
  const auto run_id = controller.start("process-cancel@1");
  const auto child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    clear_distributed_fixture_environment();
    setenv("LASO_DISTRIBUTED_TEST_DSN", database.dsn.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_SCHEMA", database.schema.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_RUN_ID", run_id.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_HOLD_MS", "10000", 1);
    execl(LASO_DISTRIBUTED_PROCESS, LASO_DISTRIBUTED_PROCESS, nullptr);
    _exit(127);
  }
  bool running = false;
  for (unsigned i = 0; i < 300; ++i) {
    for (const auto &value : controller.list(RecordKind::NodeWork, run_id))
      running = running || value.get<NodeWork>().state == NodeWorkState::Running;
    if (running)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  ASSERT_TRUE(running);
  controller.runtime().cancel(run_id);
  auto result = controller.get(RecordKind::Run, run_id).get<laso::Run>();
  for (unsigned i = 0; i < 300 && !terminal(result.state); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    result = controller.get(RecordKind::Run, run_id).get<laso::Run>();
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(result.state, RunState::Cancelled);
  for (const auto &value : controller.list(RecordKind::NodeWork, run_id))
    EXPECT_EQ(value.get<NodeWork>().state, NodeWorkState::Cancelled);
#else
  GTEST_SKIP() << "distributed process fixture is not built";
#endif
}

TEST(DistributedExecution, TimedOutWorkerAttemptReconcilesNodeWork) {
  IsolatedSchema database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
#if defined(LASO_DISTRIBUTED_PROCESS)
  TemporaryDirectory directory;
  Config configuration = config(directory.path);
  configuration.storage_backend = "postgres";
  configuration.postgres_dsn = database.dsn;
  configuration.postgres_schema = database.schema;
  configuration.execution_mode = "multi_instance";
  configuration.max_runs = 1;
  configuration.max_nodes = 2;
  configuration.max_nodes_per_run = 2;
  configuration.coordination_lease_ttl_ms = 1000;
  configuration.coordination_heartbeat_interval_ms = 100;
  configuration.validate();

  asio::io_context io;
  Service controller(io, configuration);
  const auto pipeline = R"yaml(
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
)yaml";
  controller.register_pipeline(pipeline);
  const auto run_id = controller.start("process-worker-timeout@1", Json{{"value", "timeout"}});
  const auto child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    clear_distributed_fixture_environment();
    setenv("LASO_DISTRIBUTED_TEST_DSN", database.dsn.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_SCHEMA", database.schema.c_str(), 1);
    setenv("LASO_DISTRIBUTED_TEST_WORKER_HOST", LASO_PROCESS_WORKER_HOST, 1);
    setenv("LASO_DISTRIBUTED_TEST_MAX_NODES", "2", 1);
    setenv("LASO_DISTRIBUTED_TEST_RUN_ID", run_id.c_str(), 1);
    execl(LASO_DISTRIBUTED_PROCESS, LASO_DISTRIBUTED_PROCESS, nullptr);
    _exit(127);
  }

  laso::Run result;
  for (unsigned i = 0; i < 300; ++i) {
    result = controller.get(RecordKind::Run, run_id).get<laso::Run>();
    if (terminal(result.state))
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 1);
  result = controller.get(RecordKind::Run, run_id).get<laso::Run>();
  controller.shutdown();
  ASSERT_EQ(result.state, RunState::Failed) << result.error;
  const auto work = controller.list(RecordKind::NodeWork, run_id);
  ASSERT_EQ(work.size(), 2U);
  for (const auto &value : work)
    EXPECT_TRUE(terminal(value.get<NodeWork>().state)) << value.dump();
  const auto attempts = controller.list(RecordKind::Attempt, run_id);
  std::set<std::string> worker_jobs;
  for (const auto &value : attempts) {
    const auto attempt = value.get<NodeExecution>();
    if (attempt.node_id == "timeout") {
      if (!attempt.worker_job_id.empty())
        worker_jobs.insert(attempt.worker_job_id);
    }
  }
  EXPECT_EQ(worker_jobs.size(), 2U);
  const auto jobs = controller.worker_jobs(run_id);
  ASSERT_EQ(jobs.size(), 2U);
  for (const auto &value : jobs)
    EXPECT_EQ(value.at("status"), "TimedOut");
#else
  GTEST_SKIP() << "distributed process fixture is not built";
#endif
}

TEST(DistributedExecution, MultiInstanceRejectsSQLite) {
  TemporaryDirectory directory;
  auto configuration = config(directory.path);
  configuration.execution_mode = "multi_instance";
  EXPECT_THROW(configuration.validate(), Error);
}
#else
TEST(DistributedExecution, PostgreSQLBackendNotBuilt) {
  GTEST_SKIP() << "LASO_ENABLE_POSTGRES is not enabled";
}
#endif
