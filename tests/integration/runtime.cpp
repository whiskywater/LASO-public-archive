#include "../support.hpp"
#include <atomic>
#include <fstream>
#include <set>

using namespace laso;
using namespace laso::test;
namespace {
std::string composed_pipeline(const std::string &name, unsigned version,
                              const std::string &root_fields, const std::string &nodes,
                              const std::string &edges) {
  return "laso: '1'\nname: " + name + "\nversion: " + std::to_string(version) + "\n" + root_fields +
         "nodes:\n" + nodes + "edges:\n" + edges;
}
std::string identity_pipeline(const std::string &name, unsigned version,
                              const std::string &root_fields = "") {
  return composed_pipeline(name, version, root_fields,
                           "  action:\n    type: function\n    function: identity\n",
                           "  - {from: input, to: action}\n  - {from: action, to: output}\n");
}
} // namespace
TEST(Runtime, DeterministicExecutionAndHistory) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service service(io, config(dir.path));
  auto run = execute(service, io, fixture("hello-pipeline"), {{"number", 3}});
  EXPECT_EQ(run.state, RunState::Completed);
  EXPECT_EQ(run.message.payload.at("greeting"), "Hello from LASO");
  EXPECT_EQ(service.list(RecordKind::Attempt, run.id).size(), 3U);
  EXPECT_EQ(service.list(RecordKind::Message, run.id).size(), 3U);
  EXPECT_GT(service.list(RecordKind::Event, run.id).size(), 3U);
}
TEST(Runtime, AgentReviewUsesMockProviders) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto r = execute(s, io, fixture("agent-review"));
  EXPECT_EQ(r.state, RunState::Completed);
  EXPECT_TRUE(r.message.payload.at("reviewed").get<bool>());
}
TEST(Runtime, RecordsNodeFailureWithoutThrowingToHost) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add("fail",
                    std::make_shared<Function>([](ExecutionContext &, const Json &) -> Task<Json> {
                      throw std::runtime_error("DO_NOT_PERSIST_EXCEPTION_CONTENT");
                      co_return nullptr;
                    }));
  auto r = execute(s, io, single("type: function\n    function: fail"));
  EXPECT_EQ(r.state, RunState::Failed);
  auto attempts = s.list(RecordKind::Attempt, r.id);
  ASSERT_EQ(attempts.size(), 2U);
  EXPECT_EQ(attempts.back().at("state"), "Failed");
  EXPECT_EQ(Json(r).dump().find("DO_NOT_PERSIST"), std::string::npos);
}
TEST(Runtime, RetriesBoundedAttempts) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add(
      "flaky", std::make_shared<Function>([](ExecutionContext &c, const Json &j) -> Task<Json> {
        if (c.attempt < 3)
          throw Error(ErrorCode::Execution, "Retry");
        co_return j;
      }));
  auto r = execute(
      s, io,
      single("type: function\n    function: flaky\n    max_attempts: 3\n    retry_delay_ms: 1"));
  EXPECT_EQ(r.state, RunState::Completed);
  EXPECT_EQ(s.list(RecordKind::Attempt, r.id).size(), 5U);
}
TEST(Runtime, RetryExhaustionFails) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add("fail",
                    std::make_shared<Function>([](ExecutionContext &, const Json &) -> Task<Json> {
                      throw Error(ErrorCode::Execution, "Failure");
                      co_return nullptr;
                    }));
  auto r = execute(s, io, single("type: function\n    function: fail\n    max_attempts: 2"));
  EXPECT_EQ(r.state, RunState::Failed);
  EXPECT_EQ(s.list(RecordKind::Attempt, r.id).size(), 3U);
}
TEST(Runtime, CooperativeNodeTimeout) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add(
      "delay", std::make_shared<Function>([](ExecutionContext &c, const Json &j) -> Task<Json> {
        co_await c.delay(Milliseconds{50});
        co_return j;
      }));
  auto r = execute(s, io, single("type: function\n    function: delay\n    timeout_ms: 2"));
  EXPECT_EQ(r.state, RunState::TimedOut);
  EXPECT_EQ(s.list(RecordKind::Attempt, r.id).back().at("state"), "TimedOut");
}
TEST(Runtime, CooperativeCancellation) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add(
      "delay", std::make_shared<Function>([](ExecutionContext &c, const Json &j) -> Task<Json> {
        co_await c.delay(Milliseconds{100});
        co_return j;
      }));
  s.register_pipeline(single("type: function\n    function: delay"));
  auto id = s.start("test");
  asio::steady_timer timer(io, Milliseconds{5});
  timer.async_wait([&](const boost::system::error_code &) { s.runtime().cancel(id); });
  io.run();
  EXPECT_EQ(s.get(RecordKind::Run, id).get<laso::Run>().state, RunState::Cancelled);
}
TEST(Runtime, CancellingParentCancelsActiveChild) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add(
      "delay", std::make_shared<Function>([](ExecutionContext &c, const Json &j) -> Task<Json> {
        co_await c.delay(Milliseconds{100});
        co_return j;
      }));
  const auto child = R"(laso: "1"
name: cancel-child
version: 1
nodes:
  action: {type: function, function: delay}
edges:
  - {from: input, to: action}
  - {from: action, to: output}
)";
  const auto parent = R"(laso: "1"
name: cancel-parent
version: 1
nodes:
  child: {type: subpipeline, pipeline: cancel-child}
edges:
  - {from: input, to: child}
  - {from: child, to: output}
)";
  s.register_pipeline(child);
  s.register_pipeline(parent);
  const auto id = s.start("cancel-parent");
  asio::steady_timer timer(io, Milliseconds{5});
  timer.async_wait([&](const boost::system::error_code &) { s.runtime().cancel(id); });
  io.run();
  const auto parent_run = s.get(RecordKind::Run, id).get<laso::Run>();
  EXPECT_EQ(parent_run.state, RunState::Cancelled);
  ASSERT_FALSE(parent_run.child_id.empty());
  EXPECT_EQ(s.get(RecordKind::Run, parent_run.child_id).get<laso::Run>().state,
            RunState::Cancelled);
}
TEST(Runtime, LoopBoundsAndStepLimit) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto r = execute(s, io, fixture("bounded-loop"));
  EXPECT_EQ(r.state, RunState::Completed);
  EXPECT_EQ(r.node_visits.at("action"), 2U);
  auto failed = execute(s, io, single("type: function\n    function: identity", "max_steps: 1\n"));
  EXPECT_EQ(failed.state, RunState::Failed);
}
TEST(Runtime, EdgeBudgetCannotLoopForever) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto yaml = fixture("bounded-loop");
  auto pos = yaml.find("type: loop, max_iterations: 2");
  yaml.replace(pos, std::string("type: loop, max_iterations: 2").size(),
               "type: loop, max_iterations: 3");
  auto r = execute(s, io, yaml);
  EXPECT_EQ(r.state, RunState::Failed);
  EXPECT_LE(r.steps, 8U);
}
TEST(Runtime, PolicyDeniesBeforeToolInvocation) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.rules = {{"echo", PolicyDecision::Deny}};
  Service s(io, c);
  auto r = execute(s, io, single("type: tool\n    tool: echo"));
  EXPECT_EQ(r.state, RunState::Failed);
  EXPECT_EQ(r.node_visits.count("action"), 0U);
}
TEST(Runtime, PolicyApprovalGatesTool) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.rules = {{"echo", PolicyDecision::RequireApproval}};
  Service s(io, c);
  auto r = execute(s, io, single("type: tool\n    tool: echo"));
  ASSERT_EQ(r.state, RunState::WaitingApproval);
  auto a = s.list(RecordKind::Approval, r.id).front().get<Approval>();
  s.runtime().decide(a.id, true, "tester", "");
  io.restart();
  io.run();
  EXPECT_EQ(s.get(RecordKind::Run, r.id).get<laso::Run>().state, RunState::Completed);
}
TEST(Runtime, ApprovalSurvivesServiceRestart) {
  TemporaryDirectory dir;
  std::string id, approval;
  {
    asio::io_context io;
    Service s(io, config(dir.path));
    auto r = execute(s, io, fixture("human-approval"));
    ASSERT_EQ(r.state, RunState::WaitingApproval);
    id = r.id;
    approval = s.list(RecordKind::Approval, id).front().at("id").get<std::string>();
  }
  {
    asio::io_context io;
    Service s(io, config(dir.path));
    EXPECT_EQ(s.get(RecordKind::Run, id).get<laso::Run>().state, RunState::WaitingApproval);
    s.runtime().decide(approval, true, "tester", "Reviewed");
    io.run();
    auto r = s.get(RecordKind::Run, id).get<laso::Run>();
    EXPECT_EQ(r.state, RunState::Completed);
    EXPECT_EQ(r.node_visits.at("prepare"), 1U);
    EXPECT_EQ(s.get(RecordKind::Approval, approval).at("actor"), "tester");
  }
}
TEST(Runtime, ApprovalRejectAndDuplicateDecision) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto r = execute(s, io, fixture("human-approval"));
  auto a = s.list(RecordKind::Approval, r.id).front().get<Approval>();
  s.runtime().decide(a.id, false, "tester", "");
  EXPECT_EQ(s.get(RecordKind::Run, r.id).get<laso::Run>().state, RunState::Failed);
  EXPECT_THROW(s.runtime().decide(a.id, true, "tester", ""), Error);
}
TEST(Runtime, CancelPendingApproval) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto r = execute(s, io, fixture("human-approval"));
  s.runtime().cancel(r.id);
  EXPECT_EQ(s.get(RecordKind::Run, r.id).get<laso::Run>().state, RunState::Cancelled);
}
TEST(Runtime, ParallelJoinsInBranchOrder) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto r = execute(s, io, fixture("parallel-join"), {{"example", 1}});
  ASSERT_EQ(r.state, RunState::Completed);
  ASSERT_EQ(r.message.payload.size(), 2U);
  EXPECT_TRUE(r.message.payload.at(0).contains("greeting"));
  EXPECT_EQ(r.message.payload.at(1).at("example"), 1);
}
TEST(Runtime, ParallelBranchesOverlapBeforeRelease) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.max_nodes = 8;
  c.max_nodes_per_run = 3;
  Service s(io, c);
  auto entered = std::make_shared<std::atomic<unsigned>>(0);
  auto active = std::make_shared<std::atomic<unsigned>>(0);
  auto maximum = std::make_shared<std::atomic<unsigned>>(0);
  s.functions().add("barrier", std::make_shared<Function>([entered, active, maximum](
                                                              ExecutionContext &context,
                                                              const Json &input) -> Task<Json> {
                      const auto now = active->fetch_add(1) + 1;
                      auto old = maximum->load();
                      while (now > old && !maximum->compare_exchange_weak(old, now)) {
                      }
                      entered->fetch_add(1);
                      while (entered->load() < 3)
                        co_await context.delay(Milliseconds{1});
                      active->fetch_sub(1);
                      co_return input;
                    }));
  const auto yaml = R"(laso: "1"
name: overlap
version: 1
nodes:
  fork: {type: parallel, join: join}
  branch_a: {type: function, function: barrier}
  branch_b: {type: function, function: barrier}
  branch_c: {type: function, function: barrier}
  join: {type: join}
edges:
  - {from: input, to: fork}
  - {from: fork, to: branch_a}
  - {from: fork, to: branch_b}
  - {from: fork, to: branch_c}
  - {from: branch_a, to: join}
  - {from: branch_b, to: join}
  - {from: branch_c, to: join}
  - {from: join, to: output}
)";
  auto r = execute(s, io, yaml, Json{{"value", 7}});
  EXPECT_EQ(r.state, RunState::Completed);
  EXPECT_EQ(entered->load(), 3U);
  EXPECT_EQ(maximum->load(), 3U);
}
TEST(Runtime, ParallelPerRunLimitBoundsActiveBranches) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.max_nodes = 8;
  c.max_nodes_per_run = 2;
  Service s(io, c);
  auto active = std::make_shared<std::atomic<unsigned>>(0);
  auto maximum = std::make_shared<std::atomic<unsigned>>(0);
  s.functions().add("bounded",
                    std::make_shared<Function>([active, maximum](ExecutionContext &context,
                                                                 const Json &input) -> Task<Json> {
                      const auto now = active->fetch_add(1) + 1;
                      auto old = maximum->load();
                      while (now > old && !maximum->compare_exchange_weak(old, now)) {
                      }
                      co_await context.delay(Milliseconds{5});
                      active->fetch_sub(1);
                      co_return input;
                    }));
  const auto yaml = R"(laso: "1"
name: bounded-parallel
version: 1
nodes:
  fork: {type: parallel, join: join}
  a: {type: function, function: bounded}
  b: {type: function, function: bounded}
  c: {type: function, function: bounded}
  join: {type: join}
edges:
  - {from: input, to: fork}
  - {from: fork, to: a}
  - {from: fork, to: b}
  - {from: fork, to: c}
  - {from: a, to: join}
  - {from: b, to: join}
  - {from: c, to: join}
  - {from: join, to: output}
)";
  auto r = execute(s, io, yaml);
  EXPECT_EQ(r.state, RunState::Completed);
  EXPECT_EQ(maximum->load(), 2U);
}
TEST(Runtime, ParallelBranchesRespectGlobalStepLimit) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  const auto yaml = R"(laso: "1"
name: parallel-step-limit
version: 1
max_steps: 4
nodes:
  fork: {type: parallel, join: join}
  a: {type: function, function: identity}
  b: {type: function, function: identity}
  c: {type: function, function: identity}
  join: {type: join}
edges:
  - {from: input, to: fork}
  - {from: fork, to: a}
  - {from: fork, to: b}
  - {from: fork, to: c}
  - {from: a, to: join}
  - {from: b, to: join}
  - {from: c, to: join}
  - {from: join, to: output}
)";
  auto r = execute(s, io, yaml);
  EXPECT_EQ(r.state, RunState::Failed);
  EXPECT_LE(r.steps, 4U);
}
TEST(Runtime, ParallelBranchesEnforceEdgeBudgets) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  const auto yaml = R"(laso: "1"
name: parallel-edge-limit
version: 1
max_steps: 20
nodes:
  fork: {type: parallel, join: join}
  repeat: {type: loop, max_iterations: 3}
  action: {type: function, function: identity}
  safe: {type: function, function: identity}
  join: {type: join}
edges:
  - {from: input, to: fork}
  - {from: fork, to: repeat}
  - {from: fork, to: safe}
  - {from: repeat, condition: repeat, to: action, max_iterations: 1}
  - {from: repeat, condition: done, to: join}
  - {from: action, to: repeat}
  - {from: safe, to: join}
  - {from: join, to: output}
)";
  auto r = execute(s, io, yaml);
  EXPECT_EQ(r.state, RunState::Failed);
}
TEST(Runtime, ParallelBranchesEnforcePolicyBeforeInvocation) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.rules = {{"echo", PolicyDecision::Deny}};
  Service s(io, c);
  const auto yaml = R"(laso: "1"
name: parallel-policy
version: 1
nodes:
  fork: {type: parallel, join: join}
  blocked: {type: tool, tool: echo}
  safe: {type: function, function: identity}
  join: {type: join}
edges:
  - {from: input, to: fork}
  - {from: fork, to: blocked}
  - {from: fork, to: safe}
  - {from: blocked, to: join}
  - {from: safe, to: join}
  - {from: join, to: output}
)";
  auto r = execute(s, io, yaml);
  EXPECT_EQ(r.state, RunState::Failed);
}
TEST(Runtime, SuccessfulAttemptsHaveCompletionTimestamps) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto r = execute(s, io, fixture("hello-pipeline"));
  ASSERT_EQ(r.state, RunState::Completed);
  for (const auto &record : s.list(RecordKind::Attempt, r.id))
    EXPECT_FALSE(record.at("finished_at").get<std::string>().empty());
}
TEST(Runtime, ParallelCancellationStopsActiveBranches) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add("slow", std::make_shared<Function>(
                                [](ExecutionContext &context, const Json &input) -> Task<Json> {
                                  co_await context.delay(Milliseconds{100});
                                  co_return input;
                                }));
  const auto yaml = R"(laso: "1"
name: cancel-parallel
version: 1
nodes:
  fork: {type: parallel, join: join}
  a: {type: function, function: slow}
  b: {type: function, function: slow}
  c: {type: function, function: slow}
  join: {type: join}
edges:
  - {from: input, to: fork}
  - {from: fork, to: a}
  - {from: fork, to: b}
  - {from: fork, to: c}
  - {from: a, to: join}
  - {from: b, to: join}
  - {from: c, to: join}
  - {from: join, to: output}
)";
  auto name = s.register_pipeline(yaml).at("name").get<std::string>();
  auto id = s.start(name);
  asio::steady_timer timer(io, Milliseconds{5});
  timer.async_wait([&](const boost::system::error_code &) { s.runtime().cancel(id); });
  io.run();
  EXPECT_EQ(s.get(RecordKind::Run, id).get<laso::Run>().state, RunState::Cancelled);
}
TEST(Runtime, ParallelApprovalCheckpointSurvivesRestart) {
  TemporaryDirectory dir;
  std::string id, approval;
  auto yaml = fixture("parallel-join");
  auto pos = yaml.find("first: {type: function, function: hello}");
  yaml.replace(pos, std::string("first: {type: function, function: hello}").size(),
               "first: {type: approval}");
  {
    asio::io_context io;
    Service s(io, config(dir.path));
    auto r = execute(s, io, yaml);
    ASSERT_EQ(r.state, RunState::WaitingApproval);
    id = r.id;
    approval = s.list(RecordKind::Approval, id).front().at("id").get<std::string>();
  }
  {
    asio::io_context io;
    Service s(io, config(dir.path));
    s.runtime().decide(approval, true, "tester", "");
    io.run();
    auto r = s.get(RecordKind::Run, id).get<laso::Run>();
    EXPECT_EQ(r.state, RunState::Completed);
    EXPECT_EQ(r.message.payload.size(), 2U);
  }
}
TEST(Runtime, RoutingFailureRetainsCompletedAttempt) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto yaml = fixture("bounded-loop");
  auto pos = yaml.find("type: loop, max_iterations: 2");
  yaml.replace(pos, std::string("type: loop, max_iterations: 2").size(),
               "type: loop, max_iterations: 3");
  auto r = execute(s, io, yaml);
  ASSERT_EQ(r.state, RunState::Failed);
  for (const auto &a : s.list(RecordKind::Attempt, r.id))
    EXPECT_NE(a.at("state"), "Running");
}
TEST(Runtime, SubpipelineReturnsOutput) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.register_pipeline(fixture("hello-pipeline"));
  auto r = execute(s, io, fixture("subpipeline"));
  ASSERT_EQ(r.state, RunState::Completed);
  EXPECT_EQ(r.message.payload.at("greeting"), "Hello from LASO");
  EXPECT_EQ(s.list(RecordKind::Run).size(), 2U);
}
TEST(Runtime, SubpipelineApprovalResumeAfterRestart) {
  TemporaryDirectory dir;
  std::string parent, approval;
  {
    asio::io_context io;
    Service s(io, config(dir.path));
    s.register_pipeline(fixture("human-approval"));
    auto r = execute(s, io, single("type: subpipeline\n    pipeline: human-approval"));
    ASSERT_EQ(r.state, RunState::Paused);
    parent = r.id;
    approval = s.list(RecordKind::Approval).front().at("id").get<std::string>();
    const auto children = s.run_view(parent).at("children");
    ASSERT_EQ(children.size(), 1U);
    EXPECT_EQ(children.front().at("parent_node_id"), "action");
    EXPECT_EQ(children.front().at("pipeline_version"), 1U);
  }
  {
    asio::io_context io;
    Service s(io, config(dir.path));
    s.runtime().decide(approval, true, "tester", "");
    io.run();
    EXPECT_EQ(s.get(RecordKind::Run, parent).get<laso::Run>().state, RunState::Completed);
  }
}
TEST(Runtime, ParentCancellationCoversParallelSubpipelineChildren) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add("slow-child", std::make_shared<Function>(
                                      [](ExecutionContext &c, const Json &j) -> Task<Json> {
                                        co_await c.delay(Milliseconds{200});
                                        co_return j;
                                      }));
  s.register_pipeline(composed_pipeline(
      "slow-composed-child", 1, "", "  action: {type: function, function: slow-child}\n",
      "  - {from: input, to: action}\n  - {from: action, to: output}\n"));
  const auto parent = composed_pipeline(
      "cancel-composed-parent", 1, "",
      "  fork: {type: parallel, join: join}\n  left: {type: subpipeline, pipeline: "
      "slow-composed-child@1}\n  right: {type: subpipeline, pipeline: slow-composed-child@1}\n  "
      "join: {type: join}\n",
      "  - {from: input, to: fork}\n  - {from: fork, to: left}\n  - {from: fork, to: right}\n  - "
      "{from: left, to: join}\n  - {from: right, to: join}\n  - {from: join, to: output}\n");
  const auto id = s.start(s.register_pipeline(parent).at("id").get<std::string>());
  asio::steady_timer timer(io, Milliseconds{5});
  timer.async_wait([&](const boost::system::error_code &) { s.runtime().cancel(id); });
  io.run();
  EXPECT_EQ(s.get(RecordKind::Run, id).get<laso::Run>().state, RunState::Cancelled);
  const auto children = s.run_view(id).at("children");
  ASSERT_EQ(children.size(), 2U);
  for (const auto &child : children)
    EXPECT_EQ(child.at("state"), "Cancelled");
}
TEST(Runtime, VersionedRegistryKeepsRevisionsImmutable) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  const auto v1 = s.register_pipeline(identity_pipeline("versioned", 1));
  const auto v2 = s.register_pipeline(identity_pipeline("versioned", 2));
  EXPECT_EQ(v1.at("id"), "versioned@1");
  EXPECT_EQ(v2.at("id"), "versioned@2");
  EXPECT_EQ(s.list(RecordKind::Pipeline).size(), 2U);
  EXPECT_NO_THROW(s.register_pipeline(identity_pipeline("versioned", 1)));
  auto conflicting = identity_pipeline("versioned", 1);
  conflicting.replace(conflicting.find("function: identity"), 18, "function: hello");
  EXPECT_THROW(s.register_pipeline(conflicting), Error);
  const auto id = s.start("versioned@1", Json{{"value", 7}});
  io.run();
  const auto run = s.get(RecordKind::Run, id).get<laso::Run>();
  EXPECT_EQ(run.state, RunState::Completed);
  EXPECT_EQ(run.pipeline_version, 1U);
}
TEST(Runtime, SubpipelineUsesConcreteRevisionAndPreservesProvenance) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.register_pipeline(identity_pipeline("child", 1));
  s.register_pipeline(identity_pipeline("child", 2));
  const auto parent = composed_pipeline(
      "parent", 1, "", "  invoke:\n    type: subpipeline\n    pipeline: child@1\n",
      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  const auto parent_record = s.register_pipeline(parent);
  const auto id = s.start(parent_record.at("id").get<std::string>(), Json{{"value", 42}});
  io.run();
  const auto run = s.get(RecordKind::Run, id).get<laso::Run>();
  ASSERT_EQ(run.state, RunState::Completed);
  EXPECT_EQ(run.message.payload, (Json{{"value", 42}}));
  ASSERT_EQ(run.child_runs.size(), 1U);
  const auto child = s.get(RecordKind::Run, run.child_runs.front()).get<laso::Run>();
  EXPECT_EQ(child.parent_id, run.id);
  EXPECT_EQ(child.parent_node_id, "invoke");
  EXPECT_EQ(child.pipeline_id, "child");
  EXPECT_EQ(child.pipeline_version, 1U);
  EXPECT_EQ(child.parent_message_id, run.message.provenance.front().parent_message);
  EXPECT_TRUE(std::any_of(run.message.provenance.begin(), run.message.provenance.end(),
                          [](const ProvenanceRecord &record) { return record.node == "invoke"; }));
  EXPECT_EQ(s.run_view(run.id).at("children").size(), 1U);
}
TEST(Runtime, SubpipelineSchemasApplyAcrossBoundary) {
  TemporaryDirectory dir;
  auto root = dir.path / "schemas";
  std::filesystem::create_directories(root);
  std::ofstream(root / "object.json")
      << R"({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}})";
  asio::io_context io;
  auto c = config(dir.path);
  c.schema_roots = {root};
  Service s(io, c);
  s.register_pipeline(identity_pipeline("contract-child", 1,
                                        "input_schema: object.json\noutput_schema: object.json\n"));
  const auto parent =
      composed_pipeline("contract-parent", 1, "",
                        "  invoke:\n    type: subpipeline\n    pipeline: contract-child@1\n    "
                        "input_schema: object.json\n    output_schema: object.json\n",
                        "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  const auto id =
      s.start(s.register_pipeline(parent).at("id").get<std::string>(), Json{{"value", 9}});
  io.run();
  EXPECT_EQ(s.get(RecordKind::Run, id).get<laso::Run>().state, RunState::Completed);
}
TEST(Runtime, ChildPipelineOutputContractFailurePropagates) {
  TemporaryDirectory dir;
  auto root = dir.path / "schemas";
  std::filesystem::create_directories(root);
  std::ofstream(root / "integer.json") << R"({"type":"integer"})";
  asio::io_context io;
  auto c = config(dir.path);
  c.schema_roots = {root};
  Service s(io, c);
  s.register_pipeline(
      identity_pipeline("contract-output-failure", 1, "output_schema: integer.json\n"));
  const auto parent =
      composed_pipeline("contract-output-parent", 1, "",
                        "  invoke: {type: subpipeline, pipeline: contract-output-failure@1}\n",
                        "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  const auto id =
      s.start(s.register_pipeline(parent).at("id").get<std::string>(), Json{{"value", 9}});
  io.run();
  const auto run = s.get(RecordKind::Run, id).get<laso::Run>();
  EXPECT_EQ(run.state, RunState::Failed);
  ASSERT_EQ(run.child_runs.size(), 1U);
  EXPECT_EQ(s.get(RecordKind::Run, run.child_runs.front()).get<laso::Run>().state,
            RunState::Failed);
}
TEST(Runtime, SubpipelineRetryCreatesDistinctChildRuns) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.functions().add("always-fail",
                    std::make_shared<Function>([](ExecutionContext &, const Json &) -> Task<Json> {
                      throw Error(ErrorCode::Execution, "child failure");
                      co_return nullptr;
                    }));
  s.register_pipeline(composed_pipeline(
      "failing-child", 1, "", "  action:\n    type: function\n    function: always-fail\n",
      "  - {from: input, to: action}\n  - {from: action, to: output}\n"));
  const auto parent = composed_pipeline(
      "retry-parent", 1, "",
      "  invoke:\n    type: subpipeline\n    pipeline: failing-child@1\n    max_attempts: 2\n",
      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  std::string id = s.start(s.register_pipeline(parent).at("id").get<std::string>());
  io.run();
  const auto run = s.get(RecordKind::Run, id).get<laso::Run>();
  EXPECT_EQ(run.state, RunState::Failed);
  ASSERT_EQ(run.child_runs.size(), 2U);
  EXPECT_NE(run.child_runs[0], run.child_runs[1]);
  EXPECT_EQ(s.get(RecordKind::Run, run.child_runs[0]).at("state"), "Failed");
  EXPECT_EQ(s.get(RecordKind::Run, run.child_runs[1]).at("state"), "Failed");
  const auto attempts = s.list(RecordKind::Attempt, run.id);
  EXPECT_EQ(attempts.at(1).at("child_run_id"), run.child_runs[0]);
  EXPECT_EQ(attempts.at(2).at("child_run_id"), run.child_runs[1]);
}
TEST(Runtime, ParallelSubpipelinesRemainNormalChildRuns) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.register_pipeline(identity_pipeline("parallel-child", 1));
  const auto parent = composed_pipeline(
      "parallel-parent", 1, "",
      "  fork: {type: parallel, join: join}\n  left: {type: subpipeline, pipeline: "
      "parallel-child@1}\n  right: {type: subpipeline, pipeline: parallel-child@1}\n  join: {type: "
      "join}\n",
      "  - {from: input, to: fork}\n  - {from: fork, to: left}\n  - {from: fork, to: right}\n  - "
      "{from: left, to: join}\n  - {from: right, to: join}\n  - {from: join, to: output}\n");
  const auto id = s.start(s.register_pipeline(parent).at("id").get<std::string>());
  io.run();
  const auto run = s.get(RecordKind::Run, id).get<laso::Run>();
  EXPECT_EQ(run.state, RunState::Completed);
  EXPECT_EQ(s.run_view(run.id).at("children").size(), 2U);
}
TEST(Runtime, SubpipelineMissingReferenceAndDirectRecursionAreRejected) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  const auto missing = composed_pipeline(
      "missing-parent", 1, "", "  invoke: {type: subpipeline, pipeline: absent@4}\n",
      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  EXPECT_THROW(s.register_pipeline(missing), Error);
  const auto recursive = composed_pipeline(
      "recursive", 1, "", "  invoke: {type: subpipeline, pipeline: recursive@1}\n",
      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  EXPECT_THROW(s.register_pipeline(recursive), Error);
}
TEST(Runtime, IndirectSubpipelineRecursionIsRejected) {
  TemporaryDirectory dir;
  auto c = config(dir.path);
  auto raw = make_storage(c.db_path);
  raw->commit(
      {{RecordKind::Pipeline,
        "stored-b@1",
        "",
        {{"id", "stored-b@1"},
         {"name", "stored-b"},
         {"version", 1},
         {"yaml", composed_pipeline(
                      "stored-b", 1, "", "  invoke: {type: subpipeline, pipeline: stored-a@1}\n",
                      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n")},
         {"resolved_subpipelines", {{"invoke", "stored-a@1"}}}}}});
  asio::io_context io;
  Service s(io, c);
  const auto a =
      composed_pipeline("stored-a", 1, "", "  invoke: {type: subpipeline, pipeline: stored-b@1}\n",
                        "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  EXPECT_THROW(s.register_pipeline(a), Error);
}
TEST(Runtime, SubpipelineDepthLimitIsEnforcedAtRuntime) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.max_subpipeline_depth = 1;
  Service s(io, c);
  s.register_pipeline(identity_pipeline("depth-leaf", 1));
  s.register_pipeline(composed_pipeline(
      "depth-child", 1, "", "  invoke: {type: subpipeline, pipeline: depth-leaf@1}\n",
      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n"));
  const auto parent = composed_pipeline(
      "depth-parent", 1, "", "  invoke: {type: subpipeline, pipeline: depth-child@1}\n",
      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n");
  std::string id = s.start(s.register_pipeline(parent).at("id").get<std::string>());
  io.run();
  EXPECT_EQ(s.get(RecordKind::Run, id).get<laso::Run>().state, RunState::Failed);
}
TEST(Runtime, HistoricalParentKeepsResolvedPipelineVersion) {
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  s.register_pipeline(identity_pipeline("historical", 1));
  const auto parent = s.register_pipeline(composed_pipeline(
      "historical-parent", 1, "", "  invoke: {type: subpipeline, pipeline: historical@1}\n",
      "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n"));
  s.register_pipeline(identity_pipeline("historical", 2));
  const auto id = s.start(parent.at("id").get<std::string>());
  io.run();
  const auto run = s.get(RecordKind::Run, id).get<laso::Run>();
  ASSERT_EQ(run.child_runs.size(), 1U);
  EXPECT_EQ(s.get(RecordKind::Run, run.child_runs.front()).at("pipeline_version"), 1U);
}
TEST(Runtime, NestedSubpipelineChainPreservesTree) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.max_nodes = 1;
  c.max_nodes_per_run = 1;
  Service s(io, c);
  s.register_pipeline(identity_pipeline("nested-c", 1));
  s.register_pipeline(
      composed_pipeline("nested-b", 1, "", "  invoke: {type: subpipeline, pipeline: nested-c@1}\n",
                        "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n"));
  s.register_pipeline(
      composed_pipeline("nested-a", 1, "", "  invoke: {type: subpipeline, pipeline: nested-b@1}\n",
                        "  - {from: input, to: invoke}\n  - {from: invoke, to: output}\n"));

  const auto id = s.start("nested-a@1", Json{{"value", 42}});
  io.run();
  const auto root = s.get(RecordKind::Run, id).get<laso::Run>();
  ASSERT_EQ(root.state, RunState::Completed);
  ASSERT_EQ(root.child_runs.size(), 1U);
  const auto child = s.get(RecordKind::Run, root.child_runs.front()).get<laso::Run>();
  ASSERT_EQ(child.pipeline_id, "nested-b");
  ASSERT_EQ(child.child_runs.size(), 1U);
  const auto grandchild = s.get(RecordKind::Run, child.child_runs.front()).get<laso::Run>();
  EXPECT_EQ(grandchild.pipeline_id, "nested-c");
  EXPECT_EQ(grandchild.pipeline_version, 1U);
  EXPECT_EQ(root.message.payload, (Json{{"value", 42}}));
  EXPECT_EQ(s.run_view(child.id).at("children").size(), 1U);
}
TEST(Runtime, CapacityIsBounded) {
  TemporaryDirectory dir;
  asio::io_context io;
  auto c = config(dir.path);
  c.max_runs = 1;
  Service s(io, c);
  s.register_pipeline(fixture("hello-pipeline"));
  s.start("hello");
  EXPECT_THROW(s.start("hello"), Error);
  io.run();
}
TEST(Runtime, EventSubscribersReceiveCommittedEvents) {
  struct Subscriber : EventSubscriber {
    unsigned count = 0;
    void receive(const Event &) override {
      ++count;
    }
  };
  TemporaryDirectory dir;
  asio::io_context io;
  Service s(io, config(dir.path));
  auto subscriber = std::make_shared<Subscriber>();
  s.event_bus().subscribe(subscriber);
  execute(s, io, fixture("hello-pipeline"));
  EXPECT_GT(subscriber->count, 5U);
}
