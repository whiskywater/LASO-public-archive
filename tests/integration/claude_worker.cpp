#include "../support.hpp"
#include <cstdlib>
#include <laso/workers/process_transport.hpp>

using namespace laso;
using namespace laso::test;

namespace {
ProcessWorkerConfig config(const std::filesystem::path &root, const std::string &mode,
                           std::uint64_t timeout = 1000) {
  ProcessWorkerConfig result;
  result.executable = LASO_CLAUDE_WORKER;
  result.args = {
      "--claude", LASO_CLAUDE_FIXTURE, "--claude-arg", "--mode",       "--claude-arg",
      mode,       "--allowed-root",    root.string(),  "--timeout-ms", std::to_string(timeout)};
  result.startup_timeout_ms = timeout;
  result.request_timeout_ms = timeout;
  result.interaction_timeout_ms = timeout;
  return result;
}

WorkerRequest request(const std::filesystem::path &root, const std::string &id,
                      const std::string &session = {}) {
  WorkerRequest result;
  result.job_id = id;
  result.worker_id = "claude";
  result.task_type = "coding";
  result.instructions = "perform the bounded fixture task";
  result.idempotency_key = id;
  result.metadata = {{"project_dir", root.string()}};
  if (!session.empty())
    result.metadata["claude_session_id"] = session;
  return result;
}
} // namespace

TEST(ClaudeWorker, StructuredSessionFollowupAndUsage) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("claude", config(root.path, "success"));
  ASSERT_NO_THROW(transport.start());
  const auto first = transport.submit(request(root.path, "claude-1"));
  ASSERT_EQ(first.state, WorkerJobState::Completed) << first.error;
  EXPECT_EQ(first.result.at("session_id"), "fixture-claude-session");
  EXPECT_EQ(first.usage.total_tokens, std::optional<std::uint64_t>(10));
  EXPECT_EQ(first.usage.tool_calls, std::optional<std::uint64_t>(1));
  const auto second = transport.submit(request(root.path, "claude-2", first.external_job_id));
  EXPECT_EQ(second.state, WorkerJobState::Completed) << second.error;
  EXPECT_EQ(second.external_job_id, first.external_job_id);
}

TEST(ClaudeWorker, SessionCanBeResumedAfterAdapterRestart) {
  TemporaryDirectory root;
  std::string session;
  {
    ProcessWorkerTransport transport("claude", config(root.path, "success"));
    transport.start();
    session = transport.submit(request(root.path, "claude-restart-1")).external_job_id;
    transport.stop();
  }
  ProcessWorkerTransport restarted("claude", config(root.path, "success"));
  restarted.start();
  const auto result = restarted.submit(request(root.path, "claude-restart-2", session));
  EXPECT_EQ(result.state, WorkerJobState::Completed) << result.error;
  EXPECT_EQ(result.external_job_id, session);
}

TEST(ClaudeWorker, ProjectRootAndSymlinkBoundariesAreEnforced) {
  TemporaryDirectory root;
  TemporaryDirectory outside;
  std::error_code symlink_error;
  std::filesystem::create_directory_symlink(outside.path, root.path / "escape", symlink_error);
  ProcessWorkerTransport transport("claude", config(root.path, "success"));
  transport.start();
  const auto result = transport.submit(request(outside.path, "claude-outside"));
  EXPECT_EQ(result.state, WorkerJobState::Failed);
  EXPECT_NE(result.error.find("outside an allowed root"), std::string::npos);
  if (!symlink_error) {
    const auto symlink_result = transport.submit(request(root.path / "escape", "claude-symlink"));
    EXPECT_EQ(symlink_result.state, WorkerJobState::Failed);
    EXPECT_NE(symlink_result.error.find("outside an allowed root"), std::string::npos);
  }
}

TEST(ClaudeWorker, PermissionRequestUsesGenericWorkerChannel) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("claude", config(root.path, "permission"));
  unsigned requests = 0;
  transport.set_interaction_handler([&](const WorkerInteractionRequest &interaction) {
    ++requests;
    EXPECT_EQ(interaction.type, WorkerInteractionType::Permission);
    return WorkerInteractionResponse{interaction.request_id, WorkerInteractionState::Approved,
                                     Json{{"scope", "once"}}, "approved by test policy"};
  });
  transport.start();
  const auto result = transport.submit(request(root.path, "claude-permission"));
  EXPECT_EQ(result.state, WorkerJobState::Completed) << result.error;
  EXPECT_EQ(requests, 1U);
}

TEST(ClaudeWorker, PermissionDenialRemainsAWorkerFailure) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("claude", config(root.path, "permission"));
  transport.set_interaction_handler([](const WorkerInteractionRequest &interaction) {
    return WorkerInteractionResponse{interaction.request_id, WorkerInteractionState::Denied,
                                     Json::object(), "denied by test policy"};
  });
  transport.start();
  const auto result = transport.submit(request(root.path, "claude-denied"));
  EXPECT_EQ(result.state, WorkerJobState::Failed);
  EXPECT_NE(result.error.find("permission denied"), std::string::npos);
}

TEST(ClaudeWorker, CancellationIsNotReportedWithoutClaudeAcknowledgement) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("claude", config(root.path, "success"));
  transport.start();
  const auto result = transport.submit(request(root.path, "claude-cancel"));
  EXPECT_FALSE(transport.cancel(result.external_job_id));
}

TEST(ClaudeWorker, QuestionRequestUsesGenericWorkerChannel) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("claude", config(root.path, "question"));
  transport.set_interaction_handler([](const WorkerInteractionRequest &interaction) {
    EXPECT_EQ(interaction.type, WorkerInteractionType::Question);
    return WorkerInteractionResponse{interaction.request_id, WorkerInteractionState::Answered,
                                     Json{{"answer", "yes"}}, "answered by test"};
  });
  transport.start();
  const auto result = transport.submit(request(root.path, "claude-question"));
  EXPECT_EQ(result.state, WorkerJobState::Completed) << result.error;
}

TEST(ClaudeWorker, MalformedAndHungClaudeProcessesAreTransportFailures) {
  TemporaryDirectory root;
  for (const auto &mode : {std::string("malformed"), std::string("crash"), std::string("hang")}) {
    ProcessWorkerTransport transport("claude", config(root.path, mode, 150));
    ASSERT_NO_THROW(transport.start()) << mode;
    EXPECT_THROW(transport.submit(request(root.path, "claude-failure")), WorkerTransportError)
        << mode;
  }
}

TEST(ClaudeWorker, RealInstalledClaudeFixtureIsOptIn) {
  if (!std::getenv("LASO_RUN_REAL_CLAUDE"))
    GTEST_SKIP() << "Set LASO_RUN_REAL_CLAUDE=1 with CLAUDE_BIN to run the configured Claude test";
  const auto binary = std::getenv("CLAUDE_BIN");
  if (!binary || std::string(binary).empty())
    GTEST_SKIP() << "CLAUDE_BIN is required for the real Claude test";
  TemporaryDirectory root;
  ProcessWorkerConfig worker = config(root.path, "success", 120000);
  worker.args[1] = binary;
  ProcessWorkerTransport transport("claude", std::move(worker));
  ASSERT_NO_THROW(transport.start());
  const auto result = transport.submit(request(root.path, "claude-real"));
  EXPECT_EQ(result.state, WorkerJobState::Completed) << result.error;
}
