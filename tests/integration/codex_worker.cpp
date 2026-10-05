#include "../support.hpp"
#include <fstream>
#include <laso/workers/process_transport.hpp>

using namespace laso;
using namespace laso::test;

namespace {
ProcessWorkerConfig codex_config(const std::filesystem::path &root,
                                 const std::string &fixture_mode = "success") {
  ProcessWorkerConfig result;
  result.executable = LASO_CODEX_WORKER;
  result.args = {"--codex",     LASO_CODEX_FIXTURE, "--allowed-root",
                 root.string(), "--timeout-ms",     "2000"};
  if (fixture_mode != "success")
    result.environment["LASO_CODEX_FIXTURE_MODE"] = fixture_mode;
  result.startup_timeout_ms = 2000;
  result.request_timeout_ms = 2000;
  return result;
}

WorkerRequest request(const std::filesystem::path &root, const std::string &key,
                      const std::string &instructions, const std::string &session = {}) {
  WorkerRequest result;
  result.job_id = key;
  result.worker_id = "codex";
  result.task_type = "coding";
  result.instructions = instructions;
  result.idempotency_key = key;
  result.run_id = "codex-run";
  result.node_id = "coding";
  result.metadata = {{"project_dir", root.string()}};
  if (!session.empty())
    result.metadata["codex_session_id"] = session;
  return result;
}
} // namespace

TEST(CodexWorker, StructuredSessionFollowupAndUsage) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("codex", codex_config(root.path));
  ASSERT_NO_THROW(transport.start());
  ASSERT_TRUE(transport.metadata().healthy);
  const auto first = transport.submit(request(root.path, "codex-first", "make the fixture change"));
  ASSERT_EQ(first.state, WorkerJobState::Completed);
  ASSERT_EQ(first.result.value("session_id", ""), "fixture-session");
  ASSERT_EQ(first.result.value("summary", ""), "FIXTURE-COMPLETE");
  ASSERT_EQ(first.usage.input_tokens, std::optional<std::uint64_t>(11));
  ASSERT_EQ(first.usage.output_tokens, std::optional<std::uint64_t>(7));
  ASSERT_EQ(first.usage.executor, "codex");
  const auto second = transport.submit(
      request(root.path, "codex-second", "continue the existing session", "fixture-session"));
  EXPECT_EQ(second.state, WorkerJobState::Completed);
  EXPECT_EQ(second.result.value("summary", ""), "FIXTURE-CONTINUED");
  transport.stop();
}

TEST(CodexWorker, DeterministicFixtureCanWriteIntoRequestedWorkspace) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("codex", codex_config(root.path, "write-workspace"));
  ASSERT_NO_THROW(transport.start());
  const auto submitted = transport.submit(
      request(root.path, "codex-workspace-artifact", "write the distributed fixture artifact"));
  ASSERT_EQ(submitted.state, WorkerJobState::Completed) << submitted.error;
  std::ifstream artifact(root.path / "remote-artifact.txt", std::ios_base::binary);
  const std::string contents((std::istreambuf_iterator<char>(artifact)),
                             std::istreambuf_iterator<char>());
  EXPECT_EQ(contents, "cross-machine-s3-artifact-v1\n");
}

TEST(CodexWorker, SessionCanBeReconciledAfterAdapterRestart) {
  TemporaryDirectory root;
  std::string session;
  {
    ProcessWorkerTransport transport("codex", codex_config(root.path));
    ASSERT_NO_THROW(transport.start());
    const auto first = transport.submit(request(root.path, "codex-restart-first", "initial turn"));
    ASSERT_EQ(first.state, WorkerJobState::Completed);
    session = first.result.value("session_id", "");
  }
  ProcessWorkerTransport restarted("codex", codex_config(root.path));
  ASSERT_NO_THROW(restarted.start());
  const auto resumed =
      restarted.submit(request(root.path, "codex-restart-second", "continue", session));
  EXPECT_EQ(resumed.state, WorkerJobState::Completed);
  EXPECT_EQ(resumed.result.value("session_id", ""), session);
  restarted.stop();
}

TEST(CodexWorker, StartsNewSessionForDifferentWorkspaceRoot) {
  TemporaryDirectory parent;
  const auto first_root = parent.path / "first";
  const auto second_root = parent.path / "second";
  std::filesystem::create_directories(first_root);
  std::filesystem::create_directories(second_root);
  ProcessWorkerTransport transport("codex", codex_config(parent.path));
  ASSERT_NO_THROW(transport.start());
  const auto first = transport.submit(request(first_root, "codex-first-root", "first"));
  ASSERT_EQ(first.state, WorkerJobState::Completed);
  const auto second = transport.submit(request(second_root, "codex-second-root", "second"));
  EXPECT_EQ(second.state, WorkerJobState::Completed);
  EXPECT_EQ(first.result.value("project_dir", ""), first_root.string());
  EXPECT_EQ(second.result.value("project_dir", ""), second_root.string());
  transport.stop();
}

TEST(CodexWorker, QuietProviderIntervalUsesOverallDeadline) {
  TemporaryDirectory root;
  auto worker_config = codex_config(root.path, "quiet-over-one-minute");
  worker_config.args = {"--codex",          LASO_CODEX_FIXTURE, "--allowed-root",
                        root.path.string(), "--timeout-ms",     "65000"};
  worker_config.startup_timeout_ms = 2000;
  worker_config.request_timeout_ms = 65000;
  ProcessWorkerTransport transport("codex", worker_config);
  ASSERT_NO_THROW(transport.start());
  const auto result = transport.submit(request(root.path, "codex-quiet-provider", "continue"));
  EXPECT_EQ(result.state, WorkerJobState::Completed) << result.error;
  transport.stop();
}

TEST(CodexWorker, ProjectRootIsEnforced) {
  TemporaryDirectory root;
  TemporaryDirectory outside;
  ProcessWorkerTransport transport("codex", codex_config(root.path));
  ASSERT_NO_THROW(transport.start());
  const auto result = transport.submit(request(outside.path, "codex-outside", "do nothing"));
  EXPECT_EQ(result.state, WorkerJobState::Failed);
  EXPECT_NE(result.error.find("outside an allowed root"), std::string::npos);
}

TEST(CodexWorker, PermissionRequestUsesGenericWorkerChannel) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("codex", codex_config(root.path));
  unsigned requests = 0;
  transport.set_interaction_handler([&](const WorkerInteractionRequest &interaction) {
    ++requests;
    EXPECT_EQ(interaction.type, WorkerInteractionType::Permission);
    EXPECT_EQ(interaction.worker_id, "codex");
    return WorkerInteractionResponse{interaction.request_id, WorkerInteractionState::Approved,
                                     Json{{"scope", "once"}}, "approved by test"};
  });
  ASSERT_NO_THROW(transport.start());
  const auto result =
      transport.submit(request(root.path, "codex-permission", "request-permission"));
  EXPECT_EQ(result.state, WorkerJobState::Completed);
  EXPECT_EQ(requests, 1U);
}

TEST(CodexWorker, PermissionDenialRemainsAWorkerJobFailure) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("codex", codex_config(root.path));
  transport.set_interaction_handler([](const WorkerInteractionRequest &interaction) {
    EXPECT_EQ(interaction.type, WorkerInteractionType::Permission);
    return WorkerInteractionResponse{interaction.request_id, WorkerInteractionState::Denied,
                                     Json::object(), "denied by test"};
  });
  ASSERT_NO_THROW(transport.start());
  const auto result =
      transport.submit(request(root.path, "codex-permission-denied", "request-permission"));
  EXPECT_EQ(result.state, WorkerJobState::Failed);
  EXPECT_NE(result.error.find("permission denied"), std::string::npos);
}

TEST(CodexWorker, QuestionUsesGenericWorkerChannel) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("codex", codex_config(root.path));
  transport.set_interaction_handler([](const WorkerInteractionRequest &interaction) {
    EXPECT_EQ(interaction.type, WorkerInteractionType::Question);
    return WorkerInteractionResponse{interaction.request_id, WorkerInteractionState::Answered,
                                     Json{{"answers", Json{{"fixture-question", "yes"}}}},
                                     "answered by test"};
  });
  ASSERT_NO_THROW(transport.start());
  const auto result = transport.submit(request(root.path, "codex-question", "request-question"));
  EXPECT_EQ(result.state, WorkerJobState::Completed);
}

TEST(CodexWorker, MalformedAppServerMessagesAreTransportFailures) {
  TemporaryDirectory root;
  ProcessWorkerTransport transport("codex", codex_config(root.path, "malformed"));
  ASSERT_NO_THROW(transport.start());
  EXPECT_THROW(transport.submit(request(root.path, "codex-malformed", "run")),
               WorkerTransportError);
  EXPECT_FALSE(transport.metadata().healthy);
}

TEST(CodexWorker, RealInstalledCodexFixtureIsOptIn) {
  if (!std::getenv("LASO_RUN_REAL_CODEX"))
    GTEST_SKIP() << "Set LASO_RUN_REAL_CODEX=1 to run the configured Codex integration";
  TemporaryDirectory root;
  std::ofstream(root.path / "fixture.txt") << "before\n";
  auto worker_config = codex_config(root.path);
  worker_config.executable = LASO_CODEX_WORKER;
  worker_config.args = {
      "--codex",        std::getenv("CODEX_BIN") ? std::getenv("CODEX_BIN") : "codex",
      "--allowed-root", root.path.string(),
      "--timeout-ms",   "120000"};
  worker_config.startup_timeout_ms = 120000;
  worker_config.request_timeout_ms = 120000;
  worker_config.interaction_timeout_ms = 300000;
  worker_config.environment_allowlist = {"HOME", "CODEX_HOME", "PATH"};
  ProcessWorkerTransport transport("codex", worker_config);
  transport.set_interaction_handler([](const WorkerInteractionRequest &request) {
    return WorkerInteractionResponse{request.request_id, WorkerInteractionState::Approved,
                                     Json{{"scope", "once"}}, "approved by integration test"};
  });
  ASSERT_NO_THROW(transport.start());
  const auto first = transport.submit(request(root.path, "codex-real-first",
                                              "Change fixture.txt so it contains exactly "
                                              "CODEX-ADAPTER-OK and do not modify any other file. "
                                              "Reply with CODEX-DONE."));
  ASSERT_EQ(first.state, WorkerJobState::Completed) << first.error;
  const auto session = first.result.value("session_id", std::string{});
  ASSERT_FALSE(session.empty());
  std::ifstream changed(root.path / "fixture.txt");
  std::string contents;
  std::getline(changed, contents);
  EXPECT_EQ(contents, "CODEX-ADAPTER-OK");
  transport.stop();

  ProcessWorkerTransport restarted("codex", worker_config);
  restarted.set_interaction_handler([](const WorkerInteractionRequest &request) {
    return WorkerInteractionResponse{request.request_id, WorkerInteractionState::Approved,
                                     Json{{"scope", "once"}}, "approved by integration test"};
  });
  ASSERT_NO_THROW(restarted.start());
  const auto followup =
      restarted.submit(request(root.path, "codex-real-followup",
                               "Reply with CODEX-RECOVERED and do not edit files.", session));
  EXPECT_EQ(followup.state, WorkerJobState::Completed) << followup.error;
  EXPECT_EQ(followup.result.value("session_id", std::string{}), session);
  restarted.stop();
}
