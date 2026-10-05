#include "../support.hpp"
#include <fstream>
#include <iostream>
#include <laso/cli/cli.hpp>
#include <sstream>

using namespace laso;
using namespace laso::test;

namespace {
struct CliResult {
  int status;
  std::string output;
};

CliResult invoke_cli(std::vector<std::string> arguments) {
  std::vector<char *> argv;
  argv.reserve(arguments.size());
  for (auto &argument : arguments)
    argv.push_back(argument.data());
  std::ostringstream output;
  auto *previous = std::cout.rdbuf(output.rdbuf());
  try {
    const auto status = cli_main(static_cast<int>(argv.size()), argv.data());
    std::cout.rdbuf(previous);
    return {status, output.str()};
  } catch (...) {
    std::cout.rdbuf(previous);
    throw;
  }
}
} // namespace

TEST(Cli, ScheduleAndTriggerDeleteReturnSuccess) {
  TemporaryDirectory directory;
  const auto pipeline = directory.path / "pipeline.yaml";
  const auto schedule = directory.path / "schedule.json";
  const auto trigger = directory.path / "trigger.json";
  std::ofstream(pipeline) << single();
  std::ofstream(schedule) << R"({
    "id": "cli-schedule",
    "name": "cli-schedule",
    "pipeline": "test@1",
    "type": "one_time",
    "at": "2099-01-01T00:00:00.000Z"
  })";
  std::ofstream(trigger) << R"({
    "id": "cli-trigger",
    "name": "cli-trigger",
    "pipeline": "test@1",
    "event": "cli.test"
  })";

  const auto data_dir = directory.path.string();
  ASSERT_EQ(invoke_cli({"laso", "--data-dir", data_dir, "pipeline", "register", pipeline.string()})
                .status,
            0);
  ASSERT_EQ(
      invoke_cli({"laso", "--data-dir", data_dir, "schedule", "create", schedule.string()}).status,
      0);
  const auto deleted_schedule =
      invoke_cli({"laso", "--data-dir", data_dir, "schedule", "delete", "cli-schedule"});
  ASSERT_EQ(deleted_schedule.status, 0);
  const Json expected_schedule{{"id", "cli-schedule"}, {"deleted", true}};
  EXPECT_EQ(Json::parse(deleted_schedule.output), expected_schedule);

  ASSERT_EQ(
      invoke_cli({"laso", "--data-dir", data_dir, "trigger", "create", trigger.string()}).status,
      0);
  const auto deleted_trigger =
      invoke_cli({"laso", "--data-dir", data_dir, "trigger", "delete", "cli-trigger"});
  ASSERT_EQ(deleted_trigger.status, 0);
  const Json expected_trigger{{"id", "cli-trigger"}, {"deleted", true}};
  EXPECT_EQ(Json::parse(deleted_trigger.output), expected_trigger);
}

TEST(Cli, OperatorInspectShowsDurableLineageWithoutPayloads) {
  TemporaryDirectory directory;
  const auto pipeline = directory.path / "pipeline.yaml";
  std::ofstream(pipeline) << single();
  const auto data_dir = directory.path.string();

  ASSERT_EQ(invoke_cli({"laso", "--data-dir", data_dir, "pipeline", "register", pipeline.string()})
                .status,
            0);
  const auto started = invoke_cli({"laso", "--data-dir", data_dir, "run", "start", "test",
                                   "--input", R"({"operator_probe":"safe"})"});
  ASSERT_EQ(started.status, 0);
  const auto run = Json::parse(started.output);
  ASSERT_TRUE(run.contains("id"));

  const auto inspected = invoke_cli(
      {"laso", "--data-dir", data_dir, "run", "inspect", run.at("id").get<std::string>()});
  ASSERT_EQ(inspected.status, 0);
  const auto view = Json::parse(inspected.output);
  EXPECT_EQ(view.at("id"), run.at("id"));
  EXPECT_TRUE(view.contains("attempts"));
  EXPECT_TRUE(view.contains("node_work"));
  EXPECT_TRUE(view.contains("worker_jobs"));
  EXPECT_TRUE(view.contains("artifacts"));
  EXPECT_EQ(inspected.output.find("operator_probe"), std::string::npos);

  const auto listed = invoke_cli({"laso", "--data-dir", data_dir, "run", "list"});
  ASSERT_EQ(listed.status, 0);
  const auto runs = Json::parse(listed.output);
  ASSERT_EQ(runs.size(), 1U);
  EXPECT_EQ(runs.front().at("id"), run.at("id"));
  EXPECT_EQ(runs.front().at("state"), "Completed");
}

TEST(Cli, ArtifactVerifyReturnsFailureForInvalidObject) {
  TemporaryDirectory directory;
  const auto invalid_object = directory.path / "artifacts" / "objects" / "invalid-object";
  std::filesystem::create_directories(invalid_object.parent_path());
  std::ofstream(invalid_object, std::ios::binary) << "synthetic-corrupt-object";

  const auto verified =
      invoke_cli({"laso", "--data-dir", directory.path.string(), "artifact", "verify"});

  EXPECT_EQ(verified.status, 2);
  const auto report = Json::parse(verified.output);
  EXPECT_EQ(report.at("invalid"), 1U);
  EXPECT_EQ(report.at("verified"), 0U);
}
