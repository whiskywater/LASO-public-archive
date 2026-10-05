// Deterministic app-server-shaped fixture for the optional Codex adapter tests.
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <laso/core/types.hpp>
#include <thread>

using namespace laso;

namespace {
void send(const Json &value) {
  std::cout << value.dump() << '\n' << std::flush;
}

Json thread(const std::string &id, const std::string &cwd) {
  return Json{
      {"id", id}, {"cwd", cwd}, {"model", "fixture-model"}, {"modelProvider", "fixture-provider"}};
}
} // namespace

int main(int argc, char **argv) {
  const std::string mode =
      argc > 1 && std::string(argv[1]) != "app-server"
          ? argv[1]
          : (std::getenv("LASO_CODEX_FIXTURE_MODE") ? std::getenv("LASO_CODEX_FIXTURE_MODE")
                                                    : "success");
  std::string line;
  while (std::getline(std::cin, line)) {
    const auto request = Json::parse(line, nullptr, false);
    if (request.is_discarded() || !request.is_object())
      return 65;
    const auto method = request.value("method", std::string{});
    const auto id = request.value("id", Json());
    if (!request.contains("id"))
      continue;
    if (method == "initialize") {
      send({{"jsonrpc", "2.0"}, {"id", id}, {"result", Json{{"server", "fixture"}}}});
      continue;
    }
    if (mode == "malformed") {
      std::cout << "not-json\n" << std::flush;
      return 0;
    }
    if (mode == "crash")
      return 73;
    if (method == "thread/start") {
      const auto cwd = request.value("params", Json::object()).value("cwd", std::string{});
      send({{"jsonrpc", "2.0"},
            {"id", id},
            {"result", Json{{"thread", thread("fixture-session", cwd)},
                            {"model", "fixture-model"},
                            {"modelProvider", "fixture-provider"}}}});
    } else if (method == "thread/resume") {
      const auto cwd = request.value("params", Json::object()).value("cwd", std::string{});
      send({{"jsonrpc", "2.0"},
            {"id", id},
            {"result", Json{{"thread", thread("fixture-session", cwd)},
                            {"model", "fixture-model"},
                            {"modelProvider", "fixture-provider"}}}});
    } else if (method == "turn/start") {
      const auto params = request.value("params", Json::object());
      const auto input = params.value("input", Json::array());
      const auto prompt = input.empty() ? std::string{} : input.front().value("text", "");
      if (mode == "write-workspace") {
        const auto cwd = params.value("cwd", std::string{});
        std::ofstream artifact(std::filesystem::path(cwd) / "remote-artifact.txt",
                               std::ios_base::binary | std::ios_base::trunc);
        if (!artifact || !(artifact << "cross-machine-s3-artifact-v1\n"))
          return 74;
      }
      if (mode == "quiet-over-one-minute")
        std::this_thread::sleep_for(std::chrono::milliseconds(60050));
      if (prompt.find("request-permission") != std::string::npos) {
        send({{"jsonrpc", "2.0"},
              {"id", 99},
              {"method", "item/commandExecution/requestApproval"},
              {"params", Json{{"command", "printf fixture"},
                              {"cwd", "/tmp/laso-codex-fixture"},
                              {"reason", "fixture permission test"}}}});
        std::string answer_line;
        if (!std::getline(std::cin, answer_line))
          return 66;
        const auto answer = Json::parse(answer_line, nullptr, false);
        if (answer.is_discarded())
          return 67;
        if (answer.value("result", Json::object()).value("decision", "") != "accept") {
          const auto turn = Json{{"id", "fixture-turn"}, {"status", "inProgress"}};
          send(Json{{"jsonrpc", "2.0"}, {"id", id}, {"result", Json{{"turn", turn}}}});
          const auto failed_turn = Json{{"id", "fixture-turn"},
                                        {"status", "failed"},
                                        {"error", Json{{"message", "permission denied"}}}};
          send(Json{{"method", "turn/completed"},
                    {"params", Json{{"threadId", "fixture-session"}, {"turn", failed_turn}}}});
          continue;
        }
      } else if (prompt.find("request-question") != std::string::npos) {
        const auto question = Json{{"id", "fixture-question"}, {"question", "Continue?"}};
        send(Json{{"jsonrpc", "2.0"},
                  {"id", 100},
                  {"method", "item/tool/requestUserInput"},
                  {"params", Json{{"questions", Json::array({question})}}}});
        std::string answer_line;
        if (!std::getline(std::cin, answer_line))
          return 68;
        const auto answer = Json::parse(answer_line, nullptr, false);
        if (answer.is_discarded() || !answer.value("result", Json::object()).is_object())
          return 69;
      }
      const auto turn = Json{{"id", "fixture-turn"}, {"status", "inProgress"}};
      send(Json{{"jsonrpc", "2.0"}, {"id", id}, {"result", Json{{"turn", turn}}}});
      const auto item =
          Json{{"type", "agentMessage"},
               {"text", prompt.find("continue") != std::string::npos ? "FIXTURE-CONTINUED"
                                                                     : "FIXTURE-COMPLETE"}};
      send(Json{{"method", "item/completed"}, {"params", Json{{"item", item}}}});
      const auto usage = Json{{"inputTokens", 11}, {"outputTokens", 7}, {"totalTokens", 18}};
      send(Json{{"method", "thread/tokenUsage/updated"},
                {"params", Json{{"tokenUsage", Json{{"last", usage}}}}}});
      const auto completed_turn =
          Json{{"id", "fixture-turn"}, {"status", "completed"}, {"durationMs", 4}};
      send(Json{{"method", "turn/completed"},
                {"params", Json{{"threadId", "fixture-session"}, {"turn", completed_turn}}}});
    } else if (method == "turn/interrupt") {
      send({{"jsonrpc", "2.0"}, {"id", id}, {"result", Json::object()}});
    } else {
      send({{"jsonrpc", "2.0"}, {"id", id}, {"result", Json::object()}});
    }
  }
  return 0;
}
