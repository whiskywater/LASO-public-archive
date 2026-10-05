// Deterministic app-server-shaped fixture for the optional Codex adapter tests.
#include <array>
#include <chrono>
#include <cstdint>
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

void repair_synthetic_project(const std::filesystem::path &root) {
  const auto math_path = root / "src/math.cpp";
  const auto label_path = root / "src/label.cpp";
  if (!std::filesystem::is_regular_file(math_path) || !std::filesystem::is_regular_file(label_path))
    return;
  std::ofstream math(math_path, std::ios_base::trunc);
  math << R"( #include "laso_phase1/math.hpp"

#include <stdexcept>

namespace laso_phase1 {
int saturating_sum(int left, int right, int lower, int upper) {
  if (lower > upper)
    throw std::invalid_argument("lower bound exceeds upper bound");
  const auto sum = static_cast<long long>(left) + static_cast<long long>(right);
  if (sum < lower)
    return lower;
  if (sum > upper)
    return upper;
  return static_cast<int>(sum);
}
} // namespace laso_phase1
)";
  std::ofstream label(label_path, std::ios_base::trunc);
  label << R"( #include "laso_phase1/label.hpp"

namespace laso_phase1 {
std::string slugify(std::string_view input) {
  std::string result;
  bool pending_separator = false;
  for (const unsigned char character : input) {
    const bool ascii_alphanumeric = (character >= 'a' && character <= 'z') ||
                                    (character >= 'A' && character <= 'Z') ||
                                    (character >= '0' && character <= '9');
    if (!ascii_alphanumeric) {
      pending_separator = !result.empty();
      continue;
    }
    if (pending_separator)
      result.push_back('-');
    pending_separator = false;
    result.push_back(character >= 'A' && character <= 'Z'
                         ? static_cast<char>(character - 'A' + 'a')
                         : static_cast<char>(character));
  }
  return result;
}
} // namespace laso_phase1
)";
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
      if (mode == "write-workspace" || mode == "write-workspace-then-quiet" ||
          mode == "write-large-workspace") {
        const auto cwd = params.value("cwd", std::string{});
        const auto *variant_value = std::getenv("LASO_CODEX_FIXTURE_ARTIFACT_CONTENT");
        const std::string variant = variant_value ? variant_value : "";
        unsigned variant_seed = 0;
        for (const auto byte : variant)
          variant_seed = (variant_seed * 33U + static_cast<unsigned char>(byte)) & 0xffU;
        std::ofstream artifact(std::filesystem::path(cwd) / "remote-artifact.txt",
                               std::ios_base::binary | std::ios_base::trunc);
        if (!artifact)
          return 74;
        if (mode == "write-large-workspace") {
          std::array<char, 64 * 1024> block{};
          for (std::size_t block_index = 0; block_index < 128; ++block_index) {
            for (std::size_t i = 0; i < block.size(); ++i)
              block[i] = static_cast<char>((block_index * 31 + i * 17 + 9 + variant_seed) & 0xff);
            artifact.write(block.data(), static_cast<std::streamsize>(block.size()));
          }
        } else if (!(artifact << (variant.empty() ? "cross-machine-s3-artifact-v1" : variant)
                              << '\n')) {
          return 74;
        }
        artifact.flush();
        if (!artifact.good())
          return 74;
        repair_synthetic_project(cwd);
        if (const auto *marker = std::getenv("LASO_CODEX_FIXTURE_MARKER");
            marker && *marker != '\0') {
          std::ofstream ready(marker, std::ios_base::trunc);
          if (!(ready << "ready\n"))
            return 74;
        }
      }
      if (mode == "quiet-over-one-minute" || mode == "write-workspace-then-quiet") {
        std::uint64_t sleep_ms = 60050;
        if (const auto *configured = std::getenv("LASO_CODEX_FIXTURE_SLEEP_MS");
            configured && *configured != '\0') {
          char *end = nullptr;
          const auto parsed = std::strtoull(configured, &end, 10);
          if (!end || *end != '\0' || parsed > 300000)
            return 74;
          sleep_ms = parsed;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
      }
      if (const auto *marker = std::getenv("LASO_CODEX_FIXTURE_DONE_MARKER");
          marker && *marker != '\0') {
        std::ofstream done(marker, std::ios_base::trunc);
        if (!(done << "done\n"))
          return 74;
      }
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
