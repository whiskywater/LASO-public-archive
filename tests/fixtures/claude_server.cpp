#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <laso/core/types.hpp>
#include <string>
#include <thread>

using namespace laso;

namespace {
std::string option(int argc, char **argv, const std::string &name, std::string fallback = {}) {
  for (int i = 1; i < argc; ++i) {
    const std::string value = argv[i];
    if (value == name && i + 1 < argc)
      return argv[++i];
    if (value.rfind(name + "=", 0) == 0)
      return value.substr(name.size() + 1);
  }
  return fallback;
}

void emit(const Json &message) {
  std::cout << message.dump() << '\n' << std::flush;
}
} // namespace

int main(int argc, char **argv) {
  const auto mode = option(argc, argv, "--mode", "success");
  const auto resume = option(argc, argv, "--resume");
  std::string line;
  while (std::getline(std::cin, line)) {
    const auto input = Json::parse(line, nullptr, false);
    if (input.is_discarded() || !input.is_object())
      return 65;
    if (mode == "malformed") {
      std::cout << "not-json\n" << std::flush;
      continue;
    }
    if (mode == "crash")
      std::_Exit(73);
    if (mode == "hang")
      for (;;)
        std::this_thread::sleep_for(std::chrono::seconds(60));
    if (input.value("type", std::string{}) != "user")
      continue;
    const auto session = resume.empty() ? "fixture-claude-session" : resume;
    emit({{"type", "system"},
          {"subtype", "init"},
          {"session_id", session},
          {"cwd", std::filesystem::current_path().string()},
          {"model", "fixture-model"},
          {"permissionMode", "default"}});
    if (mode == "permission" || mode == "question") {
      const auto id = "fixture-control-1";
      const auto subtype = mode == "permission" ? "can_use_tool" : "ask_user_question";
      emit({{"type", "control_request"},
            {"request_id", id},
            {"request",
             {{"subtype", subtype},
              {"tool_name", "Bash"},
              {"input", {{"command", "printf fixture"}}},
              {"questions", Json::array({"Continue?"})}}}});
      if (!std::getline(std::cin, line))
        return 66;
      const auto answer = Json::parse(line, nullptr, false);
      if (answer.is_discarded() || answer.value("type", std::string{}) != "control_response")
        return 67;
      const auto response =
          answer.value("response", Json::object()).value("response", Json::object());
      if (mode == "permission" && response.value("behavior", std::string{}) != "allow") {
        emit({{"type", "result"},
              {"subtype", "error_during_execution"},
              {"is_error", true},
              {"session_id", session},
              {"result", "permission denied"}});
        return 0;
      }
    }
    if (mode == "failure") {
      emit({{"type", "result"},
            {"subtype", "error_during_execution"},
            {"is_error", true},
            {"session_id", session},
            {"result", "fixture failure"}});
      return 0;
    }
    emit({{"type", "assistant"},
          {"session_id", session},
          {"message",
           {{"model", "fixture-model"},
            {"content", Json::array({Json{{"type", "text"}, {"text", "CLAUDE_DONE"}},
                                     Json{{"type", "tool_use"}, {"name", "Bash"}}})},
            {"usage", {{"input_tokens", 4}, {"output_tokens", 6}}}}}});
    emit({{"type", "result"},
          {"subtype", "success"},
          {"is_error", false},
          {"duration_ms", 12},
          {"session_id", session},
          {"result", "CLAUDE_DONE"},
          {"usage", {{"input_tokens", 4}, {"output_tokens", 6}, {"total_tokens", 10}}}});
    return 0;
  }
  return 0;
}
