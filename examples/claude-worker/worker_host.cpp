// Optional Claude Code adapter.  Claude-specific process and stream handling
// stays here; LASO Core only sees the versioned worker process protocol.
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <laso/workers/process_protocol.hpp>
#include <laso/workers/worker.hpp>
#include <map>
#include <mutex>
#include <optional>
#include <poll.h>
#include <signal.h>
#include <stdexcept>
#include <string>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace laso;

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t max_output_bytes = 4 * 1024 * 1024;
constexpr std::size_t max_error_bytes = process_protocol::max_stderr_bytes;
constexpr std::size_t max_session_bytes = 512;
constexpr std::size_t max_summary_bytes = 65536;
std::mutex output_mutex;

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

std::vector<std::string> options(int argc, char **argv, const std::string &name) {
  std::vector<std::string> result;
  for (int i = 1; i < argc; ++i) {
    const std::string value = argv[i];
    if (value == name && i + 1 < argc)
      result.push_back(argv[++i]);
    else if (value.rfind(name + "=", 0) == 0)
      result.push_back(value.substr(name.size() + 1));
  }
  return result;
}

int remaining_ms(Clock::time_point deadline) {
  const auto remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
  if (remaining.count() <= 0)
    return 0;
  return static_cast<int>(std::min<std::int64_t>(remaining.count(), 60000));
}

void close_fd(int &fd) {
  if (fd >= 0)
    (void)::close(fd);
  fd = -1;
}

bool within(const std::filesystem::path &path, const std::filesystem::path &root) {
  std::error_code error;
  const auto relative = std::filesystem::relative(path, root, error);
  const auto text = relative.generic_string();
  return !error && relative != ".." && text.rfind("../", 0) != 0;
}

void send_response(const Json &request, const Json &body) {
  std::lock_guard lock(output_mutex);
  auto result = body;
  result["protocol_version"] = process_protocol::version;
  result["request_id"] = request.value("request_id", std::string{});
  std::cout << result.dump() << '\n' << std::flush;
}

class ClaudeProcess {
public:
  ClaudeProcess(std::string executable, std::vector<std::string> arguments,
                std::filesystem::path directory)
      : executable_(std::move(executable)), arguments_(std::move(arguments)),
        directory_(std::move(directory)) {}
  ~ClaudeProcess() {
    stop();
  }
  ClaudeProcess(const ClaudeProcess &) = delete;
  ClaudeProcess &operator=(const ClaudeProcess &) = delete;

  void start() {
    int child_in[2] = {-1, -1};
    int child_out[2] = {-1, -1};
    int child_err[2] = {-1, -1};
    if (::pipe(child_in) < 0 || ::pipe(child_out) < 0 || ::pipe(child_err) < 0) {
      close_all(child_in, child_out, child_err);
      throw WorkerTransportError("Unable to create Claude process pipes");
    }
    pid_ = ::fork();
    if (pid_ < 0) {
      close_all(child_in, child_out, child_err);
      throw WorkerTransportError("Unable to start Claude process");
    }
    if (pid_ == 0) {
      // Keep the Claude process in the supervised worker-host process group.
      // ProcessWorkerTransport terminates that group on timeout, protocol
      // failure, or shutdown, so a host killed before its destructor runs
      // cannot orphan this child.  The parent-death signal covers standalone
      // host use as well.
      (void)::prctl(PR_SET_PDEATHSIG, SIGKILL);
      if (::chdir(directory_.c_str()) != 0)
        _exit(126);
      (void)::dup2(child_in[0], STDIN_FILENO);
      (void)::dup2(child_out[1], STDOUT_FILENO);
      (void)::dup2(child_err[1], STDERR_FILENO);
      close_all(child_in, child_out, child_err);
      std::vector<char *> argv;
      argv.reserve(arguments_.size() + 2);
      argv.push_back(const_cast<char *>(executable_.c_str()));
      for (auto &argument : arguments_)
        argv.push_back(argument.data());
      argv.push_back(nullptr);
      if (executable_.find('/') != std::string::npos)
        ::execv(executable_.c_str(), argv.data());
      else
        ::execvp(executable_.c_str(), argv.data());
      _exit(127);
    }
    close_fd(child_in[0]);
    close_fd(child_out[1]);
    close_fd(child_err[1]);
    input_fd_ = child_in[1];
    output_fd_ = child_out[0];
    error_fd_ = child_err[0];
    set_nonblocking(output_fd_);
    set_nonblocking(error_fd_);
  }

  void write_json(const Json &message, Clock::time_point deadline) {
    const auto wire = message.dump() + '\n';
    if (wire.size() > process_protocol::max_frame_bytes)
      throw WorkerTransportError("Claude request exceeds the frame limit");
    std::size_t offset = 0;
    while (offset < wire.size()) {
      pollfd descriptor{input_fd_, POLLOUT, 0};
      if (::poll(&descriptor, 1, remaining_ms(deadline)) <= 0)
        throw WorkerTransportError("Claude request timed out");
      if (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL))
        throw WorkerTransportError("Claude process closed its input");
      const auto count = ::write(input_fd_, wire.data() + offset, wire.size() - offset);
      if (count > 0)
        offset += static_cast<std::size_t>(count);
      else if (count < 0 && errno != EINTR && errno != EAGAIN)
        throw WorkerTransportError("Unable to write Claude request");
    }
  }

  std::string read_line(Clock::time_point deadline) {
    char buffer[4096];
    for (;;) {
      const auto newline = output_.find('\n');
      if (newline != std::string::npos) {
        if (newline > process_protocol::max_frame_bytes)
          throw WorkerTransportError("Claude output exceeds the frame limit");
        auto line = output_.substr(0, newline);
        output_.erase(0, newline + 1);
        if (!line.empty() && line.back() == '\r')
          line.pop_back();
        return line;
      }
      pollfd descriptors[2] = {{output_fd_, POLLIN, 0}, {error_fd_, POLLIN, 0}};
      const auto count = ::poll(descriptors, 2, remaining_ms(deadline));
      if (count == 0)
        throw WorkerTransportError("Claude response timed out");
      if (count < 0) {
        if (errno == EINTR)
          continue;
        throw WorkerTransportError("Unable to read Claude response");
      }
      drain_stderr(descriptors[1].revents);
      if (descriptors[0].revents & (POLLIN | POLLHUP | POLLERR)) {
        const auto received = ::read(output_fd_, buffer, sizeof(buffer));
        if (received > 0) {
          output_.append(buffer, static_cast<std::size_t>(received));
          if (output_.find('\n') == std::string::npos && output_.size() > max_output_bytes)
            throw WorkerTransportError("Claude output exceeds the capture limit");
        } else if (received == 0) {
          throw WorkerTransportError("Claude output was truncated");
        } else if (errno != EINTR && errno != EAGAIN) {
          throw WorkerTransportError("Unable to read Claude output");
        }
      }
      int status = 0;
      if (pid_ > 0 && ::waitpid(pid_, &status, WNOHANG) == pid_)
        throw WorkerTransportError("Claude process exited unexpectedly");
    }
  }

  void stop() noexcept {
    if (pid_ > 0) {
      (void)::kill(pid_, SIGTERM);
      int status = 0;
      const auto deadline = Clock::now() + std::chrono::milliseconds(500);
      while (Clock::now() < deadline) {
        if (::waitpid(pid_, &status, WNOHANG) == pid_)
          break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      if (::waitpid(pid_, &status, WNOHANG) == 0) {
        (void)::kill(pid_, SIGKILL);
        (void)::waitpid(pid_, &status, 0);
      }
    }
    pid_ = -1;
    close_fd(input_fd_);
    close_fd(output_fd_);
    close_fd(error_fd_);
  }

private:
  std::string executable_;
  std::vector<std::string> arguments_;
  std::filesystem::path directory_;
  pid_t pid_ = -1;
  int input_fd_ = -1, output_fd_ = -1, error_fd_ = -1;
  std::string output_, stderr_;

  static void close_all(int (&in)[2], int (&out)[2], int (&err)[2]) noexcept {
    close_fd(in[0]);
    close_fd(in[1]);
    close_fd(out[0]);
    close_fd(out[1]);
    close_fd(err[0]);
    close_fd(err[1]);
  }
  static void set_nonblocking(int fd) {
    const auto flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
      throw WorkerTransportError("Unable to configure Claude output pipe");
  }
  void drain_stderr(short events) {
    if (!(events & (POLLIN | POLLHUP | POLLERR)))
      return;
    char buffer[4096];
    for (;;) {
      const auto count = ::read(error_fd_, buffer, sizeof(buffer));
      if (count > 0) {
        if (stderr_.size() > max_error_bytes - static_cast<std::size_t>(count))
          throw WorkerTransportError("Claude stderr exceeds the capture limit");
        stderr_.append(buffer, static_cast<std::size_t>(count));
      } else if (count < 0 && (errno == EINTR || errno == EAGAIN)) {
        return;
      } else {
        close_fd(error_fd_);
        return;
      }
    }
  }
};

struct Job {
  std::string session_id;
  WorkerStatus status;
};

std::optional<WorkerInteractionResponse> read_worker_response(const std::string &request_id,
                                                              std::uint64_t timeout_ms) {
  const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  std::string line;
  for (;;) {
    const auto remaining = remaining_ms(deadline);
    if (remaining == 0)
      return std::nullopt;
    pollfd descriptor{STDIN_FILENO, POLLIN, 0};
    if (::poll(&descriptor, 1, remaining) <= 0)
      return std::nullopt;
    if (!(descriptor.revents & POLLIN) || !std::getline(std::cin, line) ||
        line.size() > process_protocol::max_frame_bytes)
      return std::nullopt;
    const auto message = Json::parse(line, nullptr, false);
    if (message.is_discarded() || !message.is_object() ||
        message.value("message_type", std::string{}) != "worker_response" ||
        message.value("request_id", std::string{}) != request_id)
      return std::nullopt;
    try {
      WorkerInteractionResponse response;
      response.request_id = request_id;
      response.state = message.value("decision", WorkerInteractionState::Denied);
      response.payload = message.value("payload", Json::object());
      response.reason = message.value("reason", std::string{});
      if (!response.payload.is_object() ||
          response.payload.dump().size() > process_protocol::max_interaction_payload_bytes)
        return std::nullopt;
      return response;
    } catch (...) {
      return std::nullopt;
    }
  }
}

std::filesystem::path safe_project(const Json &metadata,
                                   const std::vector<std::filesystem::path> &roots) {
  const auto raw = metadata.value("project_dir", std::string{});
  if (raw.empty())
    throw WorkerTransportError("Claude project_dir is required");
  std::error_code error;
  const auto project = std::filesystem::weakly_canonical(raw, error);
  if (error || !std::filesystem::is_directory(project))
    throw WorkerTransportError("Claude project_dir is invalid");
  for (const auto &root : roots)
    if (within(project, root))
      return project;
  throw WorkerTransportError("Claude project_dir is outside an allowed root");
}

void interaction_response(ClaudeProcess &process, const Json &event, const WorkerRequest &job,
                          const std::string &session, std::uint64_t timeout_ms,
                          const std::string &subtype) {
  const auto request_id = event.value("request_id", std::string{});
  if (request_id.empty() || request_id.size() > process_protocol::max_interaction_id_bytes)
    throw WorkerTransportError("Claude control request id is invalid");
  const auto request = event.value("request", Json::object());
  if (!request.is_object())
    throw WorkerTransportError("Claude control request is invalid");
  const bool permission = subtype == "can_use_tool";
  const bool question = subtype == "ask_user_question";
  if (!permission && !question)
    throw WorkerTransportError("Claude emitted an unsupported control request");
  WorkerInteractionRequest interaction;
  interaction.request_id = "claude-" + request_id;
  interaction.worker_job_id = job.job_id;
  interaction.worker_id = "claude";
  interaction.external_job_id = session;
  interaction.session_id = session;
  interaction.type =
      permission ? WorkerInteractionType::Permission : WorkerInteractionType::Question;
  interaction.title = permission ? "Claude requests tool permission" : "Claude asks a question";
  interaction.summary =
      permission ? "Claude requests permission to use a tool" : "Claude requests human input";
  interaction.created_at = timestamp();
  interaction.risk = permission ? "medium" : "low";
  interaction.category = permission ? "claude.permission" : "claude.question";
  interaction.payload = request;
  if (interaction.payload.dump().size() > process_protocol::max_interaction_payload_bytes)
    throw WorkerTransportError("Claude control request exceeds the interaction limit");
  {
    std::lock_guard lock(output_mutex);
    Json wire = interaction;
    wire["protocol_version"] = process_protocol::version;
    wire["message_type"] = "worker_request";
    std::cout << wire.dump() << '\n' << std::flush;
  }
  const auto answer = read_worker_response(interaction.request_id, timeout_ms);
  if (!answer)
    throw WorkerTransportError("Claude interaction response timed out");
  Json response{{"type", "control_response"}, {"response", {{"request_id", request_id}}}};
  auto &body = response["response"];
  if (question) {
    if (answer->state != WorkerInteractionState::Answered)
      body["response"] = Json{{"behavior", "deny"}, {"message", answer->reason}};
    else
      body["response"] = Json{{"behavior", "allow"}, {"answer", answer->payload}};
  } else if (answer->state == WorkerInteractionState::Approved) {
    body["subtype"] = "success";
    body["response"] = Json{{"behavior", "allow"}, {"updatedInput", Json::object()}};
  } else {
    body["subtype"] = "success";
    body["response"] = Json{{"behavior", "deny"}, {"message", answer->reason}};
  }
  process.write_json(response, Clock::now() + std::chrono::milliseconds(timeout_ms));
}

WorkerStatus run_turn(const std::string &claude, const std::vector<std::filesystem::path> &roots,
                      const WorkerRequest &request, std::uint64_t timeout_ms,
                      std::uint64_t interaction_timeout_ms,
                      const std::vector<std::string> &claude_args) {
  const auto project = safe_project(request.metadata, roots);
  const auto session = request.metadata.value("claude_session_id", std::string{});
  if (session.size() > max_session_bytes)
    throw WorkerTransportError("Claude session id is too long");
  std::vector<std::string> args{
      "--print",   "--output-format",          "stream-json", "--input-format", "stream-json",
      "--verbose", "--permission-prompt-tool", "stdio"};
  if (!session.empty()) {
    args.push_back("--resume");
    args.push_back(session);
  }
  args.insert(args.end(), claude_args.begin(), claude_args.end());
  ClaudeProcess process(claude, std::move(args), project);
  process.start();
  const auto started = Clock::now();
  process.write_json(
      Json{{"type", "user"},
           {"message",
            {{"role", "user"},
             {"content", Json::array({Json{{"type", "text"}, {"text", request.instructions}}})}}}},
      started + std::chrono::milliseconds(timeout_ms));
  WorkerStatus result;
  result.state = WorkerJobState::Unknown;
  result.result = Json::object();
  result.metadata = Json::object();
  WorkerUsage usage;
  usage.executor = "claude";
  usage.wall_duration_ms = 0;
  std::string session_id = session;
  std::string summary;
  std::string model;
  std::uint64_t tools = 0;
  for (;;) {
    const auto event = Json::parse(
        process.read_line(started + std::chrono::milliseconds(timeout_ms)), nullptr, false);
    if (event.is_discarded() || !event.is_object())
      throw WorkerTransportError("Claude emitted malformed JSON");
    const auto type = event.value("type", std::string{});
    if (type == "control_request") {
      const auto request_object = event.value("request", Json::object());
      interaction_response(process, event, request, session_id, interaction_timeout_ms,
                           request_object.value("subtype", std::string{}));
      continue;
    }
    if (type == "system" && event.value("subtype", std::string{}) == "init") {
      session_id = event.value("session_id", session_id);
      model = event.value("model", std::string{});
      usage.model = model;
      continue;
    }
    if (type == "assistant") {
      const auto message = event.value("message", Json::object());
      model = message.value("model", model);
      usage.model = model;
      if (message.contains("usage") && message.at("usage").is_object()) {
        const auto u = message.at("usage");
        if (u.contains("input_tokens") && u.at("input_tokens").is_number_unsigned())
          usage.input_tokens = u.at("input_tokens").get<std::uint64_t>();
        if (u.contains("output_tokens") && u.at("output_tokens").is_number_unsigned())
          usage.output_tokens = u.at("output_tokens").get<std::uint64_t>();
      }
      for (const auto &part : message.value("content", Json::array())) {
        if (!part.is_object())
          continue;
        if (part.value("type", std::string{}) == "text")
          summary += part.value("text", std::string{});
        if (part.value("type", std::string{}) == "tool_use")
          ++tools;
      }
      if (summary.size() > max_summary_bytes)
        summary.resize(max_summary_bytes);
      continue;
    }
    if (type != "result")
      continue;
    session_id = event.value("session_id", session_id);
    if (session_id.empty() || session_id.size() > max_session_bytes)
      throw WorkerTransportError("Claude did not return a valid session id");
    const auto duration = event.value("duration_ms", std::uint64_t{0});
    usage.wall_duration_ms =
        duration != 0
            ? std::optional<std::uint64_t>(duration)
            : std::optional<std::uint64_t>(static_cast<std::uint64_t>(
                  std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started)
                      .count()));
    if (event.contains("usage") && event.at("usage").is_object()) {
      const auto u = event.at("usage");
      if (u.contains("input_tokens") && u.at("input_tokens").is_number_unsigned())
        usage.input_tokens = u.at("input_tokens").get<std::uint64_t>();
      if (u.contains("output_tokens") && u.at("output_tokens").is_number_unsigned())
        usage.output_tokens = u.at("output_tokens").get<std::uint64_t>();
      if (u.contains("total_tokens") && u.at("total_tokens").is_number_unsigned())
        usage.total_tokens = u.at("total_tokens").get<std::uint64_t>();
    }
    if (!usage.total_tokens && usage.input_tokens && usage.output_tokens)
      usage.total_tokens = *usage.input_tokens + *usage.output_tokens;
    if (tools)
      usage.tool_calls = tools;
    result.state =
        event.value("is_error", false) || event.value("subtype", std::string{}) != "success"
            ? WorkerJobState::Failed
            : WorkerJobState::Completed;
    result.error = result.state == WorkerJobState::Failed
                       ? event.value("error", event.value("result", "Claude reported a failure"))
                       : "";
    result.result = Json{{"summary", event.value("result", summary)},
                         {"session_id", session_id},
                         {"model", model},
                         {"executor", "claude"}};
    result.metadata = Json{{"claude_session_id", session_id}, {"project_root_verified", true}};
    result.usage = usage;
    process.stop();
    return result;
  }
}
} // namespace

int main(int argc, char **argv) {
  const auto claude = option(argc, argv, "--claude", "claude");
  const auto timeout_ms = std::stoull(option(argc, argv, "--timeout-ms", "120000"));
  const auto interaction_timeout_ms =
      std::stoull(option(argc, argv, "--interaction-timeout-ms", "300000"));
  const auto claude_args = options(argc, argv, "--claude-arg");
  std::vector<std::filesystem::path> roots;
  for (const auto &raw : options(argc, argv, "--allowed-root")) {
    std::error_code error;
    const auto root = std::filesystem::weakly_canonical(raw, error);
    if (!error && std::filesystem::is_directory(root))
      roots.push_back(root);
  }
  if (roots.empty())
    return 64;
  std::map<std::string, Job> jobs;
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.size() > process_protocol::max_frame_bytes)
      return 64;
    const auto request = Json::parse(line, nullptr, false);
    if (request.is_discarded() || !request.is_object())
      return 65;
    const auto operation = request.value("operation", std::string{});
    if (operation == "hello") {
      send_response(
          request,
          {{"ok", true},
           {"metadata", Json{{"name", "Claude Code"},
                             {"version", "stream-json"},
                             {"description", "optional Claude Code adapter"},
                             {"capabilities", Json::array({"coding", "sessions", "interactions"})},
                             {"supports_recovery", true},
                             {"supports_cancellation", false}}}});
      continue;
    }
    if (operation == "shutdown") {
      send_response(request, {{"ok", true}, {"state", "Completed"}});
      return 0;
    }
    if (operation == "submit") {
      const auto payload = request.value("payload", Json::object());
      try {
        (void)safe_project(payload.value("metadata", Json::object()), roots);
      } catch (const WorkerTransportError &error) {
        send_response(request, {{"ok", true},
                                {"state", WorkerJobState::Failed},
                                {"external_job_id", "claude-invalid-project"},
                                {"error", error.what()}});
        continue;
      }
      WorkerRequest job;
      job.job_id = request.value("job_id", std::string{});
      job.worker_id = payload.value("worker_id", std::string{"claude"});
      job.instructions = payload.value("instructions", std::string{});
      job.metadata = payload.value("metadata", Json::object());
      auto status = run_turn(claude, roots, job, timeout_ms, interaction_timeout_ms, claude_args);
      const auto external = status.result.value("session_id", std::string{});
      jobs[external] = {external, status};
      Json body{{"ok", true},
                {"state", status.state},
                {"external_job_id", external},
                {"payload", status.result},
                {"metadata", status.metadata},
                {"usage", status.usage}};
      if (!status.error.empty())
        body["error"] = status.error;
      send_response(request, body);
      continue;
    }
    const auto external = request.value("external_job_id", std::string{});
    const auto found = jobs.find(external);
    if (operation == "cancel") {
      send_response(request, {{"ok", true}, {"acknowledged", false}, {"state", "Unknown"}});
      continue;
    }
    if (found == jobs.end()) {
      send_response(request, {{"ok", true}, {"state", "Unknown"}});
      continue;
    }
    send_response(request, {{"ok", true},
                            {"state", found->second.status.state},
                            {"payload", found->second.status.result},
                            {"metadata", found->second.status.metadata},
                            {"usage", found->second.status.usage},
                            {"error", found->second.status.error}});
  }
  return 0;
}
