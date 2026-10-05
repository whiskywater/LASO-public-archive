// Generic OpenCode adapter.  This executable is intentionally outside LASO
// Core: LASO speaks the versioned process protocol, while this adapter speaks
// OpenCode's local headless HTTP API and owns the OpenCode child process.
#include <cerrno>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <laso/workers/process_protocol.hpp>
#include <laso/workers/worker.hpp>
#include <limits>
#include <map>
#include <mutex>
#include <poll.h>
#include <netinet/in.h>
#include <optional>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <thread>
#include <stop_token>
#include <unistd.h>

using namespace laso;

namespace {
constexpr std::size_t max_http_body = 4 * 1024 * 1024;
constexpr std::size_t max_project_roots = 32;
std::mutex protocol_output_mutex;

bool arm_parent_death_signal() noexcept {
#ifdef __linux__
  const auto parent = ::getppid();
  if (::prctl(PR_SET_PDEATHSIG, SIGTERM) != 0 || ::getppid() != parent)
    return false;
#endif
  return true;
}

bool process_group_alive(pid_t process_group) noexcept {
  if (process_group <= 0)
    return false;
  if (::kill(-process_group, 0) == 0)
    return true;
  return errno == EPERM;
}

void terminate_process_group(pid_t process_group) noexcept {
  if (process_group <= 0)
    return;
  if (::kill(-process_group, SIGTERM) < 0 && errno != ESRCH)
    (void)::kill(process_group, SIGTERM);
  const auto graceful_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (process_group_alive(process_group) && std::chrono::steady_clock::now() < graceful_deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  if (!process_group_alive(process_group))
    return;
  if (::kill(-process_group, SIGKILL) < 0 && errno != ESRCH)
    (void)::kill(process_group, SIGKILL);
}

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

std::string encoded(const std::string &value) {
  static constexpr char hex[] = "0123456789ABCDEF";
  std::string result;
  for (const auto c : value) {
    const auto u = static_cast<unsigned char>(c);
    if ((u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') || (u >= '0' && u <= '9') ||
        u == '-' || u == '_' || u == '.' || u == '/')
      result.push_back(static_cast<char>(u));
    else {
      result.push_back('%');
      result.push_back(hex[u >> 4]);
      result.push_back(hex[u & 15]);
    }
  }
  return result;
}

bool within(const std::filesystem::path &path, const std::filesystem::path &root) {
  std::error_code ec;
  const auto relative = std::filesystem::relative(path, root, ec);
  const auto text = relative.generic_string();
  return !ec && relative != ".." && text.rfind("../", 0) != 0;
}

void response(const Json &request, const Json &body) {
  std::lock_guard lock(protocol_output_mutex);
  auto result = body;
  result["protocol_version"] = process_protocol::version;
  result["request_id"] = request.value("request_id", std::string{});
  std::cout << result.dump() << '\n' << std::flush;
}

class OpenCodeServer {
public:
  OpenCodeServer(std::string executable, unsigned port, std::uint64_t timeout_ms)
      : executable_(std::move(executable)), port_(port), timeout_ms_(timeout_ms) {}
  ~OpenCodeServer() { stop(); }
  OpenCodeServer(const OpenCodeServer &) = delete;
  OpenCodeServer &operator=(const OpenCodeServer &) = delete;

  void start() {
    if (pid_ > 0)
      return;
    pid_ = fork();
    if (pid_ < 0)
      throw std::runtime_error("cannot start OpenCode server");
    if (pid_ == 0) {
      if (!arm_parent_death_signal())
        _exit(125);
      (void)::setpgid(0, 0);
      const auto null = open("/dev/null", O_WRONLY);
      if (null >= 0) {
        dup2(null, STDOUT_FILENO);
        dup2(null, STDERR_FILENO);
        close(null);
      }
      std::vector<std::string> args{executable_, "serve", "--hostname", "127.0.0.1",
                                    "--port", std::to_string(port_)};
      std::vector<char *> argv;
      for (auto &arg : args)
        argv.push_back(arg.data());
      argv.push_back(nullptr);
      if (executable_.find('/') != std::string::npos)
        execv(executable_.c_str(), argv.data());
      else
        execvp(executable_.c_str(), argv.data());
      _exit(127);
    }
    (void)::setpgid(pid_, pid_);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms_);
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      if (waitpid(pid_, &status, WNOHANG) == pid_)
        throw std::runtime_error("OpenCode server exited during startup");
      try {
        const auto health = request(false, "/global/health", Json::object(), 250);
        if (health.status == 200)
          return;
      } catch (...) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    stop();
    throw std::runtime_error("OpenCode server startup timed out");
  }

  void stop() noexcept {
    if (pid_ <= 0)
      return;
    const auto process_group = pid_;
    terminate_process_group(process_group);
    int status = 0;
    while (waitpid(pid_, &status, 0) < 0 && errno == EINTR)
      continue;
    pid_ = -1;
  }

  struct Reply {
    int status = 0;
    std::string body;
  };

  Reply request(bool post, const std::string &target, const Json &body,
                std::uint64_t request_timeout_ms = 0) const {
    const auto fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
      throw std::runtime_error("cannot create OpenCode HTTP socket");
    struct Close {
      int fd;
      ~Close() { close(fd); }
    } close_fd{fd};
    const auto effective_timeout = request_timeout_ms == 0 ? timeout_ms_ : request_timeout_ms;
    timeval timeout{static_cast<time_t>(effective_timeout / 1000),
                    static_cast<suseconds_t>((effective_timeout % 1000) * 1000)};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port_));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0)
      throw std::runtime_error("cannot connect to OpenCode server");
    const auto content = post ? body.dump() : std::string{};
    const auto wire = std::string(post ? "POST " : "GET ") + target +
                      " HTTP/1.1\r\nHost: 127.0.0.1:" + std::to_string(port_) +
                      "\r\nUser-Agent: laso-opencode-worker/1\r\nAccept: application/json\r\n" +
                      (post ? "Content-Type: application/json\r\nContent-Length: " +
                                  std::to_string(content.size()) + "\r\n"
                            : "") +
                      "Connection: close\r\n\r\n" + content;
    std::size_t sent = 0;
    while (sent < wire.size()) {
      const auto count = send(fd, wire.data() + sent, wire.size() - sent, MSG_NOSIGNAL);
      if (count <= 0)
        throw std::runtime_error("OpenCode HTTP request failed");
      sent += static_cast<std::size_t>(count);
    }
    std::string raw;
    char buffer[4096];
    std::size_t expected_body = std::numeric_limits<std::size_t>::max();
    bool headers_parsed = false;
    bool chunked = false;
    for (;;) {
      const auto header_end = raw.find("\r\n\r\n");
      if (header_end != std::string::npos && !headers_parsed) {
        const auto headers = raw.substr(0, header_end);
        const auto marker = headers.find("Content-Length:");
        if (marker != std::string::npos) {
          const auto value_start = headers.find_first_not_of(' ', marker + 15);
          if (value_start == std::string::npos)
            throw std::runtime_error("OpenCode HTTP content length is malformed");
          expected_body = std::stoull(headers.substr(value_start, header_end - value_start));
          if (expected_body > max_http_body)
            throw std::runtime_error("OpenCode HTTP body exceeds the limit");
        } else if (headers.find("Transfer-Encoding: chunked") != std::string::npos) {
          chunked = true;
        }
        headers_parsed = true;
      }
      if (header_end != std::string::npos && !chunked &&
          expected_body != std::numeric_limits<std::size_t>::max() &&
          raw.size() - header_end - 4 >= expected_body)
        break;
      const auto count = recv(fd, buffer, sizeof(buffer), 0);
      if (count == 0)
        break;
      if (count < 0 && errno == EINTR)
        continue;
      if (count < 0)
        throw std::runtime_error("OpenCode HTTP response timed out");
      raw.append(buffer, static_cast<std::size_t>(count));
      if (raw.size() > max_http_body + 16384)
        throw std::runtime_error("OpenCode HTTP response exceeds the limit");
    }
    const auto separator = raw.find("\r\n\r\n");
    if (separator == std::string::npos)
      throw std::runtime_error("OpenCode HTTP response was truncated");
    if (expected_body == std::numeric_limits<std::size_t>::max())
      expected_body = raw.size() - separator - 4;
    const auto first_space = raw.find(' ');
    if (first_space == std::string::npos)
      throw std::runtime_error("OpenCode HTTP status is malformed");
    const auto second_space = raw.find(' ', first_space + 1);
    if (second_space == std::string::npos)
      throw std::runtime_error("OpenCode HTTP status is malformed");
    Reply reply;
    reply.status = std::stoi(raw.substr(first_space + 1, second_space - first_space - 1));
    if (!chunked) {
      reply.body = raw.substr(separator + 4, expected_body);
      if (reply.body.size() != expected_body)
        throw std::runtime_error("OpenCode HTTP response body was truncated");
    } else {
      std::size_t offset = separator + 4;
      while (offset < raw.size()) {
        const auto line_end = raw.find("\r\n", offset);
        if (line_end == std::string::npos)
          throw std::runtime_error("OpenCode chunk header was truncated");
        auto size_text = raw.substr(offset, line_end - offset);
        if (const auto extension = size_text.find(';'); extension != std::string::npos)
          size_text.resize(extension);
        const auto size = std::stoull(size_text, nullptr, 16);
        offset = line_end + 2;
        if (size == 0)
          break;
        if (size > max_http_body || size > raw.size() - offset ||
            reply.body.size() > max_http_body - size)
          throw std::runtime_error("OpenCode chunk exceeds the response limit");
        reply.body.append(raw, offset, size);
        offset += size;
        if (raw.substr(offset, 2) != "\r\n")
          throw std::runtime_error("OpenCode chunk terminator is malformed");
        offset += 2;
      }
    }
    if (reply.body.size() > max_http_body)
      throw std::runtime_error("OpenCode HTTP body exceeds the limit");
    return reply;
  }

  bool alive() const {
    if (pid_ <= 0)
      return false;
    int status = 0;
    return waitpid(pid_, &status, WNOHANG) == 0;
  }

  void watch(const std::string &directory, const std::stop_token &stop,
             const std::function<void(const Json &)> &callback) const {
    const auto fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
      return;
    struct Close {
      int fd;
      ~Close() { close(fd); }
    } close_fd{fd};
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port_));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0)
      return;
    const auto target = "/event?directory=" + encoded(directory);
    const auto wire = "GET " + target + " HTTP/1.1\r\nHost: 127.0.0.1:" +
                      std::to_string(port_) + "\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n";
    std::size_t sent = 0;
    while (sent < wire.size()) {
      const auto count = send(fd, wire.data() + sent, wire.size() - sent, MSG_NOSIGNAL);
      if (count <= 0)
        return;
      sent += static_cast<std::size_t>(count);
    }
    std::string stream;
    char buffer[4096];
    while (!stop.stop_requested()) {
      pollfd descriptor{fd, POLLIN, 0};
      if (poll(&descriptor, 1, 100) <= 0)
        continue;
      if (!(descriptor.revents & POLLIN))
        break;
      const auto count = recv(fd, buffer, sizeof(buffer), 0);
      if (count <= 0)
        break;
      stream.append(buffer, static_cast<std::size_t>(count));
      if (stream.size() > max_http_body)
        return;
      for (;;) {
        const auto marker = stream.find("data:");
        if (marker == std::string::npos)
          break;
        const auto end = stream.find("\n\n", marker);
        if (end == std::string::npos)
          break;
        auto data = stream.substr(marker + 5, end - marker - 5);
        while (!data.empty() && (data.front() == ' ' || data.front() == '\r' || data.front() == '\n'))
          data.erase(data.begin());
        const auto event = Json::parse(data, nullptr, false);
        if (!event.is_discarded() && event.is_object())
          callback(event);
        stream.erase(0, end + 2);
      }
    }
  }

private:
  std::string executable_;
  unsigned port_;
  std::uint64_t timeout_ms_;
  pid_t pid_ = -1;
};

struct Job {
  std::string session_id;
  Json result = Json::object();
  Json metadata = Json::object();
  WorkerUsage usage;
  std::string error;
};

std::pair<Json, WorkerUsage> normalize(const Json &document, const std::string &session_id,
                                       std::uint64_t duration_ms) {
  Json result{{"session_id", session_id}, {"summary", ""}, {"files_changed", Json::array()}};
  WorkerUsage usage;
  usage.executor = "opencode";
  usage.wall_duration_ms = duration_ms;
  if (document.contains("info") && document.at("info").is_object()) {
    const auto &info = document.at("info");
    result["model"] = info.value("modelID", std::string{});
    result["provider"] = info.value("providerID", std::string{});
    usage.model = info.value("modelID", std::string{});
    usage.provider = info.value("providerID", std::string{});
    const auto tokens = info.value("tokens", Json::object());
    if (tokens.is_object()) {
      if (tokens.contains("input") && tokens.at("input").is_number_unsigned())
        usage.input_tokens = tokens.at("input").get<std::uint64_t>();
      if (tokens.contains("output") && tokens.at("output").is_number_unsigned())
        usage.output_tokens = tokens.at("output").get<std::uint64_t>();
      if (usage.input_tokens && usage.output_tokens)
        usage.total_tokens = *usage.input_tokens + *usage.output_tokens;
    }
    if (info.contains("cost") && info.at("cost").is_number())
      usage.cost_units = info.at("cost").get<double>();
  }
  if (document.contains("parts") && document.at("parts").is_array()) {
    std::string text;
    std::uint64_t tools = 0;
    for (const auto &part : document.at("parts")) {
      if (!part.is_object())
        continue;
      if (part.value("type", std::string{}) == "text")
        text += part.value("text", std::string{});
      if (part.value("type", std::string{}).rfind("tool", 0) == 0)
        ++tools;
    }
    if (text.size() > 65536)
      text.resize(65536);
    result["summary"] = text;
    if (tools)
      usage.tool_calls = tools;
  }
  return {result, usage};
}

std::optional<WorkerInteractionResponse> read_interaction_response(const std::string &request_id,
                                                                    std::uint64_t timeout_ms) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  std::string line;
  while (std::chrono::steady_clock::now() < deadline) {
    pollfd descriptor{STDIN_FILENO, POLLIN, 0};
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - std::chrono::steady_clock::now());
    const auto timeout = static_cast<int>(std::min<std::int64_t>(
        std::max<std::int64_t>(remaining.count(), 1), 60000));
    const auto ready = poll(&descriptor, 1, timeout);
    if (ready == 0)
      continue;
    if (ready < 0 && errno == EINTR)
      continue;
    if (ready <= 0 || !(descriptor.revents & POLLIN))
      return std::nullopt;
    if (!std::getline(std::cin, line) || line.size() > process_protocol::max_frame_bytes)
      return std::nullopt;
    const auto message = Json::parse(line, nullptr, false);
    if (message.is_discarded() || !message.is_object() ||
        message.value("message_type", std::string{}) != "worker_response" ||
        message.value("request_id", std::string{}) != request_id)
      return std::nullopt;
    try {
      WorkerInteractionResponse answer;
      answer.request_id = request_id;
      answer.state = message.value("decision", WorkerInteractionState::Denied);
      answer.payload = message.value("payload", Json::object());
      answer.reason = message.value("reason", std::string{});
      if (!answer.payload.is_object() || answer.payload.dump().size() >
                                         process_protocol::max_interaction_payload_bytes)
        return std::nullopt;
      return answer;
    } catch (...) {
      return std::nullopt;
    }
  }
  return std::nullopt;
}

void route_opencode_interaction(OpenCodeServer &server, const Json &event,
                                const std::string &job_id, const std::string &session,
                                const std::string &directory, std::uint64_t timeout_ms) {
  const auto type = event.value("type", std::string{});
  if (type != "permission.asked" && type != "question.asked")
    return;
  const auto properties = event.value("properties", Json::object());
  if (!properties.is_object() ||
      (properties.value("sessionID", std::string{}) != session &&
       properties.value("session_id", std::string{}) != session))
    return;
  const auto external_id = properties.value("id", std::string{});
  if (external_id.empty() || external_id.size() > process_protocol::max_interaction_id_bytes)
    return;
  const bool question = type == "question.asked";
  const auto request_id = "opencode-" + external_id;
  Json payload = question
                     ? Json{{"questions", properties.value("questions", Json::array())}}
                     : Json{{"permission", properties.value("permission", std::string{})},
                            {"patterns", properties.value("patterns", Json::array())},
                            {"metadata", properties.value("metadata", Json::object())}};
  if (payload.dump().size() > process_protocol::max_interaction_payload_bytes)
    return;
  WorkerInteractionRequest request;
  request.request_id = request_id;
  request.worker_job_id = job_id;
  request.worker_id = "opencode";
  request.external_job_id = session;
  request.session_id = session;
  request.type = question ? WorkerInteractionType::Question : WorkerInteractionType::Permission;
  request.title = question ? properties.value("header", "OpenCode question")
                           : "OpenCode permission request";
  request.summary = question ? "OpenCode is asking for clarification"
                             : "OpenCode requests permission to continue";
  request.created_at = timestamp();
  request.risk = question ? "low" : "medium";
  request.category = question ? "opencode.question" : "opencode.permission";
  request.payload = std::move(payload);
  {
    std::lock_guard lock(protocol_output_mutex);
    Json wire = request;
    wire["protocol_version"] = process_protocol::version;
    wire["message_type"] = "worker_request";
    std::cout << wire.dump() << '\n' << std::flush;
  }
  auto answer = read_interaction_response(request_id, timeout_ms);
  if (!answer || answer->state == WorkerInteractionState::Denied ||
      answer->state == WorkerInteractionState::Cancelled ||
      answer->state == WorkerInteractionState::Expired) {
    if (question) {
      const auto reply = server.request(true, "/question/" + external_id +
                                                 "/reject?directory=" + encoded(directory),
                                         Json::object());
      if (reply.status < 200 || reply.status >= 300)
        throw std::runtime_error("OpenCode rejected the question response");
    } else {
      const auto reply = server.request(
          true, "/permission/" + external_id + "/reply?directory=" + encoded(directory),
          Json{{"reply", "reject"},
               {"message", answer ? answer->reason : "LASO did not resolve the request"}});
      if (reply.status < 200 || reply.status >= 300)
        throw std::runtime_error("OpenCode rejected the permission response");
    }
    return;
  }
  try {
    if (question) {
      auto answers = answer->payload.value("answers", Json::array());
      if (!answers.is_array())
        answers = Json::array();
      const auto reply = server.request(
          true, "/question/" + external_id + "/reply?directory=" + encoded(directory),
          Json{{"answers", std::move(answers)}});
      if (reply.status < 200 || reply.status >= 300)
        throw std::runtime_error("OpenCode rejected the question response");
    } else {
      const auto reply = server.request(
          true, "/permission/" + external_id + "/reply?directory=" + encoded(directory),
          Json{{"reply", "once"}});
      if (reply.status < 200 || reply.status >= 300)
        throw std::runtime_error("OpenCode rejected the permission response");
    }
  } catch (...) {
    // The OpenCode turn remains externally ambiguous if its interaction
    // response cannot be delivered; the parent transport will report the
    // resulting process/HTTP failure rather than claiming approval.
  }
}
} // namespace

int main(int argc, char **argv) {
  try {
    if (!arm_parent_death_signal())
      return 125;
    const auto opencode = option(argc, argv, "--opencode", "opencode");
    const auto port = static_cast<unsigned>(std::stoul(option(argc, argv, "--port", "18091")));
    const auto timeout = static_cast<std::uint64_t>(std::stoull(
        option(argc, argv, "--timeout-ms", "120000")));
    const auto roots_raw = options(argc, argv, "--allowed-root");
    if (roots_raw.empty() || roots_raw.size() > max_project_roots)
      return 78;
    std::vector<std::filesystem::path> roots;
    for (const auto &root : roots_raw) {
      std::error_code ec;
      auto canonical = std::filesystem::weakly_canonical(root, ec);
      if (ec || !std::filesystem::is_directory(canonical, ec))
        return 79;
      roots.push_back(std::move(canonical));
    }
    OpenCodeServer server(opencode, port, timeout);
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
        try {
          server.start();
          response(request, { {"ok", true},
                              {"metadata", Json{{"name", "OpenCode worker"},
                                                 {"version", "1"},
                                                 {"description", "OpenCode coding-agent adapter"},
                                                 {"capabilities", Json::array({"coding", "sessions"})},
                                                 {"supports_recovery", true},
                                                 {"supports_cancellation", true}}} });
        } catch (const std::exception &error) {
          response(request, {{"ok", true}, {"state", "Failed"}, {"error", error.what()}});
        }
        continue;
      }
      if (operation == "shutdown") {
        server.stop();
        response(request, {{"ok", true}, {"state", "Completed"}});
        return 0;
      }
      if (!server.alive()) {
        response(request, {{"ok", true}, {"state", "Failed"}, {"error", "OpenCode server is unavailable"}});
        continue;
      }
      if (operation == "submit") {
        const auto job_id = request.value("job_id", std::string{});
        const auto payload = request.value("payload", Json::object());
        const auto metadata = payload.value("metadata", Json::object());
        const auto project = metadata.value("project_dir", std::string{});
        std::error_code ec;
        const auto canonical = std::filesystem::weakly_canonical(project, ec);
        bool allowed = !ec && std::filesystem::is_directory(canonical, ec);
        bool in_root = false;
        for (const auto &root : roots)
          in_root = in_root || within(canonical, root);
        if (!in_root)
          allowed = false;
        if (!allowed) {
          response(request, {{"ok", true}, {"state", "Failed"},
                             {"external_job_id", "opencode-rejected-" + job_id},
                             {"error", "OpenCode project is outside an allowed root"}});
          continue;
        }
        std::string session;
        try {
          session = metadata.value("opencode_session_id", std::string{});
          if (session.empty()) {
            const auto created = server.request(true,
                                                "/session?directory=" + encoded(canonical.string()),
                                                Json::object());
            if (created.status != 200)
              throw std::runtime_error("OpenCode session creation failed");
            const auto document = Json::parse(created.body, nullptr, false);
            session = document.value("id", std::string{});
          }
          if (session.empty() || session.size() > 128)
            throw std::runtime_error("OpenCode did not return a session id");
          const auto instructions = payload.value("instructions", std::string{});
          Json message{{"parts", Json::array({Json{{"type", "text"},
                                                     {"text", instructions + "\nInput JSON: " +
                                                                   payload.value("input", Json::object()).dump()}}})}};
          const auto started = std::chrono::steady_clock::now();
          std::jthread watcher([&server, &job_id, &session, &canonical, timeout](std::stop_token stop) {
            server.watch(canonical.string(), stop, [&](const Json &event) {
              try {
                route_opencode_interaction(server, event, job_id, session, canonical.string(),
                                           timeout);
              } catch (...) {
                // A malformed/failed interaction is deliberately not turned
                // into an approval. The active turn will time out or fail.
              }
            });
          });
          const auto reply = server.request(true, "/session/" + session + "/message",
                                             message);
          watcher.request_stop();
          watcher.join();
          if (reply.status < 200 || reply.status >= 300)
            throw std::runtime_error("OpenCode rejected the coding turn");
          const auto document = Json::parse(reply.body, nullptr, false);
          if (document.is_discarded() || !document.is_object())
            throw std::runtime_error("OpenCode returned malformed structured output");
          const auto normalized = normalize(
              document, session,
              static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now() - started)
                                              .count()));
          Job stored;
          stored.session_id = session;
          stored.result = normalized.first;
          stored.metadata = Json{{"session_id", session}};
          stored.usage = normalized.second;
          jobs[session] = stored;
          Json completed{{"ok", true}, {"state", "Completed"}, {"external_job_id", session},
                         {"payload", stored.result}, {"metadata", stored.metadata},
                         {"usage", Json(stored.usage)}};
          response(request, completed);
        } catch (const std::exception &error) {
          response(request, {{"ok", true},
                             {"state", "Failed"},
                             {"external_job_id", session.empty() ? "opencode-" + job_id : session},
                             {"error", error.what()}});
        }
        continue;
      }
      const auto session = request.value("external_job_id", std::string{});
      if (operation == "cancel") {
        try {
          const auto reply = server.request(true, "/session/" + session + "/abort",
                                             Json::object());
          response(request, {{"ok", true}, {"state", "Cancelled"},
                             {"acknowledged", reply.status >= 200 && reply.status < 300}});
        } catch (const std::exception &error) {
          response(request, {{"ok", true}, {"state", "Unknown"}, {"error", error.what()}});
        }
        continue;
      }
      if (operation == "status" || operation == "result") {
        const auto found = jobs.find(session);
        if (found != jobs.end())
          response(request, {{"ok", true}, {"state", "Completed"}, {"payload", found->second.result},
                             {"metadata", found->second.metadata}, {"usage", found->second.usage}});
        else
          response(request, {{"ok", true}, {"state", "Unknown"},
                             {"metadata", Json{{"session_id", session}}},
                             {"error", "OpenCode session exists but turn state is unknown after adapter restart"}});
        continue;
      }
      response(request, {{"ok", false}, {"error", "Unsupported OpenCode worker operation"}});
    }
    server.stop();
  } catch (...) {
    return 80;
  }
  return 0;
}
