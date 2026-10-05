#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <laso/workers/process_transport.hpp>
#include <limits>
#include <map>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace laso {
namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t max_id_bytes = 512;
constexpr std::size_t max_error_bytes = 512;

void close_fd(int &fd) {
  if (fd >= 0)
    ::close(fd);
  fd = -1;
}

void nonblocking(int fd) {
  const auto flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    throw WorkerTransportError("Unable to configure worker process pipe");
}

void reserve_process_fd(int &fd) {
  if (fd >= STDERR_FILENO + 1)
    return;
  const auto replacement = ::fcntl(fd, F_DUPFD_CLOEXEC, STDERR_FILENO + 1);
  if (replacement < 0)
    throw WorkerTransportError("Unable to reserve worker process pipe");
  ::close(fd);
  fd = replacement;
}

int remaining_ms(Clock::time_point deadline) {
  const auto remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
  if (remaining.count() <= 0)
    return 0;
  // A bounded poll interval must not shorten the transport's actual request
  // deadline.  The previous one-minute cap turned a quiet but valid provider
  // response into a false terminal timeout.
  return static_cast<int>(std::min<std::int64_t>(
      remaining.count(), static_cast<std::int64_t>(std::numeric_limits<int>::max())));
}

bool bounded_text(const std::string &value, std::size_t maximum) {
  return !value.empty() && value.size() <= maximum;
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
  const auto graceful_deadline = Clock::now() + std::chrono::milliseconds(500);
  while (process_group_alive(process_group) && Clock::now() < graceful_deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  if (!process_group_alive(process_group))
    return;
  if (::kill(-process_group, SIGKILL) < 0 && errno != ESRCH)
    (void)::kill(process_group, SIGKILL);
  const auto force_deadline = Clock::now() + std::chrono::milliseconds(500);
  while (process_group_alive(process_group) && Clock::now() < force_deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
}
} // namespace

struct ProcessWorkerTransport::Impl {
  explicit Impl(std::string worker_id, ProcessWorkerConfig worker_config)
      : id(std::move(worker_id)), config(std::move(worker_config)) {
    metadata_.id = id;
    metadata_.name = id;
    metadata_.plugin = "process";
    metadata_.event_source_id = "worker." + id;
    metadata_.local = true;
    metadata_.remote = false;
    metadata_.enabled = true;
    metadata_.supports_recovery = true;
    metadata_.status = "stopped";
    publish_metadata_locked();
  }

  ~Impl() {
    stop();
  }

  WorkerMetadata metadata() const {
    std::lock_guard lock(metadata_mutex);
    return metadata_snapshot_;
  }

  void set_interaction_handler(WorkerInteractionHandler handler) {
    std::lock_guard lock(mutex);
    interaction_handler = std::move(handler);
  }

  void start() {
    std::lock_guard lock(mutex);
    start_locked();
  }

  void start_locked() {
    if (pid > 0 && metadata_.healthy)
      return;
    if (pid > 0)
      terminate_locked();
    if (pid > 0)
      return;
    metadata_.status = "starting";
    metadata_.healthy = false;
    publish_metadata_locked();
    try {
      spawn_locked();
      const auto response =
          request_locked("hello", "", "", Json{{"client", "laso"}}, config.startup_timeout_ms);
      if (!response.value("ok", false))
        throw WorkerTransportError("Worker hello was rejected");
      if (!response.contains("metadata") || !response.at("metadata").is_object() ||
          response.at("metadata").dump().size() > process_protocol::max_metadata_bytes)
        throw WorkerTransportError("Worker hello metadata is invalid");
      WorkerMetadata reported;
      try {
        reported = response.at("metadata").get<WorkerMetadata>();
      } catch (...) {
        throw WorkerTransportError("Worker hello metadata has invalid fields");
      }
      metadata_ = std::move(reported);
      metadata_.id = id;
      metadata_.plugin = "process";
      metadata_.event_source_id = "worker." + id;
      metadata_.local = true;
      metadata_.remote = false;
      metadata_.enabled = true;
      metadata_.supports_recovery = true;
      metadata_.healthy = true;
      metadata_.status = "healthy";
      publish_metadata_locked();
    } catch (...) {
      terminate_locked();
      metadata_.healthy = false;
      metadata_.status = "failed";
      publish_metadata_locked();
      throw;
    }
  }

  WorkerSubmission submit(const WorkerRequest &request) {
    std::lock_guard lock(mutex);
    ensure_started_locked();
    active_submit.store(true, std::memory_order_release);
    struct ActiveSubmitGuard {
      std::atomic<bool> &active;
      ~ActiveSubmitGuard() {
        active.store(false, std::memory_order_release);
      }
    } active_guard{active_submit};
    Json payload{{"job_id", request.job_id},
                 {"worker_id", request.worker_id},
                 {"capability", request.capability},
                 {"task_type", request.task_type},
                 {"instructions", request.instructions},
                 {"deadline", request.deadline},
                 {"idempotency_key", request.idempotency_key},
                 {"run_id", request.run_id},
                 {"node_id", request.node_id},
                 {"attempt", request.attempt},
                 {"input", request.input},
                 {"output_schema", request.output_schema},
                 {"metadata", request.metadata},
                 {"artifact_ids", request.artifact_ids}};
    const auto timeout_ms = request.timeout_ms == 0
                                ? config.request_timeout_ms
                                : std::min(request.timeout_ms, config.request_timeout_ms);
    return parse_submission_locked(
        request.job_id, request_response_locked("submit", request.job_id, "", payload, timeout_ms));
  }

  WorkerStatus status(const std::string &external_job_id) {
    return status_operation("status", external_job_id);
  }

  WorkerStatus result(const std::string &external_job_id) {
    return status_operation("result", external_job_id);
  }

  bool cancel(const std::string &external_job_id) {
    std::lock_guard lock(mutex);
    ensure_started_locked();
    const auto response = request_response_locked("cancel", "", external_job_id, Json::object(),
                                                  config.request_timeout_ms);
    if (!response.value("ok", false))
      throw WorkerTransportError("Worker cancellation was rejected");
    if (response.contains("acknowledged"))
      return response.at("acknowledged").is_boolean() && response.at("acknowledged").get<bool>();
    return response.value("payload", Json::object()).value("acknowledged", false);
  }

  bool cancel_pending(const std::string &) noexcept {
    if (!active_submit.load(std::memory_order_acquire))
      return false;
    const auto process_group = owned_process_group.load(std::memory_order_acquire);
    if (process_group <= 0)
      return false;
    terminate_process_group(process_group);
    return true;
  }

  void stop() noexcept {
    std::lock_guard lock(mutex);
    if (pid <= 0) {
      terminate_process_group(owned_process_group.load(std::memory_order_acquire));
      owned_process_group.store(-1, std::memory_order_release);
      close_fd(input_fd);
      close_fd(output_fd);
      close_fd(error_fd);
      return;
    }
    try {
      (void)request_locked("shutdown", "", "", Json::object(), 500);
    } catch (...) {
    }
    terminate_locked();
    metadata_.healthy = false;
    metadata_.status = "stopped";
    publish_metadata_locked();
  }

private:
  WorkerMetadata metadata_;
  WorkerMetadata metadata_snapshot_;
  std::string id;
  ProcessWorkerConfig config;
  mutable std::mutex mutex;
  mutable std::mutex metadata_mutex;
  pid_t pid = -1;
  std::atomic<pid_t> owned_process_group{-1};
  std::atomic<bool> active_submit{false};
  int input_fd = -1, output_fd = -1, error_fd = -1;
  std::uint64_t request_number = 0;
  std::string stderr_capture;
  std::string output_buffer;
  WorkerInteractionHandler interaction_handler;

  void publish_metadata_locked() {
    std::lock_guard lock(metadata_mutex);
    metadata_snapshot_ = metadata_;
  }

  void ensure_started_locked() {
    if (pid <= 0 || !metadata_.healthy) {
      start_locked();
      return;
    }
    int status = 0;
    if (::waitpid(pid, &status, WNOHANG) == pid) {
      mark_dead_locked();
      throw WorkerTransportError("Worker process exited unexpectedly");
    }
  }

  void spawn_locked() {
    output_buffer.clear();
    stderr_capture.clear();
    int child_input[2] = {-1, -1}, child_output[2] = {-1, -1}, child_error[2] = {-1, -1};
    if (::pipe(child_input) < 0 || ::pipe(child_output) < 0 || ::pipe(child_error) < 0) {
      close_fd(child_input[0]);
      close_fd(child_input[1]);
      close_fd(child_output[0]);
      close_fd(child_output[1]);
      close_fd(child_error[0]);
      close_fd(child_error[1]);
      throw WorkerTransportError("Unable to create worker process pipes");
    }
    try {
      reserve_process_fd(child_input[0]);
      reserve_process_fd(child_input[1]);
      reserve_process_fd(child_output[0]);
      reserve_process_fd(child_output[1]);
      reserve_process_fd(child_error[0]);
      reserve_process_fd(child_error[1]);
    } catch (...) {
      close_fd(child_input[0]);
      close_fd(child_input[1]);
      close_fd(child_output[0]);
      close_fd(child_output[1]);
      close_fd(child_error[0]);
      close_fd(child_error[1]);
      throw;
    }
    const auto close_spawn_fds = [&]() noexcept {
      close_fd(child_input[0]);
      close_fd(child_input[1]);
      close_fd(child_output[0]);
      close_fd(child_output[1]);
      close_fd(child_error[0]);
      close_fd(child_error[1]);
    };
    std::map<std::string, std::string> environment_values;
    for (const auto &name : config.environment_allowlist)
      if (const auto *value = std::getenv(name.c_str()))
        environment_values[name] = value;
    for (const auto &[name, value] : config.environment)
      environment_values[name] = value;
    std::size_t environment_bytes = 0;
    std::vector<std::string> environment_entries;
    environment_entries.reserve(environment_values.size());
    for (const auto &[name, value] : environment_values) {
      if (name.size() > std::numeric_limits<std::size_t>::max() - value.size() - 2 ||
          name.size() + value.size() + 2 > 65536 ||
          environment_bytes > 65536 - (name.size() + value.size() + 2)) {
        close_spawn_fds();
        throw WorkerTransportError("Worker environment exceeds the limit");
      }
      environment_entries.push_back(name + "=" + value);
      environment_bytes += name.size() + value.size() + 2;
    }
    std::vector<char *> environment;
    environment.reserve(environment_entries.size() + 1);
    for (auto &entry : environment_entries)
      environment.push_back(entry.data());
    environment.push_back(nullptr);

    std::vector<char *> arguments;
    arguments.reserve(config.args.size() + 2);
    arguments.push_back(const_cast<char *>(config.executable.c_str()));
    for (auto &arg : config.args)
      arguments.push_back(arg.data());
    arguments.push_back(nullptr);

    posix_spawn_file_actions_t actions;
    posix_spawnattr_t attributes;
    if (::posix_spawn_file_actions_init(&actions) != 0) {
      close_spawn_fds();
      throw WorkerTransportError("Unable to configure worker process");
    }
    if (::posix_spawnattr_init(&attributes) != 0) {
      (void)::posix_spawn_file_actions_destroy(&actions);
      close_spawn_fds();
      throw WorkerTransportError("Unable to configure worker process");
    }
    auto destroy_spawn_state = [&]() noexcept {
      (void)::posix_spawn_file_actions_destroy(&actions);
      (void)::posix_spawnattr_destroy(&attributes);
    };
    const auto add_action = [&](int result) {
      if (result != 0)
        throw WorkerTransportError("Unable to configure worker process pipes");
    };
    const auto add_close_if_distinct = [&](int descriptor, int target) {
      if (descriptor != target)
        add_action(::posix_spawn_file_actions_addclose(&actions, descriptor));
    };
    try {
      add_action(::posix_spawn_file_actions_adddup2(&actions, child_input[0], STDIN_FILENO));
      add_action(::posix_spawn_file_actions_adddup2(&actions, child_output[1], STDOUT_FILENO));
      add_action(::posix_spawn_file_actions_adddup2(&actions, child_error[1], STDERR_FILENO));
      add_close_if_distinct(child_input[0], STDIN_FILENO);
      add_close_if_distinct(child_input[1], STDIN_FILENO);
      add_close_if_distinct(child_output[0], STDOUT_FILENO);
      add_close_if_distinct(child_output[1], STDOUT_FILENO);
      add_close_if_distinct(child_error[0], STDERR_FILENO);
      add_close_if_distinct(child_error[1], STDERR_FILENO);
      short flags = POSIX_SPAWN_SETPGROUP;
      add_action(::posix_spawnattr_setflags(&attributes, flags));
      add_action(::posix_spawnattr_setpgroup(&attributes, 0));
    } catch (...) {
      destroy_spawn_state();
      close_spawn_fds();
      throw;
    }
    const auto spawn_result = ::posix_spawn(&pid, config.executable.c_str(), &actions, &attributes,
                                            arguments.data(), environment.data());
    destroy_spawn_state();
    if (spawn_result != 0) {
      pid = -1;
      close_spawn_fds();
      throw WorkerTransportError("Unable to start worker process");
    }
    close_fd(child_input[0]);
    close_fd(child_output[1]);
    close_fd(child_error[1]);
    input_fd = child_input[1];
    output_fd = child_output[0];
    error_fd = child_error[0];
    owned_process_group.store(pid, std::memory_order_release);
    try {
      nonblocking(input_fd);
      nonblocking(output_fd);
      nonblocking(error_fd);
    } catch (...) {
      terminate_locked();
      throw;
    }
    (void)::setpgid(pid, pid);
  }

  void mark_dead_locked() {
    terminate_process_group(owned_process_group.load(std::memory_order_acquire));
    owned_process_group.store(-1, std::memory_order_release);
    pid = -1;
    close_fd(input_fd);
    close_fd(output_fd);
    close_fd(error_fd);
    metadata_.healthy = false;
    metadata_.status = "failed";
    publish_metadata_locked();
    output_buffer.clear();
  }

  void terminate_locked() noexcept {
    const auto process_group = owned_process_group.load(std::memory_order_acquire);
    if (process_group > 0)
      terminate_process_group(process_group);
    if (pid > 0) {
      const auto deadline = Clock::now() + std::chrono::milliseconds(500);
      int status = 0;
      while (Clock::now() < deadline) {
        const auto result = ::waitpid(pid, &status, WNOHANG);
        if (result == pid)
          break;
        if (result < 0 && errno == ECHILD)
          break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      (void)::waitpid(pid, &status, 0);
    }
    owned_process_group.store(-1, std::memory_order_release);
    pid = -1;
    close_fd(input_fd);
    close_fd(output_fd);
    close_fd(error_fd);
  }

  void write_frame_locked(const std::string &frame, Clock::time_point deadline) {
    if (frame.size() > process_protocol::max_frame_bytes)
      throw WorkerTransportError("Worker request exceeds the frame limit");
    std::string wire = frame + '\n';
    std::size_t offset = 0;
    while (offset < wire.size()) {
      pollfd descriptor{input_fd, POLLOUT, 0};
      if (::poll(&descriptor, 1, remaining_ms(deadline)) <= 0)
        throw WorkerTransportError("Worker request timed out", true);
      if (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL))
        throw WorkerTransportError("Worker process closed its input");
      const auto written = ::write(input_fd, wire.data() + offset, wire.size() - offset);
      if (written > 0)
        offset += static_cast<std::size_t>(written);
      else if (written < 0 && errno != EAGAIN && errno != EINTR)
        throw WorkerTransportError("Unable to write worker request");
    }
  }

  void drain_stderr_locked(short revents) {
    if (!(revents & (POLLIN | POLLHUP | POLLERR)))
      return;
    char buffer[4096];
    for (;;) {
      const auto count = ::read(error_fd, buffer, sizeof(buffer));
      if (count > 0) {
        if (stderr_capture.size() >
            process_protocol::max_stderr_bytes - static_cast<std::size_t>(count))
          throw WorkerTransportError("Worker stderr exceeds the capture limit");
        stderr_capture.append(buffer, static_cast<std::size_t>(count));
      } else if (count < 0 && (errno == EAGAIN || errno == EINTR)) {
        return;
      } else {
        close_fd(error_fd);
        return;
      }
    }
  }

  std::string read_frame_locked(Clock::time_point deadline) {
    char buffer[4096];
    for (;;) {
      const auto newline = output_buffer.find('\n');
      if (newline != std::string::npos) {
        if (newline > process_protocol::max_frame_bytes)
          throw WorkerTransportError("Worker response exceeds the frame limit");
        auto frame = output_buffer.substr(0, newline);
        output_buffer.erase(0, newline + 1);
        if (!frame.empty() && frame.back() == '\r')
          frame.pop_back();
        return frame;
      }
      pollfd descriptors[2] = {{output_fd, POLLIN, 0}, {error_fd, POLLIN, 0}};
      const auto count = ::poll(descriptors, error_fd >= 0 ? 2 : 1, remaining_ms(deadline));
      if (count == 0)
        throw WorkerTransportError("Worker response timed out", true);
      if (count < 0) {
        if (errno == EINTR)
          continue;
        throw WorkerTransportError("Unable to read worker response");
      }
      if (error_fd >= 0)
        drain_stderr_locked(descriptors[1].revents);
      if (descriptors[0].revents & (POLLIN | POLLHUP | POLLERR)) {
        const auto received = ::read(output_fd, buffer, sizeof(buffer));
        if (received > 0) {
          output_buffer.append(buffer, static_cast<std::size_t>(received));
          if (output_buffer.find('\n') == std::string::npos &&
              output_buffer.size() > process_protocol::max_frame_bytes)
            throw WorkerTransportError("Worker response exceeds the frame limit");
        } else if (received == 0) {
          throw WorkerTransportError("Worker response was truncated");
        } else if (errno != EAGAIN && errno != EINTR) {
          throw WorkerTransportError("Unable to read worker response");
        }
      }
      if (pid > 0) {
        int status = 0;
        if (::waitpid(pid, &status, WNOHANG) == pid)
          throw WorkerTransportError("Worker process exited unexpectedly");
      }
    }
  }

  Json request_locked(const std::string &operation, const std::string &job_id,
                      const std::string &external_job_id, const Json &payload,
                      std::uint64_t timeout_ms) {
    const auto request_id = "req-" + std::to_string(++request_number);
    Json request{{"protocol_version", process_protocol::version},
                 {"request_id", request_id},
                 {"operation", operation},
                 {"job_id", job_id},
                 {"external_job_id", external_job_id},
                 {"payload", payload}};
    const auto wire = request.dump();
    if (wire.size() > process_protocol::max_frame_bytes)
      throw WorkerTransportError("Worker request exceeds the frame limit");
    auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    write_frame_locked(wire, deadline);
    for (;;) {
      const auto response_wire = read_frame_locked(deadline);
      const auto response = Json::parse(response_wire, nullptr, false);
      if (response.is_discarded() || !response.is_object())
        throw WorkerTransportError("Worker response is not valid JSON");
      if (!response.contains("protocol_version") ||
          !response.at("protocol_version").is_number_unsigned() ||
          response.at("protocol_version").get<std::uint32_t>() != process_protocol::version)
        throw WorkerTransportError("Worker protocol version is incompatible");
      const auto message_type = response.value("message_type", std::string{"response"});
      if (message_type == "worker_request") {
        deadline = std::max(deadline, Clock::now() +
                                          std::chrono::milliseconds(config.interaction_timeout_ms));
        if (!response.contains("request_id") || !response.at("request_id").is_string() ||
            response.at("request_id").get<std::string>().size() >
                process_protocol::max_interaction_id_bytes)
          throw WorkerTransportError("Worker interaction request id is invalid");
        WorkerInteractionRequest interaction;
        try {
          interaction = response.get<WorkerInteractionRequest>();
        } catch (...) {
          throw WorkerTransportError("Worker interaction request is invalid");
        }
        if (interaction.request_id.empty() || interaction.worker_job_id.empty() ||
            interaction.title.size() > process_protocol::max_interaction_text_bytes ||
            interaction.summary.size() > process_protocol::max_interaction_text_bytes ||
            !interaction.payload.is_object() ||
            interaction.payload.dump().size() > process_protocol::max_interaction_payload_bytes)
          throw WorkerTransportError("Worker interaction request exceeds its limits");
        if (!interaction_handler)
          throw WorkerTransportError("Worker interaction handler is unavailable");
        const auto answer = interaction_handler(interaction);
        if (answer.request_id != interaction.request_id)
          throw WorkerTransportError("Worker interaction response id does not match");
        Json response_message{{"protocol_version", process_protocol::version},
                              {"message_type", "worker_response"},
                              {"request_id", answer.request_id},
                              {"decision", answer.state},
                              {"payload", answer.payload},
                              {"reason", answer.reason}};
        if (response_message.dump().size() > process_protocol::max_frame_bytes)
          throw WorkerTransportError("Worker interaction response exceeds the frame limit");
        write_frame_locked(response_message.dump(), deadline);
        continue;
      }
      if (message_type != "response")
        throw WorkerTransportError("Worker sent an unsolicited protocol message");
      if (!response.contains("request_id") || !response.at("request_id").is_string() ||
          response.at("request_id").get<std::string>() != request_id)
        throw WorkerTransportError("Worker response request id does not match");
      if (!response.contains("ok") || !response.at("ok").is_boolean())
        throw WorkerTransportError("Worker response has no valid success flag");
      if (!response.at("ok").get<bool>())
        throw WorkerTransportError("Worker rejected protocol request");
      return response;
    }
  }

  Json request_response_locked(const std::string &operation, const std::string &job_id,
                               const std::string &external_job_id, const Json &payload,
                               std::uint64_t timeout_ms) {
    try {
      return request_locked(operation, job_id, external_job_id, payload, timeout_ms);
    } catch (...) {
      // A protocol or pipe failure makes this child unusable. Never reuse it
      // for an ambiguous external submission.
      terminate_locked();
      metadata_.healthy = false;
      metadata_.status = "failed";
      publish_metadata_locked();
      throw;
    }
  }

  static WorkerJobState response_state(const Json &response) {
    if (!response.contains("state"))
      throw WorkerTransportError("Worker response has no state");
    try {
      return response.at("state").get<WorkerJobState>();
    } catch (...) {
      throw WorkerTransportError("Worker response has an invalid state");
    }
  }

  static WorkerUsage response_usage(const Json &response) {
    if (!response.contains("usage") || response.at("usage").is_null())
      return {};
    try {
      return response.at("usage").get<WorkerUsage>();
    } catch (...) {
      throw WorkerTransportError("Worker response has invalid usage");
    }
  }

  static Json response_payload(const Json &response) {
    const auto payload = response.value("payload", Json::object());
    if (payload.dump().size() > process_protocol::max_frame_bytes)
      throw WorkerTransportError("Worker payload exceeds the frame limit");
    return payload;
  }

  static void validate_response_metadata(const Json &response) {
    const auto metadata = response.value("metadata", Json::object());
    if (!metadata.is_object() || metadata.dump().size() > process_protocol::max_metadata_bytes)
      throw WorkerTransportError("Worker response metadata exceeds the limit");
    if (response.contains("artifacts") && !response.at("artifacts").is_array())
      throw WorkerTransportError("Worker artifact references are invalid");
    const auto artifacts = response.value("artifacts", std::vector<Json>{});
    if (artifacts.size() > process_protocol::max_artifact_references)
      throw WorkerTransportError("Worker response has too many artifact references");
    for (const auto &artifact : artifacts)
      if (!artifact.is_object() || artifact.dump().size() > process_protocol::max_metadata_bytes)
        throw WorkerTransportError("Worker artifact reference is invalid");
    if (response.contains("error") && !response.at("error").is_string())
      throw WorkerTransportError("Worker error is invalid");
    const auto error = response.value("error", std::string{});
    if (error.size() > max_error_bytes)
      throw WorkerTransportError("Worker error exceeds the limit");
  }

  static WorkerSubmission parse_submission_locked(const std::string &, const Json &response) {
    try {
      validate_response_metadata(response);
      WorkerSubmission result;
      result.external_job_id = response.value("external_job_id", std::string{});
      if (!bounded_text(result.external_job_id, max_id_bytes))
        throw WorkerTransportError("Worker returned an invalid external job id");
      result.state = response_state(response);
      result.metadata = response.value("metadata", Json::object());
      result.result = response_payload(response);
      result.artifacts = response.value("artifacts", std::vector<Json>{});
      result.error = response.value("error", std::string{});
      result.usage = response_usage(response);
      return result;
    } catch (const WorkerTransportError &) {
      throw;
    } catch (...) {
      throw WorkerTransportError("Worker submission response has invalid fields");
    }
  }

  WorkerStatus status_operation(const std::string &operation, const std::string &external_job_id) {
    std::lock_guard lock(mutex);
    ensure_started_locked();
    const auto response = request_response_locked(operation, "", external_job_id, Json::object(),
                                                  config.request_timeout_ms);
    try {
      validate_response_metadata(response);
      WorkerStatus result;
      result.state = response_state(response);
      result.result = response_payload(response);
      result.metadata = response.value("metadata", Json::object());
      result.artifacts = response.value("artifacts", std::vector<Json>{});
      result.error = response.value("error", std::string{});
      result.usage = response_usage(response);
      return result;
    } catch (const WorkerTransportError &) {
      throw;
    } catch (...) {
      throw WorkerTransportError("Worker status response has invalid fields");
    }
  }
};

ProcessWorkerTransport::ProcessWorkerTransport(std::string id, ProcessWorkerConfig config)
    : impl_(std::make_unique<Impl>(std::move(id), std::move(config))) {}
ProcessWorkerTransport::~ProcessWorkerTransport() noexcept = default;
WorkerMetadata ProcessWorkerTransport::metadata() const {
  return impl_->metadata();
}
WorkerSubmission ProcessWorkerTransport::submit(const WorkerRequest &request) {
  return impl_->submit(request);
}
WorkerStatus ProcessWorkerTransport::status(const std::string &external_job_id) {
  return impl_->status(external_job_id);
}
WorkerStatus ProcessWorkerTransport::result(const std::string &external_job_id) {
  return impl_->result(external_job_id);
}
bool ProcessWorkerTransport::cancel(const std::string &external_job_id) {
  return impl_->cancel(external_job_id);
}
bool ProcessWorkerTransport::cancel_pending(const std::string &job_id) {
  return impl_->cancel_pending(job_id);
}
void ProcessWorkerTransport::start() {
  impl_->start();
}
void ProcessWorkerTransport::stop() noexcept {
  impl_->stop();
}
void ProcessWorkerTransport::set_interaction_handler(WorkerInteractionHandler handler) {
  impl_->set_interaction_handler(std::move(handler));
}
} // namespace laso
