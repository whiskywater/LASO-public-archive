#include <charconv>
#include <laso/api/api.hpp>
#include <laso/pipeline/parser.hpp>
#include <regex>
#include <set>

namespace laso {
// NOLINTBEGIN(bugprone-exception-escape): this noexcept boundary converts all exceptions to HTTP
// responses.
ApiResponse Api::handle(const std::string &method, const std::string &target,
                        const std::string &body, const std::string &credential) noexcept {
  try {
    if (target.size() > 2048 || body.size() > max_document_bytes || credential.size() > 8192)
      return {413, {{"error", "Request exceeds size limit"}}};
    auto actor = identity_.authenticate(credential);
    if (!identity_.authorize({actor, method, target}))
      return {403, {{"error", "Access denied"}}};
    auto input = body.empty() ? Json::object() : Json::parse(body);
    if (!input.is_object())
      return {400, {{"error", "Request body must be a JSON object"}}};
    auto path = target;
    std::size_t limit = 50, offset = 0;
    auto query = path.find('?');
    if (query != std::string::npos) {
      auto parameters = path.substr(query + 1);
      path.resize(query);
      std::set<std::string> seen;
      while (!parameters.empty()) {
        auto end = parameters.find('&');
        auto part = parameters.substr(0, end);
        auto equal = part.find('=');
        if (equal == std::string::npos)
          throw Error(ErrorCode::Validation, "Malformed pagination query");
        auto key = part.substr(0, equal), value = part.substr(equal + 1);
        std::size_t number = 0;
        auto parsed = std::from_chars(value.data(), value.data() + value.size(), number);
        if (!seen.insert(key).second || parsed.ec != std::errc{} ||
            parsed.ptr != value.data() + value.size())
          throw Error(ErrorCode::Validation, "Invalid pagination value");
        if (key == "limit" && number >= 1 && number <= 100)
          limit = number;
        else if (key == "offset" && number <= 100000000)
          offset = number;
        else
          throw Error(ErrorCode::Validation, "Pagination limit exceeded or unknown parameter");
        if (end == std::string::npos)
          break;
        parameters.erase(0, end + 1);
      }
    }
    auto response = route(method, path, input, actor, limit, offset);
    if (response.body.dump().size() > std::size_t{4} * 1024 * 1024)
      return {413, {{"error", "Response exceeds limit; request a smaller page"}}};
    return response;
  } catch (const Error &e) {
    unsigned status = 400;
    if (e.code == ErrorCode::NotFound)
      status = 404;
    else if (e.code == ErrorCode::Conflict)
      status = 409;
    else if (e.code == ErrorCode::Capacity)
      status = 429;
    else if (e.code == ErrorCode::Policy)
      status = 403;
    else if (e.code == ErrorCode::Storage)
      status = 503;
    return {status, {{"error", status == 503 ? "Storage unavailable" : e.what()}}};
  } catch (const Json::exception &) {
    return {400, {{"error", "Malformed JSON request"}}};
  } catch (...) {
    return {500, {{"error", "Internal service error"}}};
  }
}
// NOLINTEND(bugprone-exception-escape)
ApiResponse Api::route(const std::string &method, const std::string &target, const Json &body,
                       const Actor &actor, std::size_t limit, std::size_t offset) {
  if (method == "GET" && target == "/api/v1/health")
    return {200, {{"status", "ok"}, {"mode", "local-development"}}};
  if (method == "GET" && target == "/api/v1/version")
    return {200, {{"version", version}, {"pipeline_schema", 1}, {"plugin_abi", 1}}};
  if (method == "GET" && target == "/api/v1/providers")
    return {200, service_.providers()};
  if (method == "GET" && target == "/api/v1/tools")
    return {200, service_.tools()};
  if (method == "GET" && target == "/api/v1/plugins")
    return {200, service_.plugins()};
  if (method == "GET" && target == "/api/v1/instances")
    return {200, service_.instances()};
  std::smatch match;
  static const std::regex route_pattern("/api/v1/"
                                        "(pipelines|runs|approvals|worker-requests|schedules|"
                                        "triggers|event-sources|workers|worker-jobs)(?:/"
                                        "([A-Za-z0-9_.@-]{1,128}))?(?:/"
                                        "(runs|cancel|resume|events|attempts|messages|approve|"
                                        "reject|respond|answer|deny|enable|disable))?");
  if (!std::regex_match(target, match, route_pattern))
    return {404, {{"error", "Endpoint not found"}}};
  auto collection = match[1].str(), id = match[2].str(), action = match[3].str();
  if (collection == "event-sources") {
    if (method == "GET" && id.empty())
      return {200, service_.event_sources()};
    if (method == "GET" && action.empty())
      return {200, service_.event_source(id)};
    if (method == "POST" && !id.empty() && (action == "enable" || action == "disable")) {
      service_.set_event_source_enabled(id, action == "enable");
      return {202, service_.event_source(id)};
    }
    return {405, {{"error", "Method not supported"}}};
  }
  if (collection == "workers") {
    if (method == "GET" && id.empty())
      return {200, service_.workers()};
    if (method == "GET" && action.empty())
      return {200, service_.worker(id)};
    return {405, {{"error", "Method not supported"}}};
  }
  if (collection == "worker-jobs") {
    if (method == "GET" && id.empty())
      return {200, service_.worker_jobs("", limit, offset)};
    if (method == "GET" && action.empty())
      return {200, service_.worker_job(id)};
    if (method == "POST" && action == "cancel") {
      service_.cancel_worker_job(id);
      return {202, service_.worker_job(id)};
    }
    return {405, {{"error", "Method not supported"}}};
  }
  if (collection == "worker-requests") {
    if (method == "GET" && id.empty())
      return {200, service_.worker_interactions("", limit, offset)};
    if (method == "GET" && action.empty())
      return {200, service_.worker_interaction(id)};
    if (method == "POST" && !id.empty() &&
        (action == "respond" || action == "answer" || action == "approve" || action == "deny" ||
         action == "cancel")) {
      const auto state = action == "approve" ? WorkerInteractionState::Approved
                         : action == "answer" || action == "respond"
                             ? WorkerInteractionState::Answered
                         : action == "cancel" ? WorkerInteractionState::Cancelled
                                              : WorkerInteractionState::Denied;
      service_.resolve_worker_interaction(id, state, body.value("payload", Json::object()),
                                          actor.id, body.value("reason", std::string{}));
      return {202, service_.worker_interaction(id)};
    }
    return {405, {{"error", "Method not supported"}}};
  }
  auto kind = collection == "pipelines"   ? RecordKind::Pipeline
              : collection == "runs"      ? RecordKind::Run
              : collection == "approvals" ? RecordKind::Approval
              : collection == "schedules" ? RecordKind::Schedule
                                          : RecordKind::Trigger;
  if (method == "GET" && id.empty())
    return {200, service_.list(kind, "", limit, offset)};
  if (method == "GET" && action.empty())
    return {200, collection == "runs" ? service_.run_view(id) : service_.get(kind, id)};
  if (method == "POST" && collection == "pipelines" && id.empty())
    return {201, service_.register_pipeline(body.at("yaml").get<std::string>())};
  if (method == "POST" && collection == "schedules" && id.empty())
    return {201, service_.create_schedule(body)};
  if (method == "POST" && collection == "triggers" && id.empty())
    return {201, service_.create_trigger(body)};
  if (method == "PATCH" && collection == "schedules" && !id.empty() && action.empty())
    return {200, service_.update_schedule(id, body)};
  if (method == "PATCH" && collection == "triggers" && !id.empty() && action.empty())
    return {200, service_.update_trigger(id, body)};
  if (method == "DELETE" && collection == "schedules" && !id.empty() && action.empty()) {
    service_.delete_schedule(id);
    return {202, {{"id", id}, {"deleted", true}}};
  }
  if (method == "DELETE" && collection == "triggers" && !id.empty() && action.empty()) {
    service_.delete_trigger(id);
    return {202, {{"id", id}, {"deleted", true}}};
  }
  if (method == "POST" && collection == "schedules" && !id.empty() &&
      (action == "enable" || action == "disable")) {
    service_.set_schedule_enabled(id, action == "enable");
    return {202, service_.get(RecordKind::Schedule, id)};
  }
  if (method == "POST" && collection == "triggers" && !id.empty() &&
      (action == "enable" || action == "disable")) {
    service_.set_trigger_enabled(id, action == "enable");
    return {202, service_.get(RecordKind::Trigger, id)};
  }
  if (method == "POST" && collection == "pipelines" && action == "runs")
    return {202,
            {{"id", service_.start(id, body.value("input", Json::object()), actor.id, false,
                                   Json::object(), body.value("metadata", Json::object()))}}};
  if (collection == "runs" && !id.empty()) {
    if (method == "GET" && (action == "events" || action == "attempts" || action == "messages")) {
      (void)service_.get(RecordKind::Run, id);
      return {200, service_.list(action == "events"     ? RecordKind::Event
                                 : action == "attempts" ? RecordKind::Attempt
                                                        : RecordKind::Message,
                                 id, limit, offset)};
    }
    if (method == "POST" && action == "cancel") {
      service_.runtime().cancel(id);
      return {202, {{"id", id}, {"action", "cancellation requested"}}};
    }
    if (method == "POST" && action == "resume") {
      service_.runtime().resume(id);
      return {202, {{"id", id}}};
    }
  }
  if (method == "POST" && collection == "approvals" &&
      (action == "approve" || action == "reject")) {
    service_.runtime().decide(id, action == "approve", actor.id,
                              body.value("comment", std::string{}));
    return {202, service_.get(RecordKind::Approval, id)};
  }
  return {405, {{"error", "Method not supported"}}};
}
} // namespace laso
