#include <functional>
#include <iomanip>
#include <laso/application/service.hpp>
#include <laso/pipeline/parser.hpp>
#include <limits>
#include <set>
#include <sstream>

namespace laso {
namespace {
Config checked(Config c) {
  c.validate();
  if (c.execution_mode != "single" && c.execution_mode != "multi_instance")
    throw Error(ErrorCode::Configuration, "Unsupported LASO execution mode");
  return c;
}
std::vector<Json> all_pipeline_records(const Storage &storage) {
  constexpr std::size_t page_size = 10000;
  std::vector<Json> result;
  for (std::size_t offset = 0;;) {
    auto page = storage.list(RecordKind::Pipeline, "", page_size, offset);
    result.insert(result.end(), page.begin(), page.end());
    if (page.size() < page_size)
      return result;
    if (offset > std::numeric_limits<std::size_t>::max() - page_size)
      throw Error(ErrorCode::Storage, "Pipeline registry is too large to inspect");
    offset += page_size;
  }
}
std::string definition_fingerprint(const std::string &text) {
  // This is an identity aid, not a security primitive.  The immutable source
  // remains in the record and is compared on an idempotent registration.
  std::uint64_t hash = 1469598103934665603ULL;
  for (const auto byte : text) {
    hash ^= static_cast<unsigned char>(byte);
    hash *= 1099511628211ULL;
  }
  std::ostringstream out;
  out << std::hex << std::setw(16) << std::setfill('0') << hash;
  return out.str();
}
bool same_source(const Json &record, const std::string &yaml) {
  return record.contains("yaml") && record.at("yaml").is_string() &&
         record.at("yaml").get<std::string>() == yaml;
}
std::unique_ptr<Coordination> make_coordination(const Config &config,
                                                const std::string &instance_id) {
  if (config.execution_mode != "multi_instance")
    return nullptr;
#ifdef LASO_HAS_POSTGRES
  return create_coordination(
      {"postgres", config.postgres_dsn, config.postgres_schema,
       config.postgres_pool_min_connections, config.postgres_pool_max_connections,
       config.postgres_pool_acquisition_timeout_ms, config.instance_stale_after_ms},
      instance_id);
#else
  (void)instance_id;
  throw Error(ErrorCode::Configuration,
              "multi_instance execution requires PostgreSQL support at build time");
#endif
}

std::unique_ptr<ArtifactStore> make_artifact_store(const Config &config, Storage &storage) {
  const auto root =
      config.artifact_root.empty() ? config.data_dir / "artifacts" : config.artifact_root;
  const ArtifactStoreLimits limits{config.max_artifact_bytes, config.max_artifact_temp_bytes,
                                   config.artifact_cleanup_grace_seconds};
  if (config.artifact_backend == "s3") {
#ifdef LASO_HAS_S3
    return std::make_unique<S3ArtifactStore>(
        S3ArtifactStoreConfig{config.artifact_s3_endpoint, config.artifact_s3_bucket,
                              config.artifact_s3_region, config.artifact_s3_prefix,
                              root / "scratch", config.artifact_s3_connect_timeout_ms,
                              config.artifact_s3_request_timeout_ms, config.artifact_s3_max_retries,
                              config.artifact_s3_path_style, config.artifact_s3_allow_http,
                              config.artifact_s3_ca_file},
        storage, limits);
#else
    throw Error(ErrorCode::Configuration,
                "S3 artifact storage requires a build with LASO_ENABLE_S3=ON");
#endif
  }
  if (!config.artifact_service_url.empty())
    return std::make_unique<RemoteArtifactStore>(config.artifact_service_url,
                                                 config.artifact_service_token, root / "cache",
                                                 storage, limits);
  return std::make_unique<LocalArtifactStore>(root, storage, limits);
}
} // namespace
Service::Service(asio::io_context &io, Config config)
    : config_(checked(std::move(config))), instance_id_(generate_service_instance_id()),
      lease_(config_.storage_backend == "sqlite" ? std::make_unique<ProcessLease>(config_.db_path)
                                                 : nullptr),
      storage_(create_storage({config_.storage_backend, config_.db_path, config_.postgres_dsn,
                               config_.postgres_schema, config_.postgres_pool_min_connections,
                               config_.postgres_pool_max_connections,
                               config_.postgres_pool_acquisition_timeout_ms,
                               config_.execution_mode == "multi_instance"})),
      coordination_(make_coordination(config_, instance_id_)),
      policy_(config_.rules, config_.allow_network), schemas_(config_.schema_roots),
      ingress_(*storage_, events_, schemas_, config_.max_event_trigger_depth,
               config_.max_pending_scheduler_launches, 32),
      worker_manager_(std::make_shared<WorkerManager>(
          *storage_, worker_registry_, policy_, config_.max_worker_jobs,
          config_.max_worker_jobs_per_worker, 16, config_.max_worker_wall_time_ms,
          config_.max_worker_tokens_per_run, config_.max_worker_cost_units_per_run)),
      plugins_(
          tools_, providers_, worker_registry_,
          [this](const std::string &source, const std::string &plugin, const std::string &component,
                 const std::string &schema, const std::string &event_json) {
            return ingress_.submit(source, plugin, component, schema, event_json);
          },
          [this](const EventSourceInfo &info) {
            storage_->commit({{RecordKind::EventSource, info.id, "", Json(info)}});
          },
          [this](const std::string &id) -> std::optional<EventSourceInfo> {
            try {
              return storage_->get(RecordKind::EventSource, id).get<EventSourceInfo>();
            } catch (const Error &error) {
              if (error.code == ErrorCode::NotFound)
                return std::nullopt;
              throw;
            }
          }),
      artifacts_(make_artifact_store(config_, *storage_)),
      runtime_(io, config_,
               {*storage_, events_, providers_, tools_, functions_, nodes_, policy_, schemas_,
                worker_manager_,
                [this](const std::string &reference) { return resolve_pipeline(reference); },
                coordination_.get(), artifacts_.get(), instance_id_,
                config_.data_dir / "distributed-workspaces"}),
      scheduler_(
          io, *storage_,
          [this](const LaunchRequest &request) {
            const auto reference =
                pipeline_reference(request.pipeline_id, request.pipeline_version);
            const auto actor = request.origin.value("initiation_type", std::string{}) == "event"
                                   ? "event-trigger"
                                   : "scheduler";
            return start(reference, request.input, actor, false, request.origin);
          },
          [this](const Event &event) {
            events_.publish(event);
            log_event(event);
          },
          std::make_shared<SystemClock>(), config_.max_pending_scheduler_launches,
          config_.max_event_trigger_depth, config_.max_event_trigger_deliveries) {
  configure_logging(config_);
  providers_.add("mock", std::make_shared<MockModelProvider>());
  if (!config_.local_openai_endpoint.empty())
    providers_.add("local-openai",
                   std::make_shared<LocalOpenAICompatibleProvider>(config_.local_openai_endpoint));
  tools_.add("echo", std::make_shared<EchoTool>());
  register_functions(functions_);
  for (const auto &[id, process_config] : config_.process_workers) {
    auto transport = std::make_shared<ProcessWorkerTransport>(id, process_config);
    worker_registry_.add(id, transport);
    process_workers_.push_back(transport);
    transport->set_interaction_handler(
        [manager = worker_manager_](const WorkerInteractionRequest &request) {
          return manager->handle_interaction(request);
        });
    transport->start();
  }
  plugins_.discover(config_.plugin_dirs, config_.event_sources, config_.worker_plugins);
  for (const auto &source : plugins_.event_sources())
    if (!source.value("event_schema", std::string{}).empty())
      schemas_.validate_declaration(source.at("event_schema").get<std::string>());
  for (const auto &worker : plugins_.workers())
    if (!worker.value("event_schema", std::string{}).empty())
      schemas_.validate_declaration(worker.at("event_schema").get<std::string>());
  recover_history();
  runtime_.start_distributed();
  events_.subscribe(worker_manager_);
  events_.subscribe(scheduler_.event_subscriber());
  scheduler_.start();
  plugins_.start_event_sources();
  plugins_.start_workers();
}
Service::~Service() noexcept {
  shutdown();
}
Json Service::register_pipeline(const std::string &yaml) {
  auto extensions = nodes_.names();
  auto p = parse_pipeline(yaml, {extensions.begin(), extensions.end()});
  if (!p.input_schema.empty())
    schemas_.validate_declaration(p.input_schema);
  if (!p.output_schema.empty())
    schemas_.validate_declaration(p.output_schema);
  for (const auto &[id, node] : p.nodes) {
    if (!node.input_schema.empty())
      schemas_.validate_declaration(node.input_schema);
    if (!node.output_schema.empty())
      schemas_.validate_declaration(node.output_schema);
    if (!node.schema.empty())
      schemas_.validate_declaration(node.schema);
  }

  const auto key = pipeline_reference(p.name, p.version);
  auto existing = Json();
  bool has_existing = false;
  try {
    existing = storage_->get(RecordKind::Pipeline, key);
    has_existing = true;
  } catch (const Error &error) {
    if (error.code != ErrorCode::NotFound)
      throw;
  }
  // Read records written by the pre-versioned registry as the v1 identity.
  if (!has_existing && p.version == 1) {
    try {
      existing = storage_->get(RecordKind::Pipeline, p.name);
      has_existing = true;
    } catch (const Error &error) {
      if (error.code != ErrorCode::NotFound)
        throw;
    }
  }
  if (has_existing) {
    if (!same_source(existing, yaml))
      throw Error(ErrorCode::Conflict, "Pipeline revision is immutable",
                  {{"pipeline", key}, {"reason", "definition differs"}});
    return existing;
  }

  const auto records = all_pipeline_records(*storage_);
  const auto candidate_key = key;
  auto revisions = [&](const std::string &name) {
    std::vector<Json> found;
    for (const auto &record : records)
      if (record.value("name", std::string{}) == name)
        found.push_back(record);
    if (p.name == name)
      found.push_back({{"id", candidate_key},
                       {"name", p.name},
                       {"version", p.version},
                       {"yaml", yaml},
                       {"resolved_subpipelines", Json::object()}});
    return found;
  };
  auto resolve_reference = [&](const std::string &reference) {
    const auto parsed = parse_pipeline_reference(reference);
    if (parsed.explicit_version) {
      const auto resolved = pipeline_reference(parsed.name, parsed.version);
      if (resolved == candidate_key)
        return resolved;
      for (const auto &record : records)
        if (record.value("id", std::string{}) == resolved ||
            (record.value("name", std::string{}) == parsed.name &&
             record.value("version", 1U) == parsed.version))
          return resolved;
      throw Error(ErrorCode::NotFound, "Referenced pipeline revision is not registered",
                  {{"pipeline", resolved}});
    }
    auto found = revisions(parsed.name);
    if (found.empty())
      throw Error(ErrorCode::NotFound, "Referenced pipeline is not registered",
                  {{"pipeline", parsed.name}});
    if (found.size() != 1)
      throw Error(ErrorCode::Conflict, "Unversioned pipeline reference is ambiguous",
                  {{"pipeline", parsed.name}, {"reason", "use name@version"}});
    return pipeline_reference(parsed.name, found.front().value("version", 1U));
  };
  for (const auto &[node_id, node] : p.nodes)
    if (node.type == "subpipeline")
      p.resolved_subpipelines[node_id] = resolve_reference(node.binding);

  std::set<std::string> active, visited;
  std::function<void(const std::string &)> visit = [&](const std::string &revision) {
    if (!active.insert(revision).second)
      throw Error(ErrorCode::Validation, "Recursive subpipeline dependency",
                  {{"pipeline", revision}});
    if (!visited.insert(revision).second) {
      active.erase(revision);
      return;
    }
    Json record;
    if (revision == candidate_key) {
      record = {{"yaml", yaml}, {"resolved_subpipelines", p.resolved_subpipelines}};
    } else {
      for (const auto &item : records)
        if (item.value("id", std::string{}) == revision ||
            (item.value("name", std::string{}) + "@" + std::to_string(item.value("version", 1U))) ==
                revision)
          record = item;
      if (record.is_null())
        throw Error(ErrorCode::NotFound, "Referenced pipeline revision is not registered",
                    {{"pipeline", revision}});
    }
    std::map<std::string, std::string> dependencies;
    if (record.contains("resolved_subpipelines")) {
      dependencies = record.at("resolved_subpipelines").get<std::map<std::string, std::string>>();
    } else {
      auto child = parse_pipeline(record.at("yaml").get<std::string>(),
                                  {extensions.begin(), extensions.end()});
      for (const auto &[node_id, node] : child.nodes)
        if (node.type == "subpipeline")
          dependencies[node_id] = resolve_reference(node.binding);
    }
    for (const auto &[node_id, child] : dependencies) {
      (void)node_id;
      visit(child);
    }
    active.erase(revision);
  };
  visit(candidate_key);

  Json record = {{"id", key},
                 {"name", p.name},
                 {"version", p.version},
                 {"laso", p.schema_version},
                 {"yaml", yaml},
                 {"definition_fingerprint", definition_fingerprint(yaml)},
                 {"resolved_subpipelines", p.resolved_subpipelines},
                 {"registered_at", timestamp()}};
  Event e;
  e.pipeline_id = p.name;
  e.metadata["pipeline_version"] = p.version;
  e.type = "pipeline.registered";
  try {
    storage_->commit(
        {{RecordKind::Pipeline, key, "", record}, {RecordKind::Event, e.id, "", Json(e)}});
  } catch (const Error &error) {
    // Another registration may have won the SQLite transaction between the
    // initial lookup and this commit. Preserve idempotence for the same
    // immutable source while still rejecting a conflicting revision.
    if (error.code != ErrorCode::Conflict)
      throw;
    try {
      const auto concurrent = storage_->get(RecordKind::Pipeline, key);
      if (same_source(concurrent, yaml))
        return concurrent;
    } catch (const Error &lookup_error) {
      if (lookup_error.code != ErrorCode::NotFound)
        throw;
    }
    throw;
  }
  events_.publish(e);
  return record;
}
std::string Service::start(const std::string &name_or_path, const Json &input,
                           const std::string &actor, bool allow_file, Json origin,
                           Json message_metadata) {
  std::string name = name_or_path;
  if (allow_file && std::filesystem::is_regular_file(name_or_path))
    name = register_pipeline(read_document(name_or_path)).at("id").get<std::string>();
  auto extensions = nodes_.names();
  auto record = pipeline_record(name);
  auto p =
      parse_pipeline(record.at("yaml").get<std::string>(), {extensions.begin(), extensions.end()});
  if (record.contains("resolved_subpipelines"))
    p.resolved_subpipelines =
        record.at("resolved_subpipelines").get<std::map<std::string, std::string>>();
  return runtime_.run(p, input, actor, "", "", 0, "", std::move(origin),
                      std::move(message_metadata));
}

AgentSession Service::create_session(const std::string &pipeline_id) {
  (void)pipeline_record(pipeline_id);
  AgentSession session;
  session.pipeline_id = pipeline_id;
  storage_->commit({{RecordKind::AgentSession, session.id, session.id, Json(session)}});
  return session;
}
AgentSession Service::agent_session(const std::string &id) const {
  return storage_->get(RecordKind::AgentSession, id).get<AgentSession>();
}
void Service::close_session(const std::string &id) {
  Event event;
  event.run_id = id;
  event.type = "session.closed";
  storage_->close_agent_session(id, Json(event));
  const auto session = agent_session(id);
  if (session.state == "closing" && !session.active_run_id.empty()) {
    try {
      runtime_.cancel(session.active_run_id);
    } catch (const Error &error) {
      if (error.code != ErrorCode::Conflict && error.code != ErrorCode::NotFound)
        throw;
      const auto current = agent_session(id);
      if (current.state == "closing" && current.active_run_id == session.active_run_id)
        throw;
    }
  }
}
Json Service::submit_session_turn(const std::string &id, const std::string &idempotency_key,
                                  const Json &input) {
  if (idempotency_key.empty() || idempotency_key.size() > 512 || input.dump().size() > 1024 * 1024)
    throw Error(ErrorCode::Validation, "Invalid session input");
  const auto session = agent_session(id);
  std::uint64_t hash = 1469598103934665603ULL;
  for (const auto byte : idempotency_key) {
    hash ^= static_cast<unsigned char>(byte);
    hash *= 1099511628211ULL;
  }
  std::ostringstream turn_id;
  turn_id << id << "-turn-" << std::hex << hash;
  Json turn{{"idempotency_key", idempotency_key},
            {"input", input},
            {"state", "queued"},
            {"accepted_at", timestamp()},
            {"pipeline_id", session.pipeline_id}};
  Event event;
  event.run_id = id;
  event.type = "input.accepted";
  storage_->submit_session_turn(id, turn_id.str(), turn, Json(event));
  const auto accepted = storage_->get(RecordKind::SessionTurn, turn_id.str());
  try {
    runtime_.dispatch_session(id);
  } catch (...) {
    log_diagnostic("service.session_dispatch_deferred");
  }
  return accepted;
}
std::vector<Json> Service::session_events(const std::string &id, std::uint64_t after,
                                          std::size_t limit) const {
  (void)agent_session(id);
  return storage_->session_events(id, after, limit);
}

Json Service::create_schedule(const Json &spec) {
  auto schedule = parse_schedule_spec(spec);
  const auto raw_pipeline = schedule.pipeline_id;
  auto reference = parse_pipeline_reference(raw_pipeline);
  if (reference.explicit_version) {
    schedule.pipeline_id = reference.name;
    schedule.pipeline_version = reference.version;
  }
  const auto pipeline =
      reference.explicit_version
          ? pipeline_record(pipeline_reference(schedule.pipeline_id, schedule.pipeline_version))
          : pipeline_record(raw_pipeline);
  schedule.pipeline_id = pipeline.at("name").get<std::string>();
  schedule.pipeline_version = pipeline.at("version").get<unsigned>();
  if (schedule.next_due_at.empty()) {
    if (!schedule.at.empty())
      schedule.next_due_at = schedule.at;
    else
      schedule.next_due_at = next_schedule_due(schedule, timestamp());
  }
  validate_schedule(schedule);
  return scheduler_.create_schedule(std::move(schedule));
}

Json Service::update_schedule(const std::string &id, const Json &spec) {
  auto schedule = get(RecordKind::Schedule, id).get<ScheduleDefinition>();
  if (!spec.is_object())
    throw Error(ErrorCode::Validation, "Schedule update must be a JSON object");
  if (spec.contains("name"))
    schedule.name = spec.at("name").get<std::string>();
  if (spec.contains("type"))
    schedule.type = spec.at("type").get<std::string>();
  if (spec.contains("at"))
    schedule.at = spec.at("at").get<std::string>();
  if (spec.contains("cron"))
    schedule.cron = spec.at("cron").get<std::string>();
  if (spec.contains("interval_ms"))
    schedule.interval_ms = spec.at("interval_ms").is_string()
                               ? spec.at("interval_ms").get<std::string>()
                               : std::to_string(spec.at("interval_ms").get<std::int64_t>());
  if (spec.contains("input"))
    schedule.input = spec.at("input");
  if (spec.contains("misfire_policy"))
    schedule.misfire_policy = spec.at("misfire_policy").get<std::string>();
  if (spec.contains("overlap_policy"))
    schedule.overlap_policy = spec.at("overlap_policy").get<std::string>();
  if (spec.contains("enabled"))
    schedule.enabled = spec.at("enabled").get<bool>();
  if (spec.contains("pipeline") || spec.contains("pipeline_id") ||
      spec.contains("pipeline_version") || spec.contains("version")) {
    const auto raw = spec.value("pipeline", spec.value("pipeline_id", schedule.pipeline_id));
    const auto parsed = parse_pipeline_reference(raw);
    schedule.pipeline_id = parsed.explicit_version ? parsed.name : raw;
    if (parsed.explicit_version)
      schedule.pipeline_version = parsed.version;
    else if (spec.contains("pipeline_version"))
      schedule.pipeline_version = spec.at("pipeline_version").get<unsigned>();
    else if (spec.contains("version"))
      schedule.pipeline_version = spec.at("version").get<unsigned>();
  }
  if (spec.contains("next_due_at"))
    schedule.next_due_at = spec.at("next_due_at").get<std::string>();
  else if (spec.contains("at") || spec.contains("cron") || spec.contains("interval_ms"))
    schedule.next_due_at =
        schedule.type == "one_time" ? schedule.at : next_schedule_due(schedule, timestamp());
  const auto pipeline =
      pipeline_record(pipeline_reference(schedule.pipeline_id, schedule.pipeline_version));
  schedule.pipeline_id = pipeline.at("name").get<std::string>();
  schedule.pipeline_version = pipeline.at("version").get<unsigned>();
  return scheduler_.update_schedule(std::move(schedule));
}

void Service::set_schedule_enabled(const std::string &id, bool enabled) {
  scheduler_.set_schedule_enabled(id, enabled);
}
void Service::delete_schedule(const std::string &id) {
  scheduler_.delete_schedule(id);
}

Json Service::create_trigger(const Json &spec) {
  auto trigger = parse_trigger_spec(spec);
  const auto parsed = parse_pipeline_reference(trigger.pipeline_id);
  if (parsed.explicit_version) {
    trigger.pipeline_id = parsed.name;
    trigger.pipeline_version = parsed.version;
  }
  const auto pipeline =
      pipeline_record(pipeline_reference(trigger.pipeline_id, trigger.pipeline_version));
  trigger.pipeline_id = pipeline.at("name").get<std::string>();
  trigger.pipeline_version = pipeline.at("version").get<unsigned>();
  return scheduler_.create_trigger(std::move(trigger));
}

Json Service::update_trigger(const std::string &id, const Json &spec) {
  auto trigger = get(RecordKind::Trigger, id).get<TriggerDefinition>();
  if (!spec.is_object())
    throw Error(ErrorCode::Validation, "Trigger update must be a JSON object");
  if (spec.contains("name"))
    trigger.name = spec.at("name").get<std::string>();
  if (spec.contains("event") || spec.contains("event_type"))
    trigger.event_type = spec.value("event", spec.value("event_type", trigger.event_type));
  if (spec.contains("match"))
    trigger.match = spec.at("match");
  if (spec.contains("enabled"))
    trigger.enabled = spec.at("enabled").get<bool>();
  if (spec.contains("pipeline") || spec.contains("pipeline_id") ||
      spec.contains("pipeline_version") || spec.contains("version")) {
    const auto raw = spec.value("pipeline", spec.value("pipeline_id", trigger.pipeline_id));
    const auto parsed = parse_pipeline_reference(raw);
    trigger.pipeline_id = parsed.explicit_version ? parsed.name : raw;
    if (parsed.explicit_version)
      trigger.pipeline_version = parsed.version;
    else if (spec.contains("pipeline_version"))
      trigger.pipeline_version = spec.at("pipeline_version").get<unsigned>();
    else if (spec.contains("version"))
      trigger.pipeline_version = spec.at("version").get<unsigned>();
  }
  const auto pipeline =
      pipeline_record(pipeline_reference(trigger.pipeline_id, trigger.pipeline_version));
  trigger.pipeline_id = pipeline.at("name").get<std::string>();
  trigger.pipeline_version = pipeline.at("version").get<unsigned>();
  return scheduler_.update_trigger(std::move(trigger));
}

void Service::set_trigger_enabled(const std::string &id, bool enabled) {
  scheduler_.set_trigger_enabled(id, enabled);
}
void Service::delete_trigger(const std::string &id) {
  scheduler_.delete_trigger(id);
}
Json Service::event_sources() const {
  return plugins_.event_sources();
}
Json Service::event_source(const std::string &id) const {
  return plugins_.event_source(id);
}
void Service::set_event_source_enabled(const std::string &id, bool enabled) {
  plugins_.set_event_source_enabled(id, enabled);
}
Json Service::workers() const {
  Json result = Json::array();
  for (const auto &id : worker_registry_.names())
    result.push_back(worker_registry_.get(id)->metadata());
  return result;
}
Json Service::worker(const std::string &id) const {
  return Json(worker_registry_.get(id)->metadata());
}
std::vector<Json> Service::worker_jobs(const std::string &run_id, std::size_t limit,
                                       std::size_t offset) const {
  return worker_manager_->jobs(run_id, limit, offset);
}
Json Service::worker_job(const std::string &id) const {
  return Json(worker_manager_->job(id));
}
void Service::cancel_worker_job(const std::string &id) {
  worker_manager_->cancel(id, WorkerJobState::Cancelled, "Cancellation requested by operator");
}
std::vector<Json> Service::worker_interactions(const std::string &run_id, std::size_t limit,
                                               std::size_t offset) const {
  return worker_manager_->worker_interactions(run_id, limit, offset);
}
Json Service::worker_interaction(const std::string &id) const {
  return worker_manager_->worker_interaction(id);
}
void Service::resolve_worker_interaction(const std::string &id, WorkerInteractionState state,
                                         const Json &payload, const std::string &actor,
                                         const std::string &reason) {
  worker_manager_->resolve_interaction(id, state, payload, actor, reason);
}
Json Service::pipeline_record(const std::string &reference) const {
  const auto parsed = parse_pipeline_reference(reference);
  if (parsed.explicit_version) {
    const auto key = pipeline_reference(parsed.name, parsed.version);
    try {
      return storage_->get(RecordKind::Pipeline, key);
    } catch (const Error &error) {
      if (error.code != ErrorCode::NotFound)
        throw;
      if (parsed.version != 1)
        throw;
      // Compatibility for a database created before versioned registry keys.
      return storage_->get(RecordKind::Pipeline, parsed.name);
    }
  }
  try {
    return storage_->get(RecordKind::Pipeline, parsed.name);
  } catch (const Error &error) {
    if (error.code != ErrorCode::NotFound)
      throw;
  }
  Json found;
  for (const auto &record : all_pipeline_records(*storage_)) {
    if (record.value("name", std::string{}) != parsed.name)
      continue;
    if (!found.is_null())
      throw Error(ErrorCode::Conflict, "Unversioned pipeline reference is ambiguous",
                  {{"pipeline", parsed.name}, {"reason", "use name@version"}});
    found = record;
  }
  if (found.is_null())
    throw Error(ErrorCode::NotFound, "Pipeline is not registered", {{"pipeline", parsed.name}});
  return found;
}
PipelineDefinition Service::resolve_pipeline(const std::string &reference) const {
  const auto record = pipeline_record(reference);
  auto extensions = nodes_.names();
  auto p =
      parse_pipeline(record.at("yaml").get<std::string>(), {extensions.begin(), extensions.end()});
  if (record.contains("resolved_subpipelines"))
    p.resolved_subpipelines =
        record.at("resolved_subpipelines").get<std::map<std::string, std::string>>();
  return p;
}
Json Service::run_view(const std::string &id) const {
  auto result = storage_->get(RecordKind::Run, id);
  Json children = Json::array();
  for (std::size_t offset = 0;;) {
    auto page = storage_->list(RecordKind::Run, "", 10000, offset);
    for (const auto &item : page) {
      const auto child = item.get<Run>();
      if (child.parent_id == id)
        children.push_back({{"id", child.id},
                            {"pipeline_id", child.pipeline_id},
                            {"pipeline_version", child.pipeline_version},
                            {"parent_node_id", child.parent_node_id},
                            {"state", child.state}});
    }
    if (page.size() < 10000)
      break;
    offset += 10000;
  }
  result["children"] = std::move(children);
  return result;
}
namespace {
Json operator_run_summary(const Run &run) {
  return {{"id", run.id},
          {"pipeline_id", run.pipeline_id},
          {"pipeline_version", run.pipeline_version},
          {"state", run.state},
          {"created_at", run.created_at},
          {"updated_at", run.updated_at},
          {"active_node", run.active_node},
          {"error", run.error},
          {"cancellation_requested", run.cancellation_requested},
          {"owner_instance_id", run.owner_instance_id},
          {"fencing_token", run.fencing_token},
          {"lease_expires_at", run.lease_expires_at},
          {"worker", run.worker},
          {"worker_job_id", run.worker_job_id}};
}
Json operator_node_work_summary(const NodeWork &work) {
  return {{"id", work.id},
          {"run_id", work.run_id},
          {"node_id", work.node_id},
          {"group_id", work.group_id},
          {"index", work.index},
          {"state", work.state},
          {"created_at", work.created_at},
          {"updated_at", work.updated_at},
          {"attempt", work.attempt},
          {"attempt_id", work.attempt_id},
          {"owner_instance_id", work.owner_instance_id},
          {"fencing_token", work.fencing_token},
          {"claimed_at", work.claimed_at},
          {"last_renewed_at", work.last_renewed_at},
          {"lease_expires_at", work.lease_expires_at},
          {"required_worker_id", work.required_worker_id},
          {"required_capability", work.required_capability},
          {"error", work.error},
          {"result_present", work.result.has_value()}};
}
Json operator_attempt_summary(const Json &value) {
  const auto attempt = value.get<NodeExecution>();
  return {{"id", attempt.id},
          {"run_id", attempt.run_id},
          {"node_id", attempt.node_id},
          {"attempt", attempt.attempt},
          {"state", attempt.state},
          {"started_at", attempt.started_at},
          {"finished_at", attempt.finished_at},
          {"duration_ms", attempt.duration_ms},
          {"error", attempt.error},
          {"worker_job_id", attempt.worker_job_id},
          {"worker_id", attempt.worker_id},
          {"external_job_id", attempt.external_job_id}};
}
Json operator_worker_job_summary(const Json &value) {
  const auto job = value.get<WorkerJob>();
  Json usage = {{"provider", job.usage.provider},
                {"model", job.usage.model},
                {"executor", job.usage.executor}};
  if (job.usage.wall_duration_ms)
    usage["wall_duration_ms"] = *job.usage.wall_duration_ms;
  if (job.usage.total_tokens)
    usage["total_tokens"] = *job.usage.total_tokens;
  if (job.usage.cost_units)
    usage["cost_units"] = *job.usage.cost_units;
  return {{"id", job.id},
          {"run_id", job.run_id},
          {"node_id", job.node_id},
          {"worker_id", job.worker_id},
          {"attempt", job.attempt},
          {"state", job.state},
          {"failure_kind", job.failure_kind},
          {"submitted_at", job.submitted_at},
          {"started_at", job.started_at},
          {"completed_at", job.completed_at},
          {"external_job_id", job.external_job_id},
          {"error", job.error},
          {"cancellation_error", job.cancellation_error},
          {"cancellation_requested", job.cancellation_requested},
          {"cancellation_acknowledged", job.cancellation_acknowledged},
          {"result_present", !job.result.is_null() && !job.result.empty()},
          {"artifact_count", job.artifacts.size()},
          {"usage", std::move(usage)}};
}
Json operator_artifact_summary(const Json &value) {
  const auto artifact = value.get<Artifact>();
  Json result = {{"id", artifact.id},
                 {"run_id", artifact.run_id},
                 {"node_id", artifact.node_id},
                 {"name", artifact.name},
                 {"media_type", artifact.media_type},
                 {"created_at", artifact.created_at},
                 {"object_id", artifact.object_id},
                 {"sha256", artifact.sha256},
                 {"size", artifact.size},
                 {"location_present", !artifact.location.empty()}};
  // Only expose integrity/provenance keys; arbitrary metadata may contain
  // paths or provider-specific content and is deliberately not dumped.
  for (const auto *key : {"sha256", "size", "attempt_id", "worker_id", "fencing_token"})
    if (artifact.metadata.contains(key))
      result["metadata"][key] = artifact.metadata.at(key);
  return result;
}
} // namespace
std::vector<Json> Service::inspect_runs() const {
  std::vector<Json> result;
  for (const auto &value : storage_->list(RecordKind::Run, "", 10000, 0))
    result.push_back(operator_run_summary(value.get<Run>()));
  return result;
}
Json Service::inspect_run(const std::string &id) const {
  const auto run = storage_->get(RecordKind::Run, id).get<Run>();
  Json result = operator_run_summary(run);
  result["children"] = Json::array();
  result["node_work"] = Json::array();
  result["attempts"] = Json::array();
  result["worker_jobs"] = Json::array();
  result["artifacts"] = Json::array();
  for (const auto &value : storage_->list(RecordKind::Run, "", 10000, 0)) {
    const auto child = value.get<Run>();
    if (child.parent_id == id)
      result["children"].push_back(operator_run_summary(child));
  }
  for (const auto &value : storage_->list(RecordKind::NodeWork, id, 10000, 0))
    result["node_work"].push_back(operator_node_work_summary(value.get<NodeWork>()));
  for (const auto &value : storage_->list(RecordKind::Attempt, id, 10000, 0))
    result["attempts"].push_back(operator_attempt_summary(value));
  for (const auto &value : worker_manager_->jobs(id, 10000, 0))
    result["worker_jobs"].push_back(operator_worker_job_summary(value));
  for (const auto &value : storage_->list(RecordKind::Artifact, id, 10000, 0))
    result["artifacts"].push_back(operator_artifact_summary(value));
  return result;
}
std::vector<Json> Service::inspect_node_works(const std::string &run_id) const {
  std::vector<Json> result;
  for (const auto &value : storage_->list(RecordKind::NodeWork, run_id, 10000, 0))
    result.push_back(operator_node_work_summary(value.get<NodeWork>()));
  return result;
}
Json Service::inspect_node_work(const std::string &id) const {
  return operator_node_work_summary(storage_->get(RecordKind::NodeWork, id).get<NodeWork>());
}
std::vector<Json> Service::inspect_artifacts(const std::string &run_id) const {
  std::vector<Json> result;
  for (const auto &value : storage_->list(RecordKind::Artifact, run_id, 10000, 0))
    result.push_back(operator_artifact_summary(value));
  return result;
}
Json Service::artifact_integrity() const {
  const auto report = artifacts_->integrity();
  Json errors = Json::array();
  for (const auto &error : report.errors)
    errors.push_back(error);
  return {{"root", artifacts_->root().filename().string()},
          {"objects", report.objects},
          {"verified", report.verified},
          {"invalid", report.invalid},
          {"temporary", report.temporary},
          {"errors", std::move(errors)}};
}
Json Service::artifact_gc(bool dry_run, std::uint64_t grace_seconds) {
  return artifacts_->collect_garbage(dry_run, grace_seconds);
}
std::vector<Json> Service::inspect_worker_jobs(const std::string &run_id) const {
  std::vector<Json> result;
  for (const auto &value : worker_manager_->jobs(run_id, 10000, 0))
    result.push_back(operator_worker_job_summary(value));
  return result;
}
Json Service::inspect_worker_job(const std::string &id) const {
  return operator_worker_job_summary(Json(worker_manager_->job(id)));
}
Json Service::providers() const {
  Json result = Json::array();
  for (const auto &name : providers_.names()) {
    auto p = providers_.get(name);
    auto m = p->metadata();
    result.push_back({{"name", name},
                      {"version", m.version},
                      {"remote", m.remote},
                      {"streaming", m.streaming},
                      {"context_size", m.context_size},
                      {"plugin", m.plugin},
                      {"continuation_mode", continuation_mode_name(m.continuation_mode)},
                      {"healthy", p->health().healthy},
                      {"capabilities", m.capabilities}});
  }
  return result;
}
Json Service::tools() const {
  Json result = Json::array();
  for (const auto &name : tools_.names()) {
    auto m = tools_.get(name)->metadata();
    result.push_back({{"name", name},
                      {"description", m.description},
                      {"permission", m.permission},
                      {"network", m.network},
                      {"approval_required", m.approval_required},
                      {"plugin", m.plugin},
                      {"timeout_ms", m.timeout.count()},
                      {"input_schema", m.input_schema},
                      {"output_schema", m.output_schema}});
  }
  return result;
}
Json Service::plugins() const {
  return Json(plugins_.plugins());
}
void Service::shutdown() {
  if (shutdown_)
    return;
  shutdown_ = true;
  ingress_.stop();
  plugins_.stop_event_sources();
  worker_manager_->stop();
  scheduler_.stop();
  runtime_.shutdown();
  plugins_.stop_workers();
  for (auto it = process_workers_.rbegin(); it != process_workers_.rend(); ++it)
    (*it)->stop();
  if (coordination_) {
    try {
      coordination_->set_instance_state("STOPPED");
    } catch (const Error &) {
    }
  }
}
Json Service::instances() const {
  if (!coordination_)
    return Json::array();
  Json result = Json::array();
  for (const auto &instance : coordination_->list_instances(config_.instance_stale_after_ms))
    result.push_back(instance);
  return result;
}
void Service::recover_history() {
  if (config_.execution_mode == "multi_instance")
    return;
  for (std::size_t offset = 0;; offset += 1000) {
    auto records = storage_->list(RecordKind::Run, "", 1000, offset);
    for (const auto &record : records) {
      auto r = record.get<Run>();
      // A terminal checkpoint is immutable.  A stale cancellation flag must not
      // turn a completed run into a different terminal state during recovery.
      if (terminal(r.state))
        continue;
      if (r.cancellation_requested) {
        r.state = RunState::Cancelled;
        r.updated_at = timestamp();
        Event event;
        event.run_id = r.id;
        event.pipeline_id = r.pipeline_id;
        event.node_id = r.active_node;
        event.type = "run.cancelled";
        storage_->commit({{RecordKind::Run, r.id, r.id, Json(r)},
                          {RecordKind::Event, event.id, r.id, Json(event)}});
        continue;
      }
      if (r.state == RunState::WaitingApproval || r.state == RunState::Paused ||
          r.state == RunState::Queued)
        continue;
      r.state = RunState::Paused;
      r.error = "Process stopped during execution; explicit resume may replay the unfinished node";
      r.updated_at = timestamp();
      Event event;
      event.run_id = r.id;
      event.pipeline_id = r.pipeline_id;
      event.node_id = r.active_node;
      event.type = "run.recovery_required";
      std::vector<Record> checkpoint{{RecordKind::Run, r.id, r.id, Json(r)},
                                     {RecordKind::Event, event.id, r.id, Json(event)}};
      for (std::size_t attempt_offset = 0;;) {
        constexpr std::size_t page_size = 10000;
        auto attempts = storage_->list(RecordKind::Attempt, r.id, page_size, attempt_offset);
        for (const auto &item : attempts) {
          auto a = item.get<NodeExecution>();
          if (a.state == NodeState::Running) {
            a.state = NodeState::Failed;
            a.error = "Interrupted by process termination";
            a.finished_at = timestamp();
            checkpoint.push_back({RecordKind::Attempt, a.id, r.id, Json(a)});
          }
        }
        if (attempts.size() < page_size)
          break;
        attempt_offset += page_size;
      }
      storage_->commit(checkpoint);
    }
    if (records.size() < 1000)
      break;
  }
}
} // namespace laso
