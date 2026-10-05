#include "../pipeline/yaml.hpp"
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <laso/core/config.hpp>
#include <laso/pipeline/parser.hpp>
#include <regex>
#include <set>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

namespace laso {
void Config::validate() {
  if (data_dir.empty())
    throw Error(ErrorCode::Configuration, "Data directory is empty");
  if (storage_backend != "sqlite" && storage_backend != "postgres")
    throw Error(ErrorCode::Configuration, "Unsupported storage backend");
  if (storage_backend == "postgres" && postgres_dsn.empty())
    throw Error(ErrorCode::Configuration, "PostgreSQL DSN is required");
  if (storage_backend == "postgres" &&
      (postgres_schema.empty() || postgres_schema.size() > 63 ||
       !std::isalpha(static_cast<unsigned char>(postgres_schema.front()))))
    throw Error(ErrorCode::Configuration, "Invalid PostgreSQL schema");
  if (storage_backend == "postgres")
    for (const auto ch : postgres_schema)
      if (!std::isalnum(static_cast<unsigned char>(ch)) && ch != '_')
        throw Error(ErrorCode::Configuration, "Invalid PostgreSQL schema");
  if (execution_mode != "single" && execution_mode != "multi_instance")
    throw Error(ErrorCode::Configuration, "Invalid execution mode");
  if (coordination_mode == "experimental_multi_instance")
    execution_mode = "multi_instance";
  if (execution_mode == "multi_instance" && storage_backend != "postgres")
    throw Error(ErrorCode::Configuration, "multi_instance execution requires PostgreSQL storage");
  if (coordination_mode != "single_owner" && coordination_mode != "experimental_multi_instance")
    throw Error(ErrorCode::Configuration, "Invalid coordination mode");
  if (db_path.empty())
    db_path = data_dir / "laso.db";
  if (artifact_root.empty())
    artifact_root = data_dir / "artifacts";
  if (artifact_backend != "filesystem" && artifact_backend != "s3")
    throw Error(ErrorCode::Configuration, "Unsupported artifact backend");
  if (artifact_backend == "s3") {
#ifndef LASO_HAS_S3
    throw Error(ErrorCode::Configuration,
                "S3 artifact storage requires a build with LASO_ENABLE_S3=ON");
#else
    if (artifact_s3_bucket.size() < 3 || artifact_s3_bucket.size() > 63 ||
        !std::regex_match(artifact_s3_bucket, std::regex("[a-z0-9][a-z0-9.-]*[a-z0-9]")) ||
        artifact_s3_bucket.find("..") != std::string::npos || artifact_s3_region.empty() ||
        artifact_s3_region.size() > 128 || artifact_s3_prefix.empty() ||
        artifact_s3_prefix.size() > 256 || artifact_s3_prefix.front() == '/' ||
        artifact_s3_prefix.back() == '/' || artifact_s3_prefix.find("..") != std::string::npos ||
        artifact_s3_prefix.find("//") != std::string::npos ||
        std::regex_search(artifact_s3_prefix, std::regex("(^|/)\\.(?:/|$)")) ||
        !std::regex_match(artifact_s3_prefix, std::regex("[A-Za-z0-9._/-]+")) ||
        artifact_s3_connect_timeout_ms == 0 || artifact_s3_connect_timeout_ms > 120000 ||
        artifact_s3_request_timeout_ms == 0 || artifact_s3_request_timeout_ms > 600000 ||
        artifact_s3_max_retries > 5 || max_artifact_bytes > 5'000'000'000ULL ||
        (!artifact_s3_ca_file.empty() && (!std::filesystem::is_regular_file(artifact_s3_ca_file) ||
                                          !std::ifstream(artifact_s3_ca_file).good())) ||
        !artifact_service_url.empty())
      throw Error(ErrorCode::Configuration, "Invalid S3 artifact storage configuration");
    if (!artifact_s3_endpoint.empty()) {
      static const std::regex endpoint_pattern(
          "^(https?)://(\\[[0-9A-Fa-f:]+\\]|[A-Za-z0-9.-]+)(:[0-9]{1,5})?$");
      std::smatch match;
      if (!std::regex_match(artifact_s3_endpoint, match, endpoint_pattern))
        throw Error(ErrorCode::Configuration, "Invalid S3 artifact endpoint");
      if (match[3].matched) {
        const auto port = std::stoul(match[3].str().substr(1));
        if (port == 0 || port > 65535)
          throw Error(ErrorCode::Configuration, "Invalid S3 artifact endpoint port");
      }
      if (match[1] == "http") {
        const auto host = match[2].str();
        if (!artifact_s3_allow_http ||
            (host != "localhost" && host != "127.0.0.1" && host != "[::1]"))
          throw Error(ErrorCode::Configuration,
                      "Plain HTTP S3 endpoints are allowed only for explicit loopback tests");
      } else if (artifact_s3_allow_http)
        throw Error(ErrorCode::Configuration,
                    "artifact_s3_allow_http applies only to loopback HTTP testing");
    } else if (artifact_s3_allow_http) {
      throw Error(ErrorCode::Configuration,
                  "Plain HTTP S3 testing requires an explicit loopback endpoint");
    }
#endif
  } else if (!artifact_s3_endpoint.empty() || !artifact_s3_bucket.empty() ||
             artifact_s3_allow_http || artifact_s3_path_style || !artifact_s3_ca_file.empty() ||
             artifact_s3_region != "us-east-1" || artifact_s3_prefix != "laso" ||
             artifact_s3_connect_timeout_ms != 3000 || artifact_s3_request_timeout_ms != 30000 ||
             artifact_s3_max_retries != 2) {
    throw Error(ErrorCode::Configuration, "S3 options require artifact_backend: s3");
  }
  if (artifact_service_token.size() > 4096 ||
      (!artifact_service_url.empty() && artifact_service_token.empty()) ||
      (artifact_service_port != 0 && artifact_service_token.empty()) ||
      (!artifact_service_url.empty() && artifact_service_port != 0))
    throw Error(ErrorCode::Configuration, "Invalid artifact service configuration");
  if (max_artifact_bytes == 0 || max_artifact_bytes > (std::uint64_t{4} << 40) ||
      max_artifact_temp_bytes < max_artifact_bytes ||
      max_artifact_temp_bytes > (std::uint64_t{8} << 40) || artifact_cleanup_grace_seconds == 0 ||
      artifact_cleanup_grace_seconds > 30 * 86400)
    throw Error(ErrorCode::Configuration, "Invalid artifact storage limit");
  if (api_port == 0 || api_port > 65535 || artifact_service_port > 65535 ||
      max_session_sse_streams == 0 || max_session_sse_streams > 128 || workers == 0 ||
      workers > 64 || max_runs == 0 || max_runs > 1024 || max_nodes == 0 || max_nodes > 4096 ||
      max_nodes_per_run == 0 || max_nodes_per_run > max_nodes || max_models == 0 ||
      max_models > 1024 || max_tools == 0 || max_tools > 1024 || max_subpipeline_depth == 0 ||
      max_subpipeline_depth > 64 || max_pending_scheduler_launches == 0 ||
      max_pending_scheduler_launches > 4096 || max_event_trigger_depth == 0 ||
      max_event_trigger_depth > 64 || max_event_trigger_deliveries == 0 ||
      max_event_trigger_deliveries > 100000 || max_worker_jobs == 0 || max_worker_jobs > 4096 ||
      max_worker_jobs_per_worker == 0 || max_worker_jobs_per_worker > max_worker_jobs ||
      max_worker_wall_time_ms > 1000000000000000ULL ||
      max_worker_tokens_per_run > 1000000000000000ULL ||
      !std::isfinite(max_worker_cost_units_per_run) || max_worker_cost_units_per_run < 0 ||
      max_worker_cost_units_per_run > 1000000000000000.0 || postgres_pool_min_connections == 0 ||
      postgres_pool_max_connections < postgres_pool_min_connections ||
      postgres_pool_max_connections > 64 || postgres_pool_acquisition_timeout_ms == 0 ||
      postgres_pool_acquisition_timeout_ms > 60000 || coordination_lease_ttl_ms < 1000 ||
      coordination_lease_ttl_ms > 86400000 || coordination_heartbeat_interval_ms == 0 ||
      coordination_heartbeat_interval_ms >= coordination_lease_ttl_ms / 2 ||
      instance_stale_after_ms < coordination_lease_ttl_ms || instance_stale_after_ms > 86400000 ||
      claim_batch_size == 0 || claim_batch_size > 1024 || max_pending_runs == 0 ||
      max_pending_runs > 100000)
    throw Error(ErrorCode::Configuration, "Invalid port or concurrency limit");
  if ((api_host != "127.0.0.1" && api_host != "::1") && !allow_remote_api)
    throw Error(ErrorCode::Configuration, "Non-loopback API requires allow_remote_api=true");
  if (artifact_service_host != "127.0.0.1" && artifact_service_host != "::1" && !allow_remote_api)
    throw Error(ErrorCode::Configuration,
                "Non-loopback artifact service requires allow_remote_api=true");
  if (log_level != "debug" && log_level != "info" && log_level != "warn" && log_level != "error")
    throw Error(ErrorCode::Configuration, "Invalid log level");
  if (event_sources.size() > 64)
    throw Error(ErrorCode::Configuration, "Too many event sources");
  static const std::regex source_id_pattern("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}");
  for (const auto &[id, source] : event_sources) {
    if (!std::regex_match(id, source_id_pattern) || source.plugin.empty() ||
        source.plugin.size() > 128 || source.component.size() > 128 || source.schema.size() > 512 ||
        source.config.dump().size() > std::size_t{1024} * 1024)
      throw Error(ErrorCode::Configuration, "Invalid event source configuration");
  }
  if (worker_plugins.size() > 64)
    throw Error(ErrorCode::Configuration, "Too many workers");
  for (const auto &[id, worker] : worker_plugins) {
    if (!std::regex_match(id, source_id_pattern) || worker.plugin.empty() ||
        worker.plugin.size() > 128 || worker.component.size() > 128 ||
        worker.event_schema.size() > 512 || worker.config.dump().size() > std::size_t{1024} * 1024)
      throw Error(ErrorCode::Configuration, "Invalid worker configuration");
  }
  if (process_workers.size() > 64)
    throw Error(ErrorCode::Configuration, "Too many process workers");
  for (const auto &[id, worker] : process_workers) {
    if (!std::regex_match(id, source_id_pattern) || worker.executable.empty() ||
        worker.executable.size() > 4096 || worker.executable.front() != '/')
      throw Error(ErrorCode::Configuration, "Invalid process worker executable");
    std::size_t argument_bytes = 0;
    for (const auto &arg : worker.args) {
      if (arg.size() > 4096 || argument_bytes > 65536 || arg.size() + 1 > 65536 - argument_bytes)
        throw Error(ErrorCode::Configuration, "Invalid process worker arguments");
      argument_bytes += arg.size() + 1;
    }
    if (worker.args.size() > 128 || worker.environment_allowlist.size() > 64 ||
        worker.environment.size() > 64 || worker.startup_timeout_ms == 0 ||
        worker.startup_timeout_ms > 300000 || worker.request_timeout_ms == 0 ||
        worker.request_timeout_ms > 300000 || worker.interaction_timeout_ms == 0 ||
        worker.interaction_timeout_ms > 3600000)
      throw Error(ErrorCode::Configuration, "Invalid process worker limits");
    static const std::regex env_name("[A-Za-z_][A-Za-z0-9_]{0,127}");
    std::set<std::string> allowlisted_names;
    for (const auto &name : worker.environment_allowlist)
      if (!std::regex_match(name, env_name) || !allowlisted_names.insert(name).second)
        throw Error(ErrorCode::Configuration, "Invalid process worker environment name");
    std::size_t configured_environment_bytes = 0;
    for (const auto &[name, value] : worker.environment) {
      if (!std::regex_match(name, env_name) || value.size() > 4096 ||
          name.find('\0') != std::string::npos || value.find('\0') != std::string::npos ||
          name.size() + value.size() + 2 > 65536 ||
          configured_environment_bytes > 65536 - (name.size() + value.size() + 2))
        throw Error(ErrorCode::Configuration, "Invalid process worker environment");
      configured_environment_bytes += name.size() + value.size() + 2;
    }
  }
}
Config load_config(const std::filesystem::path &supplied,
                   const std::map<std::string, std::string> &overrides) {
  Config c;
  auto path = supplied;
  if (path.empty())
    if (auto *env = std::getenv("LASO_CONFIG"))
      path = env;
  std::map<std::string, std::string> values;
  if (!path.empty()) {
    try {
      const auto n = detail::load_safe_yaml(read_document(path));
      if (!n.IsMap())
        throw Error(ErrorCode::Configuration, "Config must be a map");
      std::set<std::string> seen;
      for (const auto &pair : n) {
        auto key = pair.first.as<std::string>();
        if (!seen.insert(key).second)
          throw Error(ErrorCode::Configuration, "Duplicate configuration field");
        if (key == "coordination") {
          if (!pair.second.IsMap())
            throw Error(ErrorCode::Configuration, "coordination must be a map");
          for (const auto &field : pair.second) {
            const auto name = field.first.as<std::string>();
            if (name == "mode")
              c.coordination_mode = field.second.as<std::string>();
            else if (name == "lease_ttl_ms")
              c.coordination_lease_ttl_ms = field.second.as<std::uint64_t>();
            else if (name == "heartbeat_interval_ms")
              c.coordination_heartbeat_interval_ms = field.second.as<std::uint64_t>();
            else
              throw Error(ErrorCode::Configuration, "Unknown coordination field");
          }
        } else if (key == "models") {
          c.models.clear();
          for (const auto &model : pair.second) {
            auto name = model.first.as<std::string>();
            if (!c.models
                     .emplace(name, ModelBinding{model.second["provider"].as<std::string>(),
                                                 model.second["model"].as<std::string>()})
                     .second)
              throw Error(ErrorCode::Configuration, "Duplicate logical model");
          }
        } else if (key == "plugin_dirs") {
          if (!pair.second.IsSequence())
            throw Error(ErrorCode::Configuration, "plugin_dirs must be a sequence");
          for (const auto &dir : pair.second)
            c.plugin_dirs.emplace_back(dir.as<std::string>());
        } else if (key == "schema_roots") {
          if (!pair.second.IsSequence())
            throw Error(ErrorCode::Configuration, "schema_roots must be a sequence");
          for (const auto &root : pair.second)
            c.schema_roots.emplace_back(root.as<std::string>());
        } else if (key == "policies") {
          for (const auto &rule : pair.second) {
            auto decision = rule["decision"].as<std::string>();
            if (decision != "allow" && decision != "deny" && decision != "approval")
              throw Error(ErrorCode::Configuration, "Unknown policy decision");
            c.rules.push_back({rule["resource"].as<std::string>(),
                               decision == "deny"       ? PolicyDecision::Deny
                               : decision == "approval" ? PolicyDecision::RequireApproval
                                                        : PolicyDecision::Allow});
          }
        } else if (key == "event_sources") {
          if (!pair.second.IsMap())
            throw Error(ErrorCode::Configuration, "event_sources must be a map");
          for (const auto &source : pair.second) {
            const auto id = source.first.as<std::string>();
            if (!std::regex_match(id, std::regex("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")))
              throw Error(ErrorCode::Configuration, "Invalid event source id");
            const auto node = source.second;
            if (!node.IsMap())
              throw Error(ErrorCode::Configuration, "Event source must be a map");
            std::set<std::string> fields;
            for (const auto &field : node)
              if (!fields.insert(field.first.as<std::string>()).second)
                throw Error(ErrorCode::Configuration, "Duplicate event source field");
            for (const auto &field : fields)
              if (field != "plugin" && field != "component" && field != "schema" &&
                  field != "enabled" && field != "config")
                throw Error(ErrorCode::Configuration, "Unknown event source field");
            EventSourceConfig cfg;
            if (!node["plugin"])
              throw Error(ErrorCode::Configuration, "Event source plugin is required");
            cfg.plugin = node["plugin"].as<std::string>();
            if (node["component"])
              cfg.component = node["component"].as<std::string>();
            if (node["schema"])
              cfg.schema = node["schema"].as<std::string>();
            if (node["enabled"])
              cfg.enabled = node["enabled"].as<bool>();
            if (node["config"])
              cfg.config = detail::yaml_value(node["config"]);
            if (!c.event_sources.emplace(id, std::move(cfg)).second)
              throw Error(ErrorCode::Configuration, "Duplicate event source id");
          }
        } else if (key == "worker_plugins") {
          if (!pair.second.IsMap())
            throw Error(ErrorCode::Configuration, "worker_plugins must be a map");
          for (const auto &worker : pair.second) {
            const auto id = worker.first.as<std::string>();
            if (!std::regex_match(id, std::regex("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")))
              throw Error(ErrorCode::Configuration, "Invalid worker id");
            const auto node = worker.second;
            if (!node.IsMap())
              throw Error(ErrorCode::Configuration, "Worker must be a map");
            std::set<std::string> fields;
            for (const auto &field : node)
              if (!fields.insert(field.first.as<std::string>()).second)
                throw Error(ErrorCode::Configuration, "Duplicate worker field");
            for (const auto &field : fields)
              if (field != "plugin" && field != "component" && field != "event_schema" &&
                  field != "enabled" && field != "config")
                throw Error(ErrorCode::Configuration, "Unknown worker field");
            WorkerConfig cfg;
            if (!node["plugin"])
              throw Error(ErrorCode::Configuration, "Worker plugin is required");
            cfg.plugin = node["plugin"].as<std::string>();
            if (node["component"])
              cfg.component = node["component"].as<std::string>();
            if (node["event_schema"])
              cfg.event_schema = node["event_schema"].as<std::string>();
            if (node["enabled"])
              cfg.enabled = node["enabled"].as<bool>();
            if (node["config"])
              cfg.config = detail::yaml_value(node["config"]);
            if (!c.worker_plugins.emplace(id, std::move(cfg)).second)
              throw Error(ErrorCode::Configuration, "Duplicate worker id");
          }
        } else if (key == "process_workers") {
          if (!pair.second.IsMap())
            throw Error(ErrorCode::Configuration, "process_workers must be a map");
          for (const auto &worker : pair.second) {
            const auto id = worker.first.as<std::string>();
            if (!std::regex_match(id, std::regex("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")))
              throw Error(ErrorCode::Configuration, "Invalid process worker id");
            const auto node = worker.second;
            if (!node.IsMap())
              throw Error(ErrorCode::Configuration, "Process worker must be a map");
            std::set<std::string> fields;
            for (const auto &field : node)
              if (!fields.insert(field.first.as<std::string>()).second)
                throw Error(ErrorCode::Configuration, "Duplicate process worker field");
            for (const auto &field : fields)
              if (field != "executable" && field != "args" && field != "environment_allowlist" &&
                  field != "environment" && field != "startup_timeout_ms" &&
                  field != "request_timeout_ms" && field != "interaction_timeout_ms")
                throw Error(ErrorCode::Configuration, "Unknown process worker field");
            ProcessWorkerConfig cfg;
            if (!node["executable"])
              throw Error(ErrorCode::Configuration, "Process worker executable is required");
            cfg.executable = node["executable"].as<std::string>();
            auto sequence = [](const YAML::Node &value, const char *name) {
              std::vector<std::string> result;
              if (!value.IsSequence())
                throw Error(ErrorCode::Configuration, std::string(name) + " must be a sequence");
              for (const auto &entry : value)
                result.push_back(entry.as<std::string>());
              return result;
            };
            if (node["args"])
              cfg.args = sequence(node["args"], "args");
            if (node["environment_allowlist"])
              cfg.environment_allowlist =
                  sequence(node["environment_allowlist"], "environment_allowlist");
            if (node["environment"]) {
              if (!node["environment"].IsMap())
                throw Error(ErrorCode::Configuration, "environment must be a map");
              for (const auto &entry : node["environment"])
                if (!cfg.environment
                         .emplace(entry.first.as<std::string>(), entry.second.as<std::string>())
                         .second)
                  throw Error(ErrorCode::Configuration,
                              "Duplicate process worker environment name");
            }
            if (node["startup_timeout_ms"])
              cfg.startup_timeout_ms = node["startup_timeout_ms"].as<std::uint64_t>();
            if (node["request_timeout_ms"])
              cfg.request_timeout_ms = node["request_timeout_ms"].as<std::uint64_t>();
            if (node["interaction_timeout_ms"])
              cfg.interaction_timeout_ms = node["interaction_timeout_ms"].as<std::uint64_t>();
            if (!c.process_workers.emplace(id, std::move(cfg)).second)
              throw Error(ErrorCode::Configuration, "Duplicate process worker id");
          }
        } else
          values[key] = pair.second.as<std::string>();
      }
    } catch (const YAML::Exception &) {
      throw Error(ErrorCode::Configuration, "Invalid configuration YAML");
    }
  }
  for (auto name : {"DATA_DIR",
                    "DB_PATH",
                    "ARTIFACT_ROOT",
                    "ARTIFACT_BACKEND",
                    "ARTIFACT_S3_ENDPOINT",
                    "ARTIFACT_S3_BUCKET",
                    "ARTIFACT_S3_REGION",
                    "ARTIFACT_S3_PREFIX",
                    "ARTIFACT_S3_CA_FILE",
                    "ARTIFACT_S3_CONNECT_TIMEOUT_MS",
                    "ARTIFACT_S3_REQUEST_TIMEOUT_MS",
                    "ARTIFACT_S3_MAX_RETRIES",
                    "ARTIFACT_S3_PATH_STYLE",
                    "ARTIFACT_S3_ALLOW_HTTP",
                    "ARTIFACT_SERVICE_URL",
                    "ARTIFACT_SERVICE_TOKEN",
                    "ARTIFACT_SERVICE_HOST",
                    "ARTIFACT_SERVICE_PORT",
                    "STORAGE_BACKEND",
                    "POSTGRES_DSN",
                    "POSTGRES_SCHEMA",
                    "EXECUTION_MODE",
                    "COORDINATION_MODE",
                    "POSTGRES_POOL_MIN_CONNECTIONS",
                    "POSTGRES_POOL_MAX_CONNECTIONS",
                    "POSTGRES_POOL_ACQUISITION_TIMEOUT_MS",
                    "COORDINATION_LEASE_TTL_MS",
                    "COORDINATION_HEARTBEAT_INTERVAL_MS",
                    "PLUGIN_DIR",
                    "LOG_LEVEL",
                    "API_HOST",
                    "API_PORT",
                    "WORKERS",
                    "MAX_RUNS",
                    "MAX_NODES",
                    "MAX_NODES_PER_RUN",
                    "MAX_MODELS",
                    "MAX_TOOLS",
                    "MAX_SUBPIPELINE_DEPTH",
                    "MAX_PENDING_SCHEDULER_LAUNCHES",
                    "MAX_EVENT_TRIGGER_DEPTH",
                    "MAX_EVENT_TRIGGER_DELIVERIES",
                    "MAX_WORKER_JOBS",
                    "MAX_WORKER_JOBS_PER_WORKER",
                    "CLAIM_BATCH_SIZE",
                    "MAX_PENDING_RUNS",
                    "INSTANCE_STALE_AFTER_MS",
                    "MAX_WORKER_WALL_TIME_MS",
                    "MAX_WORKER_TOKENS_PER_RUN",
                    "MAX_WORKER_COST_UNITS_PER_RUN",
                    "MAX_ARTIFACT_BYTES",
                    "MAX_ARTIFACT_TEMP_BYTES",
                    "ARTIFACT_CLEANUP_GRACE_SECONDS",
                    "JSON_LOGS",
                    "ALLOW_NETWORK",
                    "ALLOW_REMOTE_API",
                    "LOCAL_OPENAI_ENDPOINT"}) {
    auto variable = std::string("LASO_") + name;
    if (auto *v = std::getenv(variable.c_str())) {
      std::string key = name;
      for (auto &ch : key)
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
      values[key] = v;
    }
  }
  for (const auto &[k, v] : overrides)
    values[k] = v;
  auto boolean = [](const std::string &s) {
    if (s == "true")
      return true;
    if (s == "false")
      return false;
    throw Error(ErrorCode::Configuration, "Boolean must be true or false");
  };
  auto integer = [](const std::string &s) {
    try {
      std::size_t end = 0;
      auto result = std::stoul(s, &end);
      if (end != s.size() || result > 100000)
        throw std::out_of_range("limit");
      return static_cast<unsigned>(result);
    } catch (...) {
      throw Error(ErrorCode::Configuration, "Invalid numeric configuration");
    }
  };
  auto uint64 = [](const std::string &s) {
    try {
      std::size_t end = 0;
      const auto result = std::stoull(s, &end);
      if (end != s.size() || result > 1000000000000000ULL)
        throw std::out_of_range("limit");
      return static_cast<std::uint64_t>(result);
    } catch (...) {
      throw Error(ErrorCode::Configuration, "Invalid numeric configuration");
    }
  };
  auto real = [](const std::string &s) {
    try {
      std::size_t end = 0;
      const auto result = std::stod(s, &end);
      if (end != s.size() || !std::isfinite(result) || result < 0 || result > 1e15)
        throw std::out_of_range("limit");
      return result;
    } catch (...) {
      throw Error(ErrorCode::Configuration, "Invalid numeric configuration");
    }
  };
  for (const auto &[k, v] : values) {
    if (k == "data_dir")
      c.data_dir = v;
    else if (k == "artifact_root")
      c.artifact_root = v;
    else if (k == "artifact_backend")
      c.artifact_backend = v;
    else if (k == "artifact_s3_endpoint")
      c.artifact_s3_endpoint = v;
    else if (k == "artifact_s3_bucket")
      c.artifact_s3_bucket = v;
    else if (k == "artifact_s3_region")
      c.artifact_s3_region = v;
    else if (k == "artifact_s3_prefix")
      c.artifact_s3_prefix = v;
    else if (k == "artifact_s3_ca_file")
      c.artifact_s3_ca_file = v;
    else if (k == "artifact_s3_connect_timeout_ms")
      c.artifact_s3_connect_timeout_ms = uint64(v);
    else if (k == "artifact_s3_request_timeout_ms")
      c.artifact_s3_request_timeout_ms = uint64(v);
    else if (k == "artifact_s3_max_retries")
      c.artifact_s3_max_retries = integer(v);
    else if (k == "artifact_s3_path_style")
      c.artifact_s3_path_style = boolean(v);
    else if (k == "artifact_s3_allow_http")
      c.artifact_s3_allow_http = boolean(v);
    else if (k == "artifact_service_url")
      c.artifact_service_url = v;
    else if (k == "artifact_service_token")
      c.artifact_service_token = v;
    else if (k == "artifact_service_host")
      c.artifact_service_host = v;
    else if (k == "artifact_service_port")
      c.artifact_service_port = integer(v);
    else if (k == "storage_backend")
      c.storage_backend = v;
    else if (k == "postgres_dsn")
      c.postgres_dsn = v;
    else if (k == "postgres_schema")
      c.postgres_schema = v;
    else if (k == "execution_mode")
      c.execution_mode = v;
    else if (k == "coordination_mode")
      c.coordination_mode = v;
    else if (k == "postgres_pool_min_connections")
      c.postgres_pool_min_connections = integer(v);
    else if (k == "postgres_pool_max_connections")
      c.postgres_pool_max_connections = integer(v);
    else if (k == "postgres_pool_acquisition_timeout_ms")
      c.postgres_pool_acquisition_timeout_ms = uint64(v);
    else if (k == "coordination_lease_ttl_ms")
      c.coordination_lease_ttl_ms = uint64(v);
    else if (k == "coordination_heartbeat_interval_ms")
      c.coordination_heartbeat_interval_ms = uint64(v);
    else if (k == "db_path")
      c.db_path = v;
    else if (k == "max_artifact_bytes")
      c.max_artifact_bytes = uint64(v);
    else if (k == "max_artifact_temp_bytes")
      c.max_artifact_temp_bytes = uint64(v);
    else if (k == "artifact_cleanup_grace_seconds")
      c.artifact_cleanup_grace_seconds = uint64(v);
    else if (k == "plugin_dir")
      c.plugin_dirs = {std::filesystem::path(v)};
    else if (k == "api_host")
      c.api_host = v;
    else if (k == "api_port")
      c.api_port = integer(v);
    else if (k == "max_session_sse_streams")
      c.max_session_sse_streams = integer(v);
    else if (k == "log_level")
      c.log_level = v;
    else if (k == "local_openai_endpoint")
      c.local_openai_endpoint = v;
    else if (k == "json_logs")
      c.json_logs = boolean(v);
    else if (k == "allow_network")
      c.allow_network = boolean(v);
    else if (k == "allow_remote_api")
      c.allow_remote_api = boolean(v);
    else if (k == "workers")
      c.workers = integer(v);
    else if (k == "max_runs")
      c.max_runs = integer(v);
    else if (k == "max_nodes")
      c.max_nodes = integer(v);
    else if (k == "max_nodes_per_run")
      c.max_nodes_per_run = integer(v);
    else if (k == "max_models")
      c.max_models = integer(v);
    else if (k == "max_tools")
      c.max_tools = integer(v);
    else if (k == "max_subpipeline_depth")
      c.max_subpipeline_depth = integer(v);
    else if (k == "max_pending_scheduler_launches")
      c.max_pending_scheduler_launches = integer(v);
    else if (k == "max_event_trigger_depth")
      c.max_event_trigger_depth = integer(v);
    else if (k == "max_event_trigger_deliveries")
      c.max_event_trigger_deliveries = integer(v);
    else if (k == "max_worker_jobs")
      c.max_worker_jobs = integer(v);
    else if (k == "max_worker_jobs_per_worker")
      c.max_worker_jobs_per_worker = integer(v);
    else if (k == "claim_batch_size")
      c.claim_batch_size = integer(v);
    else if (k == "max_pending_runs")
      c.max_pending_runs = integer(v);
    else if (k == "instance_stale_after_ms")
      c.instance_stale_after_ms = uint64(v);
    else if (k == "max_worker_wall_time_ms")
      c.max_worker_wall_time_ms = uint64(v);
    else if (k == "max_worker_tokens_per_run")
      c.max_worker_tokens_per_run = uint64(v);
    else if (k == "max_worker_cost_units_per_run")
      c.max_worker_cost_units_per_run = real(v);
    else
      throw Error(ErrorCode::Configuration, "Unknown configuration field");
  }
  c.validate();
  return c;
}
void configure_logging(const Config &c) {
  auto sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
  spdlog::set_default_logger(std::make_shared<spdlog::logger>("laso", sink));
  spdlog::set_level(spdlog::level::from_str(c.log_level));
  spdlog::set_pattern(c.json_logs ? "%v" : "[%H:%M:%S] [%l] %v");
}
void log_event(const Event &e) {
  // Payloads, prompts and arbitrary exception messages are deliberately excluded.
  spdlog::info("{}", Json{{"timestamp", e.time},
                          {"severity", "info"},
                          {"event", e.type},
                          {"run_id", e.run_id},
                          {"pipeline_id", e.pipeline_id},
                          {"node_id", e.node_id},
                          {"provider", e.metadata.value("provider", std::string{})},
                          {"model", e.metadata.value("model", std::string{})},
                          {"tool", e.metadata.value("tool", std::string{})},
                          {"plugin", e.metadata.value("plugin", std::string{})}}
                         .dump());
}
void log_diagnostic(const std::string &event, const Json &safe_metadata) {
  spdlog::info("{}", Json{{"timestamp", timestamp()},
                          {"severity", "info"},
                          {"event", event},
                          {"metadata", safe_metadata}}
                         .dump());
}
} // namespace laso
