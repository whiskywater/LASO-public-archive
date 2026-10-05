#include <CLI/CLI.hpp>
#include <iostream>
#include <laso/application/service.hpp>
#include <laso/cli/cli.hpp>
#include <laso/pipeline/parser.hpp>
#include <sys/stat.h>

namespace laso {
int cli_main(int argc, char **argv) {
  umask(0077);
  CLI::App app{"LASO native orchestration framework"};
  std::string config_path, data_dir, target, input = "{}", comment, actor = "local";
  app.add_option("--config", config_path, "Configuration file");
  app.add_option("--data-dir", data_dir, "Override data directory");
  auto *version_command = app.add_subcommand("version");
  auto *health = app.add_subcommand("health", "Check local storage initialization");
  auto *pipeline = app.add_subcommand("pipeline");
  pipeline->require_subcommand(1);
  auto *validate = pipeline->add_subcommand("validate");
  validate->add_option("file", target)->required();
  auto *register_command = pipeline->add_subcommand("register");
  register_command->add_option("file", target)->required();
  auto *pipeline_list = pipeline->add_subcommand("list");
  auto *pipeline_show = pipeline->add_subcommand("show");
  pipeline_show->add_option("name", target)->required();
  auto *run = app.add_subcommand("run");
  run->require_subcommand(1);
  auto *start = run->add_subcommand("start");
  start->add_option("pipeline", target)->required();
  start->add_option("--input", input, "JSON payload");
  start->add_option("--actor", actor);
  auto *run_list = run->add_subcommand("list");
  auto *show = run->add_subcommand("show");
  show->add_option("id", target)->required();
  auto *inspect = run->add_subcommand("inspect", "Show safe durable execution lineage");
  inspect->add_option("id", target)->required();
  auto *cancel = run->add_subcommand("cancel");
  cancel->add_option("id", target)->required();
  auto *resume = run->add_subcommand("resume");
  resume->add_option("id", target)->required();
  auto *approval = app.add_subcommand("approval");
  approval->require_subcommand(1);
  auto *approval_list = approval->add_subcommand("list");
  auto *approve = approval->add_subcommand("approve");
  auto *reject = approval->add_subcommand("reject");
  for (auto *command : {approve, reject}) {
    command->add_option("id", target)->required();
    command->add_option("--actor", actor);
    command->add_option("--comment", comment);
  }
  auto *plugin = app.add_subcommand("plugin");
  plugin->require_subcommand(1);
  auto *plugin_list = plugin->add_subcommand("list");
  auto *provider = app.add_subcommand("provider");
  provider->require_subcommand(1);
  auto *provider_list = provider->add_subcommand("list");
  auto *tool = app.add_subcommand("tool");
  tool->require_subcommand(1);
  auto *tool_list = tool->add_subcommand("list");
  auto *schedule = app.add_subcommand("schedule");
  schedule->require_subcommand(1);
  auto *schedule_list = schedule->add_subcommand("list");
  auto *schedule_show = schedule->add_subcommand("show");
  schedule_show->add_option("id", target)->required();
  auto *schedule_create = schedule->add_subcommand("create");
  schedule_create->add_option("file", target)->required();
  auto *schedule_enable = schedule->add_subcommand("enable");
  schedule_enable->add_option("id", target)->required();
  auto *schedule_disable = schedule->add_subcommand("disable");
  schedule_disable->add_option("id", target)->required();
  auto *schedule_delete = schedule->add_subcommand("delete");
  schedule_delete->add_option("id", target)->required();
  auto *trigger = app.add_subcommand("trigger");
  trigger->require_subcommand(1);
  auto *trigger_list = trigger->add_subcommand("list");
  auto *trigger_show = trigger->add_subcommand("show");
  trigger_show->add_option("id", target)->required();
  auto *trigger_create = trigger->add_subcommand("create");
  trigger_create->add_option("file", target)->required();
  auto *trigger_enable = trigger->add_subcommand("enable");
  trigger_enable->add_option("id", target)->required();
  auto *trigger_disable = trigger->add_subcommand("disable");
  trigger_disable->add_option("id", target)->required();
  auto *trigger_delete = trigger->add_subcommand("delete");
  trigger_delete->add_option("id", target)->required();
  auto *event_source = app.add_subcommand("event-source");
  event_source->require_subcommand(1);
  auto *event_source_list = event_source->add_subcommand("list");
  auto *event_source_show = event_source->add_subcommand("show");
  event_source_show->add_option("id", target)->required();
  auto *event_source_enable = event_source->add_subcommand("enable");
  event_source_enable->add_option("id", target)->required();
  auto *event_source_disable = event_source->add_subcommand("disable");
  event_source_disable->add_option("id", target)->required();
  auto *worker = app.add_subcommand("worker");
  worker->require_subcommand(1);
  auto *worker_list = worker->add_subcommand("list");
  auto *worker_show = worker->add_subcommand("show");
  worker_show->add_option("id", target)->required();
  auto *worker_job = app.add_subcommand("worker-job");
  worker_job->require_subcommand(1);
  auto *worker_job_list = worker_job->add_subcommand("list");
  auto *worker_job_show = worker_job->add_subcommand("show");
  worker_job_show->add_option("id", target)->required();
  auto *worker_job_cancel = worker_job->add_subcommand("cancel");
  worker_job_cancel->add_option("id", target)->required();
  auto *worker_job_inspect = worker_job->add_subcommand("inspect", "Show safe job diagnostics");
  worker_job_inspect->add_option("id", target)->required();
  auto *node_work = app.add_subcommand("node-work", "Inspect durable distributed work");
  node_work->require_subcommand(1);
  auto *node_work_list = node_work->add_subcommand("list");
  std::string node_work_run_id;
  node_work_list->add_option("--run-id", node_work_run_id);
  auto *node_work_show = node_work->add_subcommand("show");
  node_work_show->add_option("id", target)->required();
  auto *artifact = app.add_subcommand("artifact", "Inspect and maintain durable artifact objects");
  artifact->require_subcommand(1);
  auto *artifact_list = artifact->add_subcommand("list");
  std::string artifact_run_id;
  artifact_list->add_option("--run-id", artifact_run_id);
  auto *artifact_verify = artifact->add_subcommand("verify");
  auto *artifact_gc = artifact->add_subcommand("gc");
  bool artifact_execute = false;
  std::uint64_t artifact_grace_seconds = 0;
  artifact_gc->add_flag("--execute", artifact_execute, "Delete only unreferenced, aged objects");
  artifact_gc->add_option("--grace-seconds", artifact_grace_seconds);
  auto *instance = app.add_subcommand("instance");
  instance->require_subcommand(1);
  auto *instance_list = instance->add_subcommand("list");
  app.require_subcommand(1);
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }
  try {
    if (*version_command) {
      std::cout << version << '\n';
      return 0;
    }
    if (*validate) {
      auto p = parse_pipeline(read_document(target));
      auto config = load_config(config_path);
      SchemaValidator schemas(
          config.schema_roots.empty()
              ? std::vector<std::filesystem::path>{std::filesystem::current_path()}
              : config.schema_roots);
      for (const auto &[id, node] : p.nodes) {
        (void)id;
        if (!node.input_schema.empty())
          schemas.validate_declaration(node.input_schema);
        if (!node.output_schema.empty())
          schemas.validate_declaration(node.output_schema);
        if (!node.schema.empty())
          schemas.validate_declaration(node.schema);
      }
      if (!p.input_schema.empty())
        schemas.validate_declaration(p.input_schema);
      if (!p.output_schema.empty())
        schemas.validate_declaration(p.output_schema);
      std::cout << Json{{"valid", true}, {"name", p.name}, {"version", p.version}}.dump(2) << '\n';
      return 0;
    }
    std::map<std::string, std::string> overrides;
    if (!data_dir.empty())
      overrides["data_dir"] = data_dir;
    auto config = load_config(config_path, overrides);
    Executor executor(config.workers);
    Service service(executor.context(), config);
    Json result;
    std::string run_id;
    bool run_executor = false;
    if (*health)
      result = {{"status", "ok"}, {"scope", "local storage; not a daemon probe"}};
    else if (*register_command)
      result = service.register_pipeline(read_document(target));
    else if (*pipeline_list)
      result = service.list(RecordKind::Pipeline);
    else if (*pipeline_show)
      result = service.get(RecordKind::Pipeline, target);
    else if (*run_list)
      result = service.inspect_runs();
    else if (*show)
      result = service.run_view(target);
    else if (*inspect)
      result = service.inspect_run(target);
    else if (*start) {
      run_id = service.start(target, Json::parse(input), actor, true);
      run_executor = true;
    } else if (*cancel) {
      service.runtime().cancel(target);
      result = service.get(RecordKind::Run, target);
      run_executor = true;
    } else if (*resume) {
      service.runtime().resume(target);
      run_id = target;
      run_executor = true;
    } else if (*approval_list)
      result = service.list(RecordKind::Approval);
    else if (*approve || *reject) {
      run_id = service.get(RecordKind::Approval, target).at("run_id").get<std::string>();
      service.runtime().decide(target, static_cast<bool>(*approve), actor, comment);
      run_executor = true;
    } else if (*plugin_list)
      result = service.plugins();
    else if (*provider_list)
      result = service.providers();
    else if (*tool_list)
      result = service.tools();
    else if (*schedule_list)
      result = service.list(RecordKind::Schedule);
    else if (*schedule_show)
      result = service.get(RecordKind::Schedule, target);
    else if (*schedule_create)
      result = service.create_schedule(Json::parse(read_document(target)));
    else if (*schedule_enable) {
      service.set_schedule_enabled(target, true);
      result = service.get(RecordKind::Schedule, target);
    } else if (*schedule_disable) {
      service.set_schedule_enabled(target, false);
      result = service.get(RecordKind::Schedule, target);
    } else if (*schedule_delete) {
      service.delete_schedule(target);
      result = {{"id", target}, {"deleted", true}};
    } else if (*trigger_list)
      result = service.list(RecordKind::Trigger);
    else if (*trigger_show)
      result = service.get(RecordKind::Trigger, target);
    else if (*trigger_create)
      result = service.create_trigger(Json::parse(read_document(target)));
    else if (*trigger_enable) {
      service.set_trigger_enabled(target, true);
      result = service.get(RecordKind::Trigger, target);
    } else if (*trigger_disable) {
      service.set_trigger_enabled(target, false);
      result = service.get(RecordKind::Trigger, target);
    } else if (*trigger_delete) {
      service.delete_trigger(target);
      result = {{"id", target}, {"deleted", true}};
    } else if (*event_source_list)
      result = service.event_sources();
    else if (*event_source_show)
      result = service.event_source(target);
    else if (*event_source_enable) {
      service.set_event_source_enabled(target, true);
      result = service.event_source(target);
      run_executor = true;
    } else if (*event_source_disable) {
      service.set_event_source_enabled(target, false);
      result = service.event_source(target);
    } else if (*worker_list)
      result = service.workers();
    else if (*worker_show)
      result = service.worker(target);
    else if (*worker_job_list)
      result = service.worker_jobs();
    else if (*worker_job_show)
      result = service.worker_job(target);
    else if (*worker_job_inspect)
      result = service.inspect_worker_job(target);
    else if (*worker_job_cancel) {
      service.cancel_worker_job(target);
      result = service.worker_job(target);
    } else if (*node_work_list)
      result = service.inspect_node_works(node_work_run_id);
    else if (*node_work_show)
      result = service.inspect_node_work(target);
    else if (*artifact_list)
      result = service.inspect_artifacts(artifact_run_id);
    else if (*artifact_verify)
      result = service.artifact_integrity();
    else if (*artifact_gc)
      result = service.artifact_gc(!artifact_execute, artifact_grace_seconds);
    else if (*instance_list)
      result = service.instances();
    if (run_executor) {
      executor.start();
      executor.join();
    }
    if (!run_id.empty())
      result = service.run_view(run_id);
    std::cout << result.dump(2) << '\n';
    const bool artifact_integrity_failed =
        *artifact_verify && result.is_object() && result.value("invalid", 0U) > 0;
    return result.is_object() &&
                   (result.value("state", std::string{}) == "Failed" ||
                    result.value("state", std::string{}) == "TimedOut" || artifact_integrity_failed)
               ? 2
               : 0;
  } catch (const Error &e) {
    std::cerr << e.what() << '\n';
    return 2;
  } catch (...) {
    std::cerr << "Invalid input or operation failed\n";
    return 2;
  }
}
} // namespace laso
