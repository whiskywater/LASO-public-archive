#include "../support.hpp"
#include <fstream>
#include <laso/policies/policy.hpp>
#include <laso_plugin.h>
#include <type_traits>

using namespace laso;
using namespace laso::test;
static_assert(std::is_constructible_v<SQLiteStorage, const std::filesystem::path &>);
TEST(Pipeline, ParsesTypedDefinition) {
  auto p = parse_pipeline(fixture("hello-pipeline"));
  EXPECT_EQ(p.name, "hello");
  EXPECT_EQ(p.schema_version, 1U);
  EXPECT_EQ(p.nodes.at("greet").binding, "hello");
  EXPECT_EQ(p.nodes.size(), 3U);
}
TEST(Pipeline, RejectsUnsupportedVersion) {
  auto yaml = single();
  yaml.replace(yaml.find("'1'"), 3, "'2'");
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RequiresVersion) {
  auto yaml = single();
  yaml.erase(yaml.find("version: 1\n"), 11);
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RejectsDuplicateNode) {
  EXPECT_THROW(parse_pipeline("laso: '1'\nname: bad\nversion: 1\nnodes:\n  x: {type: input}\n  x: "
                              "{type: output}\nedges: []"),
               Error);
}
TEST(Pipeline, RejectsDuplicateRootField) {
  EXPECT_THROW(parse_pipeline(single() + "name: duplicate\n"), Error);
}
TEST(Pipeline, RejectsMissingTarget) {
  auto yaml = single();
  yaml.replace(yaml.find("to: output"), 10, "to: absent");
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RejectsMalformedEdge) {
  auto yaml = single();
  yaml.replace(yaml.find("to: output"), 10, "target: output");
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RejectsMalformedEdgeIdentifiers) {
  auto yaml = single();
  yaml.replace(yaml.find("to: output"), 10, "to: ../output");
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RejectsUnknownNode) {
  EXPECT_THROW(parse_pipeline(single("type: unknown")), Error);
}
TEST(Pipeline, RejectsWrongBindingField) {
  EXPECT_THROW(parse_pipeline(single("type: tool\n    function: echo")), Error);
}
TEST(Pipeline, ParsesExplicitSubpipelineRevision) {
  const auto reference = parse_pipeline_reference("research@2");
  EXPECT_EQ(reference.name, "research");
  EXPECT_EQ(reference.version, 2U);
  EXPECT_TRUE(reference.explicit_version);
  EXPECT_EQ(pipeline_reference(reference.name, reference.version), "research@2");
  const auto yaml = single("type: subpipeline\n    pipeline: research@2");
  EXPECT_NO_THROW(parse_pipeline(yaml));
}
TEST(Pipeline, RejectsMalformedSubpipelineRevision) {
  EXPECT_THROW(parse_pipeline(single("type: subpipeline\n    pipeline: research@0")), Error);
  EXPECT_THROW(parse_pipeline(single("type: subpipeline\n    pipeline: research@2@3")), Error);
  EXPECT_THROW(parse_pipeline(single("type: subpipeline\n    pipeline: ../research@2")), Error);
}
TEST(Configuration, ValidatesSubpipelineDepth) {
  Config c;
  c.max_subpipeline_depth = 0;
  EXPECT_THROW(c.validate(), Error);
  c.max_subpipeline_depth = 16;
  EXPECT_NO_THROW(c.validate());
}
TEST(Configuration, RejectsUnsupportedStorageBackend) {
  Config c;
  c.storage_backend = "postgres";
  EXPECT_THROW(c.validate(), Error);
  c.storage_backend = "sqlite";
  EXPECT_NO_THROW(c.validate());
}
TEST(Configuration, ParsesDeclarativeEventSource) {
  TemporaryDirectory dir;
  const auto path = dir.path / "laso.yaml";
  std::ofstream(path) << "event_sources:\n  offline:\n    plugin: example-event-source\n"
                         "    component: example-event\n    enabled: true\n    config:\n"
                         "      mode: once\n";
  const auto c = load_config(path);
  ASSERT_EQ(c.event_sources.size(), 1U);
  EXPECT_EQ(c.event_sources.at("offline").plugin, "example-event-source");
  EXPECT_TRUE(c.event_sources.at("offline").enabled);
  EXPECT_EQ(c.event_sources.at("offline").config.at("mode"), "once");
}
TEST(Configuration, ParsesWorkerBudgets) {
  TemporaryDirectory dir;
  const auto path = dir.path / "laso.yaml";
  std::ofstream(path) << "max_worker_wall_time_ms: 120000\n"
                         "max_worker_tokens_per_run: 500000\n"
                         "max_worker_cost_units_per_run: 2.5\n";
  const auto c = load_config(path);
  EXPECT_EQ(c.max_worker_wall_time_ms, 120000U);
  EXPECT_EQ(c.max_worker_tokens_per_run, 500000U);
  EXPECT_DOUBLE_EQ(c.max_worker_cost_units_per_run, 2.5);
}
TEST(Configuration, ParsesArtifactStoreSettings) {
  TemporaryDirectory dir;
  const auto path = dir.path / "laso.yaml";
  std::ofstream(path) << "artifact_root: /var/tmp/laso-artifacts\n"
                         "max_artifact_bytes: 1048576\n"
                         "max_artifact_temp_bytes: 2097152\n"
                         "artifact_cleanup_grace_seconds: 7200\n";
  const auto c = load_config(path);
  EXPECT_EQ(c.artifact_root, "/var/tmp/laso-artifacts");
  EXPECT_EQ(c.max_artifact_bytes, 1048576U);
  EXPECT_EQ(c.max_artifact_temp_bytes, 2097152U);
  EXPECT_EQ(c.artifact_cleanup_grace_seconds, 7200U);
}
TEST(Configuration, ParsesAuthenticatedArtifactGatewaySettings) {
  TemporaryDirectory dir;
  const auto path = dir.path / "laso.yaml";
  std::ofstream(path) << "artifact_service_host: 127.0.0.1\n"
                         "artifact_service_port: 9090\n"
                         "artifact_service_token: synthetic-token\n";
  const auto c = load_config(path);
  EXPECT_EQ(c.artifact_service_host, "127.0.0.1");
  EXPECT_EQ(c.artifact_service_port, 9090U);
  EXPECT_EQ(c.artifact_service_token, "synthetic-token");
  std::ofstream(path) << "artifact_service_url: http://127.0.0.1:9090\n"
                         "artifact_service_token: synthetic-token\n";
  const auto worker = load_config(path);
  EXPECT_EQ(worker.artifact_service_url, "http://127.0.0.1:9090");
  EXPECT_EQ(worker.artifact_service_token, "synthetic-token");
}
TEST(Configuration, S3ArtifactBackendIsOptionalAndExplicit) {
  TemporaryDirectory dir;
  const auto path = dir.path / "laso.yaml";
  std::ofstream(path) << "artifact_backend: s3\n"
                         "artifact_s3_endpoint: http://127.0.0.1:9000\n"
                         "artifact_s3_bucket: laso-test-bucket\n"
                         "artifact_s3_prefix: artifact-test\n"
                         "artifact_s3_path_style: true\n"
                         "artifact_s3_allow_http: true\n";
#ifdef LASO_HAS_S3
  const auto config = load_config(path);
  EXPECT_EQ(config.artifact_backend, "s3");
  EXPECT_EQ(config.artifact_s3_bucket, "laso-test-bucket");
  EXPECT_EQ(config.artifact_s3_prefix, "artifact-test");
  EXPECT_TRUE(config.artifact_s3_path_style);
  EXPECT_TRUE(config.artifact_s3_allow_http);
#else
  EXPECT_THROW(load_config(path), Error);
#endif
}
TEST(Configuration, RejectsS3NamespaceEscapeAndUntrustedPlainHttp) {
#ifdef LASO_HAS_S3
  Config config;
  config.artifact_backend = "s3";
  config.artifact_s3_bucket = "laso-test-bucket";
  config.artifact_s3_prefix = "../outside";
  EXPECT_THROW(config.validate(), Error);
  config.artifact_s3_prefix = "artifact-test";
  config.artifact_s3_endpoint = "http://object-store.invalid:9000";
  config.artifact_s3_allow_http = true;
  EXPECT_THROW(config.validate(), Error);
#else
  GTEST_SKIP() << "S3 configuration validation is available only in an S3 build";
#endif
}
TEST(Pipeline, RejectsUnboundedCycle) {
  auto yaml = fixture("bounded-loop");
  auto position = yaml.find(", max_iterations: 2}");
  yaml.erase(position, 19);
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RequiresLoopLimit) {
  auto yaml = fixture("bounded-loop");
  auto pos = yaml.find("type: loop, max_iterations: 2");
  yaml.replace(pos, std::string("type: loop, max_iterations: 2").size(), "type: loop");
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RejectsUnknownCondition) {
  auto yaml = fixture("bounded-loop");
  auto pos = yaml.find("condition: repeat");
  yaml.replace(pos, 17, "condition: arbitrary");
  EXPECT_THROW(parse_pipeline(yaml), Error);
}
TEST(Pipeline, RejectsUnsafeTag) {
  EXPECT_THROW(parse_pipeline("!!python/object/apply:os.system ['echo unsafe']"), Error);
}
TEST(Pipeline, RejectsAliasRecursion) {
  EXPECT_THROW(parse_pipeline("a: &a [*a]"), Error);
}
TEST(Pipeline, RejectsOversizedInput) {
  EXPECT_THROW(parse_pipeline(std::string(max_document_bytes + 1, 'x')), Error);
}
TEST(Pipeline, RejectsMultipleDocuments) {
  EXPECT_THROW(parse_pipeline(single() + "---\na: b"), Error);
}
TEST(Pipeline, SupportsCustomNodeContracts) {
  EXPECT_NO_THROW(parse_pipeline(single("type: custom"), {"custom"}));
}
TEST(State, ValidTransitionsAndTerminalImmutability) {
  EXPECT_TRUE(valid_transition(RunState::Queued, RunState::Starting));
  EXPECT_TRUE(valid_transition(RunState::Running, RunState::WaitingApproval));
  EXPECT_TRUE(valid_transition(RunState::WaitingApproval, RunState::Queued));
  EXPECT_FALSE(valid_transition(RunState::Completed, RunState::Running));
  EXPECT_FALSE(valid_transition(RunState::Queued, RunState::Completed));
  EXPECT_FALSE(valid_transition(RunState::Queued, RunState::Paused));
  EXPECT_FALSE(valid_transition(RunState::WaitingTool, RunState::WaitingApproval));
  EXPECT_TRUE(valid_transition(RunState::Running, RunState::Cancelled));
}
TEST(Message, RoundTripsEnvelope) {
  Message m;
  m.payload = {{"value", 1}};
  m.confidence = 0.8;
  auto copy = Json(m).get<Message>();
  EXPECT_EQ(copy.id, m.id);
  EXPECT_EQ(copy.payload, m.payload);
  EXPECT_EQ(copy.confidence, m.confidence);
}
TEST(Message, RejectsInvalidConfidence) {
  Message m;
  Json json = m;
  json["confidence"] = 2.0;
  EXPECT_THROW(json.get<Message>(), Error);
}
TEST(Policy, AllowsLocal) {
  PolicyEngine p;
  EXPECT_EQ(p.evaluate({}).decision, PolicyDecision::Allow);
}
TEST(Policy, DeniesNetworkByDefault) {
  PolicyEngine p;
  PolicyContext context;
  context.network = true;
  EXPECT_EQ(p.evaluate(context).decision, PolicyDecision::Deny);
}
TEST(Policy, ExplicitDenyWins) {
  PolicyEngine p({{"echo", PolicyDecision::Deny}});
  PolicyContext context;
  context.resource = "echo";
  EXPECT_EQ(p.evaluate(context).decision, PolicyDecision::Deny);
}
TEST(Policy, ApprovalRequirement) {
  PolicyEngine p;
  PolicyContext context;
  context.approval_required = true;
  EXPECT_EQ(p.evaluate(context).decision, PolicyDecision::RequireApproval);
}
TEST(Policy, RemotePrivateDataDenied) {
  PolicyEngine p({}, true);
  PolicyContext context;
  context.classification = "restricted";
  context.remote = true;
  EXPECT_EQ(p.evaluate(context).decision, PolicyDecision::Deny);
}
TEST(Registry, ToolNamesAndDuplicateProtection) {
  ToolRegistry r;
  r.add("echo", std::make_shared<EchoTool>());
  EXPECT_EQ(r.names().size(), 1U);
  EXPECT_THROW(r.add("echo", std::make_shared<EchoTool>()), Error);
  EXPECT_THROW(r.get("missing"), Error);
}
TEST(Registry, ProviderLookup) {
  ProviderRegistry r;
  r.add("mock", std::make_shared<MockModelProvider>());
  EXPECT_TRUE(r.get("mock")->health().healthy);
}
TEST(Registry, BatchIsAtomic) {
  ToolRegistry r;
  r.add("echo", std::make_shared<EchoTool>());
  EXPECT_THROW(
      r.add_batch({{"a", std::make_shared<EchoTool>()}, {"echo", std::make_shared<EchoTool>()}}),
      Error);
  EXPECT_EQ(r.names().size(), 1U);
}
TEST(Provider, MockIsOfflineAndStructured) {
  asio::io_context io;
  MockModelProvider p;
  ExecutionContext c{"r", "p", "n", {}, std::chrono::steady_clock::now() + Milliseconds{1000}};
  ModelRequest request{"mock", "", {{"question", "example"}}, Json::object()};
  auto future = asio::co_spawn(io, p.generate(request, c), asio::use_future);
  io.run();
  EXPECT_EQ(future.get().output.at("reviewed"), true);
  EXPECT_FALSE(p.metadata().remote);
}
TEST(Provider, LocalOpenAIAdapterRequiresLoopbackHTTP) {
  LocalOpenAICompatibleProvider provider("http://127.0.0.1:18081");
  EXPECT_TRUE(provider.metadata().network);
  EXPECT_FALSE(provider.metadata().remote);
  EXPECT_THROW(LocalOpenAICompatibleProvider("https://127.0.0.1:18081"), Error);
  EXPECT_THROW(LocalOpenAICompatibleProvider("http://example.invalid:18081"), Error);
}
TEST(Configuration, RejectsRemoteAnonymousDefault) {
  Config c;
  c.api_host = "0.0.0.0";
  EXPECT_THROW(c.validate(), Error);
  c.allow_remote_api = true;
  EXPECT_NO_THROW(c.validate());
}
TEST(Configuration, RejectsZeroConcurrency) {
  Config c;
  c.max_runs = 0;
  EXPECT_THROW(c.validate(), Error);
}
TEST(Configuration, ValidatesPerRunNodeLimit) {
  Config c;
  c.max_nodes_per_run = 0;
  EXPECT_THROW(c.validate(), Error);
  c.max_nodes_per_run = c.max_nodes + 1;
  EXPECT_THROW(c.validate(), Error);
}
TEST(PluginABI, IsPlainCVersionedBoundary) {
  EXPECT_EQ(LASO_PLUGIN_ABI_VERSION, 1U);
  EXPECT_GE(sizeof(laso_host_api), sizeof(void *) * 2);
}
