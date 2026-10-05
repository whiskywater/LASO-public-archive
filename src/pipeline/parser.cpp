#include "yaml.hpp"
#include <fstream>
#include <functional>
#include <laso/pipeline/parser.hpp>
#include <regex>
#include <set>
#include <yaml-cpp/yaml.h>

namespace laso {
namespace {
void inspect(const YAML::Node &node, unsigned depth, std::size_t &count) {
  if (depth > 32 || ++count > 20000)
    throw Error(ErrorCode::Validation, "YAML structure limit exceeded");
  const auto tag = node.Tag();
  static const std::set<std::string> tags = {"",
                                             "?",
                                             "!",
                                             "tag:yaml.org,2002:map",
                                             "tag:yaml.org,2002:seq",
                                             "tag:yaml.org,2002:str",
                                             "tag:yaml.org,2002:int",
                                             "tag:yaml.org,2002:float",
                                             "tag:yaml.org,2002:bool",
                                             "tag:yaml.org,2002:null"};
  if (!tags.contains(tag))
    throw Error(ErrorCode::Validation, "Custom YAML tags are forbidden");
  if (node.IsMap()) {
    std::set<std::string> keys;
    for (const auto &pair : node) {
      if (!pair.first.IsScalar() || !keys.insert(pair.first.as<std::string>()).second)
        throw Error(ErrorCode::Validation, "Duplicate or non-scalar YAML key");
      inspect(pair.second, depth + 1, count);
    }
  } else if (node.IsSequence())
    for (const auto &child : node)
      inspect(child, depth + 1, count);
}
void keys(const YAML::Node &node, const std::set<std::string> &allowed) {
  if (!node.IsMap())
    throw Error(ErrorCode::Validation, "Expected a mapping");
  for (const auto &item : node)
    if (!allowed.contains(item.first.as<std::string>()))
      throw Error(ErrorCode::Validation, "Unknown configuration field");
}
std::string str(const YAML::Node &n, const char *key, const std::string &fallback = "") {
  return n[key] ? n[key].as<std::string>() : fallback;
}
unsigned number(const YAML::Node &n, const char *key, unsigned fallback, unsigned min,
                unsigned max) {
  auto value = n[key] ? n[key].as<long long>() : static_cast<long long>(fallback);
  if (value < min || value > max)
    throw Error(ErrorCode::Validation, "Numeric configuration out of bounds");
  return static_cast<unsigned>(value);
}
Json yaml_value_impl(const YAML::Node &n) {
  if (!n || n.IsNull())
    return nullptr;
  if (n.IsSequence()) {
    Json j = Json::array();
    for (const auto &v : n)
      j.push_back(yaml_value_impl(v));
    return j;
  }
  if (n.IsMap()) {
    Json j = Json::object();
    for (const auto &v : n)
      j[v.first.as<std::string>()] = yaml_value_impl(v.second);
    return j;
  }
  const auto s = n.as<std::string>();
  if (n.Tag() == "!")
    return s;
  auto parsed = Json::parse(s, nullptr, false);
  return parsed.is_discarded() ? Json(s) : parsed;
}
bool identifier(const std::string &s) {
  static const std::regex pattern("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}");
  return std::regex_match(s, pattern);
}
} // namespace
Json detail::yaml_value(const YAML::Node &n) {
  return yaml_value_impl(n);
}
YAML::Node detail::load_safe_yaml(const std::string &text) {
  if (text.size() > max_document_bytes)
    throw Error(ErrorCode::Validation, "Configuration exceeds 1 MiB");
  auto documents = YAML::LoadAll(text);
  if (documents.size() != 1)
    throw Error(ErrorCode::Validation, "Exactly one YAML document is required");
  std::size_t count = 0;
  inspect(documents.front(), 0, count);
  return documents.front();
}
std::string read_document(const std::filesystem::path &path) {
  std::ifstream file(path, std::ios::binary);
  if (!file)
    throw Error(ErrorCode::Configuration, "Cannot open configuration file");
  std::string text(max_document_bytes + 1, '\0');
  file.read(text.data(), static_cast<std::streamsize>(text.size()));
  text.resize(static_cast<std::size_t>(file.gcount()));
  if (text.size() > max_document_bytes)
    throw Error(ErrorCode::Validation, "Configuration exceeds 1 MiB");
  return text;
}
PipelineReference parse_pipeline_reference(const std::string &text) {
  const auto at = text.find('@');
  if (at == std::string::npos) {
    if (!identifier(text))
      throw Error(ErrorCode::Validation, "Invalid pipeline reference");
    return {text, 0, false};
  }
  if (at == 0 || at + 1 >= text.size() || text.find('@', at + 1) != std::string::npos ||
      !identifier(text.substr(0, at)))
    throw Error(ErrorCode::Validation, "Invalid versioned pipeline reference");
  unsigned version = 0;
  const auto digits = text.substr(at + 1);
  try {
    std::size_t end = 0;
    const auto parsed = std::stoul(digits, &end);
    if (end != digits.size() || parsed == 0 || parsed > 1000000)
      throw std::out_of_range("version");
    version = static_cast<unsigned>(parsed);
  } catch (...) {
    throw Error(ErrorCode::Validation, "Invalid pipeline reference version");
  }
  return {text.substr(0, at), version, true};
}
std::string pipeline_reference(const std::string &name, unsigned version) {
  return name + "@" + std::to_string(version);
}
PipelineDefinition parse_pipeline(const std::string &text,
                                  const std::set<std::string> &extensions) {
  if (text.size() > max_document_bytes)
    throw Error(ErrorCode::Validation, "Configuration exceeds 1 MiB");
  try {
    const auto root = detail::load_safe_yaml(text);
    keys(root, {"laso", "name", "version", "nodes", "edges", "max_steps", "timeout_ms",
                "input_schema", "output_schema"});
    if (!root["laso"] || !root["version"] || !root["name"] || !root["nodes"] || !root["edges"])
      throw Error(ErrorCode::Validation, "Required pipeline fields missing");
    PipelineDefinition p;
    p.source = text;
    p.name = str(root, "name");
    p.schema_version = number(root, "laso", 1, 1, 1);
    p.version = number(root, "version", 1, 1, 1000000);
    p.input_schema = str(root, "input_schema");
    p.output_schema = str(root, "output_schema");
    p.max_steps = number(root, "max_steps", 1000, 1, 100000);
    p.timeout.timeout = Milliseconds(number(root, "timeout_ms", 300000, 1, 86400000));
    if (!root["nodes"].IsMap() || !root["edges"].IsSequence())
      throw Error(ErrorCode::Validation, "Nodes must be a map and edges a sequence");
    for (const auto &item : root["nodes"]) {
      const auto n = item.second;
      keys(n, {"type",         "function",      "tool",           "model",      "pipeline",
               "prompt",       "field",         "value",          "condition",  "reason",
               "join",         "max_attempts",  "retry_delay_ms", "timeout_ms", "max_iterations",
               "input_schema", "output_schema", "schema",         "worker",     "task_type",
               "capability",   "instructions"});
      NodeDefinition d;
      d.id = item.first.as<std::string>();
      d.type = str(n, "type");
      unsigned bindings = 0;
      for (auto key : {"function", "tool", "model", "pipeline", "worker"})
        if (n[key]) {
          d.binding = str(n, key);
          ++bindings;
        }
      if (bindings > 1)
        throw Error(ErrorCode::Validation, "Only one node binding is permitted");
      const auto expected = d.type == "agent"         ? "model"
                            : d.type == "subpipeline" ? "pipeline"
                            : d.type == "worker"      ? "worker"
                                                      : d.type.c_str();
      if (bindings && !n[expected])
        throw Error(ErrorCode::Validation, "Binding field does not match node type");
      d.prompt = str(n, "prompt");
      d.field = str(n, "field");
      d.condition = str(n, "condition");
      d.reason = str(n, "reason", "Human review requested");
      d.join = str(n, "join");
      d.input_schema = str(n, "input_schema");
      d.output_schema = str(n, "output_schema");
      d.schema = str(n, "schema");
      d.task_type = str(n, "task_type");
      d.capability = str(n, "capability");
      d.instructions = str(n, "instructions", d.prompt);
      d.value = detail::yaml_value(n["value"]);
      d.retry.max_attempts = number(n, "max_attempts", 1, 1, 10);
      d.retry.delay = Milliseconds(number(n, "retry_delay_ms", 0, 0, 60000));
      d.timeout.timeout = Milliseconds(number(n, "timeout_ms", 30000, 1, 3600000));
      d.max_iterations = number(n, "max_iterations", 0, 0, 10000);
      p.nodes.emplace(d.id, std::move(d));
    }
    NodeDefinition input;
    input.id = "input";
    input.type = "input";
    p.nodes.try_emplace(input.id, std::move(input));
    NodeDefinition output;
    output.id = "output";
    output.type = "output";
    p.nodes.try_emplace(output.id, std::move(output));
    for (const auto &e : root["edges"]) {
      keys(e, {"from", "to", "condition", "max_iterations"});
      p.edges.push_back({str(e, "from"), str(e, "to"), str(e, "condition"),
                         number(e, "max_iterations", 0, 0, 10000)});
    }
    validate_pipeline(p, extensions);
    return p;
  } catch (const YAML::Exception &) {
    throw Error(ErrorCode::Validation, "Malformed YAML or field type");
  }
}
void validate_pipeline(const PipelineDefinition &p, const std::set<std::string> &extensions) {
  static const std::set<std::string> types = {
      "input",    "output",   "function", "agent", "tool",        "router", "validator",
      "approval", "parallel", "join",     "loop",  "subpipeline", "worker"};
  if (p.schema_version != 1 || p.version == 0 || p.max_steps == 0 || p.max_steps > 100000 ||
      !p.nodes.contains("input") || !p.nodes.contains("output") || !identifier(p.name) ||
      p.nodes.size() > 256 || p.edges.size() > 1024)
    throw Error(ErrorCode::Validation, "Invalid pipeline name or graph size");
  if (p.nodes.at("input").type != "input" || p.nodes.at("output").type != "output")
    throw Error(ErrorCode::Validation, "Reserved boundary nodes have incorrect types");
  for (const auto &[id, n] : p.nodes) {
    if ((n.type == "input" && id != "input") || (n.type == "output" && id != "output"))
      throw Error(ErrorCode::Validation, "Input/output types are reserved for boundary nodes");
    if (!identifier(id) || (!types.contains(n.type) && !extensions.contains(n.type)))
      throw Error(ErrorCode::Validation, "Invalid node ID or unknown type");
    if ((n.type == "tool" || n.type == "function" || n.type == "agent") && !identifier(n.binding))
      throw Error(ErrorCode::Validation, "A logical binding is required");
    if (n.type == "worker" && n.binding.empty() && n.capability.empty())
      throw Error(ErrorCode::Validation, "Worker requires a binding or capability");
    if (n.type == "worker" && (!n.binding.empty() && !identifier(n.binding)))
      throw Error(ErrorCode::Validation, "Invalid worker binding");
    if (n.type == "worker" && n.capability.size() > 128)
      throw Error(ErrorCode::Validation, "Invalid worker capability");
    if (n.type == "subpipeline")
      (void)parse_pipeline_reference(n.binding);
    if (n.type == "loop" && n.max_iterations == 0)
      throw Error(ErrorCode::Validation, "Loop requires max_iterations");
    if (n.type == "parallel" && (!p.nodes.contains(n.join) || p.nodes.at(n.join).type != "join"))
      throw Error(ErrorCode::Validation, "Parallel node requires an existing join node");
    if ((n.type == "router" || n.type == "validator") && n.field.empty() &&
        (n.type != "validator" || n.schema.empty()))
      throw Error(ErrorCode::Validation, "Router/validator requires a top-level field");
  }
  std::map<std::string, std::vector<const EdgeDefinition *>> outgoing;
  std::set<std::pair<std::string, std::string>> paths;
  for (const auto &e : p.edges) {
    if (!identifier(e.from) || !identifier(e.to) || !p.nodes.contains(e.from) ||
        !p.nodes.contains(e.to) || e.to == "input" || e.from == "output")
      throw Error(ErrorCode::Validation, "Invalid edge reference");
    if (!e.condition.empty() && e.condition != "accepted" && e.condition != "rejected" &&
        e.condition != "repeat" && e.condition != "done")
      throw Error(ErrorCode::Validation, "Unsupported edge condition");
    if (!paths.emplace(e.from, e.to).second)
      throw Error(ErrorCode::Validation, "Duplicate edge");
    outgoing[e.from].push_back(&e);
  }
  for (const auto &[id, n] : p.nodes) {
    if (id == "output")
      continue;
    const auto &edges = outgoing[id];
    if (edges.empty())
      throw Error(ErrorCode::Validation, "Every non-output node needs an edge");
    if (n.type == "parallel") {
      if (edges.size() < 2 || edges.size() > 16)
        throw Error(ErrorCode::Validation, "Parallel requires 2..16 branches");
      for (auto e : edges)
        if (!e->condition.empty())
          throw Error(ErrorCode::Validation, "Parallel edges must be unconditional");
      continue;
    }
    if (n.type == "router" || n.type == "validator" || n.type == "loop") {
      std::set<std::string> conditions;
      for (auto e : edges)
        conditions.insert(e->condition);
      auto expected = n.type == "loop" ? std::set<std::string>{"repeat", "done"}
                                       : std::set<std::string>{"accepted", "rejected"};
      if (conditions != expected || edges.size() != 2)
        throw Error(ErrorCode::Validation, "Conditional nodes require exactly two named branches");
    } else if (edges.size() != 1 || !edges.front()->condition.empty())
      throw Error(ErrorCode::Validation, "Ambiguous outgoing edges");
  }
  // Removing all bounded edges must leave a DAG: every cycle spends a finite budget.
  std::map<std::string, int> colors;
  std::function<void(const std::string &)> visit = [&](const std::string &id) {
    if (colors[id] == 1)
      throw Error(ErrorCode::Validation, "Cycle has no bounded edge");
    if (colors[id] == 2)
      return;
    colors[id] = 1;
    for (auto e : outgoing[id])
      if (e->max_iterations == 0)
        visit(e->to);
    colors[id] = 2;
  };
  for (const auto &[id, n] : p.nodes) {
    (void)n;
    visit(id);
  }
  std::set<std::string> reachable;
  std::function<void(const std::string &)> reach = [&](const std::string &id) {
    if (!reachable.insert(id).second)
      return;
    for (auto e : outgoing[id])
      reach(e->to);
  };
  reach("input");
  if (reachable.size() != p.nodes.size())
    throw Error(ErrorCode::Validation, "Unreachable node");
}
} // namespace laso
