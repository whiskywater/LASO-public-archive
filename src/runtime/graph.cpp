#include <algorithm>
#include <laso/pipeline/parser.hpp>
#include <laso/runtime/runtime.hpp>

namespace laso {
PolicyResult Runtime::permission(const NodeDefinition &n, const Run &r) const {
  PolicyContext c;
  c.pipeline_id = r.pipeline_id;
  c.node_id = n.id;
  c.resource = n.binding;
  c.classification = r.message.metadata.value("classification", std::string("public"));
  if (n.type == "tool") {
    auto m = deps_.tools.get(n.binding)->metadata();
    c.network = m.network;
    c.approval_required = m.approval_required;
  } else if (n.type == "agent") {
    auto binding = config_.models.find(n.binding);
    if (binding == config_.models.end())
      throw Error(ErrorCode::Provider, "Logical model not configured");
    auto m = deps_.providers.get(binding->second.provider)->metadata();
    c.remote = m.remote;
    c.network = m.network || m.remote;
  } else if (n.type == "worker") {
    if (!deps_.workers)
      throw Error(ErrorCode::Execution, "Worker manager is unavailable");
    const auto worker_id = deps_.workers->resolve_worker(n.binding, n.capability);
    const auto worker = deps_.workers->worker(worker_id);
    c.resource = worker_id;
    c.remote = worker.value("remote", false);
    c.network = c.remote;
  }
  return deps_.policy.evaluate(c);
}
std::unique_ptr<Node> Runtime::make_node(const NodeDefinition &n) {
  if (n.type == "input")
    return std::make_unique<InputNode>();
  if (n.type == "output")
    return std::make_unique<OutputNode>();
  if (n.type == "approval")
    return std::make_unique<ApprovalNode>();
  if (n.type == "parallel")
    return std::make_unique<ParallelNode>();
  if (n.type == "join")
    return std::make_unique<JoinNode>();
  if (n.type == "subpipeline")
    return std::make_unique<SubpipelineNode>();
  if (n.type == "loop")
    return std::make_unique<LoopNode>(n.max_iterations);
  if (n.type == "router")
    return std::make_unique<RouterNode>(n);
  if (n.type == "validator")
    return std::make_unique<ValidatorNode>(n, &deps_.schemas);
  if (n.type == "function")
    return std::make_unique<FunctionNode>(deps_.functions.get(n.binding));
  if (n.type == "tool")
    return std::make_unique<ToolNode>(deps_.tools.get(n.binding), tools_);
  if (n.type == "worker") {
    if (!deps_.workers)
      throw Error(ErrorCode::Execution, "Worker manager is unavailable");
    auto worker_id = deps_.workers->resolve_worker(n.binding, n.capability);
    return std::make_unique<WorkerNode>(deps_.workers, worker_id, n.task_type, n.instructions,
                                        n.capability, n.output_schema);
  }
  if (n.type == "agent") {
    auto binding = config_.models.find(n.binding);
    if (binding == config_.models.end())
      throw Error(ErrorCode::Provider, "Logical model not configured");
    return std::make_unique<AgentNode>(deps_.providers.get(binding->second.provider),
                                       binding->second, n.prompt, models_);
  }
  return (*deps_.nodes.get(n.type))(n);
}
bool Runtime::next_ready(Run &r) {
  if (r.ready.empty())
    return false;
  auto token = std::move(r.ready.front());
  r.ready.erase(r.ready.begin());
  r.active_node = std::move(token.node_id);
  r.message = std::move(token.message);
  r.frames = std::move(token.frames);
  r.prepared_join.clear();
  return true;
}
bool Runtime::prepare_join(Run &r, const NodeDefinition &n) {
  if (n.type != "join")
    return true;
  if (r.prepared_join == n.id)
    return true;
  if (r.frames.empty() || r.frames.back().join != n.id)
    throw Error(ErrorCode::Execution, "Join reached outside its matching parallel group");
  const auto frame = r.frames.back();
  auto &arrived = r.joins[frame.group].messages;
  if (!arrived.emplace(frame.index, r.message).second)
    throw Error(ErrorCode::Execution, "Parallel branch arrived twice");
  if (arrived.size() == frame.width) {
    r.message.payload = Json::array();
    for (const auto &[index, message] : arrived) {
      (void)index;
      r.message.payload.push_back(message.payload);
    }
    r.frames.pop_back();
    r.joins.erase(frame.group);
    r.prepared_join = n.id;
    return true;
  }
  if (!next_ready(r))
    throw Error(ErrorCode::Execution, "Join has missing branches");
  checkpoint(r, "join.waiting");
  return false;
}
bool Runtime::advance(Run &r, const PipelineDefinition &p, const NodeDefinition &n,
                      const std::string &condition) {
  if (n.type == "output") {
    if (!r.frames.empty() || !r.ready.empty() || !r.joins.empty())
      throw Error(ErrorCode::Execution, "Parallel branches must converge before output");
    return false;
  }
  std::vector<const EdgeDefinition *> outgoing;
  for (const auto &e : p.edges)
    if (e.from == n.id && (e.condition.empty() || e.condition == condition))
      outgoing.push_back(&e);
  if (outgoing.empty())
    throw Error(ErrorCode::Execution, "No matching outgoing edge");
  if (n.type != "parallel" && outgoing.size() != 1)
    throw Error(ErrorCode::Execution, "Ambiguous routing result");
  std::string group = uuid();
  for (std::size_t i = 0; i < outgoing.size(); ++i) {
    const auto &e = *outgoing[i];
    auto key = e.from + "->" + e.to;
    if (e.max_iterations && ++r.edge_visits[key] > e.max_iterations)
      throw Error(ErrorCode::Execution, "Edge iteration limit exceeded");
    auto frames = r.frames;
    if (n.type == "parallel")
      frames.push_back(
          {group, n.join, static_cast<unsigned>(outgoing.size()), static_cast<unsigned>(i)});
    r.ready.push_back({e.to, r.message, std::move(frames)});
  }
  if (r.ready.size() > 256)
    throw Error(ErrorCode::Execution, "Ready branch limit exceeded");
  if (n.type == "parallel")
    return true;
  return next_ready(r);
}
} // namespace laso
