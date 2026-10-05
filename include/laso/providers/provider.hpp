#pragma once
#include <laso/core/async.hpp>
#include <laso/core/registry.hpp>

namespace laso {
enum class ContinuationMode { Unsupported, Stateless, Opaque };
inline const char *continuation_mode_name(ContinuationMode mode) {
  switch (mode) {
  case ContinuationMode::Unsupported:
    return "unsupported";
  case ContinuationMode::Stateless:
    return "stateless";
  case ContinuationMode::Opaque:
    return "opaque";
  }
  return "unsupported";
}
struct ProviderMetadata {
  std::string name, version, plugin;
  std::size_t context_size = 4096;
  bool remote = false, streaming = false, network = false;
  Milliseconds timeout{30000};
  std::vector<std::string> capabilities{"structured-output"};
  // Opaque providers promise to consume and return continuation state. Stateless
  // providers explicitly declare that every turn is independent.
  ContinuationMode continuation_mode = ContinuationMode::Unsupported;
};
struct ModelRequest {
  std::string model, prompt;
  Json input = Json::object(), options = Json::object();
  std::optional<OpaqueProviderContinuation> continuation;
};
struct ModelResponse {
  Json output = Json::object();
  std::string model, provider;
  std::optional<OpaqueProviderContinuation> continuation;
};
struct ProviderHealth {
  bool healthy;
  std::string detail;
};
class ModelProvider {
public:
  virtual ~ModelProvider() = default;
  virtual Task<ModelResponse> generate(const ModelRequest &, ExecutionContext &) = 0;
  virtual ProviderHealth health() const = 0;
  virtual ProviderMetadata metadata() const = 0;
};
class MockModelProvider final : public ModelProvider {
public:
  Task<ModelResponse> generate(const ModelRequest &r, ExecutionContext &c) override {
    c.check();
    Json output = r.input;
    if (!output.is_object())
      output = Json{{"input", output}};
    output["reviewed"] = true;
    output["text"] = "Offline mock response";
    ModelResponse response{output, r.model, "mock"};
    response.continuation = OpaqueProviderContinuation{
        "mock", "1", r.continuation ? r.continuation->state + ":next" : "mock-state-1"};
    co_return response;
  }
  ProviderHealth health() const override {
    return {true, "offline"};
  }
  ProviderMetadata metadata() const override {
    ProviderMetadata result;
    result.name = "mock";
    result.version = "1";
    result.continuation_mode = ContinuationMode::Opaque;
    return result;
  }
};
// An optional adapter for an OpenAI-compatible service bound to the local host.
// It deliberately accepts only clear-text loopback endpoints; TLS termination and
// remote provider adapters belong outside the v0.1 local baseline.
class LocalOpenAICompatibleProvider final : public ModelProvider {
public:
  explicit LocalOpenAICompatibleProvider(const std::string &endpoint);
  Task<ModelResponse> generate(const ModelRequest &, ExecutionContext &) override;
  ProviderHealth health() const override;
  ProviderMetadata metadata() const override;

private:
  std::string host_, port_;
};
using ProviderRegistry = Registry<ModelProvider>;
struct ModelBinding {
  std::string provider, model;
  Json options = Json::object();
};
} // namespace laso
