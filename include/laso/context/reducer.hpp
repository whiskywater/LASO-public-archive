#pragma once
#include <laso/core/registry.hpp>
#include <stop_token>

namespace laso {

// The reducer receives a bounded snapshot of completed durable history. It
// must be deterministic for the same request and must not write to LASO
// storage. LASO validates and commits the returned generation.
struct ContextReductionRequest {
  std::string session_id;
  std::optional<SessionContextGeneration> previous_generation;
  Json eligible_turns = Json::array();
  std::uint64_t through_turn_sequence = 0;
  std::size_t target_payload_bytes = 0;
  std::chrono::steady_clock::time_point deadline;
  Json configuration = Json::object();
};

struct ContextReductionResult {
  std::string representation_kind;
  std::string representation_version;
  std::uint64_t through_turn_sequence = 0;
  Json payload = Json::object();
};

class ContextReducer {
public:
  virtual ~ContextReducer() = default;
  virtual ContextReductionResult reduce(const ContextReductionRequest &,
                                        std::stop_token cancellation) = 0;
};

using ContextReducerRegistry = Registry<ContextReducer>;

// A deterministic, provider-neutral selector that keeps the newest durable
// turns fitting a serialized-byte budget. It is a reference policy, not a
// natural-language summarizer or tokenizer.
class RecentTurnsContextReducer final : public ContextReducer {
public:
  ContextReductionResult reduce(const ContextReductionRequest &,
                                std::stop_token cancellation) override;
};

} // namespace laso
