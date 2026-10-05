#include <laso/context/reducer.hpp>

namespace laso {
namespace {
void check_reduction(const ContextReductionRequest &request, const std::stop_token &cancellation) {
  if (cancellation.stop_requested())
    throw Error(ErrorCode::Cancellation, "Context reduction was cancelled");
  if (std::chrono::steady_clock::now() >= request.deadline)
    throw Error(ErrorCode::Timeout, "Context reduction deadline expired");
}
} // namespace

ContextReductionResult RecentTurnsContextReducer::reduce(const ContextReductionRequest &request,
                                                         std::stop_token cancellation) {
  check_reduction(request, cancellation);
  if (!request.eligible_turns.is_array() || request.target_payload_bytes == 0)
    throw Error(ErrorCode::Validation, "Invalid context reduction request");

  Json turns = Json::array();
  if (request.previous_generation) {
    const auto &previous = *request.previous_generation;
    if (previous.representation_kind != "laso.recent-turns" ||
        previous.representation_version != "1" || !previous.payload.is_object() ||
        !previous.payload.contains("turns") || !previous.payload.at("turns").is_array())
      throw Error(ErrorCode::Validation,
                  "Reference reducer cannot interpret the previous context representation");
    turns = previous.payload.at("turns");
  }
  for (const auto &turn : request.eligible_turns) {
    check_reduction(request, cancellation);
    if (!turn.is_object() || !turn.contains("sequence") ||
        !turn.at("sequence").is_number_unsigned())
      throw Error(ErrorCode::Validation, "Malformed durable turn supplied to context reducer");
    if (!turns.empty() && turn.at("sequence").get<std::uint64_t>() <=
                              turns.back().at("sequence").get<std::uint64_t>())
      throw Error(ErrorCode::Validation, "Context reducer input is not ordered");
    turns.push_back(turn);
  }

  // Remove only from the derived representation. Original turns remain in
  // durable history and can be selected by another reducer later.
  while (!turns.empty()) {
    Json payload{{"format", "laso.recent-turns"}, {"turns", turns}};
    if (payload.dump().size() <= request.target_payload_bytes)
      return {"laso.recent-turns", "1", request.through_turn_sequence, std::move(payload)};
    if (turns.size() == 1)
      break;
    turns.erase(turns.begin());
  }
  throw Error(ErrorCode::Capacity, "A durable turn exceeds the context reduction budget");
}

} // namespace laso
