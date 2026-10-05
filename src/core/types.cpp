#include <array>
#include <cerrno>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <laso/core/types.hpp>
#include <sstream>
#include <sys/random.h>

namespace laso {
std::string uuid() {
  std::array<unsigned char, 16> bytes{};
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    auto count = getrandom(bytes.data() + offset, bytes.size() - offset, 0);
    if (count < 0 && errno == EINTR)
      continue;
    if (count <= 0)
      throw Error(ErrorCode::Execution, "System random source unavailable");
    offset += static_cast<std::size_t>(count);
  }
  bytes[6] = (bytes[6] & 0x0fU) | 0x40U;
  bytes[8] = (bytes[8] & 0x3fU) | 0x80U;
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10)
      out << '-';
    out << std::setw(2) << static_cast<unsigned>(bytes[i]);
  }
  return out.str();
}
std::string timestamp() {
  const auto now = std::chrono::system_clock::now();
  const auto time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&time, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(3) << std::setfill('0')
      << (std::chrono::duration_cast<Milliseconds>(now.time_since_epoch()).count() % 1000) << 'Z';
  return out.str();
}
bool terminal(RunState state) {
  return state == RunState::Completed || state == RunState::Failed ||
         state == RunState::Cancelled || state == RunState::TimedOut;
}
bool valid_transition(RunState from, RunState to) {
  if (terminal(from))
    return false;
  switch (from) {
  case RunState::Queued:
    return to == RunState::Starting || to == RunState::Cancelled || to == RunState::Failed;
  case RunState::Starting:
    return to == RunState::Running || to == RunState::Failed || to == RunState::Cancelled ||
           to == RunState::TimedOut;
  case RunState::Running:
    return to == RunState::Completed || to == RunState::WaitingModel ||
           to == RunState::WaitingTool || to == RunState::WaitingWorker ||
           to == RunState::WaitingApproval || to == RunState::Retrying || to == RunState::Paused ||
           to == RunState::Failed || to == RunState::Cancelled || to == RunState::TimedOut;
  case RunState::WaitingApproval:
  case RunState::Paused:
    return to == RunState::Queued || to == RunState::Failed || to == RunState::Cancelled;
  case RunState::WaitingTool:
  case RunState::WaitingModel:
  case RunState::WaitingWorker:
  case RunState::Retrying:
    return to == RunState::Running || to == RunState::Failed || to == RunState::Cancelled ||
           to == RunState::TimedOut;
  default:
    return false;
  }
}
void to_json(Json &j, const Message &m) {
  j = {{"id", m.id},
       {"run_id", m.run_id},
       {"pipeline_id", m.pipeline_id},
       {"node_id", m.node_id},
       {"type", m.type},
       {"timestamp", m.time},
       {"payload", m.payload},
       {"metadata", m.metadata},
       {"provenance", m.provenance},
       {"confidence", m.confidence ? Json(*m.confidence) : Json(nullptr)}};
}
void from_json(const Json &j, Message &m) {
  j.at("id").get_to(m.id);
  j.at("run_id").get_to(m.run_id);
  j.at("pipeline_id").get_to(m.pipeline_id);
  j.at("node_id").get_to(m.node_id);
  j.at("type").get_to(m.type);
  j.at("timestamp").get_to(m.time);
  m.payload = j.at("payload");
  m.metadata = j.at("metadata");
  j.at("provenance").get_to(m.provenance);
  m.confidence.reset();
  if (!j.at("confidence").is_null()) {
    m.confidence = j.at("confidence").get<double>();
    if (!std::isfinite(*m.confidence) || *m.confidence < 0 || *m.confidence > 1)
      throw Error(ErrorCode::Validation, "Confidence outside [0,1]");
  }
}
} // namespace laso
