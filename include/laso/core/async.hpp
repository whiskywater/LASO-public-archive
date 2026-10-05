#pragma once
// <utility> must precede Boost.Asio on the oldest supported Boost package.
#include <boost/asio.hpp>
#include <functional>
#include <laso/core/types.hpp>
#include <stop_token>
#include <utility>

namespace laso {
namespace asio = boost::asio;
template <class T> using Task = asio::awaitable<T>;
struct ExecutionContext {
  std::string run_id, pipeline_id, node_id;
  std::stop_token stop;
  std::chrono::steady_clock::time_point deadline;
  unsigned attempt = 1, visit = 1;
  // Distributed node work supplies these only while a claimed branch is
  // executing.  They make provider jobs and durable attempts distinguishable
  // across lease takeovers without making provider transports aware of
  // NodeWork persistence.
  std::string distributed_work_id, distributed_attempt_id;
  std::function<void(const std::string &)> worker_job_started;
  std::string session_id;
  std::function<std::optional<OpaqueProviderContinuation>(const std::string &)>
      load_provider_continuation;
  std::function<void(OpaqueProviderContinuation)> stage_provider_continuation;
  void check() const;
  Task<void> delay(Milliseconds duration) const;
};
} // namespace laso
