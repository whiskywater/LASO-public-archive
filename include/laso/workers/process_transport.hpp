#pragma once

#include <laso/core/config.hpp>
#include <laso/workers/process_protocol.hpp>
#include <laso/workers/worker.hpp>
#include <memory>

namespace laso {
// A supervised local process implementation of the backend-neutral worker
// contract. The POSIX implementation details stay in the .cpp file.
class ProcessWorkerTransport final : public WorkerTransport {
public:
  ProcessWorkerTransport(std::string id, ProcessWorkerConfig config);
  ~ProcessWorkerTransport() noexcept override;

  WorkerMetadata metadata() const override;
  WorkerSubmission submit(const WorkerRequest &) override;
  WorkerStatus status(const std::string &external_job_id) override;
  WorkerStatus result(const std::string &external_job_id) override;
  bool cancel(const std::string &external_job_id) override;
  bool cancel_pending(const std::string &job_id) override;
  void set_interaction_handler(WorkerInteractionHandler) override;
  void start() override;
  void stop() noexcept override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace laso
