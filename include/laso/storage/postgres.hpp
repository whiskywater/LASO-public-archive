#pragma once
#include <laso/storage/postgres_pool.hpp>
#include <laso/storage/storage.hpp>
#include <memory>
#include <string>

namespace laso {
class PostgresStorage final : public Storage {
public:
  PostgresStorage(const std::string &dsn, const std::string &schema = "public",
                  PostgresPoolOptions pool_options = {}, bool allow_multiple_processes = false);
  ~PostgresStorage() override;
  PostgresStorage(const PostgresStorage &) = delete;
  PostgresStorage &operator=(const PostgresStorage &) = delete;
  void commit(const std::vector<Record> &) override;
  void commit_owned(const std::vector<Record> &, const std::string &, const std::string &,
                    std::uint64_t) override;
  void request_cancellation(const std::string &) override;
  bool claim(const Record &, const std::vector<Record> &associated = {}) override;
  bool submit_session_turn(const std::string &, const std::string &, const Json &, Json) override;
  std::optional<Json> claim_next_session_turn(const std::string &, const std::string &,
                                              std::uint64_t, const std::string &, Json) override;
  bool bind_session_turn_run(const std::string &, const std::string &, Json, Json,
                             const std::string &, std::uint64_t, Json) override;
  void commit_session_run(const std::vector<Record> &, const std::string &, std::uint64_t,
                          Json) override;
  bool close_agent_session(const std::string &, Json) override;
  std::vector<Json> session_events(const std::string &, std::uint64_t, std::size_t) const override;
  Json get(RecordKind, const std::string &) const override;
  std::vector<Json> list(RecordKind, const std::string &run_id = "", std::size_t limit = 1000,
                         std::size_t offset = 0) const override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace laso
