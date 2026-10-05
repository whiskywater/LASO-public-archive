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
  Json get(RecordKind, const std::string &) const override;
  std::vector<Json> list(RecordKind, const std::string &run_id = "", std::size_t limit = 1000,
                         std::size_t offset = 0) const override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace laso
