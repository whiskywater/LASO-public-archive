#pragma once
#include <filesystem>
#include <laso/storage/storage.hpp>
#include <memory>
#include <string>

namespace laso {
struct StorageOptions {
  std::string backend = "sqlite";
  std::filesystem::path db_path;
  std::string postgres_dsn;
  std::string postgres_schema = "public";
  std::size_t postgres_pool_min_connections = 1;
  std::size_t postgres_pool_max_connections = 4;
  std::uint64_t postgres_pool_acquisition_timeout_ms = 1000;
  bool allow_multiple_processes = false;
};

std::unique_ptr<Storage> create_storage(const StorageOptions &options);
} // namespace laso
