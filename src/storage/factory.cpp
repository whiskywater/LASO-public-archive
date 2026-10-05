#include <laso/storage/factory.hpp>
#include <laso/storage/postgres.hpp>

namespace laso {
std::unique_ptr<Storage> create_storage(const StorageOptions &options) {
  if (options.postgres_dsn.empty())
    throw Error(ErrorCode::Configuration, "PostgreSQL DSN is required");
  return std::make_unique<PostgresStorage>(
      options.postgres_dsn, options.postgres_schema,
      PostgresPoolOptions{options.postgres_pool_min_connections,
                          options.postgres_pool_max_connections,
                          options.postgres_pool_acquisition_timeout_ms},
      options.allow_multiple_processes);
}
} // namespace laso
