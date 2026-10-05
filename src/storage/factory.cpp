#include <laso/storage/factory.hpp>
#include <laso/storage/sqlite.hpp>
#if defined(LASO_HAS_POSTGRES)
#include <laso/storage/postgres.hpp>
#endif

namespace laso {
std::unique_ptr<Storage> create_storage(const StorageOptions &options) {
  if (options.backend == "sqlite")
    return std::make_unique<SQLiteStorage>(options.db_path);
#if defined(LASO_HAS_POSTGRES)
  if (options.backend == "postgres")
    return std::make_unique<PostgresStorage>(
        options.postgres_dsn, options.postgres_schema,
        PostgresPoolOptions{options.postgres_pool_min_connections,
                            options.postgres_pool_max_connections,
                            options.postgres_pool_acquisition_timeout_ms},
        options.allow_multiple_processes);
#endif
  throw Error(ErrorCode::Configuration, "Unsupported storage backend");
}
} // namespace laso
