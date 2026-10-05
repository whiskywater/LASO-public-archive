#pragma once
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <gtest/gtest.h>
#include <laso/application/service.hpp>
#include <laso/pipeline/parser.hpp>
#include <version>
#ifdef __cpp_lib_source_location
#undef __cpp_lib_source_location
#endif
#include <mutex>
#include <pqxx/pqxx>
#include <vector>

namespace laso::test {
inline std::string test_dsn() {
  const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN");
  if (!dsn || !*dsn)
    throw Error(ErrorCode::Configuration, "LASO_TEST_POSTGRES_DSN is required for tests");
  return dsn;
}
inline std::mutex &schema_registry_mutex() {
  static std::mutex mutex;
  return mutex;
}
inline std::vector<std::pair<std::string, std::string>> &schema_registry() {
  static std::vector<std::pair<std::string, std::string>> schemas;
  return schemas;
}
inline std::string schema_for(const std::filesystem::path &path) {
  const auto value = path.lexically_normal().string();
  std::uint64_t hash = 1469598103934665603ULL;
  for (const auto c : value) {
    hash ^= static_cast<unsigned char>(c);
    hash *= 1099511628211ULL;
  }
  const auto schema = "laso_test_" + std::to_string(hash);
  {
    std::lock_guard lock(schema_registry_mutex());
    const auto parent = path.parent_path().lexically_normal().string();
    if (std::find(schema_registry().begin(), schema_registry().end(), std::pair{parent, schema}) ==
        schema_registry().end())
      schema_registry().emplace_back(parent, schema);
  }
  return schema;
}
inline void drop_schema(const std::string &schema) noexcept {
  try {
    pqxx::connection connection(test_dsn());
    pqxx::work transaction(connection);
    transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
    transaction.commit();
  } catch (...) {
  }
}
struct TemporaryDirectory {
  std::filesystem::path path = std::filesystem::temp_directory_path() / ("laso-test-" + uuid());
  TemporaryDirectory() {
    std::filesystem::create_directories(path);
  }
  ~TemporaryDirectory() {
    std::vector<std::string> schemas;
    {
      std::lock_guard lock(schema_registry_mutex());
      for (auto i = schema_registry().begin(); i != schema_registry().end();) {
        if (i->first == path.lexically_normal().string()) {
          schemas.push_back(i->second);
          i = schema_registry().erase(i);
        } else
          ++i;
      }
    }
    for (const auto &schema : schemas)
      drop_schema(schema);
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};
inline Config config(const std::filesystem::path &dir) {
  Config c;
  c.data_dir = dir;
  c.postgres_dsn = test_dsn();
  c.postgres_schema = schema_for(dir / "service");
  c.validate();
  return c;
}
inline std::unique_ptr<Storage> make_storage(const std::filesystem::path &path) {
  StorageOptions options;
  options.postgres_dsn = test_dsn();
  options.postgres_schema = schema_for(path);
  options.allow_multiple_processes = true;
  return create_storage(options);
}
struct StorageFixture {
  std::string name;
  std::function<std::unique_ptr<Storage>(const std::filesystem::path &)> open;
  std::function<void()> cleanup;
};
inline std::vector<StorageFixture> storage_fixtures() {
  const auto schema = "laso_test_" + uuid();
  auto safe_schema = schema;
  std::replace(safe_schema.begin(), safe_schema.end(), '-', '_');
  const auto dsn = test_dsn();
  return {{"postgres",
           [dsn, safe_schema](const std::filesystem::path &) {
             StorageOptions options;
             options.postgres_dsn = dsn;
             options.postgres_schema = safe_schema;
             return create_storage(options);
           },
           [dsn, safe_schema] {
             pqxx::connection connection(dsn);
             pqxx::work transaction(connection);
             transaction.exec("DROP SCHEMA IF EXISTS \"" + safe_schema + "\" CASCADE");
             transaction.commit();
           }}};
}
template <typename Function> inline void for_each_storage_fixture(Function &&function) {
  for (auto &fixture : storage_fixtures()) {
    SCOPED_TRACE(fixture.name);
    try {
      function(fixture);
    } catch (...) {
      if (fixture.cleanup)
        fixture.cleanup();
      throw;
    }
    if (fixture.cleanup)
      fixture.cleanup();
  }
}
inline std::string fixture(const std::string &name) {
  return read_document(std::filesystem::path(LASO_SOURCE_DIR) / "examples" / name /
                       "pipeline.yaml");
}
inline std::string single(const std::string &node = "type: function\n    function: identity",
                          const std::string &extra = "") {
  return "laso: '1'\nname: test\nversion: 1\n" + extra + "nodes:\n  action:\n    " + node +
         "\nedges:\n  - {from: input, to: action}\n  - {from: action, to: output}\n";
}
inline laso::Run execute(Service &service, asio::io_context &io, const std::string &yaml,
                         Json input = Json::object()) {
  auto name = service.register_pipeline(yaml).at("name").get<std::string>();
  auto id = service.start(name, input);
  io.restart();
  io.run();
  return service.get(RecordKind::Run, id).get<laso::Run>();
}
} // namespace laso::test
