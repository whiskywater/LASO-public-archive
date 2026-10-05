#pragma once
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <gtest/gtest.h>
#include <laso/application/service.hpp>
#include <laso/pipeline/parser.hpp>
#if defined(LASO_HAS_POSTGRES)
#include <version>
#ifdef __cpp_lib_source_location
#undef __cpp_lib_source_location
#endif
#include <pqxx/pqxx>
#endif
#include <vector>

namespace laso::test {
struct TemporaryDirectory {
  std::filesystem::path path = std::filesystem::temp_directory_path() / ("laso-test-" + uuid());
  TemporaryDirectory() {
    std::filesystem::create_directories(path);
  }
  ~TemporaryDirectory() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};
inline Config config(const std::filesystem::path &dir) {
  Config c;
  c.data_dir = dir;
  c.validate();
  return c;
}
inline std::unique_ptr<Storage> make_storage(const std::filesystem::path &path) {
  StorageOptions options;
  options.backend = "sqlite";
  options.db_path = path;
  return create_storage(options);
}
struct StorageBackend {
  std::string name;
  std::function<std::unique_ptr<Storage>(const std::filesystem::path &)> open;
  std::function<void()> cleanup;
};
inline std::vector<StorageBackend> storage_backends() {
  std::vector<StorageBackend> backends;
  backends.push_back({"sqlite", make_storage, {}});
#if defined(LASO_HAS_POSTGRES)
  if (const auto *dsn = std::getenv("LASO_TEST_POSTGRES_DSN"); dsn && *dsn) {
    const auto dsn_copy = std::string(dsn);
    auto schema = "laso_test_" + uuid();
    std::replace(schema.begin(), schema.end(), '-', '_');
    backends.push_back({"postgres",
                        [dsn_copy, schema](const std::filesystem::path &) {
                          StorageOptions options;
                          options.backend = "postgres";
                          options.postgres_dsn = dsn_copy;
                          options.postgres_schema = schema;
                          return create_storage(options);
                        },
                        [dsn_copy, schema] {
                          pqxx::connection connection(dsn_copy);
                          pqxx::work transaction(connection);
                          transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
                          transaction.commit();
                        }});
  }
#endif
  return backends;
}
template <typename Function> inline void for_each_storage_backend(Function &&function) {
  for (auto &backend : storage_backends()) {
    SCOPED_TRACE(backend.name);
    try {
      function(backend);
    } catch (...) {
      if (backend.cleanup)
        backend.cleanup();
      throw;
    }
    if (backend.cleanup)
      backend.cleanup();
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
