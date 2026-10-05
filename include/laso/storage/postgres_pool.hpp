#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

// This header is PostgreSQL-specific. Generic storage and coordination
// interfaces do not expose pqxx types; this class is the bounded adapter used
// by the optional PostgreSQL implementation and its tests.
namespace pqxx {
class connection;
}

namespace laso {
struct PostgresPoolOptions {
  std::size_t min_connections = 1;
  std::size_t max_connections = 4;
  std::uint64_t acquisition_timeout_ms = 1000;
};

struct PostgresPoolDiagnostics {
  std::size_t size = 0;
  std::size_t in_use = 0;
  std::uint64_t acquisition_timeouts = 0;
  std::uint64_t replacements = 0;
};

class PostgresConnectionPool {
public:
  class Lease {
  public:
    struct State;
    Lease() = default;
    ~Lease();
    Lease(const Lease &) = delete;
    Lease &operator=(const Lease &) = delete;
    Lease(Lease &&) noexcept;
    Lease &operator=(Lease &&) noexcept;

    pqxx::connection &connection() const;
    void mark_broken() noexcept;
    explicit operator bool() const noexcept;

  private:
    std::shared_ptr<State> state_;
    std::size_t index_ = static_cast<std::size_t>(-1);
    bool broken_ = false;
    Lease(std::shared_ptr<State>, std::size_t);
    friend class PostgresConnectionPool;
  };

  PostgresConnectionPool(const std::string &dsn, const std::string &schema,
                         PostgresPoolOptions options = {});
  ~PostgresConnectionPool();
  PostgresConnectionPool(const PostgresConnectionPool &) = delete;
  PostgresConnectionPool &operator=(const PostgresConnectionPool &) = delete;

  Lease acquire();
  PostgresPoolDiagnostics diagnostics() const;

private:
  std::shared_ptr<Lease::State> state_;
};
} // namespace laso
