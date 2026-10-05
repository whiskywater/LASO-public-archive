#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <laso/storage/postgres_pool.hpp>
#include <mutex>
#include <poll.h>
#include <set>
#include <stdexcept>
#include <version>

#ifdef __cpp_lib_source_location
#undef __cpp_lib_source_location
#endif
#include <pqxx/pqxx>

#include <laso/core/types.hpp>

namespace laso {
namespace {
void validate_schema(const std::string &schema) {
  if (schema.empty() || schema.size() > 63 ||
      !std::isalpha(static_cast<unsigned char>(schema.front())))
    throw Error(ErrorCode::Configuration, "Invalid PostgreSQL schema");
  for (const auto ch : schema)
    if (!std::isalnum(static_cast<unsigned char>(ch)) && ch != '_')
      throw Error(ErrorCode::Configuration, "Invalid PostgreSQL schema");
}

std::string quoted_schema(const std::string &schema) {
  validate_schema(schema);
  return '"' + schema + '"';
}
} // namespace

struct PostgresConnectionPool::Lease::State {
  struct Slot {
    std::unique_ptr<pqxx::connection> connection;
    bool in_use = false;
  };

  std::string dsn, schema;
  PostgresPoolOptions options;
  mutable std::mutex mutex;
  std::condition_variable available;
  std::vector<Slot> slots;
  bool shutting_down = false;
  std::uint64_t acquisition_timeouts = 0;
  std::uint64_t replacements = 0;
};

namespace {
std::unique_ptr<pqxx::connection>
connect(const std::shared_ptr<PostgresConnectionPool::Lease::State> &state) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(state->options.acquisition_timeout_ms);
  pqxx::connecting pending(state->dsn);
  while (!pending.done()) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline)
      throw Error(ErrorCode::Storage, "PostgreSQL connection timed out");
    short events = 0;
    if (pending.wait_to_read())
      events |= POLLIN;
    if (pending.wait_to_write())
      events |= POLLOUT;
    const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
    pollfd socket{pending.sock(), events, 0};
    const auto ready = ::poll(&socket, 1, static_cast<int>(remaining));
    if (ready < 0) {
      if (errno == EINTR)
        continue;
      throw Error(ErrorCode::Storage, "PostgreSQL connection wait failed");
    }
    if (ready == 0)
      throw Error(ErrorCode::Storage, "PostgreSQL connection timed out");
    pending.process();
  }
  auto connection = std::make_unique<pqxx::connection>(std::move(pending).produce());
  pqxx::work transaction(*connection);
  transaction.exec("CREATE SCHEMA IF NOT EXISTS " + quoted_schema(state->schema));
  transaction.exec("SET search_path TO " + quoted_schema(state->schema) + ", public");
  transaction.commit();
  return connection;
}
} // namespace

PostgresConnectionPool::Lease::Lease(std::shared_ptr<State> state, std::size_t index)
    : state_(std::move(state)), index_(index) {}

PostgresConnectionPool::Lease::~Lease() {
  if (!state_ || index_ == static_cast<std::size_t>(-1))
    return;
  std::lock_guard lock(state_->mutex);
  if (index_ >= state_->slots.size() || !state_->slots[index_].in_use)
    return;
  auto &slot = state_->slots[index_];
  if (broken_ || !slot.connection || !slot.connection->is_open()) {
    slot.connection.reset();
    ++state_->replacements;
  }
  slot.in_use = false;
  state_->available.notify_one();
}

PostgresConnectionPool::Lease::Lease(Lease &&other) noexcept
    : state_(std::move(other.state_)), index_(other.index_), broken_(other.broken_) {
  other.index_ = static_cast<std::size_t>(-1);
  other.broken_ = false;
}

PostgresConnectionPool::Lease &PostgresConnectionPool::Lease::operator=(Lease &&other) noexcept {
  if (this == &other)
    return *this;
  if (state_ && index_ != static_cast<std::size_t>(-1)) {
    std::lock_guard lock(state_->mutex);
    if (index_ < state_->slots.size() && state_->slots[index_].in_use) {
      auto &slot = state_->slots[index_];
      if (broken_ || !slot.connection || !slot.connection->is_open()) {
        slot.connection.reset();
        ++state_->replacements;
      }
      slot.in_use = false;
      state_->available.notify_one();
    }
  }
  state_ = std::move(other.state_);
  index_ = other.index_;
  broken_ = other.broken_;
  other.index_ = static_cast<std::size_t>(-1);
  other.broken_ = false;
  return *this;
}

pqxx::connection &PostgresConnectionPool::Lease::connection() const {
  if (!state_ || index_ == static_cast<std::size_t>(-1))
    throw Error(ErrorCode::Storage, "Invalid PostgreSQL connection pool lease");
  std::lock_guard lock(state_->mutex);
  if (index_ >= state_->slots.size() || !state_->slots[index_].connection)
    throw Error(ErrorCode::Storage, "PostgreSQL connection pool lease is broken");
  return *state_->slots[index_].connection;
}

void PostgresConnectionPool::Lease::mark_broken() noexcept {
  broken_ = true;
}

PostgresConnectionPool::Lease::operator bool() const noexcept {
  return state_ && index_ != static_cast<std::size_t>(-1) && !broken_;
}

PostgresConnectionPool::PostgresConnectionPool(const std::string &dsn, const std::string &schema,
                                               PostgresPoolOptions options)
    : state_(std::make_shared<Lease::State>()) {
  if (dsn.empty())
    throw Error(ErrorCode::Configuration, "PostgreSQL DSN is required");
  if (options.min_connections == 0 || options.max_connections < options.min_connections ||
      options.max_connections > 64 || options.acquisition_timeout_ms == 0 ||
      options.acquisition_timeout_ms > 60000)
    throw Error(ErrorCode::Configuration, "Invalid PostgreSQL connection pool limits");
  validate_schema(schema);
  state_->dsn = dsn;
  state_->schema = schema;
  state_->options = options;
  try {
    for (std::size_t i = 0; i < options.min_connections; ++i)
      state_->slots.push_back({connect(state_), false});
  } catch (const pqxx::broken_connection &) {
    throw Error(ErrorCode::Storage, "Cannot connect to PostgreSQL");
  } catch (const pqxx::sql_error &) {
    throw Error(ErrorCode::Storage, "PostgreSQL connection pool initialization failed");
  } catch (const Error &) {
    throw;
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL connection pool initialization failed");
  }
}

PostgresConnectionPool::~PostgresConnectionPool() {
  if (state_) {
    std::lock_guard lock(state_->mutex);
    state_->shutting_down = true;
    state_->available.notify_all();
  }
  state_.reset();
}

PostgresConnectionPool::Lease PostgresConnectionPool::acquire() {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(state_->options.acquisition_timeout_ms);
  std::unique_lock lock(state_->mutex);
  const auto ready = [&] {
    if (state_->shutting_down)
      return true;
    for (const auto &slot : state_->slots)
      if (!slot.in_use)
        return true;
    return state_->slots.size() < state_->options.max_connections;
  };
  if (!state_->available.wait_until(lock, deadline, ready)) {
    ++state_->acquisition_timeouts;
    throw Error(ErrorCode::Capacity, "PostgreSQL connection pool acquisition timed out");
  }
  if (state_->shutting_down)
    throw Error(ErrorCode::Storage, "PostgreSQL connection pool is shutting down");
  try {
    for (std::size_t i = 0; i < state_->slots.size(); ++i) {
      auto &slot = state_->slots[i];
      if (!slot.in_use) {
        if (!slot.connection)
          slot.connection = connect(state_);
        slot.in_use = true;
        return Lease(state_, i);
      }
    }
    const auto index = state_->slots.size();
    state_->slots.push_back({connect(state_), true});
    return Lease(state_, index);
  } catch (const Error &) {
    throw;
  } catch (const pqxx::broken_connection &) {
    throw Error(ErrorCode::Storage, "Cannot connect to PostgreSQL");
  } catch (const pqxx::sql_error &) {
    throw Error(ErrorCode::Storage, "PostgreSQL connection initialization failed");
  } catch (const std::exception &) {
    throw Error(ErrorCode::Storage, "PostgreSQL connection initialization failed");
  }
}

PostgresPoolDiagnostics PostgresConnectionPool::diagnostics() const {
  std::lock_guard lock(state_->mutex);
  PostgresPoolDiagnostics result;
  result.size = state_->slots.size();
  result.acquisition_timeouts = state_->acquisition_timeouts;
  result.replacements = state_->replacements;
  for (const auto &slot : state_->slots)
    if (slot.in_use)
      ++result.in_use;
  return result;
}
} // namespace laso
