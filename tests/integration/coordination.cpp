#include "../support.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <laso/storage/coordination.hpp>
#include <laso/storage/postgres_pool.hpp>
#include <poll.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace laso;

namespace {
std::string dsn_or_skip() {
  const auto *value = std::getenv("LASO_TEST_POSTGRES_DSN");
  return value && *value ? value : std::string{};
}

struct IsolatedPostgres {
  std::string dsn;
  std::string schema = "coord_" + uuid();
  IsolatedPostgres() : dsn(dsn_or_skip()) {
    std::replace(schema.begin(), schema.end(), '-', '_');
  }
  ~IsolatedPostgres() {
    try {
      pqxx::connection connection(dsn);
      pqxx::work transaction(connection);
      transaction.exec("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
      transaction.commit();
    } catch (...) {
    }
  }
  CoordinationOptions options() const {
    CoordinationOptions options;
    options.postgres_dsn = dsn;
    options.postgres_schema = schema;
    options.pool_min_connections = 1;
    options.pool_max_connections = 4;
    options.pool_acquisition_timeout_ms = 250;
    return options;
  }
};

class SilentPostgresEndpoint {
public:
  SilentPostgresEndpoint() {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener_ < 0)
      throw std::runtime_error("unable to create PostgreSQL probe socket");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        ::listen(listener_, 1) != 0) {
      ::close(listener_);
      listener_ = -1;
      throw std::runtime_error("unable to bind PostgreSQL probe socket");
    }
    socklen_t address_size = sizeof(address);
    if (::getsockname(listener_, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
      ::close(listener_);
      listener_ = -1;
      throw std::runtime_error("unable to inspect PostgreSQL probe socket");
    }
    port_ = ntohs(address.sin_port);
    listener_thread_ = std::thread([this] {
      pollfd descriptor{listener_, POLLIN, 0};
      while (!stopping_.load()) {
        if (::poll(&descriptor, 1, 100) <= 0)
          continue;
        const auto client = ::accept(listener_, nullptr, nullptr);
        if (client < 0)
          continue;
        accepted_.store(true);
        while (!stopping_.load())
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        ::close(client);
        return;
      }
    });
  }

  ~SilentPostgresEndpoint() {
    stopping_.store(true);
    if (listener_thread_.joinable())
      listener_thread_.join();
    if (listener_ >= 0)
      ::close(listener_);
  }

  std::string dsn() const {
    return "host=127.0.0.1 port=" + std::to_string(port_) +
           " dbname=laso_timeout_probe user=laso_probe connect_timeout=2";
  }

  bool accepted() const {
    return accepted_.load();
  }

private:
  int listener_ = -1;
  unsigned short port_ = 0;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> accepted_{false};
  std::thread listener_thread_;
};
} // namespace

#if defined(LASO_HAS_POSTGRES)
TEST(PostgresPool, BoundedAcquisitionAndReplacement) {
  IsolatedPostgres database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  PostgresConnectionPool pool(database.dsn, database.schema, {1, 1, 50});
  auto held = pool.acquire();
  EXPECT_EQ(pool.diagnostics().in_use, 1U);
  EXPECT_THROW(pool.acquire(), Error);
  EXPECT_GE(pool.diagnostics().acquisition_timeouts, 1U);
  held.mark_broken();
  held = {};
  auto replacement = pool.acquire();
  EXPECT_TRUE(replacement);
  EXPECT_GE(pool.diagnostics().replacements, 1U);
}

TEST(PostgresPool, ConnectionFailureIsBoundedAndRedacted) {
  if (!std::getenv("LASO_TEST_POSTGRES_DSN"))
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  EXPECT_THROW(PostgresConnectionPool("host=127.0.0.1 port=1 dbname=missing "
                                      "connect_timeout=1",
                                      "public", {1, 1, 50}),
               Error);
}

TEST(PostgresPool, ConnectionStartupIsBoundedWhenServerStallsAfterAccept) {
  SilentPostgresEndpoint endpoint;
  const auto started = std::chrono::steady_clock::now();
  EXPECT_THROW(PostgresConnectionPool(endpoint.dsn(), "public", {1, 1, 100}), Error);
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - started)
                           .count();
  EXPECT_TRUE(endpoint.accepted());
  EXPECT_LT(elapsed, 750);
}

TEST(Coordination, InstanceIdentityIsOpaqueAndUnique) {
  const auto first = generate_service_instance_id();
  const auto second = generate_service_instance_id();
  EXPECT_FALSE(first.empty());
  EXPECT_NE(first, second);
  EXPECT_EQ(first.size(), 36U);
}

TEST(Coordination, InstanceRegistryHeartbeatsAndStaleInspection) {
  IsolatedPostgres database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  auto instance = create_coordination(database.options(), "instance-registry");
  instance->register_instance("test", "run-claims");
  auto records = instance->list_instances(90000);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().state, "ACTIVE");
  EXPECT_TRUE(instance->heartbeat_instance("ACTIVE"));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  records = instance->list_instances(1);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().state, "STALE");
  instance->set_instance_state("DRAINING");
  records = instance->list_instances(90000);
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records.front().state, "DRAINING");
}

TEST(Coordination, SingleWinnerRenewReleaseAndInspection) {
  IsolatedPostgres database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  auto first = create_coordination(database.options(), "instance-a");
  auto second = create_coordination(database.options(), "instance-b");
  auto owner = first->acquire("resource", 5000);
  ASSERT_TRUE(owner);
  EXPECT_FALSE(second->acquire("resource", 5000));
  ASSERT_TRUE(first->inspect("resource"));
  EXPECT_TRUE(first->renew(*owner, 5000));
  EXPECT_FALSE(second->renew(*owner, 5000));
  first->require_current(*owner);
  EXPECT_TRUE(first->release(*owner));
  EXPECT_FALSE(first->release(*owner));
  const auto released = first->inspect("resource");
  ASSERT_TRUE(released);
  EXPECT_FALSE(released->active);
  EXPECT_EQ(released->fencing_token, owner->fencing_token);
  const auto next_owner = first->acquire("resource", 5000);
  ASSERT_TRUE(next_owner);
  EXPECT_EQ(next_owner->fencing_token, owner->fencing_token + 1);
  EXPECT_THROW(first->require_current(*owner), Error);
  EXPECT_NO_THROW(first->require_current(*next_owner));
}

TEST(Coordination, ExpiryTakeoverRejectsStaleFencingToken) {
  IsolatedPostgres database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  auto first = create_coordination(database.options(), "instance-a");
  auto second = create_coordination(database.options(), "instance-b");
  auto old_owner = first->acquire("resource", 100);
  ASSERT_TRUE(old_owner);
  std::this_thread::sleep_for(std::chrono::milliseconds(180));
  auto new_owner = second->acquire("resource", 5000);
  ASSERT_TRUE(new_owner);
  EXPECT_EQ(new_owner->fencing_token, old_owner->fencing_token + 1);
  EXPECT_THROW(first->require_current(*old_owner), Error);
  EXPECT_FALSE(first->release(*old_owner));
  EXPECT_NO_THROW(second->require_current(*new_owner));
  EXPECT_GE(first->diagnostics().fencing_rejections, 1U);
}

TEST(Coordination, ContendedTakeoverHasOneWinner) {
  IsolatedPostgres database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  auto seed = create_coordination(database.options(), "seed");
  ASSERT_TRUE(seed->acquire("resource", 100));
  std::this_thread::sleep_for(std::chrono::milliseconds(180));
  std::vector<std::unique_ptr<Coordination>> candidates;
  for (int i = 0; i < 8; ++i)
    candidates.push_back(create_coordination(database.options(), "candidate-" + std::to_string(i)));
  std::atomic<int> winners{0};
  std::vector<std::thread> threads;
  for (auto &candidate : candidates)
    threads.emplace_back([&winners, &candidate] {
      if (candidate->acquire("resource", 5000))
        ++winners;
    });
  for (auto &thread : threads)
    thread.join();
  EXPECT_EQ(winners.load(), 1);
}

TEST(Coordination, ProcessExitAllowsCrashTakeover) {
  IsolatedPostgres database;
  if (database.dsn.empty())
    GTEST_SKIP() << "LASO_TEST_POSTGRES_DSN is not configured";
  const auto child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    try {
      auto owner = create_coordination(database.options(), "crashed-instance");
      if (!owner->acquire("resource", 100))
        _exit(2);
      _exit(0);
    } catch (...) {
      _exit(3);
    }
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(180));
  auto recovery = create_coordination(database.options(), "recovery-instance");
  auto takeover = recovery->acquire("resource", 5000);
  ASSERT_TRUE(takeover);
  EXPECT_EQ(takeover->fencing_token, 2U);
}
#else
TEST(Coordination, PostgreSQLBackendNotBuilt) {
  GTEST_SKIP() << "LASO_ENABLE_POSTGRES is not enabled";
}
#endif
