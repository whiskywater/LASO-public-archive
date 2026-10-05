#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <laso/application/service.hpp>

namespace laso {
struct ApiResponse {
  unsigned status = 200;
  Json body = Json::object();
};
class Api {
public:
  Api(Service &service, IdentityProvider &identity) : service_(service), identity_(identity) {}
  ApiResponse handle(const std::string &method, const std::string &target, const std::string &body,
                     const std::string &credential = "") noexcept;

private:
  Service &service_;
  IdentityProvider &identity_;
  ApiResponse route(const std::string &, const std::string &, const Json &, const Actor &,
                    std::size_t limit, std::size_t offset, std::uint64_t after);
};
struct HttpServerOptions {
  std::size_t max_session_streams = 32;
  std::chrono::seconds heartbeat_interval{15};
  std::chrono::seconds max_session_stream_lifetime{30 * 60};
};
struct HttpServerMetrics {
  std::size_t active_session_streams = 0;
  std::uint64_t accepted_session_streams = 0;
  std::uint64_t rejected_session_streams = 0;
  std::uint64_t closed_session_streams = 0;
  std::size_t session_stream_limit = 32;
};
class HttpServer {
public:
  HttpServer(asio::io_context &, Api &, const std::string &host, unsigned short port,
             HttpServerOptions options = {});
  ~HttpServer();
  void start();
  void stop();
  unsigned short port() const;
  HttpServerMetrics metrics() const;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};
} // namespace laso
