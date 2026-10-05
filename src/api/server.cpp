#include <atomic>
#include <boost/beast.hpp>
#include <cstddef>
#include <laso/api/api.hpp>
#include <set>

namespace laso {
namespace beast = boost::beast;
namespace http = beast::http;
using Tcp = asio::ip::tcp;
struct HttpServer::Impl : std::enable_shared_from_this<HttpServer::Impl> {
  asio::thread_pool api_pool{1};
  asio::strand<asio::thread_pool::executor_type> api_strand;
  asio::strand<asio::io_context::executor_type> strand;
  Tcp::acceptor acceptor;
  Api &api;
  std::set<std::shared_ptr<beast::tcp_stream>> sessions;
  bool stopping = false;
  Impl(asio::io_context &io, Api &api_ref, const std::string &host, unsigned short port)
      : api_strand(asio::make_strand(api_pool)), strand(asio::make_strand(io)), acceptor(strand),
        api(api_ref) {
    Tcp::endpoint endpoint(asio::ip::make_address(host), port);
    acceptor.open(endpoint.protocol());
    acceptor.set_option(Tcp::acceptor::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen(64);
  }
  Task<void> serve(std::shared_ptr<beast::tcp_stream> stream) {
    try {
      beast::flat_buffer buffer(std::size_t{1024} * 1024 + 16384);
      http::request_parser<http::string_body> parser;
      parser.body_limit(std::size_t{1024} * 1024);
      parser.header_limit(16384);
      stream->expires_after(std::chrono::seconds(15));
      co_await http::async_read(*stream, buffer, parser, asio::use_awaitable);
      auto request = parser.release();
      const auto method = std::string(request.method_string());
      const auto target = std::string(request.target());
      const auto body = request.body();
      const auto authorization = std::string(request[http::field::authorization]);
      auto result = co_await asio::co_spawn(
          api_strand,
          [this, method, target, body, authorization]() -> Task<ApiResponse> {
            co_return api.handle(method, target, body, authorization);
          },
          asio::use_awaitable);
      http::response<http::string_body> response{static_cast<http::status>(result.status), 11};
      response.set(http::field::content_type, "application/json");
      response.set(http::field::server, "LASO/0.1");
      response.keep_alive(false);
      response.body() = result.body.dump();
      response.prepare_payload();
      stream->expires_after(std::chrono::seconds(15));
      co_await http::async_write(*stream, response, asio::use_awaitable);
    } catch (...) { // NOLINT(bugprone-empty-catch): malformed requests close the connection.
    }
    boost::system::error_code ec;
    // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
    stream->socket().shutdown(Tcp::socket::shutdown_both, ec);
    // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
    stream->socket().close(ec);
    sessions.erase(stream);
  }
  Task<void> listen() {
    while (!stopping) {
      boost::system::error_code ec;
      auto socket = co_await acceptor.async_accept(asio::redirect_error(asio::use_awaitable, ec));
      if (ec) {
        if (stopping)
          break;
        continue;
      }
      if (sessions.size() >= 128) {
        // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
        socket.close(ec);
        continue;
      }
      auto stream = std::make_shared<beast::tcp_stream>(std::move(socket));
      sessions.insert(stream);
      auto self = shared_from_this();
      asio::co_spawn(strand, serve(stream), [self](const std::exception_ptr &) {});
    }
  }
};
HttpServer::HttpServer(asio::io_context &io, Api &api, const std::string &host, unsigned short port)
    : impl_(std::make_shared<Impl>(io, api, host, port)) {}
HttpServer::~HttpServer() = default;
void HttpServer::start() {
  auto self = impl_;
  asio::co_spawn(self->strand, self->listen(), [self](const std::exception_ptr &) {});
}
void HttpServer::stop() {
  auto self = impl_;
  asio::post(self->strand, [self] {
    self->stopping = true;
    boost::system::error_code ec;
    // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
    self->acceptor.cancel(
        ec); // NOLINT(bugprone-unused-return-value): error_code overload reports via ec.
    // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
    self->acceptor.close(
        ec); // NOLINT(bugprone-unused-return-value): error_code overload reports via ec.
    for (const auto &session : self->sessions) {
      // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
      session->socket().cancel(
          ec); // NOLINT(bugprone-unused-return-value): error_code overload reports via ec.
      // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
      session->socket().close(
          ec); // NOLINT(bugprone-unused-return-value): error_code overload reports via ec.
    }
  });
}
unsigned short HttpServer::port() const {
  return impl_->acceptor.local_endpoint().port();
}
} // namespace laso
