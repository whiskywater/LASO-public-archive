#include <algorithm>
#include <array>
#include <atomic>
#include <boost/beast.hpp>
#include <charconv>
#include <cstddef>
#include <laso/api/api.hpp>
#include <limits>
#include <set>
#include <stdexcept>

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
  HttpServerOptions stream_options;
  std::atomic_size_t active_event_streams{0};
  std::atomic_uint64_t accepted_event_streams{0};
  std::atomic_uint64_t rejected_event_streams{0};
  std::atomic_uint64_t closed_event_streams{0};
  bool stopping = false;
  Impl(asio::io_context &io, Api &api_ref, const std::string &host, unsigned short port,
       HttpServerOptions options)
      : api_strand(asio::make_strand(api_pool)), strand(asio::make_strand(io)), acceptor(strand),
        api(api_ref), stream_options(options) {
    if (stream_options.max_session_streams == 0 || stream_options.max_session_streams > 128 ||
        stream_options.heartbeat_interval <= std::chrono::seconds::zero() ||
        stream_options.max_session_stream_lifetime <= std::chrono::seconds::zero())
      throw std::invalid_argument("Invalid session SSE limits");
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
      const auto event_stream = target.find("/api/v1/sessions/") == 0 &&
                                target.find("/events/stream") != std::string::npos;
      if (event_stream) {
        const auto marker = std::string("/events/stream");
        const auto marker_pos = target.find(marker);
        const auto session_path = target.substr(0, marker_pos);
        const auto base = session_path + "/events";
        const auto check = co_await asio::co_spawn(
            api_strand,
            [this, target, authorization]() -> Task<ApiResponse> {
              co_return api.handle("GET", target, "", authorization);
            },
            asio::use_awaitable);
        if (check.status != 200) {
          http::response<http::string_body> denied{static_cast<http::status>(check.status), 11};
          denied.set(http::field::content_type, "application/json");
          denied.keep_alive(false);
          denied.body() = check.body.dump();
          denied.prepare_payload();
          co_await http::async_write(*stream, denied, asio::use_awaitable);
          throw std::runtime_error("SSE request rejected");
        }
        std::uint64_t cursor = 0;
        if (!request["Last-Event-ID"].empty()) {
          const auto value = std::string(request["Last-Event-ID"]);
          const auto parsed = std::from_chars(value.data(), value.data() + value.size(), cursor);
          if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() ||
              cursor > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            http::response<http::string_body> bad{http::status::bad_request, 11};
            bad.set(http::field::content_type, "application/json");
            bad.keep_alive(false);
            bad.body() = R"({"error":"Invalid Last-Event-ID"})";
            bad.prepare_payload();
            co_await http::async_write(*stream, bad, asio::use_awaitable);
            throw std::runtime_error("invalid SSE cursor");
          }
        }
        if (active_event_streams.load(std::memory_order_relaxed) >=
            stream_options.max_session_streams) {
          rejected_event_streams.fetch_add(1, std::memory_order_relaxed);
          http::response<http::string_body> full{http::status::too_many_requests, 11};
          full.set(http::field::content_type, "application/json");
          full.set(http::field::cache_control, "no-store");
          full.set(http::field::retry_after, "1");
          full.keep_alive(false);
          full.body() = R"({"error":"Session stream capacity is exhausted"})";
          full.prepare_payload();
          co_await http::async_write(*stream, full, asio::use_awaitable);
          throw std::runtime_error("SSE capacity reached");
        }
        active_event_streams.fetch_add(1, std::memory_order_relaxed);
        accepted_event_streams.fetch_add(1, std::memory_order_relaxed);
        struct StreamCountGuard {
          std::atomic_size_t &active;
          std::atomic_uint64_t &closed;
          ~StreamCountGuard() {
            active.fetch_sub(1, std::memory_order_relaxed);
            closed.fetch_add(1, std::memory_order_relaxed);
          }
        } stream_count_guard{active_event_streams, closed_event_streams};
        stream->expires_never();
        const std::string headers =
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n"
            "Connection: close\r\nX-Accel-Buffering: no\r\n\r\n";
        co_await asio::async_write(stream->socket(), asio::buffer(headers), asio::use_awaitable);
        auto poll = std::make_shared<asio::steady_timer>(stream->get_executor());
        auto peer_disconnected = std::make_shared<std::atomic_bool>(false);
        asio::co_spawn(
            stream->get_executor(),
            [stream, poll, peer_disconnected]() -> Task<void> {
              auto &socket = stream->socket();
              while (true) {
                boost::system::error_code wait_error;
                co_await socket.async_wait(Tcp::socket::wait_read,
                                           asio::redirect_error(asio::use_awaitable, wait_error));
                if (wait_error)
                  co_return;
                std::array<char, 1> probe{};
                boost::system::error_code read_error;
                (void)socket.receive(asio::buffer(probe), Tcp::socket::message_peek, read_error);
                if (read_error == asio::error::would_block || read_error == asio::error::try_again)
                  continue;
                peer_disconnected->store(true, std::memory_order_relaxed);
                boost::system::error_code ignored;
                poll->cancel(ignored);
                co_return;
              }
            },
            asio::detached);
        const auto deadline =
            std::chrono::steady_clock::now() + stream_options.max_session_stream_lifetime;
        auto last_keepalive = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() < deadline &&
               !peer_disconnected->load(std::memory_order_relaxed)) {
          const auto poll_target = base + "?after=" + std::to_string(cursor) + "&limit=1";
          auto page = co_await asio::co_spawn(
              api_strand,
              [this, poll_target, authorization]() -> Task<ApiResponse> {
                co_return api.handle("GET", poll_target, "", authorization);
              },
              asio::use_awaitable);
          if (page.status != 200)
            break;
          bool sent = false;
          bool closed_event_sent = false;
          if (page.body.is_array())
            for (const auto &event : page.body) {
              const auto sequence = event.value("sequence", std::uint64_t{0});
              if (sequence <= cursor)
                continue;
              const auto frame =
                  "id: " + std::to_string(sequence) + "\ndata: " + event.dump() + "\n\n";
              co_await asio::async_write(stream->socket(), asio::buffer(frame),
                                         asio::use_awaitable);
              cursor = sequence;
              sent = true;
              closed_event_sent =
                  closed_event_sent || event.value("type", std::string{}) == "session.closed";
            }
          if (closed_event_sent)
            break;
          if (!sent) {
            auto session = co_await asio::co_spawn(
                api_strand,
                [this, session_path, authorization]() -> Task<ApiResponse> {
                  co_return api.handle("GET", session_path, "", authorization);
                },
                asio::use_awaitable);
            if (session.status != 200)
              break;
            if (session.body.value("state", std::string{}) == "closed") {
              auto final_events = co_await asio::co_spawn(
                  api_strand,
                  [this, poll_target, authorization]() -> Task<ApiResponse> {
                    co_return api.handle("GET", poll_target, "", authorization);
                  },
                  asio::use_awaitable);
              if (final_events.status != 200)
                break;
              if (final_events.body.is_array() && !final_events.body.empty())
                continue;
              break;
            }
          }
          const auto now = std::chrono::steady_clock::now();
          if (!sent && now - last_keepalive >= stream_options.heartbeat_interval) {
            const std::string heartbeat = ": keepalive\n\n";
            co_await asio::async_write(stream->socket(), asio::buffer(heartbeat),
                                       asio::use_awaitable);
            last_keepalive = now;
          }
          poll->expires_after(sent ? std::chrono::milliseconds(0) : std::chrono::milliseconds(250));
          co_await poll->async_wait(asio::use_awaitable);
        }
        throw std::runtime_error("SSE stream ended");
      }
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
HttpServer::HttpServer(asio::io_context &io, Api &api, const std::string &host, unsigned short port,
                       HttpServerOptions options)
    : impl_(std::make_shared<Impl>(io, api, host, port, options)) {}
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
HttpServerMetrics HttpServer::metrics() const {
  return {impl_->active_event_streams.load(std::memory_order_relaxed),
          impl_->accepted_event_streams.load(std::memory_order_relaxed),
          impl_->rejected_event_streams.load(std::memory_order_relaxed),
          impl_->closed_event_streams.load(std::memory_order_relaxed),
          impl_->stream_options.max_session_streams};
}
} // namespace laso
