#include <array>
#include <boost/beast.hpp>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <laso/artifacts/server.hpp>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace laso {
namespace {
namespace beast = boost::beast;
namespace http = beast::http;
using Tcp = asio::ip::tcp;

void write_all(int fd, const unsigned char *data, std::size_t size) {
  while (size != 0) {
    const auto written = ::write(fd, data, size);
    if (written < 0 && errno == EINTR)
      continue;
    if (written <= 0)
      throw Error(ErrorCode::Storage, "Artifact gateway write failed");
    data += written;
    size -= static_cast<std::size_t>(written);
  }
}

unsigned error_status(const Error &error) {
  if (error.code == ErrorCode::NotFound)
    return 404;
  if (error.code == ErrorCode::Conflict)
    return 409;
  if (error.code == ErrorCode::Policy)
    return 403;
  if (error.code == ErrorCode::Validation)
    return 400;
  if (error.code == ErrorCode::Capacity)
    return 413;
  return 503;
}

bool bearer_matches(const std::string &header, const std::string &token) {
  return header.size() == token.size() + 7 && header.starts_with("Bearer ") &&
         header.substr(7) == token;
}
} // namespace

struct ArtifactHttpServer::Impl {
  asio::io_context &io;
  Tcp::acceptor acceptor;
  ArtifactStore &store;
  std::string token;
  std::uint64_t max_object_bytes;
  std::atomic_bool stopping = false;
  std::thread thread;

  Impl(asio::io_context &context, ArtifactStore &store_ref, const std::string &host,
       unsigned short port, std::string bearer_token, std::uint64_t max_bytes)
      : io(context), acceptor(context), store(store_ref), token(std::move(bearer_token)),
        max_object_bytes(max_bytes) {
    Tcp::endpoint endpoint(asio::ip::make_address(host), port);
    boost::system::error_code error;
    acceptor.open(endpoint.protocol(), error);
    if (error)
      throw Error(ErrorCode::Storage, "Unable to open artifact gateway listener");
    acceptor.set_option(Tcp::acceptor::reuse_address(true), error);
    acceptor.bind(endpoint, error);
    if (error)
      throw Error(ErrorCode::Storage, "Unable to bind artifact gateway listener");
    acceptor.listen(16, error);
    if (error)
      throw Error(ErrorCode::Storage, "Unable to listen on artifact gateway port");
    acceptor.non_blocking(true, error);
    if (error)
      throw Error(ErrorCode::Storage, "Unable to configure artifact gateway listener");
  }

  void write_json(beast::tcp_stream &stream, unsigned status, Json body) {
    http::response<http::string_body> response{static_cast<http::status>(status), 11};
    response.set(http::field::content_type, "application/json");
    response.keep_alive(false);
    response.body() = body.dump();
    response.prepare_payload();
    http::write(stream, response);
  }

  void serve(Tcp::socket socket) {
    beast::tcp_stream stream(std::move(socket));
    std::filesystem::path temporary;
    try {
      stream.expires_after(std::chrono::seconds(90));
      beast::flat_buffer buffer(std::size_t{128} * 1024);
      http::request_parser<http::buffer_body> parser;
      parser.body_limit(max_object_bytes + 1);
      http::read_header(stream, buffer, parser);
      const auto &request = parser.get();
      const auto method = request.method();
      const auto target = std::string(request.target());
      if (!bearer_matches(std::string(request[http::field::authorization]), token)) {
        if (method == http::verb::head) {
          http::response<http::empty_body> response{http::status::unauthorized, 11};
          response.keep_alive(false);
          http::write(stream, response);
        } else {
          write_json(stream, 401, {{"error", "Artifact gateway authorization failed"}});
        }
      } else if (method == http::verb::put && target == "/api/v1/artifacts") {
        temporary = store.root() / "temp" / ("gateway-upload-" + uuid());
        auto output =
            open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (output < 0)
          throw Error(ErrorCode::Storage, "Unable to create artifact gateway temporary object");
        try {
          std::array<unsigned char, 64 * 1024> chunk{};
          std::uint64_t total = 0;
          while (!parser.is_done()) {
            auto &body = parser.get().body();
            body.data = chunk.data();
            body.size = chunk.size();
            http::read_some(stream, buffer, parser);
            const auto consumed = chunk.size() - body.size;
            if (consumed == 0 && !parser.is_done())
              throw Error(ErrorCode::Storage, "Artifact gateway received an empty body chunk");
            total += consumed;
            if (total > max_object_bytes)
              throw Error(ErrorCode::Capacity, "Artifact exceeds the configured object limit");
            write_all(output, chunk.data(), consumed);
          }
          if (fsync(output) != 0)
            throw Error(ErrorCode::Storage, "Artifact gateway temporary flush failed");
          close(output);
          output = -1;
          const auto header = std::string(request["X-LASO-Artifact-Metadata"]);
          if (header.empty() || header.size() > 64 * 1024)
            throw Error(ErrorCode::Validation, "Artifact metadata is required");
          Artifact metadata;
          try {
            metadata = Json::parse(header).get<Artifact>();
          } catch (const Json::exception &) {
            throw Error(ErrorCode::Validation, "Artifact metadata is malformed");
          }
          metadata.id.clear();
          metadata.object_id.clear();
          metadata.sha256.clear();
          metadata.location.clear();
          metadata.size = 0;
          const auto saved = store.put_file(std::move(metadata), temporary);
          write_json(stream, 200, Json(saved));
        } catch (...) {
          if (output >= 0)
            close(output);
          throw;
        }
      } else {
        const std::string prefix = "/api/v1/artifacts/";
        if ((method != http::verb::get && method != http::verb::head) ||
            !target.starts_with(prefix) || target.size() == prefix.size() ||
            target.find('/', prefix.size()) != std::string::npos ||
            target.find('?', prefix.size()) != std::string::npos)
          throw Error(ErrorCode::NotFound, "Artifact endpoint not found");
        const auto object_id = target.substr(prefix.size());
        temporary = store.root() / "temp" / ("gateway-download-" + uuid());
        store.materialize(object_id, temporary);
        const auto [digest, size] = sha256_file(temporary);
        if (method == http::verb::head) {
          http::response<http::empty_body> response{http::status::ok, 11};
          response.set("X-LASO-SHA256", digest);
          response.set("X-LASO-SIZE", std::to_string(size));
          response.keep_alive(false);
          http::write(stream, response);
        } else {
          http::response<http::file_body> response{http::status::ok, 11};
          beast::error_code error;
          response.body().open(temporary.c_str(), beast::file_mode::scan, error);
          if (error)
            throw Error(ErrorCode::Storage, "Unable to open artifact gateway response");
          response.set(http::field::content_type, "application/octet-stream");
          response.set("X-LASO-SHA256", digest);
          response.content_length(size);
          response.keep_alive(false);
          http::write(stream, response);
          response.body().close();
        }
      }
    } catch (const Error &error) {
      try {
        write_json(stream, error_status(error), {{"error", error.what()}});
      } catch (...) {
      }
    } catch (...) {
      try {
        write_json(stream, 500, {{"error", "Artifact gateway request failed"}});
      } catch (...) {
      }
    }
    if (!temporary.empty()) {
      std::error_code ignored;
      std::filesystem::remove(temporary, ignored);
    }
    boost::system::error_code error;
    stream.socket().shutdown(Tcp::socket::shutdown_both, error);
    stream.socket().close(error);
  }

  void listen() {
    while (!stopping.load()) {
      boost::system::error_code error;
      auto socket = acceptor.accept(error);
      if (error) {
        if (stopping.load())
          break;
        if (error == asio::error::would_block || error == asio::error::try_again) {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
          continue;
        }
        continue;
      }
      serve(std::move(socket));
    }
  }
};

ArtifactHttpServer::ArtifactHttpServer(asio::io_context &io, ArtifactStore &store, std::string host,
                                       unsigned short port, std::string bearer_token,
                                       std::uint64_t max_object_bytes)
    : impl_(std::make_shared<Impl>(io, store, std::move(host), port, std::move(bearer_token),
                                   max_object_bytes)) {}
ArtifactHttpServer::~ArtifactHttpServer() {
  stop();
  if (impl_->thread.joinable())
    impl_->thread.join();
}
void ArtifactHttpServer::start() {
  auto self = impl_;
  self->thread = std::thread([self] { self->listen(); });
}
void ArtifactHttpServer::stop() {
  auto self = impl_;
  if (!self || self->stopping.exchange(true))
    return;
  boost::system::error_code error;
  self->acceptor.cancel(error);
  self->acceptor.close(error);
}
unsigned short ArtifactHttpServer::port() const {
  boost::system::error_code error;
  const auto endpoint = impl_->acceptor.local_endpoint(error);
  return error ? 0 : endpoint.port();
}
} // namespace laso
