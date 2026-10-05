#pragma once
#include <laso/artifacts/artifacts.hpp>
#include <laso/core/async.hpp>
#include <memory>

namespace laso {
// Authenticated, object-ID-only HTTP gateway for a LocalArtifactStore. The
// gateway is intentionally separate from the JSON control API so large bodies
// can stream without entering a JSON request or response.
class ArtifactHttpServer {
public:
  ArtifactHttpServer(asio::io_context &, ArtifactStore &, std::string host, unsigned short port,
                     std::string bearer_token, std::uint64_t max_object_bytes);
  ~ArtifactHttpServer();
  void start();
  void stop();
  unsigned short port() const;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};
} // namespace laso
