#pragma once
#include <cstdint>
#include <filesystem>
#include <laso/storage/storage.hpp>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace laso {
struct ArtifactStoreLimits {
  std::uint64_t max_object_bytes = std::uint64_t{256} * 1024 * 1024;
  std::uint64_t max_temp_bytes = std::uint64_t{512} * 1024 * 1024;
  std::uint64_t cleanup_grace_seconds = 3600;
};

struct ArtifactIntegrityReport {
  std::size_t objects = 0;
  std::size_t verified = 0;
  std::size_t invalid = 0;
  std::size_t temporary = 0;
  std::vector<Json> errors;
};

class ArtifactStore {
public:
  virtual ~ArtifactStore() = default;
  virtual Artifact put(Artifact metadata, std::span<const std::byte> bytes) = 0;
  virtual Artifact put_file(Artifact metadata, const std::filesystem::path &source) = 0;
  virtual bool exists(const std::string &object_id) const = 0;
  virtual void verify(const std::string &object_id, const std::string &sha256 = {},
                      std::uint64_t size = 0) const = 0;
  virtual void materialize(const std::string &object_id, const std::filesystem::path &destination,
                           const std::string &sha256 = {}, std::uint64_t size = 0) const = 0;
  virtual ArtifactIntegrityReport integrity() const = 0;
  virtual Json collect_garbage(bool dry_run, std::uint64_t grace_seconds = 0) = 0;
  virtual const std::filesystem::path &root() const = 0;
};

// Hashes a regular file with bounded memory and returns digest plus byte size.
std::pair<std::string, std::uint64_t> sha256_file(const std::filesystem::path &source);
std::string sha256_bytes(std::span<const unsigned char> bytes);

class LocalArtifactStore final : public ArtifactStore {
public:
  LocalArtifactStore(std::filesystem::path root, Storage &storage, ArtifactStoreLimits limits = {});
  Artifact put(Artifact metadata, std::span<const std::byte> bytes) override;
  Artifact put_file(Artifact metadata, const std::filesystem::path &source) override;
  bool exists(const std::string &object_id) const override;
  void verify(const std::string &object_id, const std::string &sha256 = {},
              std::uint64_t size = 0) const override;
  void materialize(const std::string &object_id, const std::filesystem::path &destination,
                   const std::string &sha256 = {}, std::uint64_t size = 0) const override;
  ArtifactIntegrityReport integrity() const override;
  Json collect_garbage(bool dry_run, std::uint64_t grace_seconds = 0) override;
  const std::filesystem::path &root() const override {
    return root_;
  }

private:
  Artifact put_fd(Artifact metadata, int source_fd, std::uint64_t source_size);
  std::filesystem::path object_path(const std::string &object_id) const;
  static std::string validate_object_id(const std::string &object_id);
  void cleanup_temporary();

  std::filesystem::path root_;
  Storage &storage_;
  ArtifactStoreLimits limits_;
};

// A narrow HTTP client for a trusted LASO artifact gateway. It transfers only
// content-addressed objects and never exposes host filesystem paths.
class RemoteArtifactStore final : public ArtifactStore {
public:
  RemoteArtifactStore(std::string service_url, std::string bearer_token,
                      std::filesystem::path cache_root, Storage &storage,
                      ArtifactStoreLimits limits = {});
  Artifact put(Artifact metadata, std::span<const std::byte> bytes) override;
  Artifact put_file(Artifact metadata, const std::filesystem::path &source) override;
  bool exists(const std::string &object_id) const override;
  void verify(const std::string &object_id, const std::string &sha256 = {},
              std::uint64_t size = 0) const override;
  void materialize(const std::string &object_id, const std::filesystem::path &destination,
                   const std::string &sha256 = {}, std::uint64_t size = 0) const override;
  ArtifactIntegrityReport integrity() const override;
  Json collect_garbage(bool dry_run, std::uint64_t grace_seconds = 0) override;
  const std::filesystem::path &root() const override {
    return cache_root_;
  }

private:
  struct Endpoint {
    std::string host;
    std::string base_path;
    unsigned short port = 80;
  };
  static Endpoint parse_endpoint(const std::string &service_url);
  static std::string validate_object_id(const std::string &object_id);
  Artifact upload(Artifact metadata, const std::filesystem::path &source,
                  std::uint64_t source_size) const;
  std::pair<std::string, std::uint64_t> head(const std::string &object_id) const;
  void download(const std::string &object_id, const std::filesystem::path &destination) const;

  Endpoint endpoint_;
  std::string bearer_token_;
  std::filesystem::path cache_root_;
  Storage &storage_;
  ArtifactStoreLimits limits_;
};

#ifdef LASO_HAS_S3
struct S3ArtifactStoreConfig {
  std::string endpoint, bucket, region = "us-east-1", prefix = "laso";
  std::filesystem::path scratch_root;
  std::uint64_t connect_timeout_ms = 3000, request_timeout_ms = 30000;
  unsigned max_retries = 2;
  bool path_style = false, allow_http = false;
};

// Uses the AWS SDK credential provider chain and a controlled content-hash key
// namespace. The scratch root is local staging only; object bytes live in S3.
class S3ArtifactStore final : public ArtifactStore {
public:
  S3ArtifactStore(S3ArtifactStoreConfig config, Storage &storage, ArtifactStoreLimits limits = {});
  ~S3ArtifactStore() override;
  S3ArtifactStore(const S3ArtifactStore &) = delete;
  S3ArtifactStore &operator=(const S3ArtifactStore &) = delete;
  Artifact put(Artifact metadata, std::span<const std::byte> bytes) override;
  Artifact put_file(Artifact metadata, const std::filesystem::path &source) override;
  bool exists(const std::string &object_id) const override;
  void verify(const std::string &object_id, const std::string &sha256 = {},
              std::uint64_t size = 0) const override;
  void materialize(const std::string &object_id, const std::filesystem::path &destination,
                   const std::string &sha256 = {}, std::uint64_t size = 0) const override;
  ArtifactIntegrityReport integrity() const override;
  Json collect_garbage(bool dry_run, std::uint64_t grace_seconds = 0) override;
  const std::filesystem::path &root() const override;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
#endif
} // namespace laso
