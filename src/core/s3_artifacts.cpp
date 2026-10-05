#include <algorithm>
#include <array>
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <laso/artifacts/artifacts.hpp>
#include <laso/core/types.hpp>
#include <memory>
#include <regex>
#include <set>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>

namespace laso {
namespace {
namespace s3 = Aws::S3;
namespace model = Aws::S3::Model;

class AwsRuntime {
public:
  AwsRuntime() {
    Aws::InitAPI(options_);
  }
  ~AwsRuntime() {
    Aws::ShutdownAPI(options_);
  }
  AwsRuntime(const AwsRuntime &) = delete;
  AwsRuntime &operator=(const AwsRuntime &) = delete;

private:
  Aws::SDKOptions options_;
};

std::shared_ptr<AwsRuntime> aws_runtime() {
  static const auto runtime = std::make_shared<AwsRuntime>();
  return runtime;
}

std::string validate_object_id(const std::string &object_id) {
  constexpr std::string_view prefix = "sha256:";
  const auto hex = object_id.starts_with(prefix) ? object_id.substr(prefix.size()) : object_id;
  if (hex.size() != 64 || !std::all_of(hex.begin(), hex.end(), [](unsigned char value) {
        return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
      }))
    throw Error(ErrorCode::Validation, "Invalid artifact object identifier");
  return hex;
}

void write_all(int fd, const unsigned char *data, std::size_t size) {
  while (size != 0) {
    const auto written = write(fd, data, size);
    if (written < 0 && errno == EINTR)
      continue;
    if (written <= 0)
      throw Error(ErrorCode::Storage, "Artifact staging write failed");
    data += written;
    size -= static_cast<std::size_t>(written);
  }
}

class TemporaryFile {
public:
  explicit TemporaryFile(const std::filesystem::path &directory) {
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (error)
      throw Error(ErrorCode::Storage, "Unable to create S3 artifact staging directory");
    for (unsigned attempt = 0; attempt < 8; ++attempt) {
      path_ = directory / ("s3-" + uuid());
      const auto fd =
          open(path_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
      if (fd >= 0) {
        close(fd);
        return;
      }
      if (errno != EEXIST)
        throw Error(ErrorCode::Storage, "Unable to create S3 artifact staging file");
    }
    throw Error(ErrorCode::Storage, "Unable to allocate S3 artifact staging file");
  }
  ~TemporaryFile() {
    if (!keep_) {
      std::error_code error;
      std::filesystem::remove(path_, error);
    }
  }
  TemporaryFile(const TemporaryFile &) = delete;
  TemporaryFile &operator=(const TemporaryFile &) = delete;
  const std::filesystem::path &path() const {
    return path_;
  }
  void keep() {
    keep_ = true;
  }

private:
  std::filesystem::path path_;
  bool keep_ = false;
};

int response_code(const auto &outcome) {
  return static_cast<int>(outcome.GetError().GetResponseCode());
}
} // namespace

struct S3ArtifactStore::Impl {
  S3ArtifactStoreConfig config;
  Storage &storage;
  ArtifactStoreLimits limits;
  std::filesystem::path scratch_root;
  std::shared_ptr<AwsRuntime> runtime;
  std::unique_ptr<s3::S3Client> client;

  Impl(S3ArtifactStoreConfig store_config, Storage &store, ArtifactStoreLimits store_limits)
      : config(std::move(store_config)), storage(store), limits(store_limits),
        scratch_root(std::move(config.scratch_root)), runtime(aws_runtime()) {
    if (config.bucket.empty() || config.region.empty() || config.prefix.empty() ||
        config.prefix.size() > 256 || config.region.size() > 128 || limits.max_object_bytes == 0 ||
        limits.max_temp_bytes < limits.max_object_bytes ||
        limits.max_object_bytes > 5'000'000'000ULL || config.connect_timeout_ms == 0 ||
        config.connect_timeout_ms > 120000 || config.request_timeout_ms == 0 ||
        config.request_timeout_ms > 600000 || config.max_retries > 5)
      throw Error(ErrorCode::Configuration, "Invalid S3 artifact store settings");
    if (!config.ca_file.empty() && (!std::filesystem::is_regular_file(config.ca_file) ||
                                    !std::ifstream(config.ca_file).good()))
      throw Error(ErrorCode::Configuration, "S3 CA bundle must be a readable regular file");
    if (config.prefix.front() == '/' || config.prefix.back() == '/' ||
        config.prefix.find("..") != std::string::npos ||
        config.prefix.find("//") != std::string::npos ||
        !std::all_of(config.prefix.begin(), config.prefix.end(), [](unsigned char value) {
          return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') ||
                 (value >= '0' && value <= '9') || value == '.' || value == '_' || value == '-' ||
                 value == '/';
        }))
      throw Error(ErrorCode::Configuration, "Invalid S3 artifact namespace");
    for (std::size_t begin = 0; begin < config.prefix.size();) {
      const auto end = config.prefix.find('/', begin);
      const auto segment =
          config.prefix.substr(begin, end == std::string::npos ? std::string::npos : end - begin);
      if (segment == "." || segment == "..")
        throw Error(ErrorCode::Configuration, "Invalid S3 artifact namespace");
      if (end == std::string::npos)
        break;
      begin = end + 1;
    }
    if (config.bucket.size() < 3 || config.bucket.size() > 63 ||
        !std::all_of(config.bucket.begin(), config.bucket.end(),
                     [](unsigned char value) {
                       return (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') ||
                              value == '.' || value == '-';
                     }) ||
        config.bucket.front() == '.' || config.bucket.front() == '-' ||
        config.bucket.back() == '.' || config.bucket.back() == '-' ||
        config.bucket.find("..") != std::string::npos)
      throw Error(ErrorCode::Configuration, "Invalid S3 bucket name");
    if (!config.endpoint.empty()) {
      static const std::regex endpoint_pattern(
          "^(https?)://(\\[[0-9A-Fa-f:]+\\]|[A-Za-z0-9.-]+)(:[0-9]{1,5})?$");
      std::smatch match;
      if (!std::regex_match(config.endpoint, match, endpoint_pattern))
        throw Error(ErrorCode::Configuration, "Invalid S3 artifact endpoint");
      if (match[3].matched) {
        const auto port = std::stoul(match[3].str().substr(1));
        if (port == 0 || port > 65535)
          throw Error(ErrorCode::Configuration, "Invalid S3 artifact endpoint port");
      }
      if (match[1] == "http") {
        const auto host = match[2].str();
        if (!config.allow_http || (host != "localhost" && host != "127.0.0.1" && host != "[::1]"))
          throw Error(ErrorCode::Configuration,
                      "Plain HTTP S3 endpoints are allowed only for explicit loopback tests");
      } else if (config.allow_http) {
        throw Error(ErrorCode::Configuration,
                    "S3 plain HTTP test option applies only to loopback HTTP endpoints");
      }
    } else if (config.allow_http) {
      throw Error(ErrorCode::Configuration,
                  "Plain HTTP S3 testing requires an explicit loopback endpoint");
    }
    if (scratch_root.empty())
      throw Error(ErrorCode::Configuration, "S3 artifact store requires a local scratch root");

    std::error_code error;
    std::filesystem::create_directories(scratch_root / "temp", error);
    if (error)
      throw Error(ErrorCode::Storage, "Unable to create S3 artifact scratch directory");
    struct stat temp_info {};
    if (lstat((scratch_root / "temp").c_str(), &temp_info) != 0 || !S_ISDIR(temp_info.st_mode) ||
        S_ISLNK(temp_info.st_mode) || chmod((scratch_root / "temp").c_str(), 0700) != 0)
      throw Error(ErrorCode::Storage, "Unable to restrict S3 artifact scratch directory");

    s3::S3ClientConfiguration client_config;
    client_config.region = config.region;
    client_config.endpointOverride = config.endpoint;
    client_config.scheme =
        config.endpoint.starts_with("http://") ? Aws::Http::Scheme::HTTP : Aws::Http::Scheme::HTTPS;
    client_config.verifySSL = true;
    if (!config.ca_file.empty())
      client_config.caFile = config.ca_file.string();
    client_config.connectTimeoutMs = static_cast<long>(config.connect_timeout_ms);
    client_config.requestTimeoutMs = static_cast<long>(config.request_timeout_ms);
    client_config.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(
        "LASO-S3", static_cast<long>(config.max_retries), 25);
    client = std::make_unique<s3::S3Client>(
        client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        !config.path_style);
  }

  std::string key(const std::string &object_id) const {
    const auto hex = validate_object_id(object_id);
    return config.prefix + "/objects/" + hex.substr(0, 2) + "/" + hex.substr(2);
  }

  bool head(const std::string &object_id, std::uint64_t *size = nullptr,
            std::string *digest = nullptr) const {
    model::HeadObjectRequest request;
    request.SetBucket(config.bucket);
    request.SetKey(key(object_id));
    const auto outcome = client->HeadObject(request);
    if (!outcome.IsSuccess()) {
      if (response_code(outcome) == 404)
        return false;
      throw Error(ErrorCode::Storage, "S3 artifact metadata request failed");
    }
    if (size)
      *size = static_cast<std::uint64_t>(outcome.GetResult().GetContentLength());
    if (digest) {
      const auto &metadata = outcome.GetResult().GetMetadata();
      const auto entry = metadata.find("laso-sha256");
      *digest = entry == metadata.end() ? std::string{} : entry->second;
    }
    return true;
  }

  void download(const std::string &object_id, const std::filesystem::path &destination) const {
    model::GetObjectRequest request;
    request.SetBucket(config.bucket);
    request.SetKey(key(object_id));
    request.SetResponseStreamFactory([path = destination]() -> Aws::IOStream * {
      return Aws::New<Aws::FStream>("LASO-S3", path.c_str(),
                                    std::ios_base::out | std::ios_base::binary |
                                        std::ios_base::trunc);
    });
    const auto outcome = client->GetObject(request);
    if (!outcome.IsSuccess()) {
      if (response_code(outcome) == 404)
        throw Error(ErrorCode::NotFound, "Artifact object is missing from S3 storage");
      throw Error(ErrorCode::Storage, "S3 artifact download failed");
    }
    auto &body = outcome.GetResult().GetBody();
    body.flush();
    if (!body)
      throw Error(ErrorCode::Storage, "S3 artifact download stream failed");
  }

  void download_verified(const std::string &object_id, const std::filesystem::path &path,
                         const std::string &expected_sha256, std::uint64_t expected_size) const {
    const auto hex = validate_object_id(object_id);
    if (!expected_sha256.empty() && validate_object_id(expected_sha256) != hex)
      throw Error(ErrorCode::Conflict, "Artifact object identity does not match expected digest");
    std::uint64_t remote_size = 0;
    std::string remote_digest;
    if (!head(object_id, &remote_size, &remote_digest))
      throw Error(ErrorCode::NotFound, "Artifact object is missing from S3 storage");
    if (remote_size > limits.max_object_bytes ||
        (expected_size != 0 && remote_size != expected_size))
      throw Error(ErrorCode::Conflict, "Artifact object size does not match expected size");
    download(object_id, path);
    const auto [actual_digest, actual_size] = sha256_file(path);
    if (actual_digest != hex || actual_size != remote_size ||
        (!remote_digest.empty() && remote_digest != actual_digest))
      throw Error(ErrorCode::Conflict, "Artifact object integrity check failed");
  }

  void verify_existing(const std::string &object_id, const std::string &digest,
                       std::uint64_t size) const {
    TemporaryFile staged(scratch_root / "temp");
    download_verified(object_id, staged.path(), digest, size);
  }

  Artifact publish(Artifact metadata, const std::filesystem::path &staged,
                   const std::string &digest, std::uint64_t size) {
    const auto object_id = "sha256:" + digest;
    std::uint64_t existing_size = 0;
    std::string existing_digest;
    if (head(object_id, &existing_size, &existing_digest)) {
      verify_existing(object_id, digest, size);
    } else {
      model::PutObjectRequest request;
      request.SetBucket(config.bucket);
      request.SetKey(key(object_id));
      request.SetIfNoneMatch("*");
      request.SetContentLength(static_cast<long long>(size));
      request.SetContentType("application/octet-stream");
      request.AddMetadata("laso-sha256", digest);
      request.AddMetadata("laso-size", std::to_string(size));
      auto body = Aws::MakeShared<Aws::FStream>("LASO-S3", staged.c_str(),
                                                std::ios_base::in | std::ios_base::binary);
      if (!body->is_open())
        throw Error(ErrorCode::Storage, "Unable to open staged S3 artifact");
      request.SetBody(body);
      const auto outcome = client->PutObject(request);
      if (!outcome.IsSuccess()) {
        const auto code = response_code(outcome);
        if (code != 409 && code != 412)
          throw Error(ErrorCode::Storage, "S3 artifact upload failed");
        verify_existing(object_id, digest, size);
      }
      std::uint64_t published_size = 0;
      std::string published_digest;
      if (!head(object_id, &published_size, &published_digest) || published_size != size ||
          published_digest != digest)
        throw Error(ErrorCode::Conflict, "S3 artifact publication verification failed");
    }

    metadata.id = uuid();
    metadata.object_id = object_id;
    metadata.sha256 = digest;
    metadata.size = size;
    metadata.location.clear();
    storage.commit({{RecordKind::Artifact, metadata.id, metadata.run_id, Json(metadata)}});
    return metadata;
  }
};

S3ArtifactStore::S3ArtifactStore(S3ArtifactStoreConfig config, Storage &storage,
                                 ArtifactStoreLimits limits)
    : impl_(std::make_unique<Impl>(std::move(config), storage, limits)) {}
S3ArtifactStore::~S3ArtifactStore() = default;

Artifact S3ArtifactStore::put(Artifact metadata, std::span<const std::byte> bytes) {
  if (bytes.size() > impl_->limits.max_object_bytes)
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  TemporaryFile staged(impl_->scratch_root / "temp");
  const auto fd = open(staged.path().c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0)
    throw Error(ErrorCode::Storage, "Unable to write staged S3 artifact");
  try {
    write_all(fd, reinterpret_cast<const unsigned char *>(bytes.data()), bytes.size());
    if (fsync(fd) != 0)
      throw Error(ErrorCode::Storage, "Unable to flush staged S3 artifact");
    close(fd);
  } catch (...) {
    close(fd);
    throw;
  }
  const auto [digest, size] = sha256_file(staged.path());
  return impl_->publish(std::move(metadata), staged.path(), digest, size);
}

Artifact S3ArtifactStore::put_file(Artifact metadata, const std::filesystem::path &source) {
  int input = open(source.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (input < 0)
    throw Error(ErrorCode::Storage, "Unable to open artifact source");
  struct stat source_info {};
  if (fstat(input, &source_info) != 0 || !S_ISREG(source_info.st_mode)) {
    close(input);
    throw Error(ErrorCode::Validation, "Artifact source is not a regular file");
  }
  if (source_info.st_size < 0 ||
      static_cast<std::uint64_t>(source_info.st_size) > impl_->limits.max_object_bytes) {
    close(input);
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  }
  TemporaryFile staged(impl_->scratch_root / "temp");
  int output = open(staged.path().c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC | O_NOFOLLOW);
  if (output < 0) {
    close(input);
    throw Error(ErrorCode::Storage, "Unable to create staged S3 artifact");
  }
  try {
    std::array<unsigned char, std::size_t{1024} * 1024> buffer{};
    std::uint64_t total = 0;
    for (;;) {
      const auto read_size = read(input, buffer.data(), buffer.size());
      if (read_size < 0 && errno == EINTR)
        continue;
      if (read_size < 0)
        throw Error(ErrorCode::Storage, "Unable to read artifact source");
      if (read_size == 0)
        break;
      total += static_cast<std::uint64_t>(read_size);
      if (total > impl_->limits.max_object_bytes)
        throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
      write_all(output, buffer.data(), static_cast<std::size_t>(read_size));
    }
    if (total != static_cast<std::uint64_t>(source_info.st_size) || fsync(output) != 0)
      throw Error(ErrorCode::Storage, "Artifact source changed or staging flush failed");
    close(input);
    input = -1;
    close(output);
    output = -1;
    const auto [digest, staged_size] = sha256_file(staged.path());
    return impl_->publish(std::move(metadata), staged.path(), digest, staged_size);
  } catch (...) {
    if (input >= 0)
      close(input);
    if (output >= 0)
      close(output);
    throw;
  }
}

bool S3ArtifactStore::exists(const std::string &object_id) const {
  return impl_->head(object_id);
}

void S3ArtifactStore::verify(const std::string &object_id, const std::string &expected_sha256,
                             std::uint64_t expected_size) const {
  TemporaryFile staged(impl_->scratch_root / "temp");
  impl_->download_verified(object_id, staged.path(), expected_sha256, expected_size);
}

void S3ArtifactStore::materialize(const std::string &object_id,
                                  const std::filesystem::path &destination,
                                  const std::string &expected_sha256,
                                  std::uint64_t expected_size) const {
  std::error_code error;
  const auto parent =
      destination.parent_path().empty() ? std::filesystem::path{"."} : destination.parent_path();
  std::filesystem::create_directories(parent, error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to create artifact destination");
  TemporaryFile staged(parent);
  impl_->download_verified(object_id, staged.path(), expected_sha256, expected_size);
  const auto fd = open(staged.path().c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0 || fsync(fd) != 0) {
    if (fd >= 0)
      close(fd);
    throw Error(ErrorCode::Storage, "Unable to flush materialized artifact");
  }
  close(fd);
  if (rename(staged.path().c_str(), destination.c_str()) != 0)
    throw Error(ErrorCode::Storage, "Unable to publish materialized artifact");
  staged.keep();
}

ArtifactIntegrityReport S3ArtifactStore::integrity() const {
  ArtifactIntegrityReport report;
  std::map<std::string, std::uint64_t> referenced;
  constexpr std::size_t page_size = 10000;
  for (std::size_t offset = 0;; offset += page_size) {
    const auto page = impl_->storage.list(RecordKind::Artifact, "", page_size, offset);
    for (const auto &record : page) {
      const auto artifact = record.get<Artifact>();
      if (!artifact.object_id.empty())
        referenced.emplace(artifact.object_id, artifact.size);
    }
    if (page.size() < page_size)
      break;
  }
  report.objects = referenced.size();
  for (const auto &[object_id, size] : referenced) {
    try {
      verify(object_id, object_id, size);
      ++report.verified;
    } catch (const Error &error) {
      ++report.invalid;
      report.errors.push_back({{"object_id", object_id}, {"error", error.what()}});
    }
  }
  std::error_code error;
  const auto temporary = impl_->scratch_root / "temp";
  for (const auto &entry : std::filesystem::directory_iterator(temporary, error))
    if (!error && entry.is_regular_file(error))
      ++report.temporary;
  if (error)
    throw Error(ErrorCode::Storage, "Unable to inspect S3 artifact staging files");
  return report;
}

Json S3ArtifactStore::collect_garbage(bool, std::uint64_t) {
  throw Error(
      ErrorCode::Configuration,
      "S3 artifact garbage collection is disabled; remote object deletion is not supported");
}

const std::filesystem::path &S3ArtifactStore::root() const {
  return impl_->scratch_root;
}
} // namespace laso
