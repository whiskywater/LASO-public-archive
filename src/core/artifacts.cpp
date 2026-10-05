#include <algorithm>
#include <array>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <fcntl.h>
#include <laso/artifacts/artifacts.hpp>
#include <laso/core/async.hpp>
#include <optional>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace laso {
namespace {
class Sha256 {
public:
  Sha256()
      : state_{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
               0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19} {}

  void update(const unsigned char *data, std::size_t size) {
    bit_count_ += static_cast<std::uint64_t>(size) * 8;
    while (size != 0) {
      const auto copied = std::min(size, buffer_.size() - buffered_);
      std::copy_n(data, copied, buffer_.data() + buffered_);
      buffered_ += copied;
      data += copied;
      size -= copied;
      if (buffered_ == buffer_.size()) {
        transform(buffer_.data());
        buffered_ = 0;
      }
    }
  }

  std::string finish() {
    const auto original_bits = bit_count_;
    buffer_[buffered_++] = 0x80;
    if (buffered_ > 56) {
      std::fill(buffer_.begin() + static_cast<std::ptrdiff_t>(buffered_), buffer_.end(), 0);
      transform(buffer_.data());
      buffered_ = 0;
    }
    std::fill(buffer_.begin() + static_cast<std::ptrdiff_t>(buffered_), buffer_.begin() + 56, 0);
    for (int shift = 56; shift >= 0; shift -= 8)
      buffer_[56 + static_cast<std::size_t>((56 - shift) / 8)] =
          static_cast<unsigned char>((original_bits >> shift) & 0xff);
    transform(buffer_.data());
    static constexpr char hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(64);
    for (const auto value : state_)
      for (int shift = 28; shift >= 0; shift -= 4)
        result.push_back(hex[(value >> shift) & 0xf]);
    return result;
  }

private:
  inline static constexpr std::array<std::uint32_t, 64> constants = {
      0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
      0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
      0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
      0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
      0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
      0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
      0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
      0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
      0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
      0xc67178f2};

  static std::uint32_t rotate_right(std::uint32_t value, unsigned count) {
    return (value >> count) | (value << (32 - count));
  }

  void transform(const unsigned char *block) {
    std::array<std::uint32_t, 64> schedule{};
    for (unsigned i = 0; i < 16; ++i)
      for (unsigned byte = 0; byte < 4; ++byte)
        schedule[i] = (schedule[i] << 8) | block[i * 4 + byte];
    for (unsigned i = 16; i < 64; ++i) {
      const auto s0 = rotate_right(schedule[i - 15], 7) ^ rotate_right(schedule[i - 15], 18) ^
                      (schedule[i - 15] >> 3);
      const auto s1 = rotate_right(schedule[i - 2], 17) ^ rotate_right(schedule[i - 2], 19) ^
                      (schedule[i - 2] >> 10);
      schedule[i] = schedule[i - 16] + s0 + schedule[i - 7] + s1;
    }
    auto working = state_;
    for (unsigned i = 0; i < 64; ++i) {
      const auto s1 =
          rotate_right(working[4], 6) ^ rotate_right(working[4], 11) ^ rotate_right(working[4], 25);
      const auto choice = (working[4] & working[5]) ^ (~working[4] & working[6]);
      const auto temp1 = working[7] + s1 + choice + constants[i] + schedule[i];
      const auto s0 =
          rotate_right(working[0], 2) ^ rotate_right(working[0], 13) ^ rotate_right(working[0], 22);
      const auto majority =
          (working[0] & working[1]) ^ (working[0] & working[2]) ^ (working[1] & working[2]);
      const auto temp2 = s0 + majority;
      working[7] = working[6];
      working[6] = working[5];
      working[5] = working[4];
      working[4] = working[3] + temp1;
      working[3] = working[2];
      working[2] = working[1];
      working[1] = working[0];
      working[0] = temp1 + temp2;
    }
    for (unsigned i = 0; i < 8; ++i)
      state_[i] += working[i];
  }

  std::array<std::uint32_t, 8> state_;
  std::array<unsigned char, 64> buffer_{};
  std::size_t buffered_ = 0;
  std::uint64_t bit_count_ = 0;
};

void write_all(int fd, const unsigned char *data, std::size_t size) {
  while (size != 0) {
    const auto written = write(fd, data, size);
    if (written < 0 && errno == EINTR)
      continue;
    if (written <= 0)
      throw Error(ErrorCode::Storage, "Artifact write failed");
    data += written;
    size -= static_cast<std::size_t>(written);
  }
}

std::pair<std::string, std::uint64_t> hash_fd(int fd) {
  if (lseek(fd, 0, SEEK_SET) < 0)
    throw Error(ErrorCode::Storage, "Unable to seek artifact");
  Sha256 hash;
  std::array<unsigned char, 1024 * 1024> buffer{};
  std::uint64_t size = 0;
  for (;;) {
    const auto read_size = read(fd, buffer.data(), buffer.size());
    if (read_size < 0 && errno == EINTR)
      continue;
    if (read_size < 0)
      throw Error(ErrorCode::Storage, "Unable to read artifact");
    if (read_size == 0)
      break;
    hash.update(buffer.data(), static_cast<std::size_t>(read_size));
    size += static_cast<std::uint64_t>(read_size);
  }
  return {hash.finish(), size};
}

bool regular_file(const std::filesystem::path &path) {
  std::error_code error;
  return std::filesystem::is_regular_file(path, error) && !error;
}

void sync_directory(const std::filesystem::path &directory) {
  const auto fd = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0)
    throw Error(ErrorCode::Storage, "Cannot open artifact directory");
  const auto result = fsync(fd);
  close(fd);
  if (result != 0)
    throw Error(ErrorCode::Storage, "Artifact directory flush failed");
}
} // namespace

std::pair<std::string, std::uint64_t> sha256_file(const std::filesystem::path &source) {
  const auto fd = open(source.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0)
    throw Error(ErrorCode::Storage, "Unable to open artifact source");
  struct stat info {};
  if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode)) {
    close(fd);
    throw Error(ErrorCode::Validation, "Artifact source is not a regular file");
  }
  try {
    auto result = hash_fd(fd);
    close(fd);
    return result;
  } catch (...) {
    close(fd);
    throw;
  }
}

std::string sha256_bytes(std::span<const unsigned char> bytes) {
  Sha256 hash;
  hash.update(bytes.data(), bytes.size());
  return hash.finish();
}

LocalArtifactStore::LocalArtifactStore(std::filesystem::path root, Storage &storage,
                                       ArtifactStoreLimits limits)
    : root_(std::move(root)), storage_(storage), limits_(limits) {
  if (limits_.max_object_bytes == 0 || limits_.max_temp_bytes < limits_.max_object_bytes)
    throw Error(ErrorCode::Configuration, "Invalid artifact store limits");
  std::error_code error;
  std::filesystem::create_directories(root_ / "objects", error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to create artifact object store");
  std::filesystem::create_directories(root_ / "temp", error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to create artifact temporary store");
  cleanup_temporary();
}

std::string LocalArtifactStore::validate_object_id(const std::string &object_id) {
  const auto prefix = std::string{"sha256:"};
  const auto hex = object_id.starts_with(prefix) ? object_id.substr(prefix.size()) : object_id;
  if (hex.size() != 64 || !std::all_of(hex.begin(), hex.end(), [](unsigned char value) {
        return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
      }))
    throw Error(ErrorCode::Validation, "Invalid artifact object identifier");
  return hex;
}

std::filesystem::path LocalArtifactStore::object_path(const std::string &object_id) const {
  const auto hex = validate_object_id(object_id);
  return root_ / "objects" / hex.substr(0, 2) / hex.substr(2);
}

Artifact LocalArtifactStore::put(Artifact metadata, std::span<const std::byte> bytes) {
  if (bytes.size() > limits_.max_object_bytes)
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  const auto temp = root_ / "temp" / ("upload-" + uuid());
  int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0)
    throw Error(ErrorCode::Storage, "Unable to create artifact temporary object");
  try {
    write_all(fd, reinterpret_cast<const unsigned char *>(bytes.data()), bytes.size());
    if (fsync(fd) != 0)
      throw Error(ErrorCode::Storage, "Artifact temporary flush failed");
    close(fd);
    fd = -1;
    const auto source = open(temp.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (source < 0)
      throw Error(ErrorCode::Storage, "Unable to reopen artifact temporary object");
    try {
      auto result = put_fd(std::move(metadata), source, bytes.size());
      close(source);
      std::filesystem::remove(temp);
      return result;
    } catch (...) {
      close(source);
      throw;
    }
  } catch (...) {
    if (fd >= 0)
      close(fd);
    std::error_code error;
    std::filesystem::remove(temp, error);
    throw;
  }
}

Artifact LocalArtifactStore::put_file(Artifact metadata, const std::filesystem::path &source) {
  const auto fd = open(source.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0)
    throw Error(ErrorCode::Storage, "Unable to open artifact source");
  struct stat info {};
  if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode)) {
    close(fd);
    throw Error(ErrorCode::Validation, "Artifact source is not a regular file");
  }
  if (info.st_size < 0 || static_cast<std::uint64_t>(info.st_size) > limits_.max_object_bytes) {
    close(fd);
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  }
  try {
    auto result = put_fd(std::move(metadata), fd, static_cast<std::uint64_t>(info.st_size));
    close(fd);
    return result;
  } catch (...) {
    close(fd);
    throw;
  }
}

Artifact LocalArtifactStore::put_fd(Artifact metadata, int source_fd, std::uint64_t source_size) {
  if (source_size > limits_.max_object_bytes)
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  if (lseek(source_fd, 0, SEEK_SET) < 0)
    throw Error(ErrorCode::Storage, "Unable to seek artifact source");
  const auto temp = root_ / "temp" / ("upload-" + uuid());
  int output = open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (output < 0)
    throw Error(ErrorCode::Storage, "Unable to create artifact object");
  try {
    Sha256 hash;
    std::array<unsigned char, 1024 * 1024> buffer{};
    std::uint64_t total = 0;
    for (;;) {
      const auto read_size = read(source_fd, buffer.data(), buffer.size());
      if (read_size < 0 && errno == EINTR)
        continue;
      if (read_size < 0)
        throw Error(ErrorCode::Storage, "Unable to read artifact source");
      if (read_size == 0)
        break;
      total += static_cast<std::uint64_t>(read_size);
      if (total > limits_.max_object_bytes)
        throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
      hash.update(buffer.data(), static_cast<std::size_t>(read_size));
      write_all(output, buffer.data(), static_cast<std::size_t>(read_size));
    }
    if (total != source_size)
      throw Error(ErrorCode::Storage, "Artifact source changed during upload");
    if (fsync(output) != 0)
      throw Error(ErrorCode::Storage, "Artifact object flush failed");
    close(output);
    output = -1;
    const auto digest = hash.finish();
    const auto object_id = std::string{"sha256:"} + digest;
    const auto final = object_path(object_id);
    std::filesystem::create_directories(final.parent_path());
    if (regular_file(final)) {
      verify(object_id, digest, total);
      std::filesystem::remove(temp);
    } else {
      if (rename(temp.c_str(), final.c_str()) != 0) {
        if (errno != EEXIST)
          throw Error(ErrorCode::Storage, "Unable to finalize artifact object");
        verify(object_id, digest, total);
        std::filesystem::remove(temp);
      }
      sync_directory(final.parent_path());
    }
    metadata.id = uuid();
    metadata.object_id = object_id;
    metadata.sha256 = digest;
    metadata.size = total;
    metadata.location.clear();
    storage_.commit({{RecordKind::Artifact, metadata.id, metadata.run_id, Json(metadata)}});
    return metadata;
  } catch (...) {
    if (output >= 0)
      close(output);
    std::error_code error;
    std::filesystem::remove(temp, error);
    throw;
  }
}

bool LocalArtifactStore::exists(const std::string &object_id) const {
  try {
    return regular_file(object_path(object_id));
  } catch (const Error &) {
    return false;
  }
}

void LocalArtifactStore::verify(const std::string &object_id, const std::string &expected_sha256,
                                std::uint64_t expected_size) const {
  const auto path = object_path(object_id);
  const auto fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0)
    throw Error(ErrorCode::NotFound, "Artifact object is missing");
  struct stat info {};
  if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode)) {
    close(fd);
    throw Error(ErrorCode::Validation, "Artifact object is not a regular file");
  }
  const auto [digest, size] = hash_fd(fd);
  close(fd);
  const auto expected =
      expected_sha256.empty() ? validate_object_id(object_id) : validate_object_id(expected_sha256);
  if (digest != expected || (expected_size != 0 && size != expected_size))
    throw Error(ErrorCode::Conflict, "Artifact object integrity check failed");
}

void LocalArtifactStore::materialize(const std::string &object_id,
                                     const std::filesystem::path &destination,
                                     const std::string &expected_sha256,
                                     std::uint64_t expected_size) const {
  verify(object_id, expected_sha256, expected_size);
  std::error_code error;
  std::filesystem::create_directories(destination.parent_path(), error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to create artifact destination");
  int source = open(object_path(object_id).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (source < 0)
    throw Error(ErrorCode::NotFound, "Artifact object is missing");
  const auto temporary = destination.string() + ".tmp-" + uuid();
  int output = open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (output < 0) {
    close(source);
    source = -1;
    throw Error(ErrorCode::Storage, "Unable to create artifact materialization");
  }
  try {
    std::array<unsigned char, 1024 * 1024> buffer{};
    for (;;) {
      const auto read_size = read(source, buffer.data(), buffer.size());
      if (read_size < 0 && errno == EINTR)
        continue;
      if (read_size < 0)
        throw Error(ErrorCode::Storage, "Unable to read artifact object");
      if (read_size == 0)
        break;
      write_all(output, buffer.data(), static_cast<std::size_t>(read_size));
    }
    if (fsync(output) != 0)
      throw Error(ErrorCode::Storage, "Unable to flush materialized artifact");
    close(source);
    source = -1;
    close(output);
    output = -1;
    if (rename(temporary.c_str(), destination.c_str()) != 0)
      throw Error(ErrorCode::Storage, "Unable to finalize materialized artifact");
  } catch (...) {
    close(source);
    if (output >= 0)
      close(output);
    std::filesystem::remove(temporary, error);
    throw;
  }
}

ArtifactIntegrityReport LocalArtifactStore::integrity() const {
  ArtifactIntegrityReport report;
  std::error_code error;
  const auto objects = root_ / "objects";
  for (const auto &entry : std::filesystem::recursive_directory_iterator(objects, error)) {
    if (error)
      throw Error(ErrorCode::Storage, "Unable to enumerate artifact objects");
    if (!entry.is_regular_file(error))
      continue;
    ++report.objects;
    const auto filename = entry.path().filename().string();
    const auto parent = entry.path().parent_path().filename().string();
    if (parent.size() != 2 || filename.size() != 62) {
      ++report.invalid;
      report.errors.push_back({{"path", "object-name"}});
      continue;
    }
    try {
      verify("sha256:" + parent + filename);
      ++report.verified;
    } catch (const Error &e) {
      ++report.invalid;
      report.errors.push_back({{"object_id", "sha256:" + parent + filename}, {"error", e.what()}});
    }
  }
  const auto temp = root_ / "temp";
  for (const auto &entry : std::filesystem::directory_iterator(temp, error))
    if (!error && entry.is_regular_file(error))
      ++report.temporary;
  return report;
}

Json LocalArtifactStore::collect_garbage(bool dry_run, std::uint64_t grace_seconds) {
  const auto grace =
      std::chrono::seconds(grace_seconds == 0 ? limits_.cleanup_grace_seconds : grace_seconds);
  std::set<std::string> live;
  constexpr std::size_t page_size = 10000;
  for (std::size_t offset = 0;; offset += page_size) {
    const auto page = storage_.list(RecordKind::Artifact, "", page_size, offset);
    for (const auto &value : page) {
      const auto artifact = value.get<Artifact>();
      if (!artifact.object_id.empty())
        live.insert(validate_object_id(artifact.object_id));
    }
    if (page.size() < page_size)
      break;
  }
  std::size_t removed = 0, eligible = 0;
  std::error_code error;
  const auto objects = root_ / "objects";
  for (const auto &entry : std::filesystem::recursive_directory_iterator(objects, error)) {
    if (error || !entry.is_regular_file(error))
      continue;
    const auto filename = entry.path().filename().string();
    const auto parent = entry.path().parent_path().filename().string();
    if (parent.size() != 2 || filename.size() != 62)
      continue;
    const auto hex = parent + filename;
    if (live.contains(hex))
      continue;
    if (std::filesystem::file_time_type::clock::now() - entry.last_write_time(error) < grace)
      continue;
    ++eligible;
    if (!dry_run && std::filesystem::remove(entry.path(), error) && !error)
      ++removed;
  }
  cleanup_temporary();
  return {{"dry_run", dry_run},
          {"live_objects", live.size()},
          {"eligible", eligible},
          {"removed", removed}};
}

void LocalArtifactStore::cleanup_temporary() {
  std::error_code error;
  const auto temp = root_ / "temp";
  const auto grace = std::chrono::seconds(limits_.cleanup_grace_seconds);
  for (const auto &entry : std::filesystem::directory_iterator(temp, error)) {
    if (error || !entry.is_regular_file(error))
      continue;
    if (std::filesystem::file_time_type::clock::now() - entry.last_write_time(error) >= grace)
      std::filesystem::remove(entry.path(), error);
  }
}

namespace {
namespace http = boost::beast::http;
namespace beast = boost::beast;
using Tcp = asio::ip::tcp;

std::string artifact_base_target(const auto &endpoint) {
  auto base = endpoint.base_path;
  if (base.empty() || base == "/")
    base = "";
  else if (base.back() == '/')
    base.pop_back();
  return base + "/api/v1/artifacts";
}

std::string object_target(const auto &endpoint, const std::string &object_id) {
  return artifact_base_target(endpoint) + "/" + object_id;
}

std::uint64_t response_size(const auto &response) {
  auto value = std::string(response[http::field::content_length]);
  if (value.empty())
    value = std::string(response["X-LASO-SIZE"]);
  std::uint64_t size = 0;
  const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), size);
  if (error != std::errc{} || ptr != value.data() + value.size())
    throw Error(ErrorCode::Storage, "Artifact gateway omitted a valid object size");
  return size;
}

template <typename Request, typename Parser>
void exchange(const auto &endpoint, const std::string &token, Request &request, Parser &parser,
              std::uint64_t body_limit) {
  asio::io_context io;
  Tcp::resolver resolver(io);
  beast::tcp_stream stream(io);
  stream.expires_after(std::chrono::seconds(90));
  const auto results = resolver.resolve(endpoint.host, std::to_string(endpoint.port));
  stream.connect(results);
  http::write(stream, request);
  beast::flat_buffer buffer;
  parser.body_limit(body_limit);
  http::read(stream, buffer, parser);
  boost::system::error_code error;
  stream.socket().shutdown(Tcp::socket::shutdown_both, error);
  stream.socket().close(error);
  (void)token;
}

void add_auth(auto &request, const std::string &token) {
  request.set(http::field::authorization, "Bearer " + token);
}

void throw_gateway_error(unsigned status, const std::string &body) {
  if (status == 404)
    throw Error(ErrorCode::NotFound, "Artifact object is missing");
  if (status == 409)
    throw Error(ErrorCode::Conflict, "Artifact gateway rejected object integrity");
  if (status == 413)
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  if (status == 401 || status == 403)
    throw Error(ErrorCode::Policy, "Artifact gateway authorization failed");
  (void)body;
  throw Error(ErrorCode::Storage, "Artifact gateway request failed");
}
} // namespace

RemoteArtifactStore::Endpoint RemoteArtifactStore::parse_endpoint(const std::string &service_url) {
  constexpr auto scheme = std::string_view{"http://"};
  if (!service_url.starts_with(scheme) || service_url.size() <= scheme.size())
    throw Error(ErrorCode::Configuration, "Artifact service URL must use http://");
  const auto authority_start = scheme.size();
  const auto slash = service_url.find('/', authority_start);
  const auto authority = service_url.substr(
      authority_start, slash == std::string::npos ? std::string::npos : slash - authority_start);
  if (authority.empty() || authority.find('@') != std::string::npos)
    throw Error(ErrorCode::Configuration, "Invalid artifact service URL");
  Endpoint result;
  result.base_path = slash == std::string::npos ? "" : service_url.substr(slash);
  const auto colon = authority.rfind(':');
  auto port_text = std::string{"80"};
  if (colon == std::string::npos)
    result.host = authority;
  else {
    result.host = authority.substr(0, colon);
    port_text = authority.substr(colon + 1);
  }
  if (result.host.empty() || result.host.find('[') != std::string::npos ||
      result.host.find(']') != std::string::npos)
    throw Error(ErrorCode::Configuration, "Invalid artifact service host");
  unsigned parsed = 0;
  const auto *begin = port_text.data();
  const auto *end = begin + port_text.size();
  const auto [ptr, error] = std::from_chars(begin, end, parsed);
  if (error != std::errc{} || ptr != end || parsed == 0 || parsed > 65535)
    throw Error(ErrorCode::Configuration, "Invalid artifact service port");
  result.port = static_cast<unsigned short>(parsed);
  return result;
}

std::string RemoteArtifactStore::validate_object_id(const std::string &object_id) {
  const auto prefix = std::string{"sha256:"};
  const auto hex = object_id.starts_with(prefix) ? object_id.substr(prefix.size()) : object_id;
  if (hex.size() != 64 || !std::all_of(hex.begin(), hex.end(), [](unsigned char value) {
        return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
      }))
    throw Error(ErrorCode::Validation, "Invalid artifact object identifier");
  return hex;
}

RemoteArtifactStore::RemoteArtifactStore(std::string service_url, std::string bearer_token,
                                         std::filesystem::path cache_root, Storage &storage,
                                         ArtifactStoreLimits limits)
    : endpoint_(parse_endpoint(service_url)), bearer_token_(std::move(bearer_token)),
      cache_root_(std::move(cache_root)), storage_(storage), limits_(limits) {
  if (bearer_token_.empty() || bearer_token_.size() > 4096)
    throw Error(ErrorCode::Configuration, "Artifact service bearer token is invalid");
  if (limits_.max_object_bytes == 0 || limits_.max_temp_bytes < limits_.max_object_bytes)
    throw Error(ErrorCode::Configuration, "Invalid artifact store limits");
  std::error_code error;
  std::filesystem::create_directories(cache_root_ / "temp", error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to create artifact transfer cache");
}

Artifact RemoteArtifactStore::upload(Artifact metadata, const std::filesystem::path &source,
                                     std::uint64_t source_size) const {
  if (source_size > limits_.max_object_bytes)
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  const auto metadata_json = Json(metadata).dump();
  if (metadata_json.size() > 64 * 1024 || metadata_json.find('\r') != std::string::npos ||
      metadata_json.find('\n') != std::string::npos)
    throw Error(ErrorCode::Validation, "Artifact metadata exceeds the transfer limit");
  http::request<http::file_body> request{http::verb::put, artifact_base_target(endpoint_), 11};
  beast::error_code error;
  request.body().open(source.c_str(), beast::file_mode::scan, error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to open artifact transfer source");
  request.content_length(source_size);
  request.set(http::field::content_type, "application/octet-stream");
  request.set("X-LASO-Artifact-Metadata", metadata_json);
  add_auth(request, bearer_token_);
  http::response_parser<http::string_body> parser;
  exchange(endpoint_, bearer_token_, request, parser, limits_.max_object_bytes);
  const auto response = parser.release();
  if (response.result_int() != 200)
    throw_gateway_error(response.result_int(), "");
  try {
    return Json::parse(response.body()).get<Artifact>();
  } catch (const Json::exception &) {
    throw Error(ErrorCode::Storage, "Artifact gateway returned malformed metadata");
  }
}

Artifact RemoteArtifactStore::put(Artifact metadata, std::span<const std::byte> bytes) {
  if (bytes.size() > limits_.max_object_bytes)
    throw Error(ErrorCode::Validation, "Artifact exceeds the configured object limit");
  const auto temporary = cache_root_ / "temp" / ("upload-" + uuid());
  auto fd = open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0)
    throw Error(ErrorCode::Storage, "Unable to create artifact transfer object");
  try {
    write_all(fd, reinterpret_cast<const unsigned char *>(bytes.data()), bytes.size());
    if (fsync(fd) != 0)
      throw Error(ErrorCode::Storage, "Artifact transfer flush failed");
    close(fd);
    fd = -1;
    const auto result = put_file(std::move(metadata), temporary);
    std::error_code ignored;
    std::filesystem::remove(temporary, ignored);
    return result;
  } catch (...) {
    if (fd >= 0)
      close(fd);
    std::error_code ignored;
    std::filesystem::remove(temporary, ignored);
    throw;
  }
}

Artifact RemoteArtifactStore::put_file(Artifact metadata, const std::filesystem::path &source) {
  std::error_code error;
  if (!std::filesystem::is_regular_file(source, error) || error)
    throw Error(ErrorCode::Validation, "Artifact source is not a regular file");
  const auto size = std::filesystem::file_size(source, error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to inspect artifact source");
  const auto result = upload(std::move(metadata), source, size);
  storage_.commit({{RecordKind::Artifact, result.id, result.run_id, Json(result)}});
  return result;
}

std::pair<std::string, std::uint64_t>
RemoteArtifactStore::head(const std::string &object_id) const {
  const auto hex = validate_object_id(object_id);
  http::request<http::empty_body> request{http::verb::head, object_target(endpoint_, hex), 11};
  add_auth(request, bearer_token_);
  http::response_parser<http::empty_body> parser;
  exchange(endpoint_, bearer_token_, request, parser, limits_.max_object_bytes);
  const auto response = parser.release();
  if (response.result_int() != 200)
    throw_gateway_error(response.result_int(), "");
  const auto digest = std::string(response["X-LASO-SHA256"]);
  if (digest.empty())
    throw Error(ErrorCode::Storage, "Artifact gateway omitted object integrity metadata");
  return {digest, response_size(response)};
}

bool RemoteArtifactStore::exists(const std::string &object_id) const {
  try {
    (void)head(object_id);
    return true;
  } catch (const Error &error) {
    if (error.code == ErrorCode::NotFound)
      return false;
    throw;
  }
}

void RemoteArtifactStore::verify(const std::string &object_id, const std::string &expected_sha256,
                                 std::uint64_t expected_size) const {
  const auto [digest, size] = head(object_id);
  const auto expected =
      expected_sha256.empty() ? validate_object_id(object_id) : validate_object_id(expected_sha256);
  if (digest != expected || (expected_size != 0 && size != expected_size))
    throw Error(ErrorCode::Conflict, "Artifact object integrity check failed");
}

void RemoteArtifactStore::download(const std::string &object_id,
                                   const std::filesystem::path &destination) const {
  const auto hex = validate_object_id(object_id);
  std::error_code error;
  std::filesystem::create_directories(destination.parent_path(), error);
  if (error)
    throw Error(ErrorCode::Storage, "Unable to create artifact destination");
  const auto temporary = destination.string() + ".tmp-" + uuid();
  http::request<http::empty_body> request{http::verb::get, object_target(endpoint_, hex), 11};
  add_auth(request, bearer_token_);
  http::response_parser<http::file_body> parser;
  beast::error_code beast_error;
  parser.get().body().open(temporary.c_str(), beast::file_mode::write, beast_error);
  if (beast_error)
    throw Error(ErrorCode::Storage, "Unable to create artifact download");
  std::optional<http::response<http::file_body>> response;
  try {
    exchange(endpoint_, bearer_token_, request, parser, limits_.max_object_bytes);
    response.emplace(parser.release());
    if (response->result_int() != 200)
      throw_gateway_error(response->result_int(), "");
    const auto [digest, size] = sha256_file(temporary);
    if (digest != validate_object_id(object_id) || size != response_size(*response))
      throw Error(ErrorCode::Conflict, "Downloaded artifact failed integrity validation");
    response->body().close();
    if (std::rename(temporary.c_str(), destination.c_str()) != 0)
      throw Error(ErrorCode::Storage, "Unable to finalize artifact download");
  } catch (...) {
    if (response)
      response->body().close();
    else
      parser.get().body().close();
    std::filesystem::remove(temporary, error);
    throw;
  }
}

void RemoteArtifactStore::materialize(const std::string &object_id,
                                      const std::filesystem::path &destination,
                                      const std::string &expected_sha256,
                                      std::uint64_t expected_size) const {
  verify(object_id, expected_sha256, expected_size);
  download(object_id, destination);
  try {
    const auto [digest, size] = sha256_file(destination);
    const auto expected = expected_sha256.empty() ? validate_object_id(object_id)
                                                  : validate_object_id(expected_sha256);
    if (digest != expected || (expected_size != 0 && size != expected_size))
      throw Error(ErrorCode::Conflict, "Materialized artifact failed integrity validation");
  } catch (...) {
    std::error_code ignored;
    std::filesystem::remove(destination, ignored);
    throw;
  }
}

ArtifactIntegrityReport RemoteArtifactStore::integrity() const {
  throw Error(ErrorCode::Policy, "Remote artifact integrity is owned by the gateway");
}

Json RemoteArtifactStore::collect_garbage(bool, std::uint64_t) {
  throw Error(ErrorCode::Policy, "Remote artifact cleanup must run on the gateway owner");
}
} // namespace laso
