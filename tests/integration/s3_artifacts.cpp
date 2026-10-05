#include "../support.hpp"
#include <array>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <laso/artifacts/artifacts.hpp>
#include <optional>
#include <string_view>
#include <thread>
#include <vector>

using namespace laso;
using namespace laso::test;

namespace {
class ScopedEnvironment {
public:
  ScopedEnvironment(const char *name, const std::string &value) : name_(name) {
    if (const auto *existing = std::getenv(name))
      previous_ = existing;
    if (setenv(name_.c_str(), value.c_str(), 1) != 0)
      throw std::runtime_error("Unable to set S3 test environment");
  }
  ~ScopedEnvironment() {
    if (previous_)
      setenv(name_.c_str(), previous_->c_str(), 1);
    else
      unsetenv(name_.c_str());
  }
  ScopedEnvironment(const ScopedEnvironment &) = delete;
  ScopedEnvironment &operator=(const ScopedEnvironment &) = delete;

private:
  std::string name_;
  std::optional<std::string> previous_;
};

std::optional<S3ArtifactStoreConfig> test_config(const std::filesystem::path &scratch) {
  const auto *endpoint = std::getenv("LASO_S3_TEST_ENDPOINT");
  const auto *bucket = std::getenv("LASO_S3_TEST_BUCKET");
  if (!endpoint || !bucket || *endpoint == '\0' || *bucket == '\0')
    return std::nullopt;
  S3ArtifactStoreConfig config;
  config.endpoint = endpoint;
  config.bucket = bucket;
  config.region = "us-east-1";
  config.prefix = "artifact-test/" + uuid();
  config.scratch_root = scratch;
  config.path_style = true;
  config.allow_http = std::string_view(endpoint).starts_with("http://");
  config.connect_timeout_ms = 500;
  config.request_timeout_ms = 10000;
  config.max_retries = 1;
  return config;
}

Artifact metadata(std::string name) {
  Artifact value;
  value.run_id = "s3-conformance-run";
  value.node_id = "artifact-node";
  value.name = std::move(name);
  value.media_type = "application/octet-stream";
  return value;
}

void ensure_bucket(const S3ArtifactStoreConfig &config) {
  Aws::S3::S3ClientConfiguration client_config;
  client_config.region = config.region;
  client_config.endpointOverride = config.endpoint;
  client_config.scheme = std::string_view(config.endpoint).starts_with("http://")
                             ? Aws::Http::Scheme::HTTP
                             : Aws::Http::Scheme::HTTPS;
  client_config.connectTimeoutMs = static_cast<long>(config.connect_timeout_ms);
  client_config.requestTimeoutMs = static_cast<long>(config.request_timeout_ms);
  Aws::S3::S3Client client(client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                           config.path_style);
  Aws::S3::Model::HeadBucketRequest head;
  head.SetBucket(config.bucket);
  if (client.HeadBucket(head).IsSuccess())
    return;
  Aws::S3::Model::CreateBucketRequest create;
  create.SetBucket(config.bucket);
  if (!client.CreateBucket(create).IsSuccess())
    throw std::runtime_error("Unable to initialize disposable S3 test bucket");
}
} // namespace

TEST(S3Artifacts, StreamsLargeContentAddressedObjectAndMaterializesVerifiedBytes) {
  TemporaryDirectory dir;
  auto config = test_config(dir.path / "scratch");
  if (!config)
    GTEST_SKIP() << "S3 integration endpoint is not configured";
  auto storage = make_storage(dir.path / "state.db");
  S3ArtifactStore store(*config, *storage, {128U * 1024U * 1024U, 256U * 1024U * 1024U, 3600});
  ASSERT_NO_THROW(ensure_bucket(*config));

  const auto source = dir.path / "synthetic-64m.bin";
  {
    std::ofstream output(source, std::ios::binary);
    std::array<char, 1024 * 1024> block{};
    for (std::size_t block_index = 0; block_index < 64; ++block_index) {
      for (std::size_t i = 0; i < block.size(); ++i)
        block[i] = static_cast<char>((block_index * 31 + i * 17 + 9) & 0xff);
      output.write(block.data(), static_cast<std::streamsize>(block.size()));
    }
    ASSERT_TRUE(output.good());
  }
  const auto source_hash = sha256_file(source);
  const auto artifact = store.put_file(metadata("large.bin"), source);
  EXPECT_EQ(artifact.size, 64U * 1024U * 1024U);
  EXPECT_EQ(artifact.sha256, source_hash.first);
  EXPECT_TRUE(store.exists(artifact.object_id));
  EXPECT_NO_THROW(store.verify(artifact.object_id, artifact.sha256, artifact.size));

  const auto restored = dir.path / "restored" / "large.bin";
  EXPECT_NO_THROW(store.materialize(artifact.object_id, restored, artifact.sha256, artifact.size));
  EXPECT_EQ(sha256_file(restored), source_hash);
  const auto report = store.integrity();
  EXPECT_EQ(report.invalid, 0U);
  EXPECT_EQ(report.verified, 1U);
  EXPECT_THROW(store.collect_garbage(true), Error);
}

TEST(S3Artifacts, DuplicateConcurrentPublicationIsIdempotentAndMissingObjectsFailClosed) {
  TemporaryDirectory dir;
  auto config = test_config(dir.path / "scratch");
  if (!config)
    GTEST_SKIP() << "S3 integration endpoint is not configured";
  auto storage = make_storage(dir.path / "state.db");
  S3ArtifactStore store(*config, *storage);
  ASSERT_NO_THROW(ensure_bucket(*config));
  const auto source = dir.path / "small.bin";
  std::ofstream(source, std::ios::binary) << "synthetic content-addressed object";
  const auto expected = sha256_file(source);
  const auto missing = "sha256:" + std::string(64, '0');
  EXPECT_FALSE(store.exists(missing));
  EXPECT_THROW(store.verify(missing), Error);

  std::array<std::string, 4> results{};
  std::array<std::exception_ptr, 4> errors{};
  std::vector<std::thread> writers;
  for (std::size_t index = 0; index < results.size(); ++index) {
    writers.emplace_back([&, index] {
      try {
        results[index] = store.put_file(metadata("same.bin"), source).object_id;
      } catch (...) {
        errors[index] = std::current_exception();
      }
    });
  }
  for (auto &writer : writers)
    writer.join();
  for (std::size_t index = 0; index < results.size(); ++index) {
    if (errors[index])
      std::rethrow_exception(errors[index]);
    EXPECT_EQ(results[index], "sha256:" + expected.first);
  }
  const auto report = store.integrity();
  EXPECT_EQ(report.invalid, 0U);
  EXPECT_EQ(report.verified, 1U);
}

TEST(S3Artifacts, UnavailableEndpointFailsWithinConfiguredBound) {
  TemporaryDirectory dir;
  S3ArtifactStoreConfig config;
  config.endpoint = "http://127.0.0.1:1";
  config.bucket = "laso-test-bucket";
  config.prefix = "artifact-test";
  config.scratch_root = dir.path / "scratch";
  config.connect_timeout_ms = 100;
  config.request_timeout_ms = 250;
  config.max_retries = 0;
  config.allow_http = true;
  config.path_style = true;
  auto storage = make_storage(dir.path / "state.db");
  S3ArtifactStore store(config, *storage);
  const auto source = dir.path / "input.bin";
  std::ofstream(source, std::ios::binary) << "bounded outage";
  const auto begin = std::chrono::steady_clock::now();
  EXPECT_THROW(store.put_file(metadata("outage.bin"), source), Error);
  EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(5));
  EXPECT_TRUE(storage->list(RecordKind::Artifact).empty());
}

TEST(S3Artifacts, InvalidCredentialsFailClosedWithoutLeakingSecrets) {
  TemporaryDirectory dir;
  auto config = test_config(dir.path / "scratch");
  if (!config)
    GTEST_SKIP() << "S3 integration endpoint is not configured";
  const auto secret = "synthetic-invalid-" + uuid();
  ScopedEnvironment access_key("AWS_ACCESS_KEY_ID", "LASOINVALIDTESTKEY");
  ScopedEnvironment secret_key("AWS_SECRET_ACCESS_KEY", secret);
  ScopedEnvironment metadata_disabled("AWS_EC2_METADATA_DISABLED", "true");
  auto storage = make_storage(dir.path / "state.db");
  S3ArtifactStore store(*config, *storage);
  try {
    (void)store.exists("sha256:" + std::string(64, '0'));
    FAIL() << "Invalid S3 credentials unexpectedly succeeded";
  } catch (const Error &error) {
    EXPECT_EQ(std::string(error.what()).find(secret), std::string::npos);
  }
}

TEST(S3Artifacts, DetectsContentCorruptionAtPublishedObjectKey) {
  TemporaryDirectory dir;
  auto config = test_config(dir.path / "scratch");
  if (!config)
    GTEST_SKIP() << "S3 integration endpoint is not configured";
  auto storage = make_storage(dir.path / "state.db");
  S3ArtifactStore store(*config, *storage);
  ASSERT_NO_THROW(ensure_bucket(*config));
  const auto source = dir.path / "input.bin";
  std::ofstream(source, std::ios::binary) << "synthetic content-addressed object";
  const auto artifact = store.put_file(metadata("corrupt.bin"), source);

  Aws::S3::S3ClientConfiguration client_config;
  client_config.region = config->region;
  client_config.endpointOverride = config->endpoint;
  client_config.scheme = Aws::Http::Scheme::HTTP;
  client_config.connectTimeoutMs = 500;
  client_config.requestTimeoutMs = 5000;
  Aws::S3::S3Client client(client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                           true);
  const auto digest = artifact.sha256;
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(config->bucket);
  request.SetKey(config->prefix + "/objects/" + digest.substr(0, 2) + "/" + digest.substr(2));
  const std::string corrupt_bytes(artifact.size, 'x');
  auto body = Aws::MakeShared<Aws::StringStream>("LASO-S3-test");
  *body << corrupt_bytes;
  request.SetContentLength(static_cast<long long>(corrupt_bytes.size()));
  request.SetBody(body);
  ASSERT_TRUE(client.PutObject(request).IsSuccess());

  EXPECT_THROW(store.verify(artifact.object_id, artifact.sha256, artifact.size), Error);
  const auto report = store.integrity();
  EXPECT_EQ(report.verified, 0U);
  EXPECT_EQ(report.invalid, 1U);
}
