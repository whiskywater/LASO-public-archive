#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <laso/runtime/workspace.hpp>
#include <set>

namespace laso {
namespace {
bool valid_digest(const std::string &value) {
  return value.size() == 64 && std::all_of(value.begin(), value.end(), [](unsigned char ch) {
           return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
         });
}

bool safe_relative_path(const std::string &value) {
  if (value.empty() || value.size() > 512 || value.front() == '/' || value.front() == '\\' ||
      value.find(':') != std::string::npos || value.find('\\') != std::string::npos ||
      value.find('\0') != std::string::npos)
    return false;
  const auto path = std::filesystem::path(value);
  if (path.empty() || path.is_absolute() || path.has_root_name() || path.has_root_directory())
    return false;
  for (const auto &part : path)
    if (part == ".." || part == ".")
      return false;
  return true;
}

bool safe_component(const std::string &value) {
  return !value.empty() && value.size() <= 128 && value != "." && value != ".." &&
         std::all_of(value.begin(), value.end(), [](unsigned char c) {
           return std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == ':';
         });
}

std::vector<unsigned char> read_file(const std::filesystem::path &path, std::size_t limit) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream)
    throw Error(ErrorCode::Storage, "Unable to read workspace file");
  stream.seekg(0, std::ios::end);
  const auto size = stream.tellg();
  if (size < 0 || static_cast<std::uint64_t>(size) > limit)
    throw Error(ErrorCode::Validation, "Workspace inline file exceeds the configured limit");
  stream.seekg(0, std::ios::beg);
  std::vector<unsigned char> bytes(static_cast<std::size_t>(size));
  if (!bytes.empty() && !stream.read(reinterpret_cast<char *>(bytes.data()), bytes.size()))
    throw Error(ErrorCode::Storage, "Unable to read workspace file");
  return bytes;
}

void check_manifest_size(const Json &manifest, const WorkspaceManifestLimits &limits) {
  if (!manifest.is_object() ||
      (manifest.value("version", 0U) != 1 && manifest.value("version", 0U) != 2) ||
      !manifest.contains("files") || !manifest.at("files").is_array() ||
      manifest.dump().size() > limits.max_manifest_bytes)
    throw Error(ErrorCode::Validation, "Invalid or oversized workspace manifest");
}

std::vector<std::filesystem::path> workspace_files(const std::filesystem::path &root) {
  std::error_code error;
  const auto canonical_root = std::filesystem::canonical(root, error);
  if (error || !std::filesystem::is_directory(canonical_root, error))
    throw Error(ErrorCode::Validation, "Workspace root is not a directory");
  std::vector<std::filesystem::path> result;
  for (const auto &entry : std::filesystem::recursive_directory_iterator(
           canonical_root, std::filesystem::directory_options::none, error)) {
    if (error)
      throw Error(ErrorCode::Storage, "Unable to enumerate workspace");
    if (entry.is_symlink(error))
      throw Error(ErrorCode::Validation, "Workspace symlinks are not permitted");
    if (!entry.is_regular_file(error)) {
      if (error)
        throw Error(ErrorCode::Storage, "Unable to inspect workspace entry");
      continue;
    }
    result.push_back(entry.path());
  }
  std::sort(result.begin(), result.end());
  return result;
}

std::filesystem::path stage_workspace_impl(const Json &manifest,
                                           const std::filesystem::path &staging_root,
                                           const std::string &run_id, const std::string &work_id,
                                           const std::string &attempt_id,
                                           WorkspaceManifestLimits limits, ArtifactStore *store) {
  validate_workspace_manifest(manifest, limits);
  if (!safe_component(run_id) || !safe_component(work_id) || !safe_component(attempt_id))
    throw Error(ErrorCode::Validation, "Workspace staging identity is incomplete");
  if (manifest.at("version") == 2 && store == nullptr)
    throw Error(ErrorCode::Storage, "An artifact store is required for object-backed workspaces");
  const auto stage =
      staging_root / ("run-" + run_id) / ("work-" + work_id) / ("attempt-" + attempt_id);
  std::error_code error;
  std::filesystem::remove_all(stage, error);
  if (error || !std::filesystem::create_directories(stage, error) || error)
    throw Error(ErrorCode::Storage, "Unable to create workspace staging directory");
  try {
    for (const auto &entry : manifest.at("files")) {
      const auto relative = std::filesystem::path(entry.at("path").get<std::string>());
      const auto destination = stage / relative;
      std::filesystem::create_directories(destination.parent_path(), error);
      if (error)
        throw Error(ErrorCode::Storage, "Unable to create workspace parent directory");
      if (manifest.at("version") == 2) {
        store->materialize(entry.at("object_id").get<std::string>(), destination,
                           entry.at("sha256").get<std::string>(),
                           entry.at("size").get<std::uint64_t>());
      } else {
        const auto bytes = entry.at("data").get<std::vector<unsigned char>>();
        std::ofstream stream(destination, std::ios::binary | std::ios::trunc);
        if (!stream || (!bytes.empty() &&
                        !stream.write(reinterpret_cast<const char *>(bytes.data()), bytes.size())))
          throw Error(ErrorCode::Storage, "Unable to stage workspace file");
      }
    }
  } catch (...) {
    std::filesystem::remove_all(stage, error);
    throw;
  }
  return stage;
}
} // namespace

void validate_workspace_manifest(const Json &manifest, WorkspaceManifestLimits limits) {
  check_manifest_size(manifest, limits);
  const auto version = manifest.at("version").get<unsigned>();
  if (manifest.at("files").size() > limits.max_files)
    throw Error(ErrorCode::Validation, "Workspace file count exceeds the configured limit");
  std::set<std::string> paths;
  std::size_t total = 0;
  for (const auto &entry : manifest.at("files")) {
    if (!entry.is_object() || !entry.contains("path") || !entry.at("path").is_string() ||
        !safe_relative_path(entry.at("path").get<std::string>()) || !entry.contains("sha256") ||
        !entry.at("sha256").is_string() || !valid_digest(entry.at("sha256").get<std::string>()))
      throw Error(ErrorCode::Validation, "Invalid workspace manifest entry");
    const auto path = entry.at("path").get<std::string>();
    if (!paths.insert(path).second)
      throw Error(ErrorCode::Conflict, "Workspace manifest contains duplicate paths");
    if (version == 1) {
      if (!entry.contains("data") || !entry.at("data").is_array())
        throw Error(ErrorCode::Validation, "Inline workspace entry is missing data");
      const auto bytes = entry.at("data").get<std::vector<unsigned char>>();
      if (bytes.size() > limits.max_inline_file_bytes ||
          total > limits.max_inline_total_bytes - bytes.size() ||
          entry.at("sha256") != sha256_bytes(bytes))
        throw Error(ErrorCode::Validation, "Workspace inline integrity or size check failed");
      total += bytes.size();
    } else {
      if (!entry.contains("object_id") || !entry.at("object_id").is_string() ||
          !entry.contains("size") || !entry.at("size").is_number_unsigned())
        throw Error(ErrorCode::Validation, "Object-backed workspace entry is incomplete");
      const auto size = entry.at("size").get<std::uint64_t>();
      if (size > limits.max_file_bytes || total > limits.max_total_bytes - size)
        throw Error(ErrorCode::Validation, "Workspace object exceeds the configured limit");
      total += static_cast<std::size_t>(size);
    }
  }
}

Json workspace_manifest(const std::filesystem::path &root, WorkspaceManifestLimits limits) {
  Json result{{"version", 1}, {"files", Json::array()}};
  std::size_t total = 0;
  const auto canonical_root = std::filesystem::canonical(root);
  for (const auto &file : workspace_files(canonical_root)) {
    const auto relative = std::filesystem::relative(file, canonical_root).generic_string();
    if (!safe_relative_path(relative))
      throw Error(ErrorCode::Validation, "Workspace contains an unsafe relative path");
    const auto bytes = read_file(file, limits.max_inline_file_bytes);
    if (total > limits.max_inline_total_bytes - bytes.size())
      throw Error(ErrorCode::Validation, "Workspace inline content exceeds the configured limit");
    total += bytes.size();
    result["files"].push_back({{"path", relative},
                               {"size", bytes.size()},
                               {"sha256", sha256_bytes(bytes)},
                               {"data", bytes}});
  }
  validate_workspace_manifest(result, limits);
  return result;
}

Json workspace_manifest(const std::filesystem::path &root, ArtifactStore &store,
                        WorkspaceManifestLimits limits, const std::string &run_id,
                        const std::string &node_work_id, const std::string &attempt_id,
                        const std::string &worker_id, std::uint64_t fencing_token) {
  const auto canonical_root = std::filesystem::canonical(root);
  Json result{{"version", 2}, {"storage", "content-addressed"}, {"files", Json::array()}};
  if (!run_id.empty())
    result["provenance"] = {{"run_id", run_id},
                            {"node_work_id", node_work_id},
                            {"attempt_id", attempt_id},
                            {"worker_id", worker_id},
                            {"fencing_token", fencing_token}};
  std::size_t total = 0;
  for (const auto &file : workspace_files(canonical_root)) {
    const auto relative = std::filesystem::relative(file, canonical_root).generic_string();
    if (!safe_relative_path(relative))
      throw Error(ErrorCode::Validation, "Workspace contains an unsafe relative path");
    Artifact metadata;
    metadata.run_id = run_id;
    metadata.node_id = node_work_id;
    metadata.name = relative;
    metadata.metadata = {
        {"attempt_id", attempt_id}, {"worker_id", worker_id}, {"fencing_token", fencing_token}};
    const auto artifact = store.put_file(std::move(metadata), file);
    if (artifact.size > limits.max_file_bytes || total > limits.max_total_bytes - artifact.size)
      throw Error(ErrorCode::Validation, "Workspace exceeds the configured object limit");
    total += static_cast<std::size_t>(artifact.size);
    result["files"].push_back({{"path", relative},
                               {"object_id", artifact.object_id},
                               {"sha256", artifact.sha256},
                               {"size", artifact.size}});
  }
  validate_workspace_manifest(result, limits);
  return result;
}

std::filesystem::path stage_workspace(const Json &manifest,
                                      const std::filesystem::path &staging_root,
                                      const std::string &run_id, const std::string &work_id,
                                      const std::string &attempt_id,
                                      WorkspaceManifestLimits limits) {
  return stage_workspace_impl(manifest, staging_root, run_id, work_id, attempt_id, limits, nullptr);
}

std::filesystem::path stage_workspace(const Json &manifest,
                                      const std::filesystem::path &staging_root,
                                      const std::string &run_id, const std::string &work_id,
                                      const std::string &attempt_id, WorkspaceManifestLimits limits,
                                      ArtifactStore *store) {
  return stage_workspace_impl(manifest, staging_root, run_id, work_id, attempt_id, limits, store);
}
} // namespace laso
