#pragma once
#include <cstddef>
#include <filesystem>
#include <laso/artifacts/artifacts.hpp>
#include <laso/core/types.hpp>

namespace laso {
struct WorkspaceManifestLimits {
  std::size_t max_files = 4096;
  std::size_t max_file_bytes = std::size_t{256} * 1024 * 1024;
  std::size_t max_total_bytes = std::size_t{512} * 1024 * 1024;
  std::size_t max_manifest_bytes = std::size_t{4} * 1024 * 1024;
  std::size_t max_inline_file_bytes = 16 * 1024;
  std::size_t max_inline_total_bytes = 48 * 1024;
};

// Version 1 retains the small inline representation for compatibility. Version
// 2 contains only relative paths and immutable content-addressed object
// references; it never embeds repository-scale file contents.
Json workspace_manifest(const std::filesystem::path &root, WorkspaceManifestLimits limits = {});
Json workspace_manifest(const std::filesystem::path &root, ArtifactStore &store,
                        WorkspaceManifestLimits limits = {}, const std::string &run_id = {},
                        const std::string &node_work_id = {}, const std::string &attempt_id = {},
                        const std::string &worker_id = {}, std::uint64_t fencing_token = 0);
std::filesystem::path stage_workspace(const Json &manifest,
                                      const std::filesystem::path &staging_root,
                                      const std::string &run_id, const std::string &work_id,
                                      const std::string &attempt_id,
                                      WorkspaceManifestLimits limits = {});
std::filesystem::path stage_workspace(const Json &manifest,
                                      const std::filesystem::path &staging_root,
                                      const std::string &run_id, const std::string &work_id,
                                      const std::string &attempt_id, WorkspaceManifestLimits limits,
                                      ArtifactStore *store);
void validate_workspace_manifest(const Json &manifest, WorkspaceManifestLimits limits = {});
} // namespace laso
