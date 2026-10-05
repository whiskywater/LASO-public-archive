#include <cstdlib>
#include <laso/security/security.hpp>
#include <regex>

namespace laso {
std::string EnvironmentSecretProvider::resolve(const std::string &reference) const {
  static const std::regex valid("[A-Z_][A-Z0-9_]{0,127}");
  if (!std::regex_match(reference, valid))
    throw Error(ErrorCode::Configuration, "Invalid secret reference");
  auto *value = std::getenv(reference.c_str());
  if (!value)
    throw Error(ErrorCode::Configuration, "Required secret unavailable");
  return value;
}
} // namespace laso
