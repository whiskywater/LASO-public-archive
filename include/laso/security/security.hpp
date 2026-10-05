#pragma once
#include <filesystem>
#include <laso/core/types.hpp>

namespace laso {
struct Actor {
  std::string id = "local";
  bool authenticated = false;
};
struct AuthorizationContext {
  Actor actor;
  std::string action, resource;
};
class IdentityProvider {
public:
  virtual ~IdentityProvider() = default;
  virtual Actor authenticate(const std::string &credential) const = 0;
  virtual bool authorize(const AuthorizationContext &) const = 0;
};
class LocalDevelopmentIdentity final : public IdentityProvider {
public:
  Actor authenticate(const std::string &) const override {
    return {};
  }
  bool authorize(const AuthorizationContext &) const override {
    return true;
  }
};
class SecretProvider {
public:
  virtual ~SecretProvider() = default;
  virtual std::string resolve(const std::string &reference) const = 0;
};
class EnvironmentSecretProvider final : public SecretProvider {
public:
  std::string resolve(const std::string &reference) const override;
};
} // namespace laso
