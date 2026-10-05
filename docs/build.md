# Build and install

LASO is Linux-first and currently requires a Linux build environment. The
project uses CMake 3.22 or newer, C++20, and a pinned JSON Schema validator
fetched by CMake when it is not already available. A clean checkout is enough;
no development worktree, generated file, or provider account is required.

## Core SQLite build

On Ubuntu or Debian, install the core and test dependencies:

```sh
sudo apt-get update
sudo apt-get install -y \
  build-essential cmake ninja-build git \
  libsqlite3-dev libyaml-cpp-dev nlohmann-json3-dev \
  libspdlog-dev libcli11-dev libboost-system-dev libgtest-dev \
  curl jq
```

Configure and build from the repository root:

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

For a smaller runtime-only build, configure with `-DBUILD_TESTING=OFF`.
The core build does not require PostgreSQL development packages or any agent
provider executable.

The default binaries are:

```text
build/bin/laso
build/bin/laso-server
build/bin/laso-example-worker-host
```

## Release build and sanitizers

```sh
cmake -S . -B build-release -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel 2
ctest --test-dir build-release --output-on-failure

cmake -S . -B build-san -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DLASO_ENABLE_ASAN=ON -DLASO_ENABLE_UBSAN=ON
cmake --build build-san --parallel 2
ctest --test-dir build-san --output-on-failure
```

ThreadSanitizer is a separate build and is environment-dependent:

```sh
cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DLASO_ENABLE_TSAN=ON
```

Do not combine TSan with ASan or UBSan.

## PostgreSQL build

PostgreSQL support is optional at compile time. Install the client libraries
and `libpqxx`, then configure a separate build directory:

```sh
sudo apt-get install -y libpq-dev libpqxx-dev
cmake -S . -B build-postgres -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DLASO_ENABLE_POSTGRES=ON
cmake --build build-postgres --parallel 2
```

The PostgreSQL test suite uses `LASO_TEST_POSTGRES_DSN`. Use a disposable,
authenticated database; never put the DSN in a repository file or command log.
For a complete deterministic two-instance example, see
[the distributed example](../examples/distributed/README.md).

## Optional S3-compatible artifact store

The default build remains independent of AWS credentials and the cloud SDK. To
enable S3 support, install/build the [AWS SDK for C++ S3 component](https://github.com/aws/aws-sdk-cpp).
For a source build on Ubuntu/Debian, the SDK can be built into a user-local
prefix:

```sh
sudo apt-get install -y libcurl4-openssl-dev libssl-dev zlib1g-dev
git clone --recurse-submodules --depth 1 --branch 1.11.890 https://github.com/aws/aws-sdk-cpp.git ../aws-sdk-cpp
cmake -S ../aws-sdk-cpp -B ../aws-sdk-cpp-build \
  -DBUILD_ONLY=s3 -DAUTORUN_UNIT_TESTS=OFF -DBUILD_SHARED_LIBS=OFF \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$HOME/.local/aws-sdk"
cmake --build ../aws-sdk-cpp-build --parallel 2
cmake --install ../aws-sdk-cpp-build

CMAKE_PREFIX_PATH="$HOME/.local/aws-sdk" cmake -S . -B build-s3 -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DLASO_ENABLE_S3=ON
cmake --build build-s3 --parallel 2
ctest --test-dir build-s3 --output-on-failure
```

This opt-in SDK build is only needed for LASO's S3 backend. The S3 integration
tests skip unless `LASO_S3_TEST_ENDPOINT` and `LASO_S3_TEST_BUCKET` point to a
disposable test service and the AWS SDK credential chain is configured for
that service. Never use production buckets or credentials for these tests.

## Optional provider adapters

Provider adapters are opt-in and do not download providers or credentials:

```sh
cmake -S . -B build-providers -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DLASO_BUILD_CODEX_ADAPTER=ON \
  -DLASO_BUILD_OPENCODE_ADAPTER=ON \
  -DLASO_BUILD_CLAUDE_ADAPTER=ON
cmake --build build-providers --parallel 2
```

The corresponding provider executable must already be installed and safely
configured. Adapter tests that need a real provider are opt-in; deterministic
fixtures remain the default test path.

## Install layout

Installation is intentionally small and conventional:

```sh
cmake -S . -B build-user -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$HOME/.local" -DLASO_INSTALL_SYSTEMD_UNIT=OFF
cmake --build build-user --parallel 2
cmake --install build-user
```

This installs the CLI, server, deterministic reference worker, public headers,
the C plugin SDK header, the example configuration, and public documentation.
The state directory is still selected at runtime and is not created in the
installation prefix.

## Windows and containers

The top-level CMake project rejects non-Linux systems. Windows developers can
use WSL or a Linux container; the repository's Dockerfile and Compose example
are supported development paths, not a promise of native Windows binaries.
