# First real Linux validation checklist

Historical status at delivery: **PENDING**. This file is a procedure, not evidence
of success; see the current deployment results in `../VALIDATION.md`.
Do not promote a row to PASS until the corresponding command has actually run.

1. On Ubuntu 24.04 or Debian 13, install README dependencies and record `uname -a`,
   `c++ --version`, `cmake --version`, and distribution package versions.
2. Build GCC Debug and run all CTest tests:

   ```sh
   CC=gcc CXX=g++ cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
     -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
   cmake --build build
   ctest --test-dir build --output-on-failure
   ```

3. Build Clang in a separate directory and repeat CTest:

   ```sh
   CC=clang CXX=clang++ cmake -S . -B build-clang -G Ninja -DCMAKE_BUILD_TYPE=Debug
   cmake --build build-clang
   ctest --test-dir build-clang --output-on-failure
   ```

4. Run clang-format 18 and clang-tidy as in CONTRIBUTING. Inspect warnings rather
   than disabling checks simply to obtain a green status.
5. Run ASan and UBSan together:

   ```sh
   cmake -S . -B build-san -G Ninja -DCMAKE_BUILD_TYPE=Debug \
     -DLASO_ENABLE_ASAN=ON -DLASO_ENABLE_UBSAN=ON
   cmake --build build-san
   ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
     UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
     ctest --test-dir build-san --output-on-failure
   ```

6. Run `bash tests/integration/process-smoke.sh "$(realpath build)" "$(pwd)"`.
   It executes hello, mock review, approval across CLI processes, native plugin,
   daemon health/version, and a daemon stop/restart/approval/resume sequence.
7. Inspect tests for node failure, retry exhaustion, timeout, cancellation, policy
   denial/approval and invalid/incompatible plugin rejection. Confirm failed
   pipelines do not terminate the daemon.
8. Test deployment separately: Docker validation target, Compose persistent volume,
   systemd start/stop and service logs. Confirm API binds to loopback by default.
9. Upload logs as CI artifacts if investigating a failure. They must not contain
   application secrets. Record actual counts, compiler versions and remaining issues
   in VALIDATION.md; do not replace PENDING with a presumed result.

Optional TSan uses a separate build with `-DLASO_ENABLE_TSAN=ON`; CMake rejects
combining it with ASan/UBSan. Native plugin fault isolation is not a sanitizer
guarantee. Fix issues discovered on Linux before release.
