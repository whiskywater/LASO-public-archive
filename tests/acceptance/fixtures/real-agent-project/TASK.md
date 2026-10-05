# LASO Phase 1 synthetic workload

This repository is disposable test data. The requested feature is to provide
two small, dependency-free helpers:

* `saturating_sum` must add two integers and clamp the result to an inclusive
  lower/upper range. Reversed bounds are invalid.
* `slugify` must produce a lower-case, hyphen-separated ASCII slug and omit
  leading, trailing, and repeated separators.

The Codex branch owns `src/math.cpp`. The OpenCode branch owns `src/label.cpp`.
Do not edit the other branch's file, the test harness, or files outside this
repository.
