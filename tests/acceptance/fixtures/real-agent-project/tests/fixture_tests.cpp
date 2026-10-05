#include "laso_phase1/label.hpp"
#include "laso_phase1/math.hpp"

#include <cstdlib>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {
void expect(bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << "FAIL: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

void expect_throw(const std::function<void()> &operation, const std::string &message) {
  try {
    operation();
  } catch (const std::invalid_argument &) {
    return;
  }
  expect(false, message);
}
} // namespace

int main() {
  using laso_phase1::saturating_sum;
  using laso_phase1::slugify;

  expect(saturating_sum(2, 3, -10, 10) == 5, "ordinary sum");
  expect(saturating_sum(7, 8, -10, 10) == 10, "upper saturation");
  expect(saturating_sum(-7, -8, -10, 10) == -10, "lower saturation");
  expect_throw([] { (void)saturating_sum(1, 2, 4, 3); }, "reversed bounds are rejected");

  expect(slugify("Hello, LASO World!") == "hello-laso-world", "slugifies words");
  expect(slugify("  repeated---separators  ") == "repeated-separators", "collapses separators");
  expect(slugify("Already-Plain") == "already-plain", "normalizes case");
  return EXIT_SUCCESS;
}
