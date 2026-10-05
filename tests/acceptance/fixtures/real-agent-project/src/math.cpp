#include "laso_phase1/math.hpp"

#include <stdexcept>

namespace laso_phase1 {
int saturating_sum(int left, int right, int lower, int upper) {
  if (lower > upper)
    throw std::invalid_argument("lower bound exceeds upper bound");

  // Intentional defect: the phase-one agent must apply both bounds.
  return left + right;
}
} // namespace laso_phase1
