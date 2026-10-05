#include "laso_phase1/label.hpp"

#include <cctype>

namespace laso_phase1 {
std::string slugify(std::string_view input) {
  // Intentional defect: the phase-one agent must implement the requested
  // lower-case, separator-collapsing behavior.
  std::string result;
  for (const auto character : input) {
    if (std::isalnum(static_cast<unsigned char>(character)))
      result.push_back(character);
    else
      result.push_back('-');
  }
  return result;
}
} // namespace laso_phase1
