#include <string>

#include "utils/assert.hpp"

namespace opossum {

std::string next_value(const std::string& value, const std::string& supported_characters,
                       const uint64_t string_prefix_length) {
  Assert(value.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");

  if (value.length() < string_prefix_length) {
    return value + supported_characters.front();
  }

  // Special case: return value if it is the last supported one.
  if (value == std::string(string_prefix_length, supported_characters.back())) {
    return value;
  }

  const auto cleaned_value = value.substr(0, string_prefix_length);
  const auto last_char = cleaned_value.back();
  const auto substring = cleaned_value.substr(0, cleaned_value.length() - 1);

  if (last_char != supported_characters.back()) {
    return substring + static_cast<char>(last_char + 1);
  }

  return next_value(substring, supported_characters, string_prefix_length - 1);
}

std::string next_value(const std::string& value, const std::string& supported_characters) {
  return next_value(value, supported_characters, value.length() + 1);
}

uint64_t ipow(uint64_t base, uint64_t exp) {
  uint64_t result = 1;

  for (;;) {
    if (exp & 1) {
      result *= base;
    }

    exp >>= 1;

    if (!exp) {
      break;
    }

    base *= base;
  }

  return result;
}

uint64_t base_value_for_prefix_length(const uint64_t string_prefix_length, const std::string& supported_characters) {
  auto result = 1ul;
  for (auto exp = 1ul; exp < string_prefix_length; exp++) {
    result += ipow(supported_characters.length(), exp);
  }
  return result;
}

uint64_t convert_string_to_number_representation(const std::string& value, const std::string& supported_characters,
                                                 const uint64_t string_prefix_length) {
  if (value.empty()) {
    return 0;
  }

  DebugAssert(string_prefix_length > 0, "Invalid prefix length.");
  Assert(value.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");

  const auto base = base_value_for_prefix_length(string_prefix_length, supported_characters);
  const auto trimmed = value.substr(0, string_prefix_length);
  const auto char_value = (trimmed.front() - supported_characters.front()) * base + 1;
  return char_value +
         convert_string_to_number_representation(trimmed.substr(1, trimmed.length() - 1), supported_characters,
                                                 string_prefix_length - 1) +
         (value.length() > string_prefix_length ? 1 : 0);
}

std::string convert_number_representation_to_string(const uint64_t value, const std::string& supported_characters,
                                                    const uint64_t string_prefix_length) {
  DebugAssert(convert_string_to_number_representation(std::string(string_prefix_length, supported_characters.back()),
                                                      supported_characters, string_prefix_length) >= value,
              "Value is not in valid range for supported_characters and string_prefix_length.");

  if (value == 0ul) {
    return "";
  }

  const auto base = base_value_for_prefix_length(string_prefix_length, supported_characters);
  const auto character = supported_characters.at((value - 1) / base);
  return character +
         convert_number_representation_to_string((value - 1) % base, supported_characters, string_prefix_length - 1);
}

}  // namespace opossum
