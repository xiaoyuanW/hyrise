#include "../../../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"

namespace opossum {

class HistogramUtilsTest : public BaseTest {
  void SetUp() override {
    _supported_characters = "abcdefghijklmnopqrstuvwxyz";
    _prefix_length = 4u;
  }

 protected:
  uint64_t _convert_string_to_number_representation(const std::string& value) {
    return convert_string_to_number_representation(value, _supported_characters, _prefix_length);
  }

  std::string _convert_number_representation_to_string(const uint64_t value) {
    return convert_number_representation_to_string(value, _supported_characters, _prefix_length);
  }

  std::string _previous_value(const std::string& value) {
    return previous_value(value, _supported_characters, _prefix_length);
  }

  std::string _next_value(const std::string& value) { return next_value(value, _supported_characters, _prefix_length); }

 protected:
  std::string _supported_characters;
  uint64_t _prefix_length;
};

TEST_F(HistogramUtilsTest, PreviousValueString) {
  // Special case.
  EXPECT_EQ(_previous_value(""), "");

  EXPECT_EQ(_previous_value("a"), "");
  EXPECT_EQ(_previous_value("b"), "azzz");
  EXPECT_EQ(_previous_value("z"), "yzzz");
  EXPECT_EQ(_previous_value("az"), "ayzz");
  EXPECT_EQ(_previous_value("aaa"), "aa");
  EXPECT_EQ(_previous_value("abcd"), "abcc");
  EXPECT_EQ(_previous_value("abzz"), "abzy");
  EXPECT_EQ(_previous_value("abca"), "abc");
  EXPECT_EQ(_previous_value("abaa"), "aba");
  EXPECT_EQ(_previous_value("aba"), "ab");
}

TEST_F(HistogramUtilsTest, NextValueString) {
  EXPECT_EQ(_next_value(""), "a");
  EXPECT_EQ(_next_value("a"), "aa");
  EXPECT_EQ(_next_value("ayz"), "ayza");
  EXPECT_EQ(_next_value("ayzz"), "az");
  EXPECT_EQ(_next_value("azzz"), "b");
  EXPECT_EQ(_next_value("z"), "za");
  EXPECT_EQ(_next_value("df"), "dfa");
  EXPECT_EQ(_next_value("abcd"), "abce");
  EXPECT_EQ(_next_value("abaz"), "abb");
  EXPECT_EQ(_next_value("abzz"), "ac");
  EXPECT_EQ(_next_value("abca"), "abcb");
  EXPECT_EQ(_next_value("abaa"), "abab");

  // Special case.
  EXPECT_EQ(_next_value("zzzz"), "zzzz");
}

TEST_F(HistogramUtilsTest, StringToNumber) {
  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("a"), 0 * (ipow(26, 3)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("aa"),
            0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("aaaa"),
            0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                0 * (ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("aaab"),
            0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                1 * (ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("azzz"),
            0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                25 * (ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("b"),
            1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("ba"),
            1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("bhja"),
            1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                7 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                0 * (ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("cde"),
            2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 4 * (ipow(26, 1) + ipow(26, 0)) + 1);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"),
            25 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                25 * (ipow(26, 0)) + 1);
}

TEST_F(HistogramUtilsTest, NumberToString) {
  EXPECT_EQ(_convert_number_representation_to_string(0ul), "");
  EXPECT_EQ(_convert_number_representation_to_string(0 * (ipow(26, 3)) + 1), "a");
  EXPECT_EQ(_convert_number_representation_to_string(0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1),
            "aa");
  EXPECT_EQ(_convert_number_representation_to_string(0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     0 * (ipow(26, 1) + ipow(26, 0)) + 1 + 0 * (ipow(26, 0)) + 1),
            "aaaa");
  EXPECT_EQ(_convert_number_representation_to_string(0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     0 * (ipow(26, 1) + ipow(26, 0)) + 1 + 1 * (ipow(26, 0)) + 1),
            "aaab");
  EXPECT_EQ(_convert_number_representation_to_string(0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     25 * (ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 0)) + 1),
            "azzz");
  EXPECT_EQ(_convert_number_representation_to_string(1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1),
            "b");
  EXPECT_EQ(_convert_number_representation_to_string(1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1),
            "ba");
  EXPECT_EQ(_convert_number_representation_to_string(1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     7 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 0 * (ipow(26, 0)) + 1),
            "bhja");
  EXPECT_EQ(_convert_number_representation_to_string(2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     4 * (ipow(26, 1) + ipow(26, 0)) + 1),
            "cde");
  EXPECT_EQ(_convert_number_representation_to_string(25 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                                                     25 * (ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 0)) + 1),
            "zzzz");
}

TEST_F(HistogramUtilsTest, NumberToStringBruteForce) {
  constexpr auto max = 475'254ul;

  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_LT(_convert_number_representation_to_string(number), _convert_number_representation_to_string(number + 1));
  }
}

TEST_F(HistogramUtilsTest, StringToNumberBruteForce) {
  constexpr auto max = 475'254ul;

  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_EQ(_convert_string_to_number_representation(_convert_number_representation_to_string(number)), number);
  }
}

}  // namespace opossum
