#include "../../../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class EqualHeightHistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
    _float2 = load_table("src/test/tables/float2.tbl");
    _expected_join_result_1 = load_table("src/test/tables/joinoperators/expected_join_result_1.tbl");
    _string3 = load_table("src/test/tables/string3.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _expected_join_result_1;
  std::shared_ptr<Table> _string3;
};

TEST_F(EqualHeightHistogramTest, Basic) {
  auto hist = EqualHeightHistogram<int32_t>::from_column(
      _expected_join_result_1->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 4u);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 6 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 6 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 6 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 6 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 8));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 20));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20), 6 / 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 21));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 21), 0.f);
}

TEST_F(EqualHeightHistogramTest, UnevenBins) {
  auto hist = EqualHeightHistogram<int32_t>::from_column(
      _expected_join_result_1->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 5u);

  // Even though we requested five bins we will only get four because of the value distribution.
  // This has consequences for the cardinality estimation,
  // because the bin count is now assumed to be 24 / 4 = 6, rather than 24 / 5 = 4.8 => 5.
  EXPECT_EQ(hist->num_bins(), 4u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 6 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 8));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 6 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 19));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 19), 6 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 20));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20), 6 / 2.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 21));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 21), 0.f);
}

TEST_F(EqualHeightHistogramTest, Float) {
  auto hist = EqualHeightHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 0.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 0.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f), 4 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f), 4 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f), 4 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.1f), 4 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.2f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.5f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.6f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 4.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.4f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 4.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.5f), 4 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 6.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f), 4 / 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 6.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(EqualHeightHistogramTest, LessThan) {
  auto hist =
      EqualHeightHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  // Even though we requested three bins we will only get two because of the value distribution.
  // This has consequences for the cardinality estimation,
  // because the bin count is now assumed to be 7 / 2 = 3.5 => 4, rather than 7 / 3 ~= 2.333 => 3.
  EXPECT_EQ(hist->num_bins(), 2u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70), (70.f - 12) / (12'345 - 12 + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234),
                  (1'234.f - 12) / (12'345 - 12 + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{80'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 80'000),
                  4.f + (80'000.f - 12'346) / (123'456 - 12'346 + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  // Special case: cardinality is capped, see AbstractHistogram::estimate_cardinality().
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456), 7.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 7.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000), 7.f);
}

TEST_F(EqualHeightHistogramTest, FloatLessThan) {
  auto hist = EqualHeightHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f), (1.0f - 0.5f) / (2.5f - 0.5f) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f), (1.7f - 0.5f) / (2.5f - 0.5f) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{2.2f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.2f), (2.2f - 0.5f) / (2.5f - 0.5f) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(2.5f, 2.5f + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(2.5f, 2.5f + 1)), 5.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  5.f + (3.0f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  5.f + (3.3f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f),
                  5.f + (3.6f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  5.f + (3.9f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(4.4f, 4.4f + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(4.4f, 4.4f + 1)), 5.f + 5.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.1f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.1f),
                  5.f + 5.f + (5.1f - (std::nextafter(4.4f, 4.4f + 1))) / (6.1f - std::nextafter(4.4f, 4.4f + 1)) * 5);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  // Special case: cardinality is capped, see AbstractHistogram::estimate_cardinality().
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f), 14.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(6.1f, 6.1f + 1)), 14.f);
}

TEST_F(EqualHeightHistogramTest, StringLessThan) {
  auto hist = EqualHeightHistogram<std::string>::from_column(_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}),
                                                             4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // "abcd"
  const auto bin_1_lower = 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           1 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           3 * (ipow(26, 0)) + 1;
  // "efgh"
  const auto bin_1_upper = 4 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           5 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 6 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           7 * (ipow(26, 0)) + 1;
  // "efgi"
  const auto bin_2_lower = bin_1_upper + 1;
  // "kkkk"
  const auto bin_2_upper = 10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           10 * (ipow(26, 0)) + 1;
  // "kkkl"
  const auto bin_3_lower = bin_2_upper + 1;
  // "qrst"
  const auto bin_3_upper = 16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           17 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           19 * (ipow(26, 0)) + 1;
  // "qrsu"
  const auto bin_4_lower = bin_3_upper + 1;
  // "yyzz"
  const auto bin_4_upper = 24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           24 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           25 * (ipow(26, 0)) + 1;

  const auto bin_1_width = (bin_1_upper - bin_1_lower + 1.f);
  const auto bin_2_width = (bin_2_upper - bin_2_lower + 1.f);
  const auto bin_3_width = (bin_3_upper - bin_3_lower + 1.f);
  const auto bin_4_width = (bin_4_upper - bin_4_lower + 1.f);

  constexpr auto bin_count = 4.f;
  constexpr auto total_count = 4 * bin_count;

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "aaaa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abce"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce"), 1 / bin_1_width * bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abcf"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf"), 2 / bin_1_width * bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "cccc"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "cccc"),
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "dddd"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "dddd"),
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgg"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg"),
                  (bin_1_width - 2) / bin_1_width * bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgh"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh"),
                  (bin_1_width - 1) / bin_1_width * bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgi"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi"), bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgj"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgj"),
                  1 / bin_2_width * bin_count + bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgk"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgk"),
                  2 / bin_2_width * bin_count + bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ijkn"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn"),
      (8 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 + 13 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_count +
          bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "jjjj"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_count +
          bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "jzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "jzzz"),
                  (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_count +
                      bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kaab"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kaab"),
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   1 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_count +
                      bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkj"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkj"),
                  (bin_2_width - 2) / bin_2_width * bin_count + bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkk"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (bin_2_width - 1) / bin_2_width * bin_count + bin_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkl"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkl"), bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkm"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkm"),
                  1 / bin_3_width * bin_count + bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkn"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkn"),
                  2 / bin_3_width * bin_count + bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "loos"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "loos"),
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 14 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   18 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "nnnn"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "nnnn"),
                  (13 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   13 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 13 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   13 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qllo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qqqq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrss"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss"),
                  (bin_3_width - 2) / bin_3_width * bin_count + bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrst"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst"),
                  (bin_3_width - 1) / bin_3_width * bin_count + bin_count * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrsu"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu"), bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrsv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsv"),
                  1 / bin_4_width * bin_count + bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrsw"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsw"),
                  2 / bin_4_width * bin_count + bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "tdzr"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "tdzr"),
                  (19 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   17 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "vvvv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "xxxx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ycip"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
                  (bin_4_width - 2) / bin_4_width * bin_count + bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
                  (bin_4_width - 1) / bin_4_width * bin_count + bin_count * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz"), total_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "zzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(EqualHeightHistogramTest, StringLikePrefix) {
  auto hist = EqualHeightHistogram<std::string>::from_column(_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}),
                                                             4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // First bin: [abcd, efgh], so everything before is prunable.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "a"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "aa%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%"), 0.f);

  // Complexity of prefix pattern does not matter for pruning decision.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "aa%zz%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%zz%"), 0.f);

  // Even though "aa%" is prunable, "a%" is not!
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "a%"));
  // Since there are no values smaller than "abcd", [abcd, azzz] is the range that "a%" covers.
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "abcd", "azzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "a", "azzz"));

  // No wildcard, no party.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "abcd"),
                  hist->estimate_cardinality(PredicateCondition::Equals, "abcd"));

  // Classic cases for prefix search.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "ab%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ab%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "ab", "abzz"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "c%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "c%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "c", "czzz"));

  // If the search prefix is longer than the prefix_length the search prefix will be trimmed and used as a "range".
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "cfoobar%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "cfoobar%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "cfoo", "cfoo"));

  // There are values matching "e%" in two bins, make sure both are included.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "e%"));
  EXPECT_GT(hist->estimate_cardinality(PredicateCondition::Like, "e%"),
            hist->estimate_cardinality(PredicateCondition::Between, "e", "efgh"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "e", "ezzz"));

  // Use upper bin boundary as range limit, since there are no other values starting with y in other bins.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "y%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "y", "yyzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%"),
                  hist->estimate_cardinality(PredicateCondition::Between, "y", "yzzz"));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "z%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "z%"), 0.f);
}

}  // namespace opossum
