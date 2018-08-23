#include "../../../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_helper.hpp"
#include "utils/load_table.hpp"

namespace opossum {

template <typename T>
class BasicHistogramTest : public BaseTest {
  void SetUp() override { _int_float4 = load_table("src/test/tables/int_float4.tbl"); }

 protected:
  std::shared_ptr<Table> _int_float4;
};

// TODO(tim): basic tests for float/double/int64
using HistogramTypes =
    ::testing::Types<EqualNumElementsHistogram<int32_t>, EqualWidthHistogram<int32_t>, EqualHeightHistogram<int32_t>>;
TYPED_TEST_CASE(BasicHistogramTest, HistogramTypes);

TYPED_TEST(BasicHistogramTest, CanPruneLowerBound) {
  const auto hist = TypeParam::from_column(this->_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
}

TYPED_TEST(BasicHistogramTest, CanPruneUpperBound) {
  const auto hist = TypeParam::from_column(this->_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));
}

TYPED_TEST(BasicHistogramTest, CannotPruneExistingValue) {
  const auto hist = TypeParam::from_column(this->_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
}

class HistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
    _float2 = load_table("src/test/tables/float2.tbl");
    _int_int4 = load_table("src/test/tables/int_int4.tbl");
    _expected_join_result_1 = load_table("src/test/tables/joinoperators/expected_join_result_1.tbl");
    _string2 = load_table("src/test/tables/string2.tbl");
    _string3 = load_table("src/test/tables/string3.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _int_int4;
  std::shared_ptr<Table> _expected_join_result_1;
  std::shared_ptr<Table> _string2;
  std::shared_ptr<Table> _string3;
};

TEST_F(HistogramTest, EqualNumElementsBasic) {
  const auto hist = EqualNumElementsHistogram<int32_t>::from_column(
      this->_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'234}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'456}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 2.5f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsUnevenBuckets) {
  auto hist =
      EqualNumElementsHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'234}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'456}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsFloat) {
  auto hist =
      EqualNumElementsHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.5f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsString) {
  auto hist =
      EqualNumElementsHistogram<std::string>::from_column(_string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "a"), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "aa"), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ab"), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "b"), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "birne"), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "biscuit"), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bla"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "blubb"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bums"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ttt"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "turkey"), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uuu"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "vvv"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "www"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxx"), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyy"), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzz"), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzzzzz"), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsLessThan) {
  auto hist =
      EqualNumElementsHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70), (70.f - 12) / (123 - 12 + 1) * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234), 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 7.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000), 7.f);
}

TEST_F(HistogramTest, EqualNumElementsFloatLessThan) {
  auto hist =
      EqualNumElementsHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(2.2f, 2.2f + 1)}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{2.5f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(3.3f, 3.3f + 1)}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}));

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f),
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f),
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(2.2f, 2.2f + 1)), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(3.3f, 3.3f + 1)), 4.f + 6.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f), 4.f + 6.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f),
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(6.1f, 6.1f + 1)),
                  4.f + 6.f + 4.f);
}

TEST_F(HistogramTest, EqualNumElementsStringLessThan) {
  auto hist = EqualNumElementsHistogram<std::string>::from_column(
      _string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // "abcd"
  constexpr auto bucket_1_lower = 26 * 26 + 2 * 26 + 3;
  // "efgh"
  constexpr auto bucket_1_upper = 4 * 26 * 26 * 26 + 5 * 26 * 26 + 6 * 26 + 7;
  // "ijkl"
  constexpr auto bucket_2_lower = 8 * 26 * 26 * 26 + 9 * 26 * 26 + 10 * 26 + 11;
  // "mnop"
  constexpr auto bucket_2_upper = 12 * 26 * 26 * 26 + 13 * 26 * 26 + 14 * 26 + 15;
  // "oopp"
  constexpr auto bucket_3_lower = 14 * 26 * 26 * 26 + 14 * 26 * 26 + 15 * 26 + 15;
  // "qrst"
  constexpr auto bucket_3_upper = 16 * 26 * 26 * 26 + 17 * 26 * 26 + 18 * 26 + 19;
  // "uvwx"
  constexpr auto bucket_4_lower = 20 * 26 * 26 * 26 + 21 * 26 * 26 + 22 * 26 + 23;
  // "yyzz"
  constexpr auto bucket_4_upper = 24 * 26 * 26 * 26 + 24 * 26 * 26 + 25 * 26 + 25;

  constexpr auto bucket_1_width = (bucket_1_upper - bucket_1_lower + 1.f);
  constexpr auto bucket_2_width = (bucket_2_upper - bucket_2_lower + 1.f);
  constexpr auto bucket_3_width = (bucket_3_upper - bucket_3_lower + 1.f);
  constexpr auto bucket_4_width = (bucket_4_upper - bucket_4_lower + 1.f);

  constexpr auto bucket_1_count = 4.f;
  constexpr auto bucket_2_count = 6.f;
  constexpr auto bucket_3_count = 3.f;
  constexpr auto bucket_4_count = 3.f;
  constexpr auto total_count = bucket_1_count + bucket_2_count + bucket_3_count + bucket_4_count;

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce"),
                  1 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf"),
                  2 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "cccc"),
                  (2 * 26 * 26 * 26 + 2 * 26 * 26 + 2 * 26 + 2 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "dddd"),
                  (3 * 26 * 26 * 26 + 3 * 26 * 26 + 3 * 26 + 3 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg"),
                  (bucket_1_width - 2) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh"),
                  (bucket_1_width - 1) / bucket_1_width * bucket_1_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi"), bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkl"), bucket_1_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkm"),
                  1 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn"),
                  2 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
                  (9 * 26 * 26 * 26 + 9 * 26 * 26 + 9 * 26 + 9 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (10 * 26 * 26 * 26 + 10 * 26 * 26 + 10 * 26 + 10 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz"),
                  (11 * 26 * 26 * 26 + 25 * 26 * 26 + 25 * 26 + 25 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoo"),
                  (bucket_2_width - 2) / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnop"),
                  (bucket_2_width - 1) / bucket_2_width * bucket_2_count + bucket_1_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoq"), bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopp"), bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopq"),
                  1 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopr"),
                  2 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp"),
                  (15 * 26 * 26 * 26 + 15 * 26 * 26 + 15 * 26 + 15 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * 26 * 26 * 26 + 16 * 26 * 26 + 16 * 26 + 16 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * 26 * 26 * 26 + 11 * 26 * 26 + 11 * 26 + 14 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss"),
                  (bucket_3_width - 2) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst"),
                  (bucket_3_width - 1) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu"),
                  bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwx"),
                  bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwy"),
                  1 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwz"),
                  2 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * 26 * 26 * 26 + 21 * 26 * 26 + 21 * 26 + 21 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * 26 * 26 * 26 + 23 * 26 * 26 + 23 * 26 + 23 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * 26 * 26 * 26 + 2 * 26 * 26 + 8 * 26 + 15 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
      (bucket_4_width - 2) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
      (bucket_4_width - 1) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yzaa"), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(HistogramTest, EqualWidthHistogramBasic) {
  auto hist = EqualWidthHistogram<int32_t>::from_column(_int_int4->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 6u);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 5 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramUnevenBuckets) {
  auto hist = EqualWidthHistogram<int32_t>::from_column(_int_int4->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 4u);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17), 2 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramMoreBucketsThanDistinctValuesIntEquals) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 10u);
  EXPECT_EQ(hist->num_buckets(), 10u);

  // constexpr auto bucket_width = (123456 - 12 + 1) / 10;
  // constexpr auto num_buckets_with_larger_range = (123456 - 12 + 1) % 10;

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10'000), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'345), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'356), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'357), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20'000), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 50'000), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100'000), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 3 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'457), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramMoreBucketsThanDistinctValuesIntLessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 10u);
  EXPECT_EQ(hist->num_buckets(), 10u);

  constexpr auto hist_min = 12;
  constexpr auto hist_max = 123'456;

  // First five buckets are one element "wider", because the range of the column is not evenly divisible by 10.
  constexpr auto bucket_width = (hist_max - hist_min + 1) / 10;
  constexpr auto bucket_0_min = hist_min;
  constexpr auto bucket_9_min = hist_min + 9 * bucket_width + 5;

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100),
                  4.f * (100 - bucket_0_min) / (bucket_width + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123),
                  4.f * (123 - bucket_0_min) / (bucket_width + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000),
                  4.f * (1'000 - bucket_0_min) / (bucket_width + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10'000),
                  4.f * (10'000 - bucket_0_min) / (bucket_width + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'345),
                  4.f * (12'345 - bucket_0_min) / (bucket_width + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'356),
                  4.f * (12'356 - bucket_0_min) / (bucket_width + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'357), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 20'000), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 50'000), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100'000), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bucket_9_min), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bucket_9_min + 1),
                  4.f + 3 * (1.f / bucket_width));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456),
                  4.f + 3.f * (123'456 - bucket_9_min) / bucket_width);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 7.f);
}

TEST_F(HistogramTest, EqualWidthHistogramMoreBucketsThanRepresentableValues) {
  auto hist = EqualWidthHistogram<int32_t>::from_column(_int_int4->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 19u);
  // There must not be more buckets than representable values in the column domain.
  EXPECT_EQ(hist->num_buckets(), 17 - 0 + 1);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 16), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 19), 0.f);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1), 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3), 5.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4), 5.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5), 6.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 6), 6.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 7), 6.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 8), 7.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 9), 7.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10), 7.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 11), 8.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 8.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 13), 8.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 14), 9.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 15), 10.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 16), 10.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 17), 10.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 18), 11.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 19), 11.f);
}

TEST_F(HistogramTest, EqualWidthFloat) {
  auto hist = EqualWidthHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.9f), 3 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.0f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.1f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.2f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f), 7 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.4f), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.4f), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.5f), 3 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f), 1 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(HistogramTest, EqualWidthLessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));

  // The first bucket's range is one value wider (because (123'456 - 12 + 1) % 3 = 1).
  const auto bucket_width = (123'456 - 12 + 1) / 3;

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70), (70.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234),
                  (1'234.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346),
                  (12'346.f - 12) / (bucket_width + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 80'000), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456),
                  4.f + (123'456.f - (12 + 2 * bucket_width + 1)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 4.f + 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000), 4.f + 3.f);
}

TEST_F(HistogramTest, EqualWidthFloatLessThan) {
  auto hist = EqualWidthHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  const auto bucket_width = std::nextafter(6.1f - 0.5f, 6.1f - 0.5f + 1) / 3;

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan,
                               AllTypeVariant{std::nextafter(0.5f + bucket_width, 0.5f + bucket_width + 1)}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{2.5f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan,
                               AllTypeVariant{std::nextafter(0.5f + 2 * bucket_width, 0.5f + 2 * bucket_width + 1)}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{4.4f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}));

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f), (1.0f - 0.5f) / bucket_width * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f), (1.7f - 0.5f) / bucket_width * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(0.5f + bucket_width, 0.5f + bucket_width + 1)),
                  4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f),
                  4.f + (2.5f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  4.f + (3.0f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  4.f + (3.3f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f),
                  4.f + (3.6f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  4.f + (3.9f - (0.5f + bucket_width)) / bucket_width * 7);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(0.5f + 2 * bucket_width, 0.5f + 2 * bucket_width + 1)),
                  4.f + 7.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4.4f),
                  4.f + 7.f + (4.4f - (0.5f + 2 * bucket_width)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f),
                  4.f + 7.f + (5.9f - (0.5f + 2 * bucket_width)) / bucket_width * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(6.1f, 6.1f + 1)),
                  4.f + 7.f + 3.f);
}

TEST_F(HistogramTest, EqualWidthStringLessThan) {
  auto hist = EqualWidthHistogram<std::string>::from_column(_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}),
                                                            4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // "abcd"
  constexpr auto bucket_1_lower = 26 * 26 + 2 * 26 + 3;
  // "ghbp"
  constexpr auto bucket_1_upper = 6 * 26 * 26 * 26 + 7 * 26 * 26 + 1 * 26 + 15;
  // "ghbq"
  constexpr auto bucket_2_lower = bucket_1_upper + 1;
  // "mnbb"
  constexpr auto bucket_2_upper = 12 * 26 * 26 * 26 + 13 * 26 * 26 + 1 * 26 + 1;
  // "mnbc"
  constexpr auto bucket_3_lower = bucket_2_upper + 1;
  // "stan"
  constexpr auto bucket_3_upper = 18 * 26 * 26 * 26 + 19 * 26 * 26 + 0 * 26 + 13;
  // "stao"
  constexpr auto bucket_4_lower = bucket_3_upper + 1;
  // "yyzz"
  constexpr auto bucket_4_upper = 24 * 26 * 26 * 26 + 24 * 26 * 26 + 25 * 26 + 25;

  constexpr auto bucket_1_width = (bucket_1_upper - bucket_1_lower + 1.f);
  constexpr auto bucket_2_width = (bucket_2_upper - bucket_2_lower + 1.f);
  constexpr auto bucket_3_width = (bucket_3_upper - bucket_3_lower + 1.f);
  constexpr auto bucket_4_width = (bucket_4_upper - bucket_4_lower + 1.f);

  constexpr auto bucket_1_count = 4.f;
  constexpr auto bucket_2_count = 5.f;
  constexpr auto bucket_3_count = 4.f;
  constexpr auto bucket_4_count = 3.f;
  constexpr auto total_count = bucket_1_count + bucket_2_count + bucket_3_count + bucket_4_count;

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce"),
                  1 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf"),
                  2 / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "cccc"),
                  (2 * 26 * 26 * 26 + 2 * 26 * 26 + 2 * 26 + 2 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "dddd"),
                  (3 * 26 * 26 * 26 + 3 * 26 * 26 + 3 * 26 + 3 - bucket_1_lower) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbo"),
                  (bucket_1_width - 2) / bucket_1_width * bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbp"),
                  (bucket_1_width - 1) / bucket_1_width * bucket_1_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbq"), bucket_1_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbr"),
                  1 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbs"),
                  2 / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
                  (9 * 26 * 26 * 26 + 9 * 26 * 26 + 9 * 26 + 9 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (10 * 26 * 26 * 26 + 10 * 26 * 26 + 10 * 26 + 10 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz"),
                  (11 * 26 * 26 * 26 + 25 * 26 * 26 + 25 * 26 + 25 - bucket_2_lower) / bucket_2_width * bucket_2_count +
                      bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnaz"),
                  (bucket_2_width - 3) / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnba"),
                  (bucket_2_width - 2) / bucket_2_width * bucket_2_count + bucket_1_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbb"),
                  (bucket_2_width - 1) / bucket_2_width * bucket_2_count + bucket_1_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbc"), bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbd"),
                  1 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbe"),
                  2 / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp"),
                  (15 * 26 * 26 * 26 + 15 * 26 * 26 + 15 * 26 + 15 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * 26 * 26 * 26 + 16 * 26 * 26 + 16 * 26 + 16 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * 26 * 26 * 26 + 11 * 26 * 26 + 11 * 26 + 14 - bucket_3_lower) / bucket_3_width * bucket_3_count +
                      bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stam"),
                  (bucket_3_width - 2) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stan"),
                  (bucket_3_width - 1) / bucket_3_width * bucket_3_count + bucket_1_count + bucket_2_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stao"),
                  bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stap"),
                  1 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "staq"),
                  2 / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * 26 * 26 * 26 + 21 * 26 * 26 + 21 * 26 + 21 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * 26 * 26 * 26 + 23 * 26 * 26 + 23 * 26 + 23 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * 26 * 26 * 26 + 2 * 26 * 26 + 8 * 26 + 15 - bucket_4_lower) / bucket_4_width * bucket_4_count +
                      bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
      (bucket_4_width - 2) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
      (bucket_4_width - 1) / bucket_4_width * bucket_4_count + bucket_1_count + bucket_2_count + bucket_3_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yzaa"), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(HistogramTest, EqualHeightHistogramBasic) {
  auto hist = EqualHeightHistogram<int32_t>::from_column(
      _expected_join_result_1->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 4u);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 6 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20), 6 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 21), 0.f);
}

TEST_F(HistogramTest, EqualHeightHistogramUnevenBuckets) {
  auto hist = EqualHeightHistogram<int32_t>::from_column(
      _expected_join_result_1->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 5u);

  // Even though we requested five buckets we will only get four because of the value distribution.
  // This has consequences for the cardinality estimation,
  // because the bucket count is now assumed to be 24 / 4 = 6, rather than 24 / 5 = 4.8 => 5.
  EXPECT_EQ(hist->num_buckets(), 4u);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 6 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 6 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 19), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20), 6 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 21), 0.f);
}

TEST_F(HistogramTest, EqualHeightFloat) {
  auto hist = EqualHeightHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f), 4 / 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.1f), 4 / 2.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.2f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.5f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.4f), 4 / 3.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.5f), 4 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f), 4 / 1.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(HistogramTest, EqualHeightLessThan) {
  auto hist =
      EqualHeightHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  // Even though we requested three buckets we will only get two because of the value distribution.
  // This has consequences for the cardinality estimation,
  // because the bucket count is now assumed to be 7 / 2 = 3.5 => 4, rather than 7 / 3 ~= 2.333 => 3.
  EXPECT_EQ(hist->num_buckets(), 2u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70), (70.f - 12) / (12'345 - 12 + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234),
                  (1'234.f - 12) / (12'345 - 12 + 1) * 4);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346), 4.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 80'000),
                  4.f + (80'000.f - 12'346) / (123'456 - 12'346 + 1) * 4);
  // Special case: cardinality is capped, see AbstractHistogram::estimate_cardinality().
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456), 7.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 7.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000), 7.f);
}

TEST_F(HistogramTest, EqualHeightFloatLessThan) {
  auto hist = EqualHeightHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{2.2f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(2.5f, 2.5f + 1)}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(3.3f, 3.3f + 1)}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(4.4f, 4.4f + 1)}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}));

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f), (1.0f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f), (1.7f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.2f), (2.2f - 0.5f) / (2.5f - 0.5f) * 5);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(2.5f, 2.5f + 1)), 5.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  5.f + (3.0f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  5.f + (3.3f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f),
                  5.f + (3.6f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  5.f + (3.9f - (std::nextafter(2.5f, 2.5f + 1))) / (4.4f - std::nextafter(2.5f, 2.5f + 1)) * 5);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(4.4f, 4.4f + 1)), 5.f + 5.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.1f),
                  5.f + 5.f + (5.1f - (std::nextafter(4.4f, 4.4f + 1))) / (6.1f - std::nextafter(4.4f, 4.4f + 1)) * 5);
  // Special case: cardinality is capped, see AbstractHistogram::estimate_cardinality().
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f), 14.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(6.1f, 6.1f + 1)), 14.f);
}

TEST_F(HistogramTest, EqualHeightStringLessThan) {
  auto hist = EqualHeightHistogram<std::string>::from_column(_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}),
                                                             4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // "abcd"
  constexpr auto bucket_1_lower = 26 * 26 + 2 * 26 + 3;
  // "efgh"
  constexpr auto bucket_1_upper = 4 * 26 * 26 * 26 + 5 * 26 * 26 + 6 * 26 + 7;
  // "efgi"
  constexpr auto bucket_2_lower = bucket_1_upper + 1;
  // "kkkk"
  constexpr auto bucket_2_upper = 10 * 26 * 26 * 26 + 10 * 26 * 26 + 10 * 26 + 10;
  // "kkkl"
  constexpr auto bucket_3_lower = bucket_2_upper + 1;
  // "qrst"
  constexpr auto bucket_3_upper = 16 * 26 * 26 * 26 + 17 * 26 * 26 + 18 * 26 + 19;
  // "qrsu"
  constexpr auto bucket_4_lower = bucket_3_upper + 1;
  // "yyzz"
  constexpr auto bucket_4_upper = 24 * 26 * 26 * 26 + 24 * 26 * 26 + 25 * 26 + 25;

  constexpr auto bucket_1_width = (bucket_1_upper - bucket_1_lower + 1.f);
  constexpr auto bucket_2_width = (bucket_2_upper - bucket_2_lower + 1.f);
  constexpr auto bucket_3_width = (bucket_3_upper - bucket_3_lower + 1.f);
  constexpr auto bucket_4_width = (bucket_4_upper - bucket_4_lower + 1.f);

  constexpr auto bucket_count = 4.f;
  constexpr auto total_count = 4 * bucket_count;

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce"), 1 / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf"), 2 / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "cccc"),
                  (2 * 26 * 26 * 26 + 2 * 26 * 26 + 2 * 26 + 2 - bucket_1_lower) / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "dddd"),
                  (3 * 26 * 26 * 26 + 3 * 26 * 26 + 3 * 26 + 3 - bucket_1_lower) / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg"),
                  (bucket_1_width - 2) / bucket_1_width * bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh"),
                  (bucket_1_width - 1) / bucket_1_width * bucket_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi"), bucket_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgj"),
                  1 / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgk"),
                  2 / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn"),
      (8 * 26 * 26 * 26 + 9 * 26 * 26 + 10 * 26 + 13 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
      (9 * 26 * 26 * 26 + 9 * 26 * 26 + 9 * 26 + 9 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jzzz"),
      (9 * 26 * 26 * 26 + 25 * 26 * 26 + 25 * 26 + 25 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kaab"),
                  (10 * 26 * 26 * 26 + 1 - bucket_2_lower) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkj"),
                  (bucket_2_width - 2) / bucket_2_width * bucket_count + bucket_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (bucket_2_width - 1) / bucket_2_width * bucket_count + bucket_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkl"), bucket_count * 2);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkm"),
                  1 / bucket_3_width * bucket_count + bucket_count * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkn"),
                  2 / bucket_3_width * bucket_count + bucket_count * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "loos"),
                  (11 * 26 * 26 * 26 + 14 * 26 * 26 + 14 * 26 + 18 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "nnnn"),
                  (13 * 26 * 26 * 26 + 13 * 26 * 26 + 13 * 26 + 13 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * 26 * 26 * 26 + 16 * 26 * 26 + 16 * 26 + 16 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * 26 * 26 * 26 + 11 * 26 * 26 + 11 * 26 + 14 - bucket_3_lower) / bucket_3_width * bucket_count +
                      bucket_count * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss"),
                  (bucket_3_width - 2) / bucket_3_width * bucket_count + bucket_count * 2);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst"),
                  (bucket_3_width - 1) / bucket_3_width * bucket_count + bucket_count * 2);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu"), bucket_count * 3);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsv"),
                  1 / bucket_4_width * bucket_count + bucket_count * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsw"),
                  2 / bucket_4_width * bucket_count + bucket_count * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "tdzr"),
                  (19 * 26 * 26 * 26 + 3 * 26 * 26 + 25 * 26 + 17 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * 26 * 26 * 26 + 21 * 26 * 26 + 21 * 26 + 21 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * 26 * 26 * 26 + 23 * 26 * 26 + 23 * 26 + 23 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * 26 * 26 * 26 + 2 * 26 * 26 + 8 * 26 + 15 - bucket_4_lower) / bucket_4_width * bucket_count +
                      bucket_count * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
                  (bucket_4_width - 2) / bucket_4_width * bucket_count + bucket_count * 3);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
                  (bucket_4_width - 1) / bucket_4_width * bucket_count + bucket_count * 3);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yzaa"), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(HistogramTest, StringConstructorTests) {
  EXPECT_NO_THROW(EqualNumElementsHistogram<std::string>(_string2, "abcdefghijklmnopqrstuvwxyz", 13u));
  EXPECT_THROW(EqualNumElementsHistogram<std::string>(_string2, "abcdefghijklmnopqrstuvwxyz", 14u), std::exception);

  auto hist = EqualNumElementsHistogram<std::string>(_string2, "zyxwvutsrqponmlkjihgfedcba");
  EXPECT_EQ(hist.supported_characters(), "abcdefghijklmnopqrstuvwxyz");

  EXPECT_THROW(EqualNumElementsHistogram<std::string>(_string2, "ac"), std::exception);
  EXPECT_THROW(EqualNumElementsHistogram<std::string>(_string2, "ac", 10), std::exception);
}

TEST_F(HistogramTest, GenerateHistogramUnsupportedCharacters) {
  // Generation should fail if we remove 'z' from the list of supported characters,
  // because it appears in the column.
  EXPECT_NO_THROW(EqualNumElementsHistogram<std::string>::from_column(
      _string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u));
  EXPECT_THROW(EqualNumElementsHistogram<std::string>::from_column(
                   _string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxy", 4u),
               std::exception);

  EXPECT_NO_THROW(EqualHeightHistogram<std::string>::from_column(
      _string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u));
  EXPECT_THROW(EqualHeightHistogram<std::string>::from_column(_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}),
                                                              4u, "abcdefghijklmnopqrstuvwxy", 4u),
               std::exception);

  EXPECT_NO_THROW(EqualWidthHistogram<std::string>::from_column(
      _string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u));
  EXPECT_THROW(EqualWidthHistogram<std::string>::from_column(_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}),
                                                             4u, "abcdefghijklmnopqrstuvwxy", 4u),
               std::exception);
}

TEST_F(HistogramTest, EstimateCardinalityUnsupportedCharacters) {
  auto hist = EqualNumElementsHistogram<std::string>::from_column(
      _string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  EXPECT_NO_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abcd"));
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abc1"), std::exception);
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "aBcd"), std::exception);
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "@abc"), std::exception);
}

class HistogramPrivateTest : public BaseTest {
  void SetUp() override {
    const auto _string2 = load_table("src/test/tables/string2.tbl");
    _hist = EqualNumElementsHistogram<std::string>::from_column(
        _string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u, "abcdefghijklmnopqrstuvwxyz", 4u);
  }

 protected:
  uint64_t _convert_string_to_number_representation(const std::string& value) {
    return _hist->_convert_string_to_number_representation(value);
  }

  std::string _convert_number_representation_to_string(const uint64_t value) {
    return _hist->_convert_number_representation_to_string(value);
  }

 protected:
  std::shared_ptr<EqualNumElementsHistogram<std::string>> _hist;
};

TEST_F(HistogramPrivateTest, PreviousValueString) {
  // Special case.
  EXPECT_EQ(_hist->get_previous_value(""), "");

  EXPECT_EQ(_hist->get_previous_value("a"), "");
  EXPECT_EQ(_hist->get_previous_value("b"), "azzz");
  EXPECT_EQ(_hist->get_previous_value("z"), "yzzz");
  EXPECT_EQ(_hist->get_previous_value("az"), "ayzz");
  EXPECT_EQ(_hist->get_previous_value("aaa"), "aa");
  EXPECT_EQ(_hist->get_previous_value("abcd"), "abcc");
  EXPECT_EQ(_hist->get_previous_value("abzz"), "abzy");
  EXPECT_EQ(_hist->get_previous_value("abca"), "abc");
  EXPECT_EQ(_hist->get_previous_value("abaa"), "aba");
  EXPECT_EQ(_hist->get_previous_value("aba"), "ab");
}

TEST_F(HistogramPrivateTest, NextValueString) {
  EXPECT_EQ(_hist->get_next_value(""), "a");
  EXPECT_EQ(_hist->get_next_value("a"), "aa");
  EXPECT_EQ(_hist->get_next_value("ayz"), "ayza");
  EXPECT_EQ(_hist->get_next_value("ayzz"), "az");
  EXPECT_EQ(_hist->get_next_value("azzz"), "b");
  EXPECT_EQ(_hist->get_next_value("z"), "za");
  EXPECT_EQ(_hist->get_next_value("df"), "dfa");
  EXPECT_EQ(_hist->get_next_value("abcd"), "abce");
  EXPECT_EQ(_hist->get_next_value("abaz"), "abb");
  EXPECT_EQ(_hist->get_next_value("abzz"), "ac");
  EXPECT_EQ(_hist->get_next_value("abca"), "abcb");
  EXPECT_EQ(_hist->get_next_value("abaa"), "abab");

  // Special case.
  EXPECT_EQ(_hist->get_next_value("zzzz"), "zzzza");
}

TEST_F(HistogramPrivateTest, StringToNumber) {
  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("a"), 1ul);
  EXPECT_EQ(_convert_string_to_number_representation("aa"), 2ul);
  EXPECT_EQ(_convert_string_to_number_representation("aaaa"), 4ul);
  EXPECT_EQ(_convert_string_to_number_representation("aaab"), 5ul);
  EXPECT_EQ(_convert_string_to_number_representation("azzz"), 18'279ul);
  EXPECT_EQ(_convert_string_to_number_representation("b"), 18'280ul);
  EXPECT_EQ(_convert_string_to_number_representation("ba"), 18'281ul);
  EXPECT_EQ(_convert_string_to_number_representation("bhja"), 23'447ul);
  EXPECT_EQ(_convert_string_to_number_representation("cde"), 38'778ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), 475'254ul);
}

TEST_F(HistogramPrivateTest, NumberToString) {
  EXPECT_EQ(_convert_number_representation_to_string(0ul), "");
  EXPECT_EQ(_convert_number_representation_to_string(1ul), "a");
  EXPECT_EQ(_convert_number_representation_to_string(2ul), "aa");
  EXPECT_EQ(_convert_number_representation_to_string(4ul), "aaaa");
  EXPECT_EQ(_convert_number_representation_to_string(5ul), "aaab");
  EXPECT_EQ(_convert_number_representation_to_string(18'279ul), "azzz");
  EXPECT_EQ(_convert_number_representation_to_string(18'280ul), "b");
  EXPECT_EQ(_convert_number_representation_to_string(18'281ul), "ba");
  EXPECT_EQ(_convert_number_representation_to_string(23'447ul), "bhja");
  EXPECT_EQ(_convert_number_representation_to_string(38'778ul), "cde");
  EXPECT_EQ(_convert_number_representation_to_string(475'254ul), "zzzz");
}

// TODO(tim): remove
TEST_F(HistogramTest, StringComparisonTest) {
  EXPECT_TRUE(std::string("abcd") < std::string("abce"));
  EXPECT_TRUE(std::string("abc") < std::string("abca"));

  EXPECT_TRUE(std::string("Z") < std::string("a"));
  EXPECT_FALSE(std::string("azaaaaaaa") < std::string("aza"));
  EXPECT_TRUE(std::string("aZaaaaaaa") < std::string("aza"));
}

}  // namespace opossum
