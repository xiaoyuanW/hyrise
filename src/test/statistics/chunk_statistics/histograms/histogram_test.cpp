#include "../../../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
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
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 2.5f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsUnevenBins) {
  auto hist =
      EqualNumElementsHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsFloat) {
  auto hist =
      EqualNumElementsHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0.4f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1.1f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.2f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f), 4 / 4.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f), 6 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.5f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.6f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{6.1f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f), 4 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{6.2f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsString) {
  auto hist =
      EqualNumElementsHistogram<std::string>::from_column(_string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "a"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "a"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "aa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "aa"), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "ab"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ab"), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "b"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "b"), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "birne"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "birne"), 3 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "biscuit"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "biscuit"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bla"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bla"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "blubb"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "blubb"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bums"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bums"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "ttt"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ttt"), 4 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "turkey"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "turkey"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uuu"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uuu"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "vvv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "vvv"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "www"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "www"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxx"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yyy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyy"), 4 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "zzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzz"), 4 / 2.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "zzzzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzzzzz"), 0.f);
}

TEST_F(HistogramTest, EqualNumElementsStringPruning) {
  /**
   * 4 bins
   *  [aa, b, birne]    -> [aa, bir]
   *  [bla, bums, ttt]  -> [bla, ttt]
   *  [uuu, www, xxx]   -> [uuu, xxx]
   *  [yyy, zzz]        -> [yyy, zzz]
   */
  auto hist = EqualNumElementsHistogram<std::string>::from_column(
      _string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 3u);

  // These values are smaller than values in bin 0.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, ""));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "a"));

  // These values fall within bin 0.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "aa"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "aaa"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "b"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bir"));

  // Even though these values are greater than the stored bin boundary (birne truncated to three characters),
  // these values are not prunable because their truncated substring is the same as the bin boundary.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bira"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "birne"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "birz"));

  // These values are between bin 0 and 1.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bis"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "biscuit"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bja"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bk"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bkz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bl"));

  // These values fall within bin 1.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bla"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "c"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "mmopasdasdasd"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "s"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "t"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "tt"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "ttt"));

  // Even though these values are greater than the stored bin boundary (ttt),
  // these values are not prunable because their truncated substring is the same as the bin boundary.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "tttu"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "tttzzzzz"));

  // These values are between bin 1 and 2.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "turkey"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "uut"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "uutzzzzz"));

  // These values fall within bin 2.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uuu"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uuuzzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uv"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uvz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "v"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "w"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "wzzzzzzzzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "x"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxw"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxx"));

  // Even though these values are greater than the stored bin boundary (xxx),
  // these values are not prunable because their truncated substring is the same as the bin boundary.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxxa"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxxzzzzzz"));

  // These values are between bin 2 and 3.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "xy"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "xyzz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "y"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "yyx"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "yyxzzzzz"));

  // These values fall within bin 3.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yyy"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yyyzzzzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "z"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "zzz"));

  // Even though these values are greater than the stored bin boundary (zzz),
  // these values are not prunable because their truncated substring is the same as the bin boundary.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "zzza"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "zzzzzzzzz"));
}

TEST_F(HistogramTest, EqualNumElementsLessThan) {
  auto hist =
      EqualNumElementsHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70), (70.f - 12) / (123 - 12 + 1) * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234), 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 7.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000), 7.f);
}

TEST_F(HistogramTest, EqualNumElementsFloatLessThan) {
  auto hist =
      EqualNumElementsHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f),
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f),
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, 2.2f - 0.5f + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(2.2f, 2.2f + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(2.2f, 2.2f + 1)), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{2.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, 3.3f - 2.5f + 1) * 6);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(3.3f, 3.3f + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(3.3f, 3.3f + 1)), 4.f + 6.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f), 4.f + 6.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f),
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, 6.1f - 3.6f + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(6.1f, 6.1f + 1)),
                  4.f + 6.f + 4.f);
}

TEST_F(HistogramTest, EqualNumElementsStringLessThan) {
  auto hist = EqualNumElementsHistogram<std::string>::from_column(
      _string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // "abcd"
  const auto bin_1_lower = 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           1 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           3 * (ipow(26, 0)) + 1;
  // "efgh"
  const auto bin_1_upper = 4 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           5 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 6 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           7 * (ipow(26, 0)) + 1;
  // "ijkl"
  const auto bin_2_lower = 8 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           11 * (ipow(26, 0)) + 1;
  // "mnop"
  const auto bin_2_upper = 12 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           13 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 14 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           15 * (ipow(26, 0)) + 1;
  // "oopp"
  const auto bin_3_lower = 14 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           14 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           15 * (ipow(26, 0)) + 1;
  // "qrst"
  const auto bin_3_upper = 16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           17 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           19 * (ipow(26, 0)) + 1;
  // "uvwx"
  const auto bin_4_lower = 20 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 22 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           23 * (ipow(26, 0)) + 1;
  // "yyzz"
  const auto bin_4_upper = 24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           24 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           25 * (ipow(26, 0)) + 1;

  const auto bin_1_width = (bin_1_upper - bin_1_lower + 1.f);
  const auto bin_2_width = (bin_2_upper - bin_2_lower + 1.f);
  const auto bin_3_width = (bin_3_upper - bin_3_lower + 1.f);
  const auto bin_4_width = (bin_4_upper - bin_4_lower + 1.f);

  constexpr auto bin_1_count = 4.f;
  constexpr auto bin_2_count = 6.f;
  constexpr auto bin_3_count = 3.f;
  constexpr auto bin_4_count = 3.f;
  constexpr auto total_count = bin_1_count + bin_2_count + bin_3_count + bin_4_count;

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "aaaa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abce"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce"), 1 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abcf"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf"), 2 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abcf"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "cccc"),
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "dddd"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "dddd"),
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgg"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg"),
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgh"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh"),
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgi"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi"), bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ijkl"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkl"), bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ijkm"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkm"),
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ijkn"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn"),
                  2 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "jjjj"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_2_count +
          bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkk"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "lzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz"),
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnoo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoo"),
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnop"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnop"),
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnoq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoq"), bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "oopp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopp"), bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "oopq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopq"),
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "oopr"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopr"),
                  2 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "pppp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp"),
                  (15 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qqqq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qllo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrss"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss"),
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrst"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst"),
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrsu"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu"),
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "uvwx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwx"),
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "uvwy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwy"),
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "uvwz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwz"),
                  2 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "vvvv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "xxxx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ycip"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
                  (bin_4_width - 2) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
                  (bin_4_width - 1) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz"), total_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "zzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(HistogramTest, EqualWidthHistogramBasic) {
  auto hist = EqualWidthHistogram<int32_t>::from_column(_int_int4->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 6u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, -1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 5 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 5 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17), 1 / 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramUnevenBins) {
  auto hist = EqualWidthHistogram<int32_t>::from_column(_int_int4->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 4u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, -1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4), 6 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 1 / 1.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15), 2 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17), 2 / 2.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramMoreBinsThanDistinctValuesIntEquals) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 10u);
  EXPECT_EQ(hist->num_bins(), 10u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 100));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 123));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 10'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10'000), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 12'345));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'345), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 12'356));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'356), 4 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 12'357));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'357), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 20'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20'000), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 50'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 50'000), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 100'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100'000), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 123'456));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 3 / 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 123'457));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'457), 0.f);
}

TEST_F(HistogramTest, EqualWidthHistogramMoreBinsThanDistinctValuesIntLessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 10u);
  EXPECT_EQ(hist->num_bins(), 10u);

  constexpr auto hist_min = 12;
  constexpr auto hist_max = 123'456;

  // First five bins are one element "wider", because the range of the column is not evenly divisible by 10.
  constexpr auto bin_width = (hist_max - hist_min + 1) / 10;
  constexpr auto bin_0_min = hist_min;
  constexpr auto bin_9_min = hist_min + 9 * bin_width + 5;

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 100));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100),
                  4.f * (100 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 123));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123),
                  4.f * (123 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 1'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000),
                  4.f * (1'000 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 10'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10'000),
                  4.f * (10'000 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 12'345));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'345),
                  4.f * (12'345 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 12'356));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'356),
                  4.f * (12'356 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 12'357));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'357), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 20'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 20'000), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 50'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 50'000), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 100'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100'000), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, bin_9_min));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, bin_9_min + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min + 1), 4.f + 3 * (1.f / bin_width));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 123'456));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456),
                  4.f + 3.f * (123'456 - bin_9_min) / bin_width);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 123'457));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 7.f);
}

TEST_F(HistogramTest, EqualWidthHistogramMoreBinsThanRepresentableValues) {
  auto hist = EqualWidthHistogram<int32_t>::from_column(_int_int4->get_chunk(ChunkID{0})->get_column(ColumnID{1}), 19u);
  // There must not be more bins than representable values in the column domain.
  EXPECT_EQ(hist->num_bins(), 17 - 0 + 1);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, -1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1), 0.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 1.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1), 3.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2), 1.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 3));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3), 0.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4), 1.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5), 0.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6), 0.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7), 1.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 8));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8), 0.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9), 0.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10), 1.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 0.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 0.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13), 1.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14), 1.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15), 0.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 16));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 16), 0.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17), 1.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18), 0.f);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 19));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 19), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0), 0.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1), 1.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2), 4.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 3));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3), 5.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4), 5.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5), 6.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 6), 6.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 7), 6.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 8));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 8), 7.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 9), 7.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10), 7.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 11), 8.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 8.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 13), 8.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 14), 9.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 15), 10.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 16));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 16), 10.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 17), 10.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 18), 11.f);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, 19));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 19), 11.f);
}

TEST_F(HistogramTest, EqualWidthFloat) {
  auto hist = EqualWidthHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 0.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 0.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 1.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.9f), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.0f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.0f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 2.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.1f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.2f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f), 7 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.4f), 3 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.6f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f), 3 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 3.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f), 3 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 4.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.4f), 3 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 4.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.5f), 3 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, 6.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f), 1 / 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, 6.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(HistogramTest, EqualWidthLessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_column(_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  // The first bin's range is one value wider (because (123'456 - 12 + 1) % 3 = 1).
  const auto bin_width = (123'456 - 12 + 1) / 3;

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70), (70.f - 12) / (bin_width + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234),
                  (1'234.f - 12) / (bin_width + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346),
                  (12'346.f - 12) / (bin_width + 1) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{80'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 80'000), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456),
                  4.f + (123'456.f - (12 + 2 * bin_width + 1)) / bin_width * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 4.f + 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000), 4.f + 3.f);
}

TEST_F(HistogramTest, EqualWidthFloatLessThan) {
  auto hist = EqualWidthHistogram<float>::from_column(_float2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 3u);

  const auto bin_width = std::nextafter(6.1f - 0.5f, 6.1f - 0.5f + 1) / 3;

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan,
                               AllTypeVariant{std::nextafter(0.5f + bin_width, 0.5f + bin_width + 1)}));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(0.5f + bin_width, 0.5f + bin_width + 1)),
      4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f), (1.0f - 0.5f) / bin_width * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f), (1.7f - 0.5f) / bin_width * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{2.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f),
                  4.f + (2.5f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  4.f + (3.0f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  4.f + (3.3f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f),
                  4.f + (3.6f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  4.f + (3.9f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan,
                               AllTypeVariant{std::nextafter(0.5f + 2 * bin_width, 0.5f + 2 * bin_width + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(0.5f + 2 * bin_width, 0.5f + 2 * bin_width + 1)),
                  4.f + 7.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{4.4f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4.4f),
                  4.f + 7.f + (4.4f - (0.5f + 2 * bin_width)) / bin_width * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f),
                  4.f + 7.f + (5.9f - (0.5f + 2 * bin_width)) / bin_width * 3);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{std::nextafter(6.1f, 6.1f + 1)}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, std::nextafter(6.1f, 6.1f + 1)),
                  4.f + 7.f + 3.f);
}

TEST_F(HistogramTest, EqualWidthStringLessThan) {
  auto hist = EqualWidthHistogram<std::string>::from_column(_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}),
                                                            4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // "abcd"
  const auto hist_lower = 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                          1 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                          3 * (ipow(26, 0)) + 1;

  // "yyzz"
  const auto hist_upper = 24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                          24 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                          25 * (ipow(26, 0)) + 1;

  const auto hist_width = hist_upper - hist_lower + 1;
  // Convert to float so that calculations further down are floating point divisions.
  // The division here, however, must be integral.
  const auto bin_width = static_cast<float>(hist_width / 4u);

  // hist_width % bin_width == 1, so there is one bin storing one additional value.
  const auto bin_1_width = bin_width + 1;
  const auto bin_2_width = bin_width;
  const auto bin_3_width = bin_width;
  const auto bin_4_width = bin_width;

  const auto bin_1_lower = hist_lower;
  const auto bin_2_lower = bin_1_lower + bin_1_width;
  const auto bin_3_lower = bin_2_lower + bin_2_width;
  const auto bin_4_lower = bin_3_lower + bin_3_width;

  constexpr auto bin_1_count = 4.f;
  constexpr auto bin_2_count = 5.f;
  constexpr auto bin_3_count = 4.f;
  constexpr auto bin_4_count = 3.f;
  constexpr auto total_count = bin_1_count + bin_2_count + bin_3_count + bin_4_count;

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "aaaa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abce"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce"), 1 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abcf"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf"), 2 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "cccc"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "cccc"),
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "dddd"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "dddd"),
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ghbo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbo"),
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ghbp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbp"),
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ghbq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbq"), bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ghbr"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbr"),
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ghbs"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbs"),
                  2 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "jjjj"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_2_count +
          bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkk"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "lzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz"),
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnaz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnaz"),
                  (bin_2_width - 3) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnb"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnb"),
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnba"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnba"),
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnbb"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbb"), bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnbc"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbc"),
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnbd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbd"),
                  2 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "pppp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp"),
                  (15 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qqqq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qllo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "stal"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stal"),
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "stam"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stam"),
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "stan"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stan"),
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "stao"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stao"),
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "stap"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stap"),
                  2 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "vvvv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "xxxx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ycip"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
                  (bin_4_width - 2) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
                  (bin_4_width - 1) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz"), total_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "zzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(HistogramTest, EqualHeightHistogramBasic) {
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

TEST_F(HistogramTest, EqualHeightHistogramUnevenBins) {
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

TEST_F(HistogramTest, EqualHeightFloat) {
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

TEST_F(HistogramTest, EqualHeightLessThan) {
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

TEST_F(HistogramTest, EqualHeightFloatLessThan) {
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

TEST_F(HistogramTest, EqualHeightStringLessThan) {
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

TEST_F(HistogramPrivateTest, NumberToString) {
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

TEST_F(HistogramPrivateTest, NumberToStringBruteForce) {
  constexpr auto max = 475'254ul;

  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_LT(_convert_number_representation_to_string(number), _convert_number_representation_to_string(number + 1));
  }
}

TEST_F(HistogramPrivateTest, StringToNumberBruteForce) {
  constexpr auto max = 475'254ul;

  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_EQ(_convert_string_to_number_representation(_convert_number_representation_to_string(number)), number);
  }
}

}  // namespace opossum
