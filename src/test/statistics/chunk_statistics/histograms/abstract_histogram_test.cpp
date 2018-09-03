#include "../../../base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

template <typename T>
class AbstractHistogramIntTest : public BaseTest {
  void SetUp() override { _int_float4 = load_table("src/test/tables/int_float4.tbl"); }

 protected:
  std::shared_ptr<Table> _int_float4;
};

using HistogramIntTypes =
    ::testing::Types<EqualNumElementsHistogram<int32_t>, EqualWidthHistogram<int32_t>, EqualHeightHistogram<int32_t>>;
TYPED_TEST_CASE(AbstractHistogramIntTest, HistogramIntTypes);

TYPED_TEST(AbstractHistogramIntTest, CanPruneLowerBound) {
  const auto hist = TypeParam::from_column(this->_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
}

TYPED_TEST(AbstractHistogramIntTest, CanPruneUpperBound) {
  const auto hist = TypeParam::from_column(this->_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));
}

TYPED_TEST(AbstractHistogramIntTest, CannotPruneExistingValue) {
  const auto hist = TypeParam::from_column(this->_int_float4->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 2u);
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
}

template <typename T>
class AbstractHistogramStringTest : public BaseTest {
  void SetUp() override {
    _string2 = load_table("src/test/tables/string2.tbl");
    _string3 = load_table("src/test/tables/string3.tbl");
  }

 protected:
  std::shared_ptr<Table> _string2;
  std::shared_ptr<Table> _string3;
};

using HistogramStringTypes = ::testing::Types<EqualNumElementsHistogram<std::string>, EqualWidthHistogram<std::string>,
                                              EqualHeightHistogram<std::string>>;
TYPED_TEST_CASE(AbstractHistogramStringTest, HistogramStringTypes);

TYPED_TEST(AbstractHistogramStringTest, StringConstructorTests) {
  // Histogram checks prefix length for overflow.
  EXPECT_NO_THROW(TypeParam::from_column(this->_string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u,
                                         "abcdefghijklmnopqrstuvwxyz", 13u));
  EXPECT_THROW(TypeParam::from_column(this->_string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxyz", 14u),
               std::exception);

  // Histogram rejects unsorted character ranges.
  EXPECT_THROW(TypeParam::from_column(this->_string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u,
                                      "zyxwvutsrqponmlkjihgfedcba", 13u),
               std::exception);

  // Histogram does not support non-consecutive supported characters.
  EXPECT_THROW(TypeParam::from_column(this->_string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u, "ac", 10u),
               std::exception);
}

TYPED_TEST(AbstractHistogramStringTest, GenerateHistogramUnsupportedCharacters) {
  // Generation should fail if we remove 'z' from the list of supported characters,
  // because it appears in the column.
  EXPECT_NO_THROW(TypeParam::from_column(this->_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u,
                                         "abcdefghijklmnopqrstuvwxyz", 4u));
  EXPECT_THROW(TypeParam::from_column(this->_string3->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxy", 4u),
               std::exception);
}

TYPED_TEST(AbstractHistogramStringTest, EstimateCardinalityUnsupportedCharacters) {
  auto hist = TypeParam::from_column(this->_string2->get_chunk(ChunkID{0})->get_column(ColumnID{0}), 4u,
                                     "abcdefghijklmnopqrstuvwxyz", 4u);

  EXPECT_NO_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abcd"));

  // Allow wildcards iff predicate is (NOT) LIKE.
  EXPECT_NO_THROW(hist->estimate_cardinality(PredicateCondition::Like, "abc_"));
  EXPECT_NO_THROW(hist->estimate_cardinality(PredicateCondition::NotLike, "abc%"));
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abc%"), std::exception);

  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abc1"), std::exception);
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "aBcd"), std::exception);
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "@abc"), std::exception);
}

}  // namespace opossum
