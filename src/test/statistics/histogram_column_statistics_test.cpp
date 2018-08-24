#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/histogram_column_statistics.hpp"

namespace opossum {

class HistogramColumnStatisticsTest : public BaseTest {
 protected:
  void SetUp() override {
    auto table_stats_int_float_double_string_gaps =
        generate_table_statistics(load_table("src/test/tables/int_float_double_string_gaps.tbl"), 3);
    _column_statistics_int = std::dynamic_pointer_cast<HistogramColumnStatistics<int32_t>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_stats_int_float_double_string_gaps.column_statistics()[0]));
    _column_statistics_float = std::dynamic_pointer_cast<HistogramColumnStatistics<float>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_stats_int_float_double_string_gaps.column_statistics()[1]));
    _column_statistics_double = std::dynamic_pointer_cast<HistogramColumnStatistics<double>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_stats_int_float_double_string_gaps.column_statistics()[2]));
    _column_statistics_string = std::dynamic_pointer_cast<HistogramColumnStatistics<std::string>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_stats_int_float_double_string_gaps.column_statistics()[3]));

    _int_equal_distribution = load_table("src/test/tables/int_equal_distribution.tbl");
    const auto table_stats_int_equal_distribution = generate_table_statistics(_int_equal_distribution, 18);
    _column_statistics_int_equal_distribution = table_stats_int_equal_distribution.column_statistics();
  }

  std::shared_ptr<Table> _int_equal_distribution;
  std::shared_ptr<HistogramColumnStatistics<int32_t>> _column_statistics_int;
  std::shared_ptr<HistogramColumnStatistics<float>> _column_statistics_float;
  std::shared_ptr<HistogramColumnStatistics<double>> _column_statistics_double;
  std::shared_ptr<HistogramColumnStatistics<std::string>> _column_statistics_string;
  std::vector<std::shared_ptr<const BaseColumnStatistics>> _column_statistics_int_equal_distribution;
};

TEST_F(HistogramColumnStatisticsTest, EqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{0}).selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{1}).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{3}).selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{4}).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{7}).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{8}).selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{1.f})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{3.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{4.f})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{7.f})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{8.f})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{1.})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{3.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{4.})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{7.})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{8.})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"a"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"b"})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"d"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"e"})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"h"})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"i"})
          .selectivity,
      0.f);
}

TEST_F(HistogramColumnStatisticsTest, NotEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{0})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{1})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{3})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{4})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{7})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{8})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{0.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{1.f})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{3.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{4.f})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{7.f})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{8.f})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{0.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{1.})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{3.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{4.})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{7.})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{8.})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"a"})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"b"})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"d"})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"e"})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"h"})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"i"})
          .selectivity,
      1.f);
}

TEST_F(HistogramColumnStatisticsTest, LessThanTest) {
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{0})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{1})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{3})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{4})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{7})
                      .selectivity,
                  5.f / 6.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{8})
                      .selectivity,
                  1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{1.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{3.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{7.f})
          .selectivity,
      2.f / 3.f + (1.f / 3.f) * (7.f - 6.f) / std::nextafter(7.f - 6.f, 7.f - 6.f + 1));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{8.f})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{1.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{3.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{4.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{7.})
          .selectivity,
      static_cast<float>(2. / 3. + (1. / 3.) * (7. - 6.) / std::nextafter(7. - 6., 7. - 6. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{8.})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"a"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"b"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"d"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"e"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"h"})
          .selectivity,
      static_cast<float>(2. / 3. + (1. / 3.) * (7. - 6.) / std::nextafter(7. - 6., 7. - 6. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"i"})
          .selectivity,
      1.f);
}

TEST_F(HistogramColumnStatisticsTest, LessThanEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{0})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{1})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{3})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{4})
          .selectivity,
      1.f / 2.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{7})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{8})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{1.f})
          .selectivity,
      1.f / 3.f * (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{3.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f + 1.f / 3.f * (std::nextafter(4.f, 4.f + 1) - 4.f) / std::nextafter(5.f - 4.f, 5.f - 4.f + 1));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{7.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{8.f})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{1.})
          .selectivity,
      static_cast<float>(1. / 3. * (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{3.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{4.})
          .selectivity,
      static_cast<float>(1. / 3. + 1. / 3. * (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{7.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{8.})
          .selectivity,
      1.f);

  // EXPECT_FLOAT_EQ(
  //     _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"a"})
  //         .selectivity,
  //     0.f);
  // EXPECT_FLOAT_EQ(
  //     _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"b"})
  //         .selectivity,
  //     static_cast<float>(1. / 3. * (437857484650781251ull - 437857484650781250ull) / (444491688963671875ull - 437857484650781250ull + 1ull)));
  // EXPECT_FLOAT_EQ(
  //     _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"d"})
  //         .selectivity,
  //     1.f / 3.f);
  // EXPECT_FLOAT_EQ(
  //     _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"e"})
  //         .selectivity,
  //     static_cast<float>(1. / 3. + 1. / 3. * (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1)));
  // EXPECT_FLOAT_EQ(
  //     _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"h"})
  //         .selectivity,
  //     1.f);
  // EXPECT_FLOAT_EQ(
  //     _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"i"})
  //         .selectivity,
  //     1.f);
}

TEST_F(HistogramColumnStatisticsTest, GreaterThanTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{0})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{1})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{3})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{4})
          .selectivity,
      3.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{7})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{8})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{0.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{1.f})
          .selectivity,
      2.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{3.f})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(4.f, 4.f + 1) - 4.f) / std::nextafter(5.f - 4.f, 5.f - 4.f + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{7.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{8.f})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{0.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{1.})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{3.})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{4.})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{7.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{8.})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"a"})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"b"})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"d"})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"e"})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"h"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"i"})
          .selectivity,
      0.f);
}

TEST_F(HistogramColumnStatisticsTest, GreaterThanEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{0})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{1})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{3})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{4})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{7})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{8})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{0.f})
                      .selectivity,
                  1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{1.f})
          .selectivity,
      2.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1)));
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{3.f})
                      .selectivity,
                  4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(4.f, 4.f + 1) - 4.f) / std::nextafter(5.f - 4.f, 5.f - 4.f + 1)));
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{7.f})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{8.f})
                      .selectivity,
                  0.f);

  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{0.})
                      .selectivity,
                  1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{1.})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{3.})
                      .selectivity,
                  4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{4.})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{7.})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{8.})
                      .selectivity,
                  0.f);

  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"a"})
                      .selectivity,
                  1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"b"})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"d"})
                      .selectivity,
                  4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"e"})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"h"})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"i"})
                      .selectivity,
                  0.f);
}

TEST_F(HistogramColumnStatisticsTest, BetweenTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{-1}, AllTypeVariant{0})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{-1}, AllTypeVariant{2})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_int
                      ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{1}, AllTypeVariant{2})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_int
                      ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{7})
                      .selectivity,
                  1.f);
  EXPECT_FLOAT_EQ(_column_statistics_int
                      ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{5}, AllTypeVariant{6})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_int
                      ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{5}, AllTypeVariant{9})
                      .selectivity,
                  1.f / 2.f);
  EXPECT_FLOAT_EQ(_column_statistics_int
                      ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{8}, AllTypeVariant{9})
                      .selectivity,
                  0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{-1.f}, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{-1.f}, AllTypeVariant{2.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{1.f}, AllTypeVariant{2.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{0.f}, AllTypeVariant{7.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{5.f}, AllTypeVariant{6.f})
          .selectivity,
      ((2.f + 2.f + 2.f * ((std::nextafter(6.f, 6.f + 1.f) - 6.f) / std::nextafter(7.f - 6.f, 7.f - 6.f + 1.f))) -
       (2.f + 2.f * (5.f - 4.f) / (std::nextafter(5.f - 4.f, 5.f - 4.f + 1)))) /
          6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{5.f}, AllTypeVariant{9.f})
          .selectivity,
      1.f - (1.f / 3.f + (1.f / 3.f) * (5.f - 4.f) / (std::nextafter(5.f - 4.f, 5.f - 4.f + 1.f))));
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{8.f}, AllTypeVariant{9.f})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{-1.}, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{-1.}, AllTypeVariant{2.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{1.}, AllTypeVariant{2.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{0.}, AllTypeVariant{7.})
          .selectivity,
      1.f);
  // Floating point is not precise enough to capture that this is slightly higher than 0.
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{5.}, AllTypeVariant{6.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{5.}, AllTypeVariant{9.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{8.}, AllTypeVariant{9.})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{""}, AllTypeVariant{"a"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{""}, AllTypeVariant{"c"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{"b"}, AllTypeVariant{"c"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{"a"}, AllTypeVariant{"h"})
          .selectivity,
      1.f);
  // Floating point is not precise enough to capture that this is slightly higher than 0.
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{"f"}, AllTypeVariant{"g"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{"f"}, AllTypeVariant{"j"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::Between, AllTypeVariant{"i"}, AllTypeVariant{"j"})
          .selectivity,
      0.f);
}

TEST_F(HistogramColumnStatisticsTest, StoredProcedureEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
}

TEST_F(HistogramColumnStatisticsTest, StoredProcedureNotEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
}

TEST_F(HistogramColumnStatisticsTest, StoredProcedureOpenEndedTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals)
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
}

TEST_F(HistogramColumnStatisticsTest, StoredProcedureBetweenTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{0})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{1})
          .selectivity,
      (1.f / 3.f) * (1.f / 6.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{3})
          .selectivity,
      (1.f / 3.f) * (1.f / 3.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{4})
          .selectivity,
      (1.f / 3.f) * (1.f / 2.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{7})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{8})
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{0.f})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{1.f})
          .selectivity,
      (1.f / 3.f) * (1.f / 3.f * (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1)));
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{3.f})
                      .selectivity,
                  (1.f / 3.f) * (1.f / 3.f));
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{4.f})
                      .selectivity,
                  (1.f / 3.f) * (1.f / 3.f + 1.f / 3.f * (std::nextafter(4.f, 4.f + 1) - 4.f) /
                                                 std::nextafter(5.f - 4.f, 5.f - 4.f + 1)));
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{7.f})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{8.f})
                      .selectivity,
                  1.f / 3.f);

  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{0.})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{1.})
                      .selectivity,
                  static_cast<float>((1. / 3.) * (1. / 3. * (std::nextafter(1., 1. + 1) - 1.) /
                                                  std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{3.})
                      .selectivity,
                  (1.f / 3.f) * (1.f / 3.f));
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{4.})
                      .selectivity,
                  static_cast<float>((1. / 3.) * (1. / 3. + 1. / 3. * (std::nextafter(4., 4. + 1) - 4.) /
                                                                std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{7.})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{8.})
                      .selectivity,
                  1.f / 3.f);

  // EXPECT_FLOAT_EQ(_column_statistics_string
  //                     ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{"a"})
  //                     .selectivity,
  //                 0.f);
  // EXPECT_FLOAT_EQ(_column_statistics_string
  //                     ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{"b"})
  //                     .selectivity,
  //                 static_cast<float>((1. / 3.) * (1. / 3. * (437857484650781251ull - 437857484650781250ull) / (444491688963671875ull - 437857484650781250ull + 1ull))));
  // EXPECT_FLOAT_EQ(_column_statistics_string
  //                     ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{"d"})
  //                     .selectivity,
  //                 (1.f / 3.f) * (1.f / 3.f));
  // EXPECT_FLOAT_EQ(_column_statistics_string
  //                     ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{"e"})
  //                     .selectivity,
  //                 static_cast<float>((1. / 3.) * 1. / 3. + 1. / 3. * (std::nextafter(4., 4. + 1) - 4.) /
  //                                                              std::nextafter(5. - 4., 5. - 4. + 1)));
  // EXPECT_FLOAT_EQ(_column_statistics_string
  //                     ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{"h"})
  //                     .selectivity,
  //                 1.f / 3.f);
  // EXPECT_FLOAT_EQ(_column_statistics_string
  //                     ->estimate_predicate_with_value_placeholder(PredicateCondition::Between, AllTypeVariant{"i"})
  //                     .selectivity,
  //                 1.f / 3.f);
}

TEST_F(HistogramColumnStatisticsTest, TwoColumnsEqualsTest) {
  const auto table_stats_int = generate_table_statistics(load_table("src/test/tables/int_int4.tbl"), 2);
  const auto col_stats_int_a = table_stats_int.column_statistics()[0];
  const auto col_stats_int_b = table_stats_int.column_statistics()[1];

  auto selectivity = ((7 + ((18 - 9) / (18 - 9 + 1.f)) * 4.f) / 11.f) / 9.f;
  EXPECT_FLOAT_EQ(
      col_stats_int_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_int_b).selectivity,
      selectivity);
  EXPECT_FLOAT_EQ(
      col_stats_int_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_int_a).selectivity,
      selectivity);

  const auto table_stats_float = generate_table_statistics(load_table("src/test/tables/float_float.tbl"), 2);
  const auto col_stats_float_a = table_stats_float.column_statistics()[0];
  const auto col_stats_float_b = table_stats_float.column_statistics()[1];

  selectivity =
      ((7 + ((std::nextafter(17.f, 17.f + 1.f) - 9.f) / std::nextafter(18.f - 9.f, 18.f - 9.f + 1.f)) * 4.f) / 11.f) /
      9.f;
  EXPECT_FLOAT_EQ(
      col_stats_float_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_float_b).selectivity,
      selectivity);
  EXPECT_FLOAT_EQ(
      col_stats_float_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_float_a).selectivity,
      selectivity);

  const auto table_stats_double = generate_table_statistics(load_table("src/test/tables/double_double.tbl"), 2);
  const auto col_stats_double_a = table_stats_double.column_statistics()[0];
  const auto col_stats_double_b = table_stats_double.column_statistics()[1];

  selectivity = static_cast<float>(
      ((7 + ((std::nextafter(17., 17. + 1.) - 9.) / std::nextafter(18. - 9., 18. - 9. + 1.)) * 4.) / 11.) / 9.f);
  EXPECT_FLOAT_EQ(
      col_stats_double_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_double_b).selectivity,
      selectivity);
  EXPECT_FLOAT_EQ(
      col_stats_double_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_double_a).selectivity,
      selectivity);

  const auto table_stats_string = generate_table_statistics(load_table("src/test/tables/string_string.tbl"), 2);
  const auto col_stats_string_a = table_stats_string.column_statistics()[0];
  const auto col_stats_string_b = table_stats_string.column_statistics()[1];

  selectivity = static_cast<float>(
      ((7 + ((std::nextafter(17., 17. + 1.) - 9.) / std::nextafter(18. - 9., 18. - 9. + 1.)) * 4.) / 11.) / 9.f);
  EXPECT_FLOAT_EQ(
      col_stats_string_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_string_b).selectivity,
      selectivity);
  EXPECT_FLOAT_EQ(
      col_stats_string_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_string_a).selectivity,
      selectivity);
}

TEST_F(HistogramColumnStatisticsTest, TwoColumnsEqualsNoOverlapTest) {
  const auto table_stats_int = generate_table_statistics(load_table("src/test/tables/int_int.tbl"), 2);
  const auto col_stats_int_a = table_stats_int.column_statistics()[0];
  const auto col_stats_int_b = table_stats_int.column_statistics()[1];

  EXPECT_FLOAT_EQ(
      col_stats_int_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_int_b).selectivity, 0.f);
  EXPECT_FLOAT_EQ(
      col_stats_int_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_int_a).selectivity, 0.f);

  const auto table_stats_float = generate_table_statistics(load_table("src/test/tables/float_float2.tbl"), 2);
  const auto col_stats_float_a = table_stats_float.column_statistics()[0];
  const auto col_stats_float_b = table_stats_float.column_statistics()[1];

  EXPECT_FLOAT_EQ(
      col_stats_float_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_float_b).selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      col_stats_float_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_float_a).selectivity,
      0.f);

  const auto table_stats_double = generate_table_statistics(load_table("src/test/tables/double_double2.tbl"), 2);
  const auto col_stats_double_a = table_stats_double.column_statistics()[0];
  const auto col_stats_double_b = table_stats_double.column_statistics()[1];

  EXPECT_FLOAT_EQ(
      col_stats_double_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_double_b).selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      col_stats_double_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_double_a).selectivity,
      0.f);

  const auto table_stats_string = generate_table_statistics(load_table("src/test/tables/string_string2.tbl"), 2);
  const auto col_stats_string_a = table_stats_string.column_statistics()[0];
  const auto col_stats_string_b = table_stats_string.column_statistics()[1];

  EXPECT_FLOAT_EQ(
      col_stats_string_a->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_string_b).selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      col_stats_string_b->estimate_predicate_with_column(PredicateCondition::Equals, col_stats_string_a).selectivity,
      0.f);
}

// TEST_F(HistogramColumnStatisticsTest, TwoColumnsLessThanTest) {
//   PredicateCondition predicate_condition = PredicateCondition::LessThan;
//
//   auto col_stat1 = std::make_shared<HistogramColumnStatistics<int>>(0.0f, 10.f, 1, 20);
//   auto col_stat2 = std::make_shared<HistogramColumnStatistics<int>>(0.0f, 30.f, 11, 40);
//
//   auto result1 = col_stat1->estimate_predicate_with_column(predicate_condition, col_stat2);
//   auto expected_selectivity = ((10.f / 20.f) * (10.f / 30.f) - 0.5f * 1.f / 30.f) * 0.5f + (10.f / 20.f) +
//                               (20.f / 30.f) - (10.f / 20.f) * (20.f / 30.f);
//   EXPECT_FLOAT_EQ(result1.selectivity, expected_selectivity);
//
//   auto result2 = col_stat2->estimate_predicate_with_column(predicate_condition, col_stat1);
//   expected_selectivity = ((10.f / 20.f) * (10.f / 30.f) - 0.5f * 1.f / 30.f) * 0.5f;
//   EXPECT_FLOAT_EQ(result2.selectivity, expected_selectivity);
//
//   auto col_stat3 = std::make_shared<HistogramColumnStatistics<float>>(0.0f, 6.f, 0, 10);
//   auto col_stat4 = std::make_shared<HistogramColumnStatistics<float>>(0.0f, 12.f, -10, 30);
//
//   auto result3 = col_stat3->estimate_predicate_with_column(predicate_condition, col_stat4);
//   expected_selectivity = ((10.f / 10.f) * (10.f / 40.f) - 1.f / (4 * 6)) * 0.5f + (20.f / 40.f);
//   EXPECT_FLOAT_EQ(result3.selectivity, expected_selectivity);
//
//   auto result4 = col_stat4->estimate_predicate_with_column(predicate_condition, col_stat3);
//   expected_selectivity = ((10.f / 10.f) * (10.f / 40.f) - 1.f / (4 * 6)) * 0.5f + (10.f / 40.f);
//   EXPECT_FLOAT_EQ(result4.selectivity, expected_selectivity);
// }

TEST_F(HistogramColumnStatisticsTest, TwoColumnsRealDataTest) {
  // Test selectivity calculations for all predicate conditions and column combinations of int_equal_distribution.tbl.
  // The estimated selectivities should be accurate because of the equal, continuous distribution of the data.
  std::vector<PredicateCondition> predicate_conditions{PredicateCondition::Equals, PredicateCondition::NotEquals,
                                                       PredicateCondition::LessThan, PredicateCondition::LessThanEquals,
                                                       PredicateCondition::GreaterThan};

  auto row_count = _int_equal_distribution->row_count();
  auto table_wrapper = std::make_shared<TableWrapper>(_int_equal_distribution);
  table_wrapper->execute();

  for (auto predicate_condition : predicate_conditions) {
    for (ColumnID::base_type column_1 = 0; column_1 < _column_statistics_int_equal_distribution.size(); column_1++) {
      for (ColumnID::base_type column_2 = 0;
           column_2 < _column_statistics_int_equal_distribution.size() && column_1 != column_2; column_2++) {
        auto result_container = _column_statistics_int_equal_distribution[column_1]->estimate_predicate_with_column(
            predicate_condition, _column_statistics_int_equal_distribution[column_2]);
        auto table_scan =
            std::make_shared<TableScan>(table_wrapper, ColumnID{column_1}, predicate_condition, ColumnID{column_2});
        table_scan->execute();
        auto result_row_count = table_scan->get_output()->row_count();
        EXPECT_FLOAT_EQ(result_container.selectivity, static_cast<float>(result_row_count) / row_count);
      }
    }
  }
}

// TEST_F(HistogramColumnStatisticsTest, NonNullRatioOneColumnTest) {
//   // null value ratio of 0 not tested here, since this is done in all other tests
//   _column_statistics_int->set_null_value_ratio(0.25f);   // non-null value ratio: 0.75
//   _column_statistics_float->set_null_value_ratio(0.5f);  // non-null value ratio: 0.5
//   _column_statistics_string->set_null_value_ratio(1.f);  // non-null value ratio: 0
//
//   auto predicate_condition = PredicateCondition::Equals;
//   auto result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(1));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(2.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f / 6.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("a"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::NotEquals;
//   result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(1));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 5.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(2.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 5.f / 6.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("a"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::LessThan;
//   result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 2.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 2.f / 5.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("c"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::GreaterThanEquals;
//   result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 4.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 3.f / 5.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("c"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::Between;
//   result =
//       _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(2), AllTypeVariant(4));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 3.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(4.f),
//                                                                    AllTypeVariant(6.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 2.f / 5.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("c"),
//                                                                     AllTypeVariant("d"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
// }
//
// TEST_F(HistogramColumnStatisticsTest, NonNullRatioTwoColumnTest) {
//   auto stats_0 =
//       std::const_pointer_cast<BaseColumnStatistics>(_column_statistics_int_equal_distribution[0]);  // values from 0 to 5
//   auto stats_1 =
//       std::const_pointer_cast<BaseColumnStatistics>(_column_statistics_int_equal_distribution[1]);  // values from 0 to 2
//   auto stats_2 =
//       std::const_pointer_cast<BaseColumnStatistics>(_column_statistics_int_equal_distribution[2]);  // values from 1 to 2
//
//   stats_0->set_null_value_ratio(0.1);   // non-null value ratio: 0.9
//   stats_1->set_null_value_ratio(0.2);   // non-null value ratio: 0.8
//   stats_2->set_null_value_ratio(0.15);  // non-null value ratio: 0.85
//
//   auto predicate_condition = PredicateCondition::Equals;
//   auto result = stats_0->estimate_predicate_with_column(predicate_condition, stats_1);
//   EXPECT_FLOAT_EQ(result.selectivity, 0.9f * 0.8f * 0.5f / 3.f);
//
//   predicate_condition = PredicateCondition::LessThan;
//   result = stats_1->estimate_predicate_with_column(predicate_condition, stats_2);
//   EXPECT_FLOAT_EQ(result.selectivity, 0.8f * 0.85f * (1.f / 3.f + 1.f / 3.f * 1.f / 2.f));
// }

TEST_F(HistogramColumnStatisticsTest, NonNullRatioTwoColumnTest) {
  auto stats_0 = std::const_pointer_cast<BaseColumnStatistics>(
      _column_statistics_int_equal_distribution[0]);  // values from 0 to 5
  auto stats_1 = std::const_pointer_cast<BaseColumnStatistics>(
      _column_statistics_int_equal_distribution[1]);  // values from 0 to 2
  auto stats_2 = std::const_pointer_cast<BaseColumnStatistics>(
      _column_statistics_int_equal_distribution[2]);  // values from 1 to 2

  stats_0->set_null_value_ratio(0.1);   // non-null value ratio: 0.9
  stats_1->set_null_value_ratio(0.2);   // non-null value ratio: 0.8
  stats_2->set_null_value_ratio(0.15);  // non-null value ratio: 0.85

  EXPECT_FLOAT_EQ(stats_0->estimate_predicate_with_column(PredicateCondition::Equals, stats_1).selectivity,
                  0.9f * 0.8f * 0.5f / 3.f);
  EXPECT_FLOAT_EQ(stats_1->estimate_predicate_with_column(PredicateCondition::LessThan, stats_2).selectivity,
                  0.8f * 0.85f * (1.f / 3.f + 1.f / 3.f * 1.f / 2.f));
}

}  // namespace opossum
