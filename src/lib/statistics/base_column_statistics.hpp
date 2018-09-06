#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class BaseColumnStatistics;

// Result of a cardinality estimation of filtering by value
struct FilterByValueEstimate final {
  float selectivity{0.0f};
  std::shared_ptr<BaseColumnStatistics> column_statistics;
};

// Result of a cardinality estimation of filtering by comparing two columns
struct FilterByColumnComparisonEstimate {
  float selectivity{0.0f};
  std::shared_ptr<BaseColumnStatistics> left_column_statistics;
  std::shared_ptr<BaseColumnStatistics> right_column_statistics;
};

class BaseColumnStatistics {
 public:
  BaseColumnStatistics(const DataType data_type, const float null_value_ratio);
  virtual ~BaseColumnStatistics() = default;

  /**
   * @defgroup Member access
   * @{
   */
  DataType data_type() const;
  float null_value_ratio() const;
  float non_null_value_ratio() const;

  void set_null_value_ratio(const float null_value_ratio);
  /** @} */

  /**
   * @return the distinct count
   */
  virtual float distinct_count() const = 0;

  virtual AllTypeVariant min() const = 0;
  virtual AllTypeVariant max() const = 0;

  /**
   * @return a clone of the concrete ColumnStatistics object
   */
  virtual std::shared_ptr<BaseColumnStatistics> clone() const = 0;

  /**
   * @return a clone() of this, with the null_value_ratio set to 0
   */
  std::shared_ptr<BaseColumnStatistics> without_null_values() const;

  virtual float estimate_range_selectivity(const AllTypeVariant& variant_minimum,
                                           const AllTypeVariant& variant_maximum) const = 0;

  virtual FilterByValueEstimate estimate_range(const AllTypeVariant& variant_minimum,
                                               const AllTypeVariant& variant_maximum) const = 0;

  virtual float estimate_distinct_count(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                        const std::optional<AllTypeVariant>& variant_value2) const = 0;

  /**
   * @return a clone() of this, with the null_value_ratio set to 1
   */
  std::shared_ptr<BaseColumnStatistics> only_null_values() const;

  /**
   * @defgroup Cardinality estimation
   * @{
   */
  /**
   * Estimate a Column-Value Predicate, e.g. "a > 5"
   */
  virtual FilterByValueEstimate estimate_predicate_with_value(
      const PredicateCondition predicate_condition, const AllTypeVariant& value,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate a Column-ValuePlaceholder Predicate, e.g. "a > ?"
   * Since the value of the ValuePlaceholder (naturally) isn't known, has to resort to magic values.
   */
  virtual FilterByValueEstimate estimate_predicate_with_value_placeholder(
      const PredicateCondition predicate_condition,
      const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate a Column-Column Predicate, e.g. "a > b"
   */
  virtual FilterByColumnComparisonEstimate estimate_predicate_with_column(
      const PredicateCondition predicate_condition,
      const std::shared_ptr<const BaseColumnStatistics>& base_right_column_statistics) const = 0;
  /** @} */

  std::string description() const;

 protected:
  virtual std::string _description() const = 0;

 protected:
  const DataType _data_type;
  float _null_value_ratio;
};

}  // namespace opossum
