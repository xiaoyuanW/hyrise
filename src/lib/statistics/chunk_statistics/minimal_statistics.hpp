#pragma once

#include "statistics/abstract_statistics_object.hpp"

namespace opossum {

template<typename T>
class MinimalStatistics : public AbstractStatisticsObject {
 public:
  CardinalityEstimate estimate_cardinality(
  const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
  const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> slice_with_predicate(
  const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
  const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scale_with_selectivity(const Selectivity selectivity) const override;

  T minimum;
  T maximum;
  StatisticsObjectCountType distinct_count;
  StatisticsObjectCountType value_count;
};

}  // namespace opossum
