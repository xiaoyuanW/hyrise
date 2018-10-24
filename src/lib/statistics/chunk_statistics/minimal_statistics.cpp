#include "minimal_statistics.hpp"

namespace opossum {

template<typename T>
CardinalityEstimate MinimalStatistics<T>::estimate_cardinality(
const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
const std::optional<AllTypeVariant>& variant_value2) const {

  switch(predicate_type) {
    case PredicateCondition::Equals:

  }

}

template<typename T>
std::shared_ptr<AbstractStatisticsObject> MinimalStatistics<T>::slice_with_predicate(
const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
const std::optional<AllTypeVariant>& variant_value2) const {

}

template<typename T>
std::shared_ptr<AbstractStatisticsObject> MinimalStatistics<T>::scale_with_selectivity(const Selectivity selectivity) const {

}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MinimalStatistics);

}  // namespace opossum