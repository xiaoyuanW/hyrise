#pragma once

#include <memory>

#include "abstract_cardinality_estimator.hpp"

namespace opossum {

class AbstractExpression;
template <typename T>
class AbstractHistogram;
class BaseSegmentStatistics;
class ChunkStatistics2;

class CardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
  std::shared_ptr<TableStatistics2> estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const override;

  template <typename T>
  static Cardinality estimate_cardinality_of_inner_equi_join_with_numeric_histograms(
      const std::shared_ptr<AbstractHistogram<T>>& histogram_left,
      const std::shared_ptr<AbstractHistogram<T>>& histogram_right);

  static std::shared_ptr<BaseSegmentStatistics> estimate_segment_statistics_for_expression(
    const AbstractExpression& expression,
    const ChunkStatistics2& chunk_statistics
  );
};

}  // namespace opossum

#include "cardinality_estimator.ipp"
