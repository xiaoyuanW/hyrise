#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class AggregateFunction { Min, Max, Sum, Avg, Count, CountDistinct };

class AggregateExpression : public AbstractExpression {
 public:
  // Constructor for COUNT(*)
  explicit AggregateExpression(const AggregateFunction aggregate_function);

  // Constructor for any other AggregateFunction
  AggregateExpression(const AggregateFunction aggregate_function, const std::shared_ptr<AbstractExpression>& argument);

  std::shared_ptr<AbstractExpression> argument() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  const AggregateFunction aggregate_function;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
