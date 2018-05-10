#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/lqp_column_expression.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

MockNode::MockNode(const ColumnDefinitions& column_definitions)
    : AbstractLQPNode(LQPNodeType::Mock), _constructor_arguments(column_definitions) {
}

//MockNode::MockNode(const std::shared_ptr<TableStatistics>& statistics, const std::optional<std::string>& alias)
//    : AbstractLQPNode(LQPNodeType::Mock), _constructor_arguments(statistics) {
//  set_statistics(statistics);

//  for (size_t column_statistics_idx = 0; column_statistics_idx < statistics->column_statistics().size();
//       ++column_statistics_idx) {
//    _output_column_names.emplace_back("MockCol" + std::to_string(column_statistics_idx));
//  }
//}

const boost::variant<MockNode::ColumnDefinitions, std::shared_ptr<TableStatistics>>& MockNode::constructor_arguments()
    const {
  return _constructor_arguments;
}

const std::vector<std::shared_ptr<AbstractExpression>>& MockNode::output_column_expressions() const {
  if (!_output_column_expressions) {
    _output_column_expressions.emplace();

    const auto& column_definitions = boost::get<ColumnDefinitions>(_constructor_arguments);

    for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
      const auto column_reference = LQPColumnReference(shared_from_this(), column_id);
      _output_column_expressions->emplace_back(std::make_shared<LQPColumnExpression>(column_reference));
    }
  }

  return *_output_column_expressions;
}

//std::string MockNode::description() const { return "[MockTable]"; }

std::shared_ptr<AbstractLQPNode> MockNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
//  if (_constructor_arguments.type() == typeid(std::shared_ptr<TableStatistics>)) {
//    return MockNode::make(boost::get<std::shared_ptr<TableStatistics>>(_constructor_arguments));
//  }
//
//  Assert(_constructor_arguments.type() == typeid(ColumnDefinitions), "Invalid constructor args state. Bug.");

  return MockNode::make(boost::get<ColumnDefinitions>(_constructor_arguments));
}

bool  MockNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);

  Assert(_constructor_arguments.type() != typeid(std::shared_ptr<TableStatistics>),
         "Comparison of statistics not implemented, because this is painful");
  return _constructor_arguments == mock_node._constructor_arguments;
}

}  // namespace opossum
