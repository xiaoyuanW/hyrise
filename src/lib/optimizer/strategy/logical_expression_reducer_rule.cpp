#include "logical_expression_reducer_rule.hpp"

#include <functional>
#include <unordered_set>

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

std::string LogicalExpressionReducerRule::name() const { return "Logical Expression Reducer Rule"; }

bool LogicalExpressionReducerRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto changed = false;

  MapType previously_reduced_expressions;

  changed |= _apply_to_node(node, previously_reduced_expressions);

  return changed;
}

bool LogicalExpressionReducerRule::_apply_to_node(const std::shared_ptr<AbstractLQPNode>& node,
                                                  MapType& previously_reduced_expressions) const {
  auto changed = false;

  // std::cout << "\tLooking at node " << node->description() << std::endl;

  // We only deal with predicates and projections, as these are the only LQP node types that handle complex expressions
  if (node->type == LQPNodeType::Predicate) {
    const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
    auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{predicate_node->predicate};

    changed |= _apply_to_expressions(expressions, previously_reduced_expressions);
    DebugAssert(expressions.size() == 1, "A PredicateNode should not have more than one top-level expression");

    // If we are working on a predicate, we can extract the elements from a top-level conjunctive chain and place them into their own predicate
    ExpressionUnorderedSet and_expressions;
    _collect_chained_logical_expressions(expressions[0], LogicalOperator::And, and_expressions);

    if (and_expressions.size() > 1) {
      // std::cout << "Found " << and_expressions.size() << " expressions" << std::endl;
      for(const auto& predicate : and_expressions) {
        // std::cout << "Extracting " << predicate->as_column_name() << std::endl;
        auto new_predicate_node = PredicateNode::make(predicate);
        // std::cout << "Created " << new_predicate_node->description() << std::endl;
        lqp_insert_node(predicate_node, LQPInputSide::Left, new_predicate_node);
      }
      lqp_remove_node(predicate_node);
    }

  } else if (node->type == LQPNodeType::Projection) {
    const auto& projection_node = std::static_pointer_cast<ProjectionNode>(node);
    changed |= _apply_to_expressions(projection_node->expressions, previously_reduced_expressions);
  }

  // We have to do _apply_to_inputs manually, because we want to pass a parameter
  if (node->left_input()) {
    changed |= _apply_to_node(node->left_input(), previously_reduced_expressions);
  }
  if (node->right_input()) {
    changed |= _apply_to_node(node->right_input(), previously_reduced_expressions);
  }

  return changed;
}

bool LogicalExpressionReducerRule::_apply_to_expressions(std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                                         MapType& previously_reduced_expressions) const {
  auto changed = false;

  for (auto& expression : expressions) {
    // std::cout << "\tLooking at expression " << expression->as_column_name() << std::endl;
    visit_expression(expression, [&](auto& subexpression) {
      // Step 0: Check if we already reduced this expression previously, if yes, reuse it
      auto reduced_expression_it = previously_reduced_expressions.find(subexpression);
      if (reduced_expression_it != previously_reduced_expressions.cend()) {
        subexpression = reduced_expression_it->second;
        // std::cout << "\tReused." << std::endl;
        return ExpressionVisitation::DoNotVisitArguments;
      }

      if (subexpression->type != ExpressionType::Logical) {
        return ExpressionVisitation::VisitArguments;
      }

      const auto& logical_expression = std::static_pointer_cast<LogicalExpression>(subexpression);
      if (logical_expression->logical_operator == LogicalOperator::Or) {
        // Step 1: Collect the outer chain. For the comments, we assume that the outer condition is OR and the inner
        // condition is AND: `(a AND b AND x) OR (c AND d AND x)`.
        ExpressionUnorderedSet or_expressions;
        _collect_chained_logical_expressions(subexpression, LogicalOperator::Or, or_expressions);

        // std::cout << "\t\tOr expressions: " << std::endl;
        // for (auto& subexpression : or_expressions) {
        //   std::cout << "\t\t\t- " << subexpression->as_column_name() << std::endl;
        // }

        ExpressionUnorderedSet common_and_expressions;
        {
          // Step 2: Fill common_and_expressions with the expressions found in the first outer expression. Then iterate over the others and intersect their expressions with common_and_expressions.

          auto or_expression_it = or_expressions.begin();
          _collect_chained_logical_expressions(*or_expression_it, LogicalOperator::And, common_and_expressions);
          ++or_expression_it;
          while (or_expression_it != or_expressions.end()) {
            // std::cout << "\t\tLooking in next OR: " << (*or_expression_it)->as_column_name() << std::endl;
            ExpressionUnorderedSet current_and_expressions;
            _collect_chained_logical_expressions(*or_expression_it, LogicalOperator::And, current_and_expressions);
            for (auto and_expression_it = common_and_expressions.begin();
                 and_expression_it != common_and_expressions.end();) {
              // std::cout << "\t\t\tTesting: " << (*and_expression_it)->as_column_name() << std::endl;
              if (!current_and_expressions.count(*and_expression_it)) {
                and_expression_it = common_and_expressions.erase(and_expression_it);
              } else {
                ++and_expression_it;
              }
            }
            ++or_expression_it;
          }

          // std::cout << "\t\tCommon expressions: " << std::endl;
          // for (const auto& subexpression : common_and_expressions) {
          //   std::cout << "\t\t\t- " << subexpression->as_column_name() << std::endl;
          // }

          // Step 3: If there are no common_and_expressions, we are done.
          if (common_and_expressions.empty()) {
            return ExpressionVisitation::DoNotVisitArguments;
          }
        }

        changed = true;

        {
          // Step 4: Rewrite the expression

          // Step 4.1: Rebuild the inner expressions, but without the common expressions
          auto or_expression_it = or_expressions.cbegin();
          auto new_chain = *or_expression_it;
          _remove_expressions_from_chain(new_chain, LogicalOperator::And, common_and_expressions);
          for (; or_expression_it != or_expressions.cend(); ++or_expression_it) {
            auto or_expression = *or_expression_it;  // We want a copy here, because we can't modify the set
            _remove_expressions_from_chain(or_expression, LogicalOperator::And, common_and_expressions);
            new_chain = or_(new_chain, or_expression);
          }

          // Step 4.2: Add the commond expressions to the outside
          for (const auto& subexpression : common_and_expressions) {
            new_chain = and_(subexpression, new_chain);
          }

          // std::cout << "New expression: " << new_chain->as_column_name() << std::endl;

          // Step 4.3: Store the result and replace it in the node that is to be optimized
          previously_reduced_expressions.emplace(subexpression, new_chain);
          subexpression = new_chain;
        }
      } else {
        // TODO
      }

      return ExpressionVisitation::DoNotVisitArguments;
    });
  }

  return changed;
}

// This is a helper method that puts all elements of the chain (a AND (b AND (c AND ...))) into result, `AND` being defined by logical_operator
void LogicalExpressionReducerRule::_collect_chained_logical_expressions(
    const std::shared_ptr<AbstractExpression>& expression, LogicalOperator logical_operator,
    ExpressionUnorderedSet& result) const {
  // TODO test these separately
  if (expression->type != ExpressionType::Logical) {
    // Not a logical expression, so for our purposes, we consider it "atomic"
    result.emplace(expression);
    return;
  }

  // Potentially continuing a chain of logical ANDs/ORs
  const auto& logical_expression = std::static_pointer_cast<LogicalExpression>(expression);
  if (logical_expression->logical_operator != logical_operator) {
    // A logical expression, but not of the right type
    result.emplace(expression);
    return;
  }

  _collect_chained_logical_expressions(logical_expression->left_operand(), logical_operator, result);
  _collect_chained_logical_expressions(logical_expression->right_operand(), logical_operator, result);
}

// This is a helper method that removes all elements in the chain (a AND (b AND (c AND ...))) that are contained in expressions_to_remove, `AND` being defined by logical_operator
void LogicalExpressionReducerRule::_remove_expressions_from_chain(
    std::shared_ptr<AbstractExpression>& chain, LogicalOperator logical_operator,
    const ExpressionUnorderedSet& expressions_to_remove) const {
  // TODO test these separately
  if (chain->type != ExpressionType::Logical) {
    // Not a logical expression, so for our purposes, we consider it "atomic"
    return;
  }

  // Potentially continuing a chain of logical ANDs/ORs
  const auto& logical_expression = std::static_pointer_cast<LogicalExpression>(chain);
  if (logical_expression->logical_operator != logical_operator) {
    // A logical expression, but not of the right type
    return;
  }

  // See if the left side is to be removed, if yes, remove the current expression from the chain and continue only with the right expression
  if (expressions_to_remove.count(logical_expression->left_operand())) {
    chain = logical_expression->right_operand();
    _remove_expressions_from_chain(chain, logical_operator, expressions_to_remove);
    return;
  }

  // See if the right side is to be removed, if yes, remove the current expression from the chain and continue only with the left expression
  if (expressions_to_remove.count(logical_expression->right_operand())) {
    chain = logical_expression->left_operand();
    _remove_expressions_from_chain(chain, logical_operator, expressions_to_remove);
    return;
  }

  // Nothing removed, recurse into both left and right
  _remove_expressions_from_chain(logical_expression->left_operand(), logical_operator, expressions_to_remove);
  _remove_expressions_from_chain(logical_expression->right_operand(), logical_operator, expressions_to_remove);
}

}  // namespace opossum