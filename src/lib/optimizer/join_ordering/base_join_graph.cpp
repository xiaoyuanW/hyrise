#include "base_join_graph.hpp"

#include <unordered_set>
#include <sstream>

#include "boost/functional/hash.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/assert.hpp"
#include "base_join_graph.hpp"
#include "join_plan_predicate.hpp"

namespace opossum {

BaseJoinGraph BaseJoinGraph::from_joined_graphs(const BaseJoinGraph& left, const BaseJoinGraph& right) {
  BaseJoinGraph joined_graph;
  joined_graph.vertices.insert(joined_graph.vertices.end(), left.vertices.begin(), left.vertices.end());
  joined_graph.vertices.insert(joined_graph.vertices.end(), right.vertices.begin(), right.vertices.end());
  joined_graph.predicates.insert(joined_graph.predicates.end(), left.predicates.begin(), left.predicates.end());
  joined_graph.predicates.insert(joined_graph.predicates.end(), right.predicates.begin(), right.predicates.end());
  return joined_graph;
}

BaseJoinGraph::BaseJoinGraph(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates):
  vertices(vertices), predicates(predicates) {}

std::shared_ptr<AbstractLQPNode> BaseJoinGraph::find_vertex(const LQPColumnReference& column_reference) const {
  for (const auto& vertex : vertices) {
    if (vertex->find_output_column_id(column_reference)) return vertex;
  }
  Fail("Couldn't find vertex");
}

std::string BaseJoinGraph::description() const {
  std::stringstream stream;
  stream << "[";
  for (const auto& vertex : vertices) {
    stream << vertex->description() << ";";
  }
  stream << "] [";
  for (const auto& predicate : predicates) {
    predicate->print(stream);
    stream << "; ";
  }
  stream << "] (";
  stream << std::hash<BaseJoinGraph>{}(*this) << ")";
  return stream.str();
}

nlohmann::json BaseJoinGraph::to_json() const {

  /**
   * Vertices to JSON
   */
  auto vertices_json = nlohmann::json::array();
  for (const auto& vertex : vertices) {
    Assert(vertex->type() == LQPNodeType::StoredTable, "to_json() only support StoredTableNodes right now");

    const auto stored_table_node = std::static_pointer_cast<StoredTableNode>(vertex);

    auto vertex_json = nlohmann::json{};
    vertex_json["table_name"] = stored_table_node->table_name();

    if (stored_table_node->alias()) {
      vertex_json["alias"] = *vertex->alias();
    }

    vertices_json.push_back(vertex_json);
  }

  /**
   * Predicates to JSON
   */
  auto predicates_json = nlohmann::json::array();
  for (const auto& predicate : predicates) {
    predicates_json.emplace_back(predicate->to_json());
  }

  auto json = nlohmann::json();

  json["vertices"] = vertices_json;
  json["predicates"] = predicates_json;

  return json;
}

BaseJoinGraph BaseJoinGraph::from_json(const nlohmann::json& json) {
  BaseJoinGraph base_join_graph;

  if (json.count("vertices")) {
    for (const auto &vertex_json : json["vertices"]) {
      const auto table_name = vertex_json["table_name"].get<std::string>();
      const auto stored_table_node = StoredTableNode::make(table_name);
      if (vertex_json.count("alias")) {
        stored_table_node->set_alias(vertex_json["alias"].get<std::string>());
      }

      base_join_graph.vertices.emplace_back(stored_table_node);
    }
  }

  if (json.count("predicates")) {
    for (const auto &predicate_json : json["predicates"]) {
      const auto predicate = join_plan_predicate_from_json(predicate_json, base_join_graph.vertices);
      base_join_graph.predicates.emplace_back(predicate);
    }
  }

  return base_join_graph;
}

bool BaseJoinGraph::operator==(const BaseJoinGraph& rhs) const {
  return std::hash<BaseJoinGraph>{}(*this) == std::hash<BaseJoinGraph>{}(rhs);
}

std::shared_ptr<const AbstractJoinPlanPredicate> BaseJoinGraph::normalize(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) {
  if (predicate->type() == JoinPlanPredicateType::Atomic) {
    const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
    if (is_lqp_column_reference(atomic_predicate->right_operand)) {
      const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

      if (std::hash<LQPColumnReference>{}(right_operand_column_reference) < std::hash<LQPColumnReference>{}(atomic_predicate->left_operand)) {
        if (atomic_predicate->predicate_condition != PredicateCondition::Like) {
          const auto flipped_predicate_condition = flip_predicate_condition(atomic_predicate->predicate_condition);
          return std::make_shared<JoinPlanAtomicPredicate>(right_operand_column_reference, flipped_predicate_condition, atomic_predicate->left_operand);
        }
      }
    }
  } else {
    const auto logical_predicate = std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);

    auto normalized_left = normalize(logical_predicate->left_operand);
    auto normalized_right = normalize(logical_predicate->right_operand);

    if (normalized_right->hash() < normalized_left->hash()) {
      std::swap(normalized_left, normalized_right);
    }

    return std::make_shared<JoinPlanLogicalPredicate>(normalized_left,
                                                      logical_predicate->logical_operator,
                                                      normalized_right);

  }

  return predicate;
}


BaseJoinGraph BaseJoinGraph::normalized() const {
  auto normalized_join_graph = *this;

  std::sort(normalized_join_graph.vertices.begin(), normalized_join_graph.vertices.end(), [](const auto& lhs, const auto& rhs) {
    return lhs->hash() < rhs->hash();
  });

  for (auto& predicate : normalized_join_graph.predicates) {
    predicate = normalize(predicate);
  }

  return normalized_join_graph;
}

}

namespace std {

using namespace opossum;  // NOLINT

size_t hash<opossum::BaseJoinGraph>::operator()(const opossum::BaseJoinGraph& join_graph) const {
  auto vertices = join_graph.vertices;
  std::sort(vertices.begin(), vertices.end(), [](const auto& lhs, const auto& rhs) {
    return lhs->hash() < rhs->hash();
  });

  auto predicates = join_graph.predicates;
  std::sort(predicates.begin(), predicates.end(), [](const auto& lhs, const auto& rhs) {
    return lhs->hash() < rhs->hash();
  });

  auto hash = boost::hash_value(vertices.size());
  boost::hash_combine(hash, predicates.size());

  for (const auto& vertex : vertices) {
    boost::hash_combine(hash, vertex->hash());
  }
  for (const auto& predicate : predicates) {
    boost::hash_combine(hash, predicate->hash());
  }

  return hash;
}

}