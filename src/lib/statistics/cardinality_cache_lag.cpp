#include "cardinality_cache_lag.hpp"

#include <cmath>

#include "utils/assert.hpp"

namespace opossum {


CardinalityCacheLAG::CardinalityCacheLAG(const size_t capacity):
  _capacity(capacity) {}

std::shared_ptr<CardinalityCacheEntry> CardinalityCacheLAG::get(const BaseJoinGraph &join_graph) {
  const auto normalized = join_graph.normalized();
  const auto iter = _map.find(normalized);
  if (iter == _map.end()) return nullptr;
  return iter->second;
}

std::optional<CardinalityCacheLAG::KeyValuePair> CardinalityCacheLAG::set(const BaseJoinGraph &join_graph, const std::shared_ptr<CardinalityCacheEntry>& entry) {
  Assert(_lag.size() == _map.size(), "CardinalityCacheLAG internal error 1");
  Assert(_lag.size() <= _capacity , "CardinalityCacheLAG internal error 2");

  const auto normalized = join_graph.normalized();

  Assert(_map.find(normalized) == _map.end(), "can't insert same elem twice");
  _map.emplace(normalized, entry);
  _lag.emplace(normalized, entry);
  Assert(_lag.size() == _map.size(), "CardinalityCacheLAG internal error 1");

  if (_lag.size() > _capacity) {
    const auto removal = *_lag.begin();
    _lag.erase(_lag.begin());
    const auto removed = _map.erase(removal.first);
    Assert(removed == 1, "");
    Assert(_lag.size() == _map.size(), "CardinalityCacheLAG internal error 1");

    return removal;
  }

  return std::nullopt;
}

void CardinalityCacheLAG::visit(const CardinalityCacheVisitor &visitor) const {
  for (const auto& pair : _lag) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheLAG::clear() {
  _lag.clear();
  _map.clear();
}

size_t CardinalityCacheLAG::size() const {
  Assert(_lag.size() == _map.size(), "CardinalityCacheLAG internal error 1");
  return _lag.size();
}

bool CardinalityCacheLAG::LAGCmp::operator()(const KeyValuePair& lhs, const KeyValuePair& rhs) const {
  Assert(lhs.second->estimated_cardinality && lhs.second->cardinality, "Can't cmp entries");
  Assert(rhs.second->estimated_cardinality && rhs.second->cardinality, "Can't cmp entries");

  const auto llag = std::fabs(*lhs.second->estimated_cardinality - *lhs.second->cardinality);
  const auto rlag = std::fabs(*rhs.second->estimated_cardinality - *rhs.second->cardinality);

  if (llag < rlag) return true;
  if (llag > rlag) return false;
  return std::hash<BaseJoinGraph>{}(lhs.first) < std::hash<BaseJoinGraph>{}(rhs.first);
}

}  // namespace opossum