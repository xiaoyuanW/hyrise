#include "cardinality_cache.hpp"

#include <algorithm>
#include <chrono>
#include <experimental/filesystem>
#include <iostream>
#include <fstream>
#include <thread>

#include "boost/interprocess/sync/file_lock.hpp"
#include "boost/interprocess/sync/sharable_lock.hpp"
#include "boost/interprocess/sync/scoped_lock.hpp"

#include "json.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "cardinality_cache_uncapped.hpp"
#include "utils/assert.hpp"

namespace opossum {

CardinalityCache::CardinalityCache(const std::shared_ptr<AbstractEngagedCardinalityCache>& engaged_cache):
  _engaged_cache(engaged_cache) {}

std::optional<Cardinality> CardinalityCache::get_cardinality(const BaseJoinGraph& join_graph) {
  const auto normalized = join_graph.normalized();
  auto entry = _get_entry(normalized);

  if (_log) (*_log) << "CardinalityCache [" << (entry->request_count == 0 ? "I" : "S") << "]";

  ++entry->request_count;

  std::optional<Cardinality> result;

  if (entry->cardinality) {
    if (_log) (*_log) << "[HIT ]: ";
    ++_hit_count;
    result = *entry->cardinality;
  } else {
    if (_log) (*_log) << "[MISS]: ";
    ++_miss_count;
  }

  if (_log) (*_log) << normalized.description();

  if (entry->cardinality) {
    if (_log) (*_log) << ": " << *entry->cardinality;
  }

  if (_log) (*_log) << std::endl;

  return result;
}

void CardinalityCache::set_cardinality(const BaseJoinGraph& join_graph, const Cardinality cardinality, const Cardinality cardinality_estimation) {
  const auto normalized_join_graph = join_graph.normalized();
  auto entry = _engaged_cache->get(normalized_join_graph);



  if (entry) {
    Assert(entry->cardinality && entry->cardinality == cardinality, "Can't change cardinality");
  } else {
    const auto iter = _disengaged_cache.find(normalized_join_graph);
    if (iter != _disengaged_cache.end()) {
      entry = iter->second;
      _disengaged_cache.erase(iter);
    } else {
      entry = std::make_shared<CardinalityCacheEntry>();
    }

    entry->cardinality = cardinality;
    entry->estimated_cardinality = cardinality_estimation;

    const auto disengaged = _engaged_cache->set(normalized_join_graph, entry);
    if (disengaged) {
      disengaged->second->cardinality.reset();
      disengaged->second->estimated_cardinality.reset();
      const auto emplaced = _disengaged_cache.emplace(disengaged->first, disengaged->second).second;
      Assert(emplaced, "Entry was already disengaged");

    }

    if (_log) {
      if (std::hash<BaseJoinGraph>{}(normalized_join_graph) == 4984748667107325363) {
        std::cout << "Here we are" << std::endl;
      }

      if (disengaged && disengaged->first == normalized_join_graph) {
        (*_log) << "CardinalityCache [" << (entry->request_count == 0 ? "I" : "S") << "][REJC]: " << normalized_join_graph.description() << ": " << cardinality << " / " << cardinality_estimation << std::endl;
      } else {
        (*_log) << "CardinalityCache [" << (entry->request_count == 0 ? "I" : "S") << "][PUT ]: "
                << normalized_join_graph.description() << ": " << cardinality << " / " << cardinality_estimation << std::endl;
      }

      if (disengaged && !(disengaged->first == normalized_join_graph)) {
        (*_log) << "CardinalityCache [" << (entry->request_count == 0 ? "I" : "S") << "][DROP]: " << disengaged->first.description() << ": " << *disengaged->second->cardinality << " / " << *disengaged->second->estimated_cardinality << std::endl;
      }
    }
  }
}

std::optional<std::chrono::seconds> CardinalityCache::get_timeout(const BaseJoinGraph& join_graph) {
  return _get_entry(join_graph)->timeout;
}

void CardinalityCache::set_timeout(const BaseJoinGraph& join_graph, const std::optional<std::chrono::seconds>& timeout) {
  _get_entry(join_graph)->timeout = timeout;
}

std::shared_ptr<CardinalityCacheEntry> CardinalityCache::_get_entry(const BaseJoinGraph &join_graph) {
  auto entry = _engaged_cache->get(join_graph);

  if (entry) {
    Assert(entry->cardinality, "Cache entry has no associated cardinality");
    return entry;
  }

  const auto normalized_join_graph = join_graph.normalized();

  const auto iter = _disengaged_cache.find(normalized_join_graph);
  if (iter != _disengaged_cache.end()) {
    entry = iter->second;
  } else {
    entry = std::make_shared<CardinalityCacheEntry>();
    _disengaged_cache.emplace(normalized_join_graph, entry);
  }

  Assert(!entry->cardinality && !entry->estimated_cardinality, "Disengaged entry contains value");

  return entry;
}

size_t CardinalityCache::cache_hit_count() const {
  return _hit_count;
}

size_t CardinalityCache::cache_miss_count() const {
  return _miss_count;
}

size_t CardinalityCache::distinct_hit_count() const {
  auto count = size_t{0};

  visit_entries([&](const auto& key, const auto& value) {
    if (value->cardinality && value->request_count > 0) ++count;
  });

  return count;
}

size_t CardinalityCache::distinct_miss_count() const {
  auto count = size_t{0};

  visit_entries([&](const auto& key, const auto& value) {
    if (!value->cardinality && value->request_count > 0) ++count;
  });

  return count;
}

void CardinalityCache::reset_distinct_hit_miss_counts() {
  visit_entries([&](const auto& key, const auto& value) {
    value->request_count = 0;
  });
}

void CardinalityCache::clear() {
  _engaged_cache->clear();
  _hit_count = 0;
  _miss_count = 0;
  _log.reset();
  _disengaged_cache.clear();
}

void CardinalityCache::set_log(const std::shared_ptr<std::ostream>& log) {
  _log = log;
}

void CardinalityCache::print(std::ostream& stream) const {
  stream << "-------------------- ENGAGED ENTRIES ------------------------" << std::endl;
  visit_entries([&](const auto& key, const auto& value) {
    if (value->cardinality) {
      stream << key.description() << ": " << *value->cardinality << " / " << *value->estimated_cardinality << std::endl;
    }
  });
  stream << std::endl;
  stream << "------------------- DISENGAGED ENTRIES ------------------------" << std::endl;
  visit_entries([&](const auto& key, const auto& value) {
    if (!value->cardinality) {
      stream << key.description() << ": " << "-" << std::endl;
    }
  });
}

void CardinalityCache::load(const std::string& path) {
  std::ifstream istream{path};
  Assert(istream.is_open(), "Couldn't open persistent CardinalityCache");

  boost::interprocess::file_lock file_lock(path.c_str());
  boost::interprocess::scoped_lock<boost::interprocess::file_lock> lock(file_lock);

  load(istream);
}

void CardinalityCache::load(std::istream& stream) {
  stream.seekg(0, std::ios_base::end);
  if (stream.tellg() == 0) return;
  stream.seekg(0, std::ios_base::beg);

  nlohmann::json json;
  stream >> json;

  from_json(json);
}

void CardinalityCache::from_json(const nlohmann::json& json) {
  for (const auto& pair : json) {
    const auto key = BaseJoinGraph::from_json(pair["key"]);

    const auto entry = std::make_shared<CardinalityCacheEntry>();

    if (pair.count("timeout")) entry->timeout = std::chrono::seconds{pair["timeout"].get<int>()};
    if (pair.count("value")) entry->cardinality = pair["value"].get<Cardinality>();

    if (entry->cardinality) {
      _engaged_cache->set(key.normalized(), entry);
    } else {
      _disengaged_cache.emplace(key.normalized(), entry);
    }
  }
}

void CardinalityCache::store(const std::string& path) const {
  std::ofstream stream{path};
  stream << to_json();
}

void CardinalityCache::update(const std::string& path) const {
  std::ifstream istream;
  std::ofstream ostream;

  // "Touch" the file
  if (!std::experimental::filesystem::exists(path)) {
    std::cout << "Touch cardinality estimation cache '" << path << "'" << std::endl;
    std::ofstream{path, std::ios_base::app};
  }

  {
    boost::interprocess::file_lock file_lock(path.c_str());
    boost::interprocess::scoped_lock<boost::interprocess::file_lock> lock(file_lock);

    istream.open(path);

    CardinalityCache persistent_cache(std::make_shared<CardinalityCacheUncapped>());
    persistent_cache.load(istream);
    visit_entries([&](const auto& key, const auto& value) {
      persistent_cache._get_entry(key) = value;
    });

    ostream.open(path);
    ostream << persistent_cache.to_json();

    ostream.flush(); // Make sure write-to-disk happened before unlocking
  }
}

nlohmann::json CardinalityCache::to_json() const {
  nlohmann::json json;

  json = nlohmann::json::array();

  visit_entries([&](const auto& key, const auto& value) {
    auto entry_json = nlohmann::json();

    entry_json["key"] = key.to_json();
    if (value->cardinality) entry_json["value"] = *value->cardinality;
    if (value->timeout) entry_json["timeout"] = value->timeout->count();

    json.push_back(entry_json);
  });

  return json;
}

size_t CardinalityCache::memory_consumption() const {
  auto memory = size() * 4;  // Entries = float

  visit_entries([&](const auto& key, const auto& value) {
    const auto& join_graph = key;

    memory += join_graph.vertices.size() * 8; // Pointers to the vertices
    memory += join_graph.predicates.size() * 8; // Pointers to the predicates

    for (const auto& vertex : join_graph.vertices) {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(vertex);
      Assert(stored_table_node, "Expected StoredTableNode");
      memory += stored_table_node->table_name().size();
    }

    for (const auto& predicate : join_graph.predicates) {
      memory += predicate->memory_consumption();
    }
  });

  return memory;
}

size_t CardinalityCache::memory_consumption_alt() const {
  auto memory = size() * 4;  // Entries = float

  visit_entries([&](const auto& key, const auto& value) {
    const auto& join_graph = key;

    memory += 8;

    for (const auto& predicate : join_graph.predicates) {
      std::stringstream stream;
      predicate->print(stream);
      memory += stream.str().size();
    }
  });

  return memory;
}

size_t CardinalityCache::size() const {
  return _disengaged_cache.size() + _engaged_cache->size();
}

}  // namespace opossum