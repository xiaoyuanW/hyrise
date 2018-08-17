#include "joe_query.hpp"

#include <chrono>
#include <sstream>

#include "out.hpp"
#include "write_csv.hpp"
#include "planviz/join_graph_visualizer.hpp"
#include "joe.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoeQuerySample &sample) {
  stream << sample.name << ",";

  if (sample.best_plan) {
    stream << sample.best_plan->sample.execution_duration.count();
  } else {
    stream << 0;
  }

  return stream;
}

JoeQuery::JoeQuery(const Joe& joe, const std::string& name, const std::string& sql):
  joe(joe), sql(sql) {
  sample.name = name;
}

void JoeQuery::run() {
  out() << "-- Evaluating Query: " << sample.name << std::endl;

  const auto config = joe.config;

  // Enable CardinalityCache logging
  if (config->cardinality_estimation_cache_log) {
    config->cardinality_estimation_cache->set_log(std::make_shared<std::ofstream>(prefix() + "CardinalityCache-" + sample.name + ".log"));
  }

  execution_begin = std::chrono::steady_clock::now();
  save_plan_results = config->save_results;

  // Init Query Iterations
  for (size_t query_iteration_idx = size_t{0}; query_iteration_idx < config->iterations_per_query; ++query_iteration_idx) {
    query_iterations.emplace_back(*this, query_iteration_idx);
  }

  // Run Query Iterations
  for (auto& query_iteration : query_iterations) {
    query_iteration.run();

    if (!sample.best_plan || sample.best_plan->sample.execution_duration > query_iteration.sample.best_plan->sample.execution_duration) {
      sample.best_plan = query_iteration.sample.best_plan;
    }

    if (config->save_query_iterations_results) {
      write_csv(query_iterations,
                "RankZeroPlanExecutionDuration,BestPlanExecutionDuration,PlanningDuration,CECachingDuration,CECacheHitCount,CECacheMissCount,CECacheSize,"
                "CECacheDistinctHitCount,CECacheDistinctMissCount,CECacheMemoryConsumption,CECacheMemoryConsumptionAlt,RankZeroPlanHash,BestPlanExecutionDuration",
                prefix() + "Iterations.csv");
    }

    if (config->visualize && !join_graph_visualized) {
      visualize_join_graph(query_iteration.join_graph);
      join_graph_visualized = true;
    }
  }

}

void JoeQuery::visualize_join_graph(const std::shared_ptr<JoinGraph>& join_graph) {
  const auto config = joe.config;

  try {
    JoinGraphVisualizer visualizer{{}, {}, {}, {}};
    visualizer.visualize(join_graph, config->tmp_dot_file_path,
                         std::string(config->evaluation_dir + "/viz/") + sample.name + "-JoinGraph.svg");
  } catch (const std::exception &e) {
    out() << "----- Error while visualizing: " << e.what() << std::endl;
  }
}

std::string JoeQuery::prefix() const {
  std::stringstream stream;
  stream << joe.prefix() << sample.name << ".";
  return stream.str();
}

}  // namespace opossum
