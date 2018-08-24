#include <iostream>

#include <json.hpp>

#include <jit/jit_table_generator.hpp>
#include <jit_evaluation_helper.hpp>
#include <operators/jit_operator/specialization/jit_repository.hpp>
#include <planviz/lqp_visualizer.hpp>
#include <planviz/sql_query_plan_visualizer.hpp>
#include <sql/sql_pipeline.hpp>
#include <tpch/tpch_db_generator.hpp>
#include <resolve_type.hpp>
#include <storage/chunk_encoder.hpp>
#include <scheduler/current_scheduler.hpp>
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"

#include <papi.h>
#include <sql/sql_pipeline_builder.hpp>
#include "types.hpp"
#include "global.hpp"
#include "sql/sql_query_cache.hpp"

const size_t cache_line = 64;

using namespace opossum;

template <typename Vector>
void remove_vector_from_cache(Vector& vector) {
  for (auto& value : vector) {
    const auto address = reinterpret_cast<size_t>(&value);
    if (address % cache_line == 0) {
      asm volatile("clflush (%0)\n\t" : : "r"(&value) : "memory");
    }
  }
  asm volatile("sfence\n\t" : : : "memory");
};

void remove_table_from_cache(opossum::Table& table) {
  for (opossum::ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto chunk = table.get_chunk(chunk_id);
    for (opossum::ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
      auto column = chunk->get_column(column_id);
      const auto data_type = table.column_data_type(column_id);
      opossum::resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        if (auto value_column = std::dynamic_pointer_cast<const opossum::ValueColumn<ColumnDataType>>(column)) {
          remove_vector_from_cache(value_column->values());
          if (table.column_is_nullable(column_id)) {
            remove_vector_from_cache(value_column->null_values());
          }
        } else if (auto dict_column = std::dynamic_pointer_cast<const opossum::DictionaryColumn<ColumnDataType>>(column)) {
          remove_vector_from_cache(*dict_column->dictionary());
          auto base_attribute_vector = dict_column->attribute_vector();

          if (auto attribute_vector = std::dynamic_pointer_cast<const opossum::FixedSizeByteAlignedVector<uint8_t>>(base_attribute_vector)) {
            remove_vector_from_cache(attribute_vector->data());
          } else if (auto attribute_vector = std::dynamic_pointer_cast<const opossum::FixedSizeByteAlignedVector<uint16_t>>(base_attribute_vector)) {
            remove_vector_from_cache(attribute_vector->data());
          } else if (auto attribute_vector = std::dynamic_pointer_cast<const opossum::FixedSizeByteAlignedVector<uint32_t>>(base_attribute_vector)) {
            remove_vector_from_cache(attribute_vector->data());
          } else {
            throw std::logic_error("could not flush cache, unknown attribute vector type");
          }
        } else {
          throw std::logic_error("could not flush cache, unknown column type");
        }
      });
    }
  }
}

void lqp() {
  auto& experiment = opossum::JitEvaluationHelper::get().experiment();
  const std::string query_id = experiment["query_id"];
  const bool optimize = experiment["optimize"];
  const std::string lqp_file = experiment["lqp_file"];
  bool mvcc = experiment["mvcc"];

  const std::string query_string = opossum::JitEvaluationHelper::get().queries()[query_id]["query"];

  opossum::SQLPipeline pipeline = opossum::SQLPipelineBuilder(query_string).with_mvcc(UseMvcc(mvcc)).create_pipeline();
  const auto plans = optimize ? pipeline.get_optimized_logical_plans() : pipeline.get_unoptimized_logical_plans();

  opossum::LQPVisualizer visualizer;
  visualizer.visualize(plans, lqp_file + ".dot", lqp_file + ".png");
}

void pqp() {
  auto& experiment = opossum::JitEvaluationHelper::get().experiment();
  const std::string query_id = experiment["query_id"];
  const std::string pqp_file = experiment["pqp_file"];
  bool mvcc = experiment["mvcc"];

  const std::string query_string = opossum::JitEvaluationHelper::get().queries()[query_id]["query"];

  opossum::SQLPipeline pipeline = opossum::SQLPipelineBuilder(query_string).with_mvcc(UseMvcc(mvcc)).create_pipeline();
  opossum::SQLQueryPlan query_plan(opossum::CleanupTemporaries::Yes);
  const auto plans = pipeline.get_query_plans();
  for (const auto& plan : plans) {
    query_plan.append_plan(*plan);
  }

  opossum::SQLQueryPlanVisualizer visualizer;
  visualizer.visualize(query_plan, pqp_file + ".dot", pqp_file + ".png");
}

void run() {
  auto& experiment = opossum::JitEvaluationHelper::get().experiment();
  const std::string query_id = experiment["query_id"];
  const auto query = opossum::JitEvaluationHelper::get().queries()[query_id];
  std::string query_string = query["query"];
  bool mvcc = experiment["mvcc"];

  const auto table_names = query["tables"];
  for (const auto& table_name : table_names) {
    auto table = opossum::StorageManager::get().get_table(table_name.get<std::string>());
    remove_table_from_cache(*table);
  }

  // Make sure all table statistics are generated and ready.
  opossum::SQLPipelineBuilder(query_string).with_mvcc(UseMvcc(mvcc)).create_pipeline().get_optimized_logical_plans();
  auto& result = opossum::JitEvaluationHelper::get().result();

  result = nlohmann::json::object();
  if (experiment.at("engine") == "jit") {
    opossum::JitEvaluationHelper::get().result()["dynamic_resolved"] = 0;
    opossum::JitEvaluationHelper::get().result()["static_resolved"] = 0;
    opossum::JitEvaluationHelper::get().result()["resolved_vtables"] = 0;
    opossum::JitEvaluationHelper::get().result()["not_resolved_vtables"] = 0;
    opossum::JitEvaluationHelper::get().result()["inlined_functions"] = 0;
    opossum::JitEvaluationHelper::get().result()["replaced_values"] = 0;
  }

  opossum::SQLPipeline pipeline = opossum::SQLPipelineBuilder(query_string).with_mvcc(UseMvcc(mvcc)).create_pipeline();
  const auto table = pipeline.get_result_table();

  if (experiment.count("print") && experiment["print"]) {
    opossum::Print::print(table, 0, std::cerr);
  }

  result["result_rows"] = table->row_count();
  result["pipeline_compile_time"] = pipeline.metrics().statement_metrics.front()->compile_time_micros.count();
  result["pipeline_execution_time"] = pipeline.metrics().statement_metrics.front()->execution_time_micros.count();
  result["pipeline_optimize_time"] = pipeline.metrics().statement_metrics.front()->optimize_time_micros.count();
}

int main(int argc, char* argv[]) {
  std::cerr << "Starting the JIT benchmarking suite" << std::endl;

  if (argc <= 1) {
    freopen("input.json", "r", stdin);
    freopen("output.json", "w", stdout);
  }

  nlohmann::json config;
  std::cin >> config;
  opossum::JitEvaluationHelper::get().queries() = config["queries"];
  opossum::JitEvaluationHelper::get().globals() = config["globals"];

  const auto additional_scale_factor = argc > 1 ? std::stod(argv[1]) : 1.0;
  double scale_factor = config["globals"]["scale_factor"].get<double>() * additional_scale_factor;
  config["globals"]["scale_factor"] = scale_factor;

  if (config["globals"]["use_tpch_tables"]) {
    std::cerr << "Generating TPCH tables with scale factor " << scale_factor << std::endl;
    opossum::TpchDbGenerator generator(scale_factor, opossum::ChunkID(100000));
    generator.generate_and_store();
  }

  if (config["globals"]["use_other_tables"]) {
    std::cerr << "Generating JIT tables with scale factor " << scale_factor << std::endl;
    opossum::JitTableGenerator generator(scale_factor, opossum::ChunkID(100000));
    generator.generate_and_store();
  }

  if (config["globals"]["dictionary_compress"]) {
    std::cerr << "Dictionary encoding tables" << std::endl;
    for (const auto& table_name : opossum::StorageManager::get().table_names()) {
      auto table = opossum::StorageManager::get().get_table(table_name);
      opossum::ChunkEncoder::encode_all_chunks(table);
    }
  }

  std::cerr << "Table Information" << std::endl;
  for (const auto& table_name : opossum::StorageManager::get().table_names()) {
    auto table = opossum::StorageManager::get().get_table(table_name);
    std::cerr << table_name << ": " << table->row_count() << " rows, " << table->chunk_count() << " chunks, " << table->column_count() << " columns, " << table->estimate_memory_usage() << " bytes" <<  std::endl;
  }

  std::cerr << "Initializing JIT repository" << std::endl;
  opossum::JitRepository::get();

  std::cerr << "Initializing PAPI" << std::endl;
  if (PAPI_library_init(PAPI_VER_CURRENT) < 0) throw std::logic_error("PAPI error");
  std::cerr << "  supports " << PAPI_num_counters() << " event counters" << std::endl;
  std::cout << "{" << std::endl << "\"results\":[" << std::endl;

  opossum::Global::get().jit_evaluate = true;

  const size_t num_experiments = config["experiments"].size();
  for (size_t current_experiment = 0; current_experiment < num_experiments; ++current_experiment) {
    auto& experiment = config["experiments"][current_experiment];
    if (!experiment.count("mvcc")) experiment["mvcc"] = false;
    if (experiment.at("engine") == "opossum") {
      opossum::Global::get().jit = false;
    } else if (experiment.at("engine") == "jit") {
      if (!experiment.count("lazy_load")) experiment["lazy_load"] = true;
      if (!experiment.count("jit_validate")) experiment["jit_validate"] = true;
      opossum::Global::get().jit = true;
      opossum::Global::get().lazy_load = experiment["lazy_load"];
      opossum::Global::get().jit_validate = experiment["jit_validate"];
    } else {
      opossum::Fail("unknown query engine parameter");
    }
    opossum::JitEvaluationHelper::get().experiment() = experiment;
    nlohmann::json output{{"globals", config["globals"]}, {"experiment", experiment}, {"results", nlohmann::json::array()}};
    const uint32_t num_repetitions = experiment.count("repetitions") ? experiment["repetitions"].get<uint32_t>() : 1;
    uint32_t current_repetition = 0;
    for (uint32_t i = 0; i < num_repetitions; ++i) {
      current_repetition++;
      std::cerr << "Running experiment " << (current_experiment+1) << "/" << num_experiments << " repetition " << current_repetition << "/" << num_repetitions << std::endl;

      opossum::JitEvaluationHelper::get().result() = nlohmann::json::object();
      if (experiment["task"] == "lqp") {
        lqp();
      } else if (experiment["task"] == "pqp") {
        pqp();
      } else if (experiment["task"] == "run") {
        run();
      } else {
        throw std::logic_error("unknown task");
      }
      output["results"].push_back(opossum::JitEvaluationHelper::get().result());
    }
    SQLQueryCache<SQLQueryPlan>::get().clear();
    std::cout << output;
    bool not_last = current_experiment + 1 < num_experiments;
    if (not_last) std::cout << ",";
    std::cout << std::endl;
  }
  std::cout << "]" << std::endl << "}" << std::endl;
  std::cerr << "Done" << std::endl;
}
