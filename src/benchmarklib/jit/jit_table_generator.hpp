#pragma once

#include <benchmark_utilities/abstract_benchmark_table_generator.hpp>
#include <benchmark_utilities/random_generator.hpp>
#include <storage/storage_manager.hpp>

namespace opossum {

class JitTableGenerator : public AbstractBenchmarkTableGenerator {
 public:
  explicit JitTableGenerator(float scale_factor, uint32_t chunk_size = Chunk::MAX_SIZE);

  void generate_and_store();

  std::map<std::string, std::shared_ptr<opossum::Table>> generate_all_tables();

 private:
  const float _scale_factor;
};

}  // namespace opossum
