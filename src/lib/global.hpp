#pragma once

#include <chrono>
#include <map>

namespace opossum {

struct OperatorTimes {
  std::chrono::microseconds preparation_time;
  std::chrono::microseconds execution_time;
};

enum class OperatorType;

struct Global {
  static Global& get();

  bool jit = false;
  bool lazy_load = false;
  bool jit_validate = false;
  bool deep_copy_exists = false;
  bool jit_evaluate = false;
  std::map<OperatorType, OperatorTimes> times;
};

}  // namespace opossum
