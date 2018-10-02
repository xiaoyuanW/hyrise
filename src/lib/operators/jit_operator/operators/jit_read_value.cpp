#include "jit_read_value.hpp"

#include "constant_mappings.hpp"
#include "jit_segment_reader.hpp"

namespace opossum {

std::string JitReadValue::description() const {
  std::stringstream desc;
#if JIT_LAZY_LOAD
  desc << "[ReadValue] x" << _input_column.tuple_value.tuple_index() << " = Col#" << _input_column.column_id << ", ";
#endif
  return desc.str();
}

void JitReadValue::_consume(JitRuntimeContext& context) const {
#if JIT_LAZY_LOAD
  _input_segment_wrapper->read_value(context);
#endif
  _emit(context);
}

}  // namespace opossum
