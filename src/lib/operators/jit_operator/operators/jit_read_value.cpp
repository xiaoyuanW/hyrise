#include "jit_read_value.hpp"
#include "constant_mappings.hpp"

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
  context.inputs[_input_column_index]->read_value(context);
#endif
  _emit(context);
}

}  // namespace opossum
