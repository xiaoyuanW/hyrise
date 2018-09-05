#include "jit_filter.hpp"

namespace opossum {

JitFilter::JitFilter(const JitTupleValue& condition) : AbstractJittable(JitOperatorType::Filter), _condition{condition} {
  DebugAssert(condition.data_type() == DataType::Bool || condition.data_type() == DataTypeBool,
              "Filter condition must be a boolean");
}

std::string JitFilter::description() const { return "[Filter] on x" + std::to_string(_condition.tuple_index()); }

JitTupleValue JitFilter::condition() { return _condition; }

void JitFilter::_consume(JitRuntimeContext& context) const {
  if (!_condition.is_null(context) && _condition.get<bool>(context)) {
    jit_end_operator_filter();
    _emit(context);
  } else {
    jit_end_operator_filter();
#if JIT_MEASURE
    _end(context);
#endif
  }
}

std::map<size_t, bool> JitFilter::accessed_column_ids() const {
  std::map<size_t, bool> column_ids;
  column_ids.emplace(_condition.tuple_index(), false);
  return column_ids;
}

}  // namespace opossum
