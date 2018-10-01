#pragma once

#include "abstract_jittable.hpp"
#include "jit_read_tuples.hpp"

namespace opossum {

class JitReadValue : public AbstractJittable {
 public:
  explicit JitReadValue(const JitInputColumn input_column, const size_t input_column_index)
#if JIT_LAZY_LOAD
      : _input_column(input_column),
        _input_column_index(input_column_index)
#endif
  {
  }

  std::string description() const final;

 private:
  void _consume(JitRuntimeContext& context) const final;
#if JIT_LAZY_LOAD
  const JitInputColumn _input_column;
  const size_t _input_column_index;
#endif
};

}  // namespace opossum
