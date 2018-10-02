#pragma once

#include "abstract_jittable.hpp"
#include "jit_read_tuples.hpp"

namespace opossum {

class BaseJitSegmentReaderWrapper;

class JitReadValue : public AbstractJittable {
 public:
  explicit JitReadValue(const JitInputColumn input_column,
          std::shared_ptr<BaseJitSegmentReaderWrapper> input_segment_wrapper)
#if JIT_LAZY_LOAD
      : _input_column(input_column),
        _input_segment_wrapper(input_segment_wrapper)
#endif
  {
  }

  std::string description() const final;

 private:
  void _consume(JitRuntimeContext& context) const final;
#if JIT_LAZY_LOAD
  const JitInputColumn _input_column;
  const std::shared_ptr<BaseJitSegmentReaderWrapper> _input_segment_wrapper;
#endif
};

}  // namespace opossum
