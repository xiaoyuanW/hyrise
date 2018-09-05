#pragma once

#include <vector>

#include "operators/jit_operator/jit_types.hpp"
#include "../jit_utils.hpp"

namespace opossum {

/**
 * AbstractJittable is the abstract super class of all operators used within a JitOperatorWrapper.
 * Usually, multiple operators are linked together to form an operator chain.
 * The operators work in a push-based fashion: The virtual "next" function is called for each tuple.
 * The operator can then process the tuple and finally call its own "emit" function to pass the tuple
 * on to the next operator in the chain.
 */
class AbstractJittable {
 public:
  AbstractJittable(const JitOperatorType type) : jit_operator_type(type) {}
  virtual ~AbstractJittable() = default;

  void set_next_operator(const std::shared_ptr<AbstractJittable>& next_operator) { _next_operator = next_operator; }

  std::shared_ptr<AbstractJittable> next_operator() { return _next_operator; }

  virtual std::string description() const = 0;

  // if bool is true, it means that loading the value can be pushed into the operator
  virtual std::map<size_t, bool> accessed_column_ids() const { return std::map<size_t, bool>(); }

  const JitOperatorType jit_operator_type;

 protected:
  // inlined during compilation to reduce the number of functions inlined during specialization
  __attribute__((always_inline)) void _emit(JitRuntimeContext& context) const {
#if JIT_MEASURE
    const auto end_operator = std::chrono::high_resolution_clock::now();
    context.times[jit_operator_type] += end_operator - context.begin_operator;
    context.begin_operator = end_operator;
#endif
    _next_operator->_consume(context);
  }
#if JIT_MEASURE
  __attribute__((always_inline)) void _end(JitRuntimeContext& context) const {
    const auto end_operator = std::chrono::high_resolution_clock::now();
    context.times[jit_operator_type] += end_operator - context.begin_operator;
    context.begin_operator = end_operator;
  }
#endif

 private:
  virtual void _consume(JitRuntimeContext& context) const = 0;

  std::shared_ptr<AbstractJittable> _next_operator;
};

}  // namespace opossum
