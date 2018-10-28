#pragma once

#include "abstract_jittable.hpp"
#include "concurrency/transaction_context.hpp"
#include "types.hpp"

namespace opossum {

/* The JitValidate operator validates visibility of tuples
 * within the context of a given transaction
 */
template <TableType input_table_type = TableType::Data>
class JitValidate : public AbstractJittable {
 public:
  JitValidate();

  std::string description() const final;

 protected:
  void _consume(JitRuntimeContext& context) const final;

 public:
  __attribute__((optnone))
  static TransactionID _load(const copyable_atomic<TransactionID>& tid);
};

}  // namespace opossum
