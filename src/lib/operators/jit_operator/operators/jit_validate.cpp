#include "jit_validate.hpp"

#include "operators/jit_operator/jit_types.hpp"
#include "operators/jit_operator/jit_utils.hpp"
#include "operators/validate.hpp"

namespace opossum {

JitValidate::JitValidate(const TableType input_table_type) : AbstractJittable(JitOperatorType::Validate),
                                                             _input_table_type(input_table_type) {}

std::string JitValidate::description() const {
  return "[Validate]";
}

void JitValidate::set_input_table_type(const TableType input_table_type) {
  _input_table_type = input_table_type;
}

void JitValidate::_consume(JitRuntimeContext& context) const {
  if (_input_table_type == TableType::References) {
    const auto row_id = (*context.pos_list)[context.chunk_offset];
    const auto referenced_chunk = context.referenced_table->get_chunk(row_id.chunk_id);
    const auto mvcc_data = referenced_chunk->mvcc_data();
    if (_is_row_visible(context.transaction_id, context.snapshot_commit_id, row_id.chunk_offset, *mvcc_data)) {
      _emit(context);
    }
  } else {
    if (_is_row_visible(context.transaction_id, context.snapshot_commit_id, context.chunk_offset,
                                        *context.mvcc_data)) {
      _emit(context);
    }
  }
#if JIT_MEASURE
  _end(context);
#endif
}

bool JitValidate::_is_row_visible(const CommitID our_tid, const CommitID snapshot_commit_id,
                                     const ChunkOffset chunk_offset, const MvccData& mvcc_data) const {
  const auto row_tid = _load_atomic_value(mvcc_data.tids[chunk_offset]);
  const auto begin_cid = mvcc_data.begin_cids[chunk_offset];
  const auto end_cid = mvcc_data.end_cids[chunk_offset];
  return Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid);
}

TransactionID JitValidate::_load_atomic_value(const copyable_atomic<TransactionID>& transaction_id) {
  return transaction_id.load();
}

}  // namespace opossum
