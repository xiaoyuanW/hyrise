#pragma once

#include "abstract_jittable_sink.hpp"

namespace opossum {

struct JitOutputReferenceColumn {
  const std::string column_name;
  const DataType data_type;
  const bool is_nullable;
  const ColumnID referenced_column_id;
};

/* JitWriteOffset must be the last operator in any chain of jit operators.
 * It is responsible for
 * 1) adding column definitions to the output table
 * 2) appending the current offset to the current output chunk
 * 3) creating a new output chunk and adding output chunks to the output table
 */
class JitWriteOffset : public AbstractJittableSink {
 public:
  JitWriteOffset() : AbstractJittableSink(JitOperatorType::WriteOffset) {}

  std::string description() const final;

  std::shared_ptr<Table> create_output_table(const ChunkOffset input_table_chunk_size) const final;
  void before_query(const Table& in_table, Table& out_table, JitRuntimeContext& context) const override;
  void after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                   JitRuntimeContext& context) const final;

  void add_output_column(const JitOutputReferenceColumn& output_column);

  std::vector<JitOutputReferenceColumn> output_columns() const;

 private:
  void _consume(JitRuntimeContext& context) const final;

  void _create_output_chunk(JitRuntimeContext& context, const uint32_t in_chunk_size) const;

  std::vector<JitOutputReferenceColumn> _output_columns;
};

}  // namespace opossum
