#include "jit_write_tuples.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

std::string JitWriteTuples::description() const {
  std::stringstream desc;
  desc << "[WriteTuple] ";
  for (const auto& output_column : _output_columns) {
    desc << output_column.column_name << " = x" << output_column.tuple_value.tuple_index() << ", ";
  }
  return desc.str();
}

std::shared_ptr<Table> JitWriteTuples::create_output_table(const ChunkOffset input_table_chunk_size) const {
  TableColumnDefinitions column_definitions;

  for (const auto& output_column : _output_columns) {
    // Add a column definition for each output column
    const auto data_type = output_column.tuple_value.data_type();
    const auto is_nullable = output_column.tuple_value.is_nullable();
    column_definitions.emplace_back(output_column.column_name, data_type, is_nullable);
  }

  return std::make_shared<Table>(column_definitions, TableType::Data, input_table_chunk_size);
}

void JitWriteTuples::before_query(Table& out_table, JitRuntimeContext& context) const { _create_output_chunk(context); }

void JitWriteTuples::after_chunk(Table& out_table, JitRuntimeContext& context) const {
  if (!context.out_chunk.empty() && context.out_chunk[0]->size() > 0) {
    out_table.append_chunk(context.out_chunk);
    _create_output_chunk(context);
  }
}

void JitWriteTuples::add_output_column(const std::string& column_name, const JitTupleValue& value) {
  _output_columns.push_back({column_name, value});
}

std::vector<JitOutputColumn> JitWriteTuples::output_columns() const { return _output_columns; }

void JitWriteTuples::_consume(JitRuntimeContext& context) const {
  for (const auto& output : context.outputs) {
    output->write_value(context);
  }
}

void JitWriteTuples::_create_output_chunk(JitRuntimeContext& context) const {
  context.out_chunk.clear();
  context.outputs.clear();

  // Create new value segments and add them to the runtime context to make them accessible by the segment writers
  for (const auto& output_column : _output_columns) {
    const auto data_type = output_column.tuple_value.data_type();
    const auto is_nullable = output_column.tuple_value.is_nullable();

    // Create the appropriate segment writer for the output segment
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto segment = std::make_shared<ValueSegment<ColumnDataType>>(output_column.tuple_value.is_nullable());
      context.out_chunk.push_back(segment);

      if (is_nullable) {
        context.outputs.push_back(
            std::make_shared<JitSegmentWriter<ValueSegment<ColumnDataType>, ColumnDataType, true>>(
                segment, output_column.tuple_value));
      } else {
        context.outputs.push_back(
            std::make_shared<JitSegmentWriter<ValueSegment<ColumnDataType>, ColumnDataType, false>>(
                segment, output_column.tuple_value));
      }
    });
  }
}

}  // namespace opossum
