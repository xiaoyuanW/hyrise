#include "jit_read_tuples.hpp"

#include <chrono>

#include "../jit_types.hpp"
#include "constant_mappings.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
// #include "utils/tracing/probes.hpp"

namespace opossum {

JitReadTuples::JitReadTuples(const bool has_validate, const std::shared_ptr<AbstractExpression>& row_count_expression)
    : _has_validate(has_validate), _row_count_expression(row_count_expression) {}

std::string JitReadTuples::description() const {
  std::stringstream desc;
  desc << "[ReadTuple] ";
  for (const auto& input_column : _input_columns) {
    desc << "(" << data_type_to_string.left.at(input_column.tuple_value.data_type()) << " x"
         << input_column.tuple_value.tuple_index() << " = Col#" << input_column.column_id << "), ";
  }
  for (const auto& input_literal : _input_literals) {
    desc << (input_literal.tuple_value.data_type() == DataType::Null
                 ? "null"
                 : data_type_to_string.left.at(input_literal.tuple_value.data_type()))
         << " x" << input_literal.tuple_value.tuple_index() << " = " << input_literal.value << ", ";
  }
  for (const auto& input_parameter : _input_parameters) {
    desc << (input_parameter.tuple_value.data_type() == DataType::Null
                 ? "null"
                 : data_type_to_string.left.at(input_parameter.tuple_value.data_type()))
         << " x" << input_parameter.tuple_value.tuple_index() << " = Par#" << input_parameter.parameter_id
         << " with val=" << (input_parameter.value ? *input_parameter.value : "not set") << ", ";
  }
  return desc.str();
}

void JitReadTuples::before_query(const Table& in_table, JitRuntimeContext& context) const {
  // Create a runtime tuple of the appropriate size
  context.tuple.resize(_num_tuple_values);
  if (_row_count_expression) {
    const auto num_rows_expression_result =
        ExpressionEvaluator{}.evaluate_expression_to_result<int64_t>(*_row_count_expression);
    context.limit_rows = num_rows_expression_result->value(0);
  }

  const auto set_value_from_input = [&context](const JitTupleValue& tuple_value, const AllTypeVariant& value) {
    auto data_type = tuple_value.data_type();
    // If data_type is null, there is nothing to do as is_null() check on null check will always return true
    if (data_type != DataType::Null) {
      resolve_data_type(data_type, [&](auto type) {
        using DataType = typename decltype(type)::type;
        context.tuple.set<DataType>(tuple_value.tuple_index(), boost::get<DataType>(value));
        if (tuple_value.is_nullable()) {
          context.tuple.set_is_null(tuple_value.tuple_index(), variant_is_null(value));
        }
        // Non-jit operators store bool values as int values
        if constexpr (std::is_same_v<DataType, Bool>) {
          context.tuple.set<bool>(tuple_value.tuple_index(), boost::get<DataType>(value));
        }
      });
    }
  };

  // Copy all input literals to the runtime tuple
  for (const auto& input_literal : _input_literals)
    set_value_from_input(input_literal.tuple_value, input_literal.value);
  // Copy all parameter values to the runtime tuple
  for (const auto& input_parameter : _input_parameters) {
    DebugAssert(input_parameter.value,
                "Value for parameter with id #" + std::to_string(input_parameter.parameter_id) + " has not been set.");
    set_value_from_input(input_parameter.tuple_value, *input_parameter.value);
  }
}

void JitReadTuples::before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& context) const {
  context.inputs.clear();
  context.chunk_offset = 0;
  context.chunk_size = in_chunk.size();
  if (_has_validate) {
    if (in_chunk.has_mvcc_columns()) {
      auto mvcc_columns = in_chunk.mvcc_columns();
      context.mvcc_columns = &(*mvcc_columns);
    } else {
      DebugAssert(in_chunk.references_exactly_one_table(),
                  "Input to Validate contains a Chunk referencing more than one table.");
      const auto& ref_col_in = std::dynamic_pointer_cast<const ReferenceColumn>(in_chunk.get_column(ColumnID{0}));
      context.referenced_table = ref_col_in->referenced_table();
      context.pos_list = ref_col_in->pos_list();
    }
  }

  // Create the column iterator for each input column and store them to the runtime context
  for (const auto& input_column : _input_columns) {
    const auto column_id = input_column.column_id;
    const auto column = in_chunk.get_column(column_id);
    const auto is_nullable = in_table.column_is_nullable(column_id);
    resolve_data_and_column_type(*column, [&](auto type, auto& typed_column) {
      using ColumnDataType = typename decltype(type)::type;
      create_iterable_from_column<ColumnDataType>(typed_column).with_iterators([&](auto it, auto end) {
        using IteratorType = decltype(it);
        if (is_nullable) {
          context.inputs.push_back(
              std::make_shared<JitColumnReader<IteratorType, ColumnDataType, true>>(it, input_column.tuple_value));
        } else {
          context.inputs.push_back(
              std::make_shared<JitColumnReader<IteratorType, ColumnDataType, false>>(it, input_column.tuple_value));
        }
      });
    });
  }
}

void JitReadTuples::execute(JitRuntimeContext& context) const {
  for (; context.chunk_offset < context.chunk_size; ++context.chunk_offset) {
    /*
    _emit(context);
    for (const auto& input : context.inputs) {
      input->increment();
    }
     */
    // DTRACE_PROBE1(HYRISE, JIT_OPERATOR_STARTED, std::string("ReadTuple").c_str());
#if JIT_MEASURE
    auto begin = std::chrono::high_resolution_clock::now();
#endif
    for (const auto& input : context.inputs) {
      input->read_value(context);
    }
#if JIT_MEASURE
    auto end = std::chrono::high_resolution_clock::now();
    context.read_time += end - begin;
#endif
    // DTRACE_PROBE1(HYRISE, JIT_OPERATOR_EXECUTED, std::string("ReadTuple").c_str());
    _emit(context);
  }
}

std::shared_ptr<AbstractExpression> JitReadTuples::row_count_expression() const { return _row_count_expression; }

JitTupleValue JitReadTuples::add_input_column(const DataType data_type, const bool is_nullable,
                                              const ColumnID column_id) {
  // There is no need to add the same input column twice.
  // If the same column is requested for the second time, we return the JitTupleValue created previously.
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(),
                               [&column_id](const auto& input_column) { return input_column.column_id == column_id; });
  if (it != _input_columns.end()) {
    return it->tuple_value;
  }

  const auto tuple_value = JitTupleValue(data_type, is_nullable, _num_tuple_values++);
  _input_columns.push_back({column_id, tuple_value});
  return tuple_value;
}

JitTupleValue JitReadTuples::add_literal_value(const AllTypeVariant& value) {
  // Somebody needs a literal value. We assign it a position in the runtime tuple and store the literal value,
  // so we can initialize the corresponding tuple value to the correct literal value later.
  const auto it = std::find_if(_input_literals.begin(), _input_literals.end(),
                               [&value](const auto& literal_value) { return literal_value.value == value; });
  if (it != _input_literals.end()) {
    return it->tuple_value;
  }

  const auto data_type = data_type_from_all_type_variant(value);
  const auto tuple_value = JitTupleValue(data_type, false, _num_tuple_values++);
  _input_literals.push_back({value, tuple_value});
  return tuple_value;
}

JitTupleValue JitReadTuples::add_parameter_value(const DataType data_type, const bool is_nullable,
                                                 const ParameterID parameter_id) {
  const auto it =
      std::find_if(_input_parameters.begin(), _input_parameters.end(),
                   [&parameter_id](const auto& parameter) { return parameter.parameter_id == parameter_id; });
  if (it != _input_parameters.end()) {
    return it->tuple_value;
  }

  const auto tuple_value = JitTupleValue(data_type, is_nullable, _num_tuple_values++);
  _input_parameters.push_back({parameter_id, tuple_value, std::nullopt});
  return tuple_value;
}

void JitReadTuples::set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  for (auto& parameter : _input_parameters) {
    auto search = parameters.find(parameter.parameter_id);
    if (search != parameters.end()) parameter.value = search->second;
  }
}

size_t JitReadTuples::add_temporary_value() {
  // Somebody wants to store a temporary value in the runtime tuple. We don't really care about the value itself,
  // but have to remember to make some space for it when we create the runtime tuple.
  return _num_tuple_values++;
}

std::vector<JitInputColumn> JitReadTuples::input_columns() const { return _input_columns; }

std::vector<JitInputLiteral> JitReadTuples::input_literals() const { return _input_literals; }

std::vector<JitInputParameter> JitReadTuples::input_parameters() const { return _input_parameters; }

std::optional<ColumnID> JitReadTuples::find_input_column(const JitTupleValue& tuple_value) const {
  const auto it = std::find_if(_input_columns.begin(), _input_columns.end(), [&tuple_value](const auto& input_column) {
    return input_column.tuple_value == tuple_value;
  });

  if (it != _input_columns.end()) {
    return it->column_id;
  } else {
    return {};
  }
}

std::optional<AllTypeVariant> JitReadTuples::find_literal_value(const JitTupleValue& tuple_value) const {
  const auto it = std::find_if(_input_literals.begin(), _input_literals.end(),
                               [&tuple_value](const auto& literal) { return literal.tuple_value == tuple_value; });

  if (it != _input_literals.end()) {
    return it->value;
  } else {
    return {};
  }
}

}  // namespace opossum
