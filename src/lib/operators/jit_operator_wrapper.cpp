#include "jit_operator_wrapper.hpp"

#include "expression/expression_utils.hpp"
#include "global.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_read_value.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"

#include "jit_evaluation_helper.hpp"

namespace opossum {

JitOperatorWrapper::JitOperatorWrapper(
    const std::shared_ptr<const AbstractOperator>& left, const JitExecutionMode execution_mode,
    const std::list<std::shared_ptr<AbstractJittable>>& jit_operators,
    const std::function<void(const JitReadTuples*, JitRuntimeContext&)>& execute_func)
    : AbstractReadOnlyOperator{OperatorType::JitOperatorWrapper, left},
      _execution_mode{execution_mode},
      _jit_operators{jit_operators},
      _execute_func{execute_func} {
  if (JitEvaluationHelper::get().experiment().count("jit_use_jit")) {
    _execution_mode = JitEvaluationHelper::get().experiment().at("jit_use_jit") ? JitExecutionMode::Compile : JitExecutionMode::Interpret;
  }
}

const std::string JitOperatorWrapper::name() const { return "JitOperatorWrapper"; }

const std::string JitOperatorWrapper::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperatorWrapper]" << separator;
  for (const auto& op : _jit_operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

void JitOperatorWrapper::add_jit_operator(const std::shared_ptr<AbstractJittable>& op) { _jit_operators.push_back(op); }

const std::list<std::shared_ptr<AbstractJittable>>& JitOperatorWrapper::jit_operators() const { return _jit_operators; }

const std::shared_ptr<JitReadTuples> JitOperatorWrapper::_source() const {
  return std::dynamic_pointer_cast<JitReadTuples>(_jit_operators.front());
}

const std::shared_ptr<AbstractJittableSink> JitOperatorWrapper::_sink() const {
  return std::dynamic_pointer_cast<AbstractJittableSink>(_jit_operators.back());
}

void JitOperatorWrapper::insert_loads(const bool lazy) {
  if (!lazy) {
    auto itr = ++_jit_operators.cbegin();
    for (size_t index = 0; index < _source()->input_columns().size(); ++index) {
      itr = _jit_operators.insert(itr, std::make_shared<JitReadValue>(_source()->input_columns()[index], index));
    }
    return;
  }
  std::map<size_t, size_t> inverted_input_columns;
  auto input_columns = _source()->input_columns();
  for (size_t input_column_index = 0; input_column_index < input_columns.size(); ++input_column_index) {
    inverted_input_columns[input_columns[input_column_index].tuple_value.tuple_index()] = input_column_index;
  }

  std::list<std::shared_ptr<AbstractJittable>> jit_operators;
  std::vector<std::map<size_t, bool>> accessed_column_ids;
  accessed_column_ids.reserve(jit_operators.size());
  std::map<size_t, bool> column_id_used_by_one_operator;
  for (const auto& jit_operator : _jit_operators) {
    auto col_ids = jit_operator->accessed_column_ids();
    accessed_column_ids.emplace_back(col_ids);
    for (const auto& pair : accessed_column_ids.back()) {
      if (inverted_input_columns.count(pair.first)) {
        column_id_used_by_one_operator[pair.first] = !column_id_used_by_one_operator.count(pair.first);
      }
    }
  }
  auto ids_itr = accessed_column_ids.begin();
  for (auto& jit_operator : _jit_operators) {
    for (const auto& pair : *ids_itr++) {
      if (column_id_used_by_one_operator.count(pair.first)) {
        if (pair.second && column_id_used_by_one_operator[pair.first]) {
          // insert within JitCompute operator
          auto compute_ptr = std::dynamic_pointer_cast<JitCompute>(jit_operator);
          compute_ptr->set_load_column(pair.first, inverted_input_columns[pair.first]);
        } else {
          jit_operators.emplace_back(std::make_shared<JitReadValue>(
              _source()->input_columns()[inverted_input_columns[pair.first]], inverted_input_columns[pair.first]));
        }
        column_id_used_by_one_operator.erase(pair.first);
      }
    }
    jit_operators.push_back(jit_operator);
  }

  _jit_operators = jit_operators;
}

void JitOperatorWrapper::_prepare() {
  Assert(_source(), "JitOperatorWrapper does not have a valid source node.");
  Assert(_sink(), "JitOperatorWrapper does not have a valid sink node.");

  _choose_execute_func();
}

void JitOperatorWrapper::_choose_execute_func() {
  if (_execute_func) return;

  // std::cout << "Before make loads lazy:" << std::endl << description(DescriptionMode::MultiLine) << std::endl;
  // insert_loads(Global::get().lazy_load);
  // std::cout << "Specialising: " << (_execution_mode == JitExecutionMode::Compile ? "true" : "false") << std::endl;

  // Connect operators to a chain
  for (auto it = _jit_operators.begin(), next = ++_jit_operators.begin();
       it != _jit_operators.end() && next != _jit_operators.end(); ++it, ++next) {
    (*it)->set_next_operator(*(next));
  }

  // std::cerr << description(DescriptionMode::MultiLine) << std::endl;

  // We want to perform two specialization passes if the operator chain contains a JitAggregate operator, since the
  // JitAggregate operator contains multiple loops that need unrolling.
  auto two_specialization_passes = static_cast<bool>(std::dynamic_pointer_cast<JitAggregate>(_sink()));
  switch (_execution_mode) {  // _execution_mode
    case JitExecutionMode::Compile:
      // this corresponds to "opossum::JitReadTuples::execute(opossum::JitRuntimeContext&) const"
      _execute_func = _module.specialize_and_compile_function<void(const JitReadTuples*, JitRuntimeContext&)>(
          "_ZNK7opossum13JitReadTuples7executeERNS_17JitRuntimeContextE",
                  std::make_shared<JitConstantRuntimePointer>(_source().get()), two_specialization_passes);
      break;
    case JitExecutionMode::Interpret:
      _execute_func = &JitReadTuples::execute;
      break;
  }
}

std::shared_ptr<const Table> JitOperatorWrapper::_on_execute() {
  auto begin_total = std::chrono::high_resolution_clock::now();
  const auto& in_table = *input_left()->get_output();
  auto out_table = _sink()->create_output_table(in_table.max_chunk_size());

  JitRuntimeContext context;
  if (transaction_context_is_set()) {
    context.transaction_id = transaction_context()->transaction_id();
    context.snapshot_commit_id = transaction_context()->snapshot_commit_id();
  }

  std::chrono::nanoseconds before_query_time = std::chrono::nanoseconds::zero();
  std::chrono::nanoseconds after_query_time = std::chrono::nanoseconds::zero();
  std::chrono::nanoseconds before_chunk_time = std::chrono::nanoseconds::zero();
  std::chrono::nanoseconds after_chunk_time = std::chrono::nanoseconds::zero();

  {
    auto begin = std::chrono::high_resolution_clock::now();
    _source()->before_query(in_table, context);
    _sink()->before_query(*out_table, context);
    auto end = std::chrono::high_resolution_clock::now();
    before_query_time += end - begin;
  }


  for (opossum::ChunkID chunk_id{0}; chunk_id < in_table.chunk_count(); ++chunk_id) {
    { 
      auto begin = std::chrono::high_resolution_clock::now();     
      const auto& in_chunk = *in_table.get_chunk(chunk_id);
      _source()->before_chunk(in_table, in_chunk, context);
      auto end = std::chrono::high_resolution_clock::now();
      before_chunk_time += end - begin;
    }
    _execute_func(_source().get(), context);
    {
      auto begin = std::chrono::high_resolution_clock::now();
      _sink()->after_chunk(*out_table, context);
      auto end = std::chrono::high_resolution_clock::now();
      after_chunk_time += end - begin;
    }
    // break, if limit is reached
    if (context.chunk_offset == std::numeric_limits<ChunkOffset>::max()) break;
  }
  auto end_total = std::chrono::high_resolution_clock::now();
  auto diff_total = std::chrono::duration_cast<std::chrono::microseconds>(end_total - begin_total).count();
  {
    auto begin = std::chrono::high_resolution_clock::now();
    _sink()->after_query(*out_table, context);
    auto end = std::chrono::high_resolution_clock::now();
    after_query_time += end - begin;
  }
  auto before_query_time_count = std::chrono::duration_cast<std::chrono::microseconds>(before_query_time).count();
  std::cerr << "Before query time in micro s: " << before_query_time_count << std::endl;
  auto before_chunk_time_count = std::chrono::duration_cast<std::chrono::microseconds>(before_chunk_time).count();
  std::cerr << "Before chunk time in micro s: " << before_chunk_time_count << std::endl;

  auto read_time_count = std::chrono::duration_cast<std::chrono::microseconds>(context.read_time).count();
  std::cerr << "JitRead time in micro s: " << read_time_count << std::endl;
  auto compute_time_count = std::chrono::duration_cast<std::chrono::microseconds>(context.compute_time).count();
  std::cerr << "compute_time in micro s: " << compute_time_count << std::endl;
  auto filter_time_count = std::chrono::duration_cast<std::chrono::microseconds>(context.filter_time).count();
  std::cerr << "filter_time in micro s: " << filter_time_count << std::endl;
  
  auto aggregate_time_count = std::chrono::duration_cast<std::chrono::microseconds>(context.aggregate_time).count();
  if (aggregate_time_count) std::cerr << "aggregate_time in micro s: " << aggregate_time_count << std::endl;
  auto write_time_count = std::chrono::duration_cast<std::chrono::microseconds>(context.write_time).count();
  if (write_time_count) std::cerr << "write_time in micro s: " << write_time_count << std::endl;
  auto after_chunk_count = std::chrono::duration_cast<std::chrono::microseconds>(after_chunk_time).count();
  std::cerr << "After chunk time in micro s: " << after_chunk_count << std::endl;
  auto after_query_count = std::chrono::duration_cast<std::chrono::microseconds>(after_query_time).count();
  std::cerr << "After query time in micro s: " << after_query_count << std::endl;
    auto sum = before_query_time_count + before_chunk_time_count + read_time_count + compute_time_count + filter_time_count + aggregate_time_count + write_time_count + after_chunk_count + after_query_count;
  std::cerr << "Total time in micro s: " << diff_total << " ratio: " << static_cast<double>(sum) / diff_total << std::endl;


  return out_table;
}

std::shared_ptr<AbstractOperator> JitOperatorWrapper::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  if (Global::get().deep_copy_exists) const_cast<JitOperatorWrapper*>(this)->_choose_execute_func();
  return std::make_shared<JitOperatorWrapper>(copied_input_left, _execution_mode, _jit_operators,
                                              Global::get().deep_copy_exists ? _execute_func : nullptr);
}

void JitOperatorWrapper::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  _source()->set_parameters(parameters);
}

void JitOperatorWrapper::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  if (const auto row_count_expression = _source()->row_count_expression())
    expression_set_transaction_context(row_count_expression, transaction_context);
}

}  // namespace opossum
