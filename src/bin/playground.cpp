#include <cstdlib>
#include <iostream>
#if HYRISE_JIT_SUPPORT
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "global.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "operators/jit_operator_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT
#endif

int main() {
#if HYRISE_JIT_SUPPORT
  auto table_a = opossum::load_table("src/test/tables/int_float.tbl", 2);
  opossum::StorageManager::get().add_table("table_a", table_a);

  auto table_b = opossum::load_table("src/test/tables/int.tbl", 1000);
  opossum::StorageManager::get().add_table("tmp", table_b);

  auto table_c = opossum::load_table("src/test/tables/int3.tbl", 1000);
  opossum::StorageManager::get().add_table("tmp2", table_c);

  auto& global = opossum::Global::get();
  global.jit = true;
  global.lazy_load = true;
  global.jit_validate = true;

  auto context = opossum::TransactionManager::get().new_transaction_context();
  auto get_table = std::make_shared<opossum::GetTable>("tmp");
  // get_table->set_transaction_context(context);
  get_table->execute();

  const auto output_table = get_table->get_output();
  const auto column_id = opossum::ColumnID{0};
  const auto& column_definition = output_table->column_definitions().at(column_id);

  const auto column_expression = expression_functional::pqp_column_(column_id, column_definition.data_type,
                                                                    column_definition.nullable, column_definition.name);

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, column_expression,
                                                               expression_functional::value_(200));
  auto table_scan = std::make_shared<opossum::TableScan>(get_table, predicate);
  // table_scan->set_transaction_context(context);
  table_scan->execute();
  auto delete_op = std::make_shared<opossum::Delete>("tmp", table_scan);
  delete_op->set_transaction_context(context);
  delete_op->execute();
  context->commit();

  context = opossum::TransactionManager::get().new_transaction_context();
  predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThan, column_expression,
                                                          expression_functional::value_(10000));
  table_scan = std::make_shared<opossum::TableScan>(get_table, predicate);
  // table_scan->set_transaction_context(context);
  table_scan->execute();
  delete_op = std::make_shared<opossum::Delete>("tmp", table_scan);
  delete_op->set_transaction_context(context);
  delete_op->execute();

  auto gt2 = std::make_shared<opossum::GetTable>("tmp2");
  gt2->execute();
  auto ins = std::make_shared<opossum::Insert>("tmp", gt2);
  ins->set_transaction_context(context);
  ins->execute();

  auto print_before = std::make_shared<opossum::Print>(get_table);
  print_before->execute();
  std::cout << std::endl;

  /* get_table = std::make_shared<opossum::GetTable>("tmp");
  get_table->set_transaction_context(context);
  get_table->execute(); */

  predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, column_expression,
                                                          expression_functional::value_(0));
  auto filter = std::make_shared<opossum::TableScan>(get_table, predicate);
  filter->execute();

  // auto validate = std::make_shared<opossum::Validate>(get_table);
  // validate->set_transaction_context(context);
  auto jit_operator =
      std::make_shared<opossum::JitOperatorWrapper>(filter, opossum::JitExecutionMode::Compile);  // Interpret validate
  auto read_tuple = std::make_shared<opossum::JitReadTuples>(true);
  opossum::JitTupleValue tuple_val = read_tuple->add_input_column(opossum::DataType::Int, false, opossum::ColumnID(0));
  jit_operator->add_jit_operator(read_tuple);
  jit_operator->add_jit_operator(std::make_shared<opossum::JitValidate<TableType::References>>());

  auto id = read_tuple->add_temporary_value();

  auto expression = std::make_shared<opossum::JitExpression>(std::make_shared<opossum::JitExpression>(tuple_val),
                                                             opossum::JitExpressionType::Addition,
                                                             std::make_shared<opossum::JitExpression>(tuple_val), id);

  auto compute = std::make_shared<opossum::JitCompute>(expression);

  jit_operator->add_jit_operator(compute);

  auto tuple_value = expression->result();

  auto write_table = std::make_shared<opossum::JitWriteTuples>();
  write_table->add_output_column("a", tuple_value);  // tuple_value
  jit_operator->add_jit_operator(write_table);

  jit_operator->set_transaction_context(context);
  auto print = std::make_shared<opossum::Print>(jit_operator);
  print->set_transaction_context(context);
  // validate->execute();
  jit_operator->execute();
  print->execute();
  context->commit();
#endif
  return 0;
}
