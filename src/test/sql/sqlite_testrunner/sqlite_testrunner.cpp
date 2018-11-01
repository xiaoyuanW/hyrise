#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/print.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sqlite_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class SQLiteTestRunner : public BaseTestWithParam<std::string> {
 public:
  static void SetUpTestCase() {  // called ONCE before the tests
    _sqlite = std::make_unique<SQLiteWrapper>();

    std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner.tables");
    std::string line;
    while (std::getline(file, line)) {
      if (line.empty()) {
        continue;
      }

      std::vector<std::string> args;
      boost::algorithm::split(args, line, boost::is_space());

      if (args.size() != 2) {
        continue;
      }

      std::string table_file = args.at(0);
      std::string table_name = args.at(1);

      // Store loaded tables in a map that basically caches the loaded tables. In case the table
      // needs to be reloaded (e.g., due to modifications), we also store the file path.
      _tables_to_test_map.emplace(table_name, TestTable{load_table(table_file, 10), table_file});

      // Prepare table copy which is later used as the master to copy from.
      _sqlite->create_table_from_tbl(table_file, table_name + _master_table_suffix);
    }

    opossum::Topology::use_numa_topology();
    opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());
  }

 protected:
  // Determines whether a table has been modified.
  bool is_table_modified(const std::shared_ptr<Table> table) const {
    // we iterate backwards, hoping for early exists in case of modifications
    for (auto it = table->chunks().rbegin(); it != table->chunks().rend(); ++it) {
      const auto& chunk = *it;
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      // Be aware that we cannot reach values smaller 0 with a size_t:
      // https://stackoverflow.com/questions/665745/whats-the-best-way-to-do-a-reverse-for-loop-with-an-unsigned-index
      for (auto row = size_t{chunk->size() - 1}; row + 1 > 0; --row) {
        const auto begin = mvcc_data->begin_cids[row];
        const auto end = mvcc_data->end_cids[row];
        const auto tid = mvcc_data->tids[row];

        if (begin != 0 || end != MvccData::MAX_COMMIT_ID || tid != 0) {
          return true;
        }
      }
    }
    return false;
  }

  void SetUp() override {  // called once before each test
    // For proper testing, we reset the storage manager before EVERY test.
    StorageManager::get().reset();

    for (auto const& [table_name, test_table] : _tables_to_test_map) {
      // Opossum: we add the cached tables to the storage manager and check if it has
      // been modified in the meanwhile (using the MVCC information). If so, load
      // table using the initial tbl files.
      // SQLite: drop table and copy the whole table from the master
      // table to reset all accessed tables.
      StorageManager::get().add_table(table_name, test_table.table);

      if (is_table_modified(test_table.table)) {
        StorageManager::get().drop_table(table_name);

        // 1. reload table from tbl file, 2. add table to storage manager, 3. cache table in map
        auto reloaded_table = load_table(test_table.filename, 10);
        StorageManager::get().add_table(table_name, reloaded_table);
        (_tables_to_test_map[table_name]).table = reloaded_table;
      }

      _sqlite->reset_table_from_copy(table_name, table_name + _master_table_suffix);
    }

    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  // Structure to cache initially loaded tables and store their file paths
  // to reload the the table from the given tbl file whenever required.
  struct TestTable {
    std::shared_ptr<Table> table;
    std::string filename;
  };

  inline static std::unique_ptr<SQLiteWrapper> _sqlite;
  inline static std::map<std::string, TestTable> _tables_to_test_map;
  inline static std::string _master_table_suffix = "_master_copy";
};

std::vector<std::string> read_queries_from_file() {
  std::vector<std::string> queries;

  std::ifstream file("src/test/sql/sqlite_testrunner/sqlite_testrunner_queries.sql");
  std::string query;
  while (std::getline(file, query)) {
    if (query.empty() || query.substr(0, 2) == "--") {
      continue;
    }
    queries.emplace_back(std::move(query));
  }

  return queries;
}

TEST_P(SQLiteTestRunner, CompareToSQLite) {
  const std::string query = GetParam();

  SCOPED_TRACE(query);

  const auto prepared_statement_cache = std::make_shared<PreparedStatementCache>();

  auto sql_pipeline =
      SQLPipelineBuilder{query}.with_prepared_statement_cache(prepared_statement_cache).create_pipeline();

  const auto& result_table = sql_pipeline.get_result_table();

  auto sqlite_result_table = _sqlite->execute_query(query);

  // The problem is that we can only infer column types from sqlite if they have at least one row.
  ASSERT_TRUE(result_table && result_table->row_count() > 0 && sqlite_result_table &&
              sqlite_result_table->row_count() > 0)
      << "The SQLiteTestRunner cannot handle queries without results";

  auto order_sensitivity = OrderSensitivity::No;

  const auto& parse_result = sql_pipeline.get_parsed_sql_statements().back();
  if (parse_result->getStatements().front()->is(hsql::kStmtSelect)) {
    auto select_statement = dynamic_cast<const hsql::SelectStatement*>(parse_result->getStatements().back());
    if (select_statement->order != nullptr) {
      order_sensitivity = OrderSensitivity::Yes;
    }
  }

  ASSERT_TRUE(check_table_equal(result_table, sqlite_result_table, order_sensitivity, TypeCmpMode::Lenient,
                                FloatComparisonMode::RelativeDifference))
      << "Query failed: " << query;
}

INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerInstances, SQLiteTestRunner,
                        testing::ValuesIn(read_queries_from_file()), );  // NOLINT

}  // namespace opossum
