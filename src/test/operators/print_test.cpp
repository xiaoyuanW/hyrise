#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorsPrintTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int);
    column_definitions.emplace_back("column_2", DataType::String);
    _t = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size);
    StorageManager::get().add_table(_table_name, _t);

    _gt = std::make_shared<GetTable>(_table_name);
    _gt->execute();
  }

  std::ostringstream output;

  const std::string _table_name = "printTestTable";

  uint32_t _chunk_size = 10;

  std::shared_ptr<GetTable> _gt;
  std::shared_ptr<Table> _t = nullptr;
};

// class used to make protected methods visible without
// modifying the base class with testing code.
class PrintWrapper : public Print {
  std::shared_ptr<const Table> tab;

 public:
  explicit PrintWrapper(const std::shared_ptr<AbstractOperator> in) : Print(in), tab(in->get_output()) {}
  explicit PrintWrapper(const std::shared_ptr<AbstractOperator> in, std::ostream& out, uint32_t flags)
      : Print(in, out, flags), tab(in->get_output()) {}

  std::vector<uint16_t> test_column_string_widths(uint16_t min, uint16_t max) {
    return _column_string_widths(min, max, tab);
  }

  std::string test_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) {
    return _truncate_cell(cell, max_width);
  }

  uint16_t get_max_cell_width() { return _max_cell_width; }

  bool is_printing_empty_chunks() { return !(_flags & PrintIgnoreEmptyChunks); }

  bool is_printing_mvcc_information() { return _flags & PrintMvcc; }
};

TEST_F(OperatorsPrintTest, TableColumnDefinitions) {
  auto pr = std::make_shared<Print>(_gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), _t);

  auto output_string = output.str();

  // rather hard-coded tests
  EXPECT_TRUE(output_string.find("column_1") != std::string::npos);
  EXPECT_TRUE(output_string.find("column_2") != std::string::npos);
  EXPECT_TRUE(output_string.find("int") != std::string::npos);
  EXPECT_TRUE(output_string.find("string") != std::string::npos);
}

TEST_F(OperatorsPrintTest, FilledTable) {
  const size_t chunk_count = 117;
  auto tab = StorageManager::get().get_table(_table_name);
  for (size_t i = 0; i < _chunk_size * chunk_count; i++) {
    // char 97 is an 'a'. Modulo 26 to stay within the alphabet.
    tab->append({static_cast<int>(i % _chunk_size), std::string(1, 97 + static_cast<int>(i / _chunk_size) % 26)});
  }

  auto pr = std::make_shared<Print>(_gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), tab);

  auto output_string = output.str();

  // check the line count of the output string
  size_t line_count = std::count(output_string.begin(), output_string.end(), '\n');
  size_t expected_line_count = 4 + 11 * chunk_count;  // 4 header lines + all 10-line chunks with chunk header
  EXPECT_EQ(line_count, expected_line_count);

  EXPECT_TRUE(output_string.find("Chunk 0") != std::string::npos);
  auto non_existing_chunk_header = std::string("Chunk ").append(std::to_string(chunk_count));
  EXPECT_TRUE(output_string.find(non_existing_chunk_header) == std::string::npos);

  // remove spaces for some simple tests
  output_string.erase(remove_if(output_string.begin(), output_string.end(), isspace), output_string.end());
  EXPECT_TRUE(output_string.find("|9|b|") != std::string::npos);
  EXPECT_TRUE(output_string.find("|7|z|") != std::string::npos);
  EXPECT_TRUE(output_string.find("|10|a|") == std::string::npos);
}

TEST_F(OperatorsPrintTest, GetColumnWidths) {
  uint16_t min = 8;
  uint16_t max = 20;

  auto tab = StorageManager::get().get_table(_table_name);

  auto pr_wrap = std::make_shared<PrintWrapper>(_gt);
  auto print_lengths = pr_wrap->test_column_string_widths(min, max);

  // we have two columns, thus two 'lengths'
  ASSERT_EQ(print_lengths.size(), static_cast<size_t>(2));
  // with empty columns and short column names, we should see the minimal lengths
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(min));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(min));

  int ten_digits_ints = 1234567890;

  tab->append({ten_digits_ints, "quite a long string with more than $max chars"});

  print_lengths = pr_wrap->test_column_string_widths(min, max);
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(10));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(max));
}

TEST_F(OperatorsPrintTest, OperatorName) {
  auto pr = std::make_shared<opossum::Print>(_gt, output);

  EXPECT_EQ(pr->name(), "Print");
}

TEST_F(OperatorsPrintTest, TruncateLongValue) {
  auto print_wrap = std::make_shared<PrintWrapper>(_gt);

  auto cell = AllTypeVariant{"abcdefghijklmnopqrstuvwxyz"};

  auto truncated_cell_20 = print_wrap->test_truncate_cell(cell, 20);
  EXPECT_EQ(truncated_cell_20, "abcdefghijklmnopq...");

  auto truncated_cell_30 = print_wrap->test_truncate_cell(cell, 30);
  EXPECT_EQ(truncated_cell_30, "abcdefghijklmnopqrstuvwxyz");

  auto truncated_cell_10 = print_wrap->test_truncate_cell(cell, 10);
  EXPECT_EQ(truncated_cell_10, "abcdefg...");
}

TEST_F(OperatorsPrintTest, TruncateLongValueInOutput) {
  auto print_wrap = std::make_shared<PrintWrapper>(_gt);
  auto tab = StorageManager::get().get_table(_table_name);

  std::string cell_string = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
  auto input = AllTypeVariant{cell_string};

  tab->append({0, input});

  auto substr_length = std::min(static_cast<int>(cell_string.length()), print_wrap->get_max_cell_width() - 3);

  std::string manual_substring = "|";
  for (uint16_t i = 0; i < substr_length; i++) {
    manual_substring += cell_string.at(i);
  }
  manual_substring += "...|";

  auto wrap = std::make_shared<TableWrapper>(tab);
  wrap->execute();

  auto printer = std::make_shared<Print>(wrap, output);
  printer->execute();

  auto output_string = output.str();
  EXPECT_TRUE(output_string.find(manual_substring) != std::string::npos);
}

TEST_F(OperatorsPrintTest, EmptyChunkFlag) {
  // Start off by adding an empty chunk
  auto tab = StorageManager::get().get_table(_table_name);
  Segments empty_segments;
  auto column_1 = std::make_shared<opossum::ValueSegment<int>>();
  auto column_2 = std::make_shared<opossum::ValueSegment<std::string>>();
  empty_segments.push_back(column_1);
  empty_segments.push_back(column_2);
  tab->append_chunk(empty_segments);

  auto wrap = std::make_shared<TableWrapper>(tab);
  wrap->execute();

  // Flags = 0 is the default. As such, empty chunks will be printed.
  std::ostringstream output_withempty;
  auto print_wrap_withempty = PrintWrapper(wrap, output_withempty, 0);
  print_wrap_withempty.execute();

  auto expected_output_withempty =
      "=== Columns\n"
      "|column_1|column_2|\n"
      "|     int|  string|\n"
      "|not null|not null|\n"
      "=== Chunk 0 ===\n"
      "Empty chunk.\n";

  EXPECT_EQ(output_withempty.str(), expected_output_withempty);
  EXPECT_TRUE(print_wrap_withempty.is_printing_empty_chunks());
  EXPECT_FALSE(print_wrap_withempty.is_printing_mvcc_information());

  // And now skip empty chunks.
  std::ostringstream output_noempty;
  auto print_wrap_noempty = PrintWrapper(wrap, output_noempty, 1);
  print_wrap_noempty.execute();

  auto expected_output_noempty =
      "=== Columns\n"
      "|column_1|column_2|\n"
      "|     int|  string|\n"
      "|not null|not null|\n";

  EXPECT_EQ(output_noempty.str(), expected_output_noempty);
  EXPECT_FALSE(print_wrap_noempty.is_printing_empty_chunks());
  EXPECT_FALSE(print_wrap_noempty.is_printing_mvcc_information());
}

TEST_F(OperatorsPrintTest, MVCCFlag) {
  auto print_wrap = PrintWrapper(_gt, output, 2);
  print_wrap.execute();

  auto expected_output =
      "=== Columns\n"
      "|column_1|column_2||        MVCC        |\n"
      "|     int|  string||_BEGIN|_END  |_TID  |\n"
      "|not null|not null||      |      |      |\n";

  EXPECT_EQ(output.str(), expected_output);
  EXPECT_TRUE(print_wrap.is_printing_empty_chunks());
  EXPECT_TRUE(print_wrap.is_printing_mvcc_information());
}

TEST_F(OperatorsPrintTest, AllFlags) {
  auto print_wrap = PrintWrapper(_gt, output, 3);
  print_wrap.execute();

  EXPECT_FALSE(print_wrap.is_printing_empty_chunks());
  EXPECT_TRUE(print_wrap.is_printing_mvcc_information());
}

TEST_F(OperatorsPrintTest, MVCCTableLoad) {
  // Per default, MVCC data is created when loading tables.
  // This test passes the flag for printing MVCC information, which is not printed by default.
  std::shared_ptr<TableWrapper> table = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
  table->execute();

  Print::print(table, 2, output);

  auto expected_output =
      "=== Columns\n"
      "|       a|       b||        MVCC        |\n"
      "|     int|   float||_BEGIN|_END  |_TID  |\n"
      "|not null|not null||      |      |      |\n"
      "=== Chunk 0 ===\n"
      "|   12345|   458.7||     0|      |      |\n"
      "|     123|   456.7||     0|      |      |\n"
      "=== Chunk 1 ===\n"
      "|    1234|   457.7||     0|      |      |\n";
  EXPECT_EQ(output.str(), expected_output);
}

TEST_F(OperatorsPrintTest, DirectInstantiations) {
  // We expect the same output from both instantiations.
  auto expected_output =
      "=== Columns\n"
      "|column_1|column_2|\n"
      "|     int|  string|\n"
      "|not null|not null|\n";

  std::ostringstream output_ss_op_inst;
  Print::print(_gt, 0, output_ss_op_inst);
  EXPECT_EQ(output_ss_op_inst.str(), expected_output);

  std::ostringstream output_ss_tab_inst;
  Print::print(_t, 0, output_ss_tab_inst);
  EXPECT_EQ(output_ss_tab_inst.str(), expected_output);
}

}  // namespace opossum
