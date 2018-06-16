#include "product.hpp"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/reference_column.hpp"

namespace opossum {
Product::Product(const std::shared_ptr<const AbstractOperator> left,
                 const std::shared_ptr<const AbstractOperator> right)
    : AbstractReadOnlyOperator(OperatorType::Product, left, right) {}

const std::string Product::name() const { return "Product"; }

std::shared_ptr<const Table> Product::_on_execute() {
  TableColumnDefinitions column_definitions;

  // add columns from left table to output
  for (ColumnID column_id{0}; column_id < input_table_left()->column_count(); ++column_id) {
    column_definitions.emplace_back(input_table_left()->column_definitions()[column_id]);
  }

  // add columns from right table to output
  for (ColumnID column_id{0}; column_id < input_table_right()->column_count(); ++column_id) {
    column_definitions.emplace_back(input_table_right()->column_definitions()[column_id]);
  }

  auto output = std::make_shared<Table>(column_definitions, TableType::References);

  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < input_table_left()->chunk_count(); ++chunk_id_left) {
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < input_table_right()->chunk_count(); ++chunk_id_right) {
      add_product_of_two_chunks(output, chunk_id_left, chunk_id_right);
    }
  }

  return output;
}

void Product::add_product_of_two_chunks(std::shared_ptr<Table> output, ChunkID chunk_id_left, ChunkID chunk_id_right) {
  const auto chunk_left = input_table_left()->get_chunk(chunk_id_left);
  const auto chunk_right = input_table_right()->get_chunk(chunk_id_right);

  // we use an approach here in which we do not have nested loops for left and right but create both sides separately
  // When the result looks like this:
  //   l1 r1
  //   l1 r2
  //   l1 r3
  //   l2 r1
  //   l2 r2
  //   l2 r3
  // we can first repeat each line on the left side #rightSide times and then repeat the ascending sequence for the
  // right side #leftSide times

  std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> calculated_pos_lists_left;
  std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> calculated_pos_lists_right;

  ChunkColumns output_columns;
  auto is_left_side = true;

  for (const auto& chunk_in : {chunk_left, chunk_right}) {
    // reusing the same code for left and right side - using a reference_wrapper is ugly, but better than code
    // duplication
    auto table = is_left_side ? input_table_left() : input_table_right();

    for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
      std::shared_ptr<const Table> referenced_table;
      ColumnID referenced_column;
      std::shared_ptr<const PosList> pos_list_in;

      if (auto ref_col_in = std::dynamic_pointer_cast<const ReferenceColumn>(chunk_in->get_column(column_id))) {
        referenced_table = ref_col_in->referenced_table();
        referenced_column = ref_col_in->referenced_column_id();
        pos_list_in = ref_col_in->pos_list();
      } else {
        referenced_table = is_left_side ? input_table_left() : input_table_right();
        referenced_column = column_id;
      }

      // see if we can reuse a PosList that we already calculated - important to use a reference here so that the map
      // gets updated accordingly
      auto& pos_list_out = (is_left_side ? calculated_pos_lists_left : calculated_pos_lists_right)[pos_list_in];
      if (!pos_list_out) {
        // can't reuse
        pos_list_out = std::make_shared<PosList>();
        pos_list_out->reserve(chunk_left->size() * chunk_right->size());
        for (size_t i = 0; i < chunk_left->size() * chunk_right->size(); ++i) {
          // size_t is sufficient here, because ChunkOffset::max is 2^32 and (2^32 * 2^32 = 2^64)
          ChunkOffset offset = is_left_side ? (i / chunk_right->size()) : (i % chunk_right->size());
          if (pos_list_in) {
            pos_list_out->emplace_back((*pos_list_in)[offset]);
          } else {
            pos_list_out->emplace_back(RowID{is_left_side ? chunk_id_left : chunk_id_right, offset});
          }
        }
      }
      output_columns.push_back(std::make_shared<ReferenceColumn>(referenced_table, referenced_column, pos_list_out));
    }

    is_left_side = false;
  }

  output->append_chunk(output_columns);
}
std::shared_ptr<AbstractOperator> Product::_on_recreate(
    const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<Product>(recreated_input_left, recreated_input_right);
}
}  // namespace opossum
