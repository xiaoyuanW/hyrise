#include "jit_optimal_operator.hpp"

#include <functional>

#include "abstract_read_only_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "operators/jit_operator/jit_operations.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_read_value.hpp"
#include "operators/jit_operator/operators/jit_write_offset.hpp"
#include "storage/reference_segment.hpp"
#include "storage/storage_manager.hpp"
#include "validate.hpp"

namespace opossum {

const std::string JitOptimalOperator::name() const { return "JitOperatorWrapper"; }

std::shared_ptr<const Table> JitOptimalOperator::_on_execute() {
  const auto left_table = StorageManager::get().get_table("TABLE_SCAN");
  const auto right_table = StorageManager::get().get_table("TABLE_AGGREGATE");

  JitRuntimeHashmap hashmap;
  std::vector<std::vector<RowID>> row_ids;

  {
    JitRuntimeContext context;
    if (transaction_context_is_set()) {
      context.transaction_id = transaction_context()->transaction_id();
      context.snapshot_commit_id = transaction_context()->snapshot_commit_id();
    }
    auto read_tuples = JitReadTuples(true);
    const auto col_a = right_table->column_id_by_name("A");
    const auto col_x = right_table->column_id_by_name("X100000");
    constexpr auto a_id = 0;
    read_tuples.add_input_column(DataType::Int, false, col_a, false);
    constexpr auto x_id = 1;
    const auto x_tpl = read_tuples.add_input_column(DataType::Int, false, col_x, false);
    // const auto l_id = read_tuples.add_literal_value(0);
    read_tuples.before_query(*right_table, context);

    auto jit_aggregate = JitAggregate();
    jit_aggregate.add_groupby_column("x", x_tpl);
    const auto group_by_col = jit_aggregate.groupby_columns().front();

    auto expected_entries = left_table->row_count() * 0.06f;

    hashmap.columns.resize(1);
    hashmap.indices = std::unordered_map<uint64_t, std::vector<size_t>>(expected_entries);
    row_ids.reserve(expected_entries);

    for (opossum::ChunkID chunk_id{0}; chunk_id < right_table->chunk_count(); ++chunk_id) {
      read_tuples.before_chunk(*right_table, chunk_id, context);

      for (; context.chunk_offset < context.chunk_size; ++context.chunk_offset) {
        for (const auto input : context.inputs) {
          input->read_value(context);
        }

        const auto row_tid = context.mvcc_data->tids[context.chunk_offset].load();
        const auto begin_cid = context.mvcc_data->begin_cids[context.chunk_offset];
        const auto end_cid = context.mvcc_data->end_cids[context.chunk_offset];
        const auto our_tid = context.transaction_id;
        const auto snapshot_commit_id = context.snapshot_commit_id;
        bool is_visible = Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid);
        if (!is_visible) {
          continue;
        }
        bool filter = context.tuple.get<int>(a_id) > 0;
        if (!filter) {
          continue;
        }
        uint64_t hash_value = std::hash<int>()(context.tuple.get<int>(x_id));
        auto& hash_bucket = hashmap.indices[hash_value];

        bool found_match = false;
        uint64_t row_index;

        for (const auto& index : hash_bucket) {
          // Compare all values of the row to the currently consumed tuple.
          bool equal = context.tuple.get<int>(x_id) == hashmap.columns[0].get<int>(index);
          if (equal) {
            found_match = true;
            row_index = index;
            break;
          }
        }

        if (!found_match) {
          row_index = hashmap.columns[0].grow_by_one<int>(JitVariantVector::InitialValue::Zero);
          hashmap.columns[0].set(row_index, context.tuple.get<int>(x_id));
          hash_bucket.emplace_back(row_index);
          row_ids.push_back({RowID(context.chunk_id, context.chunk_offset)});
        } else {
          row_ids[row_index].emplace_back(context.chunk_id, context.chunk_offset);
        }
      }
    }
  }

  {
    JitRuntimeContext context;
    if (transaction_context_is_set()) {
      context.transaction_id = transaction_context()->transaction_id();
      context.snapshot_commit_id = transaction_context()->snapshot_commit_id();
    }
    auto read_tuples = JitReadTuples(true);
    const auto col_a = left_table->column_id_by_name("A");
    const auto col_x = left_table->column_id_by_name("ID");
    constexpr auto a_id = 0;
    read_tuples.add_input_column(DataType::Int, false, col_a, false);
    constexpr auto x_id = 1;
    const auto x_tpl = read_tuples.add_input_column(DataType::Int, false, col_x, false);
    // const auto l_id = read_tuples.add_literal_value(0);
    read_tuples.before_query(*left_table, context);

    auto jit_aggregate = JitAggregate();
    jit_aggregate.add_groupby_column("x", x_tpl);
    const auto group_by_col = jit_aggregate.groupby_columns().front();

    const auto in_table = left_table;
    auto write = JitWriteOffset();
    JitOutputReferenceColumn ref_col{"T1_ID", DataType::Int, false, ColumnID(0)};
    write.add_output_column(ref_col);
    JitOutputReferenceColumn ref_col2{"T2_ID", DataType::Int, false, ColumnID(0)};
    write.add_output_column(ref_col2);
    auto out_table = write.create_output_table(in_table->max_chunk_size());

    for (opossum::ChunkID chunk_id{0}; chunk_id < left_table->chunk_count(); ++chunk_id) {
      Segments out_segments;
      context.output_pos_list.clear();
      float expected_size = left_table->get_chunk(chunk_id)->size() * 0.2f;
      context.output_pos_list.reserve(expected_size);

      auto output_pos_list2 = std::make_shared<PosList>();
      output_pos_list2->reserve(expected_size);

      read_tuples.before_chunk(*left_table, chunk_id, context);

      for (; context.chunk_offset < context.chunk_size; ++context.chunk_offset) {
        for (const auto input : context.inputs) {
          input->read_value(context);
        }

        const auto row_tid = context.mvcc_data->tids[context.chunk_offset].load();
        const auto begin_cid = context.mvcc_data->begin_cids[context.chunk_offset];
        const auto end_cid = context.mvcc_data->end_cids[context.chunk_offset];
        const auto our_tid = context.transaction_id;
        const auto snapshot_commit_id = context.snapshot_commit_id;
        bool is_visible = Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid);
        if (!is_visible) {
          continue;
        }
        bool filter = context.tuple.get<int>(a_id) < 5000;
        if (!filter) {
          continue;
        }
        uint64_t hash_value = std::hash<int>()(context.tuple.get<int>(x_id));
        if (!hashmap.indices.count(hash_value)) {
          continue;
        }
        auto& hash_bucket = hashmap.indices[hash_value];

        bool found_match = false;
        uint64_t row_index;

        for (const auto& index : hash_bucket) {
          // Compare all values of the row to the currently consumed tuple.
          bool equal = context.tuple.get<int>(x_id) == hashmap.columns[0].get<int>(index);
          if (equal) {
            found_match = true;
            row_index = index;
            break;
          }
        }

        if (!found_match) {
          continue;
        }

        for (const auto id : row_ids[row_index]) {
          context.output_pos_list.emplace_back(context.chunk_id, context.chunk_offset);
          output_pos_list2->emplace_back(id);
        }
      }
      auto output_pos_list = std::make_shared<PosList>();
      output_pos_list->resize(context.output_pos_list.size());
      std::copy(context.output_pos_list.cbegin(), context.output_pos_list.cend(), output_pos_list->begin());
      auto ref_segment_out =
          std::make_shared<ReferenceSegment>(in_table, ref_col.referenced_column_id, output_pos_list);
      auto ref_segment_out2 =
          std::make_shared<ReferenceSegment>(right_table, ref_col2.referenced_column_id, output_pos_list2);
      out_segments.push_back(ref_segment_out);
      out_segments.push_back(ref_segment_out2);
      out_table->append_chunk(out_segments);
    }
    return out_table;
  }
}

}  // namespace opossum
