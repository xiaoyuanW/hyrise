#include <iostream>

#include "resolve_type.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/pos_list.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

class AbstractSingleColumnTableScanImpl {
 public:
  AbstractSingleColumnTableScanImpl(const std::shared_ptr<const Table>& table, const ColumnID column_id)
      : _table(table), _column_id(column_id) {}

  std::shared_ptr<PosList> scan() {
    auto results = std::make_shared<PosList>();
    for (auto chunk_id = ChunkID{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
      scan(chunk_id, results);
    }
    return results;
  }

  void scan(const ChunkID chunk_id, std::shared_ptr<PosList> results) {
    // TODO Resolve ReferenceSegments to their separate segments
    _on_scan(chunk_id, results);
  }

 protected:
  const std::shared_ptr<const Table>& _table;
  const ColumnID _column_id;

  virtual void _on_scan(const ChunkID chunk_id, std::shared_ptr<PosList> results) = 0;
};

class LiteralTableScan : public AbstractSingleColumnTableScanImpl {
 public:
  LiteralTableScan(const std::shared_ptr<const Table>& table, const ColumnID column_id)
      : AbstractSingleColumnTableScanImpl(table, column_id) {}

 protected:
  void scan_segment(const BaseSegment& segment) {
    std::cout << "generic LiteralTableScan on " << typeid(segment).name() << std::endl;
  }

  void scan_segment(const BaseDictionarySegment& segment) {
    std::cout << "LiteralTableScan on DictionarySegment" << std::endl;
  }

  void _on_scan(const ChunkID chunk_id, std::shared_ptr<PosList> results) override {
    // This block will get copied into every scan impl. While copying things is bad, the alternative would be CRTP.
    // Starting that just to save two lines seems to be overkill.

    resolve_data_and_segment_type(*_table->get_chunk(chunk_id)->get_segment(_column_id),
                                  [&](const auto type, const auto& segment) { scan_segment(segment); });
  }
};

int main() {
  auto vs_int = make_shared_by_data_type<BaseValueSegment, ValueSegment>(DataType::Int);
  auto ds_int = encode_segment(EncodingType::Dictionary, DataType::Int, vs_int);

  TableColumnDefinitions column_definitions{{"column_1", DataType::Int}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  table->append_chunk({vs_int});
  table->append_chunk({ds_int});

  LiteralTableScan{table, ColumnID{0}}.scan();

  // Output:
  // generic LiteralTableScan on N7opossum12ValueSegmentIiEE
  // LiteralTableScan on DictionarySegment

  return 0;
}
