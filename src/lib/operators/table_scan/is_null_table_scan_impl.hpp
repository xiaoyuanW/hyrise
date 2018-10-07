#pragma once

#include <functional>
#include <memory>

#include "base_single_column_table_scan_impl.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;
class BaseValueSegment;

class IsNullTableScanImpl : public BaseSingleColumnTableScanImpl {
 public:
  IsNullTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID base_column_id,
                      const PredicateCondition& predicate_condition);

  void handle_segment(const ReferenceSegment& base_segment,
                      std::shared_ptr<SegmentVisitorContext> base_context) override;

  void handle_segment(const BaseValueSegment& base_segment,
                      std::shared_ptr<SegmentVisitorContext> base_context) override;

  void handle_segment(const BaseDictionarySegment& base_segment,
                      std::shared_ptr<SegmentVisitorContext> base_context) override;

  void handle_segment(const BaseEncodedSegment& base_segment,
                      std::shared_ptr<SegmentVisitorContext> base_context) override;

  using BaseSingleColumnTableScanImpl::handle_segment;

 private:
  /**
   * @defgroup Methods used for handling value segments
   * @{
   */

  bool _matches_all(const BaseValueSegment& segment);

  bool _matches_none(const BaseValueSegment& segment);

  void _add_all(Context& context, size_t segment_size);

  /**@}*/

};

}  // namespace opossum
