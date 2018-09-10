#pragma once

#include <unordered_set>

#include "base_column_statistics.hpp"
#include "chunk_statistics/histograms/equal_num_elements_histogram.hpp"
// #include "histogram_column_statistics.hpp"
#include "minimal_column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * Generate the statistics of a single column. Used by generate_table_statistics().
 * If num_bin is set, create statistics of type HistogramColumnStatistics, otherwise create basic statistics.
 */
template <typename ColumnDataType>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics(const std::shared_ptr<const Table>& table,
                                                                 const ColumnID column_id) {
  std::unordered_set<ColumnDataType> distinct_set;

  auto null_value_count = size_t{0};

  auto min = std::numeric_limits<ColumnDataType>::max();
  auto max = std::numeric_limits<ColumnDataType>::lowest();

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto base_segment = table->get_chunk(chunk_id)->get_segment(column_id);

    resolve_segment_type<ColumnDataType>(*base_segment, [&](auto& segment) {
      auto iterable = create_iterable_from_segment<ColumnDataType>(segment);
      iterable.for_each([&](const auto& segment_value) {
        if (segment_value.is_null()) {
          ++null_value_count;
        } else {
          distinct_set.insert(segment_value.value());
          min = std::min(min, segment_value.value());
          max = std::max(max, segment_value.value());
        }
      });
    });
  }

  const auto null_value_ratio =
      table->row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table->row_count()) : 0.0f;
  const auto distinct_count = static_cast<float>(distinct_set.size());

  if (distinct_count == 0.0f) {
    min = std::numeric_limits<ColumnDataType>::min();
    max = std::numeric_limits<ColumnDataType>::max();
  }

  return std::make_shared<MinimalColumnStatistics<ColumnDataType>>(null_value_ratio, distinct_count, min, max);
}

template <>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics<std::string>(const std::shared_ptr<const Table>& table,
                                                                              const ColumnID column_id);

// TODO(tim): this needs to create the column wrapper that aggregates chunk-level statistics
// template <typename ColumnDataType>
// std::shared_ptr<BaseColumnStatistics> generate_column_statistics(const std::shared_ptr<const Table>& table,
//                                                                  const ColumnID column_id, const int64_t num_bins) {
//   auto histogram = std::make_shared<EqualNumElementsHistogram<ColumnDataType>>(table);
//   histogram->generate(column_id, num_bins > 0 ? static_cast<size_t>(num_bins) : 100u);
//
//   // TODO(tim): incorporate into Histograms
//   const auto null_value_ratio = 0.0f;
//   return std::make_shared<HistogramColumnStatistics<ColumnDataType>>(histogram, null_value_ratio);
// }

}  // namespace opossum
