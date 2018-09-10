#include "segment_statistics.hpp"

#include <algorithm>
#include <iterator>
#include <type_traits>
#include <unordered_set>

#include "resolve_type.hpp"

#include "abstract_filter.hpp"
#include "histograms/equal_num_elements_histogram.hpp"
#include "min_max_filter.hpp"
#include "range_filter.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
static std::shared_ptr<SegmentStatistics> build_statistics_from_dictionary(const pmr_vector<T>& dictionary) {
  auto statistics = std::make_shared<SegmentStatistics>();
  // only create statistics when the compressed dictionary is not empty
  if (!dictionary.empty()) {
    // no range filter for strings
    // clang-format off
    if constexpr(std::is_arithmetic_v<T>) {
      auto range_filter = RangeFilter<T>::build_filter(dictionary);
      statistics->add_filter(std::move(range_filter));
    } else {
      // we only need the min-max filter if we cannot have a range filter
      auto min_max_filter = std::make_unique<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
      statistics->add_filter(std::move(min_max_filter));
    }
    // clang-format on
  }
  return statistics;
}

std::shared_ptr<SegmentStatistics> SegmentStatistics::build_statistics(
    DataType data_type, const std::shared_ptr<const BaseSegment>& segment) {
  auto statistics = std::make_shared<SegmentStatistics>();

  resolve_data_and_segment_type(*segment, [&statistics, &segment](auto type, auto& typed_segment) {
    using SegmentType = typename std::decay<decltype(typed_segment)>::type;
    using DataTypeT = typename decltype(type)::type;

    /**
     * Derive the number of bins from the number of distinct elements iff we have a DictColumn.
     * This number is currently arbitrarily chosen.
     * TODO(tim): benchmark
     * Otherwise, take the default of 100 bins.
     */
    size_t num_bins = 100u;
    if constexpr (std::is_same_v<SegmentType, DictionarySegment<DataTypeT>>) {
      const size_t proposed_bins = typed_segment.dictionary()->size() / 25;
      num_bins = std::max(num_bins, proposed_bins);
    }

    statistics->add_filter(EqualNumElementsHistogram<DataTypeT>::from_segment(segment, num_bins));
  });

  return statistics;
}

void SegmentStatistics::add_filter(std::shared_ptr<AbstractFilter> filter) { _filters.emplace_back(filter); }

bool SegmentStatistics::can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                  const std::optional<AllTypeVariant>& variant_value2) const {
  for (const auto& filter : _filters) {
    if (filter->can_prune(predicate_type, variant_value, variant_value2)) {
      return true;
    }
  }
  return false;
}
}  // namespace opossum
