#include "equal_height_histogram.hpp"

#include <memory>
#include <numeric>

#include "histogram_utils.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
EqualHeightHistogram<T>::EqualHeightHistogram(const std::vector<T>& maxs, const std::vector<uint64_t>& distinct_counts,
                                              T min, const uint64_t total_count)
    : AbstractHistogram<T>(), _maxs(maxs), _distinct_counts(distinct_counts), _min(min), _total_count(total_count) {}

template <>
EqualHeightHistogram<std::string>::EqualHeightHistogram(const std::vector<std::string>& maxs,
                                                        const std::vector<uint64_t>& distinct_counts,
                                                        const std::string& min, const uint64_t total_count,
                                                        const std::string& supported_characters,
                                                        const uint64_t string_prefix_length)
    : AbstractHistogram<std::string>(supported_characters, string_prefix_length),
      _maxs(maxs),
      _distinct_counts(distinct_counts),
      _min(min),
      _total_count(total_count) {
  for (const auto& edge : maxs) {
    Assert(edge.find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
  }
}

template <typename T>
EqualHeightBinStats<T> EqualHeightHistogram<T>::_get_bin_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                                               const size_t max_num_bins) {
  const auto min = value_counts.front().first;
  const auto num_bins = max_num_bins <= value_counts.size() ? max_num_bins : value_counts.size();

  // Bins shall have (approximately) the same height.
  const auto total_count = std::accumulate(value_counts.cbegin(), value_counts.cend(), uint64_t{0},
                                           [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
  auto count_per_bin = total_count / num_bins;

  if (total_count % num_bins > 0u) {
    // Make sure that we never create more bins than requested.
    count_per_bin++;
  }

  std::vector<T> maxs;
  std::vector<uint64_t> distinct_counts;
  maxs.reserve(num_bins);
  distinct_counts.reserve(num_bins);

  auto current_begin = 0u;
  auto current_height = 0u;
  for (auto current_end = 0u; current_end < value_counts.size(); current_end++) {
    current_height += value_counts[current_end].second;

    if (current_height >= count_per_bin) {
      const auto current_value = value_counts[current_end].first;

      maxs.emplace_back(current_value);
      distinct_counts.emplace_back(current_end - current_begin + 1);
      current_height = 0u;
      current_begin = current_end + 1;
    }
  }

  if (current_height > 0u) {
    const auto current_value = value_counts.back().first;

    maxs.emplace_back(current_value);
    distinct_counts.emplace_back(value_counts.size() - current_begin);
  }

  return {maxs, distinct_counts, min, total_count};
}

template <typename T>
std::shared_ptr<EqualHeightHistogram<T>> EqualHeightHistogram<T>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_bins,
    const std::optional<std::string>& supported_characters, const std::optional<uint64_t>& string_prefix_length) {
  std::string characters;
  uint64_t prefix_length;
  if constexpr (std::is_same_v<T, std::string>) {
    const auto pair = AbstractHistogram<T>::_get_or_check_prefix_settings(supported_characters, string_prefix_length);
    characters = pair.first;
    prefix_length = pair.second;
  }

  const auto value_counts = AbstractHistogram<T>::_calculate_value_counts(column);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bin_stats = EqualHeightHistogram<T>::_get_bin_stats(value_counts, max_num_bins);

  if constexpr (std::is_same_v<T, std::string>) {
    return std::make_shared<EqualHeightHistogram<T>>(bin_stats.maxs, bin_stats.distinct_counts, bin_stats.min,
                                                     bin_stats.total_count, characters, prefix_length);
  } else {
    return std::make_shared<EqualHeightHistogram<T>>(bin_stats.maxs, bin_stats.distinct_counts, bin_stats.min,
                                                     bin_stats.total_count);
  }
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> EqualHeightHistogram<T>::clone() const {
  return std::make_shared<EqualHeightHistogram<T>>(_maxs, _distinct_counts, _min, _total_count);
}

template <>
std::shared_ptr<AbstractHistogram<std::string>> EqualHeightHistogram<std::string>::clone() const {
  return std::make_shared<EqualHeightHistogram<std::string>>(_maxs, _distinct_counts, _min, _total_count,
                                                             _supported_characters, _string_prefix_length);
}

template <typename T>
HistogramType EqualHeightHistogram<T>::histogram_type() const {
  return HistogramType::EqualHeight;
}

template <typename T>
size_t EqualHeightHistogram<T>::num_bins() const {
  return _maxs.size();
}

template <typename T>
BinID EqualHeightHistogram<T>::_bin_for_value(const T value) const {
  if (value < _min) {
    return INVALID_BIN_ID;
  }

  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);

  if (it == _maxs.end()) {
    return INVALID_BIN_ID;
  }

  return static_cast<BinID>(std::distance(_maxs.begin(), it));
}

template <typename T>
BinID EqualHeightHistogram<T>::_upper_bound_for_value(const T value) const {
  const auto it = std::upper_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BinID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
T EqualHeightHistogram<T>::_bin_min(const BinID index) const {
  DebugAssert(index < this->num_bins(), "Index is not a valid bin.");

  // If it's the first bin, return _min.
  if (index == 0u) {
    return _min;
  }

  return this->get_next_value(this->_bin_max(index - 1));
}

template <typename T>
T EqualHeightHistogram<T>::_bin_max(const BinID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bin.");
  return _maxs[index];
}

template <typename T>
uint64_t EqualHeightHistogram<T>::_bin_count(const BinID index) const {
  DebugAssert(index < this->num_bins(), "Index is not a valid bin.");
  // Rather estimate more than less.
  return total_count() / num_bins() + (total_count() % num_bins() > 0 ? 1 : 0);
}

template <typename T>
uint64_t EqualHeightHistogram<T>::_bin_count_distinct(const BinID index) const {
  DebugAssert(index < _distinct_counts.size(), "Index is not a valid bin.");
  return _distinct_counts[index];
}

template <typename T>
uint64_t EqualHeightHistogram<T>::total_count() const {
  return _total_count;
}

template <typename T>
uint64_t EqualHeightHistogram<T>::total_count_distinct() const {
  return std::accumulate(_distinct_counts.begin(), _distinct_counts.end(), 0ul);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualHeightHistogram);

}  // namespace opossum
