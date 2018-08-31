#include "equal_num_elements_histogram.hpp"

#include <memory>
#include <numeric>

#include "histogram_utils.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
EqualNumElementsHistogram<T>::EqualNumElementsHistogram(const std::vector<T>& mins, const std::vector<T>& maxs,
                                                        const std::vector<uint64_t>& counts,
                                                        const uint64_t distinct_count_per_bin,
                                                        const uint64_t num_bins_with_extra_value)
    : AbstractHistogram<T>(nullptr),
      _mins(mins),
      _maxs(maxs),
      _counts(counts),
      _distinct_count_per_bin(distinct_count_per_bin),
      _num_bins_with_extra_value(num_bins_with_extra_value) {}

template <>
EqualNumElementsHistogram<std::string>::EqualNumElementsHistogram(
    const std::vector<std::string>& mins, const std::vector<std::string>& maxs, const std::vector<uint64_t>& counts,
    const uint64_t distinct_count_per_bin, const uint64_t num_bins_with_extra_value,
    const std::string& supported_characters, const uint64_t string_prefix_length)
    : AbstractHistogram<std::string>(nullptr, supported_characters, string_prefix_length),
      _mins(mins),
      _maxs(maxs),
      _counts(counts),
      _distinct_count_per_bin(distinct_count_per_bin),
      _num_bins_with_extra_value(num_bins_with_extra_value) {}

template <typename T>
EqualNumElementsBinStats<T> EqualNumElementsHistogram<T>::_get_bin_stats(
    const std::vector<std::pair<T, uint64_t>>& value_counts, const size_t max_num_bins) {
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto distinct_count = value_counts.size();
  const auto num_bins = distinct_count < max_num_bins ? static_cast<size_t>(distinct_count) : max_num_bins;

  // Split values evenly among bins.
  const auto distinct_count_per_bin = distinct_count / num_bins;
  const auto num_bins_with_extra_value = distinct_count % num_bins;

  std::vector<T> mins;
  std::vector<T> maxs;
  std::vector<uint64_t> counts;

  auto begin_index = 0ul;
  for (BinID bin_index = 0; bin_index < num_bins; bin_index++) {
    auto end_index = begin_index + distinct_count_per_bin - 1;
    if (bin_index < num_bins_with_extra_value) {
      end_index++;
    }

    const auto current_min = value_counts[begin_index].first;
    const auto current_max = value_counts[end_index].first;

    mins.emplace_back(current_min);
    maxs.emplace_back(current_max);
    counts.emplace_back(std::accumulate(value_counts.cbegin() + begin_index, value_counts.cbegin() + end_index + 1,
                                        uint64_t{0},
                                        [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; }));

    begin_index = end_index + 1;
  }

  return {mins, maxs, counts, distinct_count_per_bin, num_bins_with_extra_value};
}

template <>
std::shared_ptr<EqualNumElementsHistogram<std::string>> EqualNumElementsHistogram<std::string>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_bins, const std::string& supported_characters,
    const uint64_t string_prefix_length) {
  Assert(supported_characters.length() > 1, "String range must consist of more than one character.");
  Assert(ipow(supported_characters.length() + 1, string_prefix_length) < ipow(2ul, 63ul), "Prefix too long.");

  for (auto it = supported_characters.begin(); it < supported_characters.end(); it++) {
    Assert(std::distance(supported_characters.begin(), it) == *it - supported_characters.front(),
           "Non-consecutive or unordered string ranges are not supported.");
  }

  const auto value_counts =
      AbstractHistogram<std::string>::_calculate_value_counts(column, supported_characters, string_prefix_length);

  const auto bin_stats = EqualNumElementsHistogram<std::string>::_get_bin_stats(value_counts, max_num_bins);

  return std::make_shared<EqualNumElementsHistogram<std::string>>(
      bin_stats.mins, bin_stats.maxs, bin_stats.counts, bin_stats.distinct_count_per_bin,
      bin_stats.num_bins_with_extra_value, supported_characters, string_prefix_length);
}

template <>
std::shared_ptr<EqualNumElementsHistogram<std::string>> EqualNumElementsHistogram<std::string>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_bins) {
  return EqualNumElementsHistogram<std::string>::from_column(
      column, max_num_bins,
      " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~", 9);
}

template <typename T>
std::shared_ptr<EqualNumElementsHistogram<T>> EqualNumElementsHistogram<T>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_bins) {
  const auto value_counts = AbstractHistogram<T>::_calculate_value_counts(column);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bin_stats = EqualNumElementsHistogram<T>::_get_bin_stats(value_counts, max_num_bins);

  return std::make_shared<EqualNumElementsHistogram<T>>(bin_stats.mins, bin_stats.maxs, bin_stats.counts,
                                                        bin_stats.distinct_count_per_bin,
                                                        bin_stats.num_bins_with_extra_value);
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> EqualNumElementsHistogram<T>::clone() const {
  return std::make_shared<EqualNumElementsHistogram<T>>(_mins, _maxs, _counts, _distinct_count_per_bin,
                                                        _num_bins_with_extra_value);
}

template <>
std::shared_ptr<AbstractHistogram<std::string>> EqualNumElementsHistogram<std::string>::clone() const {
  return std::make_shared<EqualNumElementsHistogram<std::string>>(_mins, _maxs, _counts, _distinct_count_per_bin,
                                                                  _num_bins_with_extra_value, _supported_characters,
                                                                  _string_prefix_length);
}

template <typename T>
HistogramType EqualNumElementsHistogram<T>::histogram_type() const {
  return HistogramType::EqualNumElements;
}

template <typename T>
size_t EqualNumElementsHistogram<T>::num_bins() const {
  return _counts.size();
}

template <typename T>
BinID EqualNumElementsHistogram<T>::_bin_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BinID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end() || value < _bin_min(index) || value > _bin_max(index)) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualNumElementsHistogram<T>::_lower_bound_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BinID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
BinID EqualNumElementsHistogram<T>::_upper_bound_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  const auto it = std::upper_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BinID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BIN_ID;
  }

  return index;
}

template <typename T>
T EqualNumElementsHistogram<T>::_bin_min(const BinID index) const {
  DebugAssert(index < _mins.size(), "Index is not a valid bin.");
  return _mins[index];
}

template <typename T>
T EqualNumElementsHistogram<T>::_bin_max(const BinID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bin.");
  return _maxs[index];
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::_bin_count(const BinID index) const {
  DebugAssert(index < _counts.size(), "Index is not a valid bin.");
  return _counts[index];
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::_bin_count_distinct(const BinID index) const {
  return _distinct_count_per_bin + (index < _num_bins_with_extra_value ? 1 : 0);
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::total_count() const {
  return std::accumulate(_counts.begin(), _counts.end(), 0ul);
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::total_count_distinct() const {
  return _distinct_count_per_bin * num_bins() + _num_bins_with_extra_value;
}

template <typename T>
void EqualNumElementsHistogram<T>::_generate(const std::shared_ptr<const ValueColumn<T>> distinct_column,
                                             const std::shared_ptr<const ValueColumn<int64_t>> count_column,
                                             const size_t max_num_bins) {
  // If there are fewer distinct values than the number of desired bins use that instead.
  const auto distinct_count = distinct_column->size();
  const auto num_bins = distinct_count < max_num_bins ? static_cast<size_t>(distinct_count) : max_num_bins;

  // Split values evenly among bins.
  _distinct_count_per_bin = distinct_count / num_bins;
  _num_bins_with_extra_value = distinct_count % num_bins;

  auto begin_index = 0ul;
  for (BinID bin_index = 0; bin_index < num_bins; bin_index++) {
    auto end_index = begin_index + _distinct_count_per_bin - 1;
    if (bin_index < _num_bins_with_extra_value) {
      end_index++;
    }

    const auto current_min = *(distinct_column->values().cbegin() + begin_index);
    const auto current_max = *(distinct_column->values().cbegin() + end_index);

    if constexpr (std::is_same_v<T, std::string>) {
      Assert(current_min.find_first_not_of(this->_supported_characters) == std::string::npos,
             "Unsupported characters.");
      Assert(current_max.find_first_not_of(this->_supported_characters) == std::string::npos,
             "Unsupported characters.");
    }

    _mins.emplace_back(current_min);
    _maxs.emplace_back(current_max);
    _counts.emplace_back(std::accumulate(count_column->values().cbegin() + begin_index,
                                         count_column->values().cbegin() + end_index + 1, uint64_t{0}));

    begin_index = end_index + 1;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualNumElementsHistogram);

}  // namespace opossum
