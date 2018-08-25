#include "equal_height_histogram.hpp"

#include <memory>
#include <numeric>

#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
EqualHeightHistogram<T>::EqualHeightHistogram(const std::vector<T>& maxs, const std::vector<uint64_t>& distinct_counts,
                                              T min, const uint64_t total_count)
    : AbstractHistogram<T>(nullptr),
      _maxs(maxs),
      _distinct_counts(distinct_counts),
      _min(min),
      _total_count(total_count) {}

template <>
EqualHeightHistogram<std::string>::EqualHeightHistogram(const std::vector<std::string>& maxs,
                                                        const std::vector<uint64_t>& distinct_counts,
                                                        const std::string& min, const uint64_t total_count,
                                                        const std::string& supported_characters,
                                                        const uint64_t string_prefix_length)
    : AbstractHistogram<std::string>(nullptr, supported_characters, string_prefix_length),
      _maxs(maxs),
      _distinct_counts(distinct_counts),
      _min(min),
      _total_count(total_count) {}

template <typename T>
EqualHeightBucketStats<T> EqualHeightHistogram<T>::_get_bucket_stats(
    const std::vector<std::pair<T, uint64_t>>& value_counts, const size_t max_num_buckets) {
  const auto min = value_counts.front().first;
  const auto num_buckets = max_num_buckets <= value_counts.size() ? max_num_buckets : value_counts.size();

  // Buckets shall have (approximately) the same height.
  const auto total_count = std::accumulate(value_counts.cbegin(), value_counts.cend(), uint64_t{0},
                                           [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
  auto count_per_bucket = total_count / num_buckets;

  if (total_count % num_buckets > 0u) {
    // Make sure that we never create more buckets than requested.
    count_per_bucket++;
  }

  std::vector<T> maxs;
  std::vector<uint64_t> distinct_counts;

  auto current_begin = 0u;
  auto current_height = 0u;
  for (auto current_end = 0u; current_end < value_counts.size(); current_end++) {
    current_height += value_counts[current_end].second;

    if (current_height >= count_per_bucket) {
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

template <>
std::shared_ptr<EqualHeightHistogram<std::string>> EqualHeightHistogram<std::string>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_buckets,
    const std::string& supported_characters, const uint64_t string_prefix_length) {
  const auto value_counts =
      AbstractHistogram<std::string>::_calculate_value_counts(column, supported_characters, string_prefix_length);

  const auto bucket_stats = EqualHeightHistogram<std::string>::_get_bucket_stats(value_counts, max_num_buckets);

  return std::make_shared<EqualHeightHistogram<std::string>>(bucket_stats.maxs, bucket_stats.distinct_counts,
                                                             bucket_stats.min, bucket_stats.total_count,
                                                             supported_characters, string_prefix_length);
}

template <>
std::shared_ptr<EqualHeightHistogram<std::string>> EqualHeightHistogram<std::string>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_buckets) {
  return EqualHeightHistogram<std::string>::from_column(
      column, max_num_buckets,
      " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~", 9);
}

template <typename T>
std::shared_ptr<EqualHeightHistogram<T>> EqualHeightHistogram<T>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_buckets) {
  const auto value_counts = AbstractHistogram<T>::_calculate_value_counts(column);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bucket_stats = EqualHeightHistogram<T>::_get_bucket_stats(value_counts, max_num_buckets);

  return std::make_shared<EqualHeightHistogram<T>>(bucket_stats.maxs, bucket_stats.distinct_counts, bucket_stats.min,
                                                   bucket_stats.total_count);
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
size_t EqualHeightHistogram<T>::num_buckets() const {
  return _maxs.size();
}

template <typename T>
BucketID EqualHeightHistogram<T>::_bucket_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);

  if (it == _maxs.end() || value < _min) {
    return INVALID_BUCKET_ID;
  }

  return static_cast<BucketID>(std::distance(_maxs.begin(), it));
}

template <typename T>
BucketID EqualHeightHistogram<T>::_lower_bound_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
BucketID EqualHeightHistogram<T>::_upper_bound_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  const auto it = std::upper_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
T EqualHeightHistogram<T>::_bucket_min(const BucketID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bucket.");

  // If it's the first bucket, return _min.
  if (index == 0u) {
    return _min;
  }

  return this->get_next_value(this->_bucket_max(index - 1));
}

template <typename T>
T EqualHeightHistogram<T>::_bucket_max(const BucketID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bucket.");
  return _maxs[index];
}

template <typename T>
uint64_t EqualHeightHistogram<T>::_bucket_count(const BucketID index) const {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  // Rather estimate more than less.
  return total_count() / num_buckets() + (total_count() % num_buckets() > 0 ? 1 : 0);
}

template <typename T>
uint64_t EqualHeightHistogram<T>::_bucket_count_distinct(const BucketID index) const {
  DebugAssert(index < _distinct_counts.size(), "Index is not a valid bucket.");
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

template <typename T>
void EqualHeightHistogram<T>::_generate(const std::shared_ptr<const ValueColumn<T>> distinct_column,
                                        const std::shared_ptr<const ValueColumn<int64_t>> count_column,
                                        const size_t max_num_buckets) {
  _min = distinct_column->get(0u);

  auto table = this->_table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");

  const auto num_buckets = max_num_buckets <= distinct_column->size() ? max_num_buckets : distinct_column->size();

  // Buckets shall have (approximately) the same height.
  _total_count = table->row_count();
  auto count_per_bucket = _total_count / num_buckets;

  if (_total_count % num_buckets > 0u) {
    // Add 1 so that we never create more buckets than requested.
    count_per_bucket++;
  }

  auto current_begin = 0u;
  auto current_height = 0u;
  for (auto current_end = 0u; current_end < distinct_column->size(); current_end++) {
    current_height += count_column->get(current_end);

    if (current_height >= count_per_bucket) {
      const auto current_value = distinct_column->get(current_end);

      if constexpr (std::is_same_v<T, std::string>) {
        Assert(current_value.find_first_not_of(this->_supported_characters) == std::string::npos,
               "Unsupported characters.");
      }

      _maxs.emplace_back(current_value);
      _distinct_counts.emplace_back(current_end - current_begin + 1);
      current_height = 0u;
      current_begin = current_end + 1;
    }
  }

  if (current_height > 0u) {
    const auto current_value = distinct_column->get(distinct_column->size() - 1);

    if constexpr (std::is_same_v<T, std::string>) {
      Assert(current_value.find_first_not_of(this->_supported_characters) == std::string::npos,
             "Unsupported characters.");
    }

    _maxs.emplace_back(current_value);
    _distinct_counts.emplace_back(distinct_column->size() - current_begin);
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualHeightHistogram);

}  // namespace opossum
