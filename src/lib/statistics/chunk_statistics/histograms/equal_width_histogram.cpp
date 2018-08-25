#include "equal_width_histogram.hpp"

#include <memory>
#include <numeric>

#include "histogram_helper.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
EqualWidthHistogram<T>::EqualWidthHistogram(const T min, const T max, const std::vector<uint64_t>& counts,
                                            const std::vector<uint64_t>& distinct_counts,
                                            const uint64_t num_buckets_with_larger_range)
    : AbstractHistogram<T>(nullptr),
      _min(min),
      _max(max),
      _counts(counts),
      _distinct_counts(distinct_counts),
      _num_buckets_with_larger_range(num_buckets_with_larger_range) {}

template <>
EqualWidthHistogram<std::string>::EqualWidthHistogram(const std::string& min, const std::string& max,
                                                      const std::vector<uint64_t>& counts,
                                                      const std::vector<uint64_t>& distinct_counts,
                                                      const uint64_t num_buckets_with_larger_range,
                                                      const std::string& supported_characters,
                                                      const uint64_t string_prefix_length)
    : AbstractHistogram<std::string>(nullptr, supported_characters, string_prefix_length),
      _min(min),
      _max(max),
      _counts(counts),
      _distinct_counts(distinct_counts),
      _num_buckets_with_larger_range(num_buckets_with_larger_range) {}

template <typename T>
EqualWidthBucketStats<T> EqualWidthHistogram<T>::_get_bucket_stats(
    const std::vector<std::pair<T, uint64_t>>& value_counts, const size_t max_num_buckets) {
  // Buckets shall have the same range.
  const auto min = value_counts.front().first;
  const auto max = value_counts.back().first;
  const T base_width = next_value(max - min);

  // Never have more buckets than representable values.
  // TODO(anyone): fix for floats. This is more of a theoretical issue, however.
  auto num_buckets = max_num_buckets;
  if constexpr (std::is_integral_v<T>) {
    num_buckets = max_num_buckets <= static_cast<uint64_t>(base_width) ? max_num_buckets : base_width;
  }

  const T bucket_width = base_width / num_buckets;
  uint64_t num_buckets_with_larger_range;
  if constexpr (std::is_integral_v<T>) {
    num_buckets_with_larger_range = base_width % num_buckets;
  } else {
    num_buckets_with_larger_range = 0u;
  }

  std::vector<uint64_t> counts;
  std::vector<uint64_t> distinct_counts;
  T current_begin_value = min;
  auto current_begin_it = value_counts.cbegin();
  auto current_begin_index = 0l;
  for (auto current_bucket_id = 0u; current_bucket_id < num_buckets; current_bucket_id++) {
    T next_begin_value = current_begin_value + bucket_width;
    T current_end_value = previous_value(next_begin_value);

    if constexpr (std::is_integral_v<T>) {
      if (current_bucket_id < num_buckets_with_larger_range) {
        current_end_value++;
        next_begin_value++;
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      // This is intended to compensate for the fact that floating point arithmetic is not exact.
      // Adding up floating point numbers adds an error over time, and the more buckets there are, the larger it gets.
      // So this is how we make sure that the last bucket contains the rest of the values.
      if (current_bucket_id == num_buckets - 1) {
        current_end_value = max;
      }
    }

    // TODO(tim): think about replacing with binary search (same for other hists)
    auto next_begin_it = current_begin_it;
    while (next_begin_it != value_counts.cend() && (*next_begin_it).first <= current_end_value) {
      next_begin_it++;
    }

    const auto next_begin_index = std::distance(value_counts.cbegin(), next_begin_it);
    counts.emplace_back(std::accumulate(value_counts.cbegin() + current_begin_index,
                                        value_counts.cbegin() + next_begin_index, uint64_t{0},
                                        [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; }));
    distinct_counts.emplace_back(next_begin_index - current_begin_index);

    current_begin_value = next_begin_value;
    current_begin_index = next_begin_index;
  }

  return {min, max, counts, distinct_counts, num_buckets_with_larger_range};
}

template <>
EqualWidthBucketStats<std::string> EqualWidthHistogram<std::string>::_get_bucket_stats(
    const std::vector<std::pair<std::string, uint64_t>>& value_counts, const size_t max_num_buckets) {
  // TODO(tim): disable
  Fail("Not supported.");
}

template <>
EqualWidthBucketStats<std::string> EqualWidthHistogram<std::string>::_get_bucket_stats(
    const std::vector<std::pair<std::string, uint64_t>>& value_counts, const size_t max_num_buckets,
    const std::string& supported_characters, const uint64_t string_prefix_length) {
  // Buckets shall have the same range.
  const auto min = value_counts.front().first;
  const auto max = value_counts.back().first;

  const auto repr_min = convert_string_to_number_representation(min, supported_characters, string_prefix_length);
  const auto repr_max = convert_string_to_number_representation(max, supported_characters, string_prefix_length);

  // Never have more buckets than representable values.
  const auto num_buckets =
      max_num_buckets <= static_cast<uint64_t>(repr_max - repr_min + 1) ? max_num_buckets : repr_max - repr_min + 1;

  std::vector<uint64_t> counts;
  std::vector<uint64_t> distinct_counts;
  uint64_t num_buckets_with_larger_range;

  const auto num_min = convert_string_to_number_representation(min, supported_characters, string_prefix_length);
  const auto num_max = convert_string_to_number_representation(max, supported_characters, string_prefix_length);
  const auto base_width = num_max - num_min + 1;
  const auto bucket_width = base_width / num_buckets;

  num_buckets_with_larger_range = base_width % num_buckets;

  auto current_begin_value = min;
  auto current_begin_it = value_counts.cbegin();
  auto current_begin_index = 0l;
  for (auto current_bucket_id = 0u; current_bucket_id < num_buckets; current_bucket_id++) {
    auto num_current_begin_value =
        convert_string_to_number_representation(current_begin_value, supported_characters, string_prefix_length);
    auto next_begin_value = convert_number_representation_to_string(num_current_begin_value + bucket_width,
                                                                    supported_characters, string_prefix_length);
    auto current_end_value = previous_value(next_begin_value, supported_characters, string_prefix_length);

    if (current_bucket_id < num_buckets_with_larger_range) {
      current_end_value = next_begin_value;
      next_begin_value = next_value(next_begin_value, supported_characters, string_prefix_length);
    }

    // TODO(tim): think about replacing with binary search (same for other hists)
    auto next_begin_it = current_begin_it;
    while (next_begin_it != value_counts.cend() && (*next_begin_it).first <= current_end_value) {
      next_begin_it++;
    }

    const auto next_begin_index = std::distance(value_counts.cbegin(), next_begin_it);
    counts.emplace_back(std::accumulate(value_counts.begin() + current_begin_index,
                                        value_counts.begin() + next_begin_index, uint64_t{0},
                                        [](uint64_t a, std::pair<std::string, uint64_t> b) { return a + b.second; }));
    distinct_counts.emplace_back(next_begin_index - current_begin_index);

    current_begin_value = next_begin_value;
    current_begin_index = next_begin_index;
  }

  return {min, max, counts, distinct_counts, num_buckets_with_larger_range};
}

template <>
std::shared_ptr<EqualWidthHistogram<std::string>> EqualWidthHistogram<std::string>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_buckets,
    const std::string& supported_characters, const uint64_t string_prefix_length) {
  const auto value_counts =
      AbstractHistogram<std::string>::_calculate_value_counts(column, supported_characters, string_prefix_length);

  const auto bucket_stats = EqualWidthHistogram<std::string>::_get_bucket_stats(
      value_counts, max_num_buckets, supported_characters, string_prefix_length);

  return std::make_shared<EqualWidthHistogram<std::string>>(
      bucket_stats.min, bucket_stats.max, bucket_stats.counts, bucket_stats.distinct_counts,
      bucket_stats.num_buckets_with_larger_range, supported_characters, string_prefix_length);
}

template <>
std::shared_ptr<EqualWidthHistogram<std::string>> EqualWidthHistogram<std::string>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_buckets) {
  return EqualWidthHistogram<std::string>::from_column(
      column, max_num_buckets,
      " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~", 9);
}

template <typename T>
std::shared_ptr<EqualWidthHistogram<T>> EqualWidthHistogram<T>::from_column(
    const std::shared_ptr<const BaseColumn>& column, const size_t max_num_buckets) {
  const auto value_counts = AbstractHistogram<T>::_calculate_value_counts(column);

  if (value_counts.empty()) {
    return nullptr;
  }

  const auto bucket_stats = EqualWidthHistogram<T>::_get_bucket_stats(value_counts, max_num_buckets);

  return std::make_shared<EqualWidthHistogram<T>>(bucket_stats.min, bucket_stats.max, bucket_stats.counts,
                                                  bucket_stats.distinct_counts,
                                                  bucket_stats.num_buckets_with_larger_range);
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> EqualWidthHistogram<T>::clone() const {
  return std::make_shared<EqualWidthHistogram<T>>(_min, _max, _counts, _distinct_counts,
                                                  _num_buckets_with_larger_range);
}

template <>
std::shared_ptr<AbstractHistogram<std::string>> EqualWidthHistogram<std::string>::clone() const {
  return std::make_shared<EqualWidthHistogram<std::string>>(_min, _max, _counts, _distinct_counts,
                                                            _num_buckets_with_larger_range, _supported_characters,
                                                            _string_prefix_length);
}

template <typename T>
HistogramType EqualWidthHistogram<T>::histogram_type() const {
  return HistogramType::EqualWidth;
}

template <typename T>
size_t EqualWidthHistogram<T>::num_buckets() const {
  return _counts.size();
}

template <typename T>
uint64_t EqualWidthHistogram<T>::_bucket_count(const BucketID index) const {
  DebugAssert(index < _counts.size(), "Index is not a valid bucket.");
  return _counts[index];
}

template <typename T>
uint64_t EqualWidthHistogram<T>::total_count() const {
  return std::accumulate(_counts.begin(), _counts.end(), 0ul);
}

template <typename T>
uint64_t EqualWidthHistogram<T>::total_count_distinct() const {
  return std::accumulate(_distinct_counts.begin(), _distinct_counts.end(), 0ul);
}

template <typename T>
uint64_t EqualWidthHistogram<T>::_bucket_count_distinct(const BucketID index) const {
  DebugAssert(index < _distinct_counts.size(), "Index is not a valid bucket.");
  return _distinct_counts[index];
}

template <>
std::string EqualWidthHistogram<std::string>::_bucket_width(const BucketID /*index*/) const {
  Fail("Not supported for string histograms. Use _string_bucket_width instead.");
}

template <typename T>
T EqualWidthHistogram<T>::_bucket_width(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");

  const auto base_width = this->get_next_value(_max - _min) / this->num_buckets();

  if constexpr (std::is_integral_v<T>) {
    return base_width + (index < _num_buckets_with_larger_range ? 1 : 0);
  }

  return base_width;
}

template <>
uint64_t EqualWidthHistogram<std::string>::_string_bucket_width(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");

  const auto num_min = this->_convert_string_to_number_representation(_min);
  const auto num_max = this->_convert_string_to_number_representation(_max);
  const auto base_width = (num_max - num_min + 1) / this->num_buckets();
  return base_width + (index < _num_buckets_with_larger_range ? 1 : 0);
}

template <>
std::string EqualWidthHistogram<std::string>::_bucket_min(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");

  const auto num_min = this->_convert_string_to_number_representation(_min);
  const auto base_min = num_min + index * _string_bucket_width(index);
  return this->_convert_number_representation_to_string(base_min + std::min(index, _num_buckets_with_larger_range));
}

template <typename T>
T EqualWidthHistogram<T>::_bucket_min(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");

  const auto base_min = _min + index * _bucket_width(index);

  if constexpr (std::is_integral_v<T>) {
    return base_min + std::min(index, _num_buckets_with_larger_range);
  }

  return base_min;
}

template <typename T>
T EqualWidthHistogram<T>::_bucket_max(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");

  // If it's the last bucket, return max.
  if (index == num_buckets() - 1) {
    return _max;
  }

  // Otherwise it is the value just before the minimum of the next bucket.
  return this->get_previous_value(_bucket_min(index + 1));
}

template <typename T>
BucketID EqualWidthHistogram<T>::_bucket_for_value(const T value) const {
  if (value < _min || value > _max) {
    return INVALID_BUCKET_ID;
  }

  if (_num_buckets_with_larger_range == 0u || value <= _bucket_max(_num_buckets_with_larger_range - 1u)) {
    // All buckets up to that point have the exact same width, so we can use index 0.
    return (value - _min) / _bucket_width(0u);
  }

  // All buckets after that point have the exact same width as well, so we use that as the new base and add it up.
  return _num_buckets_with_larger_range +
         (value - _bucket_min(_num_buckets_with_larger_range)) / _bucket_width(_num_buckets_with_larger_range);
}

template <>
BucketID EqualWidthHistogram<std::string>::_bucket_for_value(const std::string value) const {
  DebugAssert(value.length() <= _string_prefix_length, "Value is longer than allowed prefix.");

  if (value < _min || value > _max) {
    return INVALID_BUCKET_ID;
  }

  const auto num_value = this->_convert_string_to_number_representation(value);

  if (_num_buckets_with_larger_range == 0u || value <= _bucket_max(_num_buckets_with_larger_range - 1u)) {
    const auto num_min = this->_convert_string_to_number_representation(_min);
    return (num_value - num_min) / _string_bucket_width(0u);
  }

  const auto num_base_min = this->_convert_string_to_number_representation(_bucket_min(_num_buckets_with_larger_range));
  return _num_buckets_with_larger_range +
         (num_value - num_base_min) / _string_bucket_width(_num_buckets_with_larger_range);
}

template <typename T>
BucketID EqualWidthHistogram<T>::_lower_bound_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  if (value < _min) {
    return 0u;
  }

  return _bucket_for_value(value);
}

template <typename T>
BucketID EqualWidthHistogram<T>::_upper_bound_for_value(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    DebugAssert(value.length() <= this->_string_prefix_length, "Value is longer than allowed prefix.");
  }

  if (value < _min) {
    return 0u;
  }

  const auto index = _bucket_for_value(value);
  return index < num_buckets() - 2 ? index + 1 : INVALID_BUCKET_ID;
}

template <typename T>
void EqualWidthHistogram<T>::_generate(const std::shared_ptr<const ValueColumn<T>> distinct_column,
                                       const std::shared_ptr<const ValueColumn<int64_t>> count_column,
                                       const size_t max_num_buckets) {
  // Buckets shall have the same range.
  _min = distinct_column->get(0);
  _max = distinct_column->get(distinct_column->size() - 1u);

  const auto num_buckets = max_num_buckets <= distinct_column->size() ? max_num_buckets : distinct_column->size();

  if constexpr (!std::is_same_v<T, std::string>) {
    const T base_width = _max - _min;
    const T bucket_width = this->get_next_value(base_width) / num_buckets;

    if constexpr (std::is_integral_v<T>) {
      _num_buckets_with_larger_range = (base_width + 1) % num_buckets;
    } else {
      _num_buckets_with_larger_range = 0u;
    }

    T current_begin_value = _min;
    auto current_begin_it = distinct_column->values().cbegin();
    auto current_begin_index = 0l;
    for (auto current_bucket_id = 0u; current_bucket_id < num_buckets; current_bucket_id++) {
      T next_begin_value = current_begin_value + bucket_width;
      T current_end_value = this->get_previous_value(next_begin_value);

      if constexpr (std::is_integral_v<T>) {
        if (current_bucket_id < _num_buckets_with_larger_range) {
          current_end_value++;
          next_begin_value++;
        }
      }

      // TODO(tim): think about replacing with binary search (same for other hists)
      auto next_begin_it = current_begin_it;
      while (next_begin_it != distinct_column->values().cend() && *next_begin_it <= current_end_value) {
        next_begin_it++;
      }

      const auto next_begin_index = std::distance(distinct_column->values().cbegin(), next_begin_it);
      _counts.emplace_back(std::accumulate(count_column->values().begin() + current_begin_index,
                                           count_column->values().begin() + next_begin_index, uint64_t{0}));
      _distinct_counts.emplace_back(next_begin_index - current_begin_index);

      current_begin_value = next_begin_value;
      current_begin_index = next_begin_index;
    }
  } else {
    const auto num_min = this->_convert_string_to_number_representation(_min);
    const auto num_max = this->_convert_string_to_number_representation(_max);
    const auto base_width = num_max - num_min + 1;
    const auto bucket_width = base_width / num_buckets;

    _num_buckets_with_larger_range = base_width % num_buckets;

    T current_begin_value = _min;
    auto current_begin_it = distinct_column->values().cbegin();
    auto current_begin_index = 0l;
    for (auto current_bucket_id = 0u; current_bucket_id < num_buckets; current_bucket_id++) {
      Assert(current_begin_value.find_first_not_of(this->_supported_characters) == std::string::npos,
             "Unsupported characters.");

      auto num_current_begin_value = this->_convert_string_to_number_representation(current_begin_value);
      T next_begin_value = this->_convert_number_representation_to_string(num_current_begin_value + bucket_width);
      T current_end_value = this->get_previous_value(next_begin_value);

      if (current_bucket_id < _num_buckets_with_larger_range) {
        current_end_value = next_begin_value;
        next_begin_value = this->get_next_value(next_begin_value);
      }

      Assert(current_end_value.find_first_not_of(this->_supported_characters) == std::string::npos,
             "Unsupported characters.");
      Assert(next_begin_value.find_first_not_of(this->_supported_characters) == std::string::npos,
             "Unsupported characters.");

      // TODO(tim): think about replacing with binary search (same for other hists)
      auto next_begin_it = current_begin_it;
      while (next_begin_it != distinct_column->values().cend() && *next_begin_it <= current_end_value) {
        next_begin_it++;
      }

      const auto next_begin_index = std::distance(distinct_column->values().cbegin(), next_begin_it);
      _counts.emplace_back(std::accumulate(count_column->values().begin() + current_begin_index,
                                           count_column->values().begin() + next_begin_index, uint64_t{0}));
      _distinct_counts.emplace_back(next_begin_index - current_begin_index);

      current_begin_value = next_begin_value;
      current_begin_index = next_begin_index;
    }
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualWidthHistogram);

}  // namespace opossum
