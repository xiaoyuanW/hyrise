#include "abstract_histogram.hpp"

#include <cmath>

#include <algorithm>
#include <memory>
#include <vector>

#include "boost/algorithm/string/replace.hpp"

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "histogram_helper.hpp"
#include "operators/aggregate.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"

namespace opossum {

template <typename T>
AbstractHistogram<T>::AbstractHistogram(const std::shared_ptr<const Table>& table)
    : _table(table), _supported_characters(""), _string_prefix_length(0ul) {}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::shared_ptr<const Table>& table)
    : _table(table),
      _supported_characters(
          " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~") {
  _string_prefix_length =
      static_cast<uint64_t>(std::log(ipow(2ul, 63ul)) / std::log(_supported_characters.length() + 1));
}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::shared_ptr<const Table>& table,
                                                  const std::string& supported_characters)
    : _table(table) {
  Assert(supported_characters.length() > 1, "String range must consist of more than one character.");

  _supported_characters = supported_characters;
  std::sort(_supported_characters.begin(), _supported_characters.end());

  for (auto it = _supported_characters.begin(); it < _supported_characters.end(); it++) {
    Assert(std::distance(_supported_characters.begin(), it) == *it - _supported_characters.front(),
           "Non-consecutive string ranges are not supported.");
  }

  _string_prefix_length =
      static_cast<uint64_t>(std::log(ipow(2ul, 63ul)) / std::log(_supported_characters.length() + 1));
}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::shared_ptr<const Table>& table,
                                                  const std::string& supported_characters,
                                                  const uint64_t string_prefix_length)
    : _table(table), _string_prefix_length(string_prefix_length) {
  Assert(string_prefix_length > 0, "Invalid prefix length.");
  Assert(supported_characters.length() > 1, "String range must consist of more than one character.");
  Assert(ipow(supported_characters.length() + 1, string_prefix_length) < ipow(2ul, 63ul), "Prefix too long.");

  _supported_characters = supported_characters;
  std::sort(_supported_characters.begin(), _supported_characters.end());

  for (auto it = _supported_characters.begin(); it < _supported_characters.end(); it++) {
    Assert(std::distance(_supported_characters.begin(), it) == *it - _supported_characters.front(),
           "Non-consecutive string ranges are not supported.");
  }
}

template <typename T>
std::string AbstractHistogram<T>::description() const {
  std::stringstream stream;
  stream << histogram_type_to_string.at(histogram_type()) << std::endl;
  stream << "  distinct    " << total_count_distinct() << std::endl;
  stream << "  min         " << min() << std::endl;
  stream << "  max         " << max() << std::endl;
  // TODO(tim): consider non-null ration in histograms
  // stream << "  non-null " << non_null_value_ratio() << std::endl;
  stream << "  buckets     " << num_buckets() << std::endl;

  stream << "  boundaries / counts " << std::endl;
  for (auto bucket = 0u; bucket < num_buckets(); bucket++) {
    stream << "              [" << _bucket_min(bucket) << ", " << _bucket_max(bucket) << "]: ";
    stream << _bucket_count(bucket) << std::endl;
  }

  return stream.str();
}

template <typename T>
std::string AbstractHistogram<T>::buckets_to_csv(const bool print_header, const std::optional<std::string>& column_name,
                                                 const std::optional<uint64_t>& requested_num_buckets) const {
  std::stringstream stream;

  if (print_header) {
    stream << "histogram_type";

    if (column_name) {
      stream << ",column_name";
    }

    stream << ",actual_num_buckets";

    if (requested_num_buckets) {
      stream << ",requested_num_buckets";
    }

    stream << ",bucket_id,bucket_min,bucket_max,bucket_min_repr,bucket_max_repr,bucket_count,bucket_count_distinct";
    stream << std::endl;
  }

  for (auto bucket = 0u; bucket < num_buckets(); bucket++) {
    stream << histogram_type_to_string.at(histogram_type());

    if (column_name) {
      stream << "," << *column_name;
    }

    stream << "," << num_buckets();

    if (requested_num_buckets) {
      stream << "," << *requested_num_buckets;
    }

    stream << "," << bucket;

    if constexpr (std::is_same_v<T, std::string>) {
      constexpr auto patterns = std::array<std::pair<const char*, const char*>, 2u>{{{"\\", "\\\\"}, {"\"", "\\\""}}};
      auto min = _bucket_min(bucket);
      auto max = _bucket_max(bucket);

      for (const auto& pair : patterns) {
        boost::replace_all(min, pair.first, pair.second);
        boost::replace_all(max, pair.first, pair.second);
      }

      stream << ",\"" << min << "\"";
      stream << ",\"" << max << "\"";
      stream << "," << _convert_string_to_number_representation(_bucket_min(bucket));
      stream << "," << _convert_string_to_number_representation(_bucket_max(bucket));
    } else {
      stream << "," << _bucket_min(bucket);
      stream << "," << _bucket_max(bucket);
      stream << "," << _bucket_min(bucket);
      stream << "," << _bucket_max(bucket);
    }

    stream << "," << _bucket_count(bucket);
    stream << "," << _bucket_count_distinct(bucket);
    stream << std::endl;
  }

  return stream.str();
}

template <>
const std::string& AbstractHistogram<std::string>::supported_characters() const {
  return _supported_characters;
}

template <typename T>
const std::shared_ptr<const Table> AbstractHistogram<T>::_get_value_counts(const ColumnID column_id) const {
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto aggregate_args = std::vector<AggregateColumnDefinition>{{std::nullopt, AggregateFunction::Count}};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<ColumnID>{column_id});
  aggregate->execute();

  auto sort = std::make_shared<Sort>(aggregate, ColumnID{0});
  sort->execute();

  return sort->get_output();
}

template <>
const std::shared_ptr<const Table> AbstractHistogram<std::string>::_get_value_counts(const ColumnID column_id) const {
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto col_expression =
      std::make_shared<PQPColumnExpression>(column_id, table->column_data_type(column_id),
                                            table->column_is_nullable(column_id), table->column_name(column_id));
  const auto substr_expression =
      opossum::expression_functional::substr_(col_expression, 1, static_cast<int>(_string_prefix_length));
  auto projection =
      std::make_shared<Projection>(table_wrapper, std::vector<std::shared_ptr<AbstractExpression>>{substr_expression});
  projection->execute();

  const auto aggregate_args = std::vector<AggregateColumnDefinition>{{std::nullopt, AggregateFunction::Count}};
  auto aggregate = std::make_shared<Aggregate>(projection, aggregate_args, std::vector<ColumnID>{ColumnID{0}});
  aggregate->execute();

  auto sort = std::make_shared<Sort>(aggregate, ColumnID{0});
  sort->execute();

  return sort->get_output();
}

template <typename T>
std::vector<std::pair<T, uint64_t>> AbstractHistogram<T>::_sort_value_counts(
    const std::unordered_map<T, uint64_t>& value_counts) {
  std::vector<std::pair<T, uint64_t>> result(value_counts.cbegin(), value_counts.cend());

  std::sort(result.begin(), result.end(),
            [](const std::pair<T, uint64_t>& lhs, const std::pair<T, uint64_t>& rhs) { return lhs.first < rhs.first; });

  return result;
}

template <typename T>
std::vector<std::pair<T, uint64_t>> AbstractHistogram<T>::_calculate_value_counts(
    const std::shared_ptr<const BaseColumn>& column) {
  // TODO(anyone): reserve size based on dictionary, if possible
  std::unordered_map<T, uint64_t> value_counts;
  // TODO(tim): incorporate null values
  uint64_t nulls = 0;

  resolve_column_type<T>(*column, [&](auto& typed_column) {
    auto iterable = create_iterable_from_column<T>(typed_column);
    iterable.for_each([&](const auto& value) {
      if (value.is_null()) {
        nulls++;
      } else {
        value_counts[value.value()]++;
      }
    });
  });

  return AbstractHistogram<T>::_sort_value_counts(value_counts);
}

template <>
std::vector<std::pair<std::string, uint64_t>> AbstractHistogram<std::string>::_calculate_value_counts(
    const std::shared_ptr<const BaseColumn>& column, const std::string& supported_characters,
    const uint64_t string_prefix_length) {
  // TODO(anyone): reserve size based on dictionary, if possible
  std::unordered_map<std::string, uint64_t> value_counts;
  // TODO(tim): incorporate null values
  uint64_t nulls = 0;

  resolve_column_type<std::string>(*column, [&](auto& typed_column) {
    auto iterable = create_iterable_from_column<std::string>(typed_column);
    iterable.for_each([&](const auto& value) {
      if (value.is_null()) {
        nulls++;
      } else {
        Assert(value.value().find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
        value_counts[value.value().substr(0, string_prefix_length)]++;
      }
    });
  });

  return AbstractHistogram<std::string>::_sort_value_counts(value_counts);
}

template <typename T>
void AbstractHistogram<T>::generate(const ColumnID column_id, const size_t max_num_buckets) {
  DebugAssert(max_num_buckets > 0u, "Cannot generate histogram with less than one bucket.");

  const auto result = _get_value_counts(column_id);
  if (result->row_count() == 0u) {
    return;
  }

  // TODO(tim): fix
  DebugAssert(result->chunk_count() == 1u, "Multiple chunks are currently not supported.");

  const auto distinct_column =
      std::static_pointer_cast<const ValueColumn<T>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  const auto count_column =
      std::static_pointer_cast<const ValueColumn<int64_t>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{1}));

  _generate(distinct_column, count_column, max_num_buckets);
}

template <typename T>
T AbstractHistogram<T>::min() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return _bucket_min(0u);
}

template <typename T>
T AbstractHistogram<T>::max() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return _bucket_max(num_buckets() - 1u);
}

template <>
std::string AbstractHistogram<std::string>::_bucket_width(const BucketID /*index*/) const {
  Fail("Method not supported for string histograms.");
}

template <typename T>
T AbstractHistogram<T>::_bucket_width(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");
  return get_next_value(_bucket_max(index) - _bucket_min(index));
}

template <>
std::string AbstractHistogram<std::string>::get_previous_value(const std::string value) const {
  return previous_value(value, _supported_characters, _string_prefix_length);
}

template <typename T>
T AbstractHistogram<T>::get_previous_value(const T value) const {
  return previous_value(value);
}

template <>
std::string AbstractHistogram<std::string>::get_next_value(const std::string value) const {
  return next_value(value, _supported_characters, _string_prefix_length);
}

template <typename T>
T AbstractHistogram<T>::get_next_value(const T value) const {
  return next_value(value);
}

template <>
uint64_t AbstractHistogram<std::string>::_convert_string_to_number_representation(const std::string& value) const {
  return convert_string_to_number_representation(value, _supported_characters, _string_prefix_length);
}

template <>
std::string AbstractHistogram<std::string>::_convert_number_representation_to_string(const uint64_t value) const {
  return convert_number_representation_to_string(value, _supported_characters, _string_prefix_length);
}

template <typename T>
float AbstractHistogram<T>::_bucket_share(const BucketID bucket_id, const T value) const {
  return static_cast<float>(value - _bucket_min(bucket_id)) / _bucket_width(bucket_id);
}

template <>
float AbstractHistogram<std::string>::_bucket_share(const BucketID bucket_id, const std::string value) const {
  /**
   * Calculate range between two strings.
   * This is based on the following assumptions:
   *    - a consecutive byte range, e.g. lower case letters in ASCII
   *    - fixed-length strings
   *
   * Treat the string range similar to the decimal system (base 26 for lower case letters).
   * Characters in the beginning of the string have a higher value than ones at the end.
   * Assign each letter the value of the index in the alphabet (zero-based).
   * Then convert it to a number.
   *
   * Example with fixed-length 4 (possible range: [aaaa, zzzz]):
   *
   *  Number of possible strings: 26**4 = 456,976
   *
   * 1. aaaa - zzzz
   *
   *  repr(aaaa) = 0 * 26**3 + 0 * 26**2 + 0 * 26**1 + 0 * 26**0 = 0
   *  repr(zzzz) = 25 * 26**3 + 25 * 26**2 + 25 * 26**1 + 25 * 26**0 = 456,975
   *  Size of range: repr(zzzz) - repr(aaaa) + 1 = 456,976
   *  Share of the range: 456,976 / 456,976 = 1
   *
   * 2. bhja - mmmm
   *
   *  repr(bhja): 1 * 26**3 + 7 * 26**2 + 9 * 26**1 + 0 * 26**0 = 22,542
   *  repr(mmmm): 12 * 26**3 + 12 * 26**2 + 12 * 26**1 + 12 * 26**0 = 219,348
   *  Size of range: repr(mmmm) - repr(bhja) + 1 = 196,807
   *  Share of the range: 196,807 / 456,976 ~= 0.43
   *
   *  Note that strings shorter than the fixed-length will induce a small error,
   *  because the missing characters will be treated as 'a'.
   *  Since we are dealing with approximations this is acceptable.
   */
  const auto value_repr = _convert_string_to_number_representation(value);
  const auto min_repr = _convert_string_to_number_representation(_bucket_min(bucket_id));
  const auto max_repr = _convert_string_to_number_representation(_bucket_max(bucket_id));
  return static_cast<float>(value_repr - min_repr) / (max_repr - min_repr + 1);
}

template <typename T>
float AbstractHistogram<T>::estimate_cardinality(const PredicateCondition predicate_type, const T value,
                                                 const std::optional<T>& value2) const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");

  T cleaned_value = value;
  if constexpr (std::is_same_v<T, std::string>) {
    cleaned_value = value.substr(0, _string_prefix_length);
  }

  if constexpr (std::is_same_v<T, std::string>) {
    Assert(cleaned_value.find_first_not_of(_supported_characters) == std::string::npos, "Unsupported characters.");
  }

  if (can_prune(predicate_type, AllTypeVariant{cleaned_value}, std::optional<AllTypeVariant>(*value2))) {
    return 0.f;
  }

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      const auto index = _bucket_for_value(cleaned_value);

      if (index == INVALID_BUCKET_ID) {
        return 0.f;
      }

      const auto bucket_count = _bucket_count(index);
      const auto bucket_count_distinct = _bucket_count_distinct(index);

      if (bucket_count == 0 || bucket_count_distinct == 0) {
        return 0.f;
      }

      return static_cast<float>(bucket_count) / static_cast<float>(bucket_count_distinct);
    }
    case PredicateCondition::NotEquals: {
      const auto index = _bucket_for_value(cleaned_value);

      if (index == INVALID_BUCKET_ID) {
        return total_count();
      }

      const auto bucket_count = _bucket_count(index);
      const auto bucket_count_distinct = _bucket_count_distinct(index);

      if (bucket_count == 0 || bucket_count_distinct == 0) {
        return total_count();
      }

      return total_count() - static_cast<float>(bucket_count) / static_cast<float>(bucket_count_distinct);
    }
    case PredicateCondition::LessThan: {
      if (cleaned_value > max()) {
        return total_count();
      }

      if (cleaned_value <= min()) {
        return 0.f;
      }

      auto index = _bucket_for_value(cleaned_value);
      auto cardinality = 0.f;

      if (index == INVALID_BUCKET_ID) {
        // The value is within the range of the histogram, but does not belong to a bucket.
        // Therefore, we need to sum up the counts of all buckets with a max < value.
        index = _upper_bound_for_value(cleaned_value);
      } else {
        cardinality += _bucket_share(index, cleaned_value) * _bucket_count(index);
      }

      // Sum up all buckets before the bucket (or gap) containing the value.
      for (BucketID bucket = 0u; bucket < index; bucket++) {
        cardinality += _bucket_count(bucket);
      }

      /**
       * The cardinality is capped at total_count().
       * It is possible for a value that is smaller than or equal to the max of the EqualHeightHistogram
       * to yield a calculated cardinality higher than total_count.
       * This is due to the way EqualHeightHistograms store the count for a bucket,
       * which is in a single value (count_per_bucket) for all buckets rather than a vector (one value for each bucket).
       * Consequently, this value is the desired count for all buckets.
       * In practice, _bucket_count(n) >= _count_per_bucket for n < num_buckets() - 1,
       * because buckets are filled up until the count is at least _count_per_bucket.
       * The last bucket typically has a count lower than _count_per_bucket.
       * Therefore, if we calculate the share of the last bucket based on _count_per_bucket
       * we might end up with an estimate higher than total_count(), which is then capped.
       */
      return std::min(cardinality, static_cast<float>(total_count()));
    }
    case PredicateCondition::LessThanEquals:
      return estimate_cardinality(PredicateCondition::LessThan, get_next_value(cleaned_value));
    case PredicateCondition::GreaterThanEquals:
      return total_count() - estimate_cardinality(PredicateCondition::LessThan, cleaned_value);
    case PredicateCondition::GreaterThan:
      return total_count() - estimate_cardinality(PredicateCondition::LessThanEquals, cleaned_value);
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(value2), "Between operator needs two values.");
      return estimate_cardinality(PredicateCondition::LessThanEquals, *value2) -
             estimate_cardinality(PredicateCondition::LessThan, cleaned_value);
    }
    // TODO(tim): implement more meaningful things here
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      return total_count();
    default:
      Fail("Predicate condition not yet supported.");
  }
}

template <typename T>
float AbstractHistogram<T>::estimate_selectivity(const PredicateCondition predicate_type, const T value,
                                                 const std::optional<T>& value2) const {
  return estimate_cardinality(predicate_type, value, value2) / total_count();
}

template <typename T>
float AbstractHistogram<T>::estimate_distinct_count(const PredicateCondition predicate_type, const T value,
                                                    const std::optional<T>& value2) const {
  if (can_prune(predicate_type, value, value2)) {
    return 0.f;
  }

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      return 1.f;
    }
    case PredicateCondition::NotEquals: {
      if (_bucket_for_value(value) == INVALID_BUCKET_ID) {
        return total_count_distinct();
      }

      return total_count_distinct() - 1.f;
    }
    case PredicateCondition::LessThan: {
      if (value > max()) {
        return total_count_distinct();
      }

      auto distinct_count = 0.f;
      auto bucket_id = _bucket_for_value(value);
      if (bucket_id == INVALID_BUCKET_ID) {
        // The value is within the range of the histogram, but does not belong to a bucket.
        // Therefore, we need to sum up the distinct counts of all buckets with a max < value.
        bucket_id = _upper_bound_for_value(value);
      } else {
        distinct_count += _bucket_share(bucket_id, value) * _bucket_count_distinct(bucket_id);
      }

      // Sum up all buckets before the bucket (or gap) containing the value.
      for (BucketID bucket = 0u; bucket < bucket_id; bucket++) {
        distinct_count += _bucket_count_distinct(bucket);
      }

      return distinct_count;
    }
    case PredicateCondition::LessThanEquals:
      return estimate_distinct_count(PredicateCondition::LessThan, get_next_value(value));
    case PredicateCondition::GreaterThanEquals:
      return total_count_distinct() - estimate_distinct_count(PredicateCondition::LessThan, value);
    case PredicateCondition::GreaterThan:
      return total_count_distinct() - estimate_distinct_count(PredicateCondition::LessThanEquals, value);
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(value2), "Between operator needs two values.");
      return estimate_distinct_count(PredicateCondition::LessThanEquals, *value2) -
             estimate_distinct_count(PredicateCondition::LessThan, value);
    }
    // TODO(tim): implement more meaningful things here
    default:
      return total_count_distinct();
  }
}

template <typename T>
bool AbstractHistogram<T>::can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                     const std::optional<AllTypeVariant>& variant_value2) const {
  DebugAssert(num_buckets() > 0, "Called method on histogram before initialization.");

  T value = type_cast<T>(variant_value);
  if constexpr (std::is_same_v<T, std::string>) {
    value = value.substr(0, _string_prefix_length);
  }

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      const auto bucket_id = _bucket_for_value(value);
      // It is possible for EqualWidthHistograms to have empty buckets.
      return bucket_id == INVALID_BUCKET_ID || _bucket_count(bucket_id) == 0;
    }
    case PredicateCondition::NotEquals:
      return num_buckets() == 1 && _bucket_min(0) == value && _bucket_max(0) == value;
    case PredicateCondition::LessThan:
      return value <= min();
    case PredicateCondition::LessThanEquals:
      return value < min();
    case PredicateCondition::GreaterThanEquals:
      return value > max();
    case PredicateCondition::GreaterThan:
      return value >= max();
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast<T>(*variant_value2);
      return can_prune(PredicateCondition::GreaterThanEquals, value) ||
             can_prune(PredicateCondition::LessThanEquals, value2) ||
             (_bucket_for_value(value) == INVALID_BUCKET_ID && _bucket_for_value(value2) == INVALID_BUCKET_ID &&
              _upper_bound_for_value(value) == _upper_bound_for_value(value2));
    }
    default:
      // Rather than failing we simply do not prune for things we cannot (yet) handle.
      // TODO(tim): think about like and not like
      return false;
  }
}

template <>
bool AbstractHistogram<std::string>::can_prune(const PredicateCondition predicate_type,
                                               const AllTypeVariant& variant_value,
                                               const std::optional<AllTypeVariant>& variant_value2) const {
  DebugAssert(num_buckets() > 0, "Called method on histogram before initialization.");

  const auto value = type_cast<std::string>(variant_value);

  switch (predicate_type) {
    case PredicateCondition::Equals:
      // If the substring is between buckets, the full string must be between buckets as well.
      return _bucket_for_value(value) == INVALID_BUCKET_ID;
    case PredicateCondition::NotEquals:
      /**
       * We never know that we can prune this when using substrings.
       * Long story short: since one substring value represents an infinite number of possible strings,
       * we can never decide for sure that there are no other matching values just based on the substring.
       * Therefore, we can only prune this if the string is shorter than the prefix.
       *
       * Example:
       * prefix_length = 2
       * There is only one bucket, which has as both lower and upper bound the value 'wa'.
       * The values existing in the column are 'war' and 'wash'.
       * We cannot prune `col != war` or `col != wash` and certainly not `col != walk`.
       * However, we cannot decide this based on the substring, which is `wa` in all three cases.
       *
       * In contrast, let's assume we have only one bucket with both lower and upper bound as the value 'w'.
       * The prefix length is still 2.
       *
       * If we now look for `col != w`, we know for sure that the column only contains the value 'w'.
       * If there was another value then either the lower or upper boundary of the bucket would be different,
       * or there would be more than one bucket.
       */
      return value.size() < _string_prefix_length
                 ? num_buckets() == 1 && _bucket_min(0) == value && _bucket_max(0) == value
                 : false;
    case PredicateCondition::LessThan:
      return value <= min();
    case PredicateCondition::LessThanEquals:
      return value < min();
    case PredicateCondition::GreaterThanEquals:
      /**
       * We have to make sure to only consider the substring here.
       *
       * Example:
       * prefix_length = 2
       * values in last bucket: rain, walk, wash
       * -> last bucket: [ra, wa]
       * `col >= war` must not be pruned because `wash >= war`. However, `war > wa`.
       * Therefore, we take the substring of the search value: `wa > wa` is false, and the predicate will not be pruned.
       * Note that `col >= water` will, however, not be pruned either (which it theoretically could).
       */
      return value.substr(0, _string_prefix_length) > max();
    case PredicateCondition::GreaterThan:
      /**
       * We have to make sure to only consider the substring here.
       *
       * Example:
       * prefix_length = 2
       * values in last bucket: rain, walk, wash
       * -> last bucket: [ra, wa]
       * `col > war` must not be pruned because `wash > war`. However, `war >= wa`.
       * Instead, we need to compare the search_value to the first value that is larger than the upper bound of the
       * histogram: `war >= wb` is false, and the predicate will not be pruned.
       * Note that `col > water` will, however, not be pruned either (which it theoretically could).
       */
      return value >= get_next_value(max());
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast<std::string>(*variant_value2);
      return can_prune(PredicateCondition::GreaterThanEquals, value) ||
             can_prune(PredicateCondition::LessThanEquals, value2) ||
             (_bucket_for_value(value) == INVALID_BUCKET_ID && _bucket_for_value(value2) == INVALID_BUCKET_ID &&
              _upper_bound_for_value(value) == _upper_bound_for_value(value2));
    }
    default:
      // Rather than failing we simply do not prune for things we cannot (yet) handle.
      // TODO(tim): think about like and not like
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
