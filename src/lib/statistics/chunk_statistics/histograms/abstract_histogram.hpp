#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "statistics/chunk_statistics/abstract_filter.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class AbstractHistogram : public AbstractFilter {
  friend class HistogramPrivateTest;

 public:
  explicit AbstractHistogram(const std::shared_ptr<const Table>& table);
  AbstractHistogram(const std::shared_ptr<const Table>& table, const std::string& supported_characters);
  AbstractHistogram(const std::shared_ptr<const Table>& table, const std::string& supported_characters,
                    const uint64_t string_prefix_length);
  virtual ~AbstractHistogram() = default;

  virtual std::shared_ptr<AbstractHistogram<T>> clone() const = 0;

  virtual HistogramType histogram_type() const = 0;
  std::string description() const;
  std::string bins_to_csv(const bool print_header = true, const std::optional<std::string>& column_name = std::nullopt,
                          const std::optional<uint64_t>& requested_num_bins = std::nullopt) const;
  const std::string& supported_characters() const;

  void generate(const ColumnID column_id, const size_t max_num_bins);

  float estimate_selectivity(const PredicateCondition predicate_type, const T value,
                             const std::optional<T>& value2 = std::nullopt) const;
  float estimate_cardinality(const PredicateCondition predicate_type, const T value,
                             const std::optional<T>& value2 = std::nullopt) const;
  float estimate_distinct_count(const PredicateCondition predicate_type, const T value,
                                const std::optional<T>& value2 = std::nullopt) const;
  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  T min() const;
  T max() const;

  T get_previous_value(const T value) const;
  T get_next_value(const T value) const;

  virtual size_t num_bins() const = 0;
  virtual uint64_t total_count() const = 0;
  virtual uint64_t total_count_distinct() const = 0;

 protected:
  const std::shared_ptr<const Table> _get_value_counts(const ColumnID column_id) const;

  static
      // typename std::enable_if_t<std::is_arithmetic_v<T>, std::vector<std::pair<T, uint64_t>>>
      std::vector<std::pair<T, uint64_t>>
      _calculate_value_counts(const std::shared_ptr<const BaseColumn>& column);

  static std::vector<std::pair<std::string, uint64_t>> _calculate_value_counts(
      const std::shared_ptr<const BaseColumn>& column, const std::string& supported_characters,
      const uint64_t string_prefix_length);
  static std::vector<std::pair<T, uint64_t>> _sort_value_counts(const std::unordered_map<T, uint64_t>& value_counts);

  virtual void _generate(const std::shared_ptr<const ValueColumn<T>> distinct_column,
                         const std::shared_ptr<const ValueColumn<int64_t>> count_column, const size_t max_num_bins) = 0;

  uint64_t _convert_string_to_number_representation(const std::string& value) const;
  std::string _convert_number_representation_to_string(const uint64_t value) const;
  float _bin_share(const BinID bin_id, const T value) const;

  virtual T _bin_width(const BinID index) const;

  virtual BinID _bin_for_value(const T value) const = 0;
  virtual BinID _lower_bound_for_value(const T value) const = 0;
  virtual BinID _upper_bound_for_value(const T value) const = 0;

  virtual T _bin_min(const BinID index) const = 0;
  virtual T _bin_max(const BinID index) const = 0;
  virtual uint64_t _bin_count(const BinID index) const = 0;
  virtual uint64_t _bin_count_distinct(const BinID index) const = 0;

  const std::weak_ptr<const Table> _table;
  std::string _supported_characters;
  uint64_t _string_prefix_length;
};

}  // namespace opossum
