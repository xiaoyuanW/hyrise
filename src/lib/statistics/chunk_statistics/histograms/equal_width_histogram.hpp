#pragma once

#include <memory>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualWidthBinStats {
  T min;
  T max;
  std::vector<uint64_t> counts;
  std::vector<uint64_t> distinct_counts;
  uint64_t num_bins_with_larger_range;
};

class Table;

template <typename T>
class EqualWidthHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualWidthHistogram(const T min, const T max, const std::vector<uint64_t>& counts,
                      const std::vector<uint64_t>& distinct_counts, const uint64_t num_bins_with_larger_range);
  EqualWidthHistogram(const std::string& min, const std::string& max, const std::vector<uint64_t>& counts,
                      const std::vector<uint64_t>& distinct_counts, const uint64_t num_bins_with_larger_range,
                      const std::string& supported_characters, const uint64_t string_prefix_length);

  static std::shared_ptr<EqualWidthHistogram<T>> from_segment(
      const std::shared_ptr<const BaseSegment>& segment, const size_t max_num_bins,
      const std::optional<std::string>& supported_characters = std::nullopt,
      const std::optional<uint64_t>& string_prefix_length = std::nullopt);

  std::shared_ptr<AbstractHistogram<T>> clone() const override;

  HistogramType histogram_type() const override;
  uint64_t total_count_distinct() const override;
  uint64_t total_count() const override;
  size_t num_bins() const override;

 protected:
  static EqualWidthBinStats<T> _get_bin_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                              const size_t max_num_bins);
  static EqualWidthBinStats<std::string> _get_bin_stats(
      const std::vector<std::pair<std::string, uint64_t>>& value_counts, const size_t max_num_bins,
      const std::string& supported_characters, const uint64_t string_prefix_length);

  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  uint64_t _bin_count(const BinID index) const override;
  uint64_t _bin_count_distinct(const BinID index) const override;

  // Overriding because it would otherwise recursively call itself.
  T _bin_width(const BinID index) const override;

 private:
  T _min;
  T _max;
  std::vector<uint64_t> _counts;
  std::vector<uint64_t> _distinct_counts;
  uint64_t _num_bins_with_larger_range;
};

}  // namespace opossum
