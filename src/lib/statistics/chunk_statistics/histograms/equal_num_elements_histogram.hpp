#pragma once

#include <memory>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualNumElementsBinStats {
  std::vector<T> mins;
  std::vector<T> maxs;
  std::vector<uint64_t> counts;
  uint64_t distinct_count_per_bin;
  uint64_t num_bins_with_extra_value;
};

class Table;

template <typename T>
class EqualNumElementsHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualNumElementsHistogram(const std::vector<T>& mins, const std::vector<T>& maxs, const std::vector<uint64_t>& counts,
                            const uint64_t distinct_count_per_bin, const uint64_t num_bins_with_extra_value);
  EqualNumElementsHistogram(const std::vector<std::string>& mins, const std::vector<std::string>& maxs,
                            const std::vector<uint64_t>& counts, const uint64_t distinct_count_per_bin,
                            const uint64_t num_bins_with_extra_value, const std::string& supported_characters,
                            const uint64_t string_prefix_length);

  static std::shared_ptr<EqualNumElementsHistogram<T>> from_segment(
      const std::shared_ptr<const BaseSegment>& segment, const size_t max_num_bins,
      const std::optional<std::string>& supported_characters = std::nullopt,
      const std::optional<uint64_t>& string_prefix_length = std::nullopt);

  std::shared_ptr<AbstractHistogram<T>> clone() const override;

  HistogramType histogram_type() const override;
  uint64_t total_count_distinct() const override;
  uint64_t total_count() const override;
  size_t num_bins() const override;

 protected:
  static EqualNumElementsBinStats<T> _get_bin_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                                    const size_t max_num_bins);

  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  uint64_t _bin_count(const BinID index) const override;

  /**
   * Returns the number of distinct values that are part of this bin.
   * This number is precise for the state of the table at time of generation.
   */
  uint64_t _bin_count_distinct(const BinID index) const override;

 private:
  std::vector<T> _mins;
  std::vector<T> _maxs;
  std::vector<uint64_t> _counts;
  uint64_t _distinct_count_per_bin;
  uint64_t _num_bins_with_extra_value;
};

}  // namespace opossum
