#pragma once

#include <memory>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualHeightBinStats {
  std::vector<T> maxs;
  std::vector<uint64_t> distinct_counts;
  T min;
  uint64_t total_count;
};

class Table;

template <typename T>
class EqualHeightHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualHeightHistogram(const std::vector<T>& maxs, const std::vector<uint64_t>& distinct_counts, const T min,
                       const uint64_t total_count);
  EqualHeightHistogram(const std::vector<std::string>& maxs, const std::vector<uint64_t>& distinct_counts,
                       const std::string& min, const uint64_t total_count, const std::string& supported_characters,
                       const uint64_t string_prefix_length);

  static std::shared_ptr<EqualHeightHistogram<T>> from_column(
      const std::shared_ptr<const BaseColumn>& column, const size_t max_num_bins,
      const std::optional<std::string>& supported_characters = std::nullopt,
      const std::optional<uint64_t>& string_prefix_length = std::nullopt);

  std::shared_ptr<AbstractHistogram<T>> clone() const override;

  HistogramType histogram_type() const override;
  uint64_t total_count_distinct() const override;
  uint64_t total_count() const override;
  size_t num_bins() const override;

 protected:
  static EqualHeightBinStats<T> _get_bin_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                               const size_t max_num_bins);

  BinID _bin_for_value(const T value) const override;
  BinID _upper_bound_for_value(const T value) const override;

  T _bin_min(const BinID index) const override;
  T _bin_max(const BinID index) const override;
  uint64_t _bin_count(const BinID index) const override;
  uint64_t _bin_count_distinct(const BinID index) const override;

 private:
  std::vector<T> _maxs;
  std::vector<uint64_t> _distinct_counts;
  T _min;
  uint64_t _total_count;
};

}  // namespace opossum
