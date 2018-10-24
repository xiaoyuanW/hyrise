#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "statistics/selectivity.hpp"
#include "types.hpp"

namespace opossum {

/**
 * We use multiple vectors rather than a vector of structs for ease-of-use with STL library functions.
 */
template <typename T>
struct EqualDistinctCountBinData {
  // Min values on a per-bin basis.
  std::vector<T> bin_minima;

  // Max values on a per-bin basis.
  std::vector<T> bin_maxima;

  // Number of values on a per-bin basis.
  std::vector<StatisticsObjectCountType> bin_heights;

  // Number of distinct values per bin.
  StatisticsObjectCountType distinct_count_per_bin;

  // The first bin_count_with_extra_value bins have an additional distinct value.
  BinID bin_count_with_extra_value;
};

/**
 * Distinct-balanced histogram.
 * Bins contain roughly the same number of distinct values actually occurring in the data.
 * There might be gaps between bins.
 */
template <typename T>
class EqualDistinctCountHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualDistinctCountHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                              std::vector<StatisticsObjectCountType>&& bin_heights,
                              const StatisticsObjectCountType distinct_count_per_bin, const BinID bin_count_with_extra_value);
  EqualDistinctCountHistogram(std::vector<std::string>&& bin_minima, std::vector<std::string>&& bin_maxima,
                              std::vector<StatisticsObjectCountType>&& bin_heights,
                              const StatisticsObjectCountType distinct_count_per_bin, const BinID bin_count_with_extra_value,
                              const std::string& supported_characters, const size_t string_prefix_length);

  /**
   * Create a histogram based on the data in a given segment.
   * @param segment The segment containing the data.
   * @param max_bin_count The number of bins to create. The histogram might create fewer, but never more.
   * @param supported_characters A sorted, consecutive string of characters supported in case of string histograms.
   * Can be omitted and will be filled with default value.
   * @param string_prefix_length The prefix length used to calculate string ranges.
   * * Can be omitted and will be filled with default value.
   */
  static std::shared_ptr<EqualDistinctCountHistogram<T>> from_segment(
      const std::shared_ptr<const BaseSegment>& segment, const BinID max_bin_count,
      const std::optional<std::string>& supported_characters = std::nullopt,
      const std::optional<uint32_t>& string_prefix_length = std::nullopt);

  HistogramType histogram_type() const override;
  std::string histogram_name() const override;
  std::shared_ptr<AbstractHistogram<T>> clone() const override;
  StatisticsObjectCountType total_distinct_count() const override;
  StatisticsObjectCountType total_count() const override;

  /**
   * Returns the number of bins actually present in the histogram.
   * This number can be smaller than the number of bins requested when creating a histogram.
   * The number of bins is capped at the number of distinct values in the segment.
   * Otherwise, there would be empty bins without any benefit.
   */
  BinID bin_count() const override;

  T bin_minimum(const BinID index) const override;
  T bin_maximum(const BinID index) const override;
  StatisticsObjectCountType bin_height(const BinID index) const override;
  StatisticsObjectCountType bin_distinct_count(const BinID index) const override;

  std::shared_ptr<AbstractStatisticsObject> scale_with_selectivity(const Selectivity selectivity) const override;

 protected:
  /**
   * Creates bins and their statistics.
   */
  static EqualDistinctCountBinData<T> _build_bins(const std::vector<std::pair<T, StatisticsObjectCountType>>& value_counts,
                                                  const BinID max_bin_count);

  BinID _bin_for_value(const T& value) const override;
  BinID _next_bin_for_value(const T& value) const override;

 private:
  const EqualDistinctCountBinData<T> _bin_data;
};

}  // namespace opossum
