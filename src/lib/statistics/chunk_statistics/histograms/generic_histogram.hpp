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
struct GenericBinData {
  // Min values on a per-bin basis.
  std::vector<T> bin_minima;

  // Max values on a per-bin basis.
  std::vector<T> bin_maxima;

  // Number of values on a per-bin basis.
  std::vector<StatisticsObjectCountType> bin_heights;

  // Number of distinct values on a per-bin basis.
  std::vector<StatisticsObjectCountType> bin_distinct_counts;
};

/**
 * Generic histogram.
 * Bins do not necessarily share any common traits such as height or width or distinct count.
 * This histogram should only be used to create temporary statistics objects, as its space complexity is high.
 */
template <typename T>
class GenericHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  GenericHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                   std::vector<StatisticsObjectCountType>&& bin_heights,
                   std::vector<StatisticsObjectCountType>&& bin_count_distincts);
  GenericHistogram(std::vector<T>&& bin_minima, std::vector<T>&& bin_maxima,
                   std::vector<StatisticsObjectCountType>&& bin_heights, std::vector<StatisticsObjectCountType>&& bin_count_distincts,
                   const std::string& supported_characters, const size_t string_prefix_length);

  HistogramType histogram_type() const override;
  std::string histogram_name() const override;
  std::shared_ptr<AbstractHistogram<T>> clone() const override;
  StatisticsObjectCountType total_distinct_count() const override;
  StatisticsObjectCountType total_count() const override;

  BinID bin_count() const override;

  T bin_minimum(const BinID index) const override;
  T bin_maximum(const BinID index) const override;
  StatisticsObjectCountType bin_height(const BinID index) const override;
  StatisticsObjectCountType bin_distinct_count(const BinID index) const override;

  std::shared_ptr<AbstractStatisticsObject> scale_with_selectivity(const Selectivity selectivity) const override;

 protected:
  BinID _bin_for_value(const T& value) const override;

  BinID _next_bin_for_value(const T& value) const override;

 private:
  const GenericBinData<T> _bin_data;
};

}  // namespace opossum
