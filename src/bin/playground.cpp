#include <ctime>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <locale>
#include <memory>
#include <random>
#include <regex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "types.hpp"

#include "expression/evaluation/like_matcher.hpp"
#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/table.hpp"
#include "utils/filesystem.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

template <typename Clock, typename Duration>
std::ostream& operator<<(std::ostream& stream, const std::chrono::time_point<Clock, Duration>& time_point) {
  const time_t time = Clock::to_time_t(time_point);
#if __GNUC__ > 4 || ((__GNUC__ == 4) && __GNUC_MINOR__ > 8 && __GNUC_REVISION__ > 1)
  // Maybe the put_time will be implemented later?
  struct tm tm;
  localtime_r(&time, &tm);
  return stream << std::put_time(&tm, "%c");  // Print standard date&time
#else
  char buffer[26];
  ctime_r(&time, buffer);
  buffer[24] = '\0';  // Removes the newline that is added
  return stream << buffer;
#endif
}

/**
 * Converts string to T.
 */
template <typename T>
T str2T(const std::string& param) {
  if constexpr (std::is_same_v<T, std::string>) {
    return param;
  }

  if constexpr (std::is_same_v<T, float>) {
    return stof(param);
  }

  if constexpr (std::is_same_v<T, double>) {
    return stod(param);
  }

  if constexpr (std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t>) {
    return stol(param);
  }

  if constexpr (std::is_same_v<T, int64_t>) {
    return stoll(param);
  }

  if constexpr (std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t>) {
    return stoul(param);
  }

  if constexpr (std::is_same_v<T, uint64_t>) {
    return stoull(param);
  }

  Fail("Unknown conversion method.");
}

/**
 * Converts char pointer to T.
 */
template <typename T>
T str2T(char* param) {
  return str2T<T>(std::string{param});
}

/**
 * Returns the value in argv after option, or default_value if option is not found or there is no value after it.
 * If no default value is given and the search does not yield anything, the method fails as well.
 */
template <typename T>
T get_cmd_option(char** begin, char** end, const std::string& option,
                 const std::optional<T>& default_value = std::nullopt) {
  const auto iter = std::find(begin, end, option);
  if (iter == end || iter + 1 == end) {
    if (static_cast<bool>(default_value)) {
      return *default_value;
    }

    Fail("Option '" + option + "' was not specified, and no default was given.");
  }

  return str2T<T>(*(iter + 1));
}

/**
 * Similar to get_cmd_option, but interprets the value of the option as a comma-delimited list.
 * Returns a vector of T values splitted by comma.
 */
template <typename T>
std::vector<T> get_cmd_option_list(char** begin, char** end, const std::string& option,
                                   const std::vector<T>& default_value = std::vector<T>{}) {
  const auto iter = std::find(begin, end, option);
  if (iter == end || iter + 1 == end) {
    return default_value;
  }

  std::vector<T> result;

  std::stringstream ss(*(iter + 1));
  std::string token;

  while (std::getline(ss, token, ',')) {
    result.push_back(str2T<T>(token));
  }

  return result;
}

/**
 * Checks whether option exists in argv.
 */
bool cmd_option_exists(char** begin, char** end, const std::string& option) {
  return std::find(begin, end, option) != end;
}

/**
 * Prints a vector python style.
 */
template <typename T>
std::string vec2str(const std::vector<T>& items) {
  if (items.empty()) {
    return "[]";
  }

  std::stringstream stream;
  stream << "[";

  for (auto idx = 0u; idx < items.size() - 1; idx++) {
    stream << items[idx] << ", ";
  }

  stream << items.back() << "]";

  return stream.str();
}

/**
 * Returns the distinct values of a segment.
 */
template <typename T>
std::unordered_set<T> get_distinct_values(const std::shared_ptr<const BaseSegment>& segment) {
  std::unordered_set<T> distinct_values;

  resolve_segment_type<T>(*segment, [&](auto& typed_segment) {
    auto iterable = create_iterable_from_segment<T>(typed_segment);
    iterable.for_each([&](const auto& value) {
      if (!value.is_null()) {
        distinct_values.insert(value.value());
      }
    });
  });

  return distinct_values;
}

/**
 * Returns the distinct count for a segment.
 */
uint64_t get_distinct_count(const std::shared_ptr<const BaseSegment>& segment) {
  uint64_t distinct_count;

  resolve_data_type(segment->data_type(), [&](auto type) {
    using DataTypeT = typename decltype(type)::type;
    distinct_count = get_distinct_values<DataTypeT>(segment).size();
  });

  return distinct_count;
}

/**
 * Returns a hash map from column id to distinct count for all column ids occuring in filters_by_column.
 */
std::unordered_map<ColumnID, uint64_t> get_distinct_count_by_column(
    const std::shared_ptr<Table>& table,
    const std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>>& filters_by_column) {
  Assert(table->chunk_count() == 1u, "Table has more than one chunk.");

  std::unordered_map<ColumnID, uint64_t> distinct_count_by_column;

  for (const auto& p : filters_by_column) {
    const auto column_id = p.first;

    if (distinct_count_by_column.find(column_id) == distinct_count_by_column.end()) {
      distinct_count_by_column[column_id] = get_distinct_count(table->get_chunk(ChunkID{0})->get_segment(column_id));
    }
  }

  return distinct_count_by_column;
}

/**
 * Generates a list of num_filters filters on column_id with predicate_type.
 * Values are between min and max in equal steps.
 */
template <typename T>
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, const T min, const T max,
    const uint32_t num_filters) {
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  const auto step = (max - min) / num_filters;
  for (auto v = min; v <= max; v += step) {
    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
  }

  return filters;
}

/**
 * Generates a list of num_filters filters on column_id with predicate_type.
 * Values are random float values. Each random value appears at most once.
 */
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, std::mt19937 gen,
    std::uniform_real_distribution<> dis, const uint32_t num_filters) {
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;
  std::unordered_set<double> used_values;

  for (auto i = 0u; i < num_filters; i++) {
    auto v = dis(gen);
    while (used_values.find(v) != used_values.end()) {
      v = dis(gen);
    }

    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
    used_values.insert(v);
  }

  return filters;
}

/**
 * Generates a list of num_filters filters on column_id with predicate_type.
 * Values are random int values. Each random value appears at most once.
 */
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, std::mt19937 gen,
    std::uniform_int_distribution<> dis, const uint32_t num_filters) {
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;
  std::unordered_set<int64_t> used_values;

  for (auto i = 0u; i < num_filters; i++) {
    auto v = dis(gen);
    while (used_values.find(v) != used_values.end()) {
      v = dis(gen);
    }

    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
    used_values.insert(v);
  }

  return filters;
}

/**
 * Generates a filter for every distinct value in a given column of the table with the given predicate.
 * Generates a maximum of num_filters.
 */
std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const std::shared_ptr<Table>& table, const ColumnID column_id, const PredicateCondition predicate_type,
    const std::optional<uint32_t> num_filters = std::nullopt) {
  Assert(table->chunk_count() == 1u, "Table has more than one chunk.");

  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  resolve_data_type(table->column_data_type(column_id), [&](auto type) {
    using T = typename decltype(type)::type;
    const auto distinct_values = get_distinct_values<T>(table->get_chunk(ChunkID{0})->get_segment(column_id));

    auto i = 0u;
    for (const auto& v : distinct_values) {
      if (num_filters && i >= *num_filters) {
        break;
      }

      filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
      i++;
    }
  });

  return filters;
}

/**
 * Groups filters by ColumnID and returns a hash map from ColumnID to a vector of pairs of predicate types and values.
 */
std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> get_filters_by_column(
    const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters) {
  std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> filters_by_column;

  for (const auto& filter : filters) {
    const auto column_id = std::get<0>(filter);
    const auto predicate_condition = std::get<1>(filter);
    const auto& value = std::get<2>(filter);
    filters_by_column[column_id].emplace_back(
        std::pair<PredicateCondition, AllTypeVariant>{predicate_condition, value});
  }

  return filters_by_column;
}

/**
 * Returns the list of value-count pairs sorted by value.
 */
template <typename T>
std::vector<std::pair<T, uint64_t>> sort_value_counts(const std::unordered_map<T, uint64_t>& value_counts) {
  std::vector<std::pair<T, uint64_t>> result(value_counts.cbegin(), value_counts.cend());

  std::sort(result.begin(), result.end(),
            [](const std::pair<T, uint64_t>& lhs, const std::pair<T, uint64_t>& rhs) { return lhs.first < rhs.first; });

  return result;
}

/**
 * Returns a list of distinct values and the number of their occurences in segment.
 */
template <typename T>
std::vector<std::pair<T, uint64_t>> calculate_value_counts(const std::shared_ptr<const BaseSegment>& segment) {
  std::unordered_map<T, uint64_t> value_counts;

  resolve_segment_type<T>(*segment, [&](auto& typed_segment) {
    auto iterable = create_iterable_from_segment<T>(typed_segment);
    iterable.for_each([&](const auto& value) {
      if (!value.is_null()) {
        value_counts[value.value()]++;
      }
    });
  });

  return sort_value_counts(value_counts);
}

/**
 * For all filters, return a map from ColumnID via predicate type via value
 * to the number of rows matching the predicate on this column with this value.
 */
std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, uint64_t>>>
get_row_count_for_filters(
    const std::shared_ptr<Table>& table,
    const std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>>& filters_by_column) {
  std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, uint64_t>>>
      row_count_by_filter;
  Assert(table->chunk_count() == 1u, "Table has more than one chunk.");
  const auto total_count = table->row_count();

  for (const auto& column_filters : filters_by_column) {
    const auto column_id = column_filters.first;
    const auto filters = column_filters.second;

    resolve_data_type(table->column_data_type(column_id), [&](auto type) {
      using T = typename decltype(type)::type;

      const auto value_counts = calculate_value_counts<T>(table->get_chunk(ChunkID{0})->get_segment(column_id));

      for (const auto& filter : filters) {
        const auto predicate_type = std::get<0>(filter);
        const auto& value = std::get<1>(filter);
        const auto t_value = type_cast<T>(value);

        switch (predicate_type) {
          case PredicateCondition::Equals: {
            const auto it = std::find_if(value_counts.cbegin(), value_counts.cend(),
                                         [&](const std::pair<T, uint64_t>& p) { return p.first == t_value; });
            if (it != value_counts.cend()) {
              row_count_by_filter[column_id][predicate_type][value] = (*it).second;
            }
            break;
          }
          case PredicateCondition::NotEquals: {
            const auto it = std::find_if(value_counts.cbegin(), value_counts.cend(),
                                         [&](const std::pair<T, uint64_t>& p) { return p.first == t_value; });
            uint64_t count = total_count;
            if (it != value_counts.cend()) {
              count -= (*it).second;
            }
            row_count_by_filter[column_id][predicate_type][value] = count;
            break;
          }
          case PredicateCondition::LessThan: {
            const auto it =
                std::lower_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const std::pair<T, uint64_t>& lhs, const T& rhs) { return lhs.first < rhs; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(value_counts.cbegin(), it, uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::LessThanEquals: {
            const auto it =
                std::upper_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const T& lhs, const std::pair<T, uint64_t>& rhs) { return lhs < rhs.first; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(value_counts.cbegin(), it, uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::GreaterThanEquals: {
            const auto it =
                std::lower_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const std::pair<T, uint64_t>& lhs, const T& rhs) { return lhs.first < rhs; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(it, value_counts.cend(), uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::GreaterThan: {
            const auto it =
                std::upper_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                                 [](const T& lhs, const std::pair<T, uint64_t>& rhs) { return lhs < rhs.first; });
            row_count_by_filter[column_id][predicate_type][value] =
                std::accumulate(it, value_counts.cend(), uint64_t{0},
                                [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
            break;
          }
          case PredicateCondition::Like: {
            if constexpr (!std::is_same_v<T, std::string>) {
              Fail("LIKE predicates only work for string columns");
            } else {
              const auto regex_string = LikeMatcher::sql_like_to_regex(t_value);
              const auto regex = std::regex{regex_string};

              uint64_t sum = 0;
              for (const auto &p : value_counts) {
                if (std::regex_match(p.first, regex)) {
                  sum += p.second;
                }
              }

              row_count_by_filter[column_id][predicate_type][value] = sum;
            }
            break;
          }
          case PredicateCondition::NotLike: {
            if constexpr (!std::is_same_v<T, std::string>) {
              Fail("NOT LIKE predicates only work for string columns");
            } else {
              const auto regex_string = LikeMatcher::sql_like_to_regex(t_value);
              const auto regex = std::regex{regex_string};

              uint64_t sum = 0;
              for (const auto &p : value_counts) {
                if (!std::regex_match(p.first, regex)) {
                  sum += p.second;
                }
              }

              row_count_by_filter[column_id][predicate_type][value] = sum;
            }
            break;
          }
          default:
            Fail("Predicate type not supported.");
        }
      }
    });
  }

  return row_count_by_filter;
}

void run(const std::shared_ptr<Table> table, const std::vector<uint64_t> num_bins_list,
         const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters,
         const std::string& output_path) {
  Assert(table->chunk_count() == 1u, "Table has more than one chunk.");

  // std::cout.imbue(std::locale("en_US.UTF-8"));
  // auto start = std::chrono::high_resolution_clock::now();
  // auto end = std::chrono::high_resolution_clock::now();
  // std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;

  auto results_log = std::ofstream(output_path + "/estimation_results.log", std::ios_base::out | std::ios_base::trunc);
  results_log << "total_count,distinct_count,num_bins,column_name,predicate_condition,value,actual_count,equal_"
                 "height_hist_count,equal_num_elements_hist_count,equal_width_hist_count\n";

  auto histogram_log = std::ofstream(output_path + "/estimation_bins.log", std::ios_base::out | std::ios_base::trunc);
  histogram_log << "histogram_type,column_name,actual_num_bins,requested_num_bins,bin_id,bin_min,bin_max,"
                   "bin_min_repr,bin_max_repr,bin_count,bin_count_distinct\n";

  const auto filters_by_column = get_filters_by_column(filters);
  const auto row_count_by_filter = get_row_count_for_filters(table, filters_by_column);
  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bins_list) {
    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto distinct_count = distinct_count_by_column.at(column_id);
      const auto column_name = table->column_name(column_id);
      std::cout << column_name << std::endl;

      const auto column_data_type = table->column_data_type(column_id);
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        const auto equal_height_hist =
            EqualHeightHistogram<T>::from_segment(table->get_chunk(ChunkID{0})->get_segment(column_id), num_bins);
        const auto equal_num_elements_hist =
            EqualNumElementsHistogram<T>::from_segment(table->get_chunk(ChunkID{0})->get_segment(column_id), num_bins);
        const auto equal_width_hist =
            EqualWidthHistogram<T>::from_segment(table->get_chunk(ChunkID{0})->get_segment(column_id), num_bins);

        histogram_log << equal_height_hist->bins_to_csv(false, column_name, num_bins);
        histogram_log << equal_num_elements_hist->bins_to_csv(false, column_name, num_bins);
        histogram_log << equal_width_hist->bins_to_csv(false, column_name, num_bins);
        histogram_log.flush();

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;
          const auto value = pair.second;

          const auto actual_count = row_count_by_filter.at(column_id).at(predicate_condition).at(value);
          const auto equal_height_hist_count = equal_height_hist->estimate_cardinality(predicate_condition, value);
          const auto equal_num_elements_hist_count =
              equal_num_elements_hist->estimate_cardinality(predicate_condition, value);
          const auto equal_width_hist_count = equal_width_hist->estimate_cardinality(predicate_condition, value);

          results_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                      << std::to_string(num_bins) << "," << column_name << ","
                      << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                      << std::to_string(actual_count) << "," << std::to_string(equal_height_hist_count) << ","
                      << std::to_string(equal_num_elements_hist_count) << "," << std::to_string(equal_width_hist_count)
                      << "\n";
          results_log.flush();
        }
      });
    }
  }
}

int main(int argc, char** argv) {
  const auto argv_end = argv + argc;
  const auto table_path = get_cmd_option<std::string>(argv, argv_end, "--table-path");
  const auto filter_mode = get_cmd_option<std::string>(argv, argv_end, "--filter-mode");
  const auto num_bins_list = get_cmd_option_list<uint64_t>(argv, argv_end, "--num-bins");
  const auto chunk_size = get_cmd_option<uint32_t>(argv, argv_end, "--chunk-size", Chunk::MAX_SIZE);
  const auto output_path = get_cmd_option<std::string>(argv, argv_end, "--output-path", "../results/");

  const auto table = load_table(table_path, chunk_size);

  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  if (filter_mode == "random-on-column") {
    std::random_device rd;
    std::mt19937 gen(rd());

    const auto column_id = ColumnID{get_cmd_option<uint16_t>(argv, argv_end, "--column-id")};
    const auto predicate_type =
        predicate_condition_to_string.right.at(get_cmd_option<std::string>(argv, argv_end, "--predicate-type"));
    const auto num_filters = get_cmd_option<uint32_t>(argv, argv_end, "--num-filters");
    const auto data_type = table->column_data_type(column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto min = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-min");
      const auto max = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-max");
      Assert(min < max, "Min has to be smaller than max.");

      if constexpr (std::is_floating_point_v<ColumnDataType>) {
        std::uniform_real_distribution<> dis(min, max);
        filters = generate_filters(column_id, predicate_type, gen, dis, num_filters);
      } else if constexpr (std::is_integral_v<ColumnDataType>) {
        Assert(static_cast<uint64_t>(max - min) + 1 >= num_filters,
               "Cannot generate " + std::to_string(num_filters) + " unique random values between " +
                   std::to_string(min) + " and " + std::to_string(max) + ".");
        std::uniform_int_distribution<> dis(min, max);
        filters = generate_filters(column_id, predicate_type, gen, dis, num_filters);
      } else {
        Fail("Data type not supported to generate random values.");
      }
    });
  } else if (filter_mode == "step-on-column") {
    const auto column_id = ColumnID{get_cmd_option<uint16_t>(argv, argv_end, "--column-id")};
    const auto predicate_type =
        predicate_condition_to_string.right.at(get_cmd_option<std::string>(argv, argv_end, "--predicate-type"));
    const auto num_filters = get_cmd_option<uint32_t>(argv, argv_end, "--num-filters");
    const auto data_type = table->column_data_type(column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto min = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-min");
      const auto max = get_cmd_option<ColumnDataType>(argv, argv_end, "--filter-max");

      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        filters = generate_filters(column_id, predicate_type, min, max, num_filters);
      } else {
        Fail("Data type not supported to generate values in steps.");
      }
    });
  } else if (filter_mode == "distinct-on-column") {
    const auto column_id = ColumnID{get_cmd_option<uint16_t>(argv, argv_end, "--column-id")};
    const auto predicate_type =
        predicate_condition_to_string.right.at(get_cmd_option<std::string>(argv, argv_end, "--predicate-type"));

    std::optional<uint32_t> num_filters;
    if (cmd_option_exists(argv, argv_end, "--num-filters")) {
      num_filters = get_cmd_option<uint32_t>(argv, argv_end, "--num-filters");
    }

    filters = generate_filters(table, column_id, predicate_type, num_filters);
  } else if (filter_mode == "from_file") {
    const auto filter_file = get_cmd_option<std::string>(argv, argv_end, "--filter-file");

    std::ifstream infile(filter_file);
    Assert(infile.is_open(), "Could not find file: " + filter_file + ".");

    std::string line;
    while (std::getline(infile, line)) {
      std::vector<std::string> fields;
      std::stringstream ss(line);
      std::string token;

      while (std::getline(ss, token, ',')) {
        fields.push_back(token);
      }

      Assert(fields.size() == 3, "Filter file invalid in line: '" + line + "'.");

      const auto column_id = ColumnID{str2T<uint16_t>(fields[0])};
      const auto predicate_type = predicate_condition_to_string.right.at(fields[1]);

      resolve_data_type(table->column_data_type(column_id), [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        const auto v = AllTypeVariant{str2T<ColumnDataType>(fields[2])};
        filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
      });
    }
  } else {
    Fail("Mode '" + filter_mode + "' not supported.");
  }

  run(table, num_bins_list, filters, output_path);

  return 0;
}
