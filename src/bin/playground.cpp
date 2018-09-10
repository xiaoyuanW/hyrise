#include <ctime>

#include <chrono>
#include <fstream>
#include <iostream>
#include <locale>
#include <random>
#include <tuple>

#include "constant_mappings.hpp"
#include "types.hpp"

#include "operators/aggregate.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
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

// // lineitem
// const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters = {
//     /**
//      * TPC-H 1
//      */
//     // L_SHIPDATE <= '1998-12-01' - interval '[DELTA]' day (3)
//     {ColumnID{10}, PredicateCondition::LessThanEquals, "1998-11-28"},
//     /**
//      * TPC-H 4
//      */
//     // L_COMMITDATE < L_RECEIPTDATE
//     {ColumnID{11}, PredicateCondition::LessThan, ColumnID{12}},
//     /**
//      * TPC-H 6
//      */
//     // L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < dateadd(yy, 1, cast('1994-01-01' as datetime))
//     {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1994-01-01"},
//     {ColumnID{10}, PredicateCondition::LessThan, "1995-01-01"},
//     // L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
//     {ColumnID{6}, PredicateCondition::GreaterThanEquals, .05f},
//     {ColumnID{6}, PredicateCondition::LessThanEquals, .07f},
//     // L_QUANTITY < 24
//     {ColumnID{4}, PredicateCondition::LessThan, 24},
//     /**
//      * TPC-H 7
//      */
//     // L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
//     {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1995-01-01"},
//     {ColumnID{10}, PredicateCondition::LessThanEquals, "1996-12-31"},
//     /**
//      * TPC-H 10
//      */
//     // L_RETURNFLAG = 'R'
//     {ColumnID{8}, PredicateCondition::Equals, "R"},
//     /**
//      * TPC-H 12
//      */
//     // TODO(tim): enable once TableScan supports IN predicate
//     // L_SHIPMODE in ('[SHIPMODE1]', '[SHIPMODE2]')
//     // {ColumnID{14}, PredicateCondition::In, {"MAIL", "SHIP"}},
//     // L_SHIPDATE < L_COMMITTDATE
//     {ColumnID{10}, PredicateCondition::LessThan, ColumnID{11}},
//     // L_RECEIPTDATE >= date '[DATE]' AND L_RECEIPTDATE < date '[DATE]' + interval '1' year
//     {ColumnID{12}, PredicateCondition::GreaterThanEquals, "1995-06-01"},
//     {ColumnID{12}, PredicateCondition::LessThan, "1996-06-01"},
//     /**
//      * TPC-H 14
//      */
//     // L_SHIPDATE >= date '[DATE]' AND L_SHIPDATE < date '[DATE]' + interval '1' month
//     {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1995-06-01"},
//     {ColumnID{10}, PredicateCondition::LessThan, "1995-07-01"},
//     /**
//      * TPC-H 15
//      */
//     // L_SHIPDATE >= date '[DATE]' AND L_SHIPDATE < date '[DATE]' + interval '3' month
//     {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1995-06-01"},
//     {ColumnID{10}, PredicateCondition::LessThan, "1995-09-01"},
//     /**
//      * TPC-H 19
//      */
//     // TODO(tim): enable once TableScan supports IN predicate
//     // L_SHIPMODE in ('AIR', 'AIR REG')
//     // {ColumnID{14}, PredicateCondition::In, {"AIR", "AIR REG"}},
//     // L_SHIPINSTRUCT = 'DELIVER IN PERSON'
//     {ColumnID{13}, PredicateCondition::Equals, "DELIVER IN PERSON"},
//     // L_QUANTITY >= [QUANTITY1] AND L_QUANTITY <= [QUANTITY1] + 10
//     {ColumnID{4}, PredicateCondition::GreaterThanEquals, 1},
//     {ColumnID{4}, PredicateCondition::LessThanEquals, 11},
//     // L_QUANTITY >= [QUANTITY1] AND L_QUANTITY <= [QUANTITY1] + 10
//     {ColumnID{4}, PredicateCondition::GreaterThanEquals, 15},
//     {ColumnID{4}, PredicateCondition::LessThanEquals, 25},
//     // L_QUANTITY >= [QUANTITY1] AND L_QUANTITY <= [QUANTITY1] + 10
//     {ColumnID{4}, PredicateCondition::GreaterThanEquals, 30},
//     {ColumnID{4}, PredicateCondition::LessThanEquals, 40},
// };

// // orders
// const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters = {
//         /**
//          * TPC-H 3
//          */
//         // O_ORDERDATE < date '[DATE]'
//         {ColumnID{4}, PredicateCondition::LessThan, "1995-03-15"},
//         /**
//          * TPC-H 4
//          */
//         // O_ORDERDATE >= date '[DATE]' AND O_ORDERDATE < date '[DATE]' + interval '3' month
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1996-07-01"},
//         {ColumnID{4}, PredicateCondition::LessThan, "1996-10-01"},
//         /**
//          * TPC-H 5
//          */
//         // O_ORDERDATE >= date '[DATE]' AND O_ORDERDATE < date '[DATE]' + interval '1' year
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1994-01-01"},
//         {ColumnID{4}, PredicateCondition::LessThan, "1995-01-01"},
//         /**
//          * TPC-H 8
//          */
//         // O_ORDERDATE between date '1995-01-01' AND date '1996-12-31'
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1995-01-01"},
//         {ColumnID{4}, PredicateCondition::LessThanEquals, "1996-12-31"},
//         /**
//          * TPC-H 10
//          */
//         // O_ORDERDATE >= date '[DATE]' AND O_ORDERDATE < date '[DATE]' + interval '3' month
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1993-10-01"},
//         {ColumnID{4}, PredicateCondition::LessThan, "1994-01-01"},
//         /**
//          * TPC-H 21
//          */
//         // o_orderstatus = 'F'
//         {ColumnID{2}, PredicateCondition::Equals, "F"},
//         /**
//          * custom predicates
//          * totalprice column chosen because of its skewed distribution
//          */
//         {ColumnID{3}, PredicateCondition::LessThan, 10.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 100.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 1'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 1'453.8},
//         {ColumnID{3}, PredicateCondition::LessThan, 2'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 2'912.1},
//         {ColumnID{3}, PredicateCondition::LessThan, 3'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 3'239.9},
//         {ColumnID{3}, PredicateCondition::LessThan, 4'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 4'245.3},
//         {ColumnID{3}, PredicateCondition::LessThan, 5'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 5'456.4},
//         {ColumnID{3}, PredicateCondition::LessThan, 6'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 6'897.8},
//         {ColumnID{3}, PredicateCondition::LessThan, 7'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 7'211.2},
//         {ColumnID{3}, PredicateCondition::LessThan, 8'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 8'019.1},
//         {ColumnID{3}, PredicateCondition::LessThan, 9'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 9'349.9},
//         {ColumnID{3}, PredicateCondition::LessThan, 10'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 15'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 20'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 25'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 30'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 35'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 40'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 45'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 50'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 55'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 60'000.0},
// };

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

std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, std::mt19937 gen,
    std::uniform_real_distribution<> dis, const uint32_t num_filters) {
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  for (auto i = 0u; i < num_filters; i++) {
    const auto v = dis(gen);
    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
  }

  return filters;
}

std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const ColumnID column_id, const PredicateCondition predicate_type, std::mt19937 gen,
    std::uniform_int_distribution<> dis, const uint32_t num_filters) {
  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  for (auto i = 0u; i < num_filters; i++) {
    const auto v = dis(gen);
    filters.emplace_back(std::tuple<ColumnID, PredicateCondition, AllTypeVariant>{column_id, predicate_type, v});
  }

  return filters;
}

uint64_t get_predicate_count(const std::shared_ptr<Table>& table, const ColumnID column_id,
                             const PredicateCondition predicate_condition, const AllTypeVariant& value) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto table_scan = std::make_shared<TableScan>(table_wrapper, column_id, predicate_condition, value);
  table_scan->execute();

  // const auto print = std::make_shared<Print>(aggregate);
  // print->execute();

  return table_scan->get_output()->row_count();
}

uint64_t get_distinct_count(const std::shared_ptr<Table>& table, const ColumnID column_id) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto aggregate = std::make_shared<Aggregate>(table_wrapper, std::vector<AggregateColumnDefinition>{},
                                               std::vector<ColumnID>{column_id});
  aggregate->execute();

  // const auto print = std::make_shared<Print>(aggregate);
  // print->execute();

  return aggregate->get_output()->row_count();
}

std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> get_filters_by_column(
    const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters) {
  std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> filters_by_column;

  for (const auto& filter : filters) {
    const auto column_id = std::get<0>(filter);
    const auto predicate_condition = std::get<1>(filter);
    const auto value = std::get<2>(filter);
    filters_by_column[column_id].emplace_back(
        std::pair<PredicateCondition, AllTypeVariant>{predicate_condition, value});
  }

  return filters_by_column;
};

std::unordered_map<ColumnID, int64_t> get_distinct_count_by_column(
    const std::shared_ptr<Table>& table,
    const std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>>& filters) {
  std::unordered_map<ColumnID, int64_t> row_count_by_filter;

  std::cout << std::chrono::system_clock::now() << " Getting distinct counts for " << filters.size() << " columns...";
  const auto start = std::chrono::high_resolution_clock::now();

  for (const auto& p : filters) {
    row_count_by_filter[p.first] = get_distinct_count(table, p.first);
  }

  const auto end = std::chrono::high_resolution_clock::now();
  std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
            << std::endl;

  return row_count_by_filter;
};

std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, int64_t>>>
get_row_count_by_filter(const std::shared_ptr<Table>& table,
                        const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters) {
  std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, int64_t>>>
      row_count_by_filter;

  std::cout << std::chrono::system_clock::now() << " Getting predicate counts for " << filters.size() << " filters...";
  const auto start = std::chrono::high_resolution_clock::now();

  for (const auto& filter : filters) {
    const auto column_id = std::get<0>(filter);
    const auto predicate_condition = std::get<1>(filter);
    const auto value = std::get<2>(filter);
    row_count_by_filter[column_id][predicate_condition][value] =
        get_predicate_count(table, column_id, predicate_condition, value);
  }

  const auto end = std::chrono::high_resolution_clock::now();
  std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
            << std::endl;

  return row_count_by_filter;
};

template <typename T>
std::vector<std::pair<T, uint64_t>> sort_value_counts(const std::unordered_map<T, uint64_t>& value_counts) {
  std::vector<std::pair<T, uint64_t>> result(value_counts.cbegin(), value_counts.cend());

  std::sort(result.begin(), result.end(),
            [](const std::pair<T, uint64_t>& lhs, const std::pair<T, uint64_t>& rhs) { return lhs.first < rhs.first; });

  return result;
}

template <typename T>
std::vector<std::pair<T, uint64_t>> calculate_value_counts(const std::shared_ptr<const BaseColumn>& column) {
  std::unordered_map<T, uint64_t> value_counts;

  resolve_column_type<T>(*column, [&](auto& typed_column) {
    auto iterable = create_iterable_from_segment<T>(typed_column);
    iterable.for_each([&](const auto& value) {
      if (!value.is_null()) {
        value_counts[value.value()]++;
      }
    });
  });

  return sort_value_counts(value_counts);
}

std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, int64_t>>>
get_row_count_for_filters(const std::shared_ptr<Table>& table, const ColumnID column_id,
                          const PredicateCondition predicate_type,
                          const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters) {
  std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, int64_t>>>
      row_count_by_filter;

  std::cout << std::chrono::system_clock::now() << " Getting predicate counts for " << filters.size() << " filters...";
  const auto start = std::chrono::high_resolution_clock::now();

  Assert(table->chunk_count() == 1u, "Table has more than one chunk.");

  resolve_data_type(table->column_data_types()[column_id], [&](auto type) {
    using T = typename decltype(type)::type;

    const auto value_counts = calculate_value_counts<T>(table->get_chunk(ChunkID{0})->get_segment(column_id));

    for (const auto& filter : filters) {
      const auto value = std::get<2>(filter);
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
        case PredicateCondition::LessThan: {
          const auto it =
              std::lower_bound(value_counts.cbegin(), value_counts.cend(), t_value,
                               [](const std::pair<T, uint64_t>& lhs, const T& rhs) { return lhs.first < rhs; });
          row_count_by_filter[column_id][predicate_type][value] =
              std::accumulate(value_counts.cbegin(), it, uint64_t{0},
                              [](uint64_t a, const std::pair<T, uint64_t>& b) { return a + b.second; });
          break;
        }
        default:
          Fail("Predicate type not supported.");
      }
    }
  });

  const auto end = std::chrono::high_resolution_clock::now();
  std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
            << std::endl;

  return row_count_by_filter;
};

template <typename T>
std::unordered_set<T> get_distinct_values(const std::shared_ptr<const BaseColumn>& column) {
  std::unordered_set<T> distinct_values;

  resolve_column_type<T>(*column, [&](auto& typed_column) {
    auto iterable = create_iterable_from_segment<T>(typed_column);
    iterable.for_each([&](const auto& value) {
      if (!value.is_null()) {
        distinct_values.insert(value.value());
      }
    });
  });

  return distinct_values;
}

std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> generate_filters(
    const std::shared_ptr<Table>& table, const ColumnID column_id, const PredicateCondition predicate_type,
    const std::optional<uint32_t> num_filters = std::nullopt) {
  Assert(table->chunk_count() == 1u, "Table has more than one chunk.");

  std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters;

  resolve_data_type(table->column_data_types()[column_id], [&](auto type) {
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

template <typename T>
float estimate_cardinality(const std::shared_ptr<AbstractHistogram<T>>& hist,
                           const PredicateCondition predicate_condition, const AllTypeVariant value) {
  const auto t_value = type_cast<T>(value);
  return hist->estimate_cardinality(predicate_condition, t_value);
}

int main() {
  std::cout.imbue(std::locale("en_US.UTF-8"));
  std::cout << std::chrono::system_clock::now() << " Loading table...";
  auto start = std::chrono::high_resolution_clock::now();

  // const auto table = load_table("../src/test/tables/random_string_data.tbl");
  const auto table = load_table("../src/test/tables/tpch-jan/orders.tbl");
  // const auto table = load_table("../src/test/tables/tpch-jan/orders_1000.tbl");
  // const auto table = load_table("../src/test/tables/jcch/sf-0.01/lineitem_header.tbl");
  // const auto table = load_table("../src/test/tables/jcch/sf-0.1/lineitem_header.tbl");
  // const auto table = load_table("../src/test/tables/jcch/sf-1/lineitem_header.tbl");

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
            << std::endl;

  const auto num_bin_combinations = {
      // 1u, 2u, 5u, 10u, 25u, 50u, 100u, 150u, 200u, 250u, 300u, 400u, 500u, 750u, 1000u,
      1u, 10u, 25u, 100u, 250u, 1'000u, 2'500u, 5'000u, 7'500u, 10'000u,
      // 10u, 50u, 250u,
  };

  const auto predicate_type = PredicateCondition::Equals;

  auto results_log = std::ofstream("../estimation_results.log", std::ios_base::out | std::ios_base::trunc);
  results_log << "total_count,distinct_count,num_bins,column_name,predicate_condition,value,actual_count,equal_"
                 "height_hist_count,equal_num_elements_hist_count,equal_width_hist_count\n";

  auto histogram_log = std::ofstream("../histogram_bins.log", std::ios_base::out | std::ios_base::trunc);
  histogram_log << "histogram_type,column_name,actual_num_bins,requested_num_bins,bin_id,bin_min,bin_max,"
                   "bin_min_repr,bin_max_repr,bin_count,bin_count_distinct\n";

  // std::random_device rd;
  // std::mt19937 gen(rd());
  // std::uniform_real_distribution<> dis(0.f, 70'000.f);
  // const auto filters = generate_filters(ColumnID{3}, PredicateCondition::LessThan, gen, dis, 700000);
  // const auto filters = generate_filters(ColumnID{3}, PredicateCondition::LessThan, 1'000.f, 700'500.f, 6'995'000);
  const auto filters = generate_filters(table, ColumnID{3}, predicate_type);
  const auto filters_by_column = get_filters_by_column(filters);

  const auto row_count_by_filter = get_row_count_for_filters(table, ColumnID{3}, predicate_type, filters);

  const auto distinct_count_by_column = get_distinct_count_by_column(table, filters_by_column);
  const auto total_count = table->row_count();

  for (auto num_bins : num_bin_combinations) {
    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto column_name = table->column_name(column_id);
      std::cout << column_name << std::endl;

      const auto distinct_count = distinct_count_by_column.at(column_id);

      const auto column_data_type = table->column_data_types()[column_id];
      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        std::cout << std::chrono::system_clock::now() << " Generating EqualHeightHistogram with " << num_bins
                  << " bins...";
        start = std::chrono::high_resolution_clock::now();

        const auto equal_height_hist =
            EqualHeightHistogram<T>::from_segment(table->get_chunk(ChunkID{0})->get_segment(column_id), num_bins);

        end = std::chrono::high_resolution_clock::now();
        std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                  << std::endl;

        std::cout << std::chrono::system_clock::now() << " Generating EqualNumElementsHistogram with " << num_bins
                  << " bins...";
        start = std::chrono::high_resolution_clock::now();

        const auto equal_num_elements_hist =
            EqualNumElementsHistogram<T>::from_segment(table->get_chunk(ChunkID{0})->get_segment(column_id), num_bins);

        end = std::chrono::high_resolution_clock::now();
        std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                  << std::endl;

        std::cout << std::chrono::system_clock::now() << " Generating EqualWidthHistogram with " << num_bins
                  << " bins...";
        start = std::chrono::high_resolution_clock::now();

        const auto equal_width_hist =
            EqualWidthHistogram<T>::from_segment(table->get_chunk(ChunkID{0})->get_segment(column_id), num_bins);

        end = std::chrono::high_resolution_clock::now();
        std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                  << std::endl;

        histogram_log << equal_height_hist->bins_to_csv(false, column_name, num_bins);
        histogram_log << equal_num_elements_hist->bins_to_csv(false, column_name, num_bins);
        histogram_log << equal_width_hist->bins_to_csv(false, column_name, num_bins);
        histogram_log.flush();

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;
          const auto value = pair.second;

          // std::cout << " " << predicate_condition_to_string.left.at(predicate_condition) << " " << value << std::endl;

          const auto actual_count = row_count_by_filter.at(column_id).at(predicate_condition).at(value);

          // std::cout << std::chrono::system_clock::now() << " Estimating with EqualHeightHistogram...";
          // start = std::chrono::high_resolution_clock::now();

          const auto equal_height_hist_count = estimate_cardinality<T>(equal_height_hist, predicate_condition, value);

          // end = std::chrono::high_resolution_clock::now();
          // std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
          //           << std::endl;
          //
          // std::cout << std::chrono::system_clock::now() << " Estimating with EqualNumElementsHistogram...";
          // start = std::chrono::high_resolution_clock::now();

          const auto equal_num_elements_hist_count =
              estimate_cardinality<T>(equal_num_elements_hist, predicate_condition, value);

          // end = std::chrono::high_resolution_clock::now();
          // std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
          //           << std::endl;
          //
          // std::cout << std::chrono::system_clock::now() << " Estimating with EqualWidthHistogram...";
          // start = std::chrono::high_resolution_clock::now();

          const auto equal_width_hist_count = estimate_cardinality<T>(equal_width_hist, predicate_condition, value);

          // end = std::chrono::high_resolution_clock::now();
          // std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
          //           << std::endl;

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

  return 0;
}
