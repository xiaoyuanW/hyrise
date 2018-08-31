#include "generate_table_statistics.hpp"

#include <unordered_set>

#include "base_column_statistics.hpp"
#include "generate_column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

namespace opossum {

TableStatistics generate_table_statistics(const std::shared_ptr<const Table>& table, const int64_t num_bins) {
  std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics;
  column_statistics.reserve(table->column_count());

  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    const auto column_data_type = table->column_data_types()[column_id];

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      if (num_bins == 0) {
        column_statistics.emplace_back(generate_column_statistics<ColumnDataType>(table, column_id));
      } else {
        column_statistics.emplace_back(generate_column_statistics<ColumnDataType>(table, column_id, num_bins));
      }
    });
  }

  return {table->type(), static_cast<float>(table->row_count()), column_statistics};
}

}  // namespace opossum
