#include "jit_utils.hpp"

#include <string>

#include "all_type_variant.hpp"
#include "jit_types.hpp"
#include "resolve_type.hpp"

namespace opossum {

template <typename NewType, typename CurrentType,
          typename = typename std::enable_if_t<std::is_scalar_v<NewType> == std::is_scalar_v<CurrentType>>>
NewType cast_value_to_type(CurrentType value) {
  return value;
}
template <typename NewType, typename CurrentType,
          typename = typename std::enable_if_t<!std::is_scalar_v<NewType> && std::is_scalar_v<CurrentType>>>
std::string cast_value_to_type(CurrentType value) {
  return std::to_string(value);
}
template <typename NewType, typename CurrentType,
          typename = typename std::enable_if_t<std::is_scalar_v<NewType> && !std::is_scalar_v<CurrentType>>>
NewType cast_value_to_type(std::string value) {
  Fail("String to number conversions not supported.");
}

AllTypeVariant cast_all_type_variant_to_type(const AllTypeVariant& variant, const DataType data_type) {
  if (data_type == DataType::Null) return NULL_VALUE;

  const auto current_type = data_type_from_all_type_variant(variant);

  if (current_type == data_type) return variant;

  if (variant_is_null(variant)) Fail("Cannot convert null variant");

  // optinal as all type variants cannot be assigned to other variants
  AllTypeVariant casted_variant;

  resolve_data_type(current_type, [&](const auto current_data_type_t) {
    using CurrentType = typename decltype(current_data_type_t)::type;
    resolve_data_type(data_type, [&](const auto new_data_type_t) {
      using NewType = typename decltype(new_data_type_t)::type;
      casted_variant = cast_value_to_type<NewType, CurrentType>(get<CurrentType>(variant));
    });
  });

  DebugAssert(data_type_from_all_type_variant(casted_variant) == data_type, "Casting failed.");

  return casted_variant;
}

JitExpressionType swap_expression_type(const JitExpressionType expression_type) {
  switch (expression_type) {
    case JitExpressionType::GreaterThan:
      return JitExpressionType::LessThanEquals;
    case JitExpressionType::GreaterThanEquals:
      return JitExpressionType::LessThan;
    case JitExpressionType::LessThan:
      return JitExpressionType::GreaterThanEquals;
    case JitExpressionType::LessThanEquals:
      return JitExpressionType::GreaterThan;
    default:
      return expression_type;
  }
}

}  // namespace opossum
