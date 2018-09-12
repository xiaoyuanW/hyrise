#pragma once

#include "all_type_variant.hpp"

namespace opossum {

enum class JitExpressionType;

AllTypeVariant cast_all_type_variant_to_type(const AllTypeVariant& variant, const DataType data_type);

JitExpressionType swap_expression_type(const JitExpressionType expression_type);

}  // namespace opossum
