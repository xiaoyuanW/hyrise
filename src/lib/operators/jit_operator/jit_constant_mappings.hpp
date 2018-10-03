#pragma once

#include <boost/bimap.hpp>
#include <string>
#include <unordered_map>

#include "jit_types.hpp"

namespace opossum {

extern const boost::bimap<JitExpressionType, std::string> jit_expression_type_to_string;
extern const boost::bimap<JitOperatorType, std::string> jit_operator_type_to_string;

}  // namespace opossum
