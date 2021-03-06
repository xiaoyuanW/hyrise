#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This is the base class for all SegmentAccessor types.
 * It provides the common interface to access individual values of a segment.
 */
template <typename T>
class BaseSegmentAccessor {
 public:
  virtual const std::optional<T> access(ChunkOffset offset) const = 0;
  virtual ~BaseSegmentAccessor() {}
};

}  // namespace opossum
