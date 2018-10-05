#pragma once

#include <array>
#include <functional>
#include <memory>

#include <x86intrin.h> // TODO remove

#include "storage/segment_iterables.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

/**
 * @brief the base class of all table scan impls
 */
class BaseTableScanImpl {
 public:
  BaseTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                    const PredicateCondition predicate_condition)
      : _in_table{in_table}, _left_column_id{left_column_id}, _predicate_condition{predicate_condition} {}

  virtual ~BaseTableScanImpl() = default;

  virtual std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) = 0;

 protected:
  /**
   * @defgroup The hot loops of the table scan
   * @{
   */

  template <typename UnaryFunctor, typename LeftIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline)) _unary_scan(const UnaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                             const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;

      if (left.is_null()) continue;

      if (func(left.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  // Version with a constant value on the right side. Sometimes we prefer this over _unary_scan because we can use
  // with_comparator.
  template <bool LeftIsNullable, typename BinaryFunctor, typename LeftIterator, typename RightValue>
  void __attribute__((noinline))
  _unary_scan_with_value(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end, const RightValue right_value,
                         const ChunkID chunk_id, PosList& matches_out) {
    // This entire if-block is an optimization. If you are looking at _unary_scan_with_value for the first time,
    // continue below. The method works even if this block is removed. Because it has no benefit for iterators that
    // block vectorization (mostly iterators that do not operate on contiguous storage), it is only enabled for
    // std::vector (currently used by FixedSizeByteAlignedVector). Also, the AnySegmentIterator is not vectorizable
    // because it relies on virtual method calls. While the check for `IS_DEBUG` is redudant, it makes people aware of
    // this.
    if constexpr (!IS_DEBUG && LeftIterator::IsVectorizable) {
      // Concept: Partition the vector into blocks of BUFFER_SIZE entries. The remainder is handled below. For each
      // block, iterate over the input data and write the chunk offsets of matching rows into the buffer. This can be
      // parallelized using auto-vectorization/SIMD. After each block, collect the matches and add them to the result
      // vector.
      constexpr long SIMD_SIZE = 32;  // Assuming a maximum SIMD register size of 256 bit
      constexpr long BUFFER_SIZE = SIMD_SIZE / sizeof(ValueID);

      while (left_end - left_it > BUFFER_SIZE) {
        size_t match_positions = 0;
        static_assert(sizeof(match_positions) >= BUFFER_SIZE / 8, "Can't store enough flags in match_positions");
        alignas(SIMD_SIZE) ChunkOffset all_offsets[SIMD_SIZE / sizeof(ChunkOffset)];

        // {unsigned int dummy; __rdtscp(&dummy);}

        // This promises to the compiler that there are no data dependencies within the loop. If you run into any issues
        // with the optimization, make sure that you only have only set IsVectorizable on iterators that use linear
        // storage and where the access methods do not change any state.
        //
        // Also, when using clang, this causes an error to be thrown if the loop could not be vectorized. This, however
        // does not guarantee that every instruction in the loop is using SIMD.
        #pragma GCC ivdep
        #pragma clang loop vectorize(assume_safety)
        // clang-format on
        for (auto i = 0l; i < BUFFER_SIZE; ++i) {
          const auto& left = *left_it;

          all_offsets[i] = left.chunk_offset();

          const auto matches = (!LeftIsNullable | !left.is_null()) & func(left.value(), right_value);
          match_positions |= matches << i;

          ++left_it;
        }

        if (!match_positions) continue;

#ifdef __AVX512VL__
        // Do not use 512-bit registers for now because they lead to downclocking (TODO Link). Clang does not use them
        // yet either.
        alignas(SIMD_SIZE) ChunkOffset matching_offsets[SIMD_SIZE / sizeof(ChunkOffset)];
        matching_offsets.fill(ChunkOffset{0});
        _mm256_mask_compress_epi32(*(__m256i*)&all_offsets, match_positions, *(__m256i*)&matching_offsets);
        const auto num_matches = __builtin_popcount(match_positions);
        for (auto i = 0; i < num_matches; ++i) {
          std::cout << "emplace " << matching_offsets[i] << std::endl;
          matches_out.emplace_back(RowID{chunk_id, matching_offsets[i]});
        }
#else
        for (auto i = 0; i < BUFFER_SIZE; ++i) {
          if (match_positions & 1 << i) {
            std::cout << "emplace " << all_offsets[i] << std::endl;
            matches_out.emplace_back(RowID{chunk_id, all_offsets[i]});
          }
        }
#endif
      }
    }

    // Do the remainder the easy way. If we did not use the optimization above, left_it was not yet touched, so we
    // iterate over the entire input data.
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;
      if ((!LeftIsNullable || !left.is_null()) & func(left.value(), right_value)) {
        matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void __attribute__((noinline)) _binary_scan(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                              RightIterator right_it, const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;

      if (left.is_null() || right.is_null()) continue;

      if (func(left.value(), right.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  /**@}*/

 protected:
  const std::shared_ptr<const Table> _in_table;
  const ColumnID _left_column_id;
  const PredicateCondition _predicate_condition;
};

}  // namespace opossum
