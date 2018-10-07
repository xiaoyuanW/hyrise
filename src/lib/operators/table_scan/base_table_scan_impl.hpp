#pragma once

#include <array>
#include <functional>
#include <memory>

#include <x86intrin.h>

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
   * @defgroup The hot loop of the table scan
   * @{
   */

  template <bool CheckForNull, typename Functor, typename LeftIterator>
  void __attribute__((noinline)) _scan(const Functor& func, LeftIterator left_it, LeftIterator left_end,
                                             const ChunkID chunk_id, PosList& matches_out) {
    // Can't use a default argument for this because default arguments are non-type deduced contexts
    _scan<CheckForNull>(func, left_it, left_end, chunk_id, matches_out, std::false_type{});
  }

  // The signature of this method is a bit complex because it is used for different types of scans: Unary value scans,
  // between scans, and LIKE scans. We try to resolve as many things at compile time as possible. If no right iterator
  // is passed, `constexpr` codes make sure that the related code is not used. While left and right might be
  // independently nullable, we only have one CheckForNull template parameter, as having two would make the  template
  // definition harder to understand for cases where we only have a single iterator.
  template <bool CheckForNull, typename Functor, typename LeftIterator, typename RightIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline)) _scan(const Functor& func, LeftIterator left_it, LeftIterator left_end,
                                             const ChunkID chunk_id, PosList& matches_out, RightIterator right_it) {
    // This entire if-block is an optimization to enable auto-vectorization/SIMD. If you are looking at the scan for the
    // first time, first read the part below this block. The scan works even if this block is removed. Because it has no
    // benefit for iterators that block vectorization (mostly iterators that do not operate on contiguous storage), it
    // is only enabled for std::vector (currently used by FixedSizeByteAlignedVector). Also, the AnySegmentIterator is
    // not vectorizable because it relies on virtual method calls. While the check for `IS_DEBUG` is redudant, it makes
    // people aware of this. Unfortunately, vectorization is only really beneficial when we can use AVX-512VL. However,
    // since this branch is not slower on CPUs without it, we still use it there as well, as this reduces the divergence
    // across different systems.
    if constexpr (!IS_DEBUG && LeftIterator::IsVectorizable) {
      // Concept: Partition the vector into blocks of BLOCK_SIZE entries. The remainder is handled outside of this optimization. For each
      // row write 0 to `offsets` if the row does not match, or `chunk_offset + 1` if the row matches. The reason why we need `+1` is given below. This set can be
      // parallelized using auto-vectorization/SIMD. Afterwards, add all matching rows into `matches_out`.

      auto matches_out_index = matches_out.size();
      constexpr long SIMD_SIZE = 64;  // Assuming a SIMD register size of 512 bit
      constexpr long BLOCK_SIZE = SIMD_SIZE / sizeof(ValueID);

      // Continue doing this until we have too few rows left to run over a whole block
      while (left_end - left_it > BLOCK_SIZE) {
        alignas(SIMD_SIZE) std::array<ChunkOffset, SIMD_SIZE / sizeof(ChunkOffset)> offsets;

        // The pragmas promise to the compiler that there are no data dependencies within the loop. If you run into any
        // issues with the optimization, make sure that you only have only set IsVectorizable on iterators that use
        // linear storage and where the access methods do not change any state.
        //
        // Also, when using clang, this causes an error to be thrown if the loop could not be vectorized. This, however
        // does not guarantee that every instruction in the loop is using SIMD.
        ;  // clang-format off
        #pragma GCC ivdep
        #pragma clang loop vectorize(assume_safety)
        // clang-format on
        for (auto i = 0l; i < BLOCK_SIZE; ++i) {
          const auto& left = *left_it;

          bool matches;
          if constexpr(std::is_same_v<RightIterator, std::false_type>) {
            matches = (!CheckForNull | !left.is_null()) & func(left.value());
          } else {
            const auto& right = *left_it;
            matches = (!CheckForNull | (!left.is_null() & !right.is_null())) & func(left.value(), right.value());
          }

          // If the row matches, write its offset plus one into `offsets`, otherwise write 0. We need to increment the
          // offset because otherwise a zero would also be written for offset 0. This is safe because the last
          // possible chunk offset is defined as INVALID_CHUNK_OFFSET anyway.
          offsets[i] = matches * (left.chunk_offset() + 1);

          ++left_it;
          if constexpr(!std::is_same_v<RightIterator, std::false_type>) ++right_it;
        }

        // As we write directly into the matches_out vector, make sure that is has enough size
        if (matches_out_index + BLOCK_SIZE >= matches_out.size()) {
          matches_out.resize((BLOCK_SIZE + matches_out.size()) * 3, RowID{chunk_id, 0});
        }

        // Now write the matches into matches_out. For better understanding, first look at the non-AVX12VL block.
#ifdef __AVX512VL__
        // Build a mask where a bit indicates if the row in `offsets` matched the criterion.
        const auto mask = _mm512_cmpneq_epu32_mask(*(__m512i*)&offsets, __m512i{});

        // Compress `offsets`, that is move all values where the mask is set to 1 to the front. This is essentially
        // std::remove(offsets.begin(), offsets.end(), ChunkOffset{0});
        *(__m512i*)&offsets = _mm512_maskz_compress_epi32(mask, *(__m512i*)&offsets);

        // Copy all offsets into `matches_out` - even those that are set to 0. This does not matter because they will
        // be overwritten in the next round anyway. Copying more than necessary is better than stopping at the number
        // of matching rows because we do not need a branch for this.
        for (auto i = 0; i < BLOCK_SIZE; ++i) {
          matches_out[matches_out_index + i].chunk_offset = offsets[i] - 1;
        }

        // Count the number of matches and increase the index of the next write to matches_out accordingly
        matches_out_index += __builtin_popcount(mask);
#else
        for (auto i = 0; i < BLOCK_SIZE; ++i) {
          if (offsets[i]) {
            matches_out[matches_out_index++].chunk_offset = offsets[i] - 1;
          }
        }
#endif
      }

      // Remove all the entries that we have overallocated
      matches_out.resize(matches_out_index);
    }

    // Do the remainder the easy way. If we did not use the optimization above, left_it was not yet touched, so we
    // iterate over the entire input data.
    for (; left_it != left_end; ++left_it) {
      if constexpr(std::is_same_v<RightIterator, std::false_type>) {
        const auto left = *left_it;
        if ((!CheckForNull || !left.is_null()) && func(left.value())) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
      } else {
        const auto left = *left_it;
        const auto right = *right_it;
        if ((!CheckForNull || (!left.is_null() && !right.is_null())) && func(left.value(), right.value())) {
          matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
        }
        ++right_it;
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
