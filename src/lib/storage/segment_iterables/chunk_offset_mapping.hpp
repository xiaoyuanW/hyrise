#pragma once

#include <unordered_map>
#include <vector>

#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Mapping between chunk offset into a reference segment and
 * its dereferenced counter part, i.e., a reference into the
 * referenced value or dictionary segment.
 */
struct ChunkOffsetMapping {
  ChunkOffsetMapping(ChunkOffset into_referencing, ChunkOffset into_referenced)
      : into_referencing(into_referencing), into_referenced(into_referenced) {}
  ChunkOffset into_referencing;  // chunk offset into reference segment
  ChunkOffset into_referenced;   // used to access values in the referenced data segment
};

/**
 * @brief list of chunk offset mappings
 */
using ChunkOffsetsList = std::vector<ChunkOffsetMapping>;

using ChunkOffsetsIterator = ChunkOffsetsList::const_iterator;
using ChunkOffsetsByChunkID = std::vector<ChunkOffsetsList>;

ChunkOffsetsByChunkID split_pos_list_by_chunk_id(const PosList& pos_list, const size_t number_of_chunks);

}  // namespace opossum
