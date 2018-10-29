#include "jit_segment_reader.hpp"

#include <boost/preprocessor/seq/for_each.hpp>
#include "storage/value_segment/value_segment_iterable.hpp"
#include "storage/reference_segment/reference_segment_iterable.hpp"
#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"

namespace opossum {

#define U_INTS (uint8_t)(uint16_t)(uint32_t)

#define JIT_ACTUAL_INSTANTIOAN(Iterator, Type, Nullable) \
  template class JitSegmentReader<Iterator, Type, Nullable>; \
  template class JitSegmentReaderWrapper<JitSegmentReader<Iterator, Type, Nullable>>;

#define JIT_EXPLICIT_INSTANTIATION_W_TYPE(r, _, type) \
  template class AnySegmentIterator<type>; \
  JIT_ACTUAL_INSTANTIOAN(AnySegmentIterator<type>, type, true) \
  JIT_ACTUAL_INSTANTIOAN(AnySegmentIterator<type>, type, false) \
  template class ValueSegmentIterable<type>; \
  JIT_ACTUAL_INSTANTIOAN(ValueSegmentIterable<type>::Iterator, type, true) \
  JIT_ACTUAL_INSTANTIOAN(ValueSegmentIterable<type>::NonNullIterator, type, false) \
  JIT_ACTUAL_INSTANTIOAN(ReferenceSegmentIterable<type>::MultipleChunkIterator, type, true) \
  template class ReferenceSegmentIterable<type>; \
  JIT_ACTUAL_INSTANTIOAN(ReferenceSegmentIterable<type>::MultipleChunkIterator, type, false)

// pmr_vector

JIT_ACTUAL_INSTANTIOAN(AttributeVectorIterable::Iterator<std::vector<uint8_t>::const_iterator>, JitValueID, false)
JIT_ACTUAL_INSTANTIOAN(AttributeVectorIterable::Iterator<std::vector<uint16_t>::const_iterator>, JitValueID, false)
JIT_ACTUAL_INSTANTIOAN(AttributeVectorIterable::Iterator<std::vector<uint32_t>::const_iterator>, JitValueID, false)
JIT_ACTUAL_INSTANTIOAN(AttributeVectorIterable::Iterator<std::vector<uint8_t>::const_iterator>, JitValueID, true)
JIT_ACTUAL_INSTANTIOAN(AttributeVectorIterable::Iterator<std::vector<uint16_t>::const_iterator>, JitValueID, true)
JIT_ACTUAL_INSTANTIOAN(AttributeVectorIterable::Iterator<std::vector<uint32_t>::const_iterator>, JitValueID, true)

#define JIT_EXPLICIT_INSTANTIATION(r, _, type) JIT_EXPLICIT_INSTANTIATION_W_TYPE(r, _, BOOST_PP_TUPLE_ELEM(3, 0, type))

// template class JitSegmentReader<ValueSegmentIterable<type>::Iterator, type, true>; \
  // template class JitSegmentReader<ValueSegmentIterable<type>::NonNullIterator, type, false>; \

// JIT_EXPLICIT_INSTANTIATION(_, int)
// JIT_EXPLICIT_INSTANTIATION(_, long)
// JIT_EXPLICIT_INSTANTIATION(_, float)
// JIT_EXPLICIT_INSTANTIATION(_, double)
// template class DictionarySegmentIterable<int32_t, pmr_vector<uint8_t>>;
// template class DictionarySegmentIterable<int, pmr_vector<int>>;
// template class DictionarySegmentIterable<std::string, opossum::FixedStringVector>;

// template class DictionarySegmentIterable<int, pmr_vector<int>>;

// template class DictionarySegmentIterable<int, pmr_vector<int>>::Iterator<FixedSizeByteAlignedVector<uint8_t>>;

// template class JitSegmentReaderWrapper<JitSegmentReader<DictionarySegmentIterable<int, pmr_vector<int>>::Iterator<FixedSizeByteAlignedVector<uint8_t>>, int, false>>;


#define TYPES_WO_STRING (int32_t)(int64_t)(float)(double)(std::string)
BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_INSTANTIATION, _, DATA_TYPE_INFO)

// JIT_EXPLICIT_INSTANTIATION(_, std::string)

// BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_INSTANTIATION, DATA_TYPES)

#undef JIT_EXPLICIT_INSTANTIATION
#undef JIT_EXPLICIT_INSTANTIATION_W_TYPE

}  // namespace opossum
