#include "jit_segment_reader.hpp"

#include <boost/preprocessor/seq/for_each.hpp>

namespace opossum {

#define JIT_EXPLICIT_INSTANTIATION(r, type) \
  template class JitSegmentReaderWrapper<JitSegmentReader<AnySegmentIterator<type>, type, true>>; \
  template class JitSegmentReaderWrapper<JitSegmentReader<AnySegmentIterator<type>, type, false>>;

JIT_EXPLICIT_INSTANTIATION(_, int)
JIT_EXPLICIT_INSTANTIATION(_, long)
JIT_EXPLICIT_INSTANTIATION(_, float)
JIT_EXPLICIT_INSTANTIATION(_, double)

// JIT_EXPLICIT_INSTANTIATION(_, std::string)

// BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_INSTANTIATION, DATA_TYPES)
// BOOST_PP_SEQ_FOR_EACH(JIT_EXPLICIT_INSTANTIATION, DATA_TYPES)

#undef JIT_EXPLICIT_INSTANTIATION
// template class JitValidate<TableType::References>;

}  // namespace opossum
