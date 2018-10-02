#pragma once

#include "../jit_types.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"

namespace opossum {

/* Base class for all segment readers.
 * We need this class, so we can store a number of JitSegmentReaders with different template
 * specializations in a common data structure.
 */
class BaseJitSegmentReader {
 public:
  virtual ~BaseJitSegmentReader() = default;
  virtual void read_value(JitRuntimeContext& context) = 0;
};

class BaseJitSegmentReaderWrapper {
 public:
  BaseJitSegmentReaderWrapper(size_t reader_index) : reader_index(reader_index) {}
  virtual ~BaseJitSegmentReaderWrapper() = default;
  virtual void read_value(JitRuntimeContext& context) const {
    context.inputs[reader_index]->read_value(context);
  }

  const size_t reader_index;
};

/* JitSegmentReaders wrap the segment iterable interface used by most operators and makes it accessible
 * to the JitOperatorWrapper.
 *
 * Why we need this wrapper:
 * Most operators access data by creating a fixed number (usually one or two) of segment iterables and
 * then immediately use those iterators in a lambda. The JitOperatorWrapper, on the other hand, processes
 * data in a tuple-at-a-time fashion and thus needs access to an arbitrary number of segment iterators
 * at the same time.
 *
 * We solve this problem by introducing a template-free super class to all segment iterators. This allows us to
 * create an iterator for each input segment (before processing each chunk) and store these iterators in a
 * common vector in the runtime context.
 * We then use JitSegmentReader instances to access these iterators. JitSegmentReaders are templated with the
 * type of iterator they are supposed to handle. They are initialized with an input_index and a tuple value.
 * When requested to read a value, they will access the iterator from the runtime context corresponding to their
 * input_index and copy the value to their JitTupleValue.
 *
 * All segment readers have a common template-free base class. That allows us to store the segment readers in a
 * vector as well and access all types of segments with a single interface.
 */
template <typename Iterator, typename DataType, bool Nullable>
class JitSegmentReader : public BaseJitSegmentReader {
 public:
  using ITERATOR = Iterator;
  JitSegmentReader(const Iterator& iterator, const JitTupleValue& tuple_value)
      : _iterator{iterator}, _tuple_index{tuple_value.tuple_index()} {
    std::cout << "";
  }

  // Reads a value from the _iterator into the _tuple_value and increments the _iterator.
  void read_value(JitRuntimeContext& context) final {
#if JIT_LAZY_LOAD
    const size_t current_offset = context.chunk_offset;
    _iterator += current_offset - _chunk_offset;
    _chunk_offset = current_offset;
#endif
    const auto& value = *_iterator;
    // clang-format off
    if constexpr (Nullable) {
      context.tuple.set_is_null(_tuple_index, value.is_null());
      if (!value.is_null()) {
        context.tuple.set<DataType>(_tuple_index, value.value());
      }
    } else {
      context.tuple.set<DataType>(_tuple_index, value.value());
    }
    // clang-format on
#if !JIT_LAZY_LOAD
    ++_iterator;
#endif
  }

 private:
  Iterator _iterator;
  const size_t _tuple_index;
#if JIT_LAZY_LOAD
  size_t _chunk_offset = 0;
#endif
};

template <typename JitSegmentReader>
class JitSegmentReaderWrapper : public BaseJitSegmentReaderWrapper {
 public:
  // using BaseJitSegmentReaderWrapper::BaseJitSegmentReaderWrapper;
  JitSegmentReaderWrapper(size_t reader_index)
  : BaseJitSegmentReaderWrapper(reader_index) {
    std::cout << "";
  }

  // Reads a value from the _iterator into the _tuple_value and increments the _iterator.
  void read_value(JitRuntimeContext& context) const final {
    DebugAssert(std::dynamic_pointer_cast<JitSegmentReader>(context.inputs[reader_index]), "Different reader type, no " + std::to_string(reader_index));
    auto tmp = std::static_pointer_cast<JitSegmentReader>(context.inputs[reader_index]);
    tmp->read_value(context);
    // context.inputs[reader_index]->read_value(context);
  }
};

}  // namespace opossum
