#pragma once

#include "../jit_types.hpp"
#include "abstract_jittable.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/table.hpp"

namespace opossum {

class AbstractExpression;
class JitExpression;

/* Base class for all segment readers.
 * We need this class, so we can store a number of JitSegmentReaders with different template
 * specializations in a common data structure.
 */
class BaseJitSegmentReader {
 public:
  virtual ~BaseJitSegmentReader() = default;
#if JIT_OLD_LAZY_LOAD
  virtual void increment() = 0;
  virtual void read_value(JitRuntimeContext& context) const = 0;
#else
  virtual void read_value(JitRuntimeContext& context) = 0;
#endif
};

// data_type and tuple_value._data_type are different for value id columns as data_type describes the actual type of the
// column and tuple_value._data_type describes the data type in the jit code which is DataTypeValueID for value ids.
struct JitInputColumn {
  ColumnID column_id;
  DataType data_type;
  JitTupleValue tuple_value;
  bool use_value_id;
};

struct JitInputLiteral {
  AllTypeVariant value;
  JitTupleValue tuple_value;
  bool use_value_id;
};

struct JitInputParameter {
  ParameterID parameter_id;
  JitTupleValue tuple_value;
  std::optional<AllTypeVariant> value;
  bool use_value_id;
};

struct JitValueIDPredicate {
  const size_t input_column_index;
  const JitExpressionType expression_type;
  const std::optional<size_t> input_literal_index;
  const std::optional<size_t> input_parameter_index;
};

/* JitReadTuples must be the first operator in any chain of jit operators.
 * It is responsible for:
 * 1) storing literal values to the runtime tuple before the query is executed
 * 2) reading data from the the input table to the runtime tuple
 * 3) advancing the segment iterators
 * 4) keeping track of the number of values in the runtime tuple. Whenever
 *    another operator needs to store a temporary value in the runtime tuple,
 *    it can request a slot in the tuple from JitReadTuples.
 */
class JitReadTuples : public AbstractJittable {
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
    JitSegmentReader(const Iterator& iterator, const JitTupleValue& tuple_value)
        : _iterator{iterator}, _tuple_index{tuple_value.tuple_index()} {}

    // Reads a value from the _iterator into the _tuple_value and increments the _iterator.
#if JIT_OLD_LAZY_LOAD
    void increment() final { ++_iterator; }
    void read_value(JitRuntimeContext& context) const final {
#else
    void read_value(JitRuntimeContext& context) final {
#if JIT_LAZY_LOAD && !JIT_OLD_LAZY_LOAD
      const size_t current_offset = context.chunk_offset;
      _iterator += current_offset - _chunk_offset;
      _chunk_offset = current_offset;
#endif
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
#if JIT_LAZY_LOAD && !JIT_OLD_LAZY_LOAD
    size_t _chunk_offset = 0;
#endif
  };

 public:
  explicit JitReadTuples(const bool has_validate = false,
                         const std::shared_ptr<AbstractExpression>& row_count_expression = nullptr);

  std::string description() const final;

  virtual void before_query(const Table& in_table, JitRuntimeContext& context) const;
  virtual void before_chunk(const Table& in_table, const ChunkID chunk_id, JitRuntimeContext& context) const;

  JitTupleValue add_input_column(const DataType data_type, const bool is_nullable, const ColumnID column_id,
                                 const bool use_value_id = false);
  JitTupleValue add_literal_value(const AllTypeVariant& value, const bool use_value_id = false);
  JitTupleValue add_parameter_value(const DataType data_type, const bool is_nullable, const ParameterID parameter_id,
                                    const bool use_value_id = false);
  void add_value_id_predicate(const JitExpression& jit_expression);
  size_t add_temporary_value();

  void set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters);

  std::vector<JitInputColumn> input_columns() const;
  std::vector<JitInputLiteral> input_literals() const;
  std::vector<JitInputParameter> input_parameters() const;
  std::vector<JitValueIDPredicate> value_id_predicates() const;

  std::optional<ColumnID> find_input_column(const JitTupleValue& tuple_value) const;
  std::optional<AllTypeVariant> find_literal_value(const JitTupleValue& tuple_value) const;

  void execute(JitRuntimeContext& context) const;

  std::shared_ptr<AbstractExpression> row_count_expression() const;

 protected:
  uint32_t _num_tuple_values{0};
  std::vector<JitInputColumn> _input_columns;
  std::vector<JitInputLiteral> _input_literals;
  std::vector<JitInputParameter> _input_parameters;
  std::vector<JitValueIDPredicate> _value_id_predicates;

 private:
  void _consume(JitRuntimeContext& context) const final {}
  const bool _has_validate;
  const std::shared_ptr<AbstractExpression> _row_count_expression;
};

}  // namespace opossum
