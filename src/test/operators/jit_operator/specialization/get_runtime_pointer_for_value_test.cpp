#include <llvm/IR/IRBuilder.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/llvm_extensions.hpp"

namespace opossum {

class GetRuntimePointerForValueTest : public BaseTest {
 protected:
  void SetUp() override {
    _llvm_context = std::make_shared<llvm::LLVMContext>();
    _context.module = std::make_unique<llvm::Module>("test_module", *_llvm_context);

    // Create bitcode for the following function
    //
    // define void @foo(i64*** %0) {
    // entry:
    //   %1 = getelementptr i64**, i64*** %0, i32 1
    //   %2 = load i64**, i64*** %0
    //   %3 = bitcast i64*** %0 to i32*
    //   %4 = getelementptr i32, i32* %3, i32 2
    //   %5 = getelementptr i32, i32* %4, i32 3
    //   %6 = getelementptr i64*, i64** %2, i32 4
    //   %7 = load i64*, i64** %2
    // }

    auto argument_type =
        llvm::PointerType::get(llvm::PointerType::get(llvm::Type::getInt64PtrTy(*_llvm_context), 0), 0);
    auto foo_fn = llvm::dyn_cast<llvm::Function>(
        _context.module->getOrInsertFunction("foo", llvm::Type::getVoidTy(*_llvm_context), argument_type));

    // Create a single basic block as the function's body
    auto block = llvm::BasicBlock::Create(*_llvm_context, "entry", foo_fn);
    llvm::IRBuilder<> builder(block);

    _value_0 = foo_fn->arg_begin();
    _value_1 = builder.CreateConstGEP1_32(_value_0, 1);
    _value_2 = builder.CreateLoad(_value_0);
    _value_3 = builder.CreateBitCast(_value_0, llvm::Type::getInt32PtrTy(*_llvm_context));
    _value_4 = builder.CreateConstGEP1_32(_value_3, 2);
    _value_5 = builder.CreateConstGEP1_32(_value_4, 3);
    _value_6 = builder.CreateConstGEP1_32(_value_2, 4);
    _value_7 = builder.CreateLoad(_value_2);
  }

  void assert_address_eq(const std::shared_ptr<const JitRuntimePointer>& runtime_pointer,
                         const uint64_t expected_address) const {
    auto known_runtime_pointer = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(runtime_pointer);
    ASSERT_NE(known_runtime_pointer, nullptr);
    ASSERT_TRUE(known_runtime_pointer->is_valid());
    ASSERT_EQ(known_runtime_pointer->address(), expected_address);
  }

  std::shared_ptr<llvm::LLVMContext> _llvm_context;
  SpecializationContext _context;
  llvm::Value* _value_0;
  llvm::Value* _value_1;
  llvm::Value* _value_2;
  llvm::Value* _value_3;
  llvm::Value* _value_4;
  llvm::Value* _value_5;
  llvm::Value* _value_6;
  llvm::Value* _value_7;
};

TEST_F(GetRuntimePointerForValueTest, Test1) {
  ASSERT_FALSE(GetRuntimePointerForValue(_value_0, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_1, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_2, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_3, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_4, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_5, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_6, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_7, _context)->is_valid());
}

TEST_F(GetRuntimePointerForValueTest, Test2) {
  int64_t some_value;
  int64_t* some_pointer_1 = &some_value;
  int64_t** some_pointer_2 = &some_pointer_1;
  int64_t*** some_pointer_3 = &some_pointer_2;

  _context.runtime_value_map[_value_0] = std::make_shared<JitConstantRuntimePointer>(some_pointer_3);

  assert_address_eq(GetRuntimePointerForValue(_value_0, _context), reinterpret_cast<uint64_t>(some_pointer_3));
  assert_address_eq(GetRuntimePointerForValue(_value_1, _context),
                    reinterpret_cast<uint64_t>(some_pointer_3) + 1 * sizeof(int64_t));
  assert_address_eq(GetRuntimePointerForValue(_value_2, _context), reinterpret_cast<uint64_t>(some_pointer_2));
  assert_address_eq(GetRuntimePointerForValue(_value_3, _context), reinterpret_cast<uint64_t>(some_pointer_3));
  assert_address_eq(GetRuntimePointerForValue(_value_4, _context),
                    reinterpret_cast<uint64_t>(some_pointer_3) + 2 * sizeof(int32_t));
  assert_address_eq(GetRuntimePointerForValue(_value_5, _context),
                    reinterpret_cast<uint64_t>(some_pointer_3) + (2 + 3) * sizeof(int32_t));
  assert_address_eq(GetRuntimePointerForValue(_value_6, _context),
                    reinterpret_cast<uint64_t>(some_pointer_2) + 4 * sizeof(int64_t));
  assert_address_eq(GetRuntimePointerForValue(_value_7, _context), reinterpret_cast<uint64_t>(some_pointer_1));
}

}  // namespace opossum