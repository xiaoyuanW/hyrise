#pragma once

// no namespace so that functions are not inlined

__attribute__((noinline)) void jit_start_operator();
__attribute__((noinline)) void jit_end_operator_read();
__attribute__((noinline)) void jit_end_operator_write();
__attribute__((noinline)) void jit_end_operator_read_value();
__attribute__((noinline)) void jit_end_operator_aggregate();
__attribute__((noinline)) void jit_end_operator_limit();
__attribute__((noinline)) void jit_end_operator_filter();
__attribute__((noinline)) void jit_end_operator_compute();
__attribute__((noinline)) void jit_end_operator_validate();
