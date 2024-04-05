#include <cstring>
#include <string>
#include "common.h"

void __attribute__ ((noinline)) test_sequential_scan(int size, uint64_t* input, uint64_t* output) {
  int accum = 0;
  for (int i = 0; i < size; i++) {
    accum += input[i];
    output[i] = accum;
  }
}

void __attribute__ ((noinline)) test_multidim_sequential_scan(int size, int row_length, int row_count, uint64_t* input, uint64_t* output) {
  int start = 0;
  int accum = 0;

  for (int i = 0; i < row_count; i++) {
    start = i * row_length;
    accum = 0;
    for (int j = 0; j < row_length; j++) {
      accum += input[start + j];
      output[start + j] = accum;
    }  
  }

}

void __attribute__ ((noinline)) test_sequential_compact(uint64_t mask, int size, uint64_t* input, uint64_t* output) {
  int output_index = 0;
  for (int i = 0; i < size; i++) {
    uint64_t value = input[i];
    if (predicate(mask, value)) {
      output[output_index] = value;
      output_index++;
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    printf("Usage: ./main test-case input-size row-length row-count (thread-count)\n");
    return 0;
  }

  // Parse input size
  int size = std::stoi(argv[2]);
  if (size <= 0) {
    printf("input-size should be positive.\n");
    return 0;
  }

  // Parse shape information
  int row_length = std::stoi(argv[3]);
  if (row_length <= 0) {
    printf("row-length should be positive.\n");
    return 0;
  }
  int row_count = std::stoi(argv[4]);
  if (row_count <= 0) {
    printf("row-count should be positive.\n");
    return 0;
  }

  if (row_count * row_length != size) {
    printf("combination of row-count and row-length should match the input-size\n");
    return 0;
  }

  // Allocate input and output arrays
  uint64_t* input = new uint64_t[size];
  uint64_t* output = new uint64_t[size];

  fill(size, input);

  // Switch on test case
  if (std::strcmp(argv[1], "scan-sequential") == 0) {
    run(
      [&] () {},
      [&] () { test_sequential_scan(size, input, output); }
    );

  } else if (std::strcmp(argv[1], "scan-multidim-sequential") == 0) {
    run(
      [&] () {},
      [&] () { test_multidim_sequential_scan(size, row_length, row_count, input, output); }
    );

  } else if (std::strcmp(argv[1], "scan-inplace-sequential") == 0) {
    run(
      [&] () { fill(size, input); },
      [&] () { test_sequential_scan(size, input, input); }
    );
  } else if (std::strcmp(argv[1], "scan-inplace-multidim-sequential") == 0) {
    run(
      [&] () { fill(size, input); },
      [&] () { test_multidim_sequential_scan(size, row_length, row_count, input, input); }
    );
  } else if (std::strcmp(argv[1], "compact-2-sequential") == 0 || std::strcmp(argv[1], "compact-8-sequential") == 0) {
    int ratio = std::strcmp(argv[1], "compact-2-sequential") == 0 ? 2 : 8;
    int mask = ratio - 1;
    run(
      [&] () { fill(size, input); },
      [&] () { test_sequential_compact(mask, size, input, output); }
    );

  } else {
    printf("Unknown test case.\n");
  }

  return 0;
}
