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
    printf("Usage: ./main test-case input-size (thread-count)\n");
    return 0;
  }

  // Parse input size
  int size = std::stoi(argv[2]);
  if (size <= 0) {
    printf("input-size should be positive.\n");
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

  } else if (std::strcmp(argv[1], "scan-inplace-sequential") == 0) {
    run(
      [&] () { fill(size, input); },
      [&] () { test_sequential_scan(size, input, input); }
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
