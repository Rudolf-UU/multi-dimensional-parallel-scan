#include <parlay/parallel.h>
#include <parlay/primitives.h>
#include <parlay/sequence.h>
#include "common.h"

void test_parallel_compact(int mask, parlay::sequence<uint64_t> input, parlay::sequence<uint64_t> output) {
  parlay::filter_into_uninitialized(input, output, [&] (uint64_t value) {
    return predicate(mask, value);
  });
}

void parlay_fill(parlay::sequence<uint64_t> values) {
  for (int i = 0; i < values.size(); i++) {
    values[i] = randomize(i);
  }
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    printf("Usage: ./main-parlaylib test-case input-size\n");
    return 0;
  }

  // Parse input size
  int size = std::stoi(argv[2]);
  if (size <= 0) {
    printf("input-size should be positive.\n");
    return 0;
  }

  // Allocate input and output arrays
  parlay::sequence<uint64_t> input = parlay::tabulate(size, [&] (ulong i) {
    return (uint64_t) randomize((uint64_t) i);
  });

  // Switch on test case
  if (std::strcmp(argv[1], "scan-parlay") == 0) {
    run(
      [&] () {},
      [&] () { parlay::scan_inclusive(input); }
    );

  } else if (std::strcmp(argv[1], "scan-inplace-parlay") == 0) {
    run(
      [&] () { parlay_fill(input); },
      [&] () { parlay::scan_inclusive_inplace(input); }
    );

  } else if (std::strcmp(argv[1], "compact-2-parlay") == 0 || std::strcmp(argv[1], "compact-8-parlay") == 0) {
    int ratio = std::strcmp(argv[1], "compact-2-parlay") == 0 ? 2 : 8;
    int mask = ratio - 1;

    parlay::sequence<uint64_t> output = parlay::tabulate(size, [&] (ulong i) { return (uint64_t) 0; });

    run(
      [&] () {},
      [&] () { test_parallel_compact(mask, input, output); }
    );

  } else {
    printf("Unknown test case.\n");
  }

  return 0;
}
