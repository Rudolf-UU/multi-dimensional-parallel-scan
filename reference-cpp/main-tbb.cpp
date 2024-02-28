#include <oneapi/tbb.h>
#include "common.h"

void test_parallel_scan(int size, uint64_t* input, uint64_t* output) {
  oneapi::tbb::parallel_scan(
    oneapi::tbb::blocked_range<int>(0, size, 1024),
    0,
    [input, output](const oneapi::tbb::blocked_range<int>& r, int sum, bool is_final_scan) -> int {
      int temp = sum;
      int rend = r.end();
      for (int i=r.begin(); i<rend; ++i) {
        temp = temp + input[i];
        if (is_final_scan) {
          output[i] = temp;
        }
      }

      return temp;
    },
    []( int left, int right ) {
      return left + right;
    }
  );
}

// Measures the ratio of work of the scan that was executed in a sequential mode (single pass).
float measure_parallel_scan(int size, uint64_t* input, uint64_t* output) {
  std::atomic<uint> non_final_count(0);
  std::atomic<uint>* non_final_count_ptr = &non_final_count;

  oneapi::tbb::parallel_scan(
    oneapi::tbb::blocked_range<int>(0, size, 1024),
    0,
    [input, output, non_final_count_ptr](const oneapi::tbb::blocked_range<int>& r, int sum, bool is_final_scan) -> int {
      int temp = sum;
      int rend = r.end();
      if (!is_final_scan) {
        non_final_count_ptr->fetch_add(rend - r.begin());
      }
      for (int i=r.begin(); i<rend; ++i) {
        temp = temp + input[i];
        if (is_final_scan) {
          output[i] = temp;
        }
      }

      return temp;
    },
    []( int left, int right ) {
      return left + right;
    }
  );

  return ((float) size - (float) non_final_count) / (float) size;
}

void test_parallel_compact(uint64_t mask, int size, uint64_t* input, uint64_t* output) {
  oneapi::tbb::parallel_scan(
    oneapi::tbb::blocked_range<int>(0, size, 1024),
    0,
    [mask, input, output](const oneapi::tbb::blocked_range<int>& r, int sum, bool is_final_scan) -> int {
      int temp = sum;
      int rend = r.end();
      for (int i=r.begin(); i<rend; ++i) {
        uint64_t value = input[i];
        if (predicate(mask, value)) {
          temp++;
          if (is_final_scan) {
            output[i] = temp;
          }
        }
      }

      return temp;
    },
    []( int left, int right ) {
      return left + right;
    }
  );
}

int main(int argc, char *argv[]) {
  if (argc < 4) {
    printf("Usage: ./main-tbb test-case input-size thread-count\n");
    return 0;
  }

  // Parse input size
  int size = std::stoi(argv[2]);
  if (size <= 0) {
    printf("input-size should be positive.\n");
    return 0;
  }

  // Configure TBB with maximal thread count
  int thread_count = std::stoi(argv[3]);
  if (thread_count <= 0) {
    printf("thread count should be positive.\n");
    return 0;
  }
  tbb::global_control global_limit = tbb::global_control(tbb::global_control::max_allowed_parallelism, thread_count);

  // Allocate input and output arrays
  uint64_t* input = new uint64_t[size];
  uint64_t* output = new uint64_t[size];

  fill(size, input);

  // Switch on test case
  if (std::strcmp(argv[1], "scan-tbb") == 0) {
    run(
      [&] () {},
      [&] () { test_parallel_scan(size, input, output); }
    );

  } else if (std::strcmp(argv[1], "scan-inplace-tbb") == 0) {
    run(
      [&] () { fill(size, input); },
      [&] () { test_parallel_scan(size, input, input); }
    );

  } else if (std::strcmp(argv[1], "compact-2-tbb") == 0 || std::strcmp(argv[1], "compact-8-tbb") == 0) {
    int ratio = std::strcmp(argv[1], "compact-2-tbb") == 0 ? 2 : 8;
    int mask = ratio - 1;

    run(
      [&] () {},
      [&] () { test_parallel_compact(mask, size, input, output); }
    );

  } else if (std::strcmp(argv[1], "scan-measure-ratio") == 0) {
    int thread_count = std::stoi(argv[3]);
    if (thread_count <= 0) {
      printf("thread count should be positive.\n");
      return 0;
    }
    tbb::global_control global_limit = tbb::global_control(tbb::global_control::max_allowed_parallelism, thread_count);

    // Warm-up run
    measure_parallel_scan(size, input, output);

    float value = 0.0;
    for (int j = 0; j < RUNS; j++) {
      value += measure_parallel_scan(size, input, output);
    }
    printf("%f\n", value / RUNS);

  } else if (std::strcmp(argv[1], "scan-measure-ratio-inplace") == 0) {
    int thread_count = std::stoi(argv[3]);
    if (thread_count <= 0) {
      printf("thread count should be positive.\n");
      return 0;
    }
    tbb::global_control global_limit = tbb::global_control(tbb::global_control::max_allowed_parallelism, thread_count);

    // Warm-up run
    measure_parallel_scan(size, input, input);

    float value = 0.0;
    for (int j = 0; j < RUNS; j++) {
      fill(size, input);
      value += measure_parallel_scan(size, input, input);
    }
    printf("%f\n", value / RUNS);

  } else {
    printf("Unknown test case.\n");
  }

  return 0;
}
