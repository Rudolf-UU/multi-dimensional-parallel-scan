#include <chrono>

#define RUNS 50

uint randomize(uint64_t seed) {
  seed ^= seed << 13;
  seed ^= seed >> 17;
  seed ^= seed << 5;
  return (uint) seed;
}

bool predicate(uint64_t mask, uint64_t value) {
  value ^= value >> 11;
  value ^= value << 7;
  value ^= value >> 5;
  return (value & mask) == mask;
}

void fill(int size, uint64_t* values) {
  for (int i = 0; i < size; i++) {
    values[i] = randomize(i);
  }
}

template<class P, class F>
void run(P prepare, F f) {
  // Warm-up run
  prepare();
  f();

  std::chrono::steady_clock::duration total(0);
  for (int j = 0; j < RUNS; j++) {
    prepare();
    auto before = std::chrono::steady_clock::now();
    f();
    auto t = std::chrono::steady_clock::now() - before;
    total += t;
  }

  auto msec = std::chrono::duration_cast<std::chrono::microseconds>(total);
  printf("%ld\n", msec.count() / RUNS);
}
