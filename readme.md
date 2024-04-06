# Multidimensional Parallel Scans for Multi-Core CPUs <!-- Zero-Overhead Parallel Scans for CPUs -->

# Still needs to be updated!

This repository is based on the [zero-overhead-parallel-scan](https://github.com/ivogabe/zero-overhead-parallel-scans). It contains several parallel scan algorithms
for multi-dimensional data structures, including an algorithm that has zero overhead compared to sequential scan when executed on a single core, as well as zero overhead
compared to the adaptive chained scan presented in the 'zero-overhead' paper. 

The scan algorithms are implemented using the [work-assisting scheduler](https://github.com/ivogabe/workassisting), but could also be applied with other schedulers.

<!-- This repository contains implementations of three parallel scan algorithms for multi-core CPUs which do not need to fix the number of available
cores at the start, and have zero overhead compared to sequential scans when executed on a single core. These two
properties are in contrast with most existing parallel scan algorithms, which are asymptotically optimal, but have a
constant factor overhead compared to sequential scans when executed on a single core.
We achieve these properties by adapting the classic three-phase scan algorithms. The resulting algorithms exhibit
better performance than the original ones on multiple cores. Furthermore, we adapt the chained scan with decoupled
look-back algorithm to also have these two properties.  While this algorithm was originally designed for GPUs, we show
it is also suitable for multi-core CPUs, outperforming the classic three-phase scans
in our benchmarks, by better using the caches of the processor at the cost of more synchronisation.
In general our *adaptive chained scan* is the fastest parallel scan, but in specific situations our *assisted reduce-then-scan* is better.

The scan algorithms are implemented using the [work-assisting scheduler](https://github.com/ivogabe/workassisting), but could also be applied with other schedulers. -->

## Instructions
To run the benchmarks, the Rust compiler and cargo need to be installed. Furthermore gnuplot needs to be installed, as the benchmark code automatically generates charts of the results. The benchmarks can be run with `cargo run`. The generated charts and tables are placed in `./results`. Depending on the processor, it may be needed to tune `AFFINITY_MAPPING` in `./src/utils/thread_pinning.rs`. This specifies the order in which the cores of the processor are used.

The program will ask if a sequential implementation in C++ should be enabled. This requires Linux, clang++ and cmake. When enabled, it will automatically build and execute the reference C++ implementation.

<!-- To run the benchmarks, the Rust compiler and cargo need to be installed. Furthermore gnuplot needs to be installed, as the benchmark code automatically generates charts of the results. The benchmarks can be run with `cargo run`. The generated charts and tables are placed in `./results`. Depending on the processor, it may be needed to tune `AFFINITY_MAPPING` in `./src/utils/thread_pinning.rs`. This specifies the order in which the cores of the processor are used.

The program will ask if a sequential implementation in C++ and parallel implementations in oneTBB and ParlayLib should be enabled. This requires Linux, clang++, cmake and git. When enabled, it will automatically download and install oneTBB and ParlayLib locally in `./reference-cpp`. -->