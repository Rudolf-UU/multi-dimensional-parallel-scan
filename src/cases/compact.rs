use core::sync::atomic::{Ordering, AtomicU64};
use std::sync::atomic::AtomicUsize;
use num_format::{Locale, ToFormattedString};
use crate::core::worker::*;
use crate::utils;
use crate::utils::benchmark::{benchmark_with_max_speedup, ChartStyle};

mod chained;
mod reduce_then_scan;
mod scan_then_propagate;
mod our_chained;
mod our_reduce_then_scan;
mod our_scan_then_propagate;

pub const SIZE: usize = 1024 * 1024 * 256;
pub const BLOCK_COUNT: u64 = 32 * 8;
pub const MIN_BLOCK_SIZE: u64 = 1024;

pub fn run(cpp_enabled: bool) {
  for size in [SIZE / 8, SIZE] {
    for ratio in [2, 8] {
      let input = create_input(size);
      // Array to store the aggregates of all blocks
      let temp1 = unsafe { utils::array::alloc_undef_u64_array(size) };
      let temp2 = unsafe { utils::array::alloc_undef_u64_array(BLOCK_COUNT as usize) };
      let temp3 = chained::create_temp(size);
      let output = unsafe { utils::array::alloc_undef_u64_array(size) };
      let name = "Compact (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ", r = 1/" + &ratio.to_string() + ")";
      let mask = ratio - 1; // Assumes ratio is a power of two
      benchmark_with_max_speedup(
          ChartStyle::WithoutKey,
          &name,
          || {},
          || reference_sequential_single(mask, &input, &output),
          24,
          6
        )
        .parallel("Scan-then-propagate", 3, Some(13), false, || {}, |thread_count| {
          let output_count = AtomicUsize::new(0);
          let task = scan_then_propagate::create_task(mask, &input, &temp1, &output, &output_count);
          Workers::run(thread_count, task);
          compute_output(&output, output_count.load(Ordering::Relaxed))
        })
        .parallel("Reduce-then-scan", 5, None, false, || {}, |thread_count| {
          let output_count = AtomicUsize::new(0);
          let task = reduce_then_scan::create_task(mask, &input, &temp2, &output, &output_count);
          Workers::run(thread_count, task);
          compute_output(&output, output_count.load(Ordering::Relaxed))
        })
        .parallel("Chained scan", 7, None, false, || {}, |thread_count| {
          let output_count = AtomicUsize::new(0);
          let task = chained::create_task(mask, &input, &temp3, &output, &output_count);
          Workers::run(thread_count, task);
          compute_output(&output, output_count.load(Ordering::Relaxed))
        })
        .parallel("Assisted scan-t.-prop.", 2, Some(12), true, || {}, |thread_count| {
          let output_count = AtomicUsize::new(0);
          let task = our_scan_then_propagate::create_task(mask, &input, &temp1, &output, &output_count);
          Workers::run(thread_count, task);
          compute_output(&output, output_count.load(Ordering::Relaxed))
        })
        .parallel("Assisted reduce-t.-scan", 4, None, true, || {}, |thread_count| {
          let output_count = AtomicUsize::new(0);
          let task = our_reduce_then_scan::create_task(mask, &input, &temp2, &output, &output_count);
          Workers::run(thread_count, task);
          compute_output(&output, output_count.load(Ordering::Relaxed))
        })
        .parallel("Adaptive chained scan", 6, None, true, || {}, |thread_count| {
          let output_count = AtomicUsize::new(0);
          let task = our_chained::create_task(mask, &input, &temp3, &output, &output_count);
          Workers::run(thread_count, task);
          compute_output(&output, output_count.load(Ordering::Relaxed))
        })
        .cpp_sequential(cpp_enabled, "Reference C++", &("compact-".to_owned() + &ratio.to_string() + "-sequential"), size)
        .cpp_tbb(cpp_enabled, "oneTBB", 1, None, &("compact-".to_owned() + &ratio.to_string() + "-tbb"), size)
        .cpp_parlay(cpp_enabled, "ParlayLib", 8, None, &("compact-".to_owned() + &ratio.to_string() + "-parlay"), size);
    }
  }
}

pub fn create_input(size: usize) -> Box<[u64]> {
  (0..size).map(|x| random(x as u64) as u64).collect()
}

pub fn compute_output(output: &[AtomicU64], count: usize) -> (usize, u64) {
  (
    count,
    output[0].load(Ordering::Relaxed) + output[98238].load(Ordering::Relaxed) + output[count - 1].load(Ordering::Relaxed)
  )
}

pub fn compact_sequential(mask: u64, input: &[u64], output: &[AtomicU64], mut output_index: usize) -> usize {
  for &value in input {
    if predicate(mask, value) {
      unsafe { output.get_unchecked(output_index) }.store(value, Ordering::Relaxed);
      output_index += 1;
    }
  }

  output_index
}

pub fn reference_sequential_single(mask: u64, input: &[u64], output: &[AtomicU64]) -> (usize, u64) {
  let output_count = compact_sequential(mask, input, output, 0);
  compute_output(output, output_count)
}

pub fn count_sequential(mask: u64, input: &[u64]) -> usize {
  let mut count = 0;
  for &value in input {
    if predicate(mask, value) {
      count += 1;
    }
  }
  count
}

pub fn scan_indices_sequential(mask: u64, input: &[u64], output: &[AtomicU64]) -> usize {
  assert_eq!(input.len(), output.len());
  let mut count = 0;
  for i in 0 .. input.len() {
    if predicate(mask, input[i]) {
      count += 1;
    }
    output[i].store(count as u64, Ordering::Relaxed);
  }
  count
}

fn random(mut seed: u64) -> u32 {
  seed ^= seed << 13;
  seed ^= seed >> 17;
  seed ^= seed << 5;
  seed as u32
}

pub fn predicate(mask: u64, mut value: u64) -> bool {
  value ^= value >> 11;
  value ^= value << 7;
  value ^= value >> 5;
  (value & mask) == mask
}
