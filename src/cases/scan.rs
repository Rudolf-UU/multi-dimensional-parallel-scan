use core::sync::atomic::{Ordering, AtomicU64};
use num_format::{Locale, ToFormattedString};
use crate::core::worker::*;
use crate::utils;
use crate::utils::benchmark::{benchmark, ChartStyle};

mod zero_overhead;
mod column_row_chained;
mod parallel_rowbased;
mod rowwise_chained;
mod columnwise_chained;

pub const SIZE: usize = 1024 * 1024 * 64;
pub const BLOCK_SIZE:u64 = 1024 * 4;
pub const MULTIDIM_SHAPES:[[usize;2];4] = [[8192, 8192], [2048, 32768], [4, 16777216], [65536,1024]];

pub fn run(cpp_enabled: bool) {
  for size in [SIZE] {
    let input = unsafe { utils::array::MultArray::new([size]) };
    let output = unsafe { utils::array::MultArray::new([size]) };
    let temp = column_row_chained::create_temp(&input);

    fill(&input.get_data());

    let name = "Prefix-sum (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ")";
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_single(&input.get_data(), &output.get_data()) }
      )
      .parallel("Adaptive chained", 7, None, false, || {}, |thread_count| {
        let task = zero_overhead::init_single(&input.get_data(), &temp, &output.get_data());
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Column-row chained", 6, None, true, || {}, |thread_count| {
        let task = column_row_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-sequential", size, size, 1);
  }
}

pub fn run_multidim(cpp_enabled: bool) {
  for shape in MULTIDIM_SHAPES {
    let input = unsafe { utils::array::MultArray::new(shape) };
    let output = unsafe { utils::array::MultArray::new(shape) };
    let temp = column_row_chained::create_temp(&input);

    fill(input.get_data());

    let name = "Prefix-sum -- m (n = ".to_owned() + &(input.get_data().len()).to_formatted_string(&Locale::en) + &format!(" -- {:?}", shape) + ")"; //
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_multidim(&input, &output) }
      )
      .parallel("Sequential row-based", 3, None, false, || {}, |thread_count| {
        let task = parallel_rowbased::create_task(&input, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Column-wise chained", 5, None, true, || {}, |thread_count| {
        let task = columnwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Row-wise chained", 7, None, true, || {}, |thread_count| {
        let task = rowwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      
      .parallel("Column-row chained", 6, None, true, || {}, |thread_count| {
        let task = column_row_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-multidim-sequential", input.get_data().len(), input.get_inner_size(), input.total_inner_count());
  }
}

pub fn run_inplace(cpp_enabled: bool) {
  for size in [SIZE] {
    let values = unsafe { utils::array::MultArray::new([size]) };
    let temp = column_row_chained::create_temp(&values);

    let name = "Prefix-sum inplace (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ")";
    benchmark(
        ChartStyle::WithKey,
        &name,
        || { fill(&values.get_data()) },
        || { reference_sequential_single(&values.get_data(), &values.get_data()) }
      )
      .parallel("Adaptive chained", 7, None, false, || { fill(&values.get_data()) }, |thread_count| {
        let task = zero_overhead::init_single(&values.get_data(), &temp, &values.get_data());
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Column-row chained", 6, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = column_row_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-inplace-sequential", size, size, 1);
  }
}

pub fn run_inplace_multidim(cpp_enabled: bool) {
  for shape in MULTIDIM_SHAPES {
    let values = unsafe { utils::array::MultArray::new(shape) };
    let temp = column_row_chained::create_temp(&values);
    let name = "Prefix-sum inplace -- m (n = ".to_owned() + &(values.get_data().len()).to_formatted_string(&Locale::en) + &format!(" -- {:?}", shape) + ")";
    
    benchmark(
        ChartStyle::WithKey,
        &name,
        || { fill(&values.get_data()) },
        || { reference_sequential_multidim(&values, &values) }
      )
      .parallel("Sequential row-based", 3, None, false, || { fill(&values.get_data()) }, |thread_count| {
        let task = parallel_rowbased::create_task(&values, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Column-wise chained", 5, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = column_row_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Row-wise chained", 7, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = column_row_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Column-row chained", 6, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = column_row_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-inplace-sequential", values.get_data().len(), values.get_inner_size(), values.total_inner_count());
  }
}

pub fn fill(values: &[AtomicU64]) {
  for (idx, value) in values.iter().enumerate() {
    value.store(random(idx as u64) as u64, Ordering::Relaxed);
  }
}

pub fn compute_output(output: &[AtomicU64]) -> u64 {
  output[0].load(Ordering::Relaxed) + output[98238].load(Ordering::Relaxed) + output[output.len() - 123].load(Ordering::Relaxed) + output[output.len() - 1].load(Ordering::Relaxed)
}

pub fn reference_sequential_single(input: &[AtomicU64], output: &[AtomicU64]) -> u64 {
  scan_sequential(input, 0, output);
  compute_output(output)
}

pub fn reference_sequential_multidim<const N: usize>(input: &utils::array::MultArray<N>, output: &utils::array::MultArray<N>) -> u64 {
  let rows = input.total_inner_count();
  let size = input.get_inner_size();
  let input_data = input.get_data();
  let output_data = output.get_data();

  for i in 0 .. rows {
    scan_sequential(&input_data[i*size .. (i+1)*size], 0, &output_data[i*size .. (i+1)*size]);
  }
  
  compute_output(output_data)
}

pub fn scan_sequential(input: &[AtomicU64], initial: u64, output: &[AtomicU64]) -> u64 {
  let mut accumulator = initial;
  assert_eq!(input.len(), output.len());
  for i in 0 .. output.len() {
    accumulator += input[i].load(Ordering::Relaxed);
    output[i].store(accumulator, Ordering::Relaxed);
  }
  accumulator
}

pub fn fold_sequential(array: &[AtomicU64]) -> u64 {
  let mut accumulator = 0;
  for value in array {
    accumulator += value.load(Ordering::Relaxed);
  }
  accumulator
}

fn random(mut seed: u64) -> u32 {
  seed ^= seed << 13;
  seed ^= seed >> 17;
  seed ^= seed << 5;
  seed as u32
}
