use core::sync::atomic::{Ordering, AtomicU64};
use num_format::{Locale, ToFormattedString};
use crate::core::worker::*;
use crate::utils;
use crate::utils::benchmark::{benchmark, ChartStyle};

mod zero_overhead;
mod row_column_chained;
mod sequential_rowbased;
mod rowwise_chained;
mod columnwise_chained;

// Const block_size used during scanning
pub const BLOCK_SIZE:u64 = 1024 * 4;
// One dimensional input size used for the one-dim prefix sum/inplace prefix sum
pub const ONEDIM_SIZE: usize = 1024 * 1024 * 64;
// Input shapes used for the multidimensional prefix sum/inplace prefix sum
pub const MULTIDIM_SHAPES:[[usize;2];4] = [[10000, 10000], [4000, 25000], [4, 25000000], [100000, 1000]];
// Higher dimensional input, in comparison to the original input: [10000, 10000]
pub const HIGHERDIM_SHAPE:[[usize;3];1] = [[100, 100, 10000]];
// Four-dimensional input
pub const FOURDIM_SHAPE:[[usize;4];1] = [[100, 100, 100, 100]];

pub fn run(cpp_enabled: bool) { // One-dimensional prefix sum
  for size in [ONEDIM_SIZE] {
    let input = unsafe { utils::array::MultArray::new([size]) };
    let output = unsafe { utils::array::MultArray::new([size]) };
    let temp = row_column_chained::create_temp(&input);
    fill(&input.get_data());

    let name = "Prefix-sum (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ")";
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_single(&input.get_data(), &output.get_data()) }
      )
      .parallel("Adaptive chained", 7, Some(13), false, || {}, |thread_count| {
        let task = zero_overhead::init_single(&input.get_data(), &temp, &output.get_data());
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Assisting column-wise chained", 6, None, true, || {}, |thread_count| {
        let task = row_column_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-sequential", size, size, 1);
  }
}

pub fn run_multidim(cpp_enabled: bool) { // Multidimensional prefix sum
  for shape in MULTIDIM_SHAPES {
    let input = unsafe { utils::array::MultArray::new(shape) };
    let output = unsafe { utils::array::MultArray::new(shape) };
    let temp = row_column_chained::create_temp(&input);
    fill(input.get_data());

    let name = "Prefix-sum (sh = ".to_owned() + &format!("{:?}", shape) + ")"; //
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_multidim(&input.get_data(), &output.get_data(), input.get_inner_size(), input.total_inner_count()) }
      )
      .parallel("Sequential row-based", 5, None, false, || {}, |thread_count| {
        let task = sequential_rowbased::create_task(&input, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Column-wise chained", 7, None, true, || {}, |thread_count| {
        let task = columnwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Row-wise chained", 8, None, true, || {}, |thread_count| {
        let task = rowwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Assisting column-wise chained", 6, None, true, || {}, |thread_count| {
        let task = row_column_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-multidim-sequential", input.get_data().len(), input.get_inner_size(), input.total_inner_count());
  }
}

pub fn run_inplace(cpp_enabled: bool) { // One-dimensional in-place prefix sum
  for size in [ONEDIM_SIZE] {
    let values = unsafe { utils::array::MultArray::new([size]) };
    let temp = row_column_chained::create_temp(&values);

    let name = "In-place prefix-sum (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ")";
    benchmark(
        ChartStyle::WithKey,
        &name,
        || { fill(&values.get_data()) },
        || { reference_sequential_single(&values.get_data(), &values.get_data()) }
      )
      .parallel("Adaptive chained", 7, Some(13), false, || { fill(&values.get_data()) }, |thread_count| {
        let task = zero_overhead::init_single(&values.get_data(), &temp, &values.get_data());
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Assisting column-wise chained", 6, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = row_column_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-inplace-sequential", size, size, 1);
  }
}

pub fn run_inplace_multidim(cpp_enabled: bool) { // Multidimensional in-place prefix sum
  for shape in MULTIDIM_SHAPES {
    let values = unsafe { utils::array::MultArray::new(shape) };
    let temp = row_column_chained::create_temp(&values);
  
    let name = "In-place prefix-sum (sh = ".to_owned() + &format!("{:?}", shape) + ")";
    benchmark(
        ChartStyle::WithKey,
        &name,
        || { fill(&values.get_data()) },
        || { reference_sequential_multidim(&values.get_data(), &values.get_data(), values.get_inner_size(), values.total_inner_count()) }
      )
      .parallel("Sequential row-based", 5, None, false, || { fill(&values.get_data()) }, |thread_count| {
        let task = sequential_rowbased::create_task(&values, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Column-wise chained", 7, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = columnwise_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Row-wise chained", 8, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = rowwise_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Assisting column-wise chained", 6, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = row_column_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference sequential C++", "scan-inplace-multidim-sequential", values.get_data().len(), values.get_inner_size(), values.total_inner_count());
  }
}

pub fn run_rowwise_vs_columnwise() { // Prefix sum comparison between row-wise and column-wise scanning
  for shape in [[10000, 10000]] {
    let input = unsafe { utils::array::MultArray::new(shape) };
    let output = unsafe { utils::array::MultArray::new(shape) };
    let temp = row_column_chained::create_temp(&input);
    fill(input.get_data());

    let name = "Row vs Column (sh = ".to_owned() + &format!("{:?}", shape) + ")"; //
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_multidim(&input.get_data(), &output.get_data(), input.get_inner_size(), input.total_inner_count()) }
      )
      .parallel("Column-wise chained", 7, None, true, || {}, |thread_count| {
        let task = columnwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Row-wise chained", 8, None, true, || {}, |thread_count| {
        let task = rowwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      });
  }
}

pub fn run_custom_shape<const N: usize, const M:usize>(shapes: [[usize; N]; M]) { // Multidimensional prefix sum, to show that the performance is indepent of the number of dimensions
  for shape in shapes {
    let input = unsafe { utils::array::MultArray::new(shape) };
    let output = unsafe { utils::array::MultArray::new(shape) };
    let temp = row_column_chained::create_temp(&input);
    fill(input.get_data());

    let name = "Prefix-sum (sh = ".to_owned() + &format!("{:?}", shape) + ")"; //
    benchmark(
        ChartStyle::WithoutKey,
        &name,
        || {},
        || { reference_sequential_multidim(&input.get_data(), &output.get_data(), input.get_inner_size(), input.total_inner_count()) }
      )
      .parallel("Sequential row-based", 5, None, false, || {}, |thread_count| {
        let task = sequential_rowbased::create_task(&input, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Column-wise chained", 7, None, true, || {}, |thread_count| {
        let task = columnwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Row-wise chained", 8, None, true, || {}, |thread_count| {
        let task = rowwise_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Assisting column-wise chained", 6, None, true, || {}, |thread_count| {
        let task = row_column_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      });
  }
}

pub fn fill(values: &[AtomicU64]) {
  for (idx, value) in values.iter().enumerate() {
    value.store(random(idx as u64) as u64, Ordering::Relaxed);
  }
}

pub fn compute_output(output: &[AtomicU64]) -> u64 {
  output[1].load(Ordering::Relaxed) + output[98238].load(Ordering::Relaxed) + output[output.len() - 123].load(Ordering::Relaxed) + output[output.len() - 1].load(Ordering::Relaxed)
}

pub fn reference_sequential_single(input: &[AtomicU64], output: &[AtomicU64]) -> u64 {
  scan_sequential(input, 0, output);
  compute_output(output)
}

pub fn reference_sequential_multidim(input: &[AtomicU64], output: &[AtomicU64], row_length: usize, row_count: usize) -> u64 {
  for i in 0 .. row_count {
    scan_sequential(&input[i*row_length .. (i+1)*row_length], 0, &output[i*row_length .. (i+1)*row_length]);
  }
  compute_output(output)
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
