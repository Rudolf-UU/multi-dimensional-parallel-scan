use core::sync::atomic::{Ordering, AtomicU64};
use num_format::{Locale, ToFormattedString};
use crate::core::worker::*;
use crate::utils;
use crate::utils::benchmark::{benchmark, ChartStyle};

mod chained;
mod our_chained;
pub mod our_reduce_then_scan;
pub mod our_scan_then_propagate;
mod reduce_then_scan;
mod scan_then_propagate;
mod multi_chained;
mod parallel_rowbased;

pub const SIZE: usize = 1024 * 1024 * 64;
pub const BLOCK_SIZE:u64 = 1024 * 4;

pub fn run(cpp_enabled: bool) {
  for size in [SIZE] {
    let temp = chained::create_temp();
    let input = unsafe { utils::array::alloc_undef_u64_array(size) };
    let output = unsafe { utils::array::alloc_undef_u64_array(size) };
    fill(&input);

    let name = "Prefix-sum (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ")";
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_single(&input, &output) }
      )
      .parallel("Adaptive chained scan", 6, None, true, || {}, |thread_count| {
        let task = our_chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output)
      })
      .parallel("Scan-then-propagate", 3, Some(13), false, || {}, |thread_count| {
        let task = scan_then_propagate::create_task(&input, &output);
        Workers::run(thread_count, task);
        compute_output(&output)
      })
      .parallel("Reduce-then-scan", 5, None, false, || {}, |thread_count| {
        let task = reduce_then_scan::create_task(&input, &output);
        Workers::run(thread_count, task);
        compute_output(&output)
      })
      .parallel("Chained scan", 7, None, false, || {}, |thread_count| {
        let task = chained::init_single(&input, &temp, &output);
        Workers::run(thread_count, task);
        compute_output(&output)
      })
      .parallel("Assisted scan-t.-prop.", 2, Some(12), true, || {}, |thread_count| {
        let task = our_scan_then_propagate::create_task(&input, &output, None);
        Workers::run(thread_count, task);
        compute_output(&output)
      })
      .parallel("Assisted reduce-t.-scan", 4, None, true, || {}, |thread_count| {
        let task = our_reduce_then_scan::create_task(&input, &output, None);
        Workers::run(thread_count, task);
        compute_output(&output)
      })
      
      .cpp_sequential(cpp_enabled, "Reference C++", "scan-sequential", size)
      .cpp_tbb(cpp_enabled, "oneTBB", 1, None, "scan-tbb", size)
      .cpp_parlay(cpp_enabled, "ParlayLib", 8, None, "scan-parlay", size);
  }
}

pub fn run_multidim(cpp_enabled: bool) {
  for size in [SIZE] {
    let shape = [1300, 100000]; //[3, 30000000] [10000, 1000] [100000, 1000] [5, 15000000] [10000, 10000] [1300, 100000]
    let input = unsafe { utils::array::MultArray::new(shape) };
    let output = unsafe { utils::array::MultArray::new(shape) };
    let temp = multi_chained::create_temp(&input);
    // let temp2 = chained::create_temp();
    mult_fill(&input);

    // let shape2 = [2,3,5]; //[3, 30000000] [10000, 1000] [100000, 1000] [5, 15000000] [10000, 10000] [1300, 100000]
    // let input2 = unsafe { utils::array::MultArray::new(shape2) };
    // let output2 = unsafe { utils::array::MultArray::new(shape2) };
    // let temp2 = multi_chained::create_temp(&input2);
    // mult_fill(&input2);

    // println!("input size {:?}", input.get_data().len());
    // let dim = input.get_shape();
    // let inner = input.get_inner_size();
    // let count_inner = input.total_inner_count();
    // println!("Shape {:?}", dim);
    // println!("Inner {:?}", inner);
    // println!("Inner count {:?}", count_inner);

    let name = "Prefix-sum (n = ".to_owned() + &(input.get_data().len()).to_formatted_string(&Locale::en) + " -- [1300, 100000]" + ")"; //
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_single_mult(&input, &output) }
      )
      .parallel("Multidimensional - Columnwise2", 6, None, true, || {}, |thread_count| {
        let task = multi_chained::init_single(&input, &temp, &output, 3);
        Workers::run(thread_count, task);
        //println!("test: {:?}", output.get_data()[input.get_data().len()-1]);
        compute_output(&output.get_data())
      })
      // .parallel("Adaptive chained scan", 5, None, true, || {}, |thread_count| {
      //   let task = our_chained::init_single(&input.get_data(), &temp2, &output.get_data());
      //   Workers::run(thread_count, task);
      //   compute_output(&output.get_data())
      // })
      .parallel("Multidimensional - Columnwise", 7, None, true, || {}, |thread_count| {
        let task = multi_chained::init_single(&input, &temp, &output, 0);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .parallel("Multidimensional - In-order", 4, None, true, || {}, |thread_count| {
        let task = multi_chained::init_single(&input, &temp, &output, 2);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      // .parallel("Multiple-rows", 7, None, true, || {}, |thread_count| {
      //   let task = multi_chained::init_single(&input, &temp, &output, 6);
      //   Workers::run(thread_count, task);
      //   compute_output(&output.get_data())
      // })
      .parallel("Parallel row-based", 5, None, true, || {}, |thread_count| {
        let task = parallel_rowbased::create_task(&input, &output);
        Workers::run(thread_count, task);
        compute_output(&output.get_data())
      })
      .cpp_sequential(cpp_enabled, "Reference C++", "scan-sequential", size)
      .cpp_tbb(cpp_enabled, "oneTBB", 1, None, "scan-tbb", size)
      .cpp_parlay(cpp_enabled, "ParlayLib", 8, None, "scan-parlay", size);
  }
}

pub fn run_inplace(cpp_enabled: bool) {
  for size in [SIZE] {
    let temp = chained::create_temp();
    let values = unsafe { utils::array::alloc_undef_u64_array(size) };
    let name = "Prefix-sum inplace (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ")";
    benchmark(
        if size < SIZE { ChartStyle::WithKey } else { ChartStyle::WithoutKey },
        &name,
        || { fill(&values) },
        || { reference_sequential_single(&values, &values) }
      )
      .parallel("Scan-then-propagate", 3, Some(13), false, || { fill(&values) }, |thread_count| {
        let task = scan_then_propagate::create_task(&values, &values);
        Workers::run(thread_count, task);
        compute_output(&values)
      })
      .parallel("Reduce-then-scan", 5, None, false, || { fill(&values) }, |thread_count| {
        let task = reduce_then_scan::create_task(&values, &values);
        Workers::run(thread_count, task);
        compute_output(&values)
      })
      .parallel("Chained scan", 7, None, false, || { fill(&values) }, |thread_count| {
        let task = chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values)
      })
      .parallel("Assisted scan-t.-prop.", 2, Some(12), true, || { fill(&values) }, |thread_count| {
        let task = our_scan_then_propagate::create_task(&values, &values, None);
        Workers::run(thread_count, task);
        compute_output(&values)
      })
      .parallel("Assisted reduce-t.-scan", 4, None, true, || { fill(&values) }, |thread_count| {
        let task = our_reduce_then_scan::create_task(&values, &values, None);
        Workers::run(thread_count, task);
        compute_output(&values)
      })
      .parallel("Adaptive chained scan", 6, None, true, || { fill(&values) }, |thread_count| {
        let task = our_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values)
      })
      .cpp_sequential(cpp_enabled, "Reference C++", "scan-inplace-sequential", size)
      .cpp_tbb(cpp_enabled, "oneTBB", 1, None, "scan-inplace-tbb", size)
      .cpp_parlay(cpp_enabled, "ParlayLib", 8, None, "scan-inplace-parlay", size);
  }
}

pub fn fill(values: &[AtomicU64]) {
  for (idx, value) in values.iter().enumerate() {
    value.store(random(idx as u64) as u64, Ordering::Relaxed);
  }
}

pub fn mult_fill<const N: usize>(array: &utils::array::MultArray<N>) {
  let values = array.get_data();
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

pub fn reference_sequential_single_mult<const N: usize>(input: &utils::array::MultArray<N>, output: &utils::array::MultArray<N>) -> u64 {
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
