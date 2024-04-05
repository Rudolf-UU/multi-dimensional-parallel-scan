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
    let shape = [1000, 100000]; //[3, 30000000] [10000, 1000] [100000, 1000] [5, 15000000] [10000, 10000] [1300, 100000]
    let input = unsafe { utils::array::MultArray::new(shape) };
    let output = unsafe { utils::array::MultArray::new(shape) };
    let output2 = unsafe { utils::array::MultArray::new(shape) };

    let temp = multi_chained::create_temp(&input);
    // let temp2 = chained::create_temp();
    fill(input.get_data());

    let name = "Prefix-sum (n = ".to_owned() + &(input.get_data().len()).to_formatted_string(&Locale::en) + &format!(" -- {:?}", shape) + ")"; //
    benchmark(
        ChartStyle::WithKey,
        &name,
        || {},
        || { reference_sequential_multidim(&input, &output) }
      )
      .parallel("Multidimensional - Columnwise2", 6, None, true, || {}, |thread_count| {
        let task = multi_chained::init_single(&input, &temp, &output2, 3);
        Workers::run(thread_count, task);
        // for i in 1 .. output2.get_data().len() {
        //   if output2.get_data()[i].load(Ordering::Relaxed) != output.get_data()[i].load(Ordering::Relaxed) {
        //     println!("some block is skipped :(");
        //   }
        // }
        compute_output(&output2.get_data())
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
      .cpp_sequential(cpp_enabled, "Reference C++", "scan-sequential", input.get_data().len());
      //.cpp_tbb(cpp_enabled, "oneTBB", 1, None, "scan-tbb", size)
      //.cpp_parlay(cpp_enabled, "ParlayLib", 8, None, "scan-parlay", size);
  }
}

pub fn run_inplace(cpp_enabled: bool) {
  for size in [SIZE] {
    let temp = chained::create_temp();
    let values = unsafe { utils::array::alloc_undef_u64_array(size) };

    let values2 = unsafe { utils::array::MultArray::new([size]) };
    let temp2 = multi_chained::create_temp(&values2);

    let name = "Prefix-sum inplace (n = ".to_owned() + &(size).to_formatted_string(&Locale::en) + ")";
    benchmark(
        if size < SIZE { ChartStyle::WithKey } else { ChartStyle::WithoutKey },
        &name,
        || { fill(&values) },
        || { reference_sequential_single(&values, &values) }
      )
      
      .parallel("Multidimensional - Columnwise2", 6, None, true, || { fill(&values2.get_data()) }, |thread_count| {
        let task = multi_chained::init_single(&values2, &temp2, &values2, 3);
        Workers::run(thread_count, task);
        // for i in 0 .. values2.get_data().len() {
        //   if values2.get_data()[i].load(Ordering::Relaxed) != values[i].load(Ordering::Relaxed) {
        //     println!("some block is skipped :( {:?}, {:?}, {:?}, {:?}", i, values2.get_data()[i].load(Ordering::Relaxed), values[i].load(Ordering::Relaxed), thread_count);
        //   }
        // }
        compute_output(&values2.get_data())
      })
      .parallel("Adaptive chained scan", 6, None, true, || { fill(&values) }, |thread_count| {
        let task = our_chained::init_single(&values, &temp, &values);
        Workers::run(thread_count, task);
        compute_output(&values)
      })
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

pub fn run_inplace_multidim(cpp_enabled: bool) {
  for size in [SIZE] {
    let shape = [1000, 100000]; //[3, 30000000] [10000, 1000] [100000, 1000] [5, 15000000] [10000, 10000] [1300, 100000]
    let values = unsafe { utils::array::MultArray::new(shape) };
    let output2 = unsafe { utils::array::MultArray::new(shape) };
    let temp2 = multi_chained::create_temp(&output2);
    let temp = multi_chained::create_temp(&values);
    let name = "Prefix-sum inplace (n = ".to_owned() + &(values.get_data().len()).to_formatted_string(&Locale::en) + &format!(" -- {:?}", shape) + ")";
    
    benchmark(
        if size < SIZE { ChartStyle::WithKey } else { ChartStyle::WithoutKey },
        &name,
        || { fill(&values.get_data()) },
        || { reference_sequential_multidim(&values, &values) }
      )
      .parallel("Multidimensional - Columnwise2", 6, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = multi_chained::init_single(&values, &temp, &values, 3);
        Workers::run(thread_count, task);
        // for i in 0 .. output2.get_data().len() {
        //   if i < 9998193 && output2.get_data()[i].load(Ordering::Relaxed) != values.get_data()[i].load(Ordering::Relaxed) {
        //     println!("some block is skipped :( {:?}, {:?}, {:?}, {:?}", i, output2.get_data()[i].load(Ordering::Relaxed), values.get_data()[i].load(Ordering::Relaxed), thread_count);
        //   }
        // }
        compute_output(&values.get_data())
      })
      .parallel("Parallel row-based", 5, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = parallel_rowbased::create_task(&values, &values);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      
      // .parallel("Adaptive chained scan", 5, None, true, || {}, |thread_count| {
      //   let task = our_chained::init_single(&input.get_data(), &temp2, &output.get_data());
      //   Workers::run(thread_count, task);
      //   compute_output(&output.get_data())
      // })
      .parallel("Multidimensional - Columnwise", 7, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = multi_chained::init_single(&values, &temp, &values, 0);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      .parallel("Multidimensional - In-order", 4, None, true, || { fill(&values.get_data()) }, |thread_count| {
        let task = multi_chained::init_single(&values, &temp, &values, 2);
        Workers::run(thread_count, task);
        compute_output(&values.get_data())
      })
      // .parallel("Multiple-rows", 7, None, true, || {}, |thread_count| {
      //   let task = multi_chained::init_single(&input, &temp, &output, 6);
      //   Workers::run(thread_count, task);
      //   compute_output(&output.get_data())
      // })
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
