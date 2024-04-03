use core::sync::atomic::{Ordering, AtomicU64};
use crate::cases::scan::scan_sequential;
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;
use crate::utils::array::MultArray;

const BLOCK_SIZE: u64 = crate::cases::scan::BLOCK_SIZE;

struct Data<'a> {
  input: &'a [AtomicU64],
  output: &'a [AtomicU64],
  seg_count: u64, // Added this now!!!
  inner_size: u64,
}

pub fn create_task<const N: usize>(input_m: &MultArray<N>, output_m: &MultArray<N>) -> Task {
  let inner_size = input_m.get_inner_size();
  let inner_rows = input_m.total_inner_count();
  let input = input_m.get_data();
  let output = output_m.get_data();
  let blocks_per_row = ((inner_size as u64 + BLOCK_SIZE - 1) / (BLOCK_SIZE)) as u64;

  Task::new_dataparallel::<Data>(run, finish, Data{ input, output, inner_size: inner_size as u64, seg_count:blocks_per_row}, inner_rows as u32, false)
}

fn run(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
    let data = unsafe { TaskObject::get_data(task) };
    // let mut sequential = true;
    //println!("Hi");

    workassisting_loop!(loop_arguments, |block_index| {
      let start = block_index as usize * data.inner_size as usize;
      let end = (start + data.inner_size as usize).min(data.input.len());

      scan_sequential(&data.input[start .. end], 0, &data.output[start .. end]);
    });
    
}

fn run2(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  // let mut sequential = true;
  //println!("Hi");

  workassisting_loop!(loop_arguments, |block_index| {
    let offset = block_index as usize * data.inner_size as usize;
    let mut aggregate = 0;

    for i in 0..data.seg_count {
      let start = offset + (i * BLOCK_SIZE) as usize;
      let end = offset + (((i+1) * BLOCK_SIZE) as usize).min(data.inner_size as usize);

      let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
      aggregate = local;
    }
    
  });
  
}

fn finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
