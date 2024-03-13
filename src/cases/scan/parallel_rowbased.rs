use core::sync::atomic::{Ordering, AtomicU64};
use std::ops::Div;
use crate::cases::scan::fold_sequential;
use crate::cases::scan::scan_sequential;
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;
use crate::utils::array::MultArray;

pub const SIZE: usize = crate::cases::scan::SIZE;
const BLOCK_SIZE: u64 = 1024 * 4;

pub fn create_temp() -> Box<[BlockInfo]> {
  (0 .. (SIZE as u64 + BLOCK_SIZE - 1) / BLOCK_SIZE).map(|_| BlockInfo{
    state: AtomicU64::new(STATE_INITIALIZED), aggregate: AtomicU64::new(0), prefix: AtomicU64::new(0)
  }).collect()
}

pub fn init_single<const N: usize>(input: &MultArray<N>, temp: &[BlockInfo], output: &MultArray<N>) -> Task {
  reset(temp);
  create_task(input, temp, output)
}

pub fn reset(temp: &[BlockInfo]) {
    for i in 0 .. temp.len() {
      temp[i].state.store(STATE_INITIALIZED, Ordering::Relaxed);
      temp[i].aggregate.store(0, Ordering::Relaxed);
      temp[i].prefix.store(0, Ordering::Relaxed);
    }
}

struct Data<'a> {
  input: &'a [AtomicU64],
  temp: &'a [BlockInfo],
  output: &'a [AtomicU64],
  inner_size: u64,
}

pub struct BlockInfo {
    pub state: AtomicU64,
    pub aggregate: AtomicU64,
    pub prefix: AtomicU64
  }
  
const STATE_INITIALIZED: u64 = 0;
const STATE_AGGREGATE_AVAILABLE: u64 = 1;
const STATE_PREFIX_AVAILABLE: u64 = 2;

fn create_task<const N: usize>(input_m: &MultArray<N>, temp: &[BlockInfo], output_m: &MultArray<N>) -> Task {
  let inner_size = input_m.get_inner_size();
  let inner_rows = input_m.total_inner_count();
  let input = input_m.get_data();
  let output = output_m.get_data();

  Task::new_dataparallel::<Data>(run, finish, Data{ input, temp, output, inner_size: inner_size as u64}, inner_rows as u32, false)
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

fn run_row_segments(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let mut sequential = true;
  println!("Bye");
}


fn run2(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let mut sequential = true;
  workassisting_loop!(loop_arguments, |block_index| {
    // reduce-then-scan
    let start = block_index as usize * BLOCK_SIZE as usize;
    let end = ((block_index as usize + 1) * BLOCK_SIZE as usize).min(data.input.len());

    // Check if we already have an aggregate of the previous block.
    // If that is the case, then we can perform the scan directly.
    // Otherwise we perform a reduce-then-scan over this block.
    let aggregate_start = if !sequential {
      None // Don't switch back from parallel mode to sequential mode
    } else if block_index ==  0 {
      Some(0)
    } else {
      let previous = block_index - 1;
      let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
      if previous_state == STATE_PREFIX_AVAILABLE {
        Some(data.temp[previous as usize].prefix.load(Ordering::Acquire))
      } else {
        None
      }
    };

    if let Some(aggregate) = aggregate_start {
      let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
      data.temp[block_index as usize].prefix.store(local, Ordering::Relaxed);
      data.temp[block_index as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);
    } else {
      sequential = false;
      let local = fold_sequential(&data.input[start .. end]);
      // Share own local value
      data.temp[block_index as usize].aggregate.store(local, Ordering::Relaxed);
      data.temp[block_index as usize].state.store(STATE_AGGREGATE_AVAILABLE, Ordering::Release);

      // Find aggregate
      let mut aggregate = 0;
      let mut previous = block_index - 1;

      loop {
        let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
        if previous_state == STATE_PREFIX_AVAILABLE {
          aggregate = data.temp[previous as usize].prefix.load(Ordering::Acquire) + aggregate;
          break;
        } else if previous_state == STATE_AGGREGATE_AVAILABLE {
          aggregate = data.temp[previous as usize].aggregate.load(Ordering::Acquire) + aggregate;
          previous = previous - 1;
        } else {
          // Continue looping until the state of previous block changes.
        }
      }

      // Make aggregate available
      data.temp[block_index as usize].prefix.store(aggregate + local, Ordering::Relaxed);
      data.temp[block_index as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);

      scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
    }
  });
}

fn finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
