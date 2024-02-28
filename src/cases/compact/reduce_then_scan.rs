use core::sync::atomic::{Ordering, AtomicU64};
use std::sync::atomic::AtomicUsize;
use crate::cases::compact::{compact_sequential, count_sequential, BLOCK_COUNT, MIN_BLOCK_SIZE};
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;

#[derive(Copy, Clone)]
struct Data<'a> {
  mask: u64,
  input: &'a [u64],
  temp: &'a [AtomicU64],
  output: &'a [AtomicU64],
  output_count: &'a AtomicUsize,
  block_size: u64,
  block_count: u64
}

pub fn create_task(mask: u64, input: &[u64], temp: &[AtomicU64], output: &[AtomicU64], output_count: &AtomicUsize) -> Task {
  let len = output.len();
  let mut block_size = (len as u64 + BLOCK_COUNT - 1) / BLOCK_COUNT;
  let mut block_count = BLOCK_COUNT;
  if block_size < MIN_BLOCK_SIZE {
    block_size = MIN_BLOCK_SIZE;
    block_count = (len as u64 + MIN_BLOCK_SIZE - 1) / MIN_BLOCK_SIZE;
  }
  Task::new_dataparallel::<Data>(phase1_run, phase1_finish, Data{ mask, input, temp, output, output_count, block_size, block_count }, block_count as u32, false)
}

fn phase1_run(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  workassisting_loop!(loop_arguments, |block_index| {
    // Local reduce
    let start = block_index as usize * data.block_size as usize;
    let end = ((block_index as usize + 1) * data.block_size as usize).min(data.output.len());
    let local = count_sequential(data.mask, &data.input[start .. end]);
    data.temp[block_index as usize].store(local as u64, Ordering::Relaxed);
  });
}

fn phase1_finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let data = unsafe { TaskObject::take_data(task) };
  // Phase 2
  // Scan the values of data.temp
  let mut accumulator = 0;
  for field in data.temp {
    let old = field.load(Ordering::Relaxed);
    field.store(accumulator, Ordering::Relaxed);
    accumulator += old;
  }
  data.output_count.store(accumulator as usize, Ordering::Relaxed);

  let block_count = data.block_count;
  workers.push_task(Task::new_dataparallel(phase3_run, phase3_finish, data, block_count as u32, false))
}

fn phase3_run(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  workassisting_loop!(loop_arguments, |block_index| {
    let start = block_index as usize * data.block_size as usize;

    // Exclusive upper bound of this block.
    // The last element of this block is already correct, so we do not iterate over that, hence the `- 1`.
    // In case of the last block, we may have a shorter block.
    let end = (start + data.block_size as usize - 1).min(data.output.len());

    let initial = data.temp[block_index as usize].load(Ordering::Relaxed);
    compact_sequential(data.mask, &data.input[start .. end], &data.output, initial as usize);
  });
}

fn phase3_finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
