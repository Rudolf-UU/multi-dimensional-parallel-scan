use core::sync::atomic::{Ordering, AtomicU64};
use crate::cases::scan::{fold_sequential, scan_sequential};
use crate::core::worker::{*};
use crate::core::task::{*};
use crate::core::workassisting_loop::{*};

const BLOCK_COUNT: u64 = 32 * 8;
const MIN_BLOCK_SIZE: u64 = 1024;

#[derive(Copy, Clone)]
struct Data<'a> {
  input: &'a [AtomicU64],
  output: &'a [AtomicU64],
  block_size: u64,
  block_count: u64
}

pub fn create_task(input: &[AtomicU64], output: &[AtomicU64]) -> Task {
  let len = output.len();
  let mut block_size = (len as u64 + BLOCK_COUNT - 1) / BLOCK_COUNT;
  let mut block_count = BLOCK_COUNT;
  if block_size < MIN_BLOCK_SIZE {
    block_size = MIN_BLOCK_SIZE;
    block_count = (len as u64 + MIN_BLOCK_SIZE - 1) / MIN_BLOCK_SIZE;
  }
  Task::new_dataparallel::<Data>(phase1_run, phase1_finish, Data{ input, output, block_size, block_count }, block_count as u32, false)
}

fn phase1_run(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  workassisting_loop!(loop_arguments, |block_index| {
    // Local reduce
    let start = block_index as usize * data.block_size as usize;
    let end = (block_index as usize + 1) * data.block_size as usize;
    if end <= data.output.len() {
      let local = fold_sequential(&data.input[start .. end]);
      data.output[end as usize - 1].store(local, Ordering::Relaxed);
    }
  });
}

fn phase1_finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let data = unsafe { TaskObject::take_data(task) };
  // Phase 2
  // Scan the values of the separate blocks.
  // After this loop, all elements at an index `i * block_size - 1` are correct. 
  let mut index = data.block_size as usize - 1;
  let mut accumulator = 0;
  while index < data.output.len() {
    accumulator = accumulator + data.output[index].load(Ordering::Relaxed);
    data.output[index].store(accumulator, Ordering::Relaxed);
    index += data.block_size as usize;
  }

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

    let initial = if block_index == 0 { 0 } else { data.output[start - 1].load(Ordering::Relaxed) };
    scan_sequential(&data.input[start .. end], initial, &data.output[start .. end]);
  });
}

fn phase3_finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
