use core::sync::atomic::{Ordering, AtomicU32, AtomicU64};
use std::sync::atomic::AtomicUsize;
use crate::cases::compact::{compact_sequential, count_sequential, BLOCK_COUNT, MIN_BLOCK_SIZE};
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;

struct Phase1<'a> {
  mask: u64,
  input: &'a [u64],
  temp: &'a [AtomicU64],
  output: &'a [AtomicU64],
  output_count: &'a AtomicUsize,
  block_size: u64,
  block_count: u64,
  // The first thread goes through the list sequentially from the left, and can thus directly compute the output values.
  // Other threads claim work from the right for a three pass scan.
  // Here we store the sizes of those two parts, as we need to later perform more work for the three pass scan.
  sequential_block_count: AtomicU32,
  // parallel_block_count: AtomicU32
}

struct Phase3<'a> {
  mask: u64,
  input: &'a [u64],
  temp: &'a [AtomicU64],
  output: &'a [AtomicU64],
  block_size: u64,
  sequential_block_count: u32
}

pub fn create_task(mask: u64, input: &[u64], temp: &[AtomicU64], output: &[AtomicU64], output_count: &AtomicUsize) -> Task {
  let len = output.len();
  let mut block_size = (len as u64 + BLOCK_COUNT - 1) / BLOCK_COUNT;
  let mut block_count = BLOCK_COUNT;
  if block_size < MIN_BLOCK_SIZE {
    block_size = MIN_BLOCK_SIZE;
    block_count = (len as u64 + MIN_BLOCK_SIZE - 1) / MIN_BLOCK_SIZE;
  }
  Task::new_dataparallel::<Phase1>(phase1_run, phase1_finish, Phase1{ mask, input, temp, output, output_count, block_size, block_count, sequential_block_count: AtomicU32::new(0) /*parallel_block_count: AtomicU32::new(0) */ }, block_count as u32, true)
}

fn phase1_run(_workers: &Workers, task: *const TaskObject<Phase1>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };

  let mut accumulator = 0;
  workassisting_loop_two_sided!(loop_arguments,
    // Sequential work for first thread
    |block_index| {
      let start = block_index as usize * data.block_size as usize;
      let end = ((block_index as usize + 1) * data.block_size as usize).min(data.output.len());
      accumulator = compact_sequential(data.mask, &data.input[start .. end], data.output, accumulator);
      data.temp[block_index as usize].store(accumulator as u64, Ordering::Relaxed);
    },
    // Parallel work for other threads
    |block_index| {
      let start = block_index as usize * data.block_size as usize;
      let end = ((block_index as usize + 1) * data.block_size as usize).min(data.output.len());
      let local = count_sequential(data.mask, &data.input[start .. end]);
      data.temp[block_index as usize].store(local as u64, Ordering::Relaxed);
    },
    |sequential_count, _parallel_count| {
      data.sequential_block_count.store(sequential_count, Ordering::Relaxed);
    }
  );
}

fn phase1_finish(workers: &Workers, task: *mut TaskObject<Phase1>) {
  let phase1 = unsafe { TaskObject::take_data(task) };
  // Phase 2
  let sequential_block_count = phase1.sequential_block_count.load(Ordering::Relaxed);
  // Scan the values of data.temp
  let mut accumulator = phase1.temp[sequential_block_count as usize - 1].load(Ordering::Relaxed);
  for field in &phase1.temp[sequential_block_count as usize ..] {
    let old = field.load(Ordering::Relaxed);
    field.store(accumulator, Ordering::Relaxed);
    accumulator += old;
  }
  phase1.output_count.store(accumulator as usize, Ordering::Relaxed);

  let block_count = phase1.block_count;

  let phase3 = Phase3{
    mask: phase1.mask,
    input: phase1.input,
    temp: phase1.temp,
    output: phase1.output,
    block_size: phase1.block_size,
    sequential_block_count,
  };

  workers.push_task(Task::new_dataparallel(phase3_run, phase3_finish, phase3, block_count as u32 - sequential_block_count, false))
}

fn phase3_run(_workers: &Workers, task: *const TaskObject<Phase3>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };

  workassisting_loop!(loop_arguments, |block_idx_offset| {
    let block_idx = block_idx_offset + data.sequential_block_count;
    let start = block_idx as usize * data.block_size as usize;

    // Exclusive upper bound of this block.
    // The last element of this block is already correct, so we do not iterate over that, hence the `- 1`.
    // In case of the last block, we may have a shorter block.
    let end = (start + data.block_size as usize - 1).min(data.output.len());

    let initial = data.temp[block_idx as usize].load(Ordering::Relaxed);
    compact_sequential(data.mask, &data.input[start .. end], &data.output, initial as usize);
  });
}

fn phase3_finish(workers: &Workers, task: *mut TaskObject<Phase3>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
