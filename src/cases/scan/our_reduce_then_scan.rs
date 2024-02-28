use core::sync::atomic::{Ordering, AtomicU32, AtomicU64};
use crate::cases::scan::{fold_sequential, scan_sequential};
use crate::core::worker::{*};
use crate::core::task::{*};
use crate::core::workassisting_loop::*;

const BLOCK_COUNT: u64 = 32 * 8;
const MIN_BLOCK_SIZE: u64 = 1024;

struct Phase1<'a> {
  input: &'a [AtomicU64],
  output: &'a [AtomicU64],
  block_size: u64,
  // The first thread goes through the list sequentially from the left, and can thus directly compute the output values.
  // Other threads claim work from the right for a three pass scan.
  // Here we store the sizes of those two parts, as we need to later perform more work for the three pass scan.
  sequential_block_count: AtomicU32,
  parallel_block_count: AtomicU32,

  // Only used for measurements
  output_sequential_size: Option<&'a AtomicU64>,
}

struct Phase3<'a> {
  input: &'a [AtomicU64],
  output: &'a [AtomicU64],
  block_size: u64,
  sequential_block_count: u32,
}

pub fn create_task(input: &[AtomicU64], output: &[AtomicU64], output_sequential_size: Option<&AtomicU64>) -> Task {
  let len = output.len();
  let mut block_size = (len as u64 + BLOCK_COUNT - 1) / BLOCK_COUNT;
  let mut block_count = BLOCK_COUNT;
  if block_size < MIN_BLOCK_SIZE {
    block_size = MIN_BLOCK_SIZE;
    block_count = (len as u64 + MIN_BLOCK_SIZE - 1) / MIN_BLOCK_SIZE;
  }
  Task::new_dataparallel::<Phase1>(phase1_run, phase1_finish, Phase1{ input, output, block_size, sequential_block_count: AtomicU32::new(0), parallel_block_count: AtomicU32::new(0), output_sequential_size }, block_count as u32, true)
}

fn phase1_run(_workers: &Workers, task: *const TaskObject<Phase1>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let mut accumulator = 0;
  workassisting_loop_two_sided!(loop_arguments,
    // Sequential work for first thread
    |block_index| {
      let start = block_index as usize * data.block_size as usize;
      let end = ((block_index as usize + 1) * data.block_size as usize).min(data.output.len());
      accumulator = scan_sequential(&data.input[start .. end], accumulator, &data.output[start .. end]);
    },
    // Parallel work for other threads
    |block_index| {
      let start = block_index as usize * data.block_size as usize;
      let end = ((block_index as usize + 1) * data.block_size as usize).min(data.output.len());
      let local = fold_sequential(&data.input[start .. end]);
      data.output[end as usize - 1].store(local, Ordering::Relaxed);
    },
    |sequential_count, parallel_count| {
      data.sequential_block_count.store(sequential_count, Ordering::Relaxed);
      data.parallel_block_count.store(parallel_count, Ordering::Relaxed);
      if let Some(r) = data.output_sequential_size {
        r.store((sequential_count as u64 * data.block_size).min(data.input.len() as u64), Ordering::Relaxed);
      }
    }
  );
}

fn phase1_finish(workers: &Workers, task: *mut TaskObject<Phase1>) {
  let data = unsafe { TaskObject::take_data(task) };
  // Phase 2
  let sequential_block_count = data.sequential_block_count.load(Ordering::Relaxed);
  let parallel_block_count = data.parallel_block_count.load(Ordering::Relaxed);

  // There should be at least 1 sequential block,
  // as the first thread to claim a block will behave sequentially.
  assert_ne!(sequential_block_count, 0);

  // Scan the values of the parallel blocks.
  // After this loop, all elements at an index `i * block_size - 1` are correct.
  let mut index = sequential_block_count as usize * data.block_size as usize - 1;
  let mut aggregate = 0;
  while index < data.output.len() {
    aggregate = aggregate + data.output[index].load(Ordering::Relaxed);
    data.output[index].store(aggregate, Ordering::Relaxed);
    index += data.block_size as usize;
  }

  workers.push_task(Task::new_dataparallel(phase3_run, phase3_finish, Phase3{ input: data.input, output: data.output, block_size: data.block_size, sequential_block_count }, parallel_block_count as u32, false))
}

fn phase3_run(_workers: &Workers, task: *const TaskObject<Phase3>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  workassisting_loop!(loop_arguments, |block_idx_offset| {
    // No work needs to be done for block 1
    let block_idx = block_idx_offset + data.sequential_block_count;
    let start = block_idx as usize * data.block_size as usize;

    // Exclusive upper bound of this block.
    // The last element of this block is already correct, so we do not iterate over that, hence the `- 1`.
    // In case of the last block, we may have a shorter block.
    let end = (start + data.block_size as usize - 1).min(data.output.len());
    let accumulator = data.output[start - 1].load(Ordering::Relaxed);
    scan_sequential(&data.input[start .. end], accumulator, &data.output[start .. end]);
  });
}

fn phase3_finish(workers: &Workers, task: *mut TaskObject<Phase3>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
