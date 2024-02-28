use core::sync::atomic::{Ordering, AtomicU32, AtomicU64};
use std::sync::atomic::AtomicUsize;
use crate::cases::compact::{compact_sequential, BLOCK_COUNT, MIN_BLOCK_SIZE, scan_indices_sequential};
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
  sequential_block_count: AtomicU32
}

struct Phase3<'a> {
  input: &'a [u64],
  temp: &'a [AtomicU64],
  output: &'a [AtomicU64],
  output_count: &'a AtomicUsize,
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
  Task::new_dataparallel::<Phase1>(phase1_run, phase1_finish, Phase1{ mask, input, temp, output, output_count, block_size, block_count, sequential_block_count: AtomicU32::new(0) }, block_count as u32, true)
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
      if block_index as u64 == data.block_count - 1 {
        data.output_count.store(accumulator, Ordering::Relaxed);
      } else {
        data.temp[(block_index + 1) as usize * data.block_size as usize - 1].store(accumulator as u64, Ordering::Relaxed);
      }
    },
    // Parallel work for other threads
    |block_index| {
      // Local scan
      let start = block_index as usize * data.block_size as usize;
      let end = ((block_index as usize + 1) * data.block_size as usize).min(data.output.len());
      scan_indices_sequential(data.mask, &data.input[start .. end], &data.temp[start .. end]);
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
  // Scan the values of the separate blocks.
  // After this loop, all elements at an index `i * block_size - 1` are correct. 
  let mut index = sequential_block_count as usize * phase1.block_size as usize - 1;
  let mut aggregate = 0;
  while index < phase1.temp.len() {
    aggregate = aggregate + phase1.temp[index].load(Ordering::Relaxed);
    phase1.temp[index].store(aggregate, Ordering::Relaxed);
    index += phase1.block_size as usize;
  }

  let block_count = phase1.block_count - sequential_block_count as u64;
  let phase3 = Phase3{
    input: phase1.input,
    temp: phase1.temp,
    output: phase1.output,
    block_size: phase1.block_size,
    sequential_block_count,
    output_count: phase1.output_count,
  };

  workers.push_task(Task::new_dataparallel(phase3_run, phase3_finish, phase3, block_count as u32, false))
}

fn phase3_run(_workers: &Workers, task: *const TaskObject<Phase3>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  workassisting_loop!(loop_arguments, |block_idx_offset| {
    let block_idx = block_idx_offset + data.sequential_block_count;
    let start = block_idx as usize * data.block_size as usize;
    
    // Exclusive upper bound of this block.
    // In case of the last block, we may have a shorter block.
    let end = (start + data.block_size as usize).min(data.output.len());

    let prefix = data.temp[start - 1].load(Ordering::Relaxed);

    let mut previous = prefix;
    for i in start .. end {
      let mut index = unsafe { data.temp.get_unchecked(i) }.load(Ordering::Relaxed);
      // The last element of this block is already correct, so we do not iterate over that, hence the `- 1`.
      if i != start + data.block_size as usize - 1 {
        index += prefix;
      }
      if index != previous {
        assert_eq!(index, previous + 1);
        unsafe {
          data.output.get_unchecked((index - 1) as usize).store(*data.input.get_unchecked(i), Ordering::Relaxed);
        }
      }

      previous = index;
    }

    if end == data.output.len() {
      data.output_count.store(previous as usize, Ordering::Relaxed);
    }
  });
}

fn phase3_finish(workers: &Workers, task: *mut TaskObject<Phase3>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
