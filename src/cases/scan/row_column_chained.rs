use core::sync::atomic::{Ordering, AtomicU64, AtomicU32};
use crate::cases::scan::{fold_sequential, scan_sequential, BLOCK_SIZE};
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;
use crate::utils::array::MultArray;

pub struct Data<'a> {
  pub input: &'a [AtomicU64],
  pub temp: &'a [BlockInfo],
  pub output: &'a [AtomicU64],
  pub blocks_per_row: u64,
  pub inner_size: u64,
}

pub struct BlockInfo {
    pub state: AtomicU64,
    pub aggregate: AtomicU64,
    pub prefix: AtomicU64
}
  
pub const STATE_INITIALIZED: u64 = 0;
pub const STATE_AGGREGATE_AVAILABLE: u64 = 1;
pub const STATE_PREFIX_AVAILABLE: u64 = 2;

pub fn create_temp<const N: usize>(input: &MultArray<AtomicU64, N>) -> Box<[BlockInfo]> {
  (0 .. ((input.get_inner_size() as u64 + BLOCK_SIZE - 1) / BLOCK_SIZE) * input.total_inner_count() as u64).map(|_| BlockInfo{
    state: AtomicU64::new(STATE_INITIALIZED), aggregate: AtomicU64::new(0), prefix: AtomicU64::new(0)
  }).collect()
}

pub fn reset(temp: &[BlockInfo]) {
  for i in 0 .. temp.len() {
    temp[i].state.store(STATE_INITIALIZED, Ordering::Relaxed);
    temp[i].aggregate.store(0, Ordering::Relaxed);
    temp[i].prefix.store(0, Ordering::Relaxed);
  }
}

pub fn init_single<const N: usize>(input: &MultArray<AtomicU64, N>, temp: &[BlockInfo], output: &MultArray<AtomicU64, N>) -> Task {
  reset(temp);
  create_task(input, temp, output)
}

fn create_task<const N: usize>(input_m: &MultArray<AtomicU64, N>, temp: &[BlockInfo], output_m: &MultArray<AtomicU64, N>) -> Task {
  let inner_size = input_m.get_inner_size() as u64;
  let inner_rows = input_m.total_inner_count() as u64;
  let input = input_m.get_data();
  let output = output_m.get_data();
  
  let blocks_per_row = (inner_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  let block_count = if blocks_per_row > 1 {
        blocks_per_row.checked_mul(inner_rows).expect("Block count overflowed u64 size") as u32
      } else {
        // Multiple rows are (optionally) combined into a single block, which changes the total block_count
        inner_rows.div_ceil(BLOCK_SIZE / inner_size) as u32
      };
  
  Task::new_dataparallel::<Data>(run, finish, Data{ input, temp, output, blocks_per_row, inner_size:inner_size as u64 }, block_count, true)
}

fn run(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let inner_rows = data.input.len() / data.inner_size as usize;
  let segments = data.blocks_per_row as u32;

  workassisting_loop_row_column!(loop_arguments, segments, 
  // Multiple-rows scan algorithm
  |block_index| {
    let rows_per_block = (BLOCK_SIZE as usize / data.inner_size as usize).min(inner_rows) as u64;
    let block_size = data.inner_size * rows_per_block;
    let mut start = block_index as usize * block_size as usize;

    for _ in 0 .. rows_per_block {
      let row_end = start + data.inner_size as usize;
      scan_sequential(&data.input[start .. row_end], 0, &data.output[start .. row_end]);
      start = row_end;
    }
  },
  // Row-wise scan algorithm
  |block_index| {
    let row_idx = block_index as usize / data.blocks_per_row as usize;
    let column_idx = block_index as usize - (row_idx * data.blocks_per_row as usize);
    let descriptor_idx = block_index as usize;
    adaptive_chained_lookback(data, row_idx, column_idx, descriptor_idx);
  },
  // Column-wise scan algorithm
  |block_index, rows_completed| {
    let new_inner_rows = inner_rows - rows_completed as usize;
    let row_idx = (block_index as usize % new_inner_rows as usize) + rows_completed as usize;
    let column_idx = block_index as usize / new_inner_rows as usize;
    let descriptor_idx = row_idx * data.blocks_per_row as usize + column_idx;
    adaptive_chained_lookback(data, row_idx, column_idx, descriptor_idx);
  });
}

fn adaptive_chained_lookback(data:&Data<'_>, row_idx:usize, column_idx:usize, descriptor_idx:usize) {
  // Check if we already have a prefix of the previous block or
  // if the current block is at the start of a row.
  // If that is the case, then we can perform the scan directly.
  // Otherwise we perform a reduce-then-scan over this block.
  let start = row_idx * data.inner_size as usize + column_idx * BLOCK_SIZE as usize;
  let end = row_idx * data.inner_size as usize + ((column_idx + 1) * BLOCK_SIZE as usize).min(data.inner_size as usize);
  
  let aggregate_start = if column_idx == 0 {
    Some(0)
  } else {
    let previous = descriptor_idx - 1;
    let previous_state = data.temp[previous].state.load(Ordering::Acquire);
    if previous_state == STATE_PREFIX_AVAILABLE {
      Some(data.temp[previous].prefix.load(Ordering::Acquire))
    } else {
      None
    }
  };

  if let Some(aggregate) = aggregate_start {
    let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
    data.temp[descriptor_idx].prefix.store(local, Ordering::Relaxed);
    data.temp[descriptor_idx].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);
  } else {
    let local = fold_sequential(&data.input[start .. end]);
    data.temp[descriptor_idx].aggregate.store(local, Ordering::Relaxed);
    data.temp[descriptor_idx].state.store(STATE_AGGREGATE_AVAILABLE, Ordering::Release);

    // Look-back phase -- computing the prefix based on predecessor aggregates
    let mut aggregate = 0;
    let mut previous = descriptor_idx - 1;

    loop {
      let previous_state = data.temp[previous].state.load(Ordering::Acquire);
      if previous_state == STATE_PREFIX_AVAILABLE {
        aggregate = data.temp[previous].prefix.load(Ordering::Acquire) + aggregate;
        break;
      } else if previous_state == STATE_AGGREGATE_AVAILABLE {
        aggregate = data.temp[previous].aggregate.load(Ordering::Acquire) + aggregate;
        previous = previous - 1;
      } else {
        // Continue looping until the state of the previous block changes.
      }
    }

    // Share calculated prefix value
    data.temp[descriptor_idx].prefix.store(aggregate + local, Ordering::Relaxed);
    data.temp[descriptor_idx].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);

    scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
  }
}

fn finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
