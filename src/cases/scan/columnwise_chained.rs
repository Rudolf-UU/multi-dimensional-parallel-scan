use core::sync::atomic::{Ordering, AtomicU32};
use crate::cases::scan::{fold_sequential, scan_sequential, BLOCK_SIZE};
use crate::cases::scan::row_column_chained::{ BlockInfo, Data, reset, STATE_PREFIX_AVAILABLE, STATE_AGGREGATE_AVAILABLE };
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;
use crate::utils::array::MultArray;

pub fn init_single<const N: usize>(input: &MultArray<N>, temp: &[BlockInfo], output: &MultArray<N>) -> Task {
  reset(temp);
  create_task(input, temp, output)
}

fn create_task<const N: usize>(input_m: &MultArray<N>, temp: &[BlockInfo], output_m: &MultArray<N>) -> Task {
  let inner_size = input_m.get_inner_size() as u64;
  let inner_rows = input_m.total_inner_count() as u64;
  let input = input_m.get_data();
  let output = output_m.get_data();
  
  let blocks_per_row = (inner_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  let block_count = blocks_per_row.checked_mul(inner_rows).expect("Block count overflowed u64 size") as u32;

  Task::new_dataparallel::<Data>(run, finish, Data{ input, temp, output, blocks_per_row, inner_size:inner_size as u64 }, block_count, false)
}

fn run(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let inner_rows = data.input.len() / data.inner_size as usize;
  let inner_size = data.inner_size as usize;

  workassisting_loop!(loop_arguments, |block_index| {
    let column_idx = block_index as usize / inner_rows as usize;
    let row_idx = block_index as usize % inner_rows as usize;

    let start = row_idx * inner_size + column_idx * BLOCK_SIZE as usize;
    let end = (row_idx * inner_size) + ((column_idx + 1) * BLOCK_SIZE as usize).min(inner_size);
    let temp_idx = row_idx * data.blocks_per_row as usize + column_idx;
    
    // Check if we already have a prefix of the previous block or
    // if the current block is at the start of a row.
    // If that is the case, then we can perform the scan directly.
    // Otherwise we perform a reduce-then-scan over this block.
    let aggregate_start = if column_idx == 0 {
        Some(0)
      } else {
        let previous = temp_idx - 1;
        let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
        if previous_state == STATE_PREFIX_AVAILABLE {
          Some(data.temp[previous as usize].prefix.load(Ordering::Acquire))
        } else {
          None
        }
      };

    if let Some(aggregate) = aggregate_start {
      let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
      data.temp[temp_idx as usize].prefix.store(local, Ordering::Relaxed);
      data.temp[temp_idx as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);
    } else {
      let local = fold_sequential(&data.input[start .. end]);
      // Share own local value
      data.temp[temp_idx as usize].aggregate.store(local, Ordering::Relaxed);
      data.temp[temp_idx as usize].state.store(STATE_AGGREGATE_AVAILABLE, Ordering::Release);

      // Find aggregate
      let mut aggregate = 0;
      let mut previous = temp_idx as usize - 1;

      loop {
        let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
        if previous_state == STATE_PREFIX_AVAILABLE {
          aggregate = data.temp[previous as usize].prefix.load(Ordering::Acquire) + aggregate;
          break;
        } else if previous_state == STATE_AGGREGATE_AVAILABLE {
          aggregate = data.temp[previous as usize].aggregate.load(Ordering::Acquire) + aggregate;
          previous = previous - 1;
        } else {
          // Continue looping until the state of the previous block changes.
        }
      }

      // Make aggregate available
      data.temp[temp_idx as usize].prefix.store(aggregate + local, Ordering::Relaxed);
      data.temp[temp_idx as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);

      scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
    }
  });
}

fn finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
