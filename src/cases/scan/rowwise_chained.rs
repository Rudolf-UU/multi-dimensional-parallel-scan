use core::sync::atomic::{Ordering, AtomicU32};
use crate::cases::scan::{fold_sequential, scan_sequential, BLOCK_SIZE};
use crate::cases::scan::column_row_chained::{ BlockInfo, Data, reset, STATE_PREFIX_AVAILABLE, STATE_AGGREGATE_AVAILABLE };
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
  let mut sequential = true;
  
  workassisting_loop!(loop_arguments, |block_index| {
    let row_index = block_index as usize / data.blocks_per_row as usize;
    let col_index = block_index as usize - (row_index * data.blocks_per_row as usize);
    
    let start = col_index * BLOCK_SIZE as usize + row_index * data.inner_size as usize;
    let end = ((col_index + 1) * BLOCK_SIZE as usize + row_index * data.inner_size as usize).min(data.inner_size as usize * (row_index + 1));

    // Check if we already have a prefix of the previous block or
    // if the current block is at the start of a row.
    // If that is the case, then we can perform the scan directly.
    // Otherwise we perform a reduce-then-scan over this block.
    let aggregate_start = if col_index == 0 {
      sequential = true;
      Some(0)
    } else if !sequential {
      None // Don't switch back from parallel mode to sequential mode
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
