use core::sync::atomic::{Ordering, AtomicU64, AtomicU32};
use crate::cases::scan::fold_sequential;
use crate::cases::scan::scan_sequential;
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;
use crate::utils::array::MultArray;

const BLOCK_SIZE: u64 = crate::cases::scan::BLOCK_SIZE;

struct Data<'a> {
  input: &'a [AtomicU64],
  temp: &'a [BlockInfo],
  output: &'a [AtomicU64],
  seg_count: u64,
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

pub fn create_temp<const N: usize>(input: &MultArray<N>) -> Box<[BlockInfo]> {
  (0 .. ((input.get_inner_size() as u64 + BLOCK_SIZE - 1) / BLOCK_SIZE) * input.total_inner_count() as u64).map(|_| BlockInfo{
    state: AtomicU64::new(STATE_INITIALIZED), aggregate: AtomicU64::new(0), prefix: AtomicU64::new(0)
  }).collect()
}

pub fn init_single<const N: usize>(input: &MultArray<N>, temp: &[BlockInfo], output: &MultArray<N>, option: u32) -> Task {
  reset(temp);
  create_task(input, temp, output, option)
}

pub fn reset(temp: &[BlockInfo]) {
    for i in 0 .. temp.len() {
      temp[i].state.store(STATE_INITIALIZED, Ordering::Relaxed);
      temp[i].aggregate.store(0, Ordering::Relaxed);
      temp[i].prefix.store(0, Ordering::Relaxed);
    }
}

fn create_task<const N: usize>(input_m: &MultArray<N>, temp: &[BlockInfo], output_m: &MultArray<N>, option: u32) -> Task {
  let inner_size = input_m.get_inner_size();
  let inner_rows = input_m.total_inner_count();
  let input = input_m.get_data();
  let output = output_m.get_data();

  if option == 6 {
    
      let rows_per_block = (BLOCK_SIZE as usize / inner_size as usize).min(inner_rows) as u64;
      Task::new_dataparallel::<Data>(run_multiple_rows, finish, Data{ input, temp, output, seg_count:rows_per_block, inner_size:inner_size as u64 }, ((inner_rows as u64).div_ceil(rows_per_block)) as u32, false)
    
  }else {

  if inner_size <= (BLOCK_SIZE as usize / 2) {
    let rows_per_block = (BLOCK_SIZE as usize / inner_size as usize).min(inner_rows) as u64;
    Task::new_dataparallel::<Data>(run_columnwise2, finish, Data{ input, temp, output, seg_count:1, inner_size:inner_size as u64 }, ((inner_rows as u64).div_ceil(rows_per_block)) as u32, true)
  }
  else {
    let blocks_per_row = ((inner_size as u64 + BLOCK_SIZE - 1) / (BLOCK_SIZE)) as u32;
    let block_count = blocks_per_row.checked_mul(inner_rows as u32).expect("Block count overflowed u32 size");

    if option == 0 {
      Task::new_dataparallel::<Data>(run_columnwise, finish, Data{ input, temp, output, seg_count:blocks_per_row as u64, inner_size:inner_size as u64 }, block_count, false)
    } else if option == 1 {
      Task::new_dataparallel::<Data>(run_columnwise_custom, finish, Data{ input, temp, output, seg_count:blocks_per_row as u64, inner_size:inner_size as u64 }, block_count, true)
    } else if option == 2 {
      Task::new_dataparallel::<Data>(run_row_segments, finish, Data{ input, temp, output, seg_count:blocks_per_row as u64, inner_size:inner_size as u64 }, block_count, false)
    } else {
      Task::new_dataparallel::<Data>(run_columnwise2, finish, Data{ input, temp, output, seg_count:blocks_per_row as u64, inner_size:inner_size as u64 }, block_count, true)
    }
  }}
}

fn run_multiple_rows(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let block_len = data.inner_size * data.seg_count;

  workassisting_loop!(loop_arguments, |block_index| {
    let mut start = block_index as usize * block_len as usize;

    for _ in 0 .. data.seg_count {
      let row_end = start + data.inner_size as usize;
      scan_sequential(&data.input[start .. row_end], 0, &data.output[start .. row_end]);
      start = row_end;
    }
  });
}

fn run_row_segments(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let mut sequential = true;
  
  workassisting_loop!(loop_arguments, |block_index| {
    let row_index = block_index as usize / data.seg_count as usize;
    let col_index = block_index as usize - (row_index * data.seg_count as usize);
    
    let start = col_index * BLOCK_SIZE as usize + row_index * data.inner_size as usize;
    let end = ((col_index + 1) * BLOCK_SIZE as usize + row_index * data.inner_size as usize).min(data.inner_size as usize * (row_index + 1));

    // Check if we already have an aggregate of the previous block.
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

fn run_columnwise(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let mut sequential = true;
  let mut inner_rows = data.input.len() / data.inner_size as usize;
  let inner_size = data.inner_size as usize;

  workassisting_loop!(loop_arguments, |block_index| {
    let column_idx = block_index as usize / inner_rows as usize;
    let row_idx = block_index as usize % inner_rows as usize;

    let start = row_idx * inner_size + column_idx * BLOCK_SIZE as usize;
    let end = (row_idx * inner_size) + ((column_idx + 1) * BLOCK_SIZE as usize).min(inner_size);
    let indexer = row_idx * data.seg_count as usize + column_idx;
    
    let aggregate_start = if column_idx == 0 {
        Some(0)
      }  else {
        let previous = indexer - 1;
        let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
        if previous_state == STATE_PREFIX_AVAILABLE {
          Some(data.temp[previous as usize].prefix.load(Ordering::Acquire))
        } else {
          None
        }
      };

    if let Some(aggregate) = aggregate_start {
      let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
      data.temp[indexer as usize].prefix.store(local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);
    } else {
      sequential = false;
      let local = fold_sequential(&data.input[start .. end]);
      // Share own local value
      data.temp[indexer as usize].aggregate.store(local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_AGGREGATE_AVAILABLE, Ordering::Release);

      // Find aggregate
      let mut aggregate = 0;
      let mut previous = indexer as usize - 1;

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
      data.temp[indexer as usize].prefix.store(aggregate + local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);

      scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
    }
  });
}

fn run_columnwise2(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let inner_rows = data.input.len() / data.inner_size as usize;
  let segments = data.seg_count as u32;

  workassisting_loop2!(loop_arguments, segments, 
    // row-wise algorithm
    |block_index| {
    let row_idx = block_index as usize / data.seg_count as usize;
    let column_idx = block_index as usize - (row_idx * data.seg_count as usize);
    
    let start = column_idx * BLOCK_SIZE as usize + row_idx * data.inner_size as usize;
    let end = ((column_idx + 1) * BLOCK_SIZE as usize + row_idx * data.inner_size as usize).min(data.inner_size as usize * (row_idx + 1));
    let indexer = block_index;

    let aggregate_start = if column_idx == 0 {
      Some(0)
    }  else {
      let previous = indexer - 1;
      let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
      if previous_state == STATE_PREFIX_AVAILABLE {
        Some(data.temp[previous as usize].prefix.load(Ordering::Acquire))
      } else {
        None
      }
    };

    if let Some(aggregate) = aggregate_start {
      let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
      data.temp[indexer as usize].prefix.store(local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);
    } else {
      let local = fold_sequential(&data.input[start .. end]);
      // Share own local value
      data.temp[indexer as usize].aggregate.store(local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_AGGREGATE_AVAILABLE, Ordering::Release);

      // Find aggregate
      let mut aggregate = 0;
      let mut previous = indexer as usize - 1;

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
      data.temp[indexer as usize].prefix.store(aggregate + local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);

      scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
  }},
  // Column-wise algorithm
  |block_index, rows_completed| {

    let new_inner_rows = inner_rows - rows_completed as usize;
    let column_idx = block_index as usize / new_inner_rows as usize;
    let row_idx = (block_index as usize % new_inner_rows as usize) + rows_completed as usize;

    let start = row_idx * data.inner_size as usize + (column_idx * BLOCK_SIZE as usize);
    let end = (row_idx * data.inner_size as usize) + ((column_idx + 1) * BLOCK_SIZE as usize).min(data.inner_size as usize);
    let indexer = row_idx * data.seg_count as usize + column_idx;

    let aggregate_start = if column_idx == 0 {
      Some(0)
    }  else {
      let previous = indexer - 1;
      let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
      if previous_state == STATE_PREFIX_AVAILABLE {
        Some(data.temp[previous as usize].prefix.load(Ordering::Acquire))
      } else {
        None
      }
    };

    if let Some(aggregate) = aggregate_start {
      let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
      data.temp[indexer as usize].prefix.store(local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);
    } else {
      let local = fold_sequential(&data.input[start .. end]);
      // Share own local value
      data.temp[indexer as usize].aggregate.store(local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_AGGREGATE_AVAILABLE, Ordering::Release);

      // Find aggregate
      let mut aggregate = 0;
      let mut previous = indexer as usize - 1;

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
      data.temp[indexer as usize].prefix.store(aggregate + local, Ordering::Relaxed);
      data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);

      scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
    }
  },
  // Multiple-rows algorithm
  |block_index| {
    let rows_per_block = (BLOCK_SIZE as usize / data.inner_size as usize).min(inner_rows) as u64;
    let block_len = data.inner_size * rows_per_block;

    let mut start = block_index as usize * block_len as usize;

    for _ in 0 .. rows_per_block {
      let row_end = start + data.inner_size as usize;
      scan_sequential(&data.input[start .. row_end], 0, &data.output[start .. row_end]);
      start = row_end;
    }
  });
}

fn run_columnwise3(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let mut sequential = true;
  let mut end_index = 0 as usize;
  let mut end_row = 0 as usize;
  let mut inner_rows = data.input.len() / data.inner_size as usize;
  let inner_size = data.inner_size as usize;

  workassisting_loop!(loop_arguments, |block_index| {
    // let column_idx = block_index as usize / inner_rows as usize;
    // let row_idx = block_index as usize % inner_rows as usize;

    // let start = row_idx * inner_size + column_idx * BLOCK_SIZE as usize;
    // let end = (row_idx * inner_size) + ((column_idx + 1) * BLOCK_SIZE as usize).min(inner_size);
    // let indexer = row_idx * data.seg_count as usize + column_idx;
    let mut column_idx;
    let mut row_idx;
    let mut start;
    let mut end;
    let mut indexer = block_index as usize;

    if sequential || block_index as usize <= end_index  { // run row-wise
      row_idx    = block_index as usize / data.seg_count as usize;
      column_idx = block_index as usize - (row_idx * data.seg_count as usize);
      end_index = ((row_idx + 1) * data.seg_count as usize) - 1;
      end_row = row_idx + 1;
      inner_rows = (data.input.len() / data.inner_size as usize) - end_row;
      
      start = column_idx * BLOCK_SIZE as usize + row_idx * data.inner_size as usize;
      end = ((column_idx + 1) * BLOCK_SIZE as usize + row_idx * data.inner_size as usize).min(data.inner_size as usize * (row_idx + 1));
    }else { // run column-wise
      let new_index = block_index as usize - end_index - 1 ;
      column_idx = new_index as usize / inner_rows as usize;
      row_idx = (new_index as usize % inner_rows as usize) + end_row;

      

      // if(new_index == 0) {
      //   println!("hey there {:?}, {:?}, {:?}", block_index, new_index, end_index);
      // }



      start = row_idx * inner_size + (column_idx * BLOCK_SIZE as usize);
      end = (row_idx * inner_size) + ((column_idx + 1) * BLOCK_SIZE as usize).min(inner_size);
      indexer = row_idx * data.seg_count as usize + column_idx;

      // if row_idx == 50 || indexer == 50{
      //   println!("hi {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}", row_idx, column_idx, block_index, end_row, indexer, start, end, new_index);
      // }

      // if row_idx == 0 || row_idx == 2 || row_idx == 3 || row_idx == 1 {
      //   let previous_state = data.temp[indexer - 1 as usize].state.load(Ordering::Acquire);
      //   println!("row index {:?}, {:?}, block: {:?}, endings: {:?}, {:?}, start {:?}, end {:?}, {:?}, {:?}", row_idx, column_idx, block_index, end_row, end_index, indexer, start, end, previous_state);
      // }
      
    }

    
      let aggregate_start = if column_idx == 0 {
          Some(0)
        }  else {
          let previous = indexer - 1;
          let previous_state = data.temp[previous as usize].state.load(Ordering::Acquire);
          if previous_state == STATE_PREFIX_AVAILABLE {
            Some(data.temp[previous as usize].prefix.load(Ordering::Acquire))
          } else {
            None
          }
        };
      
      // if (end-start) != BLOCK_SIZE as usize && (end-start) != (inner_size % BLOCK_SIZE as usize) {
      //   println!("Hi {:?}", (end-start));
      // }

      if let Some(aggregate) = aggregate_start {
        let local = scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
        data.temp[indexer as usize].prefix.store(local, Ordering::Relaxed);
        data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);
      } else {
        sequential = false;
        let local = fold_sequential(&data.input[start .. end]);
        // Share own local value
        data.temp[indexer as usize].aggregate.store(local, Ordering::Relaxed);
        data.temp[indexer as usize].state.store(STATE_AGGREGATE_AVAILABLE, Ordering::Release);

        // Find aggregate
        let mut aggregate = 0;
        let mut previous = indexer as usize - 1;

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
        data.temp[indexer as usize].prefix.store(aggregate + local, Ordering::Relaxed);
        data.temp[indexer as usize].state.store(STATE_PREFIX_AVAILABLE, Ordering::Release);

        scan_sequential(&data.input[start .. end], aggregate, &data.output[start .. end]);
      
    }
  });
}

fn run_columnwise_custom(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
  let data = unsafe { TaskObject::get_data(task) };
  let mut sequential = true;
  //let column_count = (data.inner_size as usize).div_ceil(BLOCK_SIZE as usize) as u32;
  let row_count = (data.input.len() / data.inner_size as usize) as u32;

  workassisting_loop_column_based!(loop_arguments, row_count, |row_index, column_index| {
    //let (row_index, column_index) = ((block_index % row_count) as usize, (block_index / row_count) as usize);
    // println!("blockicx{:?}", block_index);
    // println!("rowidx {:?}", row_idx);
    // println!("rowidx {:?}", column_idx);
    // println!("rowidx {:?}", data.inner_size);

    let start = (row_index as usize * data.inner_size as usize) + (column_index as usize * BLOCK_SIZE as usize);
    let end = ((row_index as usize * data.inner_size as usize) + (column_index as usize + 1) * BLOCK_SIZE as usize).min(data.inner_size as usize * (row_index + 1) as usize);
    let block_index = (row_index as usize * data.seg_count as usize) + column_index as usize;

    // println!("row {:?}, block {:?}", row_index, column_index);
    // println!("start {:?}", start);
    // println!("end {:?}", end);
    
    let aggregate_start = if column_index ==  0 {
      Some(0)
    } 
    // else if !sequential {
    //   None // Don't switch back from parallel mode to sequential mode
    // } 
    else {
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
      // sequential = false;
      //println!("row {:?}, end {:?}, col {:?}", row_index, end, column_index);
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
          //println!("hi {:?}, {:?}, {:?}, {:?}", block_index, previous, row_index, column_index);// Continue looping until the state of previous block changes.
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
