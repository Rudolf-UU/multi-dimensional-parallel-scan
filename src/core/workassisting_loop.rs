#[macro_export]
macro_rules! workassisting_loop {
  ($loop_arguments_expr: expr, |$block_index: ident| $body: block) => {
    let mut loop_arguments: LoopArguments = $loop_arguments_expr;
    // Claim work
    let mut block_idx = loop_arguments.first_index;

    while block_idx < loop_arguments.work_size {
      if block_idx == loop_arguments.work_size - 1 {
        // All work is claimed.
        loop_arguments.empty_signal.task_empty();
      }

      // Copy block_idx to an immutable variable, such that a user of this macro cannot mutate it.
      let $block_index: u32 = block_idx;
      $body

      block_idx = loop_arguments.work_index.fetch_add(1, Ordering::Relaxed);
    }
    loop_arguments.empty_signal.task_empty();
  };
}
pub(crate) use workassisting_loop;

#[macro_export]
macro_rules! workassisting_loop2 {
  ($loop_arguments_expr: expr, $seg_count: ident, |$block_index: ident| $row_wise_scan: block, |$block_index2: ident, $rows_completed: ident| $column_wise_scan: block, |$block_index3: ident| $multiple_rows_scan: block) => {
    let loop_arguments: LoopArguments = $loop_arguments_expr;
    let work_size: u32 = loop_arguments.work_size;
    let work_index: &AtomicU32 = loop_arguments.work_index;
    let mut empty_signal: EmptySignal = loop_arguments.empty_signal;
    let segments = $seg_count;

    let mut block_idx = loop_arguments.first_index;

    if segments == 1 { // Apply the multiple-rows in a single block algorithm!
      while block_idx < work_size {
        if block_idx == work_size - 1 {
          // All work is claimed.
          empty_signal.task_empty();
        }
  
        // Copy block_idx to an immutable variable, such that a user of this macro cannot mutate it.
        let $block_index3: u32 = block_idx;
        $multiple_rows_scan

        block_idx = work_index.fetch_add(1, Ordering::Relaxed);
      }
      empty_signal.task_empty();
    } 
    else { // Apply the in-order row-wise algorithm, and switch to the columnwise algorithm when multiple threads are working in parallel.
    let mut seq_idx:u32;
    let mut par_idx:u32;
    let mut claimed:u32;

    assert!(work_size < 1 << 15);
    
    if loop_arguments.first_index != 0 {
      block_idx = work_index.fetch_add(1, Ordering::Relaxed);
      par_idx = block_idx & 0xFFFF;
      seq_idx = block_idx >> 16;
      claimed = seq_idx + par_idx;

      if claimed < work_size {
        let $block_index2 = par_idx;
        let $rows_completed = seq_idx / segments;
        $column_wise_scan
      }
    }

    loop {
      seq_idx = block_idx >> 16;
      par_idx = block_idx & 0xFFFF;
      claimed = seq_idx + par_idx + 1;

      if claimed > work_size {
        empty_signal.task_empty();
        break;
      } else if claimed == work_size {
        empty_signal.task_empty();
      }

      if par_idx == 0 || ((seq_idx + 1) % segments) != 0 {
        let res = work_index.compare_exchange(block_idx, block_idx + (1 << 16), Ordering::Acquire, Ordering::Acquire);

        if res.is_ok() {
          let $block_index = seq_idx;
          $row_wise_scan
        } 
        
        block_idx = work_index.load(Ordering::Relaxed);
      } else {
        block_idx = work_index.fetch_add(1, Ordering::Relaxed);
        
        seq_idx = block_idx >> 16;
        par_idx = block_idx & 0xFFFF;
        claimed = seq_idx + par_idx + 1;

        if claimed > work_size {
          empty_signal.task_empty();
          break;
        } else if claimed == work_size {
          empty_signal.task_empty();
        }

        let $block_index2 = par_idx;
        let $rows_completed = seq_idx / segments;

        $column_wise_scan

        
      } 
    }
  }
  };
}
pub(crate) use workassisting_loop2;

#[macro_export]
macro_rules! workassisting_loop_column_based  {
    ($loop_arguments_expr: expr, $row_count:ident, |$row_index: ident, $column_index: ident| $body: block) => {
      let loop_arguments:LoopArguments = $loop_arguments_expr;
      let work_size: u32 = loop_arguments.work_size;
      let work_index: &AtomicU32 = loop_arguments.work_index;
      let mut empty_signal: EmptySignal = loop_arguments.empty_signal;
      let number:u32 = $row_count;
      
      assert!(work_size < 1 << 15);

      loop {
        let mut index;
        let mut current = work_index.load(Ordering::Relaxed);

        loop{
          let new = if ((current & 0xFFFF) < number - 1) {current + 1} else {((current >> 16) + 1) << 16 };

          match work_index.compare_exchange(current, new, Ordering::SeqCst, Ordering::Acquire) {
            Ok(v) => {index = v; break},
            Err(v) => current = v,
          }
        }

        let row_idx = index & 0xFFFF;
        let column_idx = index >> 16;
        
        let count_claimed = row_idx + (column_idx * number) + 1;
        
        if count_claimed > work_size {
          empty_signal.task_empty();
          break;
        } else if (count_claimed == work_size) {
          empty_signal.task_empty();
        }
        
       let $row_index = row_idx;
       let $column_index = column_idx;
       $body
      }
    };
}
pub(crate) use workassisting_loop_column_based;

#[macro_export]
macro_rules! workassisting_loop_two_sided {
  ($loop_arguments_expr: expr, |$block_index_1: ident| $first_thread: block, |$block_index_2: ident| $other_threads: block, |$sequential_count: ident, $parallel_count: ident| $conclude_distribution: block) => {
    // Bind inputs to variables
    let loop_arguments: LoopArguments = $loop_arguments_expr;
    let work_size: u32 = loop_arguments.work_size;
    let work_index: &AtomicU32 = loop_arguments.work_index;
    let mut empty_signal: EmptySignal = loop_arguments.empty_signal;

    let first_try = if loop_arguments.first_index == 0 {
      work_index.compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
    } else {
      Result::Err(0)
    };

    assert!(work_size < 1 << 15);

    if first_try.is_ok() {
      // This is the first thread. This thread goes from left to right.
      let mut block_idx = 0;

      if work_size == 1 {
        // This is also the last iteration
        empty_signal.task_empty();
        let $sequential_count = 1;
        let $parallel_count = 0;
        $conclude_distribution
      }
      loop {
        let $block_index_1: u32 = block_idx;
        $first_thread;

        let index = work_index.fetch_add(1, Ordering::Relaxed);
        let sequential_index = index & 0xFFFF;
        let parallel_index = index >> 16;
        let count_claimed = sequential_index + parallel_index + 1;
        if count_claimed > work_size {
          // Everything is claimed
          empty_signal.task_empty();
          break;
        } else if count_claimed == work_size {
          // This is the last iteration
          empty_signal.task_empty();
          let $sequential_count: u32 = sequential_index + 1;
          let $parallel_count: u32 = parallel_index;
          $conclude_distribution
        }
        block_idx = sequential_index;
      }
    } else {
      // This is not the first thread. This thread goes from right to left.
      loop {
        let index = work_index.fetch_add(1 << 16, Ordering::Relaxed);
        let sequential_index = index & 0xFFFF;
        let parallel_index = index >> 16;
        let count_claimed = sequential_index + parallel_index + 1;
        if count_claimed > work_size {
          // Everything is claimed
          empty_signal.task_empty();
          break;
        } else if count_claimed == work_size {
          // This is the last iteration
          empty_signal.task_empty();
          let $sequential_count: u32 = sequential_index;
          let $parallel_count: u32 = parallel_index + 1;
          $conclude_distribution
        }
        let block_index = work_size - parallel_index - 1;
        let $block_index_2: u32 = block_index;
        $other_threads
      }
    }
  }
}
pub(crate) use workassisting_loop_two_sided;
