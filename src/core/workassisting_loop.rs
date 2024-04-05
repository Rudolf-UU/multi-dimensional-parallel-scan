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
macro_rules! workassisting_loop_row_column {
  ($loop_arguments_expr: expr, $seg_count: ident, |$block_index_1: ident| $multiple_rows_scan: block, |$block_index_2: ident| $row_wise_scan: block, |$block_index_3: ident, $rows_completed: ident| $column_wise_scan: block) => {
    let loop_arguments: LoopArguments = $loop_arguments_expr;
    let work_size: u32 = loop_arguments.work_size;
    let work_index: &AtomicU32 = loop_arguments.work_index;
    let mut empty_signal: EmptySignal = loop_arguments.empty_signal;
    let segments = $seg_count;

    let mut block_idx = loop_arguments.first_index;

    if segments == 1 { 
      // A row (optionally multiple rows) can fit within a single block of BLOCK_SIZE.
      // Therefore all threads will claim blocks consecutively and sequential scan the row(s) within.
      loop {
        block_idx = work_index.fetch_add(1, Ordering::Relaxed);
        
        if block_idx == work_size - 1 {
          empty_signal.task_empty();
        } else if block_idx >= work_size {
          empty_signal.task_empty();
          break;
        }
  
        let $block_index_1: u32 = block_idx;
        $multiple_rows_scan
      }
    } 
    else { 
      // The data rows are represented by multiple blocks of BLOCK_SIZE.
      // Therefore, the first thread starts claiming consecutive blocks in row-wise order, 
      // and adapts to column-wise order when multiple threads join the computation.
      let mut seq_idx = block_idx >> 16;
      let mut par_idx = block_idx & 0xFFFF;
      let mut claimed:u32;

      assert!(work_size < 1 << 15);
      
      if loop_arguments.first_index != 0  {
        // This is not the first thread. Therefore, we increase the par_idx and perform the computation,
        // such that other threads can detect that we switch to column-wise order
        block_idx = work_index.fetch_add(1, Ordering::Relaxed);
        seq_idx = block_idx >> 16;
        par_idx = block_idx & 0xFFFF;
        let seq_rows = (seq_idx + segments - 1) / segments;

        if (seq_rows * segments) + par_idx < work_size {
          let $block_index_3 = par_idx;
          let $rows_completed = seq_rows;
          $column_wise_scan
        }
      }
      
      loop {
        if par_idx == 0 || seq_idx % segments != 0 {
          // There is only a single thread active, or the current row has not been finished.
          // Perform the scan operation in a consecutive order
          let res = work_index.compare_exchange_weak(block_idx, block_idx + (1 << 16), Ordering::Relaxed, Ordering::Relaxed);

          if res.is_ok() {
            let $block_index_2 = seq_idx;
            $row_wise_scan
          } 
          
          block_idx = work_index.load(Ordering::Relaxed);
          seq_idx = block_idx >> 16;
          par_idx = block_idx & 0xFFFF;
          claimed = seq_idx + par_idx + 1;

          if seq_idx >= work_size {
            empty_signal.task_empty();
            break;
          } else if claimed == work_size {
            empty_signal.task_empty();
          }
        } else {
          // There are multiple threads active.
          // Perform the scan operation in a column-wise order    
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

          let $block_index_3 = par_idx;
          let $rows_completed = (seq_idx + segments - 1) / segments;

          $column_wise_scan
        } 
      }
    }
  };
}
pub(crate) use workassisting_loop_row_column;