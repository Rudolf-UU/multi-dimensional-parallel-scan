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
macro_rules! workassisting_loop_row_column_old {
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
pub(crate) use workassisting_loop_row_column_old;

// The two-sided workassisting loop for row-column scanning
// The first thread start sequential on rowwise scan
// Parallel threads start columnwise scanning on next row
// Both can assist the other scan method when finished
#[macro_export]
macro_rules! workassisting_loop_row_column {
  ($loop_arguments_expr: expr, $seg_count: ident, |$block_index_1: ident| $multiple_rows_scan: block, |$block_index_2: ident| $row_wise_scan: block, |$block_index_3: ident, $rows_completed: ident| $column_wise_scan: block) => {
    let loop_arguments: LoopArguments = $loop_arguments_expr;
    let work_size: u32 = loop_arguments.work_size;
    let work_index: &AtomicU32 = loop_arguments.work_index;
    let mut empty_signal: EmptySignal = loop_arguments.empty_signal;
    let segments = $seg_count;

    let mut block_idx = loop_arguments.first_index;

    assert!(work_size < 1 << 15);

    if segments == 1 { 
      // A row (optionally multiple rows) can fit within a single block of BLOCK_SIZE.
      // Therefore all threads will claim blocks consecutively and sequentially scan the row(s) within.
      loop {  
        let $block_index_1 = block_idx;
        $multiple_rows_scan

        block_idx = work_index.fetch_add(1, Ordering::Relaxed);

        if block_idx == work_size - 1 {
          empty_signal.task_empty();
        } else if block_idx >= work_size {
          empty_signal.task_empty();
          break;
        }
      }
    } 
    else { 
      // The data rows are represented by multiple blocks of BLOCK_SIZE.
      // Therefore, the first thread starts claiming consecutive blocks in row-wise order, 
      // and adapts to column-wise order when other threads join the computation.
      let mut rowwise_thread = loop_arguments.first_index == 0;
      let mut rowwise_idx = block_idx >> 16;
      let mut colwise_idx = block_idx & 0xFFFF;
      let mut rowwise_claimed_rows = 0;
      let mut rowwise_work_size = work_size;
      let mut colwise_work_size = 0;

      if !rowwise_thread {
        rowwise_claimed_rows = (rowwise_idx + segments - 1) / segments;
        rowwise_work_size = rowwise_claimed_rows * segments;
        colwise_work_size = work_size - (rowwise_claimed_rows * segments);

        if colwise_work_size > 0 { // Row available, start scanning column-wise
          let $block_index_3 = colwise_idx;
          let $rows_completed = rowwise_claimed_rows;
          $column_wise_scan
        } else { // No (unclaimed) rows available, assist with row-wise scanning
          rowwise_thread = true;
        }
      }
      
      loop {
        if rowwise_thread {
          // There is only a single thread active, or the current row has not been finished.
          // Perform the scan operation in a consecutive order
          let res = work_index.compare_exchange_weak(block_idx, block_idx + (1 << 16), Ordering::Relaxed, Ordering::Relaxed);

          if res.is_ok() {
            let $block_index_2 = rowwise_idx;
            $row_wise_scan
          } 
          
          block_idx = work_index.load(Ordering::Relaxed);
          rowwise_idx = block_idx >> 16;
          colwise_idx = block_idx & 0xFFFF;

          if colwise_idx > 0 {
            // Parallel thread joined the computation, finish current row and then switch to column-wise
            rowwise_claimed_rows = (rowwise_idx + segments - 1) / segments;
            rowwise_work_size = rowwise_claimed_rows * segments;
            colwise_work_size = work_size - rowwise_work_size;
            rowwise_thread = rowwise_idx < rowwise_work_size;
          }

          let claimed = rowwise_idx + colwise_idx.min(colwise_work_size) + 1;

          if claimed > work_size {
            empty_signal.task_empty();
            break;
          } else if claimed == work_size {
            empty_signal.task_empty();
          } 
        }
        else {
          // There are multiple threads active.
          // Perform the scan operation in a column-wise order   
          block_idx = work_index.fetch_add(1, Ordering::Relaxed);
          rowwise_idx = block_idx >> 16;
          colwise_idx = block_idx & 0xFFFF;
          let claimed = rowwise_idx.min(rowwise_work_size) + colwise_idx + 1;

          if claimed > work_size {
            empty_signal.task_empty();
            break;
          } else if claimed == work_size {
            empty_signal.task_empty();
          }

          if colwise_idx >= colwise_work_size { 
            // The column-wise scan is finished, assist the row-wise scan
            rowwise_thread = true;
            continue;
          }

          let $block_index_3 = colwise_idx;
          let $rows_completed = rowwise_claimed_rows;
          $column_wise_scan
        } 
      }
    }
  }
}
pub(crate) use workassisting_loop_row_column;