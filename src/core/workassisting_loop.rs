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

// The two-sided workassisting loop for row-column scanning
// The first thread starts sequential with row-wise scanning
// Parallel threads start columnwise scanning on the next (unclaimed) row
// Both can assist the other scan method upon finishing
#[macro_export]
macro_rules! workassisting_loop_row_column {
  ($loop_arguments_expr: expr, $seg_count: ident, |$block_index_1: ident| $multiple_rows_scan: block, 
                                                  |$block_index_2: ident| $row_wise_scan: block, 
                                                  |$block_index_3: ident, $rows_completed: ident| $column_wise_scan: block) => {
    let loop_arguments: LoopArguments = $loop_arguments_expr;
    let work_size: u32 = loop_arguments.work_size;
    let work_index: &AtomicU32 = loop_arguments.work_index;
    let mut empty_signal: EmptySignal = loop_arguments.empty_signal;
    let segments = $seg_count;

    let mut block_idx = loop_arguments.first_index;
    
    assert!(work_size < 1 << 15);

    if segments == 1 { 
      // A row (optionally multiple rows) can fit within a single block of size BLOCK_SIZE.
      // Therefore all threads will claim blocks consecutively and sequentially scan the row(s) within their block.
      if loop_arguments.first_index == 0 { 
        block_idx = work_index.fetch_add(1, Ordering::Relaxed) & 0xFFFF; 
      } else { 
        block_idx = block_idx & 0xFFFF; 
      }

      loop {  
        let $block_index_1 = block_idx;
        $multiple_rows_scan

        block_idx = work_index.fetch_add(1, Ordering::Relaxed) & 0xFFFF;

        if block_idx == work_size - 1 {
          empty_signal.task_empty();
        } else if block_idx >= work_size {
          empty_signal.task_empty();
          break;
        }
      }
    } else { 
      // The data rows are represented by multiple blocks of size BLOCK_SIZE.
      // Therefore, the first thread starts claiming consecutive blocks in row-wise order, 
      // and adapts to column-wise order when other threads join the computation.
      let mut rowwise_thread = loop_arguments.first_index == 0;
      let mut rowwise_idx = block_idx >> 16;
      let mut colwise_idx = block_idx & 0xFFFF;
      let mut rowwise_claimed_rows = 0;
      let mut rowwise_work_size = work_size;
      let mut colwise_work_size = 0;

      if rowwise_thread { // Execute the first row-wise block (index 0)
        let $block_index_2 = rowwise_idx;
        $row_wise_scan
      } else { // Determine if column-wise scanning is possible
        rowwise_claimed_rows = (rowwise_idx + segments - 1) / segments;
        rowwise_work_size = rowwise_claimed_rows * segments;
        colwise_work_size = work_size - (rowwise_claimed_rows * segments);

        if colwise_work_size > 0 {
          let $block_index_3 = colwise_idx;
          let $rows_completed = rowwise_claimed_rows;
          $column_wise_scan
        } else { // No (unclaimed) rows available, assist with row-wise scanning
          rowwise_thread = true;
        }
      } 
      
      loop {
        if rowwise_thread { 
          // There is only a single thread active, or other threads have finished column-wise scanning
          let res = work_index.compare_exchange_weak(block_idx, block_idx + (1 << 16), Ordering::Relaxed, Ordering::Relaxed);

          if res.is_ok() {
            let $block_index_2 = rowwise_idx;
            $row_wise_scan
          } 
          
          block_idx = work_index.load(Ordering::Relaxed);
          rowwise_idx = block_idx >> 16;
          colwise_idx = block_idx & 0xFFFF;

          if colwise_idx > 0 {
            // Parallel thread(s) joined the computation, finish current row and then switch to column-wise
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

          if colwise_idx >= colwise_work_size { // Column-wise scan is finished, assist row-wise
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