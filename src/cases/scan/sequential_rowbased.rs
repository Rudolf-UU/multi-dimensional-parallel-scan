use core::sync::atomic::{Ordering, AtomicU64};
use crate::cases::scan::scan_sequential;
use crate::core::worker::*;
use crate::core::task::*;
use crate::core::workassisting_loop::*;
use crate::utils::array::MultArray;

struct Data<'a> {
  input: &'a [AtomicU64],
  output: &'a [AtomicU64],
  inner_size: u64
}

pub fn create_task<const N: usize>(input_m: &MultArray<AtomicU64, N>, output_m: &MultArray<AtomicU64, N>) -> Task {
  let inner_size = input_m.get_inner_size();
  let inner_rows = input_m.total_inner_count();
  let input = input_m.get_data();
  let output = output_m.get_data();

  Task::new_dataparallel::<Data>(run, finish, Data{ input, output, inner_size: inner_size as u64}, inner_rows as u32, false)
}

fn run(_workers: &Workers, task: *const TaskObject<Data>, loop_arguments: LoopArguments) {
    // Sequentially scan the row(s) within the block
    let data = unsafe { TaskObject::get_data(task) };

    workassisting_loop!(loop_arguments, |block_index| {
      let start = block_index as usize * data.inner_size as usize;
      let end = (start + data.inner_size as usize).min(data.input.len());

      scan_sequential(&data.input[start .. end], 0, &data.output[start .. end]);
    });
}

fn finish(workers: &Workers, task: *mut TaskObject<Data>) {
  let _ = unsafe { TaskObject::take_data(task) };
  workers.finish();
}
