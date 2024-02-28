use core::sync::atomic::{Ordering, AtomicU64};
use crate::core::worker::*;
use crate::utils;
use crate::cases::scan;

const SIZE: usize = 1024 * 1024 * 512 / 8;

pub fn run(cpp_enabled: bool, inplace: bool) {
  let input = unsafe { utils::array::alloc_undef_u64_array(SIZE) };
  scan::fill(&input);
  let output = unsafe { utils::array::alloc_undef_u64_array(SIZE) };

  let output_ref = if inplace { &input } else { &output };

  println!();
  println!("Ratio between sequential and parallel mode ({})", if inplace { "in-place" } else { "out-of-place" });
  case("Theoretical", |thread_count| 1.0 / thread_count as f32);
  case_average("Assisted scan-then-propagate", |thread_count| {
    if inplace { scan::fill(&input); }
    let sequential_size = AtomicU64::new(0);
    let task = scan::our_scan_then_propagate::create_task(&input, output_ref, Some(&sequential_size));
    Workers::run(thread_count, task);
    sequential_size.load(Ordering::Relaxed) as f32 / SIZE as f32
  });
  case_average("Assisted reduce-then-scan", |thread_count| {
    if inplace { scan::fill(&input); }
    let sequential_size = AtomicU64::new(0);
    let task = scan::our_reduce_then_scan::create_task(&input, output_ref, Some(&sequential_size));
    Workers::run(thread_count, task);
    sequential_size.load(Ordering::Relaxed) as f32 / SIZE as f32
  });
  if cpp_enabled {
    case("oneTBB", |thread_count| {
      let child = std::process::Command::new("./reference-cpp/build/main-tbb")
        .env("LD_LIBRARY_PATH", "./reference-cpp/oneTBB-install/lib")
        .arg(if inplace { "scan-measure-ratio-inplace" } else { "scan-measure-ratio" })
        .arg(SIZE.to_string())
        .arg(thread_count.to_string())
        .output()
        .expect("Reference sequential C++ implementation failed");

      let ratio_str = String::from_utf8_lossy(&child.stdout);
      ratio_str.trim().parse().expect(&("Unexpected output from reference C++ program: ".to_owned() + &ratio_str))  
    });
  }

  fn case<F: FnMut(usize) -> f32>(name: &str, mut f: F) {
    println!("{}:", name);
    for thread_count in [1, 2, 3, 4, 6, 8, 10, 12, 14, 16] {
      let ratio = f(thread_count);
      println!("  {:02} threads {:.0}%", thread_count, ratio * 100.0);
    }
  }
  fn case_average<F: FnMut(usize) -> f32>(name: &str, mut f: F) {
    case(name, |thread_count| {
      let mut value = 0.0;
      for _ in 0 .. 50 {
        value += f(thread_count);
      }
      value / 50 as f32
    })
  }
}