mod core;
mod cases;
mod utils;

use std::{path::Path, io::stdin};
use crate::utils::thread_pinning;

fn main() {
  let cpp_enabled = setup_cpp();

  if !cpp_enabled {
    println!("Running the benchmarks without the C++ implementations.");
  }

  affinity::set_thread_affinity([thread_pinning::AFFINITY_MAPPING[0]]).unwrap();

  cases::scan::run(cpp_enabled);
  cases::scan::run_multidim(cpp_enabled);
  cases::scan::run_inplace(cpp_enabled);
  cases::scan::run_inplace_multidim(cpp_enabled);
  //cases::compact::run(cpp_enabled);
}

// Utility to install and build the c++ implementation.
fn setup_cpp() -> bool {
  if !Path::new("./reference-cpp/build").is_dir() {
    println!("This benchmark program provides a reference implementation in C++. This requires Linux and clang++.");
    println!("Do you want to enable the C++ reference implementation? y/n");
    
    let enable = ask();
    if !enable {
      return false;
    }
  }

  // Build C++ code
  println!("Building the C++ code");
  match std::process::Command::new("sh").arg("./reference-cpp/build.sh").spawn() {
    Ok(mut child) => {
      match child.wait() {
        Ok(result) => {
          if !result.success() {
            println!("Build of C++ code failed.");
            return false;
          }
        }
        Err(_) => {
          println!("Build of C++ code failed.");
          return false;
        },
      }
    },
    Err(_) => {
      println!("Build of C++ code failed.");
      return false;
    }
  }

  true
}

fn ask() -> bool {
  loop {
    let mut s = String::new();
    stdin().read_line(&mut s).expect("Couldn't read from console");
    s = s.to_lowercase();
    if s.trim() == "yes" || s.trim() == "y" {
      return true;
    } else if s.trim() == "no" || s.trim() == "n" {
      return false;
    } else {
      println!("Invalid response, answer with y or n");
    }
  }
}
