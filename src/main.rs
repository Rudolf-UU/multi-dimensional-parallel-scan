mod core;
mod cases;
mod utils;

use std::{path::Path, io::stdin};

fn main() {
  let cpp_enabled = setup_cpp();

  if !cpp_enabled {
    println!("Running the benchmarks without the C++ and oneTBB implementations.");
  }

  cases::scan::run(cpp_enabled);
  cases::scan::run_inplace(cpp_enabled);
  cases::scan_ratio::run(cpp_enabled, false);
  cases::scan_ratio::run(cpp_enabled, true);
  cases::compact::run(cpp_enabled);
}

// Utilities to install and build the c++ and oneTBB implementation.
fn setup_cpp() -> bool {
  if !Path::new("./reference-cpp/oneTBB-install").is_dir() {
    println!("This benchmark suite has a reference implementation in C++ and parallel implementations in oneTBB and parlaylib. This requires Linux, clang++ and git. This program will automatically download and install oneTBB and parlaylib (locally in ./reference-cpp), if enabled.");
    println!("Do you want to enable the C++ and oneTBB implementations? y/n");

    let enable = ask();
    if !enable {
      return false;
    }

    println!("Downloading and installing oneTBB locally");
    match std::process::Command::new("sh").arg("./reference-cpp/install-oneTBB.sh").spawn() {
      Ok(mut child) => {
        match child.wait() {
          Ok(result) => {
            if !result.success() {
              println!("Build of oneTBB failed.");
              println!("The log above may contain errors and you may inspect ./reference-cpp/oneTBB.");
              println!("Remove ./reference-cpp/oneTBB and ./reference-cpp/oneTBB-install before trying again.");
              return false;
            }
          }
          Err(_) => {
            println!("Build of oneTBB failed.");
            println!("The log above may contain errors and you may inspect ./reference-cpp/oneTBB.");
            println!("Remove ./reference-cpp/oneTBB and ./reference-cpp/oneTBB-install before trying again.");
            return false;
          },
        }
      },
      Err(_) => {
        println!("Build of oneTBB failed.");
        return false;
      }
    }
  }

  if !Path::new("./reference-cpp/parlaylib").is_dir() {
    println!("Downloading parlaylib");
    match std::process::Command::new("sh").arg("./reference-cpp/install-parlaylib.sh").spawn() {
      Ok(mut child) => {
        match child.wait() {
          Ok(result) => {
            if !result.success() {
              println!("Downloading parlaylib failed.");
              println!("The log above may contain errors and you may inspect ./reference-cpp/parlaylib.");
              println!("Remove ./reference-cpp/parlaylib before trying again.");
              return false;
            }
          }
          Err(_) => {
            println!("Downloading parlaylib failed.");
            println!("The log above may contain errors and you may inspect ./reference-cpp/parlaylib.");
            println!("Remove ./reference-cpp/parlaylib before trying again.");
            return false;
          },
        }
      },
      Err(_) => {
        println!("Build of parlaylib failed.");
        return false;
      }
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
