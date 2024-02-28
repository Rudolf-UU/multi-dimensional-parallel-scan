use core::fmt::Debug;
use std::time;
use std::fs::File;
use std::io::{prelude::*, BufWriter};

pub struct Benchmarker<T> {
  chart_style: ChartStyle,
  name: String,
  max_speedup: u32,
  max_threads: u32,
  reference_time: u64,
  reference_time_cpp: Option<u64>,
  expected: T,
  output: Vec<(String, u32, Option<u32>, bool, Vec<f32>)>
}

pub const THREAD_COUNTS: [usize; 14] = [1, 2, 3, 4, 6, 8, 10, 12, 14, 16, 20, 24, 28, 32];
pub const RUNS: usize = 50;

#[derive(PartialEq, Eq, Copy, Clone)]
pub enum ChartStyle {
  WithKey,
  WithoutKey
}

pub fn benchmark<T: Debug + Eq, P: FnMut() -> (), Ref: FnMut() -> T>(chart_style: ChartStyle, name: &str, prepare: P, reference: Ref) -> Benchmarker<T> {
  benchmark_with_max_speedup(chart_style, name, prepare, reference, 16, 6)
}

pub fn benchmark_with_max_speedup<T: Debug + Eq, P: FnMut() -> (), Ref: FnMut() -> T>(chart_style: ChartStyle, name: &str, prepare: P, reference: Ref, max_threads: u32, max_speedup: u32) -> Benchmarker<T> {
  println!("");
  println!("Benchmark {}", name);
  let (expected, reference_time) = time(RUNS, prepare, reference);
  println!("Sequential   {} ms", reference_time / 1000);
  Benchmarker{ chart_style, name: name.to_owned(), max_threads, max_speedup, reference_time, reference_time_cpp: None, expected, output: vec![] }
}

impl<T: Copy + Debug + Eq + Send> Benchmarker<T> {
  pub fn parallel<Prepare: FnMut() -> (), Par: FnMut(usize) -> T>(mut self, name: &str, chart_line_style: u32, point_type: Option<u32>, our: bool, mut prepare: Prepare, mut parallel: Par) -> Self {
    println!("{}", name);
    let mut results = vec![];
    for thread_count in THREAD_COUNTS {
      if thread_count > self.max_threads as usize {
        break;
      }

      let (value, time) = time(RUNS, || { prepare() }, || parallel(thread_count));
      assert_eq!(self.expected, value);
      let relative = self.reference_time as f32 / time as f32;
      results.push(relative);
      println!("  {:02} threads {} ms ({:.2}x)", thread_count, time / 1000, relative);
    }
    self.output.push((name.to_owned(), chart_line_style, point_type, our, results));
    self
  }

  pub fn cpp_sequential(mut self, cpp_enabled: bool, name: &str, cpp_name: &str, size: usize) -> Self {
    if !cpp_enabled { return self; }

    let mut command = std::process::Command::new("./reference-cpp/build/main");
    command.env("LD_LIBRARY_PATH", "./reference-cpp/oneTBB-install/lib")
      .arg(cpp_name)
      .arg(size.to_string());

    let child = command
      .output()
      .expect("Reference sequential C++ implementation failed");
  
    let time_str = String::from_utf8_lossy(&child.stdout);
    let time: u64 = time_str.trim().parse().expect("Unexpected output from reference C++ program");
    let relative = self.reference_time as f32 / time as f32;

    if name.len() <= 12 {
      println!("{:12} {} ms ({:.2}x)", name, time / 1000, relative);
    } else {
      println!("{}", name);
      println!("{:12} {} ms ({:.2}x)", "", time / 1000, relative);
    }

    self.reference_time_cpp = Some(time);

    self
  }

  pub fn cpp_tbb(mut self, cpp_enabled: bool, name: &str, chart_line_style: u32, point_type: Option<u32>, cpp_name: &str, size: usize) -> Self {
    if !cpp_enabled { return self; }

    println!("{}", name);
    let mut results = vec![];
    for thread_count in THREAD_COUNTS {
      if thread_count > self.max_threads as usize {
        break;
      }

      let mut command = std::process::Command::new("./reference-cpp/build/main-tbb");
      command.env("LD_LIBRARY_PATH", "./reference-cpp/oneTBB-install/lib")
        .arg(cpp_name)
        .arg(size.to_string())
        .arg(thread_count.to_string());

      let child = command
        .output()
        .expect("Reference oneTBB C++ implementation failed");

      let time_str = String::from_utf8_lossy(&child.stdout);
      let time: u64 = time_str.trim().parse().expect(&("Unexpected output from reference C++ program: ".to_owned() + &time_str));
      let relative = self.reference_time as f32 / time as f32;
      results.push(relative);
      println!("  {:02} threads {} ms ({:.2}x)", thread_count, time / 1000, relative);
    }
    self.output.push((name.to_owned(), chart_line_style, point_type, false, results));

    self
  }

  pub fn cpp_parlay(mut self, cpp_enabled: bool, name: &str, chart_line_style: u32, point_type: Option<u32>, cpp_name: &str, size: usize) -> Self {
    if !cpp_enabled { return self; }

    println!("{}", name);
    let mut results = vec![];
    for thread_count in THREAD_COUNTS {
      if thread_count > self.max_threads as usize {
        break;
      }

      let mut command = std::process::Command::new("./reference-cpp/build/main-parlaylib");
      command.env("PARLAY_NUM_THREADS", thread_count.to_string())
        .arg(cpp_name)
        .arg(size.to_string());

      let child = command
        .output()
        .expect("Reference parlay C++ implementation failed");

      let time_str = String::from_utf8_lossy(&child.stdout);
      let time: u64 = time_str.trim().parse().expect(&("Unexpected output from reference C++ program: ".to_owned() + &time_str));
      let relative = self.reference_time as f32 / time as f32;
      results.push(relative);
      println!("  {:02} threads {} ms ({:.2}x)", thread_count, time / 1000, relative);
    }
    self.output.push((name.to_owned(), chart_line_style, point_type, false, results));

    self
  }
}

impl<T> Drop for Benchmarker<T> {
  fn drop(&mut self) {
    std::fs::create_dir_all("./results").unwrap();
    let filename = "./results/".to_owned() + &self.name.replace(' ', "_").replace('(', "").replace(')', "").replace('=', "_").replace('/', "_");

    // Create .gnuplot file
    let file_gnuplot = File::create(filename.clone() + ".gnuplot").unwrap();
    let mut writer_gnuplot = BufWriter::new(&file_gnuplot);
    writeln!(&mut writer_gnuplot, "set title \"{}\"", self.name).unwrap();
    writeln!(&mut writer_gnuplot, "set terminal pdf size 3.2,2.8").unwrap();
    writeln!(&mut writer_gnuplot, "set output \"{}\"", filename.clone() + ".pdf").unwrap();
    if self.chart_style == ChartStyle::WithKey {
      writeln!(&mut writer_gnuplot, "set key on").unwrap();
      writeln!(&mut writer_gnuplot, "set key top left Left reverse").unwrap();
    } else {
      writeln!(&mut writer_gnuplot, "set key off").unwrap();
    }
    writeln!(&mut writer_gnuplot, "set xrange [1:{}]", self.max_threads).unwrap();
    writeln!(&mut writer_gnuplot, "set xtics (1, 4, 8, 12, 16, 20, 24, 28, 32)").unwrap();
    writeln!(&mut writer_gnuplot, "set xlabel \"Number of threads\"").unwrap();
    writeln!(&mut writer_gnuplot, "set yrange [0:{}]", self.max_speedup).unwrap();
    writeln!(&mut writer_gnuplot, "set ylabel \"Speedup\"").unwrap();

    write!(&mut writer_gnuplot, "plot ").unwrap();
    for (idx, result) in self.output.iter().enumerate() {
      if idx != 0 {
        write!(&mut writer_gnuplot, ", \\\n  ").unwrap();
      }
      write!(&mut writer_gnuplot, "'{}.dat' using 1:{} title \"{}\" ls {} lw 1 pointsize {}", filename, idx+2, result.0, result.1, if result.3 { 0.7 } else { 0.6 }).unwrap();
      if let Some(point_type) = result.2 {
        write!(&mut writer_gnuplot, " pointtype {}", point_type).unwrap();
      }
      write!(&mut writer_gnuplot, " with linespoints").unwrap();
    }
    writeln!(&mut writer_gnuplot, "").unwrap();
    drop(writer_gnuplot);

    // Create .dat file with data points
    let file_data = File::create(filename.clone() + ".dat").unwrap();
    let mut writer_data = BufWriter::new(&file_data);
    write!(&mut writer_data, "# Benchmark {}\n", self.name).unwrap();
    write!(&mut writer_data, "# Speedup compared to a sequential implementation.\n").unwrap();
    
    // Header
    write!(&mut writer_data, "# NCPU").unwrap();
    for result in &self.output {
      write!(&mut writer_data, "\t{}", result.0).unwrap();
    }
    write!(&mut writer_data, "\n").unwrap();

    for (idx, thread_count) in THREAD_COUNTS.iter().enumerate() {
      write!(&mut writer_data, "{}", thread_count).unwrap();
      for result in &self.output {
        if idx < result.4.len() {
          write!(&mut writer_data, "\t{}", result.4[idx]).unwrap();
        } else {
          write!(&mut writer_data, "\t").unwrap();
        }
      }
      write!(&mut writer_data, "\n").unwrap();
    }
    drop(writer_data);

    _ = std::process::Command::new("gnuplot")
      .arg(filename.clone() + ".gnuplot")
      .spawn();

    // Create .tex file with table of results
    let file_tex = File::create(filename.clone() + ".tex").unwrap();
    let mut writer_tex = BufWriter::new(&file_tex);

    // Don't show high thread counts or thread counts between 8 and 16, as the results don't change that much there.
    let table_thread_counts: Vec<usize> =
      if self.max_threads == 24 {
        vec![1, 2, 3, 4, 8, 16, 24]
      } else {
        vec![1, 2, 3, 4, 6, 8, 16]
      };

    // Note that { is escaped as {{ in Rust, } as }} and \ as \\.

    // begin tabular, specify columns
    write!(&mut writer_tex, "\\begin{{tabular}}{{cl").unwrap();
    for _ in &table_thread_counts {
      write!(&mut writer_tex, "r").unwrap();
    }
    write!(&mut writer_tex, "}}\n\\toprule\n").unwrap();

    // Table header
    write!(&mut writer_tex, "\\multicolumn{{2}}{{c}}{{\\textbf{{Number of threads}}}}").unwrap();
    for thread_count in &table_thread_counts[0 .. table_thread_counts.len() - 2] {
      write!(&mut writer_tex, " & \\multicolumn{{1}}{{c}}{{\\textbf{{ {} }}}}", thread_count).unwrap();
    }
    write!(&mut writer_tex, " & \\multicolumn{{2}}{{c}}{{\\textbf{{ {} }} \\dots \\textbf{{ {} }}}}", table_thread_counts[table_thread_counts.len() - 2], table_thread_counts[table_thread_counts.len() - 1]).unwrap();
    write!(&mut writer_tex, " \\\\\n\\midrule\n").unwrap();

    // Sequential reference times
    for (name, o_time) in [("Sequential (Rust)", Some(self.reference_time)), ("Sequential (C++)", self.reference_time_cpp)] {
      if let Some(time) = o_time {
        let ms = time / 1000;
        write!(&mut writer_tex, "& {} & \\multicolumn{{1}}{{r}}{{ {:.2} }} & \\multicolumn{{ {} }}{{l}}{{({} ms)}}", name, self.reference_time as f32 / time as f32, table_thread_counts.len() - 1, ms).unwrap();
        write!(&mut writer_tex, " \\\\\n").unwrap();
      }
    }

    // Parallel times
    for result in &self.output {
      let color_factor = if result.3 { 30 } else { 10 };
      write!(&mut writer_tex, "\\rowcolor{{gnuplot{}!{}}}", result.1, color_factor).unwrap();
      write!(&mut writer_tex, "$\\color{{gnuplot{}}}{{{}}}$", result.1, latex_symbol(result)).unwrap();
      if result.3 {
        write!(&mut writer_tex, " & \\textit{{{}}}", result.0).unwrap();
      } else {
        write!(&mut writer_tex, " & {}", result.0).unwrap();
      }
      for (idx, &value) in result.4.iter().enumerate() {
        if table_thread_counts.contains(&THREAD_COUNTS[idx]) {
          write!(&mut writer_tex, " & \\cellcolor{{gnuplot{}!{}}} {:.2}", result.1, color_factor, value).unwrap();
        }
      }
      write!(&mut writer_tex, " \\\\\n").unwrap();
    }
    write!(&mut writer_tex, "\\bottomrule\n").unwrap();
    write!(&mut writer_tex, "\\end{{tabular}}\n").unwrap();
    drop(writer_tex);
  }
}

pub fn time<T: Debug + Eq, P: FnMut() -> (), F: FnMut() -> T>(runs: usize, mut prepare: P, mut f: F) -> (T, u64) {
  prepare();
  let first = f();
  
  let mut elapsed = std::time::Duration::ZERO;
  for _ in 0 .. runs {
    prepare();
    let timer = time::Instant::now();
    let result = f();
    elapsed += timer.elapsed();
    assert_eq!(first, result);
  }

  (first, (elapsed.as_micros() / runs as u128) as u64)
}

fn latex_symbol(result: &(String, u32, Option<u32>, bool, Vec<f32>)) -> &str {
  match result.1 {
    3 => "\\smallblackdiamond",
    5 => "\\blacksquare",
    7 => "\\scalebox{1.1}{$\\bullet$}",
    2 => "\\smalldiamond",
    4 => "\\square",
    6 => "\\scalebox{1.1}{$\\circ$}",
    1 => "+",
    8 => "\\triangle",
    _ => "?"
  }
}
