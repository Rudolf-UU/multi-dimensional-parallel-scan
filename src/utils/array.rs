use core::sync::atomic::AtomicU64;

pub unsafe fn alloc_undef_u64_array(length: usize) -> Box<[AtomicU64]> {
  let mut vector = Vec::with_capacity(length);
  vector.set_len(length);
  vector.into_boxed_slice()
}

#[derive(Debug)]
pub struct MultArray<const N: usize> {
  data: Box<[AtomicU64]>,
  shape: [usize; N],
}

impl<const N: usize> MultArray<N> {
  fn calc_size(shape: &[usize; N]) -> usize {
    let mut cap = 1;
    for &x in shape {
        cap = usize::checked_mul(cap, x).expect("vector capacity overflowed usize");
    }
    cap
  }

  pub unsafe fn new(shape: [usize; N]) -> Self {
    let length = Self::calc_size(&shape);
    MultArray { data: alloc_undef_u64_array(length), shape }
  }

  pub fn get_inner_size(&self) -> usize {
    match self.shape.last() {
      None => 0,
      Some(val) => *val
    }
  }

  pub fn total_inner_count(&self) -> usize {
    let mut count = 1;
    let size = self.shape.len();

    if size == 0 {0}
    else if size == 1 {1}
    else {
      for &x in self.shape.split_last().unwrap().1 {
        count = usize::checked_mul(count, x).expect("vector capactiy overflowed usize");
      }
      count
    }
  }

  pub fn get_data(&self) -> &Box<[AtomicU64]> {
    &self.data
  } 

  pub fn get_shape(&self) -> &[usize; N] {
    &self.shape
  }
}

pub struct Shape<I: ?Sized>{
  index: I,
}