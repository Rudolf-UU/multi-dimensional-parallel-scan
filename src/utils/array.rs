use core::sync::atomic::AtomicU64;

pub unsafe fn alloc_undef_u64_array(length: usize) -> Box<[AtomicU64]> {
  let mut vector = Vec::with_capacity(length);
  vector.set_len(length);
  vector.into_boxed_slice()
}
