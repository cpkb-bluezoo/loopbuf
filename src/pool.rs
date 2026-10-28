// Copyright (C) 2026 Chris Burdess <dog@gnu.org>

//! Power-of-two pool of reusable [`LoopBuf`]s, mirroring `hopf-core`'s own
//! `BufferPool` (itself the Gumdrop `DirectByteBufferPool` role) -- except
//! pooling a fixed-capacity ring instead of a plain `Vec<u8>`, so whatever
//! acquires a released buffer gets the wraparound-reuse behavior for free
//! too, not just the allocation.

use std::sync::Mutex;

use crate::LoopBuf;

const MIN_BUCKET: usize = 4096;
const MAX_BUCKET: usize = 1024 * 1024;
const DEFAULT_PER_BUCKET: usize = 64;

/// Global pool of reusable, fixed-capacity [`LoopBuf`]s, bucketed by
/// power-of-two capacity.
pub struct LoopBufPool {
    buckets: Mutex<Vec<Vec<LoopBuf>>>,
    max_per_bucket: usize,
}

impl LoopBufPool {
    /// Create a pool with the given maximum free buffers per size class.
    pub fn new(max_per_bucket: usize) -> Self {
        let n = bucket_index(MAX_BUCKET) + 1;
        Self {
            buckets: Mutex::new((0..n).map(|_| Vec::new()).collect()),
            max_per_bucket,
        }
    }

    /// Acquire an empty [`LoopBuf`] with capacity at least `min_capacity`
    /// (rounded up to a power of two).
    pub fn acquire(&self, min_capacity: usize) -> LoopBuf {
        let cap = round_up_pow2(min_capacity.clamp(MIN_BUCKET, MAX_BUCKET));
        let idx = bucket_index(cap);
        if let Ok(mut buckets) = self.buckets.lock() {
            if let Some(buf) = buckets.get_mut(idx).and_then(|b| b.pop()) {
                return buf;
            }
        }
        LoopBuf::with_capacity(cap)
    }

    /// Return a buffer to the pool. Non-power-of-two or oversized buffers
    /// are dropped.
    pub fn release(&self, mut buf: LoopBuf) {
        buf.clear();
        let cap = buf.capacity();
        if !(MIN_BUCKET..=MAX_BUCKET).contains(&cap) || !cap.is_power_of_two() {
            return;
        }
        let idx = bucket_index(cap);
        if let Ok(mut buckets) = self.buckets.lock() {
            if let Some(bucket) = buckets.get_mut(idx) {
                if bucket.len() < self.max_per_bucket {
                    bucket.push(buf);
                }
            }
        }
    }
}

impl Default for LoopBufPool {
    fn default() -> Self {
        Self::new(DEFAULT_PER_BUCKET)
    }
}

fn round_up_pow2(n: usize) -> usize {
    n.next_power_of_two().max(MIN_BUCKET)
}

fn bucket_index(pow2_cap: usize) -> usize {
    debug_assert!(pow2_cap.is_power_of_two());
    pow2_cap.trailing_zeros() as usize - MIN_BUCKET.trailing_zeros() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_roundtrip() {
        let pool = LoopBufPool::new(8);
        let mut a = pool.acquire(100);
        assert!(a.capacity() >= 4096);
        a.extend_from_slice(b"hi");
        pool.release(a);
        let b = pool.acquire(100);
        assert!(b.is_empty());
        assert!(b.capacity() >= 4096);
    }

    #[test]
    fn release_drops_non_power_of_two_capacity() {
        let pool = LoopBufPool::new(8);
        pool.release(LoopBuf::with_capacity(5000)); // not a power of two
        // Nothing was stored, so acquiring the same size class allocates fresh.
        let buckets = pool.buckets.lock().unwrap();
        assert!(buckets.iter().all(|b| b.is_empty()));
    }

    #[test]
    fn release_drops_oversized_capacity() {
        let pool = LoopBufPool::new(8);
        pool.release(LoopBuf::with_capacity(MAX_BUCKET * 2));
        let buckets = pool.buckets.lock().unwrap();
        assert!(buckets.iter().all(|b| b.is_empty()));
    }

    #[test]
    fn release_drops_once_bucket_is_full() {
        let pool = LoopBufPool::new(1);
        pool.release(LoopBuf::with_capacity(MIN_BUCKET));
        pool.release(LoopBuf::with_capacity(MIN_BUCKET));
        let buckets = pool.buckets.lock().unwrap();
        assert_eq!(buckets[bucket_index(MIN_BUCKET)].len(), 1);
    }
}
