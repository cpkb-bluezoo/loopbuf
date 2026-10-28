// Copyright (C) 2026 Chris Burdess <dog@gnu.org>

//! A fixed-capacity, pooled ring buffer for feeding bytes to push parsers.
//!
//! This exists because the same "accumulate bytes from a producer, let a
//! consumer scan them incrementally, keep whatever's unconsumed for next
//! time" problem was solved four different, ad hoc ways across this
//! workspace: `hopf-core`'s `Vec<u8>` + `BufferPool` + `buf.drain(..consumed)`
//! (a real memmove on every call that doesn't fully drain), `tractrix`'s
//! owned `bytes::Bytes` chunks with `Arc`-refcounted retention, `rjsonparser`'s
//! internal `Vec<u8>` that re-copies its own leftover on every `receive()`
//! call while a token is pending, and plume's own caller-managed `BytesMut`
//! (cheap `advance()`, but a full reallocate-and-copy-forward once tail
//! capacity runs out).
//!
//! A ring buffer removes the actual cost those all share -- reclaiming
//! space already consumed -- entirely: [`LoopBuf::advance`] just moves a
//! cursor, and whatever it frees becomes writable again via wraparound,
//! with no data movement, ever.
//!
//! **The ring never grows.** Its capacity is fixed for life at
//! [`LoopBuf::with_capacity`]. When a single pending token needs more room
//! than that -- `rmimeparser`'s MIME header values are exactly this case,
//! since nothing guarantees one fits in whatever chunk size a caller chose
//! -- the parser copies out of the ring's readable spans into its *own*
//! independently-sized, independently-capped accumulator (`rmimeparser`
//! already does this: `header_value_sink: Vec<u8>`, bounded by
//! `max_header_value_size`, entirely separate from however the feed buffer
//! itself is sized). That decoupling -- feed-buffer size is a throughput
//! knob, token size is a protocol-level limit the parser enforces itself --
//! is deliberate: this type has no business guessing what a "big enough"
//! ring would be for every possible token a parser might see.
//!
//! This is a deliberately new design, not a port of an existing Java class
//! -- `java.nio.ByteBuffer` has no ring-buffer sibling in this workspace's
//! usual Gumdrop ancestry to mirror. Nor is it a dependency on an existing
//! crates.io ring buffer: `ringbuf` (lock-free SPSC) independently converged
//! on the same two-span read/write shape used below, which is good
//! validation, but pulls in a mandatory `crossbeam-utils` dependency never
//! exercised by its single-threaded `LocalRb` and zero-fills every spare
//! span before a socket read (unavoidable for a type generic over `T`, but
//! not for one that only ever holds `u8`); `bbqueue` gives a single
//! contiguous grant always (a real BipBuffer, see its docs) but pays
//! synchronization overhead -- atomic or critical-section -- on every
//! grant/commit/release unconditionally, since it has no equivalent of
//! `ringbuf`'s `LocalRb` for the case where producer and consumer are the
//! same thread, sequentially, which is this type's only use case.
//!
//! # Reading and writing
//!
//! Both sides of the ring may be split across the backing array's physical
//! end, so both are exposed as up to two contiguous spans -- the second is
//! empty whenever nothing wraps, which is the common case for a reasonably
//! sized ring against typical token sizes:
//!
//! - [`LoopBuf::spare_capacity_mut`] hands back writable space (e.g. for a
//!   socket `read()` to fill directly, with no intermediate copy), followed
//!   by [`LoopBuf::commit_write`] once bytes have actually been written.
//! - [`LoopBuf::readable_spans`] hands back unconsumed bytes for a parser to
//!   scan, followed by [`LoopBuf::advance`] once bytes have been consumed.
//!
//! A parser that needs a token's bytes to outlive what the ring will retain
//! copies out of these spans into its own scratch storage, exactly as it
//! already would copy out of any other buffer type's slice view -- this
//! type doesn't need to know about that case at all.

mod pool;

pub use pool::LoopBufPool;

/// A fixed-capacity ring buffer of bytes.
///
/// Capacity is set once at construction and never changes. Every byte that
/// moves through it is written and read exactly once, with no intermediate
/// copy. A caller that needs to hold more than one capacity's worth of a
/// single pending token copies out into its own buffer -- see the module
/// docs.
pub struct LoopBuf {
    /// Always kept at `data.capacity()` length via `set_len` -- this type
    /// manages which bytes are logically live itself (`read`/`len` below),
    /// not `Vec`'s own length. Safe to expose as `&[u8]`/`&mut [u8]` even
    /// where nothing meaningful has been written yet: `u8` has no validity
    /// invariant, so "uninitialized" bytes read back as some arbitrary (but
    /// well-defined) value rather than undefined behavior -- the same
    /// reasoning `hopf-core`'s `TcpConnection::read_from_socket` already
    /// relies on for its own linear buffer.
    data: Vec<u8>,
    /// Index of the first unread byte.
    read: usize,
    /// Number of unread bytes currently buffered.
    len: usize,
}

impl LoopBuf {
    /// Creates a ring with exactly `capacity` bytes of room (capacity is
    /// never zero; a zero request is rounded up to 1). Fixed for life --
    /// see the module docs.
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let mut data = Vec::with_capacity(capacity);
        // SAFETY: see the `data` field doc -- `u8` has no validity
        // invariant, so treating the spare capacity as initialized is
        // sound; nothing reads past `len` bytes from `read`.
        #[allow(clippy::uninit_vec)]
        unsafe {
            data.set_len(data.capacity());
        }
        Self { data, read: 0, len: 0 }
    }

    /// Total bytes this ring can hold. Fixed for life.
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Number of unread bytes currently buffered.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity()
    }

    /// Discards all buffered content, without touching the allocation --
    /// used when returning a buffer to a [`LoopBufPool`] for reuse.
    pub fn clear(&mut self) {
        self.read = 0;
        self.len = 0;
    }

    /// Index the next written byte would land at.
    fn write_index(&self) -> usize {
        let cap = self.capacity();
        if cap == 0 {
            0
        } else {
            (self.read + self.len) % cap
        }
    }

    /// Writable space, split at the backing array's physical end if the
    /// spare region wraps. The second slice is empty whenever it doesn't.
    pub fn spare_capacity_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        let cap = self.capacity();
        let spare = cap - self.len;
        if spare == 0 {
            return (&mut [], &mut []);
        }
        let w = self.write_index();
        if w + spare <= cap {
            (&mut self.data[w..w + spare], &mut [])
        } else {
            let (head, tail) = self.data.split_at_mut(w);
            let second_len = spare - tail.len();
            (tail, &mut head[..second_len])
        }
    }

    /// Marks `n` bytes, just written into the span(s) [`Self::spare_capacity_mut`]
    /// returned (first slice first), as now part of the readable content.
    ///
    /// Panics if `n` exceeds the spare capacity at the time of the call.
    pub fn commit_write(&mut self, n: usize) {
        assert!(n <= self.capacity() - self.len, "commit_write: n exceeds spare capacity");
        self.len += n;
    }

    /// Copies as much of `data` as currently fits into spare capacity.
    /// Returns how many bytes were actually copied -- fewer than
    /// `data.len()` only once the ring is full; since it never grows (see
    /// the module docs), the caller decides what to do with the rest
    /// (apply backpressure, or copy it into its own accumulator).
    pub fn extend_from_slice(&mut self, data: &[u8]) -> usize {
        let (a, b) = self.spare_capacity_mut();
        let n_a = a.len().min(data.len());
        a[..n_a].copy_from_slice(&data[..n_a]);
        let remaining = &data[n_a..];
        let n_b = b.len().min(remaining.len());
        b[..n_b].copy_from_slice(&remaining[..n_b]);
        let total = n_a + n_b;
        self.commit_write(total);
        total
    }

    /// Unconsumed bytes, split at the backing array's physical end if the
    /// live region wraps. The second slice is empty whenever it doesn't.
    pub fn readable_spans(&self) -> (&[u8], &[u8]) {
        if self.len == 0 {
            return (&[], &[]);
        }
        let cap = self.capacity();
        if self.read + self.len <= cap {
            (&self.data[self.read..self.read + self.len], &[])
        } else {
            let first_len = cap - self.read;
            let second_len = self.len - first_len;
            (&self.data[self.read..cap], &self.data[..second_len])
        }
    }

    /// Marks `n` bytes, from the front of what [`Self::readable_spans`]
    /// returned, as consumed -- freeing that space for reuse via
    /// wraparound, with no data movement.
    ///
    /// Panics if `n` exceeds the currently readable length.
    pub fn advance(&mut self, n: usize) {
        assert!(n <= self.len, "advance: n exceeds readable length");
        let cap = self.capacity();
        self.read = if cap == 0 { 0 } else { (self.read + n) % cap };
        self.len -= n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_read_round_trips() {
        let mut ring = LoopBuf::with_capacity(16);
        assert_eq!(ring.extend_from_slice(b"hello"), 5);
        let (a, b) = ring.readable_spans();
        assert_eq!(a, b"hello");
        assert_eq!(b, b"");
        ring.advance(5);
        assert!(ring.is_empty());
    }

    #[test]
    fn advance_then_extend_reuses_space_without_growing() {
        let mut ring = LoopBuf::with_capacity(8);
        assert_eq!(ring.extend_from_slice(b"abcdefgh"), 8);
        assert!(ring.is_full());
        ring.advance(4); // consume "abcd"
        // Spare capacity must be reusable even though it's not at index 0.
        assert_eq!(ring.extend_from_slice(b"ijkl"), 4);
        assert_eq!(ring.capacity(), 8, "must never grow");
        let (a, b) = ring.readable_spans();
        let mut joined = a.to_vec();
        joined.extend_from_slice(b);
        assert_eq!(joined, b"efghijkl");
    }

    #[test]
    fn write_wraps_across_the_physical_end() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"ABCDE"); // read=0, len=5
        ring.advance(5); // fully drained, but read now sits at index 5, not 0
        let (a, b) = ring.spare_capacity_mut();
        assert_eq!(a.len() + b.len(), 8);
        assert!(!b.is_empty(), "spare capacity should wrap across the physical end here");
        assert_eq!(ring.extend_from_slice(b"abcdefgh"), 8);
        let (ra, rb) = ring.readable_spans();
        let mut joined = ra.to_vec();
        joined.extend_from_slice(rb);
        assert_eq!(joined, b"abcdefgh");
    }

    #[test]
    fn read_wraps_across_the_physical_end() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdefgh");
        ring.advance(6);
        ring.extend_from_slice(b"ABCDEF"); // live region now wraps: "gh" + "ABCDEF"
        let (a, b) = ring.readable_spans();
        assert_eq!(a, b"gh");
        assert_eq!(b, b"ABCDEF");
    }

    #[test]
    fn extend_from_slice_stops_at_capacity_when_full() {
        let mut ring = LoopBuf::with_capacity(4);
        assert_eq!(ring.extend_from_slice(b"abcdef"), 4);
        assert_eq!(ring.extend_from_slice(b"z"), 0);
    }

    #[test]
    fn clear_resets_without_reallocating() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcd");
        ring.clear();
        assert!(ring.is_empty());
        assert_eq!(ring.capacity(), 8);
    }

    #[test]
    #[should_panic(expected = "commit_write: n exceeds spare capacity")]
    fn commit_write_past_spare_capacity_panics() {
        let mut ring = LoopBuf::with_capacity(4);
        ring.commit_write(5);
    }

    #[test]
    #[should_panic(expected = "advance: n exceeds readable length")]
    fn advance_past_readable_length_panics() {
        let mut ring = LoopBuf::with_capacity(4);
        ring.extend_from_slice(b"ab");
        ring.advance(3);
    }

    /// A token scanned across many arbitrarily small chunks, including
    /// 1-byte writes, must come back identical regardless of how many
    /// times the ring had to wrap internally -- the push-parsing-codecs
    /// chunk-split convention, applied to the buffer layer itself rather
    /// than to a specific parser.
    #[test]
    fn many_small_writes_and_reads_preserve_order_across_wraps() {
        let mut ring = LoopBuf::with_capacity(5);
        let input: Vec<u8> = (0u8..=200).collect();
        let mut produced = 0;
        let mut consumed = Vec::new();
        while consumed.len() < input.len() {
            if produced < input.len() {
                produced += ring.extend_from_slice(&input[produced..(produced + 1).min(input.len())]);
            }
            if !ring.is_empty() {
                let (a, _b) = ring.readable_spans();
                let take = 1.min(a.len());
                consumed.extend_from_slice(&a[..take]);
                ring.advance(take);
            }
        }
        assert_eq!(consumed, input);
    }
}
