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
//! Because a token may straddle the wrap, [`LoopBuf::spans`] returns the same
//! readable bytes as a [`Spans`]: one logical sequence that can be indexed,
//! sliced and copied out of without caring where the seam is. A parser looks
//! at a token through it, decides how much to use, and then calls
//! [`LoopBuf::advance`].
//!
//! # `std::io`
//!
//! `LoopBuf` implements [`std::io::Write`] (append up to the spare capacity)
//! and [`std::io::Read`] (take from the front and advance), so a ring can sit
//! directly behind a `Writer`-style encoder or be drained with
//! `io::copy`. Neither ever blocks or grows: a write to a full ring fails with
//! [`io::ErrorKind::WouldBlock`] (drain it and try again), and reading an
//! empty ring returns `Ok(0)`, meaning nothing is buffered right now rather
//! than end of stream. `write_all` is all or nothing, so a `WouldBlock` leaves
//! the ring untouched and the same call can simply be repeated once it has
//! been drained.
//!
//! # Zero-copy windows
//!
//! A parser that wants to hand a *sub-range* of its input to its own
//! consumer (an HTTP/2 frame payload, say) without copying calls
//! [`LoopBuf::window`]. That narrows the buffer to the requested range,
//! returning a guard that derefs to the same `LoopBuf`; dropping the guard
//! restores the exact prior read position and length. It is the ring
//! equivalent of Gumdrop's save `position()`/`limit()`, narrow, call the
//! consumer, restore idiom, with no slice object and no copy. The consumer
//! reads the window with the ordinary [`LoopBuf::readable_spans`] and
//! [`LoopBuf::advance`]; whatever it consumes is *not* carried over, so the
//! parser decides itself how far to advance afterwards.
//!
//! A parser that needs a token's bytes to outlive what the ring will retain
//! copies out of these spans into its own scratch storage, exactly as it
//! already would copy out of any other buffer type's slice view -- this
//! type doesn't need to know about that case at all.

mod error;
mod pool;
mod spans;

use std::io;

pub use error::{WriteError, WriteResult};
pub use pool::LoopBufPool;
pub use spans::Spans;

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
    /// Number of live [`Window`] guards. While nonzero the logical end of
    /// the readable region is not the end of the buffered data, so writing
    /// is disabled.
    windows: usize,
}

impl LoopBuf {
    /// Creates a ring with exactly `capacity` bytes of room (capacity is
    /// never zero; a zero request is rounded up to 1). Fixed for life --
    /// see the module docs.
    #[inline]
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
        Self {
            data,
            read: 0,
            len: 0,
            windows: 0,
        }
    }

    /// Total bytes this ring can hold. Fixed for life.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Number of unread bytes currently buffered.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len == self.capacity()
    }

    /// Discards all buffered content, without touching the allocation --
    /// used when returning a buffer to a [`LoopBufPool`] for reuse.
    #[inline]
    pub fn clear(&mut self) {
        self.read = 0;
        self.len = 0;
    }

    /// `index` wrapped into the backing array. Every index passed is below
    /// twice the capacity (a position plus an amount that is at most the
    /// capacity), so a subtraction does what `%` would, without the division.
    #[inline]
    fn wrap(&self, index: usize) -> usize {
        let cap = self.capacity();
        if index >= cap {
            index - cap
        } else {
            index
        }
    }

    /// Index the next written byte would land at.
    fn write_index(&self) -> usize {
        let cap = self.capacity();
        if cap == 0 {
            0
        } else {
            self.wrap(self.read + self.len)
        }
    }

    /// Writable space, split at the backing array's physical end if the
    /// spare region wraps. The second slice is empty whenever it doesn't.
    ///
    /// Always empty while a [`Window`] is live: the window's end is not the
    /// end of the buffered data, so writing there would clobber it.
    #[inline]
    pub fn spare_capacity_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        let cap = self.capacity();
        let spare = cap - self.len;
        if spare == 0 || self.windows > 0 {
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
    /// Panics if `n` exceeds the spare capacity at the time of the call
    /// (which is zero while a [`Window`] is live).
    #[inline]
    pub fn commit_write(&mut self, n: usize) {
        assert!(
            self.windows == 0 || n == 0,
            "commit_write: buffer is windowed"
        );
        assert!(
            n <= self.capacity() - self.len,
            "commit_write: n exceeds spare capacity"
        );
        self.len += n;
    }

    /// Room to write right now: the spare capacity, or zero while a [`Window`]
    /// is live. This is what `spare_capacity_mut` would hand back in total.
    #[inline]
    pub fn spare_len(&self) -> usize {
        if self.windows > 0 {
            0
        } else {
            self.capacity() - self.len
        }
    }

    /// Discards the most recent writes, keeping only the first `len` readable
    /// bytes. The undo for a sequence of writes that turned out not to fit as
    /// a whole: note [`Self::len`] before starting, and truncate back to it.
    ///
    /// Panics if `len` is more than the readable length, or while a
    /// [`Window`] is live (the window's saved state would no longer match).
    pub fn truncate(&mut self, len: usize) {
        assert!(self.windows == 0, "truncate: buffer is windowed");
        assert!(len <= self.len, "truncate: len exceeds readable length");
        self.len = len;
    }

    /// Overwrites already-written bytes in place, starting `offset` bytes past
    /// the read position, without changing the length. For filling in what
    /// could not be known when it was written: a length prefix, a frame
    /// header. The bytes must not have been consumed in the meantime.
    ///
    /// Panics if the range runs past the readable bytes.
    pub fn overwrite(&mut self, offset: usize, data: &[u8]) {
        assert!(
            offset
                .checked_add(data.len())
                .is_some_and(|end| end <= self.len),
            "overwrite: range exceeds readable length"
        );
        if data.is_empty() {
            return;
        }
        let cap = self.capacity();
        let start = self.wrap(self.read + offset);
        let head = data.len().min(cap - start);
        self.data[start..start + head].copy_from_slice(&data[..head]);
        self.data[..data.len() - head].copy_from_slice(&data[head..]);
    }

    /// Copies as much of `data` as currently fits into spare capacity.
    /// Returns how many bytes were actually copied -- fewer than
    /// `data.len()` only once the ring is full; since it never grows (see
    /// the module docs), the caller decides what to do with the rest
    /// (apply backpressure, or copy it into its own accumulator).
    #[inline]
    pub fn extend_from_slice(&mut self, data: &[u8]) -> usize {
        let n = data.len().min(self.spare_len());
        if n == 0 {
            return 0;
        }
        let start = self.write_index();
        let first = n.min(self.capacity() - start);
        self.data[start..start + first].copy_from_slice(&data[..first]);
        if first < n {
            // wrapped past the physical end
            self.data[..n - first].copy_from_slice(&data[first..n]);
        }
        self.len += n;
        n
    }

    /// Unconsumed bytes, split at the backing array's physical end if the
    /// live region wraps. The second slice is empty whenever it doesn't.
    #[inline]
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

    /// The unconsumed bytes as one logical sequence that can be indexed and
    /// sliced across the wrap. See [`Spans`].
    #[inline]
    pub fn spans(&self) -> Spans<'_> {
        let (first, second) = self.readable_spans();
        Spans::new(first, second)
    }

    /// Marks `n` bytes, from the front of what [`Self::readable_spans`]
    /// returned, as consumed -- freeing that space for reuse via
    /// wraparound, with no data movement.
    ///
    /// Panics if `n` exceeds the currently readable length.
    #[inline]
    pub fn advance(&mut self, n: usize) {
        assert!(n <= self.len, "advance: n exceeds readable length");
        self.read = self.wrap(self.read + n);
        self.len -= n;
    }

    /// Narrows the readable region to `len` bytes starting `offset` bytes
    /// past the current read position, for presenting that sub-range to a
    /// consumer with no copy and no slice object.
    ///
    /// The returned [`Window`] derefs to this same `LoopBuf`, so the
    /// consumer just takes `&mut LoopBuf` and reads it with
    /// [`Self::readable_spans`] / [`Self::advance`] as usual (the range may
    /// wrap the physical end, hence two spans). Dropping the guard restores
    /// the exact read position and length from before, discarding whatever
    /// the consumer advanced past; check [`Self::len`] on the guard first if
    /// the parser needs to know how much was left unconsumed. While the
    /// window is live the buffer cannot be written to. Windows nest.
    ///
    /// Panics if `offset + len` exceeds the currently readable length.
    #[inline]
    pub fn window(&mut self, offset: usize, len: usize) -> Window<'_> {
        assert!(
            offset.checked_add(len).is_some_and(|end| end <= self.len),
            "window: range exceeds readable length"
        );
        let saved_read = self.read;
        let saved_len = self.len;
        self.read = self.wrap(self.read + offset);
        self.len = len;
        self.windows += 1;
        Window {
            buf: self,
            saved_read,
            saved_len,
        }
    }
}

/// Appends to the ring, up to its spare capacity.
///
/// [`write`](io::Write::write) is a short write when the ring is nearly full,
/// as usual. A write to a ring with no spare capacity at all returns
/// [`io::ErrorKind::WouldBlock`]: it is full right now, not closed, so drain
/// it and try again.
///
/// [`write_all`](io::Write::write_all) is **all or nothing**. If the whole
/// buffer does not fit it writes none of it and returns `WouldBlock`, so the
/// ring is exactly as it was and the caller can drain it and repeat the same
/// call. (The default `write_all` would write the part that fits and then
/// fail, leaving the caller unable to tell how much went out.) A buffer larger
/// than the ring's whole capacity can never fit, however much is drained, so
/// that is an [`io::ErrorKind::InvalidInput`] error instead.
impl io::Write for LoopBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        match self.extend_from_slice(buf) {
            0 => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "LoopBuf is full: read from it and try again",
            )),
            n => Ok(n),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        if buf.len() > self.capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write is larger than the LoopBuf's whole capacity",
            ));
        }
        // a live window leaves no room to write: see `spare_capacity_mut`
        let spare = if self.windows > 0 {
            0
        } else {
            self.capacity() - self.len
        };
        if buf.len() > spare {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "LoopBuf has no room for the whole write: read from it and try again",
            ));
        }
        self.extend_from_slice(buf);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Takes bytes from the front of the ring and advances past them, so the
/// space is writable again.
///
/// `Ok(0)` on an empty ring means nothing is buffered right now, not that a
/// stream has ended, which lets `io::copy` drain exactly what is there.
impl io::Read for LoopBuf {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.spans().copy_to(0, out);
        self.advance(n);
        Ok(n)
    }
}

/// Guard returned by [`LoopBuf::window`]. Derefs to the narrowed
/// [`LoopBuf`]; restores the original read position and length on drop.
pub struct Window<'a> {
    buf: &'a mut LoopBuf,
    saved_read: usize,
    saved_len: usize,
}

impl std::ops::Deref for Window<'_> {
    type Target = LoopBuf;
    fn deref(&self) -> &LoopBuf {
        self.buf
    }
}

impl std::ops::DerefMut for Window<'_> {
    fn deref_mut(&mut self) -> &mut LoopBuf {
        self.buf
    }
}

impl Drop for Window<'_> {
    fn drop(&mut self) {
        self.buf.read = self.saved_read;
        self.buf.len = self.saved_len;
        self.buf.windows -= 1;
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
        assert!(
            !b.is_empty(),
            "spare capacity should wrap across the physical end here"
        );
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
                produced +=
                    ring.extend_from_slice(&input[produced..(produced + 1).min(input.len())]);
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

    fn joined(ring: &LoopBuf) -> Vec<u8> {
        let (a, b) = ring.readable_spans();
        [a, b].concat()
    }

    #[test]
    fn window_narrows_then_restores_exactly() {
        let mut ring = LoopBuf::with_capacity(16);
        ring.extend_from_slice(b"HEADERpayloadTRAILER");
        let before = joined(&ring);
        {
            let mut w = ring.window(6, 7);
            assert_eq!(joined(&w), b"payload");
            assert_eq!(w.len(), 7);
            w.advance(3); // consumer eats part of it
            assert_eq!(joined(&w), b"load");
        }
        assert_eq!(joined(&ring), before, "consumer progress must not leak out");
    }

    #[test]
    fn window_across_the_physical_end_is_two_spans() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdefgh");
        ring.advance(6);
        ring.extend_from_slice(b"ABCDEF"); // "gh" + "ABCDEF" wraps
        let before = joined(&ring);
        {
            let w = ring.window(1, 4);
            let (a, b) = w.readable_spans();
            assert_eq!(a, b"h");
            assert_eq!(b, b"ABC");
        }
        assert_eq!(joined(&ring), before);
    }

    #[test]
    fn window_blocks_writes_and_unblocks_after() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcd");
        {
            let mut w = ring.window(0, 2);
            let (a, b) = w.spare_capacity_mut();
            assert!(a.is_empty() && b.is_empty());
            assert_eq!(w.extend_from_slice(b"zz"), 0);
        }
        assert_eq!(ring.extend_from_slice(b"ef"), 2);
        assert_eq!(joined(&ring), b"abcdef");
    }

    #[test]
    fn windows_nest() {
        let mut ring = LoopBuf::with_capacity(16);
        ring.extend_from_slice(b"0123456789");
        {
            let mut outer = ring.window(2, 6);
            {
                let inner = outer.window(1, 3);
                assert_eq!(joined(&inner), b"345");
            }
            assert_eq!(joined(&outer), b"234567");
        }
        assert_eq!(joined(&ring), b"0123456789");
    }

    #[test]
    fn window_restores_on_panic() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcd");
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut w = ring.window(1, 2);
            w.advance(2);
            panic!("consumer blew up");
        }));
        assert!(r.is_err());
        assert_eq!(joined(&ring), b"abcd");
        assert_eq!(ring.extend_from_slice(b"ef"), 2, "writes re-enabled");
    }

    #[test]
    #[should_panic(expected = "window: range exceeds readable length")]
    fn window_past_readable_length_panics() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abc");
        let _ = ring.window(2, 2);
    }

    #[test]
    #[should_panic(expected = "advance: n exceeds readable length")]
    fn consumer_cannot_advance_past_window() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdef");
        let mut w = ring.window(0, 2);
        w.advance(3);
    }

    #[test]
    fn spans_indexes_a_wrapped_ring_as_one_sequence() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdefgh");
        ring.advance(6);
        ring.extend_from_slice(b"ABCDEF"); // "gh" + "ABCDEF" wraps
        let spans = ring.spans();
        assert_eq!(spans.len(), 8);
        assert_eq!(spans.as_slices(), (&b"gh"[..], &b"ABCDEF"[..]));
        assert_eq!(spans.to_vec(), b"ghABCDEF");
        assert_eq!(spans[1], b'h');
        assert_eq!(spans[2], b'A');
        assert_eq!(spans.contiguous(1..3), None);
        assert_eq!(spans.contiguous(2..8), Some(&b"ABCDEF"[..]));
    }

    #[test]
    fn spans_of_a_window_cover_only_the_window() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdefgh");
        ring.advance(6);
        ring.extend_from_slice(b"ABCDEF");
        let w = ring.window(1, 4);
        assert_eq!(w.spans().to_vec(), b"hABC");
    }

    #[test]
    fn write_appends_and_reports_a_short_write_when_nearly_full() {
        use std::io::Write;
        let mut ring = LoopBuf::with_capacity(8);
        assert_eq!(ring.write(b"hello").unwrap(), 5);
        assert_eq!(ring.write(b"world").unwrap(), 3, "only 3 bytes of room");
        assert!(ring.is_full());
        assert_eq!(ring.spans().to_vec(), b"hellowor");
        assert_eq!(ring.write(b"").unwrap(), 0);
        ring.flush().unwrap();
    }

    #[test]
    fn writing_to_a_full_ring_would_block_rather_than_claim_it_is_closed() {
        use std::io::Write;
        let mut ring = LoopBuf::with_capacity(4);
        ring.write_all(b"abcd").unwrap();
        let err = ring.write(b"e").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        ring.advance(2);
        assert_eq!(ring.write(b"ef").unwrap(), 2, "draining makes room again");
        let err = ring.write_all(b"xyz").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn read_drains_across_the_wrap_and_frees_the_space() {
        use std::io::Read;
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdefgh");
        ring.advance(6);
        ring.extend_from_slice(b"ABCDEF"); // wraps
        let mut out = [0u8; 5];
        assert_eq!(ring.read(&mut out).unwrap(), 5);
        assert_eq!(&out, b"ghABC");
        assert_eq!(ring.len(), 3);
        let mut rest = Vec::new();
        assert_eq!(ring.read_to_end(&mut rest).unwrap(), 3);
        assert_eq!(rest, b"DEF");
        assert!(ring.is_empty());
        assert_eq!(ring.read(&mut out).unwrap(), 0, "empty: nothing buffered");
    }

    #[test]
    fn io_copy_moves_a_ring_into_a_writer_and_back() {
        use std::io::Write;
        let mut ring = LoopBuf::with_capacity(16);
        ring.write_all(b"through the ring").unwrap();
        let mut sink = Vec::new();
        assert_eq!(io::copy(&mut ring, &mut sink).unwrap(), 16);
        assert_eq!(sink, b"through the ring");
        assert!(ring.is_empty());
    }

    #[test]
    fn write_all_is_all_or_nothing() {
        use std::io::Write;
        let mut ring = LoopBuf::with_capacity(8);
        ring.write_all(b"abcdef").unwrap();
        // only 2 bytes of room: nothing at all may be written, so that the
        // caller can drain the ring and retry the same call
        let err = ring.write_all(b"ghijk").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(ring.spans().to_vec(), b"abcdef", "the ring is untouched");

        ring.advance(3);
        ring.write_all(b"ghijk").unwrap();
        assert_eq!(ring.spans().to_vec(), b"defghijk");
    }

    #[test]
    fn write_all_larger_than_the_whole_ring_can_never_succeed() {
        use std::io::Write;
        let mut ring = LoopBuf::with_capacity(4);
        ring.write_all(b"ab").unwrap();
        // WouldBlock would invite a retry that can never work
        let err = ring.write_all(b"cdefg").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(ring.spans().to_vec(), b"ab");
    }

    #[test]
    fn write_all_of_nothing_succeeds_even_when_full() {
        use std::io::Write;
        let mut ring = LoopBuf::with_capacity(2);
        ring.write_all(b"ab").unwrap();
        ring.write_all(b"").unwrap();
        assert_eq!(ring.len(), 2);
    }

    #[test]
    fn write_all_to_a_windowed_ring_would_block_and_writes_nothing() {
        use std::io::Write;
        let mut ring = LoopBuf::with_capacity(8);
        ring.write_all(b"abcd").unwrap();
        {
            let mut w = ring.window(0, 2);
            let err = w.write_all(b"x").unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        }
        assert_eq!(ring.spans().to_vec(), b"abcd");
    }

    #[test]
    fn spare_len_is_the_room_to_write_right_now() {
        let mut ring = LoopBuf::with_capacity(8);
        assert_eq!(ring.spare_len(), 8);
        ring.extend_from_slice(b"abcde");
        assert_eq!(ring.spare_len(), 3);
        {
            let w = ring.window(0, 2);
            assert_eq!(w.spare_len(), 0, "a window leaves no room to write");
        }
        assert_eq!(ring.spare_len(), 3);
    }

    #[test]
    fn truncate_undoes_the_most_recent_writes_even_across_the_wrap() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdefgh");
        ring.advance(6);
        ring.extend_from_slice(b"ABCDEF"); // "gh" + "ABCDEF" wraps
        ring.truncate(4);
        assert_eq!(ring.spans().to_vec(), b"ghAB");
        assert_eq!(ring.spare_len(), 4);
        ring.extend_from_slice(b"xy");
        assert_eq!(ring.spans().to_vec(), b"ghABxy");
        ring.truncate(0);
        assert!(ring.is_empty());
    }

    #[test]
    #[should_panic(expected = "truncate: len exceeds readable length")]
    fn truncate_cannot_grow() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"ab");
        ring.truncate(3);
    }

    #[test]
    #[should_panic(expected = "truncate: buffer is windowed")]
    fn truncate_is_refused_while_windowed() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcd");
        let mut w = ring.window(0, 2);
        w.truncate(1);
    }

    #[test]
    fn overwrite_patches_bytes_already_written_even_across_the_wrap() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdefgh");
        ring.advance(6);
        ring.extend_from_slice(b"ABCDEF"); // "gh" + "ABCDEF": the seam is after "gh"
        ring.overwrite(1, b"123"); // straddles the seam
        assert_eq!(ring.spans().to_vec(), b"g123CDEF");
        ring.overwrite(0, b"X");
        ring.overwrite(7, b"Y");
        assert_eq!(ring.spans().to_vec(), b"X123CDEY");
        ring.overwrite(3, b""); // nothing to do
        assert_eq!(ring.len(), 8, "overwriting never changes the length");
    }

    #[test]
    fn overwrite_is_relative_to_a_window() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abcdef");
        {
            let mut w = ring.window(2, 3);
            w.overwrite(1, b"Z");
        }
        assert_eq!(ring.spans().to_vec(), b"abcZef");
    }

    #[test]
    #[should_panic(expected = "overwrite: range exceeds readable length")]
    fn overwrite_cannot_reach_past_what_is_written() {
        let mut ring = LoopBuf::with_capacity(8);
        ring.extend_from_slice(b"abc");
        ring.overwrite(2, b"xy");
    }
}
