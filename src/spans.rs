// Copyright (C) 2026 Chris Burdess <dog@gnu.org>

//! A read-only view of a ring's readable bytes as one logical sequence.

use std::ops::{Index, Range};

/// The readable bytes of a [`crate::LoopBuf`] as one logical sequence of
/// bytes, though they may be held in two slices.
///
/// A ring's readable region wraps the end of its backing array, so it is up to
/// two contiguous slices, the second empty whenever nothing wraps. A parser
/// that needs to look at a token that may straddle the wrap (a varint, a
/// number, an escape sequence) wants to index the content as a whole without
/// moving it, and that is what this is: [`LoopBuf::spans`] borrows the ring
/// and indexes across the join.
///
/// The common, fast case is still one slice: scan with [`Self::span_from`] and
/// fall back to byte-at-a-time indexing only where a token crosses the seam.
///
/// `Spans` is `Copy`, so a parser can keep one while it decides how much of it
/// to consume, then release the borrow and call [`crate::LoopBuf::advance`].
///
/// [`LoopBuf::spans`]: crate::LoopBuf::spans
#[derive(Clone, Copy, Debug, Default)]
pub struct Spans<'a> {
    first: &'a [u8],
    second: &'a [u8],
}

impl<'a> Spans<'a> {
    /// Joins two slices into one logical sequence. Normally obtained from
    /// [`crate::LoopBuf::spans`] instead.
    #[inline]
    pub fn new(first: &'a [u8], second: &'a [u8]) -> Self {
        Self { first, second }
    }

    /// The two underlying slices, in order. The second is empty whenever the
    /// content does not wrap.
    #[inline]
    pub fn as_slices(&self) -> (&'a [u8], &'a [u8]) {
        (self.first, self.second)
    }

    /// Total number of bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.first.len() + self.second.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.first.is_empty() && self.second.is_empty()
    }

    /// The byte at `index`, or `None` if it is past the end.
    #[inline]
    pub fn get(&self, index: usize) -> Option<u8> {
        if index < self.first.len() {
            Some(self.first[index])
        } else {
            self.second.get(index - self.first.len()).copied()
        }
    }

    /// The contiguous bytes from `index` to the end of whichever slice holds
    /// it, so a scan can run over a plain slice and only cross the seam once.
    /// Empty when `index` is the end.
    ///
    /// Panics if `index` is past the end (the slice below does it, so there is
    /// no separate check on this hot path).
    #[inline]
    pub fn span_from(&self, index: usize) -> &'a [u8] {
        if index < self.first.len() {
            &self.first[index..]
        } else {
            &self.second[index - self.first.len()..]
        }
    }

    /// `range` as one slice, or `None` if it straddles the seam and so is not
    /// contiguous in memory.
    ///
    /// Panics if `range` is reversed or runs past the end.
    #[inline]
    pub fn contiguous(&self, range: Range<usize>) -> Option<&'a [u8]> {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "contiguous: range out of bounds"
        );
        let split = self.first.len();
        if range.end <= split {
            Some(&self.first[range])
        } else if range.start >= split {
            Some(&self.second[range.start - split..range.end - split])
        } else {
            None
        }
    }

    /// The same bytes without the first `count`.
    ///
    /// Panics if `count` is more than [`Self::len`].
    #[inline]
    pub fn skip(&self, count: usize) -> Spans<'a> {
        assert!(count <= self.len(), "skip: count past the end");
        if count >= self.first.len() {
            Spans {
                first: &self.second[count - self.first.len()..],
                second: &[],
            }
        } else {
            Spans {
                first: &self.first[count..],
                second: self.second,
            }
        }
    }

    /// Copies up to `dst.len()` bytes starting at `offset` into `dst`, however
    /// they are split, and returns how many were copied (fewer than
    /// `dst.len()` only if the end was reached). For reading a fixed-size
    /// value that may straddle the seam.
    ///
    /// Panics if `offset` is past the end.
    #[inline]
    pub fn copy_to(&self, offset: usize, dst: &mut [u8]) -> usize {
        assert!(offset <= self.len(), "copy_to: offset past the end");
        let mut copied = 0;
        if offset < self.first.len() {
            let src = &self.first[offset..];
            copied = src.len().min(dst.len());
            dst[..copied].copy_from_slice(&src[..copied]);
        }
        if copied < dst.len() {
            // whatever is left of dst comes from the second slice, starting
            // at its beginning unless the offset was already past the seam
            let src = &self.second[offset.saturating_sub(self.first.len())..];
            let n = src.len().min(dst.len() - copied);
            dst[copied..copied + n].copy_from_slice(&src[..n]);
            copied += n;
        }
        copied
    }

    /// The bytes as a single `Vec`, copied.
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.len());
        bytes.extend_from_slice(self.first);
        bytes.extend_from_slice(self.second);
        bytes
    }
}

impl Index<usize> for Spans<'_> {
    type Output = u8;

    /// The byte at `index`. Panics if it is past the end, like slice indexing.
    #[inline]
    fn index(&self, index: usize) -> &u8 {
        if index < self.first.len() {
            &self.first[index]
        } else {
            &self.second[index - self.first.len()]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapped() -> Spans<'static> {
        Spans::new(b"abc", b"defg")
    }

    #[test]
    fn indexes_across_the_seam() {
        let s = wrapped();
        assert_eq!(s.len(), 7);
        assert!(!s.is_empty());
        let all: Vec<u8> = (0..7).map(|i| s[i]).collect();
        assert_eq!(all, b"abcdefg");
        assert_eq!(s.get(2), Some(b'c'));
        assert_eq!(s.get(3), Some(b'd'));
        assert_eq!(s.get(7), None);
        assert!(Spans::default().is_empty());
    }

    #[test]
    #[should_panic]
    fn indexing_past_the_end_panics() {
        let _ = wrapped()[7];
    }

    #[test]
    fn span_from_runs_to_the_end_of_the_slice_that_holds_the_index() {
        let s = wrapped();
        assert_eq!(s.span_from(0), b"abc");
        assert_eq!(s.span_from(2), b"c");
        assert_eq!(s.span_from(3), b"defg");
        assert_eq!(s.span_from(5), b"fg");
        assert_eq!(s.span_from(7), b"");
    }

    #[test]
    fn contiguous_is_none_only_when_a_range_straddles_the_seam() {
        let s = wrapped();
        assert_eq!(s.contiguous(0..3), Some(&b"abc"[..]));
        assert_eq!(s.contiguous(3..7), Some(&b"defg"[..]));
        assert_eq!(s.contiguous(1..1), Some(&b""[..]));
        assert_eq!(s.contiguous(2..4), None);
        assert_eq!(s.contiguous(0..7), None);
    }

    #[test]
    fn skip_drops_a_prefix_across_the_seam() {
        let s = wrapped();
        assert_eq!(s.skip(0).to_vec(), b"abcdefg");
        assert_eq!(s.skip(2).to_vec(), b"cdefg");
        assert_eq!(s.skip(2).as_slices(), (&b"c"[..], &b"defg"[..]));
        assert_eq!(s.skip(3).to_vec(), b"defg");
        assert_eq!(s.skip(5).to_vec(), b"fg");
        assert_eq!(s.skip(7).to_vec(), b"");
        assert_eq!(s.skip(2).skip(2).to_vec(), b"efg");
    }

    #[test]
    fn copy_to_reads_a_fixed_size_value_wherever_it_sits() {
        let s = wrapped();
        for offset in 0..=4 {
            let mut dst = [0u8; 3];
            assert_eq!(s.copy_to(offset, &mut dst), 3, "offset {offset}");
            assert_eq!(&dst, &b"abcdefg"[offset..offset + 3], "offset {offset}");
        }
        // clipped by the end
        let mut dst = [0u8; 4];
        assert_eq!(s.copy_to(5, &mut dst), 2);
        assert_eq!(&dst[..2], b"fg");
        assert_eq!(s.copy_to(7, &mut dst), 0);
        // an offset inside the second slice
        let mut dst = [0u8; 2];
        assert_eq!(s.copy_to(4, &mut dst), 2);
        assert_eq!(&dst, b"ef");
    }

    #[test]
    fn a_view_with_nothing_in_the_second_slice_behaves_like_a_slice() {
        let s = Spans::new(b"hello", b"");
        assert_eq!(s.len(), 5);
        assert_eq!(s.contiguous(1..4), Some(&b"ell"[..]));
        assert_eq!(s.span_from(5), b"");
        assert_eq!(s.skip(5).len(), 0);
        let mut dst = [0u8; 8];
        assert_eq!(s.copy_to(1, &mut dst), 4);
        assert_eq!(&dst[..4], b"ello");
    }
}
