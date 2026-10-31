// Copyright (C) 2026 Chris Burdess <dog@gnu.org>

//! The error every writer built on a [`crate::LoopBuf`] returns.

use std::fmt;
use std::io;

/// A write into a [`crate::LoopBuf`] that did not happen.
///
/// Writers built on the ring make each call atomic: it appends everything it
/// was asked to or nothing, and leaves the writer as it was. Which of the two
/// reasons it failed for decides what to do about it:
///
/// - [`Self::is_would_block`]: the ring has no room right now. Drain it and
///   repeat the same call.
/// - [`io::ErrorKind::InvalidInput`]: the call can never succeed. It needs
///   more room than the whole ring has, or it is not valid for the format
///   (a NaN in JSON, `]]>` in CDATA).
#[derive(Debug)]
pub struct WriteError {
    source: io::Error,
}

impl WriteError {
    pub fn new(source: io::Error) -> Self {
        Self { source }
    }

    /// The kind of the underlying I/O error.
    pub fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }

    /// True if the ring had no room for the write right now. Nothing was
    /// written and the writer's state is unchanged: drain the ring and repeat
    /// the same call.
    pub fn is_would_block(&self) -> bool {
        self.source.kind() == io::ErrorKind::WouldBlock
    }

    /// The underlying I/O error.
    pub fn into_io_error(self) -> io::Error {
        self.source
    }
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "write error: {}", self.source)
    }
}

impl std::error::Error for WriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl From<io::Error> for WriteError {
    fn from(source: io::Error) -> Self {
        Self::new(source)
    }
}

impl From<WriteError> for io::Error {
    fn from(e: WriteError) -> Self {
        e.source
    }
}

/// The result of a write into a [`crate::LoopBuf`].
pub type WriteResult<T> = Result<T, WriteError>;
