# loopbuf

A fixed-capacity, pooled ring buffer for feeding bytes to push parsers.

Every push parser in this workspace (`rjsonparser`, `rmimeparser`,
`tractrix`, `hopf-core`'s `ProtocolHandler` plumbing, `hopf-http`'s H1
codec, `hopf-mqtt`'s codec) solves the same problem -- accumulate bytes
from a producer, let a consumer scan them incrementally, keep whatever's
unconsumed for next time -- a different, ad hoc way. `loopbuf` is a single,
properly optimized solution: reclaiming space already consumed never
moves any data, because the ring reuses it via wraparound instead of
compacting.

```rust
use loopbuf::LoopBuf;

let mut ring = LoopBuf::with_capacity(4096);

// Producer side: fill spare capacity directly (e.g. from a socket read),
// then commit however many bytes actually landed.
let (a, _b) = ring.spare_capacity_mut();
let n = a.len().min(5);
a[..n].copy_from_slice(&b"hello"[..n]);
ring.commit_write(n);

// Consumer side: scan whatever's readable, then advance past what was
// consumed -- this is the no-copy, no-memmove step.
let (readable, _) = ring.readable_spans();
assert_eq!(readable, b"hello");
ring.advance(readable.len());
```

## Design

- **Fixed capacity, for life.** The ring never grows. A token bigger than
  one capacity's worth copies out into the parser's *own* independently
  sized accumulator (see `rmimeparser`'s `header_value_sink` for the
  existing precedent) -- feed-buffer size is a throughput knob, token size
  is a protocol-level limit the parser already enforces itself.
- **Two-span read/write.** Both sides may be split across the backing
  array's physical end; the second slice is empty whenever nothing wraps.
  `ringbuf` (crates.io) independently converged on the same shape, which
  is good validation, but isn't adopted here -- see the crate docs for why.
- **`LoopBufPool`** pools released buffers by power-of-two capacity,
  mirroring `hopf-core::BufferPool`'s own role.

## License

LGPL-3.0-or-later.
