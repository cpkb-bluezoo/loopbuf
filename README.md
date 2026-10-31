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

## Zero-copy windows

To present a sub-range of the buffer to a downstream consumer (e.g. an
HTTP/2 frame payload) without copying, narrow the buffer with `window`.
The guard derefs to the same `LoopBuf` and restores the previous read
position and length when dropped -- no slice object, no copy:

```rust
let mut ring = LoopBuf::with_capacity(4096);
ring.extend_from_slice(b"HDRpayload");
ring.advance(3);

{
    let mut w = ring.window(0, 7);      // consumer sees only "payload"
    let (a, b) = w.readable_spans();    // `b` is non-empty if it wraps
    assert_eq!([a, b].concat(), b"payload");
    w.advance(7);                       // consumer's progress is local...
}
assert_eq!(ring.len(), 7);              // ...the parser advances itself
```

While a window is live the buffer is read-only (`spare_capacity_mut`
is empty). Windows nest.

## One logical sequence: `Spans`

A token may straddle the ring's wrap, so the readable region is up to two
slices. `spans()` borrows it as a `Spans`, which indexes, slices and copies
across the join without moving anything:

```rust
let spans = ring.spans();
let first = spans[0];                         // index like a slice
let run = spans.span_from(0);                 // scan the contiguous part fast
let word = spans.contiguous(4..8);            // None only if it straddles
let mut tag = [0u8; 4];
spans.copy_to(10, &mut tag);                  // a fixed-size value, wherever it sits
let rest = spans.skip(8);                     // a view without the first 8 bytes
```

## `std::io`

`LoopBuf` implements `io::Write` (append up to the spare capacity) and
`io::Read` (take from the front and advance), so an encoder can write straight
into the ring a parser reads, and a ring can be drained with `io::copy`.
Neither blocks or grows: writing to a full ring is `ErrorKind::WouldBlock`
(drain it and retry), and reading an empty one is `Ok(0)`, meaning nothing is
buffered right now rather than end of stream.

## Writing a whole token or none of it

An encoder writing into a ring that a consumer drains has to make each token
atomic: it either all goes in or none of it does, so the caller can drain and
repeat the same call. The pieces for that:

- `spare_len()` is the room to write right now (zero while a window is live).
- `write_all` (the `io::Write` impl) is all or nothing, and `WouldBlock` leaves
  the ring untouched.
- `WriteError` / `WriteResult` are the error every writer built on the ring
  returns, so callers handle them the same way whichever library wrote:
  `is_would_block()` means drain and repeat the call; `kind()` of
  `InvalidInput` means it can never succeed.
- `truncate(len)` takes back everything written since the length was `len`, to
  undo a sequence of writes that turned out not to fit as a whole.
- `overwrite(offset, bytes)` fills in bytes already written, in place, to
  back-patch what could not be known at the time: a length prefix, a frame
  header.

```rust
let start = ring.len();
ring.extend_from_slice(&[tag, 0, 0, 0, 0]);       // a length, to fill in later
if write_content(&mut ring).is_err() {
    ring.truncate(start);                         // as if it never happened
} else {
    let length = (ring.len() - start - 5) as u32;
    ring.overwrite(start + 1, &length.to_le_bytes());
}
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
