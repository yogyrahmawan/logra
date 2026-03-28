# Logra

**Logra** is a minimal, high-performance append-only log engine written in Rust.

Inspired by systems like Kafka, Logra focuses on simplicity, speed, and a solid foundation for building message queues and event streaming platforms.

---

## Features

* Append-only log (sequential write)
* Binary encoding (length-prefixed)
* Offset-based storage
* Buffered writes for high throughput
* Minimal and extensible design

---

## Example

```rust
use logra::Log;

fn main() -> std::io::Result<()> {
    let mut log = Log::new("data.log")?;

    log.append(b"hello".to_vec())?;
    log.append(b"world".to_vec())?;

    Ok(())
}
```

## Building

```bash
cargo build
cargo test
```

---

## Design Principles

Logra is built around a few core ideas:

* **Sequential I/O over random access**
* **Append-only, immutable log**
* **Offset as the source of truth**
* **Leverage OS page cache for performance**

---

## Log Format

Each message is stored as:

```
| length | offset | timestamp | value_len | value |
```

* `length` → total message size (for fast skipping)
* `offset` → byte position in log
* `timestamp` → message creation time
* `value` → payload

---

## Performance

Logra uses:

* Buffered writes (`BufWriter`)
* Sequential disk access
* OS page cache (no direct I/O)

This approach provides high throughput while keeping implementation simple.

---

## Roadmap

### Core

* [x] Append-only log
* [x] Binary message format
* [x] Basic tests

### Next

### Next

* [ ] Log reader (offset-based)
* [ ] Memory-mapped read (mmap)
* [ ] Log segmentation

### Future

* [ ] Consumer system
* [ ] Replication
* [ ] Network protocol (TCP)

---

## Benchmark (coming soon)

Planned comparisons:

* Page cache vs Direct I/O
* mmap vs read
* Throughput & latency

---

## Status

This project is experimental and under active development.

---

## License

MIT
