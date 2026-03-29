# Logra

**Logra** is a minimal, high-performance append-only log engine written in Rust.

Inspired by systems like Kafka, Logra focuses on simplicity, speed, and a solid foundation for building message queues and event streaming platforms.

---

## Features

* Append-only log (sequential write)
* Binary encoding (length-prefixed)
* Offset-based storage
* Buffered writes for high throughput
* Flush policy (interval/size based)
* Offset recovery on startup
* Offset-based log reader (std I/O)
* Memory-mapped reader (mmap)
* Log segmentation (size-based rotation)
* Minimal and extensible design

---

## Example

```rust
use logra::{Log, LogConfig};

fn main() -> std::io::Result<()> {
    let mut log = Log::new("data.log")?;

    log.append(b"hello".to_vec())?;
    log.append(b"world".to_vec())?;

    Ok(())
}
```

With custom flush policy:

```rust
let config = LogConfig {
    flush_interval_ms: Some(1000),
    flush_size_bytes: Some(4096),
};
let mut log = Log::with_config("data.log", config)?;
```

Read messages:

```rust
use logra::{Log, LogReader};

let offset1;
{
    let mut log = Log::new("data.log")?;
    offset1 = log.append(b"hello".to_vec())?;
}
drop(log);

let mut reader = LogReader::new("data.log")?;
let msg = reader.read_at(offset1)?.unwrap();
```

Memory-mapped reading (faster for random access):

```rust
use logra::{Log, MmapReader};

let offset1;
{
    let mut log = Log::new("data.log")?;
    offset1 = log.append(b"hello".to_vec())?;
}

let reader = MmapReader::new("data.log")?;
let msg = reader.read_at(offset1 as usize)?;
```

Segmented log (automatic rotation by size):

```rust
use logra::SegmentedLog;

let mut log = SegmentedLog::new("logs/", 1024 * 1024)?; // 1MB segments
log.append(b"message".to_vec())?;
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
* [x] Flush policy (interval/size)
* [x] Offset recovery on startup
* [x] Log reader (offset-based)
* [x] Memory-mapped read (mmap)
* [x] Log segmentation

### Next

* [ ] Segment index (for fast seeking)

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
