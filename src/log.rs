use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::message::{self, Message};

pub struct LogConfig {
    pub flush_interval_ms: Option<u64>,
    pub flush_size_bytes: Option<u64>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: None,
            flush_size_bytes: None,
        }
    }
}

pub struct Log {
    writer: BufWriter<File>,
    offset: u64,
    config: LogConfig,
    pending_bytes: u64,
    last_flush: Instant,
}

impl Log {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Self::with_config(path, LogConfig::default())
    }

    pub fn with_config<P: AsRef<Path>>(path: P, config: LogConfig) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        let offset = file.metadata()?.len() as u64;

        Ok(Self {
            writer: BufWriter::new(file),
            offset,
            config,
            pending_bytes: 0,
            last_flush: Instant::now(),
        })
    }

    pub fn append(&mut self, value: Vec<u8>) -> std::io::Result<u64> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let msg = Message {
            offset: self.offset,
            timestamp,
            value,
        };

        let encoded = message::encode(&msg);
        let msg_size = encoded.len() as u64;

        self.writer.write_all(&encoded)?;
        self.pending_bytes += msg_size;
        self.offset += msg_size;

        self.maybe_flush()?;

        Ok(msg.offset)
    }

    fn maybe_flush(&mut self) -> std::io::Result<()> {
        let mut should_flush = false;

        if let Some(interval_ms) = self.config.flush_interval_ms {
            if self.last_flush.elapsed() >= Duration::from_millis(interval_ms) {
                should_flush = true;
            }
        }

        if let Some(size_threshold) = self.config.flush_size_bytes {
            if self.pending_bytes >= size_threshold {
                should_flush = true;
            }
        }

        if should_flush {
            self.flush()?;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()?;
        self.pending_bytes = 0;
        self.last_flush = Instant::now();
        Ok(())
    }
}
