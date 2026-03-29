use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::message::{self, Message};

pub struct SegmentConfig {
    pub max_size: u64,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_size: 1024 * 1024,
        } // 1MB default
    }
}

pub struct Segment {
    dir: String,
    index: u64,
    writer: BufWriter<File>,
    size: u64,
    max_size: u64,
}

impl Segment {
    pub fn new(dir: &str, index: u64, max_size: u64) -> io::Result<Self> {
        let path = Self::segment_path(dir, index);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let size = file.metadata()?.len();

        Ok(Self {
            dir: dir.to_string(),
            index,
            writer: BufWriter::new(file),
            size,
            max_size,
        })
    }

    pub fn open_existing(dir: &str, index: u64, max_size: u64) -> io::Result<Self> {
        let path = Self::segment_path(dir, index);
        if !Path::new(&path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment {} not found", index),
            ));
        }

        let file = OpenOptions::new().read(true).append(true).open(&path)?;

        let size = file.metadata()?.len();

        Ok(Self {
            dir: dir.to_string(),
            index,
            writer: BufWriter::new(file),
            size,
            max_size,
        })
    }

    pub fn append(&mut self, value: Vec<u8>, timestamp: u64) -> io::Result<(u64, u64)> {
        let offset = self.size;
        let msg = Message {
            offset,
            timestamp,
            value,
        };

        let encoded = message::encode(&msg);
        let msg_size = encoded.len() as u64;

        self.writer.write_all(&encoded)?;
        self.size += msg_size;

        Ok((offset, msg_size))
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    pub fn path(&self) -> String {
        Self::segment_path(&self.dir, self.index)
    }

    fn segment_path(dir: &str, index: u64) -> String {
        format!("{}/{:016x}.log", dir, index)
    }
}

pub struct SegmentedLog {
    dir: String,
    current_segment: Segment,
    max_segment_size: u64,
}

impl SegmentedLog {
    pub fn new<P: AsRef<Path>>(dir: P, max_segment_size: u64) -> io::Result<Self> {
        let dir = dir.as_ref();
        let dir_str = dir.to_string_lossy().to_string();

        fs::create_dir_all(&dir)?;

        let index = Self::latest_segment_index(&dir_str)?;
        let segment = if index == 0 {
            Segment::new(&dir_str, 0, max_segment_size)?
        } else {
            Segment::open_existing(&dir_str, index, max_segment_size)?
        };

        Ok(Self {
            dir: dir_str,
            current_segment: segment,
            max_segment_size,
        })
    }

    pub fn append(&mut self, value: Vec<u8>) -> io::Result<u64> {
        if self.current_segment.is_full() {
            self.current_segment.flush()?;
            self.rotate_segment()?;
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let (offset, _) = self.current_segment.append(value, timestamp)?;
        Ok(offset)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.current_segment.flush()
    }

    fn rotate_segment(&mut self) -> io::Result<()> {
        let new_index = self.current_segment.index() + 1;
        self.current_segment = Segment::new(&self.dir, new_index, self.max_segment_size)?;
        Ok(())
    }

    fn latest_segment_index(dir: &str) -> io::Result<u64> {
        let entries = fs::read_dir(dir)?;

        let mut max_index: u64 = 0;
        for entry in entries.flatten() {
            let name = entry.file_name();
            if let Some(name_str) = name.to_str() {
                if name_str.ends_with(".log") {
                    if let Ok(idx) = u64::from_str_radix(&name_str[..name_str.len() - 4], 16) {
                        max_index = max_index.max(idx);
                    }
                }
            }
        }

        Ok(max_index)
    }

    pub fn current_offset(&self) -> u64 {
        self.current_segment.size()
    }

    pub fn segment_index_for_offset(&self, _offset: u64) -> u64 {
        0
    }
}
