use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::message::{self, Message};
use crate::segment_index::SegmentIndex;

pub struct SegmentConfig {
    pub max_size: u64,
    pub index_interval: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_size: 1024 * 1024,
            index_interval: 1,
        }
    }
}

pub struct Segment {
    dir: String,
    index: u64,
    writer: BufWriter<File>,
    size: u64,
    max_size: u64,
    segment_index: SegmentIndex,
    index_interval: usize,
    messages_since_index: usize,
}

impl Segment {
    pub fn new(dir: &str, index: u64, max_size: u64, index_interval: usize) -> io::Result<Self> {
        let path = Self::segment_path(dir, index);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let size = file.metadata()?.len();
        let segment_index = Self::load_index(dir, index);

        Ok(Self {
            dir: dir.to_string(),
            index,
            writer: BufWriter::new(file),
            size,
            max_size,
            segment_index,
            index_interval,
            messages_since_index: 0,
        })
    }

    pub fn open_existing(
        dir: &str,
        index: u64,
        max_size: u64,
        index_interval: usize,
    ) -> io::Result<Self> {
        let path = Self::segment_path(dir, index);
        if !Path::new(&path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment {} not found", index),
            ));
        }

        let file = OpenOptions::new().read(true).append(true).open(&path)?;

        let size = file.metadata()?.len();
        let segment_index = Self::load_index(dir, index);

        Ok(Self {
            dir: dir.to_string(),
            index,
            writer: BufWriter::new(file),
            size,
            max_size,
            segment_index,
            index_interval,
            messages_since_index: 0,
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

        self.messages_since_index += 1;
        if self.messages_since_index >= self.index_interval {
            self.segment_index.append(offset, self.size - msg_size);
            self.messages_since_index = 0;
        }

        Ok((offset, msg_size))
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.segment_index
            .save(Self::index_path(&self.dir, self.index))?;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn index_num(&self) -> u64 {
        self.index
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    pub fn path(&self) -> String {
        Self::segment_path(&self.dir, self.index)
    }

    pub fn find_position(&self, target_offset: u64) -> Option<u64> {
        self.segment_index.find_position(target_offset)
    }

    fn segment_path(dir: &str, index: u64) -> String {
        format!("{}/{:016x}.log", dir, index)
    }

    fn index_path(dir: &str, index: u64) -> String {
        format!("{}/{:016x}.idx", dir, index)
    }

    fn load_index(dir: &str, index: u64) -> SegmentIndex {
        let path = Self::index_path(dir, index);
        match SegmentIndex::load(&path) {
            Ok(idx) => idx,
            Err(_) => SegmentIndex::new(),
        }
    }
}

pub struct SegmentedLog {
    dir: String,
    current_segment: Segment,
    max_segment_size: u64,
    index_interval: usize,
}

impl SegmentedLog {
    pub fn new<P: AsRef<Path>>(dir: P, max_segment_size: u64) -> io::Result<Self> {
        Self::with_config(
            dir,
            SegmentConfig {
                max_size: max_segment_size,
                index_interval: 1,
            },
        )
    }

    pub fn with_config<P: AsRef<Path>>(dir: P, config: SegmentConfig) -> io::Result<Self> {
        let dir = dir.as_ref();
        let dir_str = dir.to_string_lossy().to_string();

        fs::create_dir_all(&dir)?;

        let index = Self::latest_segment_index(&dir_str)?;
        let segment = if index == 0 {
            Segment::new(&dir_str, 0, config.max_size, config.index_interval)?
        } else {
            Segment::open_existing(&dir_str, index, config.max_size, config.index_interval)?
        };

        Ok(Self {
            dir: dir_str,
            current_segment: segment,
            max_segment_size: config.max_size,
            index_interval: config.index_interval,
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
        let new_index = self.current_segment.index_num() + 1;
        self.current_segment = Segment::new(
            &self.dir,
            new_index,
            self.max_segment_size,
            self.index_interval,
        )?;
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

    pub fn find_segment_for_offset(&self, offset: u64) -> Option<(u64, String)> {
        let segment_index = self.current_segment.index_num();
        let _position = self.current_segment.find_position(offset)?;
        Some((
            segment_index,
            format!("{}/{:016x}.log", self.dir, segment_index),
        ))
    }
}
