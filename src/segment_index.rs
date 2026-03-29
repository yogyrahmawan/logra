use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;

pub struct SegmentIndex {
    entries: Vec<IndexEntry>,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    base_offset: u64,
    position: u64,
}

impl SegmentIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = io::BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut base_buf = [0u8; 8];
            match reader.read_exact(&mut base_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let mut pos_buf = [0u8; 8];
            reader.read_exact(&mut pos_buf)?;

            let base_offset = u64::from_be_bytes(base_buf);
            let position = u64::from_be_bytes(pos_buf);

            entries.push(IndexEntry {
                base_offset,
                position,
            });
        }

        Ok(Self { entries })
    }

    pub fn append(&mut self, base_offset: u64, position: u64) {
        self.entries.push(IndexEntry {
            base_offset,
            position,
        });
    }

    pub fn find_position(&self, target_offset: u64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        let mut best_entry: Option<&IndexEntry> = None;

        for entry in &self.entries {
            if entry.base_offset <= target_offset {
                best_entry = Some(entry);
            } else {
                break;
            }
        }

        best_entry.map(|e| e.position)
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let mut writer = BufWriter::new(file);

        for entry in &self.entries {
            writer.write_all(&entry.base_offset.to_be_bytes())?;
            writer.write_all(&entry.position.to_be_bytes())?;
        }

        writer.flush()?;
        writer.into_inner().unwrap().sync_all()?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl Default for SegmentIndex {
    fn default() -> Self {
        Self::new()
    }
}
