use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::path::Path;

use crate::message::decode_body;

pub struct MmapReader {
    file: File,
    mmap: Mmap,
}

impl MmapReader {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { file, mmap })
    }

    pub fn read_at(&self, offset: usize) -> Option<crate::message::Message> {
        if offset >= self.mmap.len() {
            return None;
        }

        let data = &self.mmap[offset..];
        if data.len() < 4 {
            return None;
        }

        let len = u32::from_be_bytes(data[..4].try_into().ok()?) as usize;
        if data.len() < 4 + len {
            return None;
        }

        let body = &data[4..4 + len];
        decode_body(body).ok()
    }

    pub fn read_from(&self, start_offset: usize) -> Vec<crate::message::Message> {
        let mut messages = Vec::new();
        let mut pos = start_offset;

        while pos < self.mmap.len() {
            let data = &self.mmap[pos..];
            if data.len() < 4 {
                break;
            }

            let len =
                match (u32::from_be_bytes(data[..4].try_into().unwrap()) as usize).checked_add(4) {
                    Some(total) => total,
                    None => break,
                };

            if data.len() < len {
                break;
            }

            let body = &data[4..len];
            if let Ok(msg) = decode_body(body) {
                messages.push(msg);
            }

            pos += len;
        }

        messages
    }

    pub fn read_all(&self) -> Vec<crate::message::Message> {
        self.read_from(0)
    }

    pub fn refresh(&mut self) -> io::Result<()> {
        self.mmap = unsafe { Mmap::map(&self.file)? };
        Ok(())
    }
}
