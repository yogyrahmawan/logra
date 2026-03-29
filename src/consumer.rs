use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::message::{decode_body, Message};

pub struct Consumer {
    file: File,
    position: u64,
}

impl Consumer {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let position = 0;
        Ok(Self { file, position })
    }

    pub fn with_offset<P: AsRef<Path>>(path: P, offset: u64) -> io::Result<Self> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(Self {
            file,
            position: offset,
        })
    }

    pub fn poll(&mut self) -> io::Result<Option<Message>> {
        self.file.seek(SeekFrom::Start(self.position))?;

        let mut len_buf = [0u8; 4];
        match self.file.read(&mut len_buf) {
            Ok(0) => return Ok(None),
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        let mut msg_buf = vec![0u8; len];
        self.file.read_exact(&mut msg_buf)?;

        let msg = decode_body(&msg_buf)?;
        self.position += 4 + len as u64;

        Ok(Some(msg))
    }

    pub fn poll_batch(&mut self, max_messages: usize) -> io::Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(max_messages);

        for _ in 0..max_messages {
            match self.poll()? {
                Some(msg) => messages.push(msg),
                None => break,
            }
        }

        Ok(messages)
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn seek(&mut self, offset: u64) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.position = offset;
        Ok(())
    }
}
