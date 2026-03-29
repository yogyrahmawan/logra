use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::message::{self, Message};

use message::decode_body;

pub struct LogReader {
    file: File,
}

impl LogReader {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    pub fn read_at(&mut self, offset: u64) -> io::Result<Option<Message>> {
        self.file.seek(SeekFrom::Start(offset))?;

        let mut len_buf = [0u8; 4];
        if self.file.read(&mut len_buf)? == 0 {
            return Ok(None);
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        let mut msg_buf = vec![0u8; len];

        self.file.read_exact(&mut msg_buf)?;

        let msg = decode_body(&msg_buf)?;
        Ok(Some(msg))
    }

    pub fn read_from(&mut self, start_offset: u64) -> io::Result<Vec<Message>> {
        self.file.seek(SeekFrom::Start(start_offset))?;
        let mut messages = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match self.file.read(&mut len_buf) {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; len];
            self.file.read_exact(&mut msg_buf)?;

            if let Ok(msg) = decode_body(&msg_buf) {
                messages.push(msg);
            }
        }

        Ok(messages)
    }

    pub fn read_all(&mut self) -> io::Result<Vec<Message>> {
        self.read_from(0)
    }
}
