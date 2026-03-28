use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::message::{self, Message};

pub struct Log {
    writer: BufWriter<File>,
    offset: u64,
}

impl Log {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new().
            create(true).
            append(true).
            open(path)?;

        let offset = file.metadata()?.len() as u64;

        Ok(Self {
            writer: BufWriter::new(file),
            offset,
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
        self.writer.write_all(&encoded)?; 

        let msg_size = encoded.len() as u64;
        self.offset += msg_size;

        Ok(msg.offset)
    }
}
