use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read};

#[derive(Debug, Clone)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub value: Vec<u8>,
}

// encode → binary
pub fn encode(msg: &Message) -> Vec<u8> {
    let mut body = Vec::with_capacity(32 + msg.value.len());

    body.write_u64::<BigEndian>(msg.offset).unwrap();
    body.write_u64::<BigEndian>(msg.timestamp).unwrap();

    body.write_u32::<BigEndian>(msg.value.len() as u32).unwrap();
    body.extend_from_slice(&msg.value);

    let mut out = Vec::with_capacity(4 + body.len());
    out.write_u32::<BigEndian>(body.len() as u32).unwrap();
    out.extend_from_slice(&body);

    out
}

// decode → struct
pub fn decode(buf: &[u8]) -> std::io::Result<Message> {
    let mut cursor = Cursor::new(buf);

    let offset = cursor.read_u64::<BigEndian>()?;
    let timestamp = cursor.read_u64::<BigEndian>()?;

    let len = cursor.read_u32::<BigEndian>()?;
    let mut value = vec![0; len as usize];
    cursor.read_exact(&mut value)?;

    Ok(Message {
        offset,
        timestamp,
        value,
    })
}