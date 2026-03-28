use logra::{message, Log, Message};

#[test]
fn test_message_encode_decode() {
    let msg = Message {
        offset: 0,
        timestamp: 1000,
        value: b"hello".to_vec(),
    };

    let encoded = message::encode(&msg);
    let decoded = message::decode(&encoded).unwrap();

    assert_eq!(msg.offset, decoded.offset);
    assert_eq!(msg.timestamp, decoded.timestamp);
    assert_eq!(msg.value, decoded.value);
}

#[test]
fn test_log_append() {
    let path = "/tmp/logra_test.log";
    let _ = std::fs::remove_file(path);

    let mut log = Log::new(path).unwrap();
    let offset1 = log.append(b"hello".to_vec()).unwrap();
    let offset2 = log.append(b"world".to_vec()).unwrap();

    assert_eq!(offset1, 0);
    assert!(offset2 > offset1);
}

#[test]
fn test_log_offset_continuity() {
    let path = "/tmp/logra_test2.log";
    let _ = std::fs::remove_file(path);

    let mut log = Log::new(path).unwrap();
    log.append(b"test".to_vec()).unwrap();
    let offset_after_first = log.append(b"test".to_vec()).unwrap();

    assert!(offset_after_first > 0);
}
