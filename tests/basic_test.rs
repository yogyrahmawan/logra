use logra::{message, Consumer, Log, LogReader, Message, MmapReader, SegmentedLog};

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

#[test]
fn test_log_reader() {
    let path = "/tmp/logra_reader_test.log";
    let _ = std::fs::remove_file(path);

    let offset1;
    let offset2;
    {
        let mut log = Log::new(path).unwrap();
        offset1 = log.append(b"hello".to_vec()).unwrap();
        offset2 = log.append(b"world".to_vec()).unwrap();
    }

    let mut reader = LogReader::new(path).unwrap();

    let msg1 = reader.read_at(offset1).unwrap().unwrap();
    assert_eq!(msg1.value, b"hello");

    let msg2 = reader.read_at(offset2).unwrap().unwrap();
    assert_eq!(msg2.value, b"world");

    let all = reader.read_all().unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].value, b"hello");
    assert_eq!(all[1].value, b"world");
}

#[test]
fn test_mmap_reader() {
    let path = "/tmp/logra_mmap_test.log";
    let _ = std::fs::remove_file(path);

    let offset1;
    let offset2;
    {
        let mut log = Log::new(path).unwrap();
        offset1 = log.append(b"hello".to_vec()).unwrap();
        offset2 = log.append(b"world".to_vec()).unwrap();
    }

    let reader = MmapReader::new(path).unwrap();

    let msg1 = reader.read_at(offset1 as usize).unwrap();
    assert_eq!(msg1.value, b"hello");

    let msg2 = reader.read_at(offset2 as usize).unwrap();
    assert_eq!(msg2.value, b"world");

    let all = reader.read_all();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].value, b"hello");
    assert_eq!(all[1].value, b"world");
}

#[test]
fn test_segmented_log() {
    let dir = "/tmp/logra_segment_test";
    let _ = std::fs::remove_dir_all(dir);
    let segment_size = 100; // small size to trigger rotation

    let mut log = SegmentedLog::new(dir, segment_size).unwrap();

    // Append messages until we trigger rotation
    let mut offsets = Vec::new();
    for i in 0..10 {
        let msg = format!("message{}", i);
        let offset = log.append(msg.into_bytes()).unwrap();
        offsets.push(offset);
    }

    log.flush().unwrap();
    drop(log);

    // Check segments were created
    let entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();

    // Should have more than 1 segment file
    let seg_count = entries
        .iter()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .count();

    assert!(
        seg_count >= 2,
        "Expected at least 2 segments, got {}",
        seg_count
    );
}

#[test]
fn test_consumer() {
    let path = "/tmp/logra_consumer_test.log";
    let _ = std::fs::remove_file(path);

    {
        let mut log = Log::new(path).unwrap();
        log.append(b"hello".to_vec()).unwrap();
        log.append(b"world".to_vec()).unwrap();
        log.append(b"test".to_vec()).unwrap();
    }

    let mut consumer = Consumer::new(path).unwrap();

    let msg1 = consumer.poll().unwrap().unwrap();
    assert_eq!(msg1.value, b"hello");

    let msg2 = consumer.poll().unwrap().unwrap();
    assert_eq!(msg2.value, b"world");

    let pos = consumer.position();
    consumer.seek(0).unwrap();
    assert_eq!(consumer.position(), 0);

    let msg = consumer.poll().unwrap().unwrap();
    assert_eq!(msg.value, b"hello");

    consumer.seek(pos).unwrap();
    let msg3 = consumer.poll().unwrap().unwrap();
    assert_eq!(msg3.value, b"test");
}

#[test]
fn test_consumer_batch() {
    let path = "/tmp/logra_consumer_batch_test.log";
    let _ = std::fs::remove_file(path);

    {
        let mut log = Log::new(path).unwrap();
        for i in 0..5 {
            log.append(format!("msg{}", i).into_bytes()).unwrap();
        }
    }

    let mut consumer = Consumer::new(path).unwrap();
    let batch = consumer.poll_batch(3).unwrap();

    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].value, b"msg0");
    assert_eq!(batch[2].value, b"msg2");
}
