use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use mojay::{Error, LogConfig, MAX_RECORD_SIZE, Message, PartitionLog, Record};
use tempfile::TempDir;

const FIRST_SEGMENT: &str = "00000000000000000000.log";
const FIRST_INDEX: &str = "00000000000000000000.idx";
const ALL: usize = usize::MAX;

fn message(i: u64) -> Message {
    Message::new(format!("k{i}"), format!("value-{i}")).header("n", i.to_string())
}

fn small_segments() -> LogConfig {
    LogConfig {
        segment_max_bytes: 300,
        index_interval_bytes: 64,
        ..LogConfig::default()
    }
}

fn assert_is_message(rec: &Record, i: u64) {
    assert_eq!(rec.offset, i);
    assert_eq!(rec.key.as_deref(), Some(format!("k{i}").as_bytes()));
    assert_eq!(rec.value.as_deref(), Some(format!("value-{i}").as_bytes()));
    assert_eq!(rec.headers[0].value, i.to_string().into_bytes());
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "log"))
        .collect();
    files.sort();
    files
}

/// Open a log, append `count` messages to it, and return the size of the
/// first segment after each append (the boundary of each record).
fn build_log(dir: &Path, config: LogConfig, count: u64) -> Vec<u64> {
    let mut log = PartitionLog::open(dir, config).unwrap();
    let mut boundaries = vec![fs::metadata(dir.join(FIRST_SEGMENT)).unwrap().len()];
    for i in 0..count {
        assert_eq!(log.append(&message(i)).unwrap(), i);
        boundaries.push(fs::metadata(dir.join(FIRST_SEGMENT)).unwrap().len());
    }
    boundaries
}

#[test]
fn empty_log_starts_at_zero() {
    let dir = TempDir::new().unwrap();
    let log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    assert_eq!((log.start_offset(), log.next_offset()), (0, 0));
    assert!(log.read(0, ALL).unwrap().is_empty());
    assert!(matches!(
        log.read(1, ALL),
        Err(Error::OffsetOutOfRange { offset: 1, .. })
    ));
}

#[test]
fn append_then_read_back_in_order() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    assert_eq!(log.append(&message(0)).unwrap(), 0);
    let batch: Vec<_> = (1..10).map(message).collect();
    assert_eq!(log.append_batch(&batch).unwrap(), 1..10);

    let records = log.read(0, ALL).unwrap();
    assert_eq!(records.len(), 10);
    for (i, rec) in records.iter().enumerate() {
        assert_is_message(rec, i as u64);
    }
    assert_is_message(&log.read(7, ALL).unwrap()[0], 7);
}

#[test]
fn explicit_timestamp_is_kept_and_missing_one_is_filled() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    log.append(&Message::with_value("a").timestamp(1234))
        .unwrap();
    log.append(&Message::with_value("b")).unwrap();
    let records = log.read(0, ALL).unwrap();
    assert_eq!(records[0].timestamp, 1234);
    assert!(records[1].timestamp > 1_600_000_000_000);
}

#[test]
fn data_survives_reopen() {
    let dir = TempDir::new().unwrap();
    build_log(dir.path(), small_segments(), 40);
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    assert_eq!(log.next_offset(), 40);
    assert_eq!(log.append(&message(40)).unwrap(), 40);
    let records = log.read(0, ALL).unwrap();
    assert_eq!(records.len(), 41);
    for (i, rec) in records.iter().enumerate() {
        assert_is_message(rec, i as u64);
    }
}

#[test]
fn segments_roll_and_reads_cross_them() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    for i in 0..100 {
        log.append(&message(i)).unwrap();
    }
    let files = log_files(dir.path());
    assert!(files.len() > 5, "only {} segments", files.len());
    for file in &files {
        assert!(fs::metadata(file).unwrap().len() <= 300);
    }
    // Every offset reads back correctly, whatever its segment.
    for i in 0..100 {
        let records = log.read(i, ALL).unwrap();
        assert_eq!(records.len() as u64, 100 - i);
        assert_is_message(&records[0], i);
    }
}

#[test]
fn batch_spanning_several_segments_keeps_offsets_contiguous() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    let batch: Vec<_> = (0..30).map(message).collect();
    assert_eq!(log.append_batch(&batch).unwrap(), 0..30);
    assert!(log_files(dir.path()).len() > 1);
    let records = log.read(0, ALL).unwrap();
    assert_eq!(records.len(), 30);
    for (i, rec) in records.iter().enumerate() {
        assert_is_message(rec, i as u64);
    }
}

#[test]
fn read_respects_byte_budget_without_skipping() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    for i in 0..60 {
        log.append(&message(i)).unwrap();
    }
    // Paging with a small budget must read everything back, in order, with no gaps.
    let mut next = 0;
    let mut seen = 0;
    while next < log.next_offset() {
        let page = log.read(next, 150).unwrap();
        assert!(!page.is_empty());
        for rec in &page {
            assert_is_message(rec, next);
            next += 1;
        }
        seen += page.len();
    }
    assert_eq!(seen, 60);
}

#[test]
fn record_bigger_than_budget_is_still_returned_alone() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    log.append(&Message::with_value(vec![7u8; 10_000])).unwrap();
    log.append(&message(1)).unwrap();
    let page = log.read(0, 10).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].offset, 0);
}

#[test]
fn invalid_message_in_batch_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    let batch = vec![message(0), Message::with_value(vec![0u8; MAX_RECORD_SIZE])];
    assert!(matches!(
        log.append_batch(&batch),
        Err(Error::RecordTooLarge { .. })
    ));
    assert_eq!(log.next_offset(), 0);
    assert!(log.read(0, ALL).unwrap().is_empty());
}

#[test]
fn invalid_config_is_rejected() {
    let dir = TempDir::new().unwrap();
    let config = LogConfig {
        segment_max_bytes: 10,
        ..LogConfig::default()
    };
    assert!(matches!(
        PartitionLog::open(dir.path(), config),
        Err(Error::InvalidConfig(_))
    ));
}

/// Simulate a crash at every possible byte: the log must always reopen
/// with exactly the complete records, then accept new writes.
#[test]
fn recovers_from_a_cut_at_every_byte() {
    let reference = TempDir::new().unwrap();
    let config = LogConfig {
        index_interval_bytes: 32,
        ..LogConfig::default()
    };
    let boundaries = build_log(reference.path(), config.clone(), 6);
    let full_log = fs::read(reference.path().join(FIRST_SEGMENT)).unwrap();
    let full_index = fs::read(reference.path().join(FIRST_INDEX)).unwrap();
    assert!(
        !full_index.is_empty(),
        "the test must cover a non-empty index"
    );

    for cut in boundaries[0]..=*boundaries.last().unwrap() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join(FIRST_SEGMENT), &full_log[..cut as usize]).unwrap();
        // The full index "survived" the crash, with entries beyond the cut.
        fs::write(dir.path().join(FIRST_INDEX), &full_index).unwrap();

        let complete = boundaries.iter().filter(|&&b| b <= cut).count() as u64 - 1;
        let mut log = PartitionLog::open(dir.path(), config.clone()).unwrap();
        assert_eq!(log.next_offset(), complete, "cut at byte {cut}");
        for i in 0..complete {
            assert_is_message(&log.read(i, ALL).unwrap()[0], i);
        }
        assert_eq!(
            fs::metadata(dir.path().join(FIRST_SEGMENT)).unwrap().len(),
            boundaries[complete as usize],
            "the incomplete tail must be truncated"
        );

        assert_eq!(log.append(&message(complete)).unwrap(), complete);
        drop(log);
        let log = PartitionLog::open(dir.path(), config.clone()).unwrap();
        assert_eq!(log.read(0, ALL).unwrap().len() as u64, complete + 1);
    }
}

#[test]
fn garbage_after_the_last_record_is_truncated() {
    let dir = TempDir::new().unwrap();
    let boundaries = build_log(dir.path(), LogConfig::default(), 3);
    let path = dir.path().join(FIRST_SEGMENT);
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_slice(&[0xAB; 37]);
    fs::write(&path, bytes).unwrap();

    let log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    assert_eq!(log.next_offset(), 3);
    assert_eq!(fs::metadata(&path).unwrap().len(), boundaries[3]);
}

#[test]
fn corrupted_record_in_active_segment_truncates_from_there() {
    let dir = TempDir::new().unwrap();
    let boundaries = build_log(dir.path(), LogConfig::default(), 5);
    let path = dir.path().join(FIRST_SEGMENT);
    let mut bytes = fs::read(&path).unwrap();
    bytes[boundaries[2] as usize + 12] ^= 0xFF; // inside the record at offset 2
    fs::write(&path, bytes).unwrap();

    let log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    assert_eq!(log.next_offset(), 2);
}

#[test]
fn missing_index_of_sealed_segment_is_rebuilt() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    for i in 0..50 {
        log.append(&message(i)).unwrap();
    }
    drop(log);
    let index = dir.path().join(FIRST_INDEX);
    let original = fs::read(&index).unwrap();
    fs::remove_file(&index).unwrap();

    let log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    assert_eq!(fs::read(&index).unwrap(), original);
    for i in 0..50 {
        assert_is_message(&log.read(i, ALL).unwrap()[0], i);
    }
}

#[test]
fn wrong_index_entries_are_ignored() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    for i in 0..50 {
        log.append(&message(i)).unwrap();
    }
    drop(log);
    // An entry pointing into the middle of nowhere, in the active segment's index.
    let last_index = log_files(dir.path()).last().unwrap().with_extension("idx");
    let mut bytes = fs::read(&last_index).unwrap();
    bytes.extend_from_slice(&5u32.to_le_bytes());
    bytes.extend_from_slice(&17u32.to_le_bytes());
    fs::write(&last_index, bytes).unwrap();

    let log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    assert_eq!(log.next_offset(), 50);
    for i in 0..50 {
        assert_is_message(&log.read(i, ALL).unwrap()[0], i);
    }
}

/// An index entry `(relative offset, position)`, as stored in a `.idx` file.
fn index_entry(relative_offset: u32, position: u64) -> Vec<u8> {
    let mut bytes = relative_offset.to_le_bytes().to_vec();
    bytes.extend_from_slice(&u32::try_from(position).unwrap().to_le_bytes());
    bytes
}

/// An index entry that names a record but points anywhere else, at each
/// position of the segment: reads step back to the start of the segment
/// and still return every record, none skipped.
#[test]
fn wrong_index_entry_at_each_position_is_skipped() {
    const COUNT: u64 = 12;
    const NAMED: u64 = 6;
    let dir = TempDir::new().unwrap();
    let boundaries = build_log(dir.path(), LogConfig::default(), COUNT);
    // The real entry of the last record, after the wrong one: opening the log
    // checks only the last entries, so the wrong one stays in the index.
    let last_entry = index_entry((COUNT - 1) as u32, boundaries[COUNT as usize - 1]);

    for position in boundaries[0]..boundaries[COUNT as usize - 1] {
        let mut index = index_entry(NAMED as u32, position);
        index.extend_from_slice(&last_entry);
        fs::write(dir.path().join(FIRST_INDEX), &index).unwrap();

        let log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
        for from in NAMED..COUNT {
            let records = log
                .read(from, ALL)
                .unwrap_or_else(|e| panic!("entry at {position}, read from {from}: {e}"));
            assert_eq!(records.len() as u64, COUNT - from, "entry at {position}");
            for (rec, i) in records.iter().zip(from..) {
                assert_is_message(rec, i);
            }
        }
    }
}

#[test]
fn corruption_in_sealed_segment_is_reported_not_repaired() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    for i in 0..50 {
        log.append(&message(i)).unwrap();
    }
    drop(log);
    let path = dir.path().join(FIRST_SEGMENT);
    let len_before = fs::metadata(&path).unwrap().len();
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    std::os::unix::fs::FileExt::write_all_at(&file, &[0xFF], 40).unwrap();

    let log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    assert!(matches!(log.read(0, ALL), Err(Error::Corrupt { .. })));
    assert_eq!(fs::metadata(&path).unwrap().len(), len_before);
}

#[test]
fn missing_segment_in_the_middle_is_reported() {
    let dir = TempDir::new().unwrap();
    let mut log = PartitionLog::open(dir.path(), small_segments()).unwrap();
    for i in 0..60 {
        log.append(&message(i)).unwrap();
    }
    drop(log);
    let files = log_files(dir.path());
    assert!(files.len() >= 3);
    fs::remove_file(&files[1]).unwrap();
    assert!(matches!(
        PartitionLog::open(dir.path(), small_segments()),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn file_that_is_not_a_segment_is_rejected() {
    let dir = TempDir::new().unwrap();
    fs::write(
        dir.path().join(FIRST_SEGMENT),
        b"this is not a Mojay segment",
    )
    .unwrap();
    assert!(matches!(
        PartitionLog::open(dir.path(), LogConfig::default()),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn segment_without_header_after_crash_is_reinitialised() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join(FIRST_SEGMENT), b"MOJ").unwrap();
    let mut log = PartitionLog::open(dir.path(), LogConfig::default()).unwrap();
    assert_eq!(log.next_offset(), 0);
    assert_eq!(log.append(&message(0)).unwrap(), 0);
}
