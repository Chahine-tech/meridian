/// A WAL segment stored in S3.
///
/// Object key format: `{prefix}{seq_start:020}-{seq_end:020}.msgpack`
/// Zero-padded 20-digit integers give lexicographic ordering that matches
/// chronological ordering — critical for `ListObjectsV2` pagination during restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentKey {
    pub seq_start: u64,
    pub seq_end: u64,
}

impl SegmentKey {
    pub fn to_object_key(&self, prefix: &str) -> String {
        format!("{}{:020}-{:020}.msgpack", prefix, self.seq_start, self.seq_end)
    }

    pub fn from_object_key(key: &str, prefix: &str) -> Option<Self> {
        let tail = key.strip_prefix(prefix)?;
        let tail = tail.strip_suffix(".msgpack")?;
        let (start_str, end_str) = tail.split_once('-')?;
        let seq_start = start_str.parse().ok()?;
        let seq_end = end_str.parse().ok()?;
        Some(Self { seq_start, seq_end })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key = SegmentKey { seq_start: 1, seq_end: 500 };
        let object_key = key.to_object_key("wal/");
        assert_eq!(object_key, "wal/00000000000000000001-00000000000000000500.msgpack");
        let parsed = SegmentKey::from_object_key(&object_key, "wal/").unwrap();
        assert_eq!(parsed, key);
    }

    #[test]
    fn lexicographic_ordering() {
        let k1 = SegmentKey { seq_start: 1, seq_end: 500 }.to_object_key("wal/");
        let k2 = SegmentKey { seq_start: 501, seq_end: 1000 }.to_object_key("wal/");
        let k3 = SegmentKey { seq_start: 10_000, seq_end: 10_500 }.to_object_key("wal/");
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn from_object_key_invalid() {
        assert!(SegmentKey::from_object_key("wal/bad.msgpack", "wal/").is_none());
        assert!(SegmentKey::from_object_key("other/00000000000000000001-00000000000000000500.msgpack", "wal/").is_none());
    }
}
