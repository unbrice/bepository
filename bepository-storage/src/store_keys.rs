// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Key encoding/decoding for the per-folder SlateDB key layout.
//!
//! Every key starts with a uniform 2-byte tag. Byte 0 partitions the keyspace
//! into two RFC-0024 segments — `b` for block-data, `m` for everything else —
//! so each memtable freeze produces at most one L0 SST per side instead of one
//! per key class. Byte 1 disambiguates the class within its segment.
//!
//! Key layout (uniform 2-byte tag, no trailing `/` on the tag):
//!   bd<seq_be8>                        — block data (block segment)
//!   mb<dir>/<hash_32>                  — block pointer (BlockRef)
//!   mr<hash_32>/<dir>//<basename>      — reverse block refs
//!   mn<dir>//<basename>                — primary file index (FileInfo)
//!   ms<seq_be8>                        — sequence → name mapping
//!   mi<epoch_base32>/<dir>//<basename> — inbox staging area
//!   md<devid_32>                       — remote device index state
//!   mx                                 — our FolderIndexMeta singleton

use std::sync::Arc;

use bytes::Bytes;
use slatedb::{BloomFilterPolicy, FilterPolicy, PrefixExtractor, PrefixTarget};

use bepository_bep::error::StorageError;

/// Hash length in bytes (SHA-256).
pub const HASH_LEN: usize = 32;

/// Device ID length in bytes.
pub const DEVID_LEN: usize = 32;

/// Blocks smaller than this are stored inline in the `FileInfo` rather than
/// as `bd` block-data keys.
pub const INLINE_BLOCK_THRESHOLD: u32 = 4096;

/// True if a block of `size` bytes is stored inline (see
/// [`INLINE_BLOCK_THRESHOLD`]). Negative sizes are never inline.
#[must_use]
pub fn is_inline_block(size: i32) -> bool {
    u32::try_from(size).is_ok_and(|s| s < INLINE_BLOCK_THRESHOLD)
}

// --- Prefix constants ---

pub const FILE_PREFIX: &[u8] = b"mn";
pub const SEQ_PREFIX: &[u8] = b"ms";
pub const BLOCK_PREFIX: &[u8] = b"mb";
pub const BLOCK_REV_PREFIX: &[u8] = b"mr";
pub const IX_KEY: &[u8] = b"mx";
pub const DEVICE_PREFIX: &[u8] = b"md";
pub const INBOX_PREFIX: &[u8] = b"mi";
pub const BLOCK_DATA_PREFIX: &[u8] = b"bd";

// --- Segment prefixes (RFC-0024) ---
//
// The segment extractor partitions the keyspace on byte 0: every key in a
// segment shares its first byte. These are the values it returns, used by
// the scheduler/compaction code to filter `CompactionSpec` per segment.

/// Segment prefix for block-data keys (`bd<…>`).
pub const BLOCK_SEGMENT_PREFIX: &[u8] = b"b";
/// Segment prefix for metadata keys (`mn<…>`, `mb<…>`, `mr<…>`, `ms<…>`,
/// `mi<…>`, `md<…>`, `mx`).
pub const METADATA_SEGMENT_PREFIX: &[u8] = b"m";

// --- Name splitting helpers ---

/// Split a file path into (directory, basename).
/// For root-level files, directory is empty.
fn split_name(name: &str) -> (&str, &str) {
    name.rsplit_once('/').unwrap_or(("", name))
}

/// Extract the directory portion from a file path (everything before the last `/`).
/// Returns empty string for root-level files.
#[must_use]
pub fn dirname(name: &str) -> &str {
    split_name(name).0
}

/// Reconstruct a full name from a (dir, basename) pair.
fn join_name(dir: &str, basename: &str) -> String {
    if dir.is_empty() {
        basename.to_string()
    } else {
        let mut s = String::with_capacity(dir.len() + 1 + basename.len());
        s.push_str(dir);
        s.push('/');
        s.push_str(basename);
        s
    }
}

// --- File key: mn<dir>//<basename> ---

#[must_use]
pub fn file_key(name: &str) -> Vec<u8> {
    let (dir, basename) = split_name(name);
    let mut key = Vec::with_capacity(2 + dir.len() + 2 + basename.len());
    key.extend_from_slice(FILE_PREFIX);
    key.extend_from_slice(dir.as_bytes());
    key.extend_from_slice(b"//");
    key.extend_from_slice(basename.as_bytes());
    key
}

#[must_use]
pub fn parse_file_key(key: &[u8]) -> Option<String> {
    let rest = key.strip_prefix(FILE_PREFIX)?;
    let s = std::str::from_utf8(rest).ok()?;
    let (dir, basename) = s.rsplit_once("//")?;
    Some(join_name(dir, basename))
}

// --- Sequence key: ms<seq_be8> ---

/// Sequence key length: 2 (prefix) + 8 (big-endian u64) = 10.
pub const SEQ_KEY_LEN: usize = 10;

pub fn seq_key(seq: i64) -> Result<[u8; SEQ_KEY_LEN], StorageError> {
    let seq_u = u64::try_from(seq).map_err(|_| {
        StorageError::Internal(format!("sequence numbers must be non-negative, got {seq}"))
    })?;
    let mut key = [0u8; SEQ_KEY_LEN];
    key[0] = b'm';
    key[1] = b's';
    key[2..].copy_from_slice(&seq_u.to_be_bytes());
    Ok(key)
}

#[must_use]
pub fn parse_seq_key(key: &[u8]) -> Option<u64> {
    if key.len() != SEQ_KEY_LEN || !key.starts_with(SEQ_PREFIX) {
        return None;
    }
    let bytes: [u8; 8] = key[2..10].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

/// The upper-bound key for scanning all `ms<…>` entries.
/// "mt" > "ms<…>" lexicographically and is the smallest 2-byte tag past `ms`.
pub const SEQ_SCAN_END: &[u8] = b"mt";

/// Build a scan start key for sequences > `since`.
pub fn seq_scan_start(since: i64) -> Result<[u8; SEQ_KEY_LEN], StorageError> {
    let next = since.checked_add(1).ok_or_else(|| {
        StorageError::Internal(format!("sequence number overflow scanning past {since}"))
    })?;
    seq_key(next)
}

// --- Block pointer key: mb<dir>/<hash_32> ---

#[must_use]
pub fn block_pointer_key(dir: &str, hash: &[u8; HASH_LEN]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + dir.len() + 1 + HASH_LEN);
    key.extend_from_slice(BLOCK_PREFIX);
    key.extend_from_slice(dir.as_bytes());
    key.push(b'/');
    key.extend_from_slice(hash);
    key
}

// --- Block data key: bd<blockseq_be8> ---

/// Block data key length: 2 (prefix) + 8 (big-endian u64) = 10.
pub const BLOCK_DATA_KEY_LEN: usize = 10;

pub fn block_data_seq_key(seq: u64) -> [u8; BLOCK_DATA_KEY_LEN] {
    let mut key = [0u8; BLOCK_DATA_KEY_LEN];
    key[0] = b'b';
    key[1] = b'd';
    key[2..].copy_from_slice(&seq.to_be_bytes());
    key
}

/// Minimum valid block sequence number. Values below this floor represent corruption.
pub const MIN_BLOCK_SEQ: u64 = 1024;

/// Validate a block sequence number. Must be >= MIN_BLOCK_SEQ.
pub fn validate_block_seq(seq: u64) -> Result<(), StorageError> {
    if seq < MIN_BLOCK_SEQ {
        return Err(StorageError::Corruption(format!(
            "invalid block sequence number: {seq} (must be >= {MIN_BLOCK_SEQ})"
        )));
    }
    Ok(())
}

#[must_use]
pub fn parse_block_data_seq_key(key: &[u8]) -> Option<u64> {
    if key.len() != BLOCK_DATA_KEY_LEN || !key.starts_with(BLOCK_DATA_PREFIX) {
        return None;
    }
    let bytes: [u8; 8] = key[2..10].try_into().ok()?;
    let seq = u64::from_be_bytes(bytes);
    if seq < MIN_BLOCK_SEQ {
        return None;
    }
    Some(seq)
}

/// Length of the bloom-filter prefix extracted from `mr<…>` keys: the literal
/// `mr` tag plus the full 32-byte hash. Lets `scan_prefix(mr<hash>/...)`
/// consult the per-SST bloom filter — without it, prefix scans would have to
/// fetch each candidate SST's index block to test membership, which is the
/// hot path during initial sync where most lookups return empty.
const BR_FILTER_PREFIX_LEN: usize = BLOCK_REV_PREFIX.len() + HASH_LEN;

/// Filter extractor that hashes the `mr<hash>` prefix of every reverse-ref
/// key into the bloom filter. Keys outside the `mr` family return `None` and
/// are not added to the prefix filter; they remain covered by point-key
/// filtering via `with_whole_key_filtering(true)`.
#[derive(Debug, Default)]
pub struct BrPrefixExtractor;

impl PrefixExtractor for BrPrefixExtractor {
    fn name(&self) -> &str {
        "bep-mr-34"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes: &Bytes = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b,
        };
        (bytes.len() >= BR_FILTER_PREFIX_LEN && bytes.starts_with(BLOCK_REV_PREFIX))
            .then_some(BR_FILTER_PREFIX_LEN)
    }
}

/// Two-segment extractor: every key is routed to either the `b` (block-data)
/// segment or the `m` (metadata) segment based on its first byte. Each
/// memtable freeze therefore produces at most two L0 SSTs — one per side —
/// rather than one per key class.
///
/// Returning `Some(1)` (rather than the 2-byte tag length) is what makes the
/// collapse work: all metadata key classes share the same one-byte prefix
/// `m`, so they all land in the same segment. The 2-byte tags exist solely
/// to disambiguate classes within a segment for the parsers; segmentation
/// only cares about byte 0.
#[derive(Debug, Default)]
pub struct BepSegmentExtractor;

impl PrefixExtractor for BepSegmentExtractor {
    fn name(&self) -> &str {
        "bep-segment-v2"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes: &Bytes = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b,
        };
        match bytes.first() {
            Some(&b'b') | Some(&b'm') => Some(1),
            _ => None,
        }
    }
}

/// Filter policies for SSTs. These must match across writer and reader components.
///
/// Registers both whole-key and prefix-aware bloom filters. SlateDB selects the
/// policy by name; keeping both ensures that SSTs written with either policy
/// remain decodable without falling back to expensive index fetches.
///
/// 1. `BloomFilterPolicy::new(10)`: Default whole-key bloom (`_bf`).
/// 2. `BloomFilterPolicy` + `BrPrefixExtractor`: Accelerates `scan_prefix("mr<hash>/")`.
#[must_use]
pub fn make_filter_policies() -> Vec<Arc<dyn FilterPolicy>> {
    vec![
        Arc::new(BloomFilterPolicy::new(10)),
        Arc::new(BloomFilterPolicy::new(10).with_prefix_extractor(Arc::new(BrPrefixExtractor))),
    ]
}

// --- Block reverse ref key: mr<hash_32>/<dir>//<basename> ---

#[must_use]
pub fn block_reverse_key(hash: &[u8; HASH_LEN], name: &str) -> Vec<u8> {
    let (dir, basename) = split_name(name);
    let mut key = Vec::with_capacity(3 + HASH_LEN + 1 + dir.len() + 2 + basename.len());
    key.extend_from_slice(BLOCK_REV_PREFIX);
    key.extend_from_slice(hash);
    key.push(b'/');
    key.extend_from_slice(dir.as_bytes());
    key.extend_from_slice(b"//");
    key.extend_from_slice(basename.as_bytes());
    key
}

/// Prefix for scanning all reverse refs of a given block hash.
#[must_use]
pub fn block_reverse_prefix(hash: &[u8; HASH_LEN]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(3 + HASH_LEN + 1);
    prefix.extend_from_slice(BLOCK_REV_PREFIX);
    prefix.extend_from_slice(hash);
    prefix.push(b'/');
    prefix
}

/// Parse a block reverse ref key, returning (hash, name).
#[must_use]
pub fn parse_block_reverse_key(key: &[u8]) -> Option<([u8; HASH_LEN], String)> {
    let rest = key.strip_prefix(BLOCK_REV_PREFIX)?;
    if rest.len() < HASH_LEN + 1 {
        return None;
    }
    let hash: [u8; HASH_LEN] = rest[..HASH_LEN].try_into().ok()?;
    if rest[HASH_LEN] != b'/' {
        return None;
    }
    let s = std::str::from_utf8(&rest[HASH_LEN + 1..]).ok()?;
    let (dir, basename) = s.rsplit_once("//")?;
    Some((hash, join_name(dir, basename)))
}

// --- Inbox key: mi<epoch_base32>/<dir>//<basename> ---
// Epoch is 8-character Crockford Base32 (same encoding as epoch filenames in
// bepository-lock), keeping keys human-readable and lexicographically ordered.

/// Length of the Crockford Base32 epoch component.
const EPOCH_BASE32_LEN: usize = 8;

#[must_use]
pub fn inbox_key(epoch: bepository_lock::Epoch, name: &str) -> Vec<u8> {
    let (dir, basename) = split_name(name);
    let mut key = Vec::with_capacity(3 + EPOCH_BASE32_LEN + 1 + dir.len() + 2 + basename.len());
    key.extend_from_slice(INBOX_PREFIX);
    key.extend_from_slice(epoch.as_base32().as_bytes());
    key.push(b'/');
    key.extend_from_slice(dir.as_bytes());
    key.extend_from_slice(b"//");
    key.extend_from_slice(basename.as_bytes());
    key
}

#[must_use]
pub fn parse_inbox_key(key: &[u8]) -> Option<(bepository_lock::Epoch, String)> {
    let rest = key.strip_prefix(INBOX_PREFIX)?;
    if rest.len() < EPOCH_BASE32_LEN + 1 {
        return None;
    }
    let epoch_str = std::str::from_utf8(&rest[..EPOCH_BASE32_LEN]).ok()?;
    let epoch = bepository_lock::Epoch::parse(epoch_str)?;
    if rest[EPOCH_BASE32_LEN] != b'/' {
        return None;
    }
    let s = std::str::from_utf8(&rest[EPOCH_BASE32_LEN + 1..]).ok()?;
    let (dir, basename) = s.rsplit_once("//")?;
    Some((epoch, join_name(dir, basename)))
}

// --- Device key: md<devid_32> ---

/// Device state key length: 2 (prefix) + 32 (device ID) = 34.
pub const DEVICE_KEY_LEN: usize = 34;

#[must_use]
pub fn device_key(devid: &[u8; DEVID_LEN]) -> [u8; DEVICE_KEY_LEN] {
    let mut key = [0u8; DEVICE_KEY_LEN];
    key[0] = b'm';
    key[1] = b'd';
    key[2..].copy_from_slice(devid);
    key
}

/// Parse a block pointer key `mb<dir>/<hash_32>`, returning the 32-byte hash.
///
/// Matches keys that start with `mb` and end with exactly 32 raw bytes after
/// a `/`.
#[must_use]
pub fn parse_block_pointer_key(key: &[u8]) -> Option<[u8; HASH_LEN]> {
    // Minimum: mb + / + HASH_LEN
    if key.len() < BLOCK_PREFIX.len() + 1 + HASH_LEN || !key.starts_with(BLOCK_PREFIX) {
        return None;
    }
    if key[key.len() - HASH_LEN - 1] != b'/' {
        return None;
    }
    key[key.len() - HASH_LEN..].try_into().ok()
}

/// Extract the directory from a file key `mn<dir>//<basename>` (borrowing).
#[must_use]
pub fn file_key_dir(key: &[u8]) -> Option<&str> {
    let rest = key.strip_prefix(FILE_PREFIX)?;
    let s = std::str::from_utf8(rest).ok()?;
    let (dir, _) = s.rsplit_once("//")?;
    Some(dir)
}

/// Extract the directory from a block pointer key `mb<dir>/<hash_32>` (borrowing).
#[must_use]
pub fn block_key_dir(key: &[u8]) -> Option<&str> {
    if !key.starts_with(BLOCK_PREFIX) {
        return None;
    }
    let suffix_len = 1 + HASH_LEN; // /<hash>
    let prefix_len = BLOCK_PREFIX.len();
    if key.len() < prefix_len + suffix_len {
        return None;
    }
    let dir_end = key.len() - suffix_len;
    std::str::from_utf8(&key[prefix_len..dir_end]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_key_round_trip() {
        let key = file_key("docs/reports/q3.pdf");
        assert_eq!(parse_file_key(&key).unwrap(), "docs/reports/q3.pdf");
    }

    #[test]
    fn file_key_root_round_trip() {
        let key = file_key("readme.txt");
        assert_eq!(parse_file_key(&key).unwrap(), "readme.txt");
    }

    #[test]
    fn file_key_round_trip_with_double_slash_in_dir() {
        let key = file_key("a//b/c.txt");
        assert_eq!(parse_file_key(&key).unwrap(), "a//b/c.txt");
        assert_eq!(file_key_dir(&key).unwrap(), "a//b");
    }

    #[test]
    fn block_reverse_key_round_trip_with_double_slash_in_dir() {
        let hash = [0xCC; HASH_LEN];
        let key = block_reverse_key(&hash, "a//b/c.txt");
        let (parsed_hash, parsed_name) = parse_block_reverse_key(&key).unwrap();
        assert_eq!(parsed_hash, hash);
        assert_eq!(parsed_name, "a//b/c.txt");
    }

    #[test]
    fn inbox_key_round_trip_with_double_slash_in_dir() {
        let e = bepository_lock::Epoch::new(7).unwrap();
        let key = inbox_key(e, "a//b/c.txt");
        let (epoch, name) = parse_inbox_key(&key).unwrap();
        assert_eq!(epoch, e);
        assert_eq!(name, "a//b/c.txt");
    }

    #[test]
    fn seq_key_round_trip() {
        let key = seq_key(42).unwrap();
        assert_eq!(parse_seq_key(&key), Some(42));
    }

    #[test]
    fn seq_key_ordering() {
        let k1 = seq_key(1).unwrap();
        let k2 = seq_key(2).unwrap();
        let k100 = seq_key(100).unwrap();
        assert!(k1 < k2);
        assert!(k2 < k100);
    }

    #[test]
    fn seq_key_negative_returns_error() {
        assert!(seq_key(-1).is_err());
    }

    #[test]
    fn seq_scan_start_overflow_returns_error() {
        assert!(seq_scan_start(i64::MAX).is_err());
        assert_eq!(parse_seq_key(&seq_scan_start(41).unwrap()), Some(42));
    }

    #[test]
    fn block_reverse_key_round_trip() {
        let hash = [0xAA; HASH_LEN];
        let key = block_reverse_key(&hash, "photos/2024/sunset.jpg");
        let (parsed_hash, parsed_name) = parse_block_reverse_key(&key).unwrap();
        assert_eq!(parsed_hash, hash);
        assert_eq!(parsed_name, "photos/2024/sunset.jpg");
    }

    #[test]
    fn inbox_key_round_trip() {
        let e = bepository_lock::Epoch::new(42).unwrap();
        let key = inbox_key(e, "docs/readme.txt");
        let (epoch, name) = parse_inbox_key(&key).unwrap();
        assert_eq!(epoch, e);
        assert_eq!(name, "docs/readme.txt");
    }

    #[test]
    fn inbox_key_epoch_ordering() {
        let k1 = inbox_key(bepository_lock::Epoch::new(1).unwrap(), "a.txt");
        let k2 = inbox_key(bepository_lock::Epoch::new(2).unwrap(), "a.txt");
        assert!(k1 < k2);
    }

    #[test]
    fn block_pointer_key_round_trip() {
        let hash = [0xBB; HASH_LEN];
        let key = block_pointer_key("docs", &hash);
        let parsed = parse_block_pointer_key(&key).unwrap();
        assert_eq!(parsed, hash);
    }

    #[test]
    fn block_pointer_key_root_round_trip() {
        let hash = [0xDD; HASH_LEN];
        let key = block_pointer_key("", &hash);
        let parsed = parse_block_pointer_key(&key).unwrap();
        assert_eq!(parsed, hash);
    }

    #[test]
    fn block_data_seq_key_round_trip() {
        let key = block_data_seq_key(12345);
        let parsed = parse_block_data_seq_key(&key).unwrap();
        assert_eq!(parsed, 12345);
    }

    #[test]
    fn dirname_subdir() {
        assert_eq!(dirname("docs/reports/q3.pdf"), "docs/reports");
    }

    #[test]
    fn dirname_root() {
        assert_eq!(dirname("readme.txt"), "");
    }

    #[test]
    fn dirname_single_level() {
        assert_eq!(dirname("docs/readme.txt"), "docs");
    }

    #[test]
    fn file_key_dir_subdir() {
        let key = file_key("docs/reports/q3.pdf");
        assert_eq!(file_key_dir(&key).unwrap(), "docs/reports");
    }

    #[test]
    fn file_key_dir_root() {
        let key = file_key("readme.txt");
        assert_eq!(file_key_dir(&key).unwrap(), "");
    }

    #[test]
    fn block_key_dir_pointer() {
        let hash = [0xBB; HASH_LEN];
        let key = block_pointer_key("photos", &hash);
        assert_eq!(block_key_dir(&key).unwrap(), "photos");
    }

    #[test]
    fn block_key_dir_empty_dir() {
        let hash = [0xAA; HASH_LEN];
        let key = block_pointer_key("", &hash);
        assert_eq!(block_key_dir(&key).unwrap(), "");
    }

    #[test]
    fn grouping_order() {
        // Files in same dir should be together, and subdirs should come after files.
        // This is guaranteed by the "dir//basename" encoding because '/' < [any alphanumeric].
        let mut paths = ["a/b", "a/c", "a/b/c"];
        let original = paths;
        paths.sort_by_key(|&p| file_key(p));
        assert_eq!(paths, original);
    }

    #[test]
    fn test_bep_segment_extractor_segments_are_antichain() {
        // The extractor produces exactly two segment prefixes: `b` and `m`.
        // They are 1-byte and pairwise non-nesting by construction.
        let extractor = BepSegmentExtractor;
        let b_seg = extractor.prefix_len(&PrefixTarget::Point(Bytes::from_static(
            b"bd\0\0\0\0\0\0\0\x10",
        )));
        let m_seg = extractor.prefix_len(&PrefixTarget::Point(Bytes::from_static(b"mx")));
        assert_eq!(b_seg, Some(1));
        assert_eq!(m_seg, Some(1));
    }

    #[test]
    fn test_bep_segment_extractor_collapses_to_two_segments() {
        let extractor = BepSegmentExtractor;
        let epoch = bepository_lock::Epoch::new(1).unwrap();
        let hash = [42u8; HASH_LEN];
        let devid = [7u8; DEVID_LEN];

        // (key, expected first-byte of segment prefix) — every metadata key
        // class collapses to `m`; the only `b` keys are block data.
        let test_cases: Vec<(Vec<u8>, u8)> = vec![
            (file_key("dir/file.txt"), b'm'),
            (seq_key(100).unwrap().to_vec(), b'm'),
            (block_pointer_key("dir", &hash), b'm'),
            (block_data_seq_key(12345).to_vec(), b'b'),
            (block_reverse_key(&hash, "dir/file.txt"), b'm'),
            (IX_KEY.to_vec(), b'm'),
            (device_key(&devid).to_vec(), b'm'),
            (inbox_key(epoch, "dir/file.txt"), b'm'),
        ];

        for (key, expected_first) in test_cases {
            let key_bytes = Bytes::from(key);

            // Point: must return Some(1) and the extracted byte must match.
            let point_len = extractor.prefix_len(&PrefixTarget::Point(key_bytes.clone()));
            assert_eq!(
                point_len,
                Some(1),
                "Point prefix len mismatch for key {key_bytes:?}"
            );
            assert_eq!(
                key_bytes[0], expected_first,
                "Segment byte mismatch for key {key_bytes:?}"
            );

            // Prefix at the extracted length: same answer.
            let exact_prefix = key_bytes.slice(0..1);
            let prefix_len_exact = extractor.prefix_len(&PrefixTarget::Prefix(exact_prefix));
            assert_eq!(
                prefix_len_exact,
                Some(1),
                "Prefix target (exact) len mismatch for key {key_bytes:?}"
            );

            // Prefix longer than the extracted length: still Some(1).
            if key_bytes.len() > 1 {
                let longer_prefix = key_bytes.slice(0..2);
                let prefix_len_longer = extractor.prefix_len(&PrefixTarget::Prefix(longer_prefix));
                assert_eq!(
                    prefix_len_longer,
                    Some(1),
                    "Prefix target (longer) len mismatch for key {key_bytes:?}"
                );
            }
        }
    }

    #[test]
    fn test_bep_segment_extractor_rejects_foreign_first_byte() {
        let extractor = BepSegmentExtractor;
        // Any key whose first byte is not `b` or `m` is outside this layout
        // and must yield `None` — slatedb will reject the write, which is
        // the desired loud failure mode (key constructors only emit `b…` /
        // `m…`, so this triggers only on a bug).
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Point(Bytes::from_static(b"x"))),
            None
        );
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Point(Bytes::from_static(b""))),
            None
        );
    }
}
