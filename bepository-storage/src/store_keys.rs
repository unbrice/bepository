// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Key encoding/decoding for the per-folder SlateDB key layout.
//!
//! Key prefixes:
//!   n/<dir>//<basename>              — primary file index (FileInfo)
//!   s/<seq_be8>                       — sequence → name mapping
//!   b/<dir>/<hash_32>                 — block data (dir-scoped)
//!   b/<dir>/<hash_32>/ref             — block reference (cross-dir dedup)
//!   br/<hash_32>/<dir>//<basename>    — reverse block refs
//!   ix                                — our FolderIndexMeta
//!   dx/<devid_32>                     — remote device index state
//!   in/<epoch_base32>/<dir>//<basename> — inbox staging area

use bepository_bep::error::StorageError;

/// Hash length in bytes (SHA-256).
pub const HASH_LEN: usize = 32;

/// Device ID length in bytes.
pub const DEVID_LEN: usize = 32;

// --- Prefix constants ---

pub const FILE_PREFIX: &[u8] = b"n/";
pub const SEQ_PREFIX: &[u8] = b"s/";
pub const BLOCK_PREFIX: &[u8] = b"b/";
pub const BLOCK_REV_PREFIX: &[u8] = b"br/";
pub const IX_KEY: &[u8] = b"ix";
pub const DEVICE_PREFIX: &[u8] = b"dx/";
pub const INBOX_PREFIX: &[u8] = b"in/";

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

// --- File key: n/<dir>//<basename> ---

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
    let (dir, basename) = s.split_once("//")?;
    Some(join_name(dir, basename))
}

// --- Sequence key: s/<seq_be8> ---

/// Sequence key length: 2 (prefix) + 8 (big-endian u64) = 10.
pub const SEQ_KEY_LEN: usize = 10;

pub fn seq_key(seq: i64) -> Result<[u8; SEQ_KEY_LEN], StorageError> {
    let seq_u = u64::try_from(seq).map_err(|_| {
        StorageError::Internal(format!("sequence numbers must be non-negative, got {seq}"))
    })?;
    let mut key = [0u8; SEQ_KEY_LEN];
    key[0] = b's';
    key[1] = b'/';
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

/// The upper-bound key for scanning all `s/` entries.
/// "t" > "s/" lexicographically.
pub const SEQ_SCAN_END: &[u8] = b"t";

/// Build a scan start key for sequences > `since`.
pub fn seq_scan_start(since: i64) -> Result<[u8; SEQ_KEY_LEN], StorageError> {
    seq_key(since + 1)
}

// --- Block data key: b/<dir>/<hash_32> ---

#[must_use]
pub fn block_data_key(dir: &str, hash: &[u8; HASH_LEN]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + dir.len() + 1 + HASH_LEN);
    key.extend_from_slice(BLOCK_PREFIX);
    key.extend_from_slice(dir.as_bytes());
    key.push(b'/');
    key.extend_from_slice(hash);
    key
}

// --- Block ref key: b/<dir>/<hash_32>/ref ---

#[must_use]
pub fn block_ref_key(dir: &str, hash: &[u8; HASH_LEN]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + dir.len() + 1 + HASH_LEN + 4);
    key.extend_from_slice(BLOCK_PREFIX);
    key.extend_from_slice(dir.as_bytes());
    key.push(b'/');
    key.extend_from_slice(hash);
    key.extend_from_slice(b"/ref");
    key
}

// --- Block reverse ref key: br/<hash_32>/<dir>//<basename> ---

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
    let (dir, basename) = s.split_once("//")?;
    Some((hash, join_name(dir, basename)))
}

// --- Inbox key: in/<epoch_base32>/<dir>//<basename> ---
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
    let (dir, basename) = s.split_once("//")?;
    Some((epoch, join_name(dir, basename)))
}

// --- Device key: dx/<devid_32> ---

/// Device state key length: 3 (prefix) + 32 (device ID) = 35.
pub const DEVICE_KEY_LEN: usize = 35;

#[must_use]
pub fn device_key(devid: &[u8; DEVID_LEN]) -> [u8; DEVICE_KEY_LEN] {
    let mut key = [0u8; DEVICE_KEY_LEN];
    key[0] = b'd';
    key[1] = b'x';
    key[2] = b'/';
    key[3..].copy_from_slice(devid);
    key
}

/// Parse a block data key `b/<dir>/<hash_32>`, returning the 32-byte hash.
///
/// Matches keys that start with `b/` and end with exactly 32 raw bytes after
/// a `/`, but do NOT end with `/ref`.
#[must_use]
pub fn parse_block_data_key(key: &[u8]) -> Option<[u8; HASH_LEN]> {
    // Minimum: b/ + / + HASH_LEN
    if key.len() < BLOCK_PREFIX.len() + 1 + HASH_LEN
        || !key.starts_with(BLOCK_PREFIX)
        || key.ends_with(b"/ref")
    {
        return None;
    }
    if key[key.len() - HASH_LEN - 1] != b'/' {
        return None;
    }
    key[key.len() - HASH_LEN..].try_into().ok()
}

/// Parse a block ref key `b/<dir>/<hash_32>/ref`, returning the 32-byte hash.
#[must_use]
pub fn parse_block_ref_key(key: &[u8]) -> Option<[u8; HASH_LEN]> {
    // Minimum: b/ + / + HASH_LEN + /ref
    if key.len() < BLOCK_PREFIX.len() + 1 + HASH_LEN + 4
        || !key.starts_with(BLOCK_PREFIX)
        || !key.ends_with(b"/ref")
    {
        return None;
    }
    let hash_end = key.len() - 4;
    // Verify the character before the hash is /
    if key[hash_end - HASH_LEN - 1] != b'/' {
        return None;
    }
    key[hash_end - HASH_LEN..hash_end].try_into().ok()
}

/// Extract the directory from a file key `n/<dir>//<basename>` (borrowing).
#[must_use]
pub fn file_key_dir(key: &[u8]) -> Option<&str> {
    let rest = key.strip_prefix(FILE_PREFIX)?;
    let s = std::str::from_utf8(rest).ok()?;
    let (dir, _) = s.split_once("//")?;
    Some(dir)
}

/// Extract the directory from a block data or ref key
/// (`b/<dir>/<hash_32>` or `b/<dir>/<hash_32>/ref`) (borrowing).
#[must_use]
pub fn block_key_dir(key: &[u8]) -> Option<&str> {
    if !key.starts_with(BLOCK_PREFIX) {
        return None;
    }
    let suffix_len = if key.ends_with(b"/ref") {
        1 + HASH_LEN + 4 // /<hash>/ref
    } else {
        1 + HASH_LEN // /<hash>
    };
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
    fn block_data_key_round_trip() {
        let hash = [0xBB; HASH_LEN];
        let key = block_data_key("docs", &hash);
        let parsed = parse_block_data_key(&key).unwrap();
        assert_eq!(parsed, hash);
    }

    #[test]
    fn block_data_key_root_round_trip() {
        let hash = [0xDD; HASH_LEN];
        let key = block_data_key("", &hash);
        let parsed = parse_block_data_key(&key).unwrap();
        assert_eq!(parsed, hash);
    }

    #[test]
    fn block_data_key_rejects_ref_key() {
        let hash = [0xBB; HASH_LEN];
        let key = block_ref_key("docs", &hash);
        assert!(parse_block_data_key(&key).is_none());
    }

    #[test]
    fn block_ref_key_round_trip() {
        let hash = [0xCC; HASH_LEN];
        let key = block_ref_key("photos", &hash);
        let parsed = parse_block_ref_key(&key).unwrap();
        assert_eq!(parsed, hash);
    }

    #[test]
    fn block_ref_key_rejects_data_key() {
        let hash = [0xCC; HASH_LEN];
        let key = block_data_key("photos", &hash);
        assert!(parse_block_ref_key(&key).is_none());
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
    fn block_key_dir_data() {
        let hash = [0xBB; HASH_LEN];
        let key = block_data_key("photos", &hash);
        assert_eq!(block_key_dir(&key).unwrap(), "photos");
    }

    #[test]
    fn block_key_dir_ref() {
        let hash = [0xCC; HASH_LEN];
        let key = block_ref_key("photos/2024", &hash);
        assert_eq!(block_key_dir(&key).unwrap(), "photos/2024");
    }

    #[test]
    fn block_key_dir_empty_dir() {
        let hash = [0xAA; HASH_LEN];
        let key = block_data_key("", &hash);
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
}
