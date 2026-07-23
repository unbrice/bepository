// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Wire-level validation of incoming `FileInfo` entries.

use crate::error::BepError;
use crate::proto::bep::FileInfo;

/// Maximum legitimate block size: BEP block sizes are powers of two in
/// [128 KiB, 16 MiB], so no block can exceed 16 MiB.
const MAX_BLOCK_SIZE: i32 = 16 * 1024 * 1024;

/// Validate a peer-supplied [`FileInfo`] against BEP block-list semantics:
/// non-negative file size, and blocks with non-negative offsets, sizes in
/// (0, 16 MiB], and SHA-256 hashes, forming a contiguous list from offset 0
/// that covers exactly `size` bytes.
///
/// A non-empty `size` with an empty block list is allowed: symlinks carry
/// their target length in `size` and have no blocks.
pub fn validate_file_info(file: &FileInfo) -> Result<(), BepError> {
    if file.size < 0 {
        return Err(BepError::PeerBadMessage(format!(
            "negative file size in {:?}: {}",
            file.name, file.size
        )));
    }
    let mut expected_offset = 0i64;
    for block in &file.blocks {
        if block.size <= 0 {
            return Err(BepError::PeerBadMessage(format!(
                "non-positive block size in {:?}: {}",
                file.name, block.size
            )));
        }
        if block.size > MAX_BLOCK_SIZE {
            return Err(BepError::PeerBadMessage(format!(
                "block size {} exceeds 16 MiB maximum in {:?}",
                block.size, file.name
            )));
        }
        if block.hash.len() != 32 {
            return Err(BepError::PeerBadMessage(format!(
                "block hash is {} bytes, not 32, in {:?}",
                block.hash.len(),
                file.name
            )));
        }
        if block.offset != expected_offset {
            return Err(BepError::PeerBadMessage(format!(
                "non-contiguous block list in {:?}: block at offset {}, expected {}",
                file.name, block.offset, expected_offset
            )));
        }
        expected_offset = expected_offset
            .checked_add(i64::from(block.size))
            .ok_or_else(|| {
                BepError::PeerBadMessage(format!(
                    "block list size overflows i64 in {:?}",
                    file.name
                ))
            })?;
    }
    if !file.blocks.is_empty() && expected_offset != file.size {
        return Err(BepError::PeerBadMessage(format!(
            "block list covers {} bytes but file size is {} in {:?}",
            expected_offset, file.size, file.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::bep::BlockInfo;

    fn block(offset: i64, size: i32) -> BlockInfo {
        BlockInfo {
            offset,
            size,
            hash: vec![0; 32],
            ..Default::default()
        }
    }

    fn file(blocks: Vec<BlockInfo>, size: i64) -> FileInfo {
        FileInfo {
            name: "a.txt".into(),
            size,
            blocks,
            ..Default::default()
        }
    }

    #[test]
    fn validate_file_info_accepts_valid_and_rejects_malformed() {
        assert!(validate_file_info(&file(vec![block(0, 128), block(128, 64)], 192)).is_ok());
        // Empty file, and symlink-like (size with no blocks).
        assert!(validate_file_info(&file(vec![], 0)).is_ok());
        assert!(validate_file_info(&file(vec![], 42)).is_ok());
        // A block at exactly the 16 MiB maximum is fine.
        assert!(
            validate_file_info(&file(
                vec![block(0, MAX_BLOCK_SIZE)],
                i64::from(MAX_BLOCK_SIZE)
            ))
            .is_ok()
        );

        let cases = [
            file(vec![], -1),                // negative file size
            file(vec![block(-1, 128)], 128), // negative offset
            file(vec![block(0, 0)], 0),      // zero block size
            file(vec![block(0, -1)], 0),     // negative block size
            file(
                vec![block(0, MAX_BLOCK_SIZE + 1)],
                i64::from(MAX_BLOCK_SIZE) + 1,
            ), // oversized block
            file(vec![block(1, 127)], 127),  // first block not at 0
            file(vec![block(0, 128), block(256, 64)], 192), // gap
            file(vec![block(0, 128), block(64, 128)], 192), // overlap
            file(vec![block(0, 128)], 129),  // coverage mismatch
        ];
        for case in cases {
            assert!(
                matches!(validate_file_info(&case), Err(BepError::PeerBadMessage(_))),
                "should reject: {case:?}"
            );
        }

        let mut bad_hash = block(0, 128);
        bad_hash.hash = vec![0; 16];
        assert!(validate_file_info(&file(vec![bad_hash], 128)).is_err());
    }
}
