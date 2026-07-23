// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The single definition of how a stored [`BlockInfo`] resolves to its bytes.
//! Shared by the BEP read path (`FolderStore::read_block`), the snapshot
//! filesystem, and fsck, so the validator can never drift from the readers.

use bytes::Bytes;

use bepository_bep::error::StorageError;

use crate::proto::storage::BlockInfo;
use crate::store::slate_err;
use crate::store_keys;

/// Read access to stored block data (`bd<seqno>` values), common to
/// `slatedb::Db` (live view) and `slatedb::DbReader` (checkpoint view).
pub(crate) trait BlockDataReader {
    async fn read_block_data(&self, seq: u64) -> Result<Option<Bytes>, StorageError>;
}

impl BlockDataReader for slatedb::Db {
    async fn read_block_data(&self, seq: u64) -> Result<Option<Bytes>, StorageError> {
        self.get(store_keys::block_data_seq_key(seq))
            .await
            .map_err(slate_err)
    }
}

impl BlockDataReader for slatedb::DbReader {
    async fn read_block_data(&self, seq: u64) -> Result<Option<Bytes>, StorageError> {
        self.get(store_keys::block_data_seq_key(seq))
            .await
            .map_err(slate_err)
    }
}

/// Resolve a block to its bytes: inline data first, then `blockseq` →
/// `bd<seqno>`. `Ok(None)` means the block is not resolvable through this
/// path (dangling or missing `blockseq`).
pub(crate) async fn resolve_block_data(
    db: &impl BlockDataReader,
    block: &BlockInfo,
) -> Result<Option<Bytes>, StorageError> {
    if let Some(inline_data) = &block.inline_data {
        return Ok(Some(Bytes::from(inline_data.clone())));
    }
    if let Some(seq) = block.blockseq {
        store_keys::validate_block_seq(seq)?;
        if let Some(data) = db.read_block_data(seq).await? {
            return Ok(Some(data));
        }
    }
    Ok(None)
}
