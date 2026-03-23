// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message as _;
use slatedb::DbReader;
use uuid::Uuid;

use bepository_bep::ids::{FolderLabel, FolderLabelRef};

use crate::api::{FolderStorageKey, SlateStorage};
use crate::proto::storage::{BlockRef, File, FileInfo};
use crate::snapshot::{FsEntry, SnapshotError, SnapshotFs, SnapshotRef};
use crate::store_keys;

#[derive(Clone, Debug)]
pub struct SlateSnapshotRef {
    pub folder_label: FolderLabel,
    pub create_time: chrono::DateTime<chrono::Utc>,
    pub(crate) folder_sk: FolderStorageKey,
    pub(crate) id: Uuid,
}

impl SnapshotRef for SlateSnapshotRef {
    fn folder_label(&self) -> &FolderLabelRef {
        &self.folder_label
    }
    fn create_time(&self) -> chrono::DateTime<chrono::Utc> {
        self.create_time
    }
}

#[async_trait]
impl SnapshotFs for SlateStorage {
    type Ref = SlateSnapshotRef;

    async fn list_snapshots(&self) -> Result<Vec<SlateSnapshotRef>, SnapshotError> {
        let (_schedules, folder_checkpoints) = self
            .list_checkpoints_unlocked()
            .await
            .map_err(snap_io("list checkpoints"))?;

        let mut snapshots = Vec::new();
        for (label, sk, checkpoints) in folder_checkpoints {
            let mut folder_snaps: Vec<SlateSnapshotRef> = checkpoints
                .into_iter()
                .map(|cp| SlateSnapshotRef {
                    folder_label: label.clone(),
                    folder_sk: sk.clone(),
                    create_time: cp.create_time,
                    id: cp.id,
                })
                .collect();
            // Newest first within each folder.
            folder_snaps.sort_by_key(|snap| std::cmp::Reverse(snap.create_time));
            snapshots.extend(folder_snaps);
        }
        Ok(snapshots)
    }

    async fn read_dir(
        &self,
        snap: &SlateSnapshotRef,
        path: &str,
    ) -> Result<Vec<FsEntry>, SnapshotError> {
        let reader = self.snapshot_reader(&snap.folder_sk, snap.id).await?;
        let prefix = read_dir_prefix(path);

        let mut iter = reader
            .scan_prefix(&prefix)
            .await
            .map_err(snap_io("scan snapshot prefix"))?;

        let mut seen_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut entries: Vec<FsEntry> = Vec::new();

        while let Some(kv) = iter.next().await.map_err(snap_io("iterate snapshot"))? {
            let Some(full_name) = store_keys::parse_file_key(&kv.key) else {
                continue;
            };
            let fi = match decode_live_file_info(kv.value) {
                Ok(Some(fi)) => fi,
                _ => continue,
            };
            let Some(relative) = relative_under(path, &full_name) else {
                continue;
            };

            match relative.split_once('/') {
                Some((dir_part, _)) => {
                    if seen_dirs.insert(dir_part.to_owned()) {
                        entries.push(FsEntry::Dir {
                            name: dir_part.to_owned(),
                        });
                    }
                }
                None => {
                    entries.push(fs_entry_from_file_info(relative, &fi)?);
                }
            }
        }

        Ok(entries)
    }

    async fn file_metadata(
        &self,
        snap: &SlateSnapshotRef,
        path: &str,
    ) -> Result<FsEntry, SnapshotError> {
        let reader = self.snapshot_reader(&snap.folder_sk, snap.id).await?;

        // Try as a file first.
        let key = store_keys::file_key(path);
        if let Some(raw) = reader
            .get(&key)
            .await
            .map_err(snap_io("read snapshot key"))?
        {
            match decode_live_file_info(raw)? {
                Some(fi) => {
                    let name = path.rsplit('/').next().unwrap_or(path).to_owned();
                    return fs_entry_from_file_info(name, &fi);
                }
                None => return Err(SnapshotError::NotFound),
            }
        }

        // Check if it's a virtual directory: any file exists under path/.
        let dir_prefix: Vec<u8> = [store_keys::FILE_PREFIX, path.as_bytes(), b"/"].concat();
        let mut iter = reader
            .scan_prefix(&dir_prefix)
            .await
            .map_err(snap_io("scan snapshot prefix"))?;
        if iter
            .next()
            .await
            .map_err(snap_io("iterate snapshot"))?
            .is_some()
        {
            let name = path.rsplit('/').next().unwrap_or(path).to_owned();
            return Ok(FsEntry::Dir { name });
        }

        Err(SnapshotError::NotFound)
    }

    async fn read_bytes(
        &self,
        snap: &SlateSnapshotRef,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Bytes, SnapshotError> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        let reader = self.snapshot_reader(&snap.folder_sk, snap.id).await?;
        let key = store_keys::file_key(path);
        let raw = reader
            .get(&key)
            .await
            .map_err(snap_io("read snapshot key"))?
            .ok_or(SnapshotError::NotFound)?;

        let fi = decode_live_file_info(raw)?.ok_or(SnapshotError::NotFound)?;
        if fi.size == 0 {
            return Ok(Bytes::new());
        }

        let dir = store_keys::dirname(path);
        copy_range_from_blocks(&reader, dir, &fi, offset, len).await
    }
}

/// Build the SlateDB key prefix to scan when listing `path` inside a snapshot.
/// Empty `path` lists the folder root.
fn read_dir_prefix(path: &str) -> Vec<u8> {
    if path.is_empty() {
        store_keys::FILE_PREFIX.to_vec()
    } else {
        [store_keys::FILE_PREFIX, path.as_bytes(), b"/"].concat()
    }
}

/// Strip the directory prefix from a file's full name to get the path relative
/// to the requested directory. Returns `None` when the entry is not actually
/// inside `dir` (defensive — should not happen given the scan prefix).
fn relative_under(dir: &str, full_name: &str) -> Option<String> {
    if dir.is_empty() {
        Some(full_name.to_owned())
    } else {
        full_name
            .strip_prefix(dir)
            .and_then(|s| s.strip_prefix('/'))
            .map(str::to_owned)
    }
}

/// Copy the byte range `[offset, offset+len)` of `fi`'s blocks into `out`,
/// reading each block via `read_block_from_reader`. Stops once `len` bytes
/// have been written. Caller is responsible for the `len == 0` and `fi.size == 0`
/// fast paths.
async fn copy_range_from_blocks(
    reader: &DbReader,
    dir: &str,
    fi: &FileInfo,
    offset: u64,
    len: usize,
) -> Result<Bytes, SnapshotError> {
    use bytes::BytesMut;
    let end_offset = offset.saturating_add(len as u64);
    let mut out = BytesMut::with_capacity(len);
    let mut remaining = len;

    for block in &fi.blocks {
        let blk_start = u64::try_from(block.offset)
            .map_err(|_| SnapshotError::Io("negative block offset".into()))?;
        let blk_size_u64 = u64::try_from(block.size)
            .map_err(|_| SnapshotError::Io("negative block size".into()))?;
        let blk_end = blk_start.saturating_add(blk_size_u64);

        if blk_end <= offset {
            continue;
        }
        if blk_start >= end_offset {
            break;
        }

        let skip = usize::try_from(offset.saturating_sub(blk_start))
            .map_err(|_| SnapshotError::Io("skip offset exceeds usize max".into()))?;
        let blk_size = usize::try_from(blk_size_u64)
            .map_err(|_| SnapshotError::Io("block size exceeds usize max".into()))?;
        let take = blk_size.saturating_sub(skip).min(remaining);

        let hash: &[u8; store_keys::HASH_LEN] = block
            .hash
            .as_slice()
            .try_into()
            .map_err(|_| SnapshotError::Io("invalid block hash length".into()))?;

        let data = read_block_from_reader(reader, dir, hash).await?;
        out.extend_from_slice(&data[skip..skip + take]);
        remaining -= take;
        if remaining == 0 {
            break;
        }
    }
    Ok(out.freeze())
}

/// Wrap an arbitrary error with a static context label as a `SnapshotError::Io`.
/// Use this for I/O and decode failures originating from external types
/// (`DbReader`, `prost`, `slatedb`).
fn snap_io<E: std::fmt::Display>(ctx: &'static str) -> impl Fn(E) -> SnapshotError {
    move |e| SnapshotError::Io(format!("{ctx}: {e}"))
}

/// Read block data from a `DbReader`, following cross-directory dedup refs.
async fn read_block_from_reader(
    reader: &DbReader,
    dir: &str,
    hash: &[u8; store_keys::HASH_LEN],
) -> Result<Bytes, SnapshotError> {
    // Try direct block data.
    let data_key = store_keys::block_data_key(dir, hash);
    if let Some(data) = reader
        .get(&data_key)
        .await
        .map_err(snap_io("read snapshot key"))?
    {
        return Ok(data);
    }

    // Try dedup reference: b/<dir>/<hash>/ref → BlockRef { source_dir }.
    let ref_key = store_keys::block_ref_key(dir, hash);
    if let Some(ref_bytes) = reader
        .get(&ref_key)
        .await
        .map_err(snap_io("read snapshot key"))?
    {
        let block_ref = BlockRef::decode(ref_bytes).map_err(snap_io("decode block ref"))?;
        let canonical_key = store_keys::block_data_key(&block_ref.source_dir, hash);
        if let Some(data) = reader
            .get(&canonical_key)
            .await
            .map_err(snap_io("read snapshot key"))?
        {
            return Ok(data);
        }
    }

    Err(SnapshotError::Io(format!(
        "block not found: {}",
        hex::encode(hash)
    )))
}

/// Decode a `File` proto and return its `FileInfo` if the entry is live (not deleted).
/// Returns `Ok(None)` when the entry is deleted or `file_info` is missing — callers
/// that require a present entry must convert the `None` to `SnapshotError::NotFound`.
fn decode_live_file_info(raw: Bytes) -> Result<Option<FileInfo>, SnapshotError> {
    let file = File::decode(raw).map_err(snap_io("decode file proto"))?;
    Ok(file.file_info.filter(|fi| !fi.deleted))
}

/// Build an `FsEntry::File` from a decoded `FileInfo` and the basename to expose.
fn fs_entry_from_file_info(name: String, fi: &FileInfo) -> Result<FsEntry, SnapshotError> {
    let modified = chrono::DateTime::from_timestamp(
        fi.modified_s,
        u32::try_from(fi.modified_ns.max(0))
            .map_err(|_| SnapshotError::Io("invalid timestamp in file info".into()))?,
    )
    .ok_or_else(|| SnapshotError::Io("invalid timestamp in file info".into()))?;
    let size =
        u64::try_from(fi.size).map_err(|_| SnapshotError::Io("negative file size".into()))?;
    Ok(FsEntry::File {
        name,
        size,
        modified,
    })
}
