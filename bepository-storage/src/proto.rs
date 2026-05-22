// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod storage {
    include!(concat!(env!("OUT_DIR"), "/storage.rs"));
}

use bepository_bep::error::StorageError;
use bepository_bep::proto::bep;

// --- FileInfoType mapping ---
//
// storage::FileInfoType has UNSPECIFIED=0 as its zero value, so its numeric
// values differ from bep::FileInfoType (FILE=0).  Conversions must use
// explicit enum matching rather than a raw i32 copy.

#[allow(deprecated)]
fn bep_type_to_storage(bep_type: i32) -> i32 {
    use bep::FileInfoType as B;
    use storage::FileInfoType as S;
    (match B::try_from(bep_type).unwrap_or(B::File) {
        B::File => S::File,
        B::Directory => S::Directory,
        B::SymlinkFile => S::Symlink,
        B::SymlinkDirectory => S::Symlink,
        B::Symlink => S::Symlink,
    }) as i32
}

fn storage_type_to_bep(stor_type: i32) -> Result<i32, StorageError> {
    use bep::FileInfoType as B;
    use storage::FileInfoType as S;
    let variant = S::try_from(stor_type).map_err(|_| {
        StorageError::Corruption(format!("invalid storage FileInfoType value {stor_type}"))
    })?;
    match variant {
        S::Unspecified => Err(StorageError::Corruption(
            "storage FileInfoType is UNSPECIFIED".into(),
        )),
        S::File => Ok(B::File as i32),
        S::Directory => Ok(B::Directory as i32),
        S::Symlink => Ok(B::Symlink as i32),
    }
}

// --- FileInfo ---

impl From<bep::FileInfo> for storage::FileInfo {
    fn from(b: bep::FileInfo) -> Self {
        Self {
            name: b.name,
            size: b.size,
            modified_s: b.modified_s,
            modified_by: b.modified_by,
            version: b.version.map(Into::into),
            sequence: b.sequence,
            blocks: b.blocks.into_iter().map(Into::into).collect(),
            symlink_target: b.symlink_target,
            blocks_hash: b.blocks_hash,
            previous_blocks_hash: b.previous_blocks_hash,
            encrypted: b.encrypted,
            r#type: bep_type_to_storage(b.r#type),
            permissions: b.permissions,
            modified_ns: b.modified_ns,
            block_size: b.block_size,
            platform: b.platform.map(Into::into),
            local_flags: b.local_flags,
            version_hash: b.version_hash,
            inode_change_ns: b.inode_change_ns,
            encryption_trailer_size: b.encryption_trailer_size,
            deleted: b.deleted,
            invalid: b.invalid,
            no_permissions: b.no_permissions,
        }
    }
}

impl TryFrom<storage::FileInfo> for bep::FileInfo {
    type Error = StorageError;

    fn try_from(s: storage::FileInfo) -> Result<Self, StorageError> {
        Ok(Self {
            name: s.name,
            size: s.size,
            modified_s: s.modified_s,
            modified_by: s.modified_by,
            version: s.version.map(Into::into),
            sequence: s.sequence,
            blocks: s.blocks.into_iter().map(Into::into).collect(),
            symlink_target: s.symlink_target,
            blocks_hash: s.blocks_hash,
            previous_blocks_hash: s.previous_blocks_hash,
            encrypted: s.encrypted,
            r#type: storage_type_to_bep(s.r#type)?,
            permissions: s.permissions,
            modified_ns: s.modified_ns,
            block_size: s.block_size,
            platform: s.platform.map(Into::into),
            local_flags: s.local_flags,
            version_hash: s.version_hash,
            inode_change_ns: s.inode_change_ns,
            encryption_trailer_size: s.encryption_trailer_size,
            deleted: s.deleted,
            invalid: s.invalid,
            no_permissions: s.no_permissions,
        })
    }
}

// --- Vector / Counter ---

impl From<bep::Vector> for storage::Vector {
    fn from(b: bep::Vector) -> Self {
        Self {
            counters: b.counters.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<storage::Vector> for bep::Vector {
    fn from(s: storage::Vector) -> Self {
        Self {
            counters: s.counters.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<bep::Counter> for storage::Counter {
    fn from(b: bep::Counter) -> Self {
        Self {
            id: b.id,
            value: b.value,
        }
    }
}

impl From<storage::Counter> for bep::Counter {
    fn from(s: storage::Counter) -> Self {
        Self {
            id: s.id,
            value: s.value,
        }
    }
}

// --- BlockInfo ---

impl From<bep::BlockInfo> for storage::BlockInfo {
    fn from(b: bep::BlockInfo) -> Self {
        Self {
            hash: b.hash,
            offset: b.offset,
            size: b.size,
            blockseq: None,
            inline_data: None,
        }
    }
}

impl From<storage::BlockInfo> for bep::BlockInfo {
    fn from(s: storage::BlockInfo) -> Self {
        Self {
            hash: s.hash,
            offset: s.offset,
            size: s.size,
        }
    }
}

// --- PlatformData and friends ---

impl From<bep::PlatformData> for storage::PlatformData {
    fn from(b: bep::PlatformData) -> Self {
        Self {
            unix: b.unix.map(Into::into),
            windows: b.windows.map(Into::into),
            linux: b.linux.map(Into::into),
            darwin: b.darwin.map(Into::into),
            freebsd: b.freebsd.map(Into::into),
            netbsd: b.netbsd.map(Into::into),
        }
    }
}

impl From<storage::PlatformData> for bep::PlatformData {
    fn from(s: storage::PlatformData) -> Self {
        Self {
            unix: s.unix.map(Into::into),
            windows: s.windows.map(Into::into),
            linux: s.linux.map(Into::into),
            darwin: s.darwin.map(Into::into),
            freebsd: s.freebsd.map(Into::into),
            netbsd: s.netbsd.map(Into::into),
        }
    }
}

impl From<bep::UnixData> for storage::UnixData {
    fn from(b: bep::UnixData) -> Self {
        Self {
            owner_name: b.owner_name,
            group_name: b.group_name,
            uid: b.uid,
            gid: b.gid,
        }
    }
}

impl From<storage::UnixData> for bep::UnixData {
    fn from(s: storage::UnixData) -> Self {
        Self {
            owner_name: s.owner_name,
            group_name: s.group_name,
            uid: s.uid,
            gid: s.gid,
        }
    }
}

impl From<bep::WindowsData> for storage::WindowsData {
    fn from(b: bep::WindowsData) -> Self {
        Self {
            owner_name: b.owner_name,
            owner_is_group: b.owner_is_group,
        }
    }
}

impl From<storage::WindowsData> for bep::WindowsData {
    fn from(s: storage::WindowsData) -> Self {
        Self {
            owner_name: s.owner_name,
            owner_is_group: s.owner_is_group,
        }
    }
}

impl From<bep::XattrData> for storage::XattrData {
    fn from(b: bep::XattrData) -> Self {
        Self {
            xattrs: b.xattrs.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<storage::XattrData> for bep::XattrData {
    fn from(s: storage::XattrData) -> Self {
        Self {
            xattrs: s.xattrs.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<bep::Xattr> for storage::Xattr {
    fn from(b: bep::Xattr) -> Self {
        Self {
            name: b.name,
            value: b.value,
        }
    }
}

impl From<storage::Xattr> for bep::Xattr {
    fn from(s: storage::Xattr) -> Self {
        Self {
            name: s.name,
            value: s.value,
        }
    }
}
