// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use base64::Engine;
use bepository_bep::ids::{FolderId, FolderLabel};
use bepository_lock::base32::to_base32;
use secrecy::{ExposeSecret, SecretSlice, SecretString};
use serde::{Deserialize, Serialize};

/// Persistent metadata stored as `bepository-{epoch}.toml` in the object store.
///
/// The epoch in the filename corresponds to the distributed lock epoch that
/// last wrote the file. `clean_meta` deletes files from prior epochs.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Meta {
    /// On-disk format version of this meta file. Used as a forward fence: an
    /// instance refuses to activate a store written by a newer format it does
    /// not understand (see [`SUPPORTED_FORMAT_VERSION`]). Old files lacking the
    /// field deserialize to the current version via [`default_format_version`].
    #[serde(default = "default_format_version")]
    pub format_version: u32,

    /// Next folder ID to allocate. Monotonically increasing to prevent reuse.
    #[serde(default)]
    pub next_folder_key: u64,

    /// TLS device identity. Absent until `init` runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<MetaIdentity>,

    /// Registered folders, keyed by base32 ID (e.g. `"00000000"`).
    /// Each folder's SlateDB directory is `folder_<key>`.
    #[serde(default)]
    pub folders: BTreeMap<String, FolderEntry>,

    /// Checkpoint schedules, keyed by interval (e.g. 1h, 1d).
    /// Each entry specifies a TTL for checkpoints created on that interval.
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        with = "checkpoint_map"
    )]
    pub checkpoint: BTreeMap<Duration, CheckpointSchedule>,
}

mod checkpoint_map {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(m: &BTreeMap<Duration, CheckpointSchedule>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let m: BTreeMap<String, &CheckpointSchedule> = m
            .iter()
            .map(|(k, v)| (humantime::format_duration(*k).to_string(), v))
            .collect();
        m.serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<BTreeMap<Duration, CheckpointSchedule>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let m: BTreeMap<String, CheckpointSchedule> = BTreeMap::deserialize(d)?;
        let mut res = BTreeMap::new();
        for (k, v) in m {
            let dur = humantime::parse_duration(&k).map_err(serde::de::Error::custom)?;
            res.insert(dur, v);
        }
        Ok(res)
    }
}

/// A registered folder in the metadata.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FolderEntry {
    /// BEP folder ID — stable cross-peer identifier used for all protocol matching.
    pub id: FolderId,
    /// BEP folder label — human-readable display name, updated from the master's ClusterConfig.
    pub label: FolderLabel,
}

/// A checkpoint schedule entry: how long to keep checkpoints created at this interval.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CheckpointSchedule {
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

/// The format version this instance reads and writes. An instance refuses to
/// activate a store whose meta reports a strictly greater version — auto-update
/// makes version skew routine, and writing back with an old binary would lose
/// information the newer format encoded.
pub const SUPPORTED_FORMAT_VERSION: u32 = 1;

/// Serde default for [`Meta::format_version`]: old meta files written before
/// the field existed are, by definition, format version 1. This is pinned to a
/// literal rather than [`SUPPORTED_FORMAT_VERSION`] on purpose — if the
/// constant ever bumps to 2, legacy files must still read as 1, not silently
/// become "current" (a version-2 reader would then misread genuinely-old data).
/// [`Meta::default`] (fresh stores) uses the constant; this default is for
/// deserialization of old files only.
#[must_use]
pub fn default_format_version() -> u32 {
    1
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            format_version: SUPPORTED_FORMAT_VERSION,
            next_folder_key: 0,
            identity: None,
            folders: BTreeMap::new(),
            checkpoint: BTreeMap::new(),
        }
    }
}

impl Meta {
    /// Check whether a folder with the given BEP folder ID is registered.
    #[must_use]
    pub fn has_folder(&self, id: FolderId) -> bool {
        self.folders.values().any(|e| e.id == id)
    }

    /// Return all registered BEP folder IDs.
    #[must_use]
    pub fn folder_ids(&self) -> Vec<FolderId> {
        self.folders.values().map(|e| e.id).collect()
    }

    /// Default checkpoint schedules written on first `init`.
    #[must_use]
    pub fn default_checkpoints() -> BTreeMap<Duration, CheckpointSchedule> {
        [
            (
                Duration::from_secs(3600),
                CheckpointSchedule {
                    ttl: Duration::from_secs(24 * 3600),
                },
            ),
            (
                Duration::from_secs(24 * 3600),
                CheckpointSchedule {
                    ttl: Duration::from_secs(7 * 24 * 3600),
                },
            ),
        ]
        .into()
    }
}

/// Format a numeric folder ID as a base32 key for the TOML map.
#[must_use]
pub fn folder_key(id: u64) -> String {
    to_base32(id).expect("folder id exceeds 40-bit base32 limit")
}

/// TLS identity stored as base64-encoded DER.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetaIdentity {
    pub cert_der: String,
    #[serde(serialize_with = "serialize_secret_string")]
    pub key_der: SecretString,
}

impl MetaIdentity {
    #[must_use]
    pub fn from_der(cert: &[u8], key: &SecretSlice<u8>) -> Self {
        let engine = base64::engine::general_purpose::STANDARD;
        Self {
            cert_der: engine.encode(cert),
            key_der: SecretString::from(engine.encode(key.expose_secret())),
        }
    }

    pub fn cert_der_bytes(&self) -> Result<Vec<u8>, base64::DecodeError> {
        base64::engine::general_purpose::STANDARD.decode(&self.cert_der)
    }

    pub fn key_der_bytes(&self) -> Result<SecretSlice<u8>, base64::DecodeError> {
        base64::engine::general_purpose::STANDARD
            .decode(self.key_der.expose_secret())
            .map(SecretSlice::from)
    }
}

fn serialize_secret_string<S>(value: &SecretString, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(value.expose_secret())
}

/// Filename prefix for meta files.
pub const META_PREFIX: &str = "bepository-";
/// Filename suffix for meta files.
pub const META_SUFFIX: &str = ".toml";
