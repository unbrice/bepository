// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

pub mod base32;
pub mod epoch;
pub mod guard;

pub use epoch::Epoch;
pub use guard::{LockGuard, LockLost};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

use crate::base32::Base32Error;

fn create_opts() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EpochFile {
    pub holder: String,
    pub priority: u32,
    pub duration: u64,
}

/// A parsed epoch file together with its object-store metadata.
pub struct EpochEntry {
    pub meta: object_store::ObjectMeta,
    pub file: EpochFile,
    pub epoch: Epoch,
}

impl EpochEntry {
    pub fn expires_at(&self) -> DateTime<Utc> {
        let dur_secs = self.file.duration.try_into().unwrap_or(i64::MAX);
        self.meta
            .last_modified
            .checked_add_signed(ChronoDuration::seconds(dur_secs))
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    pub fn is_expired_at(&self, t: DateTime<Utc>) -> bool {
        self.expires_at() <= t
    }

    pub fn is_owned_by(&self, holder: &str) -> bool {
        self.file.holder == holder
    }
}

/// Split a sorted-ascending slice of epoch metas into (lower, mine_present, higher).
/// Metas whose filename does not parse as `Epoch` are dropped (defensive; callers
/// pass output of `list_epoch_metas` which already filters).
fn partition_around(
    metas: Vec<object_store::ObjectMeta>,
    mine: Epoch,
) -> (
    Vec<object_store::ObjectMeta>,
    bool,
    Vec<object_store::ObjectMeta>,
) {
    let mut lower = Vec::new();
    let mut higher = Vec::new();
    let mut mine_present = false;
    for m in metas {
        match m.location.filename().and_then(Epoch::parse) {
            Some(e) if e < mine => lower.push(m),
            Some(e) if e == mine => mine_present = true,
            Some(_) => higher.push(m),
            None => {}
        }
    }
    (lower, mine_present, higher)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquisitionStatus {
    Owner(Epoch),
    Queued,
    QueuedBackwardClock,
    Yielded,
    Failed,
    NotOwner,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Clock regression detected")]
    ClockRegression,
    #[error("Operation cancelled")]
    Cancelled,
    #[error(transparent)]
    Base32(#[from] Base32Error),
}

pub struct Lock<'a, T: ObjectStore + ?Sized> {
    store: &'a T,
    pub(crate) prefix: Path,
    pub(crate) holder: String,
    pub(crate) priority: u32,
    pub(crate) duration: u64,
}

impl<'a, T: ObjectStore + ?Sized> Lock<'a, T> {
    pub fn new(store: &'a T, prefix: Path, holder: String, priority: u32, duration: u64) -> Self {
        Self {
            store,
            prefix,
            holder,
            priority,
            duration,
        }
    }

    /// Best-effort delete; logs nothing, swallows errors. Use only for cleanup
    /// where the caller cannot meaningfully handle a failed delete.
    async fn best_effort_delete(&self, path: &Path) {
        let result = self.store.delete(path).await;
        if let Err(err) = result {
            tracing::warn!("Failed to delete lock file: {}", err);
        }
    }

    /// LIST the prefix and return all metas whose filename parses as an epoch,
    /// sorted ascending by epoch (== ascending by filename, since the base32
    /// alphabet is lex-sorted).
    async fn list_epoch_metas(&self) -> Result<Vec<object_store::ObjectMeta>, Error> {
        let mut metas: Vec<_> = self
            .store
            .list(Some(&self.prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(Error::from)?
            .into_iter()
            .filter(|m| m.location.filename().and_then(Epoch::parse).is_some())
            .collect();
        metas.sort_unstable_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
        Ok(metas)
    }

    /// Fetch and parse a single epoch file. Returns `Ok(None)` if the file was
    /// deleted between LIST and GET (a benign race).
    async fn read_entry(
        &self,
        meta: object_store::ObjectMeta,
    ) -> Result<Option<EpochEntry>, Error> {
        let bytes = match self.store.get(&meta.location).await {
            Ok(resp) => resp.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let file: EpochFile = serde_json::from_slice(&bytes)?;
        // filename was already validated by list_epoch_metas, but callers may
        // pass arbitrary metas — so parse defensively.
        let epoch = match meta.location.filename().and_then(Epoch::parse) {
            Some(e) => e,
            None => return Ok(None),
        };
        Ok(Some(EpochEntry { meta, file, epoch }))
    }

    /// LIST, compute `highest + 1`, and create the corresponding epoch file via
    /// `put_if_not_exists` with the given content. Retries on AlreadyExists.
    /// Returns the claimed epoch, the store-reported creation timestamp, and
    /// the file path.
    async fn put_next_epoch(
        &self,
        content: EpochFile,
    ) -> Result<(Epoch, DateTime<Utc>, Path), Error> {
        let bytes_template = serde_json::to_vec(&content)?;
        loop {
            let highest = self
                .list_epoch_metas()
                .await?
                .iter()
                .filter_map(|m| m.location.filename().and_then(Epoch::parse))
                .max();
            let next_val = highest.map_or(0, |h| h.as_u64().checked_add(1).unwrap_or(h.as_u64()));
            let next_epoch = Epoch::new(next_val)?;
            let path = self.prefix.child(next_epoch.json_filename().as_str());

            match self
                .store
                .put_opts(&path, bytes_template.clone().into(), create_opts())
                .await
            {
                Ok(_) => {
                    let ts = match self.store.head(&path).await {
                        Ok(m) => m.last_modified,
                        Err(e) => {
                            self.best_effort_delete(&path).await;
                            return Err(e.into());
                        }
                    };
                    return Ok((next_epoch, ts, path));
                }
                Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn acquire(&self) -> Result<AcquisitionStatus, Error> {
        // Step 0: Fast-path (Optional)
        if let Some(entry) = self.current_owner().await?
            && !entry.is_owned_by(&self.holder)
        {
            let now = Utc::now();
            if entry.file.priority >= self.priority
                && now >= entry.meta.last_modified
                && now < entry.expires_at()
            {
                return Ok(AcquisitionStatus::NotOwner);
            }
        }

        // Step 1: Claim
        let (my_epoch, my_timestamp, path) = self.claim_epoch().await?;

        // Step 2 & 3: Scavenge and Verify
        self.verify_ownership(my_epoch, my_timestamp, path).await
    }

    /// Returns the current owner of the lock, if any.
    ///
    /// Returns `Ok(Some(entry))` if an epoch file exists (the one with the lowest epoch
    /// value is considered the owner). Returns `Ok(None)` if no epoch files are found,
    /// indicating the lock is currently unlocked.
    pub async fn current_owner(&self) -> Result<Option<EpochEntry>, Error> {
        for meta in self.list_epoch_metas().await? {
            if let Some(entry) = self.read_entry(meta).await? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn claim_epoch(&self) -> Result<(Epoch, DateTime<Utc>, Path), Error> {
        self.put_next_epoch(EpochFile {
            holder: self.holder.clone(),
            priority: self.priority,
            duration: self.duration,
        })
        .await
    }

    /// Step 2 of the algorithm: delete lower-named epoch files that are expired
    /// or owned by us; report whether any non-deleted lower file has a
    /// `last_modified` newer than ours (clock regression).
    async fn scavenge_below(
        &self,
        mine: Epoch,
        my_timestamp: DateTime<Utc>,
    ) -> Result<bool, Error> {
        let mut regression_detected = false;
        let metas = self.list_epoch_metas().await?;
        for meta in metas.into_iter().take_while(|m| {
            m.location
                .filename()
                .and_then(Epoch::parse)
                .is_some_and(|e| e < mine)
        }) {
            let entry = match self.read_entry(meta).await? {
                Some(e) => e,
                None => continue, // raced with delete
            };
            let is_expired = entry.is_expired_at(my_timestamp);
            let is_own = entry.is_owned_by(&self.holder);
            if is_expired || is_own {
                debug!(
                    "Scavenging {:?} (expired={} own={})",
                    entry.meta.location, is_expired, is_own
                );
                self.best_effort_delete(&entry.meta.location).await;
                continue;
            }
            if entry.meta.last_modified > my_timestamp {
                warn!("Clock regression detected for {:?}", entry.meta.location);
                regression_detected = true;
            }
        }
        Ok(regression_detected)
    }

    /// Read the first meta from `metas` that still exists. Returns `Ok(None)` if
    /// they all 404 (race vs. concurrent scavenge).
    async fn first_readable_entry(
        &self,
        metas: Vec<object_store::ObjectMeta>,
    ) -> Result<Option<EpochEntry>, Error> {
        for meta in metas {
            if let Some(entry) = self.read_entry(meta).await? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    /// Decide the loss reason when a lower-named owner outranks us. Deletes our
    /// file in the `Failed` case. Order matches the spec: regression > priority.
    async fn classify_loss(
        &self,
        owner: EpochEntry,
        regression_detected: bool,
        my_path: &Path,
    ) -> AcquisitionStatus {
        if regression_detected {
            return AcquisitionStatus::QueuedBackwardClock;
        }
        if self.priority > owner.file.priority {
            return AcquisitionStatus::Queued;
        }
        self.best_effort_delete(my_path).await;
        AcquisitionStatus::Failed
    }

    /// Step 4 (yield check): if any higher-named entry outranks us and is not
    /// expired relative to our timestamp, we must yield.
    async fn should_yield(
        &self,
        higher: Vec<object_store::ObjectMeta>,
        my_timestamp: DateTime<Utc>,
    ) -> Result<bool, Error> {
        for meta in higher {
            let entry = match self.read_entry(meta).await? {
                Some(e) => e,
                None => continue,
            };
            if entry.file.priority > self.priority && my_timestamp < entry.expires_at() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn verify_ownership(
        &self,
        my_epoch: Epoch,
        my_timestamp: DateTime<Utc>,
        my_path: Path,
    ) -> Result<AcquisitionStatus, Error> {
        // Step 2: scavenge lower-named files.
        let regression_detected = self.scavenge_below(my_epoch, my_timestamp).await?;

        // Step 3: re-LIST and partition around our own epoch.
        let (lower, mine_present, higher) =
            partition_around(self.list_epoch_metas().await?, my_epoch);

        // Eventual consistency may have hidden our file, or another participant
        // scavenged it. Delete to avoid leaking a barrier.
        if !mine_present {
            self.best_effort_delete(&my_path).await;
            return Ok(AcquisitionStatus::Failed);
        }

        // A lower file survived scavenging — it's the owner (or a regression).
        if let Some(owner) = self.first_readable_entry(lower).await? {
            return Ok(self
                .classify_loss(owner, regression_detected, &my_path)
                .await);
        }

        // We are lowest. Step 4: yield check against higher-priority queuers.
        if self.should_yield(higher, my_timestamp).await? {
            self.best_effort_delete(&my_path).await;
            return Ok(AcquisitionStatus::Yielded);
        }

        Ok(AcquisitionStatus::Owner(my_epoch))
    }

    pub async fn release(&self) -> Result<(), Error> {
        let metas = self.list_epoch_metas().await?;
        futures::stream::iter(metas)
            .map(|meta| async move {
                if let Some(entry) = self.read_entry(meta).await?
                    && entry.is_owned_by(&self.holder)
                {
                    self.best_effort_delete(&entry.meta.location).await;
                }
                Ok::<(), Error>(())
            })
            .buffer_unordered(100)
            .try_for_each(|_| async move { Ok(()) })
            .await?;

        Ok(())
    }

    /// Forcefully cleans up the lock directory.
    ///
    /// Creates a breaker epoch, then deletes all files except the active owner's
    /// file (if still active relative to the breaker's timestamp). Finally deletes
    /// the breaker file itself.
    ///
    /// This is a maintenance tool for clearing dead barriers or resolving
    /// persistent clock regression deadlocks.
    pub async fn unsafe_break_locks(&self) -> Result<(), Error> {
        let breaker = EpochFile {
            holder: format!("{}-breaker", self.holder),
            priority: 0,
            duration: 0,
        };
        let (my_epoch, my_timestamp, my_path) = self.put_next_epoch(breaker).await?;

        let metas = match self.list_epoch_metas().await {
            Ok(m) => m,
            Err(e) => {
                self.best_effort_delete(&my_path).await;
                return Err(e);
            }
        };

        let mut current_owner_found = false;
        for meta in metas {
            let entry_epoch = match meta.location.filename().and_then(Epoch::parse) {
                Some(e) => e,
                None => continue, // list_epoch_metas already filters, but be defensive
            };
            if entry_epoch == my_epoch {
                continue;
            }

            // Cannot use `read_entry` here: break-locks must scavenge files with
            // malformed JSON (a normal scavenge propagates the parse error).
            let bytes = match self.store.get(&meta.location).await {
                Ok(resp) => resp.bytes().await?,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => {
                    self.best_effort_delete(&my_path).await;
                    return Err(e.into());
                }
            };
            let file: EpochFile = match serde_json::from_slice(&bytes) {
                Ok(f) => f,
                Err(_) => {
                    self.best_effort_delete(&meta.location).await;
                    continue;
                }
            };

            let entry = EpochEntry {
                meta,
                file,
                epoch: entry_epoch,
            };
            let is_active = !entry.is_expired_at(my_timestamp);

            if !current_owner_found && is_active {
                current_owner_found = true;
                continue;
            }

            self.best_effort_delete(&entry.meta.location).await;
        }

        self.best_effort_delete(&my_path).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::base32::{from_base32, to_base32};

    #[test]
    fn base32_round_trip() {
        for val in [0, 1, 31, 32, 1023, (1u64 << 40) - 1] {
            assert_eq!(
                from_base32(&to_base32(val).unwrap()),
                Some(val),
                "round-trip failed for {val}"
            );
        }
    }

    #[test]
    fn base32_overflow() {
        assert!(to_base32(1u64 << 40).is_err());
        assert!(to_base32(u64::MAX).is_err());
    }

    #[test]
    fn base32_lexicographic_order_matches_numeric() {
        let values = [0, 1, 2, 31, 32, 33, 255, 256, 1000, 10000, 100_000];
        let encoded: Vec<String> = values.iter().map(|v| to_base32(*v).unwrap()).collect();
        for (v, e) in values.windows(2).zip(encoded.windows(2)) {
            assert!(
                e[0] < e[1],
                "to_base32({}) = {:?} should sort before to_base32({}) = {:?}",
                v[0],
                e[0],
                v[1],
                e[1]
            );
        }
    }
}
