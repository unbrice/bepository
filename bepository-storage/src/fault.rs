// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fault injection for the object-store layer (test-utils only).
//!
//! Wraps `Arc<dyn ObjectStore>` to enable counter-based fault injection:
//! configure "fail the next N calls to method X with error E", then let
//! the (N+1)th call succeed. Zero production binary impact — gated behind
//! `#[cfg(any(test, feature = "test-utils"))]`.

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use parking_lot::Mutex;

/// Methods on the `ObjectStore` trait that can have faults injected.
///
/// object_store 0.14 routes the convenience methods through the core `*_opts`
/// methods, so faults are intercepted at the core-method level and
/// distinguished by their options (see `FaultObjectStore`'s impl).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectStoreMethod {
    /// `get_opts` without `head` — SlateStorage meta reads, SlateDB SST reads.
    Get,
    /// `put_opts` with default options (`put`) — SlateStorage meta writes.
    Put,
    /// `put_opts` with non-default options — lock atomic creates (`PutMode::Create`).
    PutOpts,
    /// `delete_stream` (`delete`) — meta cleanup, lock release.
    Delete,
    /// `get_opts` with `head: true` (`head`) — lock file existence checks.
    Head,
    /// [`ObjectStore::list`] — lock epoch enumeration.
    List,
    /// [`ObjectStore::list_with_delimiter`] — SlateStorage meta listing.
    ListWithDelimiter,
}

type ErrorFactory = Arc<dyn Fn() -> object_store::Error + Send + Sync>;

struct ObjectStoreFaultConfigInner {
    rules: Mutex<HashMap<ObjectStoreMethod, (u32, ErrorFactory)>>,
}

/// Shared fault configuration for [`FaultObjectStore`].
///
/// Cheaply cloneable — all clones share the same rule set via `Arc`.
#[derive(Clone)]
pub struct ObjectStoreFaultConfig {
    inner: Arc<ObjectStoreFaultConfigInner>,
}

impl ObjectStoreFaultConfig {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ObjectStoreFaultConfigInner {
                rules: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Fail the next `count` calls to `method` using the given error factory.
    ///
    /// The factory is called once per injected failure to produce a fresh error.
    pub fn set(
        &self,
        method: ObjectStoreMethod,
        count: NonZeroU32,
        make_error: impl Fn() -> object_store::Error + Send + Sync + 'static,
    ) {
        self.inner
            .rules
            .lock()
            .insert(method, (count.get(), Arc::new(make_error)));
    }

    /// Clear any pending fault for `method`.
    pub fn clear(&self, method: ObjectStoreMethod) {
        self.inner.rules.lock().remove(&method);
    }

    fn check(&self, method: ObjectStoreMethod) -> object_store::Result<()> {
        let mut rules = self.inner.rules.lock();
        if let Some((count, make_error)) = rules.get_mut(&method) {
            let err = make_error();
            *count -= 1;
            if *count == 0 {
                rules.remove(&method);
            }
            return Err(err);
        }
        Ok(())
    }
}

impl Default for ObjectStoreFaultConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Wraps `Arc<dyn ObjectStore>` to enable counter-based fault injection.
///
/// Inject into [`SlateStorage::new`](crate::SlateStorage::new) in place of the
/// real object store. All calls delegate to the inner store; configure faults
/// via the [`ObjectStoreFaultConfig`] returned from [`FaultObjectStore::new`].
pub struct FaultObjectStore {
    inner: Arc<dyn ObjectStore>,
    config: ObjectStoreFaultConfig,
}

impl FaultObjectStore {
    /// Wrap `inner` with fault injection.
    ///
    /// Returns `(FaultObjectStore, ObjectStoreFaultConfig)`. The config is shared
    /// with the store instance.
    pub fn new(inner: Arc<dyn ObjectStore>) -> (Self, ObjectStoreFaultConfig) {
        let config = ObjectStoreFaultConfig::new();
        let store = Self {
            inner,
            config: config.clone(),
        };
        (store, config)
    }
}

impl fmt::Display for FaultObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultObjectStore({})", self.inner)
    }
}

impl fmt::Debug for FaultObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultObjectStore({:?})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // `ObjectStoreExt::put` delegates here with default options.
        if opts == PutOptions::default() {
            self.config.check(ObjectStoreMethod::Put)?;
        } else {
            self.config.check(ObjectStoreMethod::PutOpts)?;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // `ObjectStoreExt::head` delegates here with `head: true`.
        if options.head {
            self.config.check(ObjectStoreMethod::Head)?;
        } else {
            self.config.check(ObjectStoreMethod::Get)?;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        if let Err(e) = self.config.check(ObjectStoreMethod::Delete) {
            return Box::pin(futures::stream::once(futures::future::ready(Err(e))));
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(e) = self.config.check(ObjectStoreMethod::List) {
            return Box::pin(futures::stream::once(futures::future::ready(Err(e))));
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.config.check(ObjectStoreMethod::ListWithDelimiter)?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
