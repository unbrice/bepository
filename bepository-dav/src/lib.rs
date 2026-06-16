// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Read-only WebDAV server exposing bepository checkpoints.
//!
//! The VFS presents checkpoints in a three-level hierarchy:
//!
//! ```text
//! /                            ← folder labels as directories
//! /photos/                     ← checkpoints for that folder
//! /photos/2026-04-09T10-00/    ← files at that checkpoint
//! /photos/2026-04-09T10-00/subdir/file.jpg
//! ```
//!
//! Call [`serve`] to start the server.

use std::collections::HashSet;
use std::convert::Infallible;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::SystemTime;

use base64::Engine as _;
use bepository_storage::{FolderLabelRef, FsEntry, SnapshotError, SnapshotFs, SnapshotRef};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dav_server::DavHandler;
use dav_server::body::Body;
use dav_server::fs::{
    DavDirEntry, DavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult, FsStream,
    OpenOptions, ReadDirMeta,
};
use futures_util::stream;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use ring::digest;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const TIMESTAMP_FMT: &str = "%Y-%m-%dT%H-%M";

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Start the read-only WebDAV server.
///
/// Snapshots are fetched once at startup via [`SnapshotFs::list_snapshots`];
/// the VFS is static for the lifetime of the server. Runs until `cancel` fires
/// (e.g., Ctrl-C from the caller).
pub async fn serve<Fs: SnapshotFs>(
    fs: Arc<Fs>,
    addr: &str,
    password: &SecretString,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let snapshots = Arc::new(fs.list_snapshots().await?);
    info!("WebDAV: loaded {} snapshot(s)", snapshots.len());

    let handler = Arc::new(
        DavHandler::builder()
            .filesystem(Box::new(DavSfs { fs, snapshots }))
            .build_handler(),
    );

    let listener = TcpListener::bind(addr).await?;
    info!("WebDAV: listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                let (stream, peer_addr) = match accept_res {
                    Ok(v) => v,
                    Err(e) => {
                        error!("WebDAV accept error: {e}");
                        continue;
                    }
                };
                let io = TokioIo::new(stream);
                let handler = handler.clone();
                let password = password.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let handler = handler.clone();
                        let password = password.clone();
                        async move {
                            Ok::<_, Infallible>(handle_request(req, &handler, &password).await)
                        }
                    });
                    if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                        warn!(%peer_addr, "WebDAV connection error: {e}");
                    }
                });
            }
            _ = cancel.cancelled() => {
                info!("WebDAV server shutting down");
                break;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

/// Validate HTTP Basic auth, then delegate to the DAV handler.
async fn handle_request(
    req: Request<Incoming>,
    handler: &DavHandler,
    password: &SecretString,
) -> Response<Body> {
    tracing::debug!(method = %req.method(), path = %req.uri().path(), "dav request");
    if check_basic_auth(req.headers(), password).is_ok() {
        return handler.handle(req).await;
    }
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("WWW-Authenticate", r#"Basic realm="bepository""#)
        .body(Body::from("Unauthorized\n"))
        .expect("static 401 response is valid")
}

#[tracing::instrument(level = "debug", skip_all, err(level = "warn"))]
fn check_basic_auth(
    headers: &http::HeaderMap,
    password: &SecretString,
) -> Result<(), &'static str> {
    let Some(auth) = headers.get(http::header::AUTHORIZATION) else {
        return Err("no Authorization header");
    };
    let Ok(s) = auth.to_str() else {
        return Err("header not valid UTF-8");
    };
    let Some(b64) = s.strip_prefix("Basic ") else {
        return Err("scheme is not Basic");
    };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) else {
        return Err("base64 decode error");
    };
    let Ok(creds) = std::str::from_utf8(&decoded) else {
        return Err("credentials not valid UTF-8");
    };
    let extracted_pw = creds.split_once(':').map_or("", |(_, pw)| pw);
    if string_eq_constant_time(extracted_pw, password.expose_secret()) {
        Ok(())
    } else {
        Err("wrong password")
    }
}

fn string_eq_constant_time(user_input: &str, secret: &str) -> bool {
    let hash_a = digest::digest(&digest::SHA256, user_input.as_bytes());
    let hash_b = digest::digest(&digest::SHA256, secret.as_bytes());
    hash_a.as_ref().ct_eq(hash_b.as_ref()).into()
}

// ---------------------------------------------------------------------------
// VFS path parsing
// ---------------------------------------------------------------------------

/// Parsed level within our three-level VFS hierarchy.
enum Level<'a, R: SnapshotRef> {
    Root,
    Folder(&'a FolderLabelRef),
    Snapshot(&'a R),
    SubPath(&'a R, &'a str),
}

fn snap_dir_name<R: SnapshotRef>(snap: &R) -> String {
    snap.create_time().format(TIMESTAMP_FMT).to_string()
}

/// Parse a `DavPath` into a [`Level`].
///
/// Returns `None` when the path references a non-existent folder or snapshot.
///
/// Note that paths originating from `DavPath` are pre-normalized and will not
/// contain traversal components (`..` or `.`), meaning that downstream VFS
/// code receives safe subpaths.
fn parse_path<'a, R: SnapshotRef>(
    path: &'a dav_server::davpath::DavPath,
    snapshots: &'a [R],
) -> Option<Level<'a, R>> {
    let path_str = path.as_rel_ospath().to_str().unwrap_or("");
    let trimmed = path_str.trim_matches('/');
    if trimmed.is_empty() {
        return Some(Level::Root);
    }

    let mut parts = trimmed.splitn(3, '/');
    let folder_label = parts.next()?;

    let label = FolderLabelRef::from_str(folder_label);
    let ts = match parts.next() {
        None => {
            return if snapshots.iter().any(|s| s.folder_label() == label) {
                Some(Level::Folder(label))
            } else {
                None
            };
        }
        Some(t) => t,
    };

    let snap = snapshots
        .iter()
        .find(|s| s.folder_label() == label && snap_dir_name(*s) == ts)?;

    let subpath = parts.next().unwrap_or("");
    if subpath.is_empty() {
        Some(Level::Snapshot(snap))
    } else {
        Some(Level::SubPath(snap, subpath))
    }
}

// ---------------------------------------------------------------------------
// DavFileSystem implementation
// ---------------------------------------------------------------------------

/// Bridges [`SnapshotFs`] to the `dav-server` filesystem interface.
///
/// Manual `Clone` avoids adding a spurious `Fs: Clone` bound — `Arc<Fs>` is
/// always cloneable regardless of whether `Fs` itself is.
struct DavSfs<Fs: SnapshotFs> {
    fs: Arc<Fs>,
    snapshots: Arc<Vec<Fs::Ref>>,
}

impl<Fs: SnapshotFs> Clone for DavSfs<Fs> {
    fn clone(&self) -> Self {
        DavSfs {
            fs: Arc::clone(&self.fs),
            snapshots: Arc::clone(&self.snapshots),
        }
    }
}

impl<Fs: SnapshotFs> DavFileSystem for DavSfs<Fs> {
    fn metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> FsFuture<'a, Box<dyn DavMetaData>> {
        Box::pin(async move {
            match parse_path(path, &self.snapshots) {
                Some(Level::Root | Level::Folder(_)) => {
                    Ok(Box::new(EntryMeta::dir(SystemTime::now())) as _)
                }
                Some(Level::Snapshot(snap)) => {
                    Ok(Box::new(EntryMeta::dir(to_systime(snap.create_time()))) as _)
                }
                Some(Level::SubPath(snap, subpath)) => {
                    match self.fs.file_metadata(snap, subpath).await {
                        Ok(FsEntry::File { size, modified, .. }) => Ok(Box::new(EntryMeta {
                            size,
                            modified: to_systime(modified),
                            is_dir: false,
                        })
                            as _),
                        Ok(FsEntry::Dir { .. }) => {
                            Ok(Box::new(EntryMeta::dir(to_systime(snap.create_time()))) as _)
                        }
                        Err(SnapshotError::NotFound) => Err(FsError::NotFound),
                        Err(_) => Err(FsError::GeneralFailure),
                    }
                }
                None => Err(FsError::NotFound),
            }
        })
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        Box::pin(async move {
            let entries: Vec<FsResult<Box<dyn DavDirEntry>>> =
                match parse_path(path, &self.snapshots) {
                    Some(Level::Root) => {
                        let mut seen = HashSet::new();
                        self.snapshots
                            .iter()
                            .filter(|s| seen.insert(s.folder_label()))
                            .map(|s| Ok(dir_entry(s.folder_label().to_string(), SystemTime::now())))
                            .collect()
                    }
                    Some(Level::Folder(label)) => self
                        .snapshots
                        .iter()
                        .filter(|s| s.folder_label() == label)
                        .map(|s| Ok(dir_entry(snap_dir_name(s), to_systime(s.create_time()))))
                        .collect(),
                    Some(Level::Snapshot(snap)) => {
                        let es = self.fs.read_dir(snap, "").await.map_err(map_snap_err)?;
                        fs_entries_to_dav(es)
                    }
                    Some(Level::SubPath(snap, subpath)) => {
                        let es = self
                            .fs
                            .read_dir(snap, subpath)
                            .await
                            .map_err(map_snap_err)?;
                        fs_entries_to_dav(es)
                    }
                    None => return Err(FsError::NotFound),
                };
            Ok(Box::pin(stream::iter(entries)) as FsStream<Box<dyn DavDirEntry>>)
        })
    }

    fn open<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        Box::pin(async move {
            if options.write || options.append || options.create || options.create_new {
                return Err(FsError::Forbidden);
            }
            let Level::SubPath(snap, subpath) =
                parse_path(path, &self.snapshots).ok_or(FsError::NotFound)?
            else {
                return Err(FsError::Forbidden);
            };
            match self.fs.file_metadata(snap, subpath).await {
                Ok(FsEntry::File { size, modified, .. }) => Ok(Box::new(DavSnapshotFile {
                    fs: Arc::clone(&self.fs),
                    snap: snap.clone(),
                    path: subpath.to_string(),
                    size,
                    modified: to_systime(modified),
                    pos: 0,
                }) as _),
                Ok(FsEntry::Dir { .. }) => Err(FsError::Forbidden),
                Err(SnapshotError::NotFound) => Err(FsError::NotFound),
                Err(_) => Err(FsError::GeneralFailure),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// DavFile implementation
// ---------------------------------------------------------------------------

struct DavSnapshotFile<Fs: SnapshotFs> {
    fs: Arc<Fs>,
    snap: Fs::Ref,
    path: String,
    size: u64,
    modified: SystemTime,
    pos: u64,
}

impl<Fs: SnapshotFs> std::fmt::Debug for DavSnapshotFile<Fs> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavSnapshotFile")
            .field("snap", &self.snap.folder_label())
            .field("path", &self.path)
            .field("size", &self.size)
            .field("pos", &self.pos)
            .finish()
    }
}

impl<Fs: SnapshotFs> DavFile for DavSnapshotFile<Fs> {
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = EntryMeta {
            size: self.size,
            modified: self.modified,
            is_dir: false,
        };
        Box::pin(std::future::ready(Ok(Box::new(meta) as _)))
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, Bytes> {
        tracing::debug!(offset = self.pos, count = count, "dav read");
        let offset = self.pos;
        let remaining = match self.size.checked_sub(self.pos) {
            Some(r) => r,
            None => return Box::pin(std::future::ready(Err(FsError::GeneralFailure))),
        };
        let to_read_u64 = remaining.min(count as u64);
        self.pos += to_read_u64;
        let to_read_usize = usize::try_from(to_read_u64).expect("bounded by count (usize)");

        Box::pin(async move {
            if to_read_usize == 0 {
                return Ok(Bytes::new());
            }
            self.fs
                .read_bytes(&self.snap, &self.path, offset, to_read_usize)
                .await
                .map_err(|_| FsError::GeneralFailure)
        })
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        let new_pos_opt = match pos {
            SeekFrom::Start(n) => Some(n),
            SeekFrom::End(n) => {
                if n >= 0 {
                    // SAFE: n >= 0 ensures this will not panic
                    self.size.checked_add(n.try_into().unwrap())
                } else {
                    self.size.checked_sub(n.unsigned_abs())
                }
            }
            SeekFrom::Current(n) => {
                if n >= 0 {
                    // SAFE: n >= 0 ensures this will not panic
                    self.pos.checked_add(n.try_into().unwrap())
                } else {
                    self.pos.checked_sub(n.unsigned_abs())
                }
            }
        };

        if let Some(new_pos) = new_pos_opt {
            self.pos = new_pos;
            Box::pin(std::future::ready(Ok(new_pos)))
        } else {
            Box::pin(std::future::ready(Err(FsError::GeneralFailure)))
        }
    }

    fn write_buf(&mut self, _buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        Box::pin(std::future::ready(Err(FsError::Forbidden)))
    }

    fn write_bytes(&mut self, _buf: Bytes) -> FsFuture<'_, ()> {
        Box::pin(std::future::ready(Err(FsError::Forbidden)))
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        Box::pin(std::future::ready(Ok(())))
    }
}

// ---------------------------------------------------------------------------
// Helper types and functions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct EntryMeta {
    size: u64,
    modified: SystemTime,
    is_dir: bool,
}

impl EntryMeta {
    fn dir(modified: SystemTime) -> Self {
        EntryMeta {
            size: 0,
            modified,
            is_dir: true,
        }
    }
}

impl DavMetaData for EntryMeta {
    fn len(&self) -> u64 {
        self.size
    }
    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.modified)
    }
    fn is_dir(&self) -> bool {
        self.is_dir
    }
}

struct SnapshotDirEntry {
    name_bytes: Vec<u8>,
    meta: EntryMeta,
}

impl DavDirEntry for SnapshotDirEntry {
    fn name(&self) -> Vec<u8> {
        self.name_bytes.clone()
    }
    fn metadata(&self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let m = self.meta.clone();
        Box::pin(std::future::ready(Ok(Box::new(m) as _)))
    }
}

fn dir_entry(name: String, modified: SystemTime) -> Box<dyn DavDirEntry> {
    Box::new(SnapshotDirEntry {
        name_bytes: name.into_bytes(),
        meta: EntryMeta::dir(modified),
    })
}

fn fs_entries_to_dav(entries: Vec<FsEntry>) -> Vec<FsResult<Box<dyn DavDirEntry>>> {
    entries
        .into_iter()
        .map(|e| {
            Ok(match &e {
                FsEntry::File {
                    name,
                    size,
                    modified,
                } => Box::new(SnapshotDirEntry {
                    name_bytes: name.as_bytes().to_vec(),
                    meta: EntryMeta {
                        size: *size,
                        modified: to_systime(*modified),
                        is_dir: false,
                    },
                }) as Box<dyn DavDirEntry>,
                FsEntry::Dir { name } => dir_entry(name.clone(), SystemTime::now()),
            })
        })
        .collect()
}

fn to_systime(dt: DateTime<Utc>) -> SystemTime {
    dt.into()
}

fn map_snap_err(e: SnapshotError) -> FsError {
    match e {
        SnapshotError::NotFound => FsError::NotFound,
        SnapshotError::NotADir | SnapshotError::NotAFile => FsError::Forbidden,
        SnapshotError::Io(_) => FsError::GeneralFailure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bepository_storage::{FolderLabelRef, SnapshotRef};
    use chrono::{DateTime, Utc};
    use dav_server::davpath::DavPath;

    #[derive(Clone, Copy, Debug)]
    struct MockSnapRef;

    impl SnapshotRef for MockSnapRef {
        fn folder_label(&self) -> &'static FolderLabelRef {
            FolderLabelRef::from_str("folder")
        }

        fn create_time(&self) -> DateTime<Utc> {
            "2026-04-09T10:00:00Z".parse().unwrap()
        }
    }

    #[test]
    fn test_parse_path() {
        let snaps = vec![MockSnapRef];

        let safe_dav = DavPath::new("/folder/2026-04-09T10-00/.gitignore").unwrap();
        assert!(parse_path(&safe_dav, &snaps).is_some());
    }
}
