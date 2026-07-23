// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Wire credential-file paths from `/etc/bepository/env` into systemd
//! `LoadCredential` drop-ins, rewriting the env values to the runtime
//! credential paths; unwire on uninstall. The unit runs `DynamicUser=yes`, so
//! only systemd (reading as root) can open a root-owned key file.

use std::fs;
use std::path::Path;

use anyhow::{Result, anyhow};

use super::{SERVICE_NAME, map_fs_err, write_env_atomic};
use crate::envfile::{parse_env_lines, set_env_assignment};

/// The runtime dir systemd exposes this unit's credentials under — derived
/// from the unit name, as systemd does.
fn runtime_credentials_dir() -> String {
    format!("/run/credentials/{SERVICE_NAME}")
}

/// `runtime_credentials_dir()` with a trailing slash, for prefix tests.
fn runtime_credentials_prefix() -> String {
    format!("{}/", runtime_credentials_dir())
}

/// A credential install-service can wire into a managed drop-in.
struct Credential {
    /// Credential name: the drop-in file is `<name>.conf` and the runtime
    /// path `/run/credentials/bepository.service/<name>`.
    name: &'static str,
    /// Human description used in warnings.
    description: &'static str,
    /// Extract the credential-carrying value from the env text.
    /// `None`: detector inapplicable (e.g. non-sftp URI) — leave everything.
    /// `Some(None)`: applies but the value is absent — unwire.
    extract: fn(&str) -> Option<Option<String>>,
    /// Return the env text with this credential's value replaced by `new`.
    set: fn(&str, &str) -> Option<String>,
    /// If this credential's value equals `runtime`, return the env text with
    /// it replaced by `source`.
    restore: fn(&str, &str, &str) -> Option<String>,
    /// The value to show in the ad-hoc note (full original URI for SFTP, key
    /// path for GCS). `source` is the pre-wiring local path.
    adhoc_value: fn(&str, &str) -> Option<String>,
}

const CREDENTIALS: &[Credential] = &[
    Credential {
        name: "sftp-key",
        description: "the SFTP key in BEPOSITORY_STORAGE_URI",
        extract: sftp_extract,
        set: sftp_set,
        restore: sftp_restore,
        adhoc_value: sftp_adhoc_value,
    },
    Credential {
        name: "gcs-sa-key",
        description: "GOOGLE_APPLICATION_CREDENTIALS",
        extract: gcs_extract,
        set: gcs_set,
        restore: gcs_restore,
        adhoc_value: gcs_adhoc_value,
    },
];

/// Where a credential value points, classified textually — never a stat():
/// the runtime path legitimately does not exist while the service is stopped.
#[derive(Debug, PartialEq, Eq)]
enum CredentialSource {
    /// Empty or unset — nothing to wire.
    Absent,
    /// Not absolute — the daemon cannot resolve it; warn, keep verbatim.
    Relative(String),
    /// Absolute local path to hand to systemd via LoadCredential.
    AbsoluteLocal(String),
    /// Already points at this unit's runtime credential path (payload: the
    /// credential name).
    AbsoluteWired(String),
    /// Under `/run/credentials/` but for another unit — not ours to touch.
    AbsoluteForeign,
}

fn classify_credential_source(value: Option<&str>) -> CredentialSource {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return CredentialSource::Absent;
    };
    if let Some(name) = value.strip_prefix(&runtime_credentials_prefix()) {
        return CredentialSource::AbsoluteWired(name.to_string());
    }
    if value.starts_with("/run/credentials/") {
        return CredentialSource::AbsoluteForeign;
    }
    if value.starts_with('/') {
        return CredentialSource::AbsoluteLocal(value.to_string());
    }
    CredentialSource::Relative(value.to_string())
}

/// The value of the first assignment to `key`, matching what `load_env_file`
/// (first occurrence wins) and `set_env_assignment` (replace first) see.
fn lookup_env<'a>(env_text: &'a str, key: &str) -> Option<&'a str> {
    parse_env_lines(env_text)
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

/// The parsed `BEPOSITORY_STORAGE_URI` when it is an sftp:// URI.
fn sftp_uri(env_text: &str) -> Option<url::Url> {
    let url = url::Url::parse(lookup_env(env_text, "BEPOSITORY_STORAGE_URI")?).ok()?;
    (url.scheme() == "sftp").then_some(url)
}

fn sftp_key_param(url: &url::Url) -> Option<String> {
    url.query_pairs()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.into_owned())
}

fn sftp_extract(env_text: &str) -> Option<Option<String>> {
    Some(sftp_key_param(&sftp_uri(env_text)?))
}

fn sftp_set(env_text: &str, new: &str) -> Option<String> {
    let rewritten = rewrite_sftp_key(&sftp_uri(env_text)?, new);
    Some(set_env_assignment(
        env_text,
        "BEPOSITORY_STORAGE_URI",
        &rewritten,
    ))
}

fn sftp_restore(env_text: &str, runtime: &str, source: &str) -> Option<String> {
    let url = sftp_uri(env_text)?;
    (sftp_key_param(&url)? == runtime).then(|| sftp_set(env_text, source))?
}

fn gcs_extract(env_text: &str) -> Option<Option<String>> {
    Some(lookup_env(env_text, "GOOGLE_APPLICATION_CREDENTIALS").map(str::to_owned))
}

fn gcs_set(env_text: &str, new: &str) -> Option<String> {
    Some(set_env_assignment(
        env_text,
        "GOOGLE_APPLICATION_CREDENTIALS",
        new,
    ))
}

fn gcs_restore(env_text: &str, runtime: &str, source: &str) -> Option<String> {
    (lookup_env(env_text, "GOOGLE_APPLICATION_CREDENTIALS")? == runtime)
        .then(|| gcs_set(env_text, source))?
}

/// SFTP's ad-hoc value: the full URI with the original key path. Before wiring
/// the env still holds the user's verbatim URI — keep it (no re-encoding);
/// after, rebuild with `source` substituted back.
fn sftp_adhoc_value(env_text: &str, source: &str) -> Option<String> {
    let url = sftp_uri(env_text)?;
    if sftp_key_param(&url).as_deref() == Some(source) {
        return lookup_env(env_text, "BEPOSITORY_STORAGE_URI").map(str::to_string);
    }
    Some(rewrite_sftp_key(&url, source))
}

fn gcs_adhoc_value(_: &str, source: &str) -> Option<String> {
    Some(source.to_string())
}

/// Rebuild the URI with the `key` query param set to `new_path`. All pairs are
/// re-encoded (`query_pairs_mut` semantics), and `key` moves to the end.
fn rewrite_sftp_key(url: &url::Url, new_path: &str) -> String {
    let mut url = url.clone();
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k != "key")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    {
        let mut q = url.query_pairs_mut();
        q.clear();
        q.extend_pairs(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        q.append_pair("key", new_path);
    }
    url.into()
}

/// The managed drop-in handing `source` to the unit as credential `name`.
fn dropin_content(name: &str, source: &str) -> String {
    format!(
        "# Managed by `bepository install-service` — do not edit; \
         uninstall-service removes it.\n[Service]\nLoadCredential={name}:{source}\n"
    )
}

/// The `(name, source)` of a drop-in's `LoadCredential=<name>:<source>` line.
fn parse_load_credential(content: &str) -> Option<(String, String)> {
    content.lines().find_map(|line| {
        line.trim_start()
            .strip_prefix("LoadCredential=")
            .and_then(|rest| rest.split_once(':'))
            .map(|(name, source)| (name.to_string(), source.to_string()))
    })
}

/// The source of the first `LoadCredential=<name>:<source>` line in any
/// `*.conf` in `dropin_dir`. Line-anchored so commented-out entries do not
/// count; users of the documented manual recipe (their own override.conf)
/// must not be warned.
fn dropin_credential_source(dropin_dir: &Path, name: &str) -> Option<String> {
    let needle = format!("LoadCredential={name}:");
    let entries = fs::read_dir(dropin_dir).ok()?;
    entries
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "conf"))
        .filter_map(|e| fs::read_to_string(e.path()).ok())
        .find_map(|content| {
            content
                .lines()
                .find_map(|l| l.trim_start().strip_prefix(&needle).map(str::to_string))
        })
}

/// Write `content` to `path`, skipping files that already hold exactly it —
/// re-writing an unchanged drop-in would force a service restart for nothing.
/// Returns whether it wrote.
fn write_if_changed(path: &Path, content: &str) -> Result<bool> {
    if fs::read_to_string(path).is_ok_and(|existing| existing == content) {
        return Ok(false);
    }
    fs::write(path, content).map_err(|e| map_fs_err(e, "install-service", "writing", path))?;
    Ok(true)
}

/// Per-credential result of wiring; drives the closing notes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WiringOutcome {
    /// Detector inapplicable or nothing to change.
    Untouched,
    /// Env value (re)written to the runtime path; `dropin_changed` records
    /// whether the drop-in file was (re)written this run. `adhoc` is the
    /// original credential value for the closing note.
    Wired {
        dropin_changed: bool,
        adhoc: Option<String>,
    },
    /// Already wired before this run (drop-in presence verified or warned);
    /// `adhoc` reconstructed from the drop-in when one provides the source.
    AlreadyWired { adhoc: Option<String> },
    /// Managed drop-in removed (value absent).
    Unwired,
    /// Relative path left verbatim (warned).
    RelativeWarned,
}

/// Wire the managed drop-ins from the credential paths in `env_text`. Returns
/// the env text with any rewrites applied plus the per-credential outcomes.
/// Drop-in files are written/removed here; the env text itself is only
/// returned — the caller persists it.
pub(super) fn wire_credential_dropins(
    env_text: &str,
    dropin_dir: &Path,
) -> Result<(String, Vec<(&'static str, WiringOutcome)>)> {
    let mut text = env_text.to_string();
    let mut outcomes = Vec::with_capacity(CREDENTIALS.len());
    for cred in CREDENTIALS {
        let (new_text, outcome) = wire_credential(&text, cred, dropin_dir)?;
        text = new_text;
        outcomes.push((cred.name, outcome));
    }
    Ok((text, outcomes))
}

fn wire_credential(
    env_text: &str,
    cred: &Credential,
    dropin_dir: &Path,
) -> Result<(String, WiringOutcome)> {
    let Some(value) = (cred.extract)(env_text) else {
        return Ok((env_text.to_string(), WiringOutcome::Untouched));
    };
    let dropin_path = dropin_dir.join(format!("{}.conf", cred.name));
    match classify_credential_source(value.as_deref()) {
        CredentialSource::Absent => {
            if dropin_path.exists() {
                fs::remove_file(&dropin_path)
                    .map_err(|e| map_fs_err(e, "install-service", "removing", &dropin_path))?;
                return Ok((env_text.to_string(), WiringOutcome::Unwired));
            }
            Ok((env_text.to_string(), WiringOutcome::Untouched))
        }
        CredentialSource::Relative(value) => {
            eprintln!(
                "bepository: warning: {description} is a relative path ({value}); the service runs \
                 under DynamicUser=yes and cannot resolve it — leaving it unchanged; use an \
                 absolute path to have install-service wire a LoadCredential drop-in",
                description = cred.description
            );
            Ok((env_text.to_string(), WiringOutcome::RelativeWarned))
        }
        CredentialSource::AbsoluteForeign => Ok((env_text.to_string(), WiringOutcome::Untouched)),
        CredentialSource::AbsoluteLocal(source) => {
            if !Path::new(&source).exists() {
                eprintln!(
                    "bepository: warning: {description} source {source} does not exist; wiring the \
                     drop-in anyway (LoadCredential re-reads it at every service start)",
                    description = cred.description
                );
            }
            fs::create_dir_all(dropin_dir)
                .map_err(|e| map_fs_err(e, "install-service", "creating", dropin_dir))?;
            let dropin_changed =
                write_if_changed(&dropin_path, &dropin_content(cred.name, &source))?;
            // Before `set` rewrites the env: the note needs the original value.
            let adhoc = (cred.adhoc_value)(env_text, &source);
            let runtime = format!("{}/{}", runtime_credentials_dir(), cred.name);
            let text = (cred.set)(env_text, &runtime).ok_or_else(|| {
                anyhow!(
                    "failed to rewrite {description} in the env file",
                    description = cred.description
                )
            })?;
            println!(
                "Wired {description} into {} (credential {}).",
                dropin_path.display(),
                cred.name,
                description = cred.description
            );
            Ok((
                text,
                WiringOutcome::Wired {
                    dropin_changed,
                    adhoc,
                },
            ))
        }
        CredentialSource::AbsoluteWired(name) => {
            let source = dropin_credential_source(dropin_dir, &name);
            if source.is_none() {
                eprintln!(
                    "bepository: warning: {description} points at {}/{name} but no \
                     drop-in in {} provides LoadCredential={name}: — the credential will be \
                     missing at service start",
                    runtime_credentials_dir(),
                    dropin_dir.display(),
                    description = cred.description
                );
            }
            let adhoc = source.and_then(|src| (cred.adhoc_value)(env_text, &src));
            Ok((env_text.to_string(), WiringOutcome::AlreadyWired { adhoc }))
        }
    }
}

/// Closing notes for credentials that ended up wired: ad-hoc commands cannot
/// use the `/run/credentials` path while the service is stopped, so print the
/// override command with the original value filled in (a template when the
/// source is unknown).
pub(super) fn print_credential_notes(outcomes: &[(&'static str, WiringOutcome)]) {
    let mut any_dropin_changed = false;
    for (name, outcome) in outcomes {
        let adhoc = match outcome {
            WiringOutcome::Wired {
                dropin_changed,
                adhoc,
            } => {
                any_dropin_changed |= dropin_changed;
                adhoc
            }
            WiringOutcome::AlreadyWired { adhoc } => adhoc,
            _ => continue,
        };
        match (*name, adhoc.as_deref()) {
            ("sftp-key", uri) => {
                println!("Note: the SFTP key is wired via LoadCredential — while the service is");
                println!(
                    "  stopped, ad-hoc commands must override the URI with the real key path:"
                );
                println!(
                    "  BEPOSITORY_STORAGE_URI='{}' bepository get-id",
                    uri.unwrap_or("sftp://<user>@<host>/<path>?key=<real key path>")
                );
            }
            ("gcs-sa-key", path) => {
                println!("Note: the GCS key is wired via LoadCredential — while the service is");
                println!("  stopped, pipe it to ad-hoc commands via stdin:");
                println!(
                    "  sudo cat '{}' | GOOGLE_APPLICATION_CREDENTIALS=/dev/stdin bepository get-id",
                    path.unwrap_or("<sa-key.json>")
                );
            }
            _ => {}
        }
    }
    if any_dropin_changed {
        println!(
            "Credential drop-in(s) changed — if {SERVICE_NAME} is already running, apply them with:"
        );
        println!("  systemctl restart bepository   (a plain start won't pick up drop-in changes)");
    }
}

/// Reverse of [`wire_credential_dropins`]: restore env values pointing at the
/// runtime credential paths from the managed drop-ins' source paths, then
/// remove the managed drop-ins (and the drop-in dir if left empty). Missing
/// pieces are skipped — uninstall stays idempotent.
pub(super) fn unwire_credential_dropins(env_path: &Path, dropin_dir: &Path) -> Result<()> {
    let mut env_text = fs::read_to_string(env_path).ok();
    let mut restored = false;
    for cred in CREDENTIALS {
        let dropin_path = dropin_dir.join(format!("{}.conf", cred.name));
        let Ok(content) = fs::read_to_string(&dropin_path) else {
            continue;
        };
        let Some((name, source)) = parse_load_credential(&content) else {
            eprintln!(
                "bepository: warning: {} has no LoadCredential=<name>:<source> line; cannot \
                 restore the original path in {}",
                dropin_path.display(),
                env_path.display()
            );
            continue;
        };
        if let Some(text) = env_text.as_mut() {
            let runtime = format!("{}/{name}", runtime_credentials_dir());
            if let Some(new) = (cred.restore)(text, &runtime, &source) {
                *text = new;
                restored = true;
            }
        }
    }
    if restored && let Some(text) = &env_text {
        write_env_atomic(env_path, text, "uninstall-service")?;
        println!(
            "Restored original credential paths in {}.",
            env_path.display()
        );
    }
    for cred in CREDENTIALS {
        let dropin_path = dropin_dir.join(format!("{}.conf", cred.name));
        if dropin_path.exists() {
            fs::remove_file(&dropin_path)
                .map_err(|e| map_fs_err(e, "uninstall-service", "removing", &dropin_path))?;
        }
    }
    // Only succeeds on an empty dir — a user's own drop-ins keep it alive. A
    // failure is harmless; the unit removal below reports real problems.
    let _ = fs::remove_dir(dropin_dir);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome_of(outcomes: &[(&'static str, WiringOutcome)], name: &str) -> WiringOutcome {
        outcomes
            .iter()
            .find(|(n, _)| *n == name)
            .unwrap_or_else(|| panic!("no outcome for {name}"))
            .1
            .clone()
    }

    #[test]
    fn credential_source_classification() {
        assert_eq!(classify_credential_source(None), CredentialSource::Absent);
        // Empty values count as absent — never the relative-warn branch.
        assert_eq!(
            classify_credential_source(Some("")),
            CredentialSource::Absent
        );
        assert_eq!(
            classify_credential_source(Some(".ssh/id_ed25519")),
            CredentialSource::Relative(".ssh/id_ed25519".into())
        );
        assert_eq!(
            classify_credential_source(Some("/etc/bepository/id_ed25519")),
            CredentialSource::AbsoluteLocal("/etc/bepository/id_ed25519".into())
        );
        assert_eq!(
            classify_credential_source(Some("/run/credentials/bepository.service/sftp-key")),
            CredentialSource::AbsoluteWired("sftp-key".into())
        );
        assert_eq!(
            classify_credential_source(Some("/run/credentials/other.service/sftp-key")),
            CredentialSource::AbsoluteForeign
        );
    }

    #[test]
    fn sftp_detector_extracts_key_only_for_sftp() {
        // No URI, unparseable URI, or non-sftp scheme: detector inapplicable.
        assert_eq!(sftp_extract(""), None);
        assert_eq!(sftp_extract("BEPOSITORY_STORAGE_URI=not a uri"), None);
        assert_eq!(
            sftp_extract("BEPOSITORY_STORAGE_URI=s3://bucket/prefix"),
            None
        );
        // sftp without a key param: applies, value absent.
        assert_eq!(
            sftp_extract("BEPOSITORY_STORAGE_URI=sftp://u@h/srv"),
            Some(None)
        );
        // An empty key counts as absent.
        assert_eq!(
            sftp_extract("BEPOSITORY_STORAGE_URI=sftp://u@h/srv?key="),
            Some(Some(String::new()))
        );
        assert_eq!(
            sftp_extract("BEPOSITORY_STORAGE_URI=sftp://u@h/srv?key=/home/u/.ssh/id_ed25519"),
            Some(Some("/home/u/.ssh/id_ed25519".into()))
        );
        // Relative keys come through verbatim (classification warns on them).
        assert_eq!(
            sftp_extract("BEPOSITORY_STORAGE_URI=sftp://u@h/srv?key=id_ed25519"),
            Some(Some("id_ed25519".into()))
        );
        // A quoted env line still parses — the shared parser strips the quotes.
        assert_eq!(
            sftp_extract("BEPOSITORY_STORAGE_URI=\"sftp://u@h/srv?key=/home/u/.ssh/id_ed25519\""),
            Some(Some("/home/u/.ssh/id_ed25519".into()))
        );
    }

    #[test]
    fn gcs_detector_treats_empty_as_absent() {
        assert_eq!(gcs_extract(""), Some(None));
        assert_eq!(
            gcs_extract("GOOGLE_APPLICATION_CREDENTIALS="),
            Some(Some(String::new()))
        );
        assert_eq!(
            gcs_extract("GOOGLE_APPLICATION_CREDENTIALS=/etc/bepository/sa-key.json"),
            Some(Some("/etc/bepository/sa-key.json".into()))
        );
        assert_eq!(
            gcs_extract("GOOGLE_APPLICATION_CREDENTIALS=\"/etc/bepository/sa-key.json\""),
            Some(Some("/etc/bepository/sa-key.json".into()))
        );
    }

    /// query_pairs_mut re-encodes every pair, so assert the rewritten URI
    /// still parses — including through opendal's builder (lazy, no network).
    #[test]
    fn sftp_key_rewrite_roundtrips_through_opendal() {
        std::sync::LazyLock::force(&crate::REGISTER_SFTP);
        let url = url::Url::parse(
            "sftp://user@example.com:2222/srv/bepository?key=/home/u%20ser/.ssh/id_ed25519&timeout=30s",
        )
        .unwrap();
        let rewritten = rewrite_sftp_key(&url, "/run/credentials/bepository.service/sftp-key");
        opendal::Operator::from_uri(rewritten.as_str())
            .expect("opendal must parse the rewritten URI");
        let pairs: std::collections::HashMap<String, String> = url::Url::parse(&rewritten)
            .unwrap()
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            pairs.get("key").map(String::as_str),
            Some("/run/credentials/bepository.service/sftp-key")
        );
        assert_eq!(pairs.get("timeout").map(String::as_str), Some("30s"));
    }

    #[test]
    fn restore_only_rewrites_runtime_references() {
        let env = "BEPOSITORY_STORAGE_URI=sftp://u@h/srv?key=/run/credentials/bepository.service/sftp-key\n\
                   GOOGLE_APPLICATION_CREDENTIALS=/run/credentials/bepository.service/gcs-sa-key\n";
        let runtime_sftp = "/run/credentials/bepository.service/sftp-key";
        let runtime_gcs = "/run/credentials/bepository.service/gcs-sa-key";
        let sftp_restored = sftp_restore(env, runtime_sftp, "/home/u/.ssh/id_ed25519").unwrap();
        assert_eq!(
            sftp_extract(&sftp_restored),
            Some(Some("/home/u/.ssh/id_ed25519".into()))
        );
        let gcs_restored = gcs_restore(env, runtime_gcs, "/etc/bepository/sa-key.json").unwrap();
        assert_eq!(
            gcs_extract(&gcs_restored),
            Some(Some("/etc/bepository/sa-key.json".into()))
        );
        // Values not referencing the given runtime path are left alone.
        assert!(sftp_restore(env, runtime_gcs, "/x").is_none());
        assert!(gcs_restore(env, runtime_sftp, "/x").is_none());
        assert!(sftp_restore(&sftp_restored, runtime_sftp, "/y").is_none());
        assert!(gcs_restore(&gcs_restored, runtime_gcs, "/y").is_none());
    }

    #[test]
    fn wires_absolute_paths_and_rewrites_env() {
        let tmp = tempfile::tempdir().unwrap();
        let dropin_dir = tmp.path().join("bepository.service.d");
        let source = tmp.path().join("id_ed25519");
        fs::write(&source, "key").unwrap();
        let env = format!(
            "BEPOSITORY_STORAGE_URI=sftp://u@h/srv?key={}\nGOOGLE_APPLICATION_CREDENTIALS=\n",
            source.display()
        );

        let (env, outcomes) = wire_credential_dropins(&env, &dropin_dir).unwrap();
        assert_eq!(
            sftp_extract(&env),
            Some(Some("/run/credentials/bepository.service/sftp-key".into()))
        );
        assert_eq!(
            outcome_of(&outcomes, "sftp-key"),
            WiringOutcome::Wired {
                dropin_changed: true,
                // Not yet rewired at that point: the user's verbatim URI.
                adhoc: Some(format!("sftp://u@h/srv?key={}", source.display()))
            }
        );
        // Empty GCS value: nothing wired, no drop-in.
        assert_eq!(
            outcome_of(&outcomes, "gcs-sa-key"),
            WiringOutcome::Untouched
        );
        let content = fs::read_to_string(dropin_dir.join("sftp-key.conf")).unwrap();
        assert_eq!(
            parse_load_credential(&content),
            Some(("sftp-key".to_string(), source.to_str().unwrap().to_string()))
        );
        assert!(!dropin_dir.join("gcs-sa-key.conf").exists());

        // A second run is textually idempotent: no drop-in rewrite (which
        // would force a service restart), no env change. The note's URI is
        // rebuilt from the drop-in's source path.
        let (env2, outcomes2) = wire_credential_dropins(&env, &dropin_dir).unwrap();
        assert_eq!(env2, env);
        let WiringOutcome::AlreadyWired {
            adhoc: Some(rewired),
        } = outcome_of(&outcomes2, "sftp-key")
        else {
            panic!("expected AlreadyWired with a reconstructed URI");
        };
        assert_eq!(
            sftp_key_param(&url::Url::parse(&rewired).unwrap()).as_deref(),
            source.to_str()
        );
    }

    #[test]
    fn absent_value_removes_managed_dropin() {
        let tmp = tempfile::tempdir().unwrap();
        let dropin_dir = tmp.path().join("bepository.service.d");
        fs::create_dir_all(&dropin_dir).unwrap();
        fs::write(
            dropin_dir.join("gcs-sa-key.conf"),
            dropin_content("gcs-sa-key", "/etc/bepository/sa-key.json"),
        )
        .unwrap();

        let (_env, outcomes) = wire_credential_dropins("", &dropin_dir).unwrap();
        assert_eq!(outcome_of(&outcomes, "gcs-sa-key"), WiringOutcome::Unwired);
        assert!(!dropin_dir.join("gcs-sa-key.conf").exists());
    }

    /// A value already at the runtime path must not produce a false-positive
    /// missing-drop-in warning when *any* `*.conf` provides the credential —
    /// here the documented manual recipe's own override.conf.
    #[test]
    fn already_wired_with_manual_dropin_is_recognized() {
        let tmp = tempfile::tempdir().unwrap();
        let dropin_dir = tmp.path().join("bepository.service.d");
        fs::create_dir_all(&dropin_dir).unwrap();
        fs::write(
            dropin_dir.join("override.conf"),
            "[Service]\nLoadCredential=sa-key.json:/etc/bepository/sa-key.json\n",
        )
        .unwrap();
        assert_eq!(
            dropin_credential_source(&dropin_dir, "sa-key.json").as_deref(),
            Some("/etc/bepository/sa-key.json")
        );
        // Commented-out entries don't count.
        assert_eq!(dropin_credential_source(&dropin_dir, "other"), None);

        let env =
            "GOOGLE_APPLICATION_CREDENTIALS=/run/credentials/bepository.service/sa-key.json\n";
        let (new_env, outcomes) = wire_credential_dropins(env, &dropin_dir).unwrap();
        assert_eq!(new_env, env);
        assert_eq!(
            outcome_of(&outcomes, "gcs-sa-key"),
            WiringOutcome::AlreadyWired {
                adhoc: Some("/etc/bepository/sa-key.json".into())
            }
        );
        // The manual drop-in is left untouched.
        assert!(dropin_dir.join("override.conf").exists());
        assert!(!dropin_dir.join("gcs-sa-key.conf").exists());
    }

    #[test]
    fn unwire_restores_sources_and_removes_dropins() {
        let tmp = tempfile::tempdir().unwrap();
        let env_path = tmp.path().join("env");
        let dropin_dir = tmp.path().join("bepository.service.d");
        fs::create_dir_all(&dropin_dir).unwrap();
        fs::write(
            dropin_dir.join("sftp-key.conf"),
            dropin_content("sftp-key", "/home/u/.ssh/id_ed25519"),
        )
        .unwrap();
        fs::write(
            dropin_dir.join("gcs-sa-key.conf"),
            dropin_content("gcs-sa-key", "/etc/bepository/sa-key.json"),
        )
        .unwrap();
        // A user's own drop-in must survive — and keep the dir alive.
        fs::write(
            dropin_dir.join("override.conf"),
            "[Service]\nLoadCredential=other:/x\n",
        )
        .unwrap();
        fs::write(
            &env_path,
            "BEPOSITORY_STORAGE_URI=sftp://u@h/srv?key=/run/credentials/bepository.service/sftp-key\n\
             GOOGLE_APPLICATION_CREDENTIALS=/run/credentials/bepository.service/gcs-sa-key\n",
        )
        .unwrap();

        unwire_credential_dropins(&env_path, &dropin_dir).unwrap();

        let env = fs::read_to_string(&env_path).unwrap();
        assert_eq!(
            sftp_extract(&env),
            Some(Some("/home/u/.ssh/id_ed25519".into()))
        );
        assert_eq!(
            gcs_extract(&env),
            Some(Some("/etc/bepository/sa-key.json".into()))
        );
        assert!(!dropin_dir.join("sftp-key.conf").exists());
        assert!(!dropin_dir.join("gcs-sa-key.conf").exists());
        assert!(dropin_dir.join("override.conf").exists());

        // Without the user's drop-in, the now-empty dir goes away too.
        fs::remove_file(dropin_dir.join("override.conf")).unwrap();
        unwire_credential_dropins(&env_path, &dropin_dir).unwrap();
        assert!(!dropin_dir.exists());
    }
}
