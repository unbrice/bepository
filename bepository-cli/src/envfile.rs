// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Parser for `/etc/bepository/env`-style files, shared by `load_env_file`
//! and the service installer.
//!
//! Rules mirror systemd's `EnvironmentFile`: `KEY=VALUE` lines, blank lines
//! and `#` comments skipped, one surrounding pair of double quotes stripped
//! from the value. No shell expansion — values are literal.

/// Iterate the `KEY=VALUE` assignments in `text`. Malformed lines (no `=`)
/// are skipped.
pub(crate) fn parse_env_lines(text: &str) -> impl Iterator<Item = (&str, &str)> {
    text.lines().filter_map(|raw| {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        line.split_once('=')
            .map(|(key, value)| (key, unquote(value)))
    })
}

/// Strip one surrounding pair of double quotes, matching systemd semantics.
pub(crate) fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value)
}

/// Return `text` with the first assignment to `key` replaced by `key=value`,
/// or the assignment appended when absent. A quoted existing line is replaced
/// whole; every other line (comments, duplicates) is kept verbatim.
// Only the service installer (self-manage) rewrites env files.
#[cfg_attr(not(feature = "self-manage"), allow(dead_code))]
pub(crate) fn set_env_assignment(text: &str, key: &str, value: &str) -> String {
    let mut out = String::with_capacity(text.len() + key.len() + value.len() + 2);
    let mut replaced = false;
    for raw in text.lines() {
        let is_target = !replaced && {
            let line = raw.trim();
            !line.is_empty()
                && !line.starts_with('#')
                && line.split_once('=').is_some_and(|(k, _)| k == key)
        };
        if is_target {
            replaced = true;
            out.push_str(key);
            out.push('=');
            out.push_str(value);
        } else {
            out.push_str(raw);
        }
        out.push('\n');
    }
    if !replaced {
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_lines_applies_environment_file_rules() {
        let text = "\n  \n# comment\nA=1\nB=\"two\"\nmalformed\nC=a=b\n D = spaced\n";
        let pairs: Vec<(&str, &str)> = parse_env_lines(text).collect();
        // ` D = spaced`: the trimmed line splits at '=', leaving the trailing
        // space on the key and the leading one on the value — same semantics
        // as load_env_file.
        assert_eq!(
            pairs,
            [("A", "1"), ("B", "two"), ("C", "a=b"), ("D ", " spaced")]
        );
    }

    #[test]
    fn unquote_strips_one_pair_only() {
        assert_eq!(unquote("\"a\""), "a");
        assert_eq!(unquote("\"\"a\"\""), "\"a\"");
        assert_eq!(unquote("\"\""), "");
        assert_eq!(unquote("\"a"), "\"a");
        assert_eq!(unquote("a\""), "a\"");
        assert_eq!(unquote("\""), "\"");
        assert_eq!(unquote("'a'"), "'a'");
        assert_eq!(unquote(""), "");
    }

    #[test]
    fn set_env_assignment_replaces_first_and_appends() {
        // First occurrence replaced, later duplicates kept verbatim.
        assert_eq!(set_env_assignment("A=1\nA=2\n", "A", "9"), "A=9\nA=2\n");
        // A quoted line is replaced whole; comments and blanks preserved.
        assert_eq!(
            set_env_assignment("# c\n\nA=\"old value\"\nB=2\n", "A", "new"),
            "# c\n\nA=new\nB=2\n"
        );
        // Appended when absent, even without a trailing newline.
        assert_eq!(set_env_assignment("A=1", "B", "2"), "A=1\nB=2\n");
        assert_eq!(set_env_assignment("", "A", "1"), "A=1\n");
        // A commented-out assignment does not count as an occurrence.
        assert_eq!(set_env_assignment("#A=1\n", "A", "2"), "#A=1\nA=2\n");
        // A key with trailing whitespace before '=' is a different key.
        assert_eq!(set_env_assignment("A =1\n", "A", "2"), "A =1\nA=2\n");
    }
}
