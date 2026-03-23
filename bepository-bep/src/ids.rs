// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use aliri_braid::braid;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

/// A BEP folder ID: the unique machine-readable identifier for a shared folder (e.g., "default").
///
/// The string representation is interned so this type is cheaply cloneable and comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FolderId(Ustr);

impl FolderId {
    #[must_use]
    pub fn new(s: &str) -> Self {
        Self(Ustr::from(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        self.0.as_str()
    }
}

impl From<&str> for FolderId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for FolderId {
    fn from(s: String) -> Self {
        Self::new(&s)
    }
}

impl std::fmt::Display for FolderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A BEP folder label: the human-readable display name for a shared folder.
#[braid(serde)]
pub struct FolderLabel;
