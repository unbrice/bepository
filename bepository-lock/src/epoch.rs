// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fmt;

use crate::base32::{self, Base32Error};

/// A lock epoch — a monotonically increasing identifier for lock generations.
///
/// Caches the 8-character Crockford Base32 representation for efficient
/// use in filenames and key encoding. `Copy` (16 bytes: u64 + `[u8; 8]`).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Epoch {
    val: u64,
    base32: [u8; 8],
}

impl Epoch {
    /// Create an epoch from a numeric value.
    ///
    /// Returns an error if `val` exceeds the 40-bit Crockford Base32 limit.
    pub fn new(val: u64) -> Result<Self, Base32Error> {
        let s = base32::to_base32(val)?;
        let mut b = [0u8; 8];
        b.copy_from_slice(s.as_bytes());
        Ok(Self { val, base32: b })
    }

    /// Parse an epoch from its Base32 string representation.
    ///
    /// Accepts the bare 8-character string or with a `.json` suffix
    /// (as used in epoch filenames).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let val = base32::from_base32(s)?;
        // from_base32 guarantees val fits in 40 bits, so new() cannot fail.
        Some(Self::new(val).expect("from_base32 returned a valid value"))
    }

    /// The numeric epoch value.
    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.val
    }

    /// The 8-character Crockford Base32 representation.
    #[must_use]
    pub fn as_base32(&self) -> &str {
        // base32 is always valid ASCII produced by to_base32.
        unsafe { std::str::from_utf8_unchecked(&self.base32) }
    }

    /// The epoch filename (e.g. `"00000005.json"`).
    #[must_use]
    pub fn json_filename(&self) -> String {
        format!("{}.json", self.as_base32())
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_base32())
    }
}

impl fmt::Debug for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Epoch({}={})", self.val, self.as_base32())
    }
}

impl PartialOrd for Epoch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Epoch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.val.cmp(&other.val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for val in [0, 1, 31, 32, 1023, (1u64 << 40) - 1] {
            let e = Epoch::new(val).unwrap();
            assert_eq!(e.as_u64(), val);
            let parsed = Epoch::parse(e.as_base32()).unwrap();
            assert_eq!(parsed, e);
        }
    }

    #[test]
    fn parse_with_json_suffix() {
        let e = Epoch::new(42).unwrap();
        let parsed = Epoch::parse(&e.json_filename()).unwrap();
        assert_eq!(parsed, e);
    }

    #[test]
    fn overflow() {
        assert!(Epoch::new(1u64 << 40).is_err());
    }

    #[test]
    fn ordering_matches_numeric() {
        let a = Epoch::new(5).unwrap();
        let b = Epoch::new(10).unwrap();
        assert!(a < b);
    }

    #[test]
    fn display_shows_base32() {
        let e = Epoch::new(0).unwrap();
        assert_eq!(e.to_string(), "00000000");
    }
}
