// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fmt::{self, Write};
use std::str::FromStr;
use thiserror::Error;

/// A Syncthing device ID: Luhn-encoded SHA-256 of the device's TLS certificate.
/// Displayed as XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX-XXXXXXX
/// (52 base32 data chars + 4 Luhn check chars = 56 alphanumeric + 7 hyphens = 63 chars).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeviceId(pub [u8; 32]);

const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeviceIdError {
    #[error("invalid length: expected 56 or 63 characters, got {0}")]
    InvalidLength(usize),
    #[error("invalid character: {0}")]
    InvalidCharacter(char),
    #[error("invalid check digit")]
    InvalidCheckDigit,
}

impl DeviceId {
    /// Create a DeviceId from the raw 32-byte SHA-256 hash of a certificate.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parse a DeviceId from its string representation.
    ///
    /// Accepts the canonical `XXXXXXX-XXXXXXX-…` format (63 chars with hyphens,
    /// 56 chars without).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        s.parse().ok()
    }

    /// The raw 32-byte hash.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Encode to the canonical string format.
    ///
    /// Algorithm (matches Go master):
    /// 1. Base32-encode the 32 bytes → 52 chars
    /// 2. "luhnify": split into 4 groups of 13, append a Luhn check → 56 chars
    /// 3. "chunkify": split into groups of 7, join with hyphens → 63 chars
    #[must_use]
    pub fn to_string_repr(&self) -> String {
        self.to_string()
    }
}

impl FromStr for DeviceId {
    type Err = DeviceIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut clean = [0u8; 56];
        let mut count = 0;

        for &b in s.as_bytes() {
            if b == b'-' {
                continue;
            }
            if count < 56 {
                clean[count] = b.to_ascii_uppercase();
            }
            count += 1;
        }

        if count != 56 {
            return Err(DeviceIdError::InvalidLength(count));
        }

        let mut bytes = [0u8; 32];
        let mut buffer = 0u64;
        let mut bits = 0;
        let mut byte_idx = 0;

        // Iterate over the 4 "luhnified" groups of 14 characters
        for chunk in clean.chunks_exact(14) {
            let group = &chunk[..13];
            let expected_check = chunk[13];

            if luhn_check(group)? != expected_check {
                return Err(DeviceIdError::InvalidCheckDigit);
            }

            // Immediately decode the valid 13 bytes into the bit accumulator
            for &c in group {
                let val = decode_char(c).ok_or(DeviceIdError::InvalidCharacter(c as char))?;
                buffer = (buffer << 5) | (val as u64);
                bits += 5;

                while bits >= 8 {
                    bits -= 8;
                    if byte_idx < 32 {
                        bytes[byte_idx] = u8::try_from((buffer >> bits) & 0xFF).unwrap();
                        byte_idx += 1;
                    }
                }
            }
        }
        Ok(Self(bytes))
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Step 1: Base32 encode 32 bytes -> 52 chars
        let mut base32 = [0u8; 52];
        let mut buffer = 0u64;
        let mut bits = 0;
        let mut char_idx = 0;

        for &b in &self.0 {
            buffer = (buffer << 8) | (b as u64);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                base32[char_idx] = ALPHABET[((buffer >> bits) & 0x1f) as usize];
                char_idx += 1;
            }
        }
        if bits > 0 {
            base32[char_idx] = ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize];
        }

        // Step 2 & 3: Luhnify and chunkify
        let mut luhnified = [0u8; 56];
        for i in 0..4 {
            let src_group = &base32[i * 13..(i + 1) * 13];
            let dst_group = &mut luhnified[i * 14..(i + 1) * 14];
            dst_group[..13].copy_from_slice(src_group);
            dst_group[13] = luhn_check(src_group).expect("valid alphabet");
        }

        for (i, chunk) in luhnified.chunks(7).enumerate() {
            if i > 0 {
                f.write_char('-')?;
            }
            for &b in chunk {
                f.write_char(b as char)?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeviceId({self})")
    }
}

#[inline]
const fn decode_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a'),
        b'2'..=b'7' => Some(c - b'2' + 26),
        _ => None,
    }
}

fn luhn_check(group: &[u8]) -> Result<u8, DeviceIdError> {
    const N: u32 = 32;
    let mut sum = 0;
    for (&ch, &f) in group.iter().zip([1u32, 2].iter().cycle()) {
        let val = decode_char(ch).ok_or(DeviceIdError::InvalidCharacter(ch as char))?;
        let a = f * val as u32;
        sum += a / N + a % N;
    }
    let remainder = sum % N;
    Ok(ALPHABET[((N - remainder) % N) as usize])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB,
            0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67,
            0x89, 0xAB, 0xCD, 0xEF,
        ];
        let id = DeviceId::from_bytes(bytes);
        let s = id.to_string();

        // 63 chars total: 56 base32+check chars + 7 hyphens
        assert_eq!(s.len(), 63, "canonical form should be 63 chars: got {s}");

        // Should round-trip
        let parsed = DeviceId::parse(&s).expect("should parse");
        assert_eq!(parsed.as_bytes(), &bytes);
    }

    #[test]
    fn display_format() {
        let id = DeviceId::from_bytes([0; 32]);
        let s = id.to_string();
        // Should contain hyphens
        assert!(s.contains('-'));
        // 7 hyphens
        assert_eq!(s.chars().filter(|c| *c == '-').count(), 7);
    }

    #[test]
    fn test_invalid_checks() {
        let bytes = [0u8; 32];
        let s = DeviceId::from_bytes(bytes).to_string();
        // Flip one character in the first group
        let mut bytes_s = s.into_bytes();
        bytes_s[0] = if bytes_s[0] == b'A' { b'B' } else { b'A' };
        let s = String::from_utf8(bytes_s).unwrap();
        assert_eq!(
            DeviceId::from_str(&s),
            Err(DeviceIdError::InvalidCheckDigit)
        );
    }

    #[test]
    fn ordering() {
        let a = DeviceId::from_bytes([0; 32]);
        let b = DeviceId::from_bytes([1; 32]);
        assert!(a < b);
    }
}
