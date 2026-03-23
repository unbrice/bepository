// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

/// Crockford Base32 alphabet: lexicographic order matches numeric order,
/// and ambiguous characters (I, L, O) are excluded.
pub const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Maximum value encodable in 8 base32 characters (40 bits).
const MAX_VAL: u64 = (1u64 << 40) - 1;

/// Encode a u64 as an 8-character Crockford Base32 string.
///
/// Returns an error if `val` exceeds 2^40 - 1 (the 40-bit limit of 8 base32 characters).
pub fn to_base32(mut val: u64) -> Result<String, Base32Error> {
    if val > MAX_VAL {
        return Err(Base32Error::Overflow(val));
    }
    let mut s = String::with_capacity(8);
    for _ in 0..8 {
        s.push(ALPHABET[(val & 0x1F) as usize] as char);
        val >>= 5;
    }
    Ok(s.chars().rev().collect())
}

#[must_use]
pub fn from_base32(s: &str) -> Option<u64> {
    let s = s.strip_suffix(".json").unwrap_or(s);
    if s.len() != 8 {
        return None;
    }
    let mut val = 0u64;
    for c in s.chars() {
        let idx = ALPHABET.iter().position(|&x| x as char == c)?;
        val = (val << 5) | (idx as u64);
    }
    Some(val)
}

#[derive(Debug, thiserror::Error)]
pub enum Base32Error {
    #[error("value {0} exceeds 40-bit base32 limit (max {MAX_VAL})")]
    Overflow(u64),
}
