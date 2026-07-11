// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Export the compile-time target triple so `upgrade` can pick its release asset.

fn main() {
    let target = std::env::var("TARGET").expect("TARGET is always set by cargo during builds");
    println!("cargo:rustc-env=TARGET_TRIPLE={target}");
}
