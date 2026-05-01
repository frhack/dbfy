//! Tiny serializable Bloom filter, sized at construction with a target
//! capacity and false-positive rate. No external dep — built on `twox-hash`
//! which is already in the crate's tree.
//!
//! Layout invariants (held by serde):
//!   bits.len() == ceil(m / 8)
//!   k seeds, each one xxhash with that seed of the input bytes
//!
//! For Sprint 2 the per-chunk bloom stores at most `bloom_capacity` items
//! (defaults to chunk_rows). Caller is expected to size `expected_items`
//! to match the actual chunk size; oversizing wastes bytes, undersizing
//! degrades the FPR.

use serde::{Deserialize, Serialize};
use twox_hash::xxh3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bloom {
    bits: Vec<u8>,
    /// Number of bits (`m`). Always positive.
    m: u32,
    /// Number of hash functions (`k`). Always positive, capped at 16.
    k: u8,
    /// One seed per hash function. `seeds.len() == k`.
    seeds: Vec<u64>,
}

impl Bloom {
    pub fn new(expected_items: usize, fpr: f64) -> Self {
        let n = expected_items.max(1) as f64;
        let p = fpr.clamp(1e-9, 0.5);
        let m = ((-n * p.ln()) / (2.0_f64.ln().powi(2))).ceil() as u32;
        let m = m.max(64);
        let k = ((m as f64 / n) * 2.0_f64.ln()).round() as u8;
        let k = k.clamp(1, 16);
        let seeds: Vec<u64> = (0..k as u64)
            .map(|i| {
                0x9E37_79B9_7F4A_7C15u64
                    .wrapping_mul(i + 1)
                    .wrapping_add(0xDEAD_BEEF)
            })
            .collect();
        Self {
            bits: vec![0u8; ((m + 7) / 8) as usize],
            m,
            k,
            seeds,
        }
    }

    pub fn add_bytes(&mut self, bytes: &[u8]) {
        for &seed in &self.seeds {
            let bit = (xxh3::hash64_with_seed(bytes, seed) % self.m as u64) as u32;
            self.bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
        }
    }

    pub fn may_contain_bytes(&self, bytes: &[u8]) -> bool {
        for &seed in &self.seeds {
            let bit = (xxh3::hash64_with_seed(bytes, seed) % self.m as u64) as u32;
            if self.bits[(bit / 8) as usize] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn add_str(&mut self, s: &str) {
        self.add_bytes(s.as_bytes())
    }

    pub fn may_contain_str(&self, s: &str) -> bool {
        self.may_contain_bytes(s.as_bytes())
    }

    pub fn add_i64(&mut self, v: i64) {
        self.add_bytes(&v.to_le_bytes())
    }

    pub fn may_contain_i64(&self, v: i64) -> bool {
        self.may_contain_bytes(&v.to_le_bytes())
    }

    pub fn byte_len(&self) -> usize {
        self.bits.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut b = Bloom::new(1000, 0.01);
        for i in 0..1000 {
            b.add_str(&format!("user-{i}"));
        }
        for i in 0..1000 {
            assert!(b.may_contain_str(&format!("user-{i}")));
        }
    }

    #[test]
    fn fpr_within_target() {
        let mut b = Bloom::new(1000, 0.01);
        for i in 0..1000 {
            b.add_str(&format!("present-{i}"));
        }
        let mut hits = 0;
        let trials = 10_000;
        for i in 0..trials {
            if b.may_contain_str(&format!("absent-{i}")) {
                hits += 1;
            }
        }
        // Expected ~1 % false positive; allow 5x slack for the test.
        assert!(
            hits as f64 / trials as f64 <= 0.05,
            "FPR too high: {hits}/{trials}"
        );
    }
}
