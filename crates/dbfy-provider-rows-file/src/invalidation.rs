//! Five-branch decision tree for index validity. See `Decision` for the
//! full enumeration and `decide` for the selection rule.

use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use twox_hash::xxh3;

use crate::index::Index;

/// Maximum bytes hashed for the prefix check. 4 KiB is plenty to fingerprint
/// a file's beginning while still being one IO read.
pub const PREFIX_HASH_WINDOW: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Index valid — file unchanged. No re-parse.
    Reuse,
    /// File grew, prefix unchanged → append-only path. Index and stats stay
    /// valid up to the cached `file_size`; only the new tail needs scanning.
    Extend,
    /// Anything else: index is stale, drop it and rebuild from scratch.
    Rebuild,
}

/// File metadata used by the indexer to seed the cached header. Captured
/// once per build/scan; the `prefix_hash` is computed over the first
/// `prefix_hash_window` bytes of the file (see [`PREFIX_HASH_WINDOW`]).
#[derive(Debug, Clone)]
pub struct FileSnapshot {
    pub size: u64,
    pub mtime_micros: i64,
    pub inode: u64,
    pub prefix_hash: u64,
    pub prefix_hash_window: u32,
}

impl FileSnapshot {
    /// Cheap snapshot — `stat` plus a single read of up to
    /// `PREFIX_HASH_WINDOW` bytes for the hash.
    pub fn capture(path: &Path) -> std::io::Result<Self> {
        let meta = std::fs::metadata(path)?;
        let size = meta.len();
        let mtime_micros = meta
            .modified()
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let inode = meta.ino();

        let window = (size.min(PREFIX_HASH_WINDOW as u64)) as u32;
        let mut buf = vec![0u8; window as usize];
        let mut file = File::open(path)?;
        file.read_exact(&mut buf)?;
        let prefix_hash = xxh3::hash64(&buf);

        Ok(Self {
            size,
            mtime_micros,
            inode,
            prefix_hash,
            prefix_hash_window: window,
        })
    }
}

/// Re-hash the cached window from the current file. Used by [`decide`] to
/// verify that an apparent append-only growth really preserved the prefix.
fn rehash_prefix(path: &Path, window: u32) -> std::io::Result<u64> {
    if window == 0 {
        return Ok(xxh3::hash64(&[]));
    }
    let mut buf = vec![0u8; window as usize];
    let mut file = File::open(path)?;
    file.read_exact(&mut buf)?;
    Ok(xxh3::hash64(&buf))
}

/// Decide what to do given a fresh snapshot and a cached index. The path
/// is consulted only when an apparent append needs prefix verification.
pub fn decide(snapshot: &FileSnapshot, cached: &Index) -> Decision {
    decide_with_path(snapshot, cached, None)
}

/// Variant that re-hashes the cached prefix window from `path` to verify
/// the prefix actually matches when the file grew past the cached size.
/// `decide` (without `path`) falls back to comparing snapshot.prefix_hash
/// directly, which is correct when both ranges are equal.
pub fn decide_with_path(snapshot: &FileSnapshot, cached: &Index, path: Option<&Path>) -> Decision {
    // 1. Inode change beats everything: file replaced (mv tmp file, logrotate).
    if snapshot.inode != cached.file_inode {
        return Decision::Rebuild;
    }

    // 2. Truncation or backup-restore.
    if snapshot.size < cached.file_size || snapshot.mtime_micros < cached.file_mtime_micros {
        return Decision::Rebuild;
    }

    // 3. Same size, same mtime — almost certainly unchanged. Paranoia check
    // on the prefix hash catches the rare same-size in-place edit. Both
    // hashes are over the same window (= file_size when file ≤ 4 KiB) so
    // direct comparison is meaningful.
    if snapshot.size == cached.file_size && snapshot.mtime_micros == cached.file_mtime_micros {
        if snapshot.prefix_hash == cached.prefix_hash {
            return Decision::Reuse;
        }
        return Decision::Rebuild;
    }

    // 4. File grew. The snapshot's prefix_hash is over its NEW prefix
    // window, which may be wider than the cached window if the file grew
    // past 4 KiB or simply has more bytes available now. Re-hash the
    // CACHED window from the current file to compare apples-to-apples.
    let observed = match path {
        Some(p) => rehash_prefix(p, cached.prefix_hash_window).ok(),
        // Fallback when caller cannot provide a path: only safe when the
        // windows happen to match (e.g., both files were ≥ 4 KiB).
        None => {
            if snapshot.prefix_hash_window == cached.prefix_hash_window {
                Some(snapshot.prefix_hash)
            } else {
                None
            }
        }
    };
    match observed {
        Some(h) if h == cached.prefix_hash => Decision::Extend,
        _ => Decision::Rebuild,
    }
}
