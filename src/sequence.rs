//! Sequence (`seq`) and birth/death sequence (`bdSeq`) counters, plus bdSeq
//! persistence (spec §6.4 sequence rules).
//!
//! Both counters are `0..=255` and wrap `255 -> 0`; `u8::wrapping_add` gives the
//! spec's wrap for free (ADR-6), avoiding the `== 256` sentinel of the Java/Python
//! references.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};

/// The payload sequence number (`seq`), reset to 0 on every (re)birth and
/// incremented by one (mod 256) on every subsequent Edge Node message
/// (`tck-id-payloads-sequence-num-incrementing`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Seq(u8);

impl Seq {
    /// A counter starting at 0 (the value an NBIRTH carries).
    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }

    /// The current value (without advancing).
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// Return the current value, then advance (wrapping `255 -> 0`). Use this
    /// to stamp a message's `seq` and move the counter forward.
    pub fn next_value(&mut self) -> u8 {
        let current = self.0;
        self.0 = self.0.wrapping_add(1);
        current
    }

    /// Reset to 0 (on NBIRTH / rebirth).
    pub fn reset(&mut self) {
        self.0 = 0;
    }
}

/// The birth/death sequence number (`bdSeq`), incremented once per MQTT CONNECT
/// (not per message) and persisted across restarts so the Host can correlate an
/// NDEATH with its NBIRTH (`tck-id-payloads-nbirth-bdseq*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct BdSeq(u8);

impl BdSeq {
    /// A counter starting at `start`.
    #[must_use]
    pub const fn new(start: u8) -> Self {
        Self(start)
    }

    /// The current value.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// Advance by one (wrapping `255 -> 0`).
    pub fn advance(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

/// Persistence for the bdSeq counter so it survives process restarts.
///
/// `load_next_death` returns the bdSeq value to use for the next connection's
/// NDEATH (and matching NBIRTH); `store_next_death` persists the *next* value.
pub trait BdSeqStore {
    /// Load the bdSeq value to use for the next death/will (0 if none stored).
    ///
    /// # Errors
    /// Returns an I/O error if a stored value exists but cannot be read/parsed.
    fn load_next_death(&self) -> std::io::Result<u8>;

    /// Persist the bdSeq value to use for the next connection.
    ///
    /// # Errors
    /// Returns an I/O error if the value cannot be written.
    fn store_next_death(&self, value: u8) -> std::io::Result<()>;
}

/// An in-memory bdSeq store (non-persistent; useful for tests and ephemeral nodes).
#[derive(Debug, Default)]
pub struct InMemoryBdSeqStore {
    value: AtomicU8,
}

impl InMemoryBdSeqStore {
    /// A store seeded with `start`.
    #[must_use]
    pub fn new(start: u8) -> Self {
        Self {
            value: AtomicU8::new(start),
        }
    }
}

impl BdSeqStore for InMemoryBdSeqStore {
    fn load_next_death(&self) -> std::io::Result<u8> {
        Ok(self.value.load(Ordering::SeqCst))
    }

    fn store_next_death(&self, value: u8) -> std::io::Result<()> {
        self.value.store(value, Ordering::SeqCst);
        Ok(())
    }
}

/// A file-backed bdSeq store. Writes atomically (temp file + rename) to a
/// **durable** path (unlike Tahu's OS temp dir).
#[derive(Clone, Debug)]
pub struct FileBdSeqStore {
    path: PathBuf,
}

impl FileBdSeqStore {
    /// A store backed by `path`. The parent directory must exist.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl BdSeqStore for FileBdSeqStore {
    fn load_next_death(&self) -> std::io::Result<u8> {
        match std::fs::read_to_string(&self.path) {
            Ok(s) => s.trim().parse::<u8>().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("corrupt bdSeq file {:?}: {e}", self.path),
                )
            }),
            // A missing file means "never connected"; start at 0.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn store_next_death(&self, value: u8) -> std::io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, value.to_string())?;
        std::fs::rename(&tmp, &self.path)
    }
}
