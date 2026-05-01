//! In-memory HTTP response cache with TTL + in-flight singleflight.
//!
//! Two queries that resolve to the exact same URL share one HTTP fetch,
//! whether they fire sequentially (TTL hit) or concurrently (singleflight
//! dedup). The cache is keyed by the final URL string after pushdown +
//! pagination expansion, so a `LIMIT 10` and a `LIMIT 100` against the
//! same table do *not* collide — they produce different URLs and that's
//! the right behaviour.
//!
//! Auth is intentionally not part of the key. Each `RestTable` carries
//! its own cache instance scoped to the source's auth, so two tables
//! configured against the same URL with different credentials get
//! distinct caches by construction.
//!
//! Eviction is FIFO on insertion order with a hard cap on entries; lazy
//! TTL purge happens on lookup. This is enough for the steady-state of
//! a SQL workload — a more sophisticated LRU is unnecessary here.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::Url;
use tokio::sync::Notify;

use crate::HttpPageResponse;
use crate::Result;

const DEFAULT_MAX_ENTRIES: usize = 1024;

/// HTTP response cache shared across queries on a single `RestTable`.
///
/// Cheap to clone (internal `Arc`). Construct once per source and pass
/// the same instance to every table that should share a cache.
#[derive(Clone, Debug)]
pub struct HttpCache {
    inner: Arc<Mutex<Inner>>,
    ttl: Duration,
}

#[derive(Debug)]
struct Inner {
    entries: HashMap<String, Slot>,
    insertion_order: VecDeque<String>,
    max_entries: usize,
    metrics: Metrics,
}

#[derive(Default, Debug, Clone)]
pub struct Metrics {
    pub hits: u64,
    pub misses: u64,
    pub coalesced: u64,
    pub evictions: u64,
}

#[derive(Debug)]
enum Slot {
    /// A request for this key is in flight; subsequent callers park on
    /// the `Notify` and re-check the slot when woken.
    Pending(Arc<Notify>),
    /// Completed response. `inserted_at` drives TTL.
    Ready {
        response: HttpPageResponse,
        inserted_at: Instant,
    },
}

enum PeekOutcome {
    Hit(HttpPageResponse),
    Wait(Arc<Notify>),
    Lead(Arc<Notify>),
}

enum SlotSnapshot {
    Pending(Arc<Notify>),
    Ready(HttpPageResponse, Instant),
}

impl HttpCache {
    pub fn new(ttl: Duration, max_entries: Option<usize>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                entries: HashMap::new(),
                insertion_order: VecDeque::new(),
                max_entries: max_entries.unwrap_or(DEFAULT_MAX_ENTRIES).max(1),
                metrics: Metrics::default(),
            })),
            ttl,
        }
    }

    /// Snapshot of cache counters. Mostly used by tests and diagnostics.
    pub fn metrics(&self) -> Metrics {
        self.inner.lock().unwrap().metrics.clone()
    }

    /// Get the cached response for `url`, or run `fetch` once with
    /// singleflight semantics and cache the result on success.
    ///
    /// On error the slot is removed and the error is returned only to
    /// the originating caller; concurrent waiters are woken and will
    /// see the slot vanish, then retry the fetch themselves. This keeps
    /// transient failures from poisoning subsequent requests.
    pub(crate) async fn get_or_fetch<F, Fut>(&self, url: &Url, fetch: F) -> Result<HttpPageResponse>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<HttpPageResponse>>,
    {
        let key = url.as_str().to_string();
        let mut fetch = Some(fetch);

        loop {
            let outcome = self.peek_or_install(&key);
            match outcome {
                PeekOutcome::Hit(response) => return Ok(response),
                PeekOutcome::Wait(notify) => {
                    notify.notified().await;
                    // Loop: slot may now be Ready (TTL hit on next pass),
                    // missing (leader errored — we'll become the new
                    // leader), or still Pending in a rare race.
                }
                PeekOutcome::Lead(notify) => {
                    return self
                        .lead_fetch(key, notify, fetch.take().expect("fetch consumed once"))
                        .await;
                }
            }
        }
    }

    fn peek_or_install(&self, key: &str) -> PeekOutcome {
        let mut inner = self.inner.lock().unwrap();
        let snapshot = inner.entries.get(key).map(|slot| match slot {
            Slot::Pending(n) => SlotSnapshot::Pending(n.clone()),
            Slot::Ready {
                response,
                inserted_at,
            } => SlotSnapshot::Ready(response.clone(), *inserted_at),
        });

        match snapshot {
            Some(SlotSnapshot::Ready(response, inserted_at))
                if inserted_at.elapsed() <= self.ttl =>
            {
                inner.metrics.hits += 1;
                PeekOutcome::Hit(response)
            }
            Some(SlotSnapshot::Ready(_, _)) => {
                inner.entries.remove(key);
                inner.metrics.misses += 1;
                let notify = Arc::new(Notify::new());
                inner
                    .entries
                    .insert(key.to_string(), Slot::Pending(notify.clone()));
                PeekOutcome::Lead(notify)
            }
            Some(SlotSnapshot::Pending(notify)) => {
                inner.metrics.coalesced += 1;
                PeekOutcome::Wait(notify)
            }
            None => {
                inner.metrics.misses += 1;
                let notify = Arc::new(Notify::new());
                inner
                    .entries
                    .insert(key.to_string(), Slot::Pending(notify.clone()));
                PeekOutcome::Lead(notify)
            }
        }
    }

    async fn lead_fetch<F, Fut>(
        &self,
        key: String,
        notify: Arc<Notify>,
        fetch: F,
    ) -> Result<HttpPageResponse>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<HttpPageResponse>>,
    {
        let result = fetch().await;
        let mut inner = self.inner.lock().unwrap();
        match &result {
            Ok(response) => {
                inner.entries.insert(
                    key.clone(),
                    Slot::Ready {
                        response: response.clone(),
                        inserted_at: Instant::now(),
                    },
                );
                inner.insertion_order.push_back(key.clone());
                while inner.insertion_order.len() > inner.max_entries {
                    if let Some(victim) = inner.insertion_order.pop_front() {
                        if victim != key && inner.entries.remove(&victim).is_some() {
                            inner.metrics.evictions += 1;
                        }
                    }
                }
            }
            Err(_) => {
                // Don't cache errors. Remove the Pending slot so retries
                // can attempt a fresh fetch; coalesced waiters wake up,
                // see no slot, and re-enter the leader path themselves.
                inner.entries.remove(&key);
            }
        }
        drop(inner);
        notify.notify_waiters();
        result
    }
}

// Make the cache constructible from the YAML schema without dragging the
// config crate into private surface elsewhere.
impl HttpCache {
    pub fn from_config(cfg: &dbfy_config::CacheConfig) -> Self {
        Self::new(Duration::from_secs(cfg.ttl_seconds), cfg.max_entries)
    }
}
