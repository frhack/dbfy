//! Node.js bindings for the embedded dbfy federated SQL engine.
//!
//! Exports a single `Engine` class with sync + async query methods.
//! `napi-rs` generates the JavaScript class, the Promise<Buffer>
//! marshalling, and the TypeScript `.d.ts` definitions.
//!
//! Async semantics: `napi-rs` runs `async fn` methods on its own
//! tokio runtime and converts the future into a JS `Promise`.
//! Resolution / rejection happens on the V8 main thread via
//! `ThreadsafeFunction`, so JS callers never observe results from
//! a tokio worker.

#![deny(clippy::all)]

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use dbfy_config::Config;
use dbfy_frontend_datafusion::Engine as InnerEngine;
use napi::bindgen_prelude::*;
use napi_derive::napi;

/// Encode a slice of RecordBatches as a single Arrow IPC stream.
fn batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(to_napi)?;
        for batch in batches {
            writer.write(batch).map_err(to_napi)?;
        }
        writer.finish().map_err(to_napi)?;
    }
    Ok(buf)
}

fn to_napi<E: std::fmt::Display>(err: E) -> Error {
    Error::new(Status::GenericFailure, err.to_string())
}

/// SQL federation engine. Configure once via {@link fromYaml} or
/// {@link fromPath}, then call {@link query} (async) or {@link querySync}.
#[napi]
pub struct Engine {
    inner: Arc<InnerEngine>,
}

#[napi]
impl Engine {
    /// Build an engine from an inline YAML configuration string.
    #[napi(factory, js_name = "fromYaml")]
    pub fn from_yaml(yaml: String) -> Result<Self> {
        let config = Config::from_yaml_str(&yaml).map_err(to_napi)?;
        let inner = InnerEngine::from_config(config).map_err(to_napi)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Build an engine from a YAML configuration file path.
    #[napi(factory, js_name = "fromPath")]
    pub fn from_path(path: String) -> Result<Self> {
        let inner = InnerEngine::from_config_file(&path).map_err(to_napi)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Build an engine with no preconfigured sources.
    #[napi(factory, js_name = "newEmpty")]
    pub fn new_empty() -> Result<Self> {
        Ok(Self {
            inner: Arc::new(InnerEngine::default()),
        })
    }

    /// Execute SQL and return a Promise that resolves with the
    /// result encoded as Arrow IPC stream bytes (a Buffer in JS).
    /// The Promise is rejected with the dbfy error message on
    /// failure.
    ///
    /// napi-rs spawns the async fn on its own tokio runtime;
    /// resolution / rejection lands back on the V8 main thread via
    /// a ThreadsafeFunction.
    #[napi]
    pub async fn query(&self, sql: String) -> Result<Buffer> {
        let batches = self.inner.query(&sql).await.map_err(to_napi)?;
        let bytes = batches_to_ipc(&batches)?;
        Ok(bytes.into())
    }

    /// Synchronous variant — blocks the calling JS thread until the
    /// query completes. Use only for short queries; prefer
    /// {@link query} for anything else, especially in
    /// request-handling paths.
    #[napi(js_name = "querySync")]
    pub fn query_sync(&self, sql: String) -> Result<Buffer> {
        let runtime = tokio::runtime::Handle::try_current().or_else(|_| {
            // No ambient runtime — make a one-shot. The async path
            // (the `query` method above) reuses napi-rs's runtime so
            // this branch matters only for callers that for whatever
            // reason call `querySync` from a non-async-aware host.
            tokio::runtime::Runtime::new()
                .map(|rt| rt.handle().clone())
                .map_err(to_napi)
        })?;
        let batches = runtime.block_on(self.inner.query(&sql)).map_err(to_napi)?;
        let bytes = batches_to_ipc(&batches)?;
        Ok(bytes.into())
    }

    /// Render the optimised logical plan + per-source pushdown
    /// summary as a human-readable string.
    #[napi]
    pub async fn explain(&self, sql: String) -> Result<String> {
        self.inner.explain(&sql).await.map_err(to_napi)
    }
}
