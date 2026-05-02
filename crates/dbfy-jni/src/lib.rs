//! JNI bindings for the embedded dbfy federated SQL engine.
//!
//! Native methods are exported under `Java_com_dbfy_Dbfy_*` for the
//! companion class in `java/com/dbfy/Dbfy.java`. Query results are
//! returned as Arrow IPC stream bytes that the Java side decodes via
//! `org.apache.arrow.vector.ipc.ArrowStreamReader`.

#![allow(clippy::missing_safety_doc)]

use std::ptr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use dbfy_config::Config;
use dbfy_frontend_datafusion::Engine;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::{jbyteArray, jlong, jstring};
use tokio::runtime::Runtime;

struct JniEngine {
    /// Wrapped in `Arc` so the async entry point can clone a handle
    /// into the tokio task it spawns without holding a borrow on the
    /// caller's `*const JniEngine`. Sync entry points still call
    /// through the `Arc` via deref — no measurable overhead.
    inner: Arc<Engine>,
    runtime: Arc<Runtime>,
}

fn throw(env: &mut JNIEnv, message: impl AsRef<str>) {
    let _ = env.throw_new("java/lang/RuntimeException", message);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_dbfy_Dbfy_nativeNewFromYaml<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    yaml: JString<'local>,
) -> jlong {
    let yaml: String = match env.get_string(&yaml) {
        Ok(s) => s.into(),
        Err(err) => {
            throw(&mut env, err.to_string());
            return 0;
        }
    };
    let config = match Config::from_yaml_str(&yaml) {
        Ok(c) => c,
        Err(err) => {
            throw(&mut env, err.to_string());
            return 0;
        }
    };
    build_engine(&mut env, Engine::from_config(config))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_dbfy_Dbfy_nativeNewFromPath<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    let path: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(err) => {
            throw(&mut env, err.to_string());
            return 0;
        }
    };
    build_engine(&mut env, Engine::from_config_file(&path))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_dbfy_Dbfy_nativeNewEmpty<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    let runtime = match Runtime::new() {
        Ok(r) => Arc::new(r),
        Err(err) => {
            throw(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(JniEngine {
        inner: Arc::new(Engine::default()),
        runtime,
    })) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_dbfy_Dbfy_nativeFree<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        let _ = unsafe { Box::from_raw(handle as *mut JniEngine) };
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_dbfy_Dbfy_nativeExplain<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
) -> jstring {
    let Some(engine) = handle_to_engine(handle, &mut env) else {
        return ptr::null_mut();
    };
    let sql: String = match env.get_string(&sql) {
        Ok(s) => s.into(),
        Err(err) => {
            throw(&mut env, err.to_string());
            return ptr::null_mut();
        }
    };
    match engine.runtime.block_on(engine.inner.explain(&sql)) {
        Ok(s) => env
            .new_string(s)
            .map(|s| s.into_raw())
            .unwrap_or(ptr::null_mut()),
        Err(err) => {
            throw(&mut env, err.to_string());
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_dbfy_Dbfy_nativeQuery<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
) -> jbyteArray {
    let Some(engine) = handle_to_engine(handle, &mut env) else {
        return ptr::null_mut();
    };
    let sql: String = match env.get_string(&sql) {
        Ok(s) => s.into(),
        Err(err) => {
            throw(&mut env, err.to_string());
            return ptr::null_mut();
        }
    };
    let batches = match engine.runtime.block_on(engine.inner.query(&sql)) {
        Ok(b) => b,
        Err(err) => {
            throw(&mut env, err.to_string());
            return ptr::null_mut();
        }
    };
    let bytes = match batches_to_ipc(&batches) {
        Ok(b) => b,
        Err(err) => {
            throw(&mut env, err);
            return ptr::null_mut();
        }
    };
    match env.byte_array_from_slice(&bytes) {
        Ok(arr) => arr.into_raw(),
        Err(err) => {
            throw(&mut env, err.to_string());
            ptr::null_mut()
        }
    }
}

// ----------------------------------------------------------------
// Async query — non-blocking entry point.
//
// The Java side hands us a `CompletableFuture<byte[]>` and returns
// it immediately to its caller. We pin the future as a JNI
// `GlobalRef`, spawn the query on the engine's tokio runtime, and on
// completion (success or failure) attach the producer's tokio thread
// to the JVM and invoke either `complete(byte[])` or
// `completeExceptionally(Throwable)`. The GlobalRef is dropped from
// the same JNI scope so it's collectable as soon as Java releases it.
// ----------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_dbfy_Dbfy_nativeQueryAsync<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
    future: JObject<'local>,
) {
    let Some(engine) = handle_to_engine(handle, &mut env) else {
        return;
    };
    let sql_str: String = match env.get_string(&sql) {
        Ok(s) => s.into(),
        Err(err) => {
            throw(&mut env, err.to_string());
            return;
        }
    };

    // Pin the CompletableFuture across the call → callback round
    // trip. Local refs go away when nativeQueryAsync returns; only
    // a global ref survives.
    let future_ref = match env.new_global_ref(future) {
        Ok(r) => r,
        Err(err) => {
            throw(&mut env, err.to_string());
            return;
        }
    };
    // Get a handle to the JavaVM so the spawn closure can attach the
    // tokio worker thread when it's time to deliver the result.
    let java_vm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(err) => {
            throw(&mut env, err.to_string());
            return;
        }
    };

    let inner = engine.inner.clone();
    engine.runtime.spawn(async move {
        let outcome = inner.query(&sql_str).await;
        // Attach the current tokio thread to the JVM. The guard
        // detaches it on drop. NB: re-attaching a JVM-owned thread
        // is a no-op, so this is safe regardless of where tokio
        // happens to be running this future.
        let mut env = match java_vm.attach_current_thread() {
            Ok(env) => env,
            Err(_) => {
                // We can't surface the error — the future will hang.
                // In practice this only fails if the JVM is being
                // shut down, in which case Java consumers won't be
                // looking at this future anyway.
                return;
            }
        };
        match outcome {
            Ok(batches) => deliver_success(&mut env, &future_ref, &batches),
            Err(err) => deliver_failure(&mut env, &future_ref, &err.to_string()),
        }
        // future_ref drops here, releasing the global ref.
    });
}

/// Encode the batches as Arrow IPC and call `CompletableFuture.complete(byte[])`.
fn deliver_success(
    env: &mut JNIEnv,
    future_ref: &jni::objects::GlobalRef,
    batches: &[RecordBatch],
) {
    let bytes = match batches_to_ipc(batches) {
        Ok(b) => b,
        Err(err) => return deliver_failure(env, future_ref, &err),
    };
    let array = match env.byte_array_from_slice(&bytes) {
        Ok(a) => a,
        Err(err) => return deliver_failure(env, future_ref, &err.to_string()),
    };
    // CompletableFuture.complete returns boolean — we ignore it.
    let _ = env.call_method(
        future_ref.as_obj(),
        "complete",
        "(Ljava/lang/Object;)Z",
        &[JValue::Object(&array)],
    );
}

/// Construct a RuntimeException with the message and call
/// `CompletableFuture.completeExceptionally(Throwable)`.
fn deliver_failure(env: &mut JNIEnv, future_ref: &jni::objects::GlobalRef, message: &str) {
    let msg_obj = match env.new_string(message) {
        Ok(s) => s,
        Err(_) => return,
    };
    let throwable = match env.new_object(
        "java/lang/RuntimeException",
        "(Ljava/lang/String;)V",
        &[JValue::Object(&msg_obj)],
    ) {
        Ok(t) => t,
        Err(_) => return,
    };
    let _ = env.call_method(
        future_ref.as_obj(),
        "completeExceptionally",
        "(Ljava/lang/Throwable;)Z",
        &[JValue::Object(&throwable)],
    );
}

fn build_engine(
    env: &mut JNIEnv,
    engine: Result<Engine, dbfy_frontend_datafusion::EngineError>,
) -> jlong {
    let engine = match engine {
        Ok(e) => e,
        Err(err) => {
            throw(env, err.to_string());
            return 0;
        }
    };
    let runtime = match Runtime::new() {
        Ok(r) => Arc::new(r),
        Err(err) => {
            throw(env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(JniEngine {
        inner: Arc::new(engine),
        runtime,
    })) as jlong
}

fn handle_to_engine<'a>(handle: jlong, env: &mut JNIEnv) -> Option<&'a JniEngine> {
    if handle == 0 {
        throw(env, "engine handle is null or already closed");
        return None;
    }
    Some(unsafe { &*(handle as *const JniEngine) })
}

fn batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(|e| e.to_string())?;
        for batch in batches {
            writer.write(batch).map_err(|e| e.to_string())?;
        }
        writer.finish().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}
