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
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use dbfy_config::Config;
use dbfy_frontend_datafusion::Engine;
use tokio::runtime::Runtime;

struct JniEngine {
    inner: Engine,
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
        inner: Engine::default(),
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

fn build_engine(env: &mut JNIEnv, engine: Result<Engine, dbfy_frontend_datafusion::EngineError>) -> jlong {
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
        inner: engine,
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
