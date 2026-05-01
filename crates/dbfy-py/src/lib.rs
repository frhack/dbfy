use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_pyarrow::{FromPyArrow, IntoPyArrow};
use async_trait::async_trait;
use dbfy_config::Config;
use dbfy_frontend_datafusion::Engine as CoreEngine;
use dbfy_provider::{
    DynProvider, FilterCapabilities, ProgrammaticTableProvider, ProviderCapabilities,
    ProviderError, ProviderResult, ScalarValue, ScanRequest, ScanResponse,
};
use pyo3::create_exception;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::runtime::Runtime;

create_exception!(_dbfy, DbfyError, PyRuntimeError);

#[pyclass(name = "Engine", module = "dbfy")]
struct PyEngine {
    inner: Arc<CoreEngine>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyEngine {
    #[staticmethod]
    fn from_yaml(yaml: &str) -> PyResult<Self> {
        let config = Config::from_yaml_str(yaml).map_err(to_py)?;
        let engine = CoreEngine::from_config(config).map_err(to_py)?;
        let runtime = Arc::new(Runtime::new().map_err(to_py)?);
        Ok(Self {
            inner: Arc::new(engine),
            runtime,
        })
    }

    #[staticmethod]
    fn from_path(path: &str) -> PyResult<Self> {
        let engine = CoreEngine::from_config_file(path).map_err(to_py)?;
        let runtime = Arc::new(Runtime::new().map_err(to_py)?);
        Ok(Self {
            inner: Arc::new(engine),
            runtime,
        })
    }

    #[staticmethod]
    fn empty() -> PyResult<Self> {
        let runtime = Arc::new(Runtime::new().map_err(to_py)?);
        Ok(Self {
            inner: Arc::new(CoreEngine::default()),
            runtime,
        })
    }

    fn register_provider(
        &mut self,
        py: Python<'_>,
        table_name: String,
        provider: Py<PyAny>,
    ) -> PyResult<()> {
        let bound = provider.bind(py);
        let py_schema = bound.call_method0("schema")?;
        let schema = arrow_schema::Schema::from_pyarrow_bound(&py_schema)?;
        let schema = Arc::new(schema);

        let capabilities = if bound.hasattr("capabilities")? {
            let caps_obj = bound.call_method0("capabilities")?;
            parse_capabilities(&caps_obj)?
        } else {
            ProviderCapabilities::default()
        };

        let py_provider = PyProgrammaticProvider {
            py_obj: provider,
            schema,
            capabilities,
        };

        let engine = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            DbfyError::new_err(
                "cannot register provider while the engine is shared with an in-flight query",
            )
        })?;
        engine
            .register_provider(table_name, Arc::new(py_provider) as DynProvider)
            .map_err(to_py)?;
        Ok(())
    }

    fn registered_tables(&self) -> Vec<String> {
        self.inner.registered_tables()
    }

    fn query<'py>(&self, py: Python<'py>, sql: &str) -> PyResult<Bound<'py, PyList>> {
        let sql = sql.to_owned();
        let runtime = self.runtime.clone();
        let inner = self.inner.clone();
        let batches = py
            .detach(move || runtime.block_on(inner.query(&sql)))
            .map_err(to_py)?;

        batches_to_py_list(py, batches)
    }

    fn explain(&self, py: Python<'_>, sql: &str) -> PyResult<String> {
        let sql = sql.to_owned();
        let runtime = self.runtime.clone();
        let inner = self.inner.clone();
        py.detach(move || runtime.block_on(inner.explain(&sql)))
            .map_err(to_py)
    }

    fn query_async<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let batches = inner.query(&sql).await.map_err(to_py)?;
            Python::attach(|py| batches_to_py_list(py, batches).map(|list| list.unbind()))
        })
    }

    fn explain_async<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { inner.explain(&sql).await.map_err(to_py) })
    }

    fn __repr__(&self) -> String {
        format!("Engine(tables={})", self.inner.registered_tables().len())
    }
}

struct PyProgrammaticProvider {
    py_obj: Py<PyAny>,
    schema: arrow_schema::SchemaRef,
    capabilities: ProviderCapabilities,
}

#[async_trait]
impl ProgrammaticTableProvider for PyProgrammaticProvider {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    async fn scan(&self, request: ScanRequest) -> ProviderResult<ScanResponse> {
        let py_iter = Python::attach(|py| -> PyResult<Py<PyIterator>> {
            let provider = self.py_obj.bind(py);
            let request_dict = build_request_dict(py, &request)?;
            let scan_result = provider.call_method1("scan", (request_dict,))?;
            let iter = scan_result.try_iter()?;
            Ok(iter.unbind())
        })
        .map_err(py_to_provider)?;

        let stream = futures::stream::try_unfold(py_iter, |py_iter| async move {
            let next: ProviderResult<Option<RecordBatch>> = Python::attach(|py| {
                let mut bound = py_iter.bind(py).clone();
                match bound.next() {
                    Some(Ok(item)) => RecordBatch::from_pyarrow_bound(&item)
                        .map(Some)
                        .map_err(py_to_provider),
                    Some(Err(err)) if err.is_instance_of::<PyStopIteration>(py) => Ok(None),
                    Some(Err(err)) => Err(py_to_provider(err)),
                    None => Ok(None),
                }
            });

            match next {
                Ok(Some(batch)) => Ok(Some((batch, py_iter))),
                Ok(None) => Ok(None),
                Err(err) => Err(err),
            }
        });

        Ok(ScanResponse {
            stream: Box::pin(stream),
            handled_filters: Vec::new(),
            metadata: std::collections::BTreeMap::new(),
        })
    }
}

fn build_request_dict<'py>(py: Python<'py>, request: &ScanRequest) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("query_id", &request.query_id)?;
    dict.set_item("projection", request.projection.clone())?;
    dict.set_item("limit", request.limit)?;

    let filters_list = PyList::empty(py);
    for filter in &request.filters {
        let filter_dict = PyDict::new(py);
        filter_dict.set_item("column", &filter.column)?;
        filter_dict.set_item("operator", filter.operator.as_str())?;
        match &filter.value {
            ScalarValue::Boolean(v) => filter_dict.set_item("value", *v)?,
            ScalarValue::Int64(v) => filter_dict.set_item("value", *v)?,
            ScalarValue::Float64(v) => filter_dict.set_item("value", *v)?,
            ScalarValue::Utf8(v) => filter_dict.set_item("value", v)?,
            ScalarValue::Utf8List(v) => filter_dict.set_item("value", v.clone())?,
        }
        filters_list.append(filter_dict)?;
    }
    dict.set_item("filters", filters_list)?;

    Ok(dict)
}

fn parse_capabilities(caps: &Bound<PyAny>) -> PyResult<ProviderCapabilities> {
    use pyo3::exceptions::PyValueError;

    if caps.is_none() {
        return Ok(ProviderCapabilities::default());
    }
    let dict = caps
        .cast::<PyDict>()
        .map_err(|_| PyValueError::new_err("provider.capabilities() must return a dict or None"))?;

    let mut out = ProviderCapabilities::default();

    if let Some(v) = dict.get_item("projection_pushdown")? {
        out.projection_pushdown = v.extract::<bool>()?;
    }
    if let Some(v) = dict.get_item("limit_pushdown")? {
        out.limit_pushdown = v.extract::<bool>()?;
    }

    if let Some(filters_obj) = dict.get_item("filters")? {
        if !filters_obj.is_none() {
            let filters_dict = filters_obj.cast::<PyDict>().map_err(|_| {
                PyValueError::new_err("capabilities['filters'] must be a dict if present")
            })?;
            for (op, columns) in filters_dict.iter() {
                let op_str: String = op.extract()?;
                let columns: Vec<String> = columns.extract()?;
                let target = filter_capability_bucket(&op_str, &mut out.filter_pushdown)
                    .ok_or_else(|| {
                        PyValueError::new_err(format!(
                            "unsupported filter operator `{op_str}`; expected one of \
                             '=', '<', '<=', '>', '>=', 'IN'"
                        ))
                    })?;
                target.extend(columns);
            }
        }
    }

    Ok(out)
}

fn filter_capability_bucket<'a>(
    op: &str,
    caps: &'a mut FilterCapabilities,
) -> Option<&'a mut std::collections::BTreeSet<String>> {
    match op {
        "=" => Some(&mut caps.equals),
        "IN" | "in" => Some(&mut caps.in_list),
        ">" => Some(&mut caps.greater_than),
        ">=" => Some(&mut caps.greater_than_or_equal),
        "<" => Some(&mut caps.less_than),
        "<=" => Some(&mut caps.less_than_or_equal),
        _ => None,
    }
}

fn batches_to_py_list<'py>(
    py: Python<'py>,
    batches: Vec<RecordBatch>,
) -> PyResult<Bound<'py, PyList>> {
    let py_list = PyList::empty(py);
    for batch in batches {
        let py_batch: Bound<'py, PyAny> = batch.into_pyarrow(py)?;
        py_list.append(py_batch)?;
    }
    Ok(py_list)
}

fn to_py<E: std::fmt::Display>(error: E) -> PyErr {
    DbfyError::new_err(error.to_string())
}

fn py_to_provider(error: PyErr) -> ProviderError {
    ProviderError::Generic {
        message: error.to_string(),
    }
}

#[pymodule]
fn _dbfy(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("DbfyError", py.get_type::<DbfyError>())?;
    m.add_class::<PyEngine>()?;
    Ok(())
}
