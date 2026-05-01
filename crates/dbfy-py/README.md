# dbfy (Python)

Python bindings for the embedded federated SQL engine `dbfy`. Queries return PyArrow `RecordBatch` objects.

## Install

From a checkout, with maturin in a virtualenv:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install maturin pyarrow
maturin develop --release
```

## Usage

```python
import dbfy

engine = dbfy.Engine.from_yaml("""
version: 1
sources:
  crm:
    type: rest
    base_url: https://api.example.com
    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
""")

print(engine.registered_tables())

batches = engine.query("SELECT id, name FROM crm.customers LIMIT 10")
import pyarrow as pa
table = pa.Table.from_batches(batches)
print(table.to_pandas())

print(engine.explain("SELECT id FROM crm.customers WHERE id = 1"))
```

## API

- `Engine.from_yaml(yaml: str) -> Engine` — parse and validate a YAML config.
- `Engine.from_path(path: str) -> Engine` — load a YAML config from disk.
- `Engine.registered_tables() -> list[str]` — qualified names of registered tables.
- `Engine.query(sql: str) -> list[pyarrow.RecordBatch]` — execute SQL and return Arrow batches.
- `Engine.explain(sql: str) -> str` — render the optimized plan with pushdown details.
- `DbfyError` — base exception for engine errors (config validation, REST execution, etc.).
