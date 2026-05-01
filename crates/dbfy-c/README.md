# dbfy (C)

C bindings for the embedded dbfy federated SQL engine. Header is regenerated
by `cbindgen` on every `cargo build`.

## Build

```bash
cargo build -p dbfy-c                           # produces libdbfy.{a,so}
ls crates/dbfy-c/include/dbfy.h              # generated header
```

## Smoke test

```bash
bash crates/dbfy-c/tests/run_smoke.sh
```

The script compiles `tests/smoke.c` against the static library and runs:
- `dbfy_engine_new_empty` + `dbfy_engine_query("SELECT 1 AS n")` → 1 row
- `dbfy_engine_new_from_yaml(...)` + `dbfy_engine_explain(...)` against a REST table (no HTTP needed)
- error path: invalid YAML returns NULL, `dbfy_last_error()` is populated

## API

```c
#include "dbfy.h"

DbfyEngine *engine = dbfy_engine_new_from_yaml(yaml);
if (!engine) { /* dbfy_last_error() */ }

DbfyResult *result;
if (dbfy_engine_query(engine, "SELECT * FROM crm.customers", &result) == 0) {
    size_t n_batches = dbfy_result_batch_count(result);
    /* For each batch, export to Arrow C Data Interface and consume: */
    ArrowArray array;
    ArrowSchema schema;
    dbfy_result_export_batch(result, 0, &array, &schema);
    /* ... use Arrow C Data Interface to iterate columns/rows ... */
    array.release(&array);
    schema.release(&schema);
}
dbfy_result_free(result);

char *plan = dbfy_engine_explain(engine, "SELECT id FROM crm.customers");
dbfy_string_free(plan);

dbfy_engine_free(engine);
```

## Error handling

All functions that can fail either return NULL (pointers) or -1 (int status).
Call `dbfy_last_error()` from the same thread to retrieve the error message;
the pointer remains valid until the next failing call on the same thread.

## Memory ownership

- `DbfyEngine*` → free with `dbfy_engine_free`
- `DbfyResult*` → free with `dbfy_result_free`
- `char*` (returned from `_explain`) → free with `dbfy_string_free`
- `ArrowArray`/`ArrowSchema` exported via `_export_batch` → call `release` callback
