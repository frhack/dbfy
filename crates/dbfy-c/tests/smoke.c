/* Smoke test for the dbfy C bindings.
 *
 * Verifies engine lifecycle, SQL query against a programmatic SELECT-literal
 * (no HTTP needed), and explain output against a YAML-defined REST table. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dbfy.h"

static int fail(const char *step) {
    const char *err = dbfy_last_error();
    fprintf(stderr, "FAIL %s: %s\n", step, err ? err : "(no error message)");
    return 1;
}

static const char *YAML_CONFIG =
    "version: 1\n"
    "sources:\n"
    "  crm:\n"
    "    type: rest\n"
    "    base_url: http://localhost:9999\n"
    "    tables:\n"
    "      customers:\n"
    "        endpoint:\n"
    "          method: GET\n"
    "          path: /customers\n"
    "        root: \"$.data[*]\"\n"
    "        columns:\n"
    "          id: { path: \"$.id\", type: int64 }\n"
    "          name: { path: \"$.name\", type: string }\n";

int main(void) {
    /* 1. Empty engine + literal query (no HTTP). */
    struct DbfyEngine *engine = dbfy_engine_new_empty();
    if (!engine) return fail("new_empty");

    struct DbfyResult *result = NULL;
    if (dbfy_engine_query(engine, "SELECT 1 AS n", &result) != 0) {
        dbfy_engine_free(engine);
        return fail("query SELECT 1");
    }

    size_t batch_count = dbfy_result_batch_count(result);
    size_t row_count = dbfy_result_row_count(result);
    if (batch_count != 1 || row_count != 1) {
        fprintf(stderr, "FAIL: expected 1 batch / 1 row, got %zu / %zu\n",
                batch_count, row_count);
        dbfy_result_free(result);
        dbfy_engine_free(engine);
        return 1;
    }

    dbfy_result_free(result);
    dbfy_engine_free(engine);

    /* 2. YAML engine + explain (REST table, no HTTP call needed). */
    engine = dbfy_engine_new_from_yaml(YAML_CONFIG);
    if (!engine) return fail("new_from_yaml");

    char *explanation = dbfy_engine_explain(engine, "SELECT id, name FROM crm.customers");
    if (!explanation) {
        dbfy_engine_free(engine);
        return fail("explain");
    }

    if (!strstr(explanation, "provider: rest")) {
        fprintf(stderr, "FAIL: explanation missing 'provider: rest':\n%s\n", explanation);
        dbfy_string_free(explanation);
        dbfy_engine_free(engine);
        return 1;
    }

    dbfy_string_free(explanation);
    dbfy_engine_free(engine);

    /* 3. Error path: invalid YAML should produce a non-NULL last_error. */
    engine = dbfy_engine_new_from_yaml("not: [valid: yaml");
    if (engine) {
        fprintf(stderr, "FAIL: invalid yaml should have failed\n");
        dbfy_engine_free(engine);
        return 1;
    }
    const char *err = dbfy_last_error();
    if (!err || strlen(err) == 0) {
        fprintf(stderr, "FAIL: expected an error message after invalid YAML\n");
        return 1;
    }

    printf("OK c smoke: SELECT 1 -> 1 row, explain rest, error path covered\n");
    return 0;
}
