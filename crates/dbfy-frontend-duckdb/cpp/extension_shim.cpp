// dbfy DuckDB extension shim — bundled-mode-only filter pushdown bridge.
//
// Compiled by `build.rs` when the `duckdb` cargo feature is active and
// linked into the rlib alongside the bundled libduckdb. The shim
// reaches into the C++ side of DuckDB to expose hooks the C API does
// not (yet) cover — primarily filter pushdown for our table functions.
//
// Step 2 (this file): the optimizer extension is registered and walks
// the LogicalPlan, but only OBSERVES — it appends a line per matching
// `LogicalFilter` -> `LogicalGet[dbfy_rest|dbfy_rows_file]` pair to a
// thread-local buffer. Step 3 will turn observation into rewriting:
// the filters get serialised into a side channel keyed on something
// the table function's `init` can read.

#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace dbfy_shim {

// ----------------------------------------------------------------------------
// Observation buffer.
// ----------------------------------------------------------------------------
//
// A thread-local newline-joined log of "function_name | filter_expr"
// rows, one per matched dbfy LogicalGet that had a parent LogicalFilter.
// Tests drain it to assert the optimizer hook fired with the right
// expressions; production callers just leave it empty.

static thread_local std::string g_observations;
static std::mutex g_optimizer_install_mutex;

static bool is_dbfy_table_function(const std::string& name) {
    return name == "dbfy_rest" || name == "dbfy_rows_file";
}

static void record_observation(const std::string& fn_name, const duckdb::Expression& expr) {
    if (!g_observations.empty()) {
        g_observations.push_back('\n');
    }
    g_observations.append(fn_name);
    g_observations.append(" | ");
    g_observations.append(expr.ToString());
}

// Walk the plan, looking for LogicalFilter whose only child is a
// LogicalGet on a dbfy table function. Record the filter expressions.
static void walk_for_dbfy_filters(duckdb::LogicalOperator& op) {
    if (op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER && op.children.size() == 1) {
        auto& child = *op.children[0];
        if (child.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
            auto& get = child.Cast<duckdb::LogicalGet>();
            const std::string& fn_name = get.function.name;
            if (is_dbfy_table_function(fn_name)) {
                for (auto& expr : op.expressions) {
                    record_observation(fn_name, *expr);
                }
            }
        }
    }
    for (auto& child : op.children) {
        walk_for_dbfy_filters(*child);
    }
}

// ----------------------------------------------------------------------------
// OptimizerExtension entry point.
// ----------------------------------------------------------------------------

static void dbfy_optimize(duckdb::OptimizerExtensionInput& input,
                          duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    if (plan) {
        walk_for_dbfy_filters(*plan);
    }
}

}  // namespace dbfy_shim

// ----------------------------------------------------------------------------
// extern "C" surface for Rust.
// ----------------------------------------------------------------------------

extern "C" {

uint32_t dbfy_shim_probe(void) {
    return 0xDBFEC5;
}

const char* dbfy_shim_duckdb_version(void) {
    return duckdb::DuckDB::LibraryVersion();
}

/// Register dbfy's OptimizerExtension on the database underlying the
/// given C-API `duckdb_database` handle. Idempotent only by accident
/// (DuckDB silently accepts duplicate registrations); call once per
/// database lifetime.
///
/// Returns 0 on success, non-zero on failure (db handle null or
/// invalid layout assumption broken — should fail loud rather than
/// silently corrupt).
int32_t dbfy_shim_install_optimizer_db(duckdb_database db_handle) {
    if (!db_handle) {
        return 1;
    }
    std::lock_guard<std::mutex> guard(dbfy_shim::g_optimizer_install_mutex);
    auto wrapper = reinterpret_cast<duckdb::DatabaseWrapper*>(db_handle);
    if (!wrapper || !wrapper->database) {
        return 2;
    }
    auto& instance = *wrapper->database->instance;

    duckdb::OptimizerExtension ext;
    ext.optimize_function = &dbfy_shim::dbfy_optimize;
    duckdb::OptimizerExtension::Register(instance.config, std::move(ext));
    return 0;
}

/// Returns a pointer to the thread-local observation buffer (\n-joined
/// "function_name | filter_expr" rows). Valid until the next call to
/// `dbfy_shim_observations_clear` or any DuckDB query on this thread.
/// Empty string means no observations were recorded.
const char* dbfy_shim_observations_peek(void) {
    return dbfy_shim::g_observations.c_str();
}

/// Clear the thread-local observation buffer. Call after asserting on
/// it in a test.
void dbfy_shim_observations_clear(void) {
    dbfy_shim::g_observations.clear();
}

}  // extern "C"
