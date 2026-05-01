// dbfy DuckDB extension shim — bundled-mode-only filter pushdown bridge.
//
// Compiled by `build.rs` when the `duckdb` cargo feature is active and
// linked into the rlib alongside the bundled libduckdb. The shim
// reaches into the C++ side of DuckDB to expose hooks the C API does
// not (yet) cover — primarily filter pushdown for our table functions.
//
// The flow:
//
//   1. The Rust side calls `dbfy_shim_install_optimizer_db(raw_db)`
//      once per database, which casts through `DatabaseWrapper` to
//      register a `duckdb::OptimizerExtension` on `DBConfig`.
//   2. At every query plan, the optimizer extension walks the
//      LogicalPlan, finds `LogicalFilter` -> `LogicalGet[dbfy_*]`
//      pairs, decomposes each top-level conjunct into a
//      `(column, op, value, type)` tuple, and stores a JSON-encoded
//      list of tuples keyed by the LogicalGet's `bind_data.get()`
//      raw pointer in a global side-channel map.
//   3. At init time, the Rust table function calls
//      `dbfy_shim_take_filters(bind_data_ptr)` to retrieve the JSON,
//      parse it, and forward the filters to the dbfy provider.
//
// Filters that don't decompose cleanly (`OR`, expressions both sides
// non-trivial, unsupported types) are simply skipped — they remain
// applied by DuckDB's own filter operator above the LogicalGet, so
// correctness is preserved even when pushdown is partial.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/capi/capi_internal_table.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace dbfy_shim {

// ----------------------------------------------------------------------------
// Observation buffer (kept from step 2 — useful for debugging / tests).
// ----------------------------------------------------------------------------

static thread_local std::string g_observations;

// ----------------------------------------------------------------------------
// Filter side channel.
// ----------------------------------------------------------------------------
//
// Keyed by the `FunctionData*` pointer that DuckDB stashes in
// `LogicalGet::bind_data` and that the table function `init` retrieves
// via `InitInfo::get_bind_data`. Both sides see the same `void*` so the
// map round-trips reliably.

static std::mutex g_filters_mutex;
static std::unordered_map<void*, std::string> g_filters_by_bind;
static std::mutex g_optimizer_install_mutex;

static bool is_dbfy_table_function(const std::string& name) {
    return name == "dbfy_rest" || name == "dbfy_rows_file";
}

// Map `ExpressionType` to the operator string our providers expect.
// `flipped` inverts the inequality operators to handle "constant OP
// column" expressions (we always normalise column-on-the-left).
static const char* op_string_for(duckdb::ExpressionType t, bool flipped) {
    using duckdb::ExpressionType;
    switch (t) {
        case ExpressionType::COMPARE_EQUAL:                 return "=";
        case ExpressionType::COMPARE_NOTEQUAL:              return "!=";
        case ExpressionType::COMPARE_LESSTHAN:              return flipped ? ">"  : "<";
        case ExpressionType::COMPARE_GREATERTHAN:           return flipped ? "<"  : ">";
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:     return flipped ? ">=" : "<=";
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:  return flipped ? "<=" : ">=";
        default:                                            return nullptr;
    }
}

struct DecomposedFilter {
    std::string column;
    std::string op;
    std::string value;
    std::string type;  // DuckDB LogicalType name, e.g. "INTEGER", "VARCHAR"
};

// Try to decompose an expression of the shape `column OP constant`
// (or `constant OP column`) into a `DecomposedFilter`. Returns true
// on success.
static bool try_decompose(const duckdb::Expression& expr,
                          const duckdb::LogicalGet& get,
                          DecomposedFilter& out) {
    using duckdb::ExpressionClass;
    if (expr.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
        return false;
    }
    auto& cmp = expr.Cast<duckdb::BoundComparisonExpression>();

    const duckdb::BoundColumnRefExpression* col = nullptr;
    const duckdb::BoundConstantExpression* val = nullptr;
    bool flipped = false;

    auto try_pair = [&](const duckdb::Expression& a, const duckdb::Expression& b, bool flip) -> bool {
        if (a.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF
                && b.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
            col = &a.Cast<duckdb::BoundColumnRefExpression>();
            val = &b.Cast<duckdb::BoundConstantExpression>();
            flipped = flip;
            return true;
        }
        return false;
    };
    if (!try_pair(*cmp.left, *cmp.right, false)
            && !try_pair(*cmp.right, *cmp.left, true)) {
        return false;
    }

    // Resolve column name through LogicalGet's column_ids[i] -> names[i].
    const auto& column_ids = get.GetColumnIds();
    auto col_idx = col->binding.column_index;
    if (col_idx >= column_ids.size()) {
        return false;
    }
    auto primary = column_ids[col_idx].GetPrimaryIndex();
    if (primary >= get.names.size()) {
        return false;
    }
    out.column = get.names[primary];

    auto op = op_string_for(cmp.GetExpressionType(), flipped);
    if (!op) {
        return false;
    }
    out.op = op;

    if (val->value.IsNull()) {
        return false;  // pushing IS NULL not yet wired
    }
    out.value = val->value.ToString();
    out.type = val->value.type().ToString();
    return true;
}

// ----------------------------------------------------------------------------
// Tiny manual JSON encoding. Trusted input — the values come from
// DuckDB's own ToString() and column names from the bind. Still escape
// properly for the rare case where a value contains `"` or `\`.
// ----------------------------------------------------------------------------

static void json_escape(const std::string& s, std::string& out) {
    out.push_back('"');
    for (char c : s) {
        switch (c) {
            case '"':  out.append("\\\""); break;
            case '\\': out.append("\\\\"); break;
            case '\n': out.append("\\n");  break;
            case '\r': out.append("\\r");  break;
            case '\t': out.append("\\t");  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out.append(buf);
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

static std::string serialise_filters(const std::vector<DecomposedFilter>& filters) {
    std::string out;
    out.push_back('[');
    bool first = true;
    for (const auto& f : filters) {
        if (!first) out.push_back(',');
        first = false;
        out.append("{\"column\":");
        json_escape(f.column, out);
        out.append(",\"op\":");
        json_escape(f.op, out);
        out.append(",\"value\":");
        json_escape(f.value, out);
        out.append(",\"type\":");
        json_escape(f.type, out);
        out.push_back('}');
    }
    out.push_back(']');
    return out;
}

// ----------------------------------------------------------------------------
// Plan walker.
// ----------------------------------------------------------------------------

static void record_observation(const std::string& fn_name, const duckdb::Expression& expr) {
    if (!g_observations.empty()) {
        g_observations.push_back('\n');
    }
    g_observations.append(fn_name);
    g_observations.append(" | ");
    g_observations.append(expr.ToString());
}

static void process_filter_above_get(duckdb::LogicalFilter& filter, duckdb::LogicalGet& get) {
    const std::string& fn_name = get.function.name;
    if (!is_dbfy_table_function(fn_name)) {
        return;
    }
    std::vector<DecomposedFilter> decomposed;
    decomposed.reserve(filter.expressions.size());
    for (auto& expr : filter.expressions) {
        record_observation(fn_name, *expr);
        DecomposedFilter df;
        if (try_decompose(*expr, get, df)) {
            decomposed.push_back(std::move(df));
        }
    }
    if (decomposed.empty()) {
        return;
    }
    // The C API wraps the user's bind_data inside a `CTableBindData`
    // wrapper (`CTableBindData::bind_data` is the user pointer DuckDB
    // hands back via `duckdb_init_get_bind_data`). The OptimizerExtension
    // sees the wrapper, not the inner pointer; we have to unwrap before
    // keying the side channel so it round-trips with what `init` reads.
    auto* cdata = dynamic_cast<duckdb::CTableBindData*>(get.bind_data.get());
    if (!cdata) {
        return;
    }
    void* key = cdata->bind_data;
    if (!key) {
        return;
    }
    std::string json = serialise_filters(decomposed);
    std::lock_guard<std::mutex> guard(g_filters_mutex);
    g_filters_by_bind[key] = std::move(json);
}

static void walk_for_dbfy_filters(duckdb::LogicalOperator& op) {
    if (op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER && op.children.size() == 1) {
        auto& child = *op.children[0];
        if (child.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
            auto& filter = op.Cast<duckdb::LogicalFilter>();
            auto& get = child.Cast<duckdb::LogicalGet>();
            process_filter_above_get(filter, get);
        }
    }
    for (auto& child : op.children) {
        walk_for_dbfy_filters(*child);
    }
}

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

const char* dbfy_shim_observations_peek(void) {
    return dbfy_shim::g_observations.c_str();
}

void dbfy_shim_observations_clear(void) {
    dbfy_shim::g_observations.clear();
}

/// Take ownership of the JSON-encoded filter list for the given
/// `bind_data` pointer, removing it from the side-channel map. The
/// returned pointer was allocated with `malloc`; the caller must free
/// it via `dbfy_shim_free_string`. Returns `nullptr` if no filters
/// were stashed for this bind data (i.e. no pushdown found).
char* dbfy_shim_take_filters(void* bind_data) {
    if (!bind_data) {
        return nullptr;
    }
    std::lock_guard<std::mutex> guard(dbfy_shim::g_filters_mutex);
    auto it = dbfy_shim::g_filters_by_bind.find(bind_data);
    if (it == dbfy_shim::g_filters_by_bind.end()) {
        return nullptr;
    }
    auto json = std::move(it->second);
    dbfy_shim::g_filters_by_bind.erase(it);
    auto* buf = static_cast<char*>(std::malloc(json.size() + 1));
    if (!buf) {
        return nullptr;
    }
    std::memcpy(buf, json.data(), json.size());
    buf[json.size()] = '\0';
    return buf;
}

void dbfy_shim_free_string(char* p) {
    std::free(p);
}

}  // extern "C"
