use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use dbfy_config::{Config, SourceConfig};
use dbfy_frontend_datafusion::{Engine, RowsFileHandle, build_rows_file_handle};

mod detect;
mod duckdb_attach;
mod init;
mod probe;

#[derive(Debug, Parser)]
#[command(name = "dbfy")]
#[command(about = "Embedded federated SQL engine for REST and programmatic datasources")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate {
        config: PathBuf,
    },
    Inspect {
        config: PathBuf,
    },
    Explain {
        #[arg(short, long)]
        config: PathBuf,
        sql: String,
    },
    Query {
        #[arg(short, long)]
        config: PathBuf,
        sql: String,
    },
    /// Infer schema from a line-delimited file and print a dbfy config stanza.
    Detect {
        /// Override format detection. Otherwise inferred from extension.
        #[arg(long, value_name = "FMT")]
        format: Option<String>,
        /// How many lines to sample for type inference (default 200).
        #[arg(long, default_value_t = 200)]
        sample: usize,
        /// Name to use for the source in the emitted YAML.
        #[arg(long, default_value = "files")]
        source: String,
        /// Name to use for the table in the emitted YAML.
        #[arg(long, default_value = "events")]
        table: String,
        /// Path to the file to inspect.
        file: PathBuf,
    },
    /// Build or refresh the rows-file sidecar index for a configured table.
    ///
    /// Without `--rebuild`, this triggers the same EXTEND / REUSE / REBUILD
    /// invalidation logic the runtime uses on first scan. With `--rebuild`,
    /// the prior sidecar is discarded and the index is regenerated.
    Index {
        #[arg(short, long)]
        config: PathBuf,
        /// Qualified table name (e.g. `app.events`). Must reference a
        /// `type: rows_file` table in the config.
        #[arg(short, long)]
        table: String,
        /// Force a full rebuild instead of EXTEND/REUSE.
        #[arg(long)]
        rebuild: bool,
    },
    /// Hit a REST endpoint, infer schema from the response, and emit a
    /// dbfy config stanza ready to paste.
    Probe {
        /// URL to GET (full URL including scheme, host, path, query).
        url: String,
        #[arg(long, default_value = "api")]
        source: String,
        #[arg(long, default_value = "items")]
        table: String,
        /// Force a specific root JSONPath instead of the auto-detected one.
        #[arg(long)]
        root: Option<String>,
        /// Environment variable holding a bearer token to send.
        #[arg(long, value_name = "ENV")]
        auth_bearer_env: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_seconds: u64,
    },
    /// Interactive wizard: asks a few questions, then runs detect or probe
    /// under the hood and emits a complete YAML config.
    Init {
        /// Skip the source-kind prompt: `rest` or `rows_file`.
        #[arg(long, value_name = "KIND")]
        kind: Option<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        table: Option<String>,
    },
    /// Emit a DuckDB SQL script that materialises every table in the
    /// config as a `CREATE OR REPLACE VIEW <schema>.<table>` over the
    /// matching `dbfy_rest()` / `dbfy_rows_file()` call. Pipe the output
    /// into `duckdb`. Today's substitute for `ATTACH '...' (TYPE dbfy)`.
    DuckdbAttach {
        #[arg(short, long)]
        config: PathBuf,
        /// Target DuckDB schema. Created with `IF NOT EXISTS` if missing.
        #[arg(long, default_value = "dbfy")]
        schema: String,
        /// Optional: prepend `LOAD '<path>'` to the script so it runs
        /// standalone (`duckdb mydb < attach.sql`) without needing the
        /// extension already loaded in the host session.
        #[arg(long, value_name = "PATH")]
        extension: Option<PathBuf>,
        /// Use `CREATE VIEW` instead of `CREATE OR REPLACE VIEW`. Fail
        /// loudly if a view already exists with a different definition.
        #[arg(long)]
        strict: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate { config } => validate(config),
        Command::Inspect { config } => inspect(config),
        Command::Explain { config, sql } => explain(config, sql).await,
        Command::Query { config, sql } => query(config, sql).await,
        Command::Detect {
            format,
            sample,
            source,
            table,
            file,
        } => detect_cmd(file, format, sample, source, table),
        Command::Index {
            config,
            table,
            rebuild,
        } => index_cmd(config, table, rebuild),
        Command::Probe {
            url,
            source,
            table,
            root,
            auth_bearer_env,
            timeout_seconds,
        } => probe_cmd(url, source, table, root, auth_bearer_env, timeout_seconds).await,
        Command::Init {
            kind,
            source,
            table,
        } => init_cmd(kind, source, table).await,
        Command::DuckdbAttach {
            config,
            schema,
            extension,
            strict,
        } => duckdb_attach_cmd(config, schema, extension, strict),
    }
}

fn duckdb_attach_cmd(
    config_path: PathBuf,
    schema: String,
    extension: Option<PathBuf>,
    strict: bool,
) -> Result<()> {
    let config = Config::from_path(&config_path)?;
    let sql = duckdb_attach::emit_attach_sql(
        &config,
        &duckdb_attach::AttachOpts {
            schema,
            extension,
            strict,
        },
    )?;
    print!("{sql}");
    Ok(())
}

async fn probe_cmd(
    url: String,
    source: String,
    table: String,
    root: Option<String>,
    auth_bearer_env: Option<String>,
    timeout_seconds: u64,
) -> Result<()> {
    let yaml = probe::probe(
        url,
        probe::ProbeOpts {
            source_name: source,
            table_name: table,
            endpoint_path: None,
            root,
            auth_bearer_env,
            timeout_seconds,
        },
    )
    .await?;
    print!("{yaml}");
    Ok(())
}

async fn init_cmd(
    kind: Option<String>,
    source: Option<String>,
    table: Option<String>,
) -> Result<()> {
    let kind = match kind.as_deref() {
        Some("rest") => Some(init::SourceKind::Rest),
        Some("rows_file") | Some("rows-file") => Some(init::SourceKind::RowsFile),
        Some(other) => anyhow::bail!("--kind must be `rest` or `rows_file`, got `{other}`"),
        None => None,
    };
    let yaml = init::init(init::InitOpts {
        kind,
        source_name: source,
        table_name: table,
    })
    .await?;
    print!("{yaml}");
    Ok(())
}

fn detect_cmd(
    file: PathBuf,
    format: Option<String>,
    sample: usize,
    source: String,
    table: String,
) -> Result<()> {
    let format = format.map(|s| detect::Format::from_str(&s)).transpose()?;
    let yaml = detect::detect(
        file,
        detect::DetectOpts {
            format,
            sample,
            source_name: source,
            table_name: table,
        },
    )?;
    print!("{yaml}");
    Ok(())
}

fn index_cmd(config_path: PathBuf, qualified: String, rebuild: bool) -> Result<()> {
    let config = Config::from_path(&config_path)?;

    // Resolve `qualified = "source.table"` to the matching rows_file table.
    let (source_name, table_name) = qualified
        .split_once('.')
        .map(|(s, t)| (s.to_string(), t.to_string()))
        .ok_or_else(|| {
            anyhow::anyhow!("table must be qualified as `source.table`, got `{qualified}`")
        })?;

    let source = config
        .sources
        .get(&source_name)
        .ok_or_else(|| anyhow::anyhow!("source `{source_name}` not found in config"))?;
    let rf = match source {
        SourceConfig::RowsFile(rf) => rf,
        other => {
            anyhow::bail!(
                "source `{source_name}` is `{:?}` — `dbfy index` only operates on rows_file sources",
                std::mem::discriminant(other)
            )
        }
    };
    let table_cfg = rf.tables.get(&table_name).ok_or_else(|| {
        anyhow::anyhow!("table `{table_name}` not found in source `{source_name}`")
    })?;

    let handle = build_rows_file_handle(&source_name, &table_name, table_cfg)?;
    match handle {
        RowsFileHandle::Single(table) => {
            let decision = if rebuild {
                table.rebuild()?;
                dbfy_provider_rows_file::RefreshDecision::Rebuilt
            } else {
                table.refresh()?
            };
            let summary = table.index_summary()?;
            println!(
                "{:>10}  {}  ({} chunks, {} rows, {} bytes)",
                format!("{decision:?}"),
                table.file_path().display(),
                summary.chunks,
                summary.total_rows,
                summary.file_size,
            );
        }
        RowsFileHandle::Glob(glob) => {
            let pairs = if rebuild {
                glob.rebuild_all()?
            } else {
                glob.refresh_each()?
            };
            for (path, decision) in &pairs {
                println!("{:>10}  {}", format!("{decision:?}"), path.display());
            }
            let summary = glob.index_summary()?;
            println!(
                "{:>10}  {} files, {} chunks, {} rows total, {} bytes",
                "TOTAL",
                pairs.len(),
                summary.chunks,
                summary.total_rows,
                summary.file_size,
            );
        }
    }

    Ok(())
}

fn validate(config: PathBuf) -> Result<()> {
    let config = Config::from_path(config)?;
    let table_count = config
        .sources
        .values()
        .map(|source| match source {
            dbfy_config::SourceConfig::Rest(rest) => rest.tables.len(),
            dbfy_config::SourceConfig::RowsFile(rf) => rf.tables.len(),
            dbfy_config::SourceConfig::Parquet(p) => p.tables.len(),
            dbfy_config::SourceConfig::Excel(e) => e.tables.len(),
            dbfy_config::SourceConfig::Graphql(g) => g.tables.len(),
            dbfy_config::SourceConfig::Postgres(pg) => pg.tables.len(),
        })
        .sum::<usize>();

    println!(
        "config valida: {} sorgenti, {} tabelle",
        config.sources.len(),
        table_count
    );

    Ok(())
}

fn inspect(config: PathBuf) -> Result<()> {
    let engine = Engine::from_config_file(config)?;

    for table in engine.registered_tables() {
        println!("{table}");
    }

    Ok(())
}

async fn query(config: PathBuf, sql: String) -> Result<()> {
    let engine = Engine::from_config_file(config)?;
    let batches = engine.query(&sql).await?;
    datafusion::arrow::util::pretty::print_batches(&batches)?;
    Ok(())
}

async fn explain(config: PathBuf, sql: String) -> Result<()> {
    let engine = Engine::from_config_file(config)?;
    let explanation = engine.explain(&sql).await?;
    println!("{explanation}");
    Ok(())
}
