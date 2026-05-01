//! Build typed rows-file handles from `dbfy_config` YAML schemas.
//!
//! Both frontends (DataFusion + DuckDB) materialise rows-file tables from
//! the same YAML config, so the translator lives here next to the
//! providers rather than being duplicated per frontend.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbfy_config::{
    CellTypeConfig, CsvSourceConfig, IndexKindConfig, IndexedColumnConfig, ParserConfig,
    RowsFileTableConfig, SyslogFieldConfig,
};
use dbfy_provider::{ProviderError, ProviderResult};

use crate::parser::{CellType, DynParser};
use crate::parsers::csv::read_header;
use crate::parsers::jsonl::JsonlType;
use crate::parsers::{
    CsvColumn, CsvParser, CsvSource, JsonlColumn, JsonlParser, LogfmtColumn, LogfmtParser,
    RegexColumn, RegexParser, SyslogColumn, SyslogField, SyslogParser,
};
use crate::{IndexKind, IndexedColumn, RowsFileGlob, RowsFileTable};

/// Either a single-file table or a glob spanning many files. Both implement
/// `ProgrammaticTableProvider` and can be erased to `DynProvider` via
/// [`Self::into_dyn`]; frontends keep the typed handle when they need to
/// call `refresh` / `index_summary` / `rebuild`.
pub enum RowsFileHandle {
    Single(Arc<RowsFileTable>),
    Glob(Arc<RowsFileGlob>),
}

impl RowsFileHandle {
    pub fn into_dyn(self) -> Arc<dyn dbfy_provider::ProgrammaticTableProvider> {
        match self {
            Self::Single(t) => t,
            Self::Glob(g) => g,
        }
    }
}

/// Materialise a typed handle from a `RowsFileTableConfig`. The config has
/// already been validated by `dbfy_config::Config::validate` (one of `path`
/// or `glob` is set, parser column lists are non-empty, JSONPaths in jsonl
/// are syntactically valid, etc) so failures here are mostly I/O — the
/// glob expansion or the CSV header read.
pub fn build_handle(cfg: &RowsFileTableConfig) -> ProviderResult<RowsFileHandle> {
    let indexed: Vec<IndexedColumn> = cfg
        .indexed_columns
        .iter()
        .map(map_indexed_column)
        .collect();

    // For CSV with named columns we need a real file to read the header
    // from. Resolve a representative path: the explicit `path`, else the
    // first match of the glob.
    let header_source: Option<PathBuf> = if let Some(p) = &cfg.path {
        Some(PathBuf::from(p))
    } else if let Some(g) = &cfg.glob {
        glob::glob(g)
            .ok()
            .and_then(|mut paths| paths.find_map(|p| p.ok()))
    } else {
        None
    };

    let parser = build_parser(&cfg.parser, header_source.as_deref())?;

    if let Some(path) = &cfg.path {
        let mut table = RowsFileTable::new(PathBuf::from(path), parser, indexed);
        if let Some(n) = cfg.chunk_rows {
            table = table.with_chunk_rows(n);
        }
        Ok(RowsFileHandle::Single(Arc::new(table)))
    } else if let Some(pattern) = &cfg.glob {
        let glob = RowsFileGlob::try_new(pattern, parser, indexed)
            .map_err(|err| ProviderError::Generic { message: err.to_string() })?;
        Ok(RowsFileHandle::Glob(Arc::new(glob)))
    } else {
        Err(ProviderError::Generic {
            message: "rows-file table has neither path nor glob (config validation should have caught this)"
                .to_string(),
        })
    }
}

fn build_parser(cfg: &ParserConfig, header_source: Option<&Path>) -> ProviderResult<DynParser> {
    let map = |msg: String| ProviderError::Generic { message: msg };

    match cfg {
        ParserConfig::Jsonl { columns } => {
            let cols: Vec<JsonlColumn> = columns
                .iter()
                .map(|c| JsonlColumn {
                    name: c.name.clone(),
                    path: c.path.clone(),
                    data_type: map_jsonl_type(c.r#type),
                })
                .collect();
            let p = JsonlParser::try_new(cols).map_err(|e| map(e.to_string()))?;
            Ok(Arc::new(p))
        }
        ParserConfig::Csv {
            columns,
            delimiter,
            has_header,
        } => {
            let delim = delimiter.unwrap_or(',') as u8;
            let has_header = has_header.unwrap_or(true);
            let header = if has_header
                && columns
                    .iter()
                    .any(|c| matches!(c.source, CsvSourceConfig::Name(_)))
            {
                let path = header_source.ok_or_else(|| {
                    map(
                        "csv parser references column names but no file path is available to read the header from"
                            .to_string(),
                    )
                })?;
                Some(read_header(path, delim).map_err(|e| map(e.to_string()))?)
            } else {
                None
            };
            let cols: Vec<CsvColumn> = columns
                .iter()
                .map(|c| CsvColumn {
                    name: c.name.clone(),
                    source: match &c.source {
                        CsvSourceConfig::Name(n) => CsvSource::Name(n.clone()),
                        CsvSourceConfig::Index(i) => CsvSource::Index(*i),
                    },
                    data_type: map_cell_type(c.r#type),
                })
                .collect();
            let p = CsvParser::try_new(cols, delim, has_header, header.as_deref())
                .map_err(|e| map(e.to_string()))?;
            Ok(Arc::new(p))
        }
        ParserConfig::Logfmt { columns } => {
            let cols: Vec<LogfmtColumn> = columns
                .iter()
                .map(|c| LogfmtColumn {
                    name: c.name.clone(),
                    key: c.key.clone(),
                    data_type: map_cell_type(c.r#type),
                })
                .collect();
            let p = LogfmtParser::try_new(cols).map_err(|e| map(e.to_string()))?;
            Ok(Arc::new(p))
        }
        ParserConfig::Regex { pattern, columns } => {
            let cols: Vec<RegexColumn> = columns
                .iter()
                .map(|c| RegexColumn {
                    name: c.name.clone(),
                    group: c.group.clone(),
                    data_type: map_cell_type(c.r#type),
                })
                .collect();
            let p = RegexParser::try_new(pattern, cols).map_err(|e| map(e.to_string()))?;
            Ok(Arc::new(p))
        }
        ParserConfig::Syslog { columns } => {
            let cols: Vec<SyslogColumn> = columns
                .iter()
                .map(|c| SyslogColumn {
                    name: c.name.clone(),
                    field: map_syslog_field(c.field),
                    data_type: map_cell_type(c.r#type),
                })
                .collect();
            let p = SyslogParser::try_new(cols).map_err(|e| map(e.to_string()))?;
            Ok(Arc::new(p))
        }
    }
}

fn map_indexed_column(c: &IndexedColumnConfig) -> IndexedColumn {
    IndexedColumn {
        name: c.name.clone(),
        kind: match c.kind {
            IndexKindConfig::ZoneMap => IndexKind::ZoneMap,
            IndexKindConfig::Bloom => IndexKind::Bloom,
            IndexKindConfig::ZoneMapAndBloom => IndexKind::ZoneMapAndBloom,
        },
    }
}

fn map_cell_type(t: CellTypeConfig) -> CellType {
    match t {
        CellTypeConfig::Int64 => CellType::Int64,
        CellTypeConfig::Float64 => CellType::Float64,
        CellTypeConfig::String => CellType::String,
        CellTypeConfig::Timestamp => CellType::Timestamp,
    }
}

fn map_jsonl_type(t: CellTypeConfig) -> JsonlType {
    match t {
        CellTypeConfig::Int64 => JsonlType::Int64,
        CellTypeConfig::Float64 => JsonlType::Float64,
        CellTypeConfig::String => JsonlType::String,
        CellTypeConfig::Timestamp => JsonlType::Timestamp,
    }
}

fn map_syslog_field(f: SyslogFieldConfig) -> SyslogField {
    match f {
        SyslogFieldConfig::Priority => SyslogField::Priority,
        SyslogFieldConfig::Facility => SyslogField::Facility,
        SyslogFieldConfig::Severity => SyslogField::Severity,
        SyslogFieldConfig::Version => SyslogField::Version,
        SyslogFieldConfig::Timestamp => SyslogField::Timestamp,
        SyslogFieldConfig::Hostname => SyslogField::Hostname,
        SyslogFieldConfig::AppName => SyslogField::AppName,
        SyslogFieldConfig::ProcId => SyslogField::ProcId,
        SyslogFieldConfig::MsgId => SyslogField::MsgId,
        SyslogFieldConfig::StructuredData => SyslogField::StructuredData,
        SyslogFieldConfig::Message => SyslogField::Message,
    }
}
