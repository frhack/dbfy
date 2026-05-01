//! Built-in parsers for `LineParser`. Each is gated behind a cargo feature
//! so binary size scales with the formats actually needed.

#[cfg(any(
    feature = "regex",
    feature = "logfmt",
    feature = "csv",
    feature = "syslog"
))]
mod cells;

#[cfg(feature = "jsonl")]
pub mod jsonl;

#[cfg(feature = "csv")]
pub mod csv;

#[cfg(feature = "regex")]
pub mod regex;

#[cfg(feature = "logfmt")]
pub mod logfmt;

#[cfg(feature = "syslog")]
pub mod syslog;

#[cfg(feature = "csv")]
pub use csv::{CsvColumn, CsvParser, CsvSource};
#[cfg(feature = "jsonl")]
pub use jsonl::{JsonlColumn, JsonlParser};
#[cfg(feature = "logfmt")]
pub use logfmt::{LogfmtColumn, LogfmtParser};
#[cfg(feature = "regex")]
pub use regex::{RegexColumn, RegexParser};
#[cfg(feature = "syslog")]
pub use syslog::{SyslogColumn, SyslogField, SyslogParser};
