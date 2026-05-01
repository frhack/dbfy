//! Read the byte ranges chosen by the pruner and feed them through the
//! parser, producing one `RecordBatch` per chunk.

use std::path::Path;

use arrow_array::RecordBatch;

use crate::index::Chunk;
use crate::parser::{LineParser, ParseError, ParseResult};

pub fn read_chunk(file: &Path, chunk: &Chunk, parser: &dyn LineParser) -> ParseResult<RecordBatch> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(file)?;
    let len = (chunk.byte_end - chunk.byte_start) as usize;
    let mut buf = vec![0u8; len];
    f.seek(SeekFrom::Start(chunk.byte_start))?;
    f.read_exact(&mut buf)?;
    parser
        .parse_chunk(&buf)
        .map_err(|err| ParseError::from(format!("reader: chunk parse failed: {err}")))
}
