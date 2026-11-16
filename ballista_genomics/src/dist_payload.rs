//! The logical payload: the complete set of parameters needed to RECREATE a range-operation
//! provider on the other side of the network.
//!
//! The key decision: we do not serialize the provider's state or the plan it will build -
//! only the constructor arguments. The receiver (scheduler/executor) builds its own bio
//! session, registers the same data files and constructs the provider from scratch. This is
//! exactly what a real executor in a separate process must do, since it has no access to the
//! client's memory.
//!
//! A single `enum DistPayload` instead of a separate type per operation - because
//! `SessionConfig` accepts exactly ONE `LogicalExtensionCodec`, so a per-operation
//! variant would mean a delegation chain with as many levels as there are
//! operations.

use crate::codec_io::*;
use datafusion::common::Result;

/// The operation tag - encoded as a `u8` right after MAGIC, on both sides of the codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistOp {
    Overlap = 0,
    Merge = 1,
    Subtract = 2,
    Nearest = 3,
    Coverage = 4,
}

impl DistOp {
    pub const ALL: [DistOp; 5] = [
        DistOp::Overlap,
        DistOp::Merge,
        DistOp::Subtract,
        DistOp::Nearest,
        DistOp::Coverage,
    ];

    pub fn tag(self) -> u8 {
        self as u8
    }

    pub fn from_tag(b: u8) -> Option<Self> {
        Some(match b {
            0 => DistOp::Overlap,
            1 => DistOp::Merge,
            2 => DistOp::Subtract,
            3 => DistOp::Nearest,
            4 => DistOp::Coverage,
            _ => return None,
        })
    }

    /// The name of the table function registered in SQL.
    pub fn udtf_name(self) -> &'static str {
        match self {
            DistOp::Overlap => "dist_overlap",
            DistOp::Merge => "dist_merge",
            DistOp::Subtract => "dist_subtract",
            DistOp::Nearest => "dist_nearest",
            DistOp::Coverage => "dist_coverage",
        }
    }

    pub fn from_cli(s: &str) -> Option<Self> {
        Some(match s {
            "overlap" => DistOp::Overlap,
            "merge" => DistOp::Merge,
            "subtract" => DistOp::Subtract,
            "nearest" => DistOp::Nearest,
            "coverage" => DistOp::Coverage,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            DistOp::Overlap => "overlap",
            DistOp::Merge => "merge",
            DistOp::Subtract => "subtract",
            DistOp::Nearest => "nearest",
            DistOp::Coverage => "coverage",
        }
    }
}

/// A table to register on the receiver's side: a logical name + a path.
/// The path can point to a single CSV file or to a DIRECTORY with several
/// files - the latter matters because it gives more than one source partition, without
/// which Ballista may not insert a shuffle at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub name: String,
    pub path: String,
}

impl TableRef {
    pub fn new(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
        }
    }
}

/// A triple of column names (contig, start, end).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cols(pub String, pub String, pub String);

impl Cols {
    pub fn as_tuple(&self) -> (String, String, String) {
        (self.0.clone(), self.1.clone(), self.2.clone())
    }

    pub fn as_vec(&self) -> Vec<String> {
        vec![self.0.clone(), self.1.clone(), self.2.clone()]
    }
}

pub const DIST_LOGICAL_MAGIC: u32 = 0xD157_0004;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistPayload {
    Overlap {
        left: TableRef,
        right: TableRef,
        cols: Cols,
        strict: bool,
    },
    Merge {
        table: TableRef,
        cols: Cols,
        min_dist: i64,
        strict: bool,
    },
    Subtract {
        left: TableRef,
        right: TableRef,
        lcols: Cols,
        rcols: Cols,
        strict: bool,
    },
    Nearest {
        left: TableRef,
        right: TableRef,
        lcols: Cols,
        rcols: Cols,
        strict: bool,
        k: u32,
        include_overlaps: bool,
        compute_distance: bool,
    },
    Coverage {
        left: TableRef,
        right: TableRef,
        lcols: Cols,
        rcols: Cols,
        strict: bool,
        coverage: bool,
    },
}

impl DistPayload {
    pub fn op(&self) -> DistOp {
        match self {
            DistPayload::Overlap { .. } => DistOp::Overlap,
            DistPayload::Merge { .. } => DistOp::Merge,
            DistPayload::Subtract { .. } => DistOp::Subtract,
            DistPayload::Nearest { .. } => DistOp::Nearest,
            DistPayload::Coverage { .. } => DistOp::Coverage,
        }
    }

    /// All the tables the receiver must register before building the provider.
    pub fn tables(&self) -> Vec<&TableRef> {
        match self {
            DistPayload::Overlap { left, right, .. } => vec![left, right],
            DistPayload::Merge { table, .. } => vec![table],
            DistPayload::Subtract { left, right, .. } => vec![left, right],
            DistPayload::Nearest { left, right, .. } => vec![left, right],
            DistPayload::Coverage { left, right, .. } => vec![left, right],
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_magic(&mut buf, DIST_LOGICAL_MAGIC);
        write_u8(&mut buf, self.op().tag());
        match self {
            DistPayload::Overlap {
                left,
                right,
                cols,
                strict,
            } => {
                write_table(&mut buf, left);
                write_table(&mut buf, right);
                write_cols(&mut buf, &cols.as_tuple());
                write_bool(&mut buf, *strict);
            }
            DistPayload::Merge {
                table,
                cols,
                min_dist,
                strict,
            } => {
                write_table(&mut buf, table);
                write_cols(&mut buf, &cols.as_tuple());
                write_i64(&mut buf, *min_dist);
                write_bool(&mut buf, *strict);
            }
            DistPayload::Subtract {
                left,
                right,
                lcols,
                rcols,
                strict,
            } => {
                write_table(&mut buf, left);
                write_table(&mut buf, right);
                write_cols(&mut buf, &lcols.as_tuple());
                write_cols(&mut buf, &rcols.as_tuple());
                write_bool(&mut buf, *strict);
            }
            DistPayload::Nearest {
                left,
                right,
                lcols,
                rcols,
                strict,
                k,
                include_overlaps,
                compute_distance,
            } => {
                write_table(&mut buf, left);
                write_table(&mut buf, right);
                write_cols(&mut buf, &lcols.as_tuple());
                write_cols(&mut buf, &rcols.as_tuple());
                write_bool(&mut buf, *strict);
                write_u32(&mut buf, *k);
                write_bool(&mut buf, *include_overlaps);
                write_bool(&mut buf, *compute_distance);
            }
            DistPayload::Coverage {
                left,
                right,
                lcols,
                rcols,
                strict,
                coverage,
            } => {
                write_table(&mut buf, left);
                write_table(&mut buf, right);
                write_cols(&mut buf, &lcols.as_tuple());
                write_cols(&mut buf, &rcols.as_tuple());
                write_bool(&mut buf, *strict);
                write_bool(&mut buf, *coverage);
            }
        }
        buf
    }

    /// `None` means "this is not our payload" - the codec should then delegate onwards.
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if !has_magic(buf, DIST_LOGICAL_MAGIC) {
            return None;
        }
        let mut pos = 4usize;
        let op = DistOp::from_tag(read_u8(buf, &mut pos).ok()?)?;
        match op {
            DistOp::Overlap => Some(DistPayload::Overlap {
                left: read_table(buf, &mut pos).ok()?,
                right: read_table(buf, &mut pos).ok()?,
                cols: read_cols_struct(buf, &mut pos).ok()?,
                strict: read_bool(buf, &mut pos).ok()?,
            }),
            DistOp::Merge => Some(DistPayload::Merge {
                table: read_table(buf, &mut pos).ok()?,
                cols: read_cols_struct(buf, &mut pos).ok()?,
                min_dist: read_i64(buf, &mut pos).ok()?,
                strict: read_bool(buf, &mut pos).ok()?,
            }),
            DistOp::Subtract => Some(DistPayload::Subtract {
                left: read_table(buf, &mut pos).ok()?,
                right: read_table(buf, &mut pos).ok()?,
                lcols: read_cols_struct(buf, &mut pos).ok()?,
                rcols: read_cols_struct(buf, &mut pos).ok()?,
                strict: read_bool(buf, &mut pos).ok()?,
            }),
            DistOp::Nearest => Some(DistPayload::Nearest {
                left: read_table(buf, &mut pos).ok()?,
                right: read_table(buf, &mut pos).ok()?,
                lcols: read_cols_struct(buf, &mut pos).ok()?,
                rcols: read_cols_struct(buf, &mut pos).ok()?,
                strict: read_bool(buf, &mut pos).ok()?,
                k: read_u32(buf, &mut pos).ok()?,
                include_overlaps: read_bool(buf, &mut pos).ok()?,
                compute_distance: read_bool(buf, &mut pos).ok()?,
            }),
            DistOp::Coverage => Some(DistPayload::Coverage {
                left: read_table(buf, &mut pos).ok()?,
                right: read_table(buf, &mut pos).ok()?,
                lcols: read_cols_struct(buf, &mut pos).ok()?,
                rcols: read_cols_struct(buf, &mut pos).ok()?,
                strict: read_bool(buf, &mut pos).ok()?,
                coverage: read_bool(buf, &mut pos).ok()?,
            }),
        }
    }
}

fn write_table(buf: &mut Vec<u8>, t: &TableRef) {
    write_str(buf, &t.name);
    write_str(buf, &t.path);
}

fn read_table(buf: &[u8], pos: &mut usize) -> Result<TableRef> {
    Ok(TableRef {
        name: read_str(buf, pos)?,
        path: read_str(buf, pos)?,
    })
}

fn read_cols_struct(buf: &[u8], pos: &mut usize) -> Result<Cols> {
    let (a, b, c) = read_cols(buf, pos)?;
    Ok(Cols(a, b, c))
}
