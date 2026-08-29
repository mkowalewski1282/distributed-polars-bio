//! Ładunek logiczny: komplet parametrów potrzebnych, żeby po drugiej stronie
//! sieci ODTWORZYĆ provider operacji zakresowej.
//!
//! Kluczowa decyzja (Faza A.4, utrzymana i uogólniona w Fazie H): nie
//! serializujemy stanu providera ani planu, który on zbuduje — tylko argumenty
//! konstrukcyjne. Odbiorca (scheduler/executor) buduje własną sesję bio,
//! rejestruje te same pliki CSV i konstruuje provider od zera. To dokładnie to,
//! co musi zrobić prawdziwy executor w osobnym procesie, bo nie ma dostępu do
//! pamięci klienta.
//!
//! Jeden `enum DistPayload` zamiast osobnego typu per operacja — bo
//! `SessionConfig` przyjmuje dokładnie JEDEN `LogicalExtensionCodec`, więc
//! wariant per operacja oznaczałby łańcuch delegacji o tylu poziomach, ile
//! operacji (patrz plan, Faza H, decyzje architektoniczne).

use crate::codec_io::*;
use datafusion::common::Result;

/// Tag operacji — kodowany jako `u8` zaraz po MAGIC, po obu stronach kodeka.
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

    /// Nazwa funkcji tabelowej rejestrowanej w SQL.
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

/// Tabela do zarejestrowania po stronie odbiorcy: nazwa logiczna + ścieżka.
/// Ścieżka może wskazywać na pojedynczy plik CSV albo na KATALOG z wieloma
/// plikami — ten drugi wariant jest istotny, bo daje >1 partycję źródła, bez
/// czego Ballista może w ogóle nie wstawić shuffle (patrz plan, Faza H, §3-4).
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

/// Trójka nazw kolumn (kontig, start, koniec).
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
}

impl DistPayload {
    pub fn op(&self) -> DistOp {
        match self {
            DistPayload::Overlap { .. } => DistOp::Overlap,
            DistPayload::Merge { .. } => DistOp::Merge,
            DistPayload::Subtract { .. } => DistOp::Subtract,
        }
    }

    /// Wszystkie tabele, które odbiorca musi zarejestrować przed budową providera.
    pub fn tables(&self) -> Vec<&TableRef> {
        match self {
            DistPayload::Overlap { left, right, .. } => vec![left, right],
            DistPayload::Merge { table, .. } => vec![table],
            DistPayload::Subtract { left, right, .. } => vec![left, right],
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
        }
        buf
    }

    /// `None` oznacza „to nie jest nasz ładunek" — kodek ma wtedy delegować dalej.
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
            // Pozostałe operacje dochodzą w kolejnych krokach Fazy H.
            _ => None,
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
