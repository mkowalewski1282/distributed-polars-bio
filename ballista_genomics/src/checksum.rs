//! A row-order-independent checksum of a result - the counterpart of `bench/checksum.py`,
//! with the same definition:
//!
//! - columns: as `KEY_COLUMNS` in `bench/ops.py` (nearest without the neighbour's identity);
//! - value code: a chromosome column - CRC-32 (IEEE) of the UTF-8 bytes, a numeric one - the
//!   value modulo 2^64 (two's complement), a missing value - 2^64 - 1;
//! - row hash: `mix64(sum M_j * code_j)` in arithmetic modulo 2^64 (the splitmix64 finalizer);
//! - checksum: the sum of the row hashes modulo 2^64, as `0x` + 16 hexadecimal digits.
//!
//! Agreement with Python: `tests/test_bench_client_protocol.py` of genomics-engines-bench
//! (reference values from the checksum test vectors, computed through `bench_client --checksum`).

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use crate::dist_payload::DistOp;

/// The code of a missing value (like -1 in two's complement; result columns are non-negative).
pub const NULL_CODE: u64 = u64::MAX;

/// Key-column multipliers by position - as `MULTIPLIERS` in `bench/checksum.py`.
pub const MULTIPLIERS: [u64; 6] = [
    0x9E37_79B1_85EB_CA87,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
    0x85EB_CA77_C2B2_AE63,
    0x27D4_EB2F_1656_67C5,
    0x9E37_79B9_7F4A_7C15,
];

/// Key columns in the normalized schema - as `KEY_COLUMNS` in `bench/ops.py`.
pub fn key_columns(op: DistOp) -> &'static [&'static str] {
    match op {
        DistOp::Overlap => &["chrom_1", "start_1", "end_1", "chrom_2", "start_2", "end_2"],
        DistOp::Nearest => &["chrom_1", "start_1", "end_1", "distance"],
        DistOp::Coverage => &["chrom", "start", "end", "coverage"],
        DistOp::Merge => &["chrom", "start", "end", "n_intervals"],
        DistOp::Subtract => &["chrom", "start", "end"],
    }
}

/// The splitmix64 finalizer (a bijection on 64-bit numbers).
pub fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A checksum computed incrementally over successive batches of the result.
pub struct Checksum {
    op: DistOp,
    sum: u64,
    rows: u64,
}

impl Checksum {
    pub fn new(op: DistOp) -> Self {
        Self { op, sum: 0, rows: 0 }
    }

    pub fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut codes = Vec::new();
        for name in key_columns(self.op) {
            let column = batch.column_by_name(name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "checksum {}: no column {name} in the result",
                    self.op.as_str()
                ))
            })?;
            codes.push(if name.starts_with("chrom") {
                chrom_codes(column.as_ref())?
            } else {
                int_codes(column.as_ref())?
            });
        }
        for row in 0..batch.num_rows() {
            let mut acc = 0u64;
            for (m, column) in MULTIPLIERS.iter().zip(&codes) {
                acc = acc.wrapping_add(m.wrapping_mul(column[row]));
            }
            self.sum = self.sum.wrapping_add(mix64(acc));
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn hex(&self) -> String {
        format!("0x{:016x}", self.sum)
    }
}

fn chrom_codes(column: &dyn Array) -> Result<Vec<u64>> {
    let utf8 = cast(column, &DataType::Utf8)?;
    let strings = utf8.as_string::<i32>();
    Ok((0..strings.len())
        .map(|i| {
            if strings.is_null(i) {
                NULL_CODE
            } else {
                u64::from(crc32fast::hash(strings.value(i).as_bytes()))
            }
        })
        .collect())
}

fn int_codes(column: &dyn Array) -> Result<Vec<u64>> {
    let ints = cast(column, &DataType::Int64)?;
    let values = ints.as_primitive::<Int64Type>();
    Ok((0..values.len())
        .map(|i| if values.is_null(i) { NULL_CODE } else { values.value(i) as u64 })
        .collect())
}
