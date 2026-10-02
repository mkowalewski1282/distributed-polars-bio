//! Scenariusz narzędzia pomiarowego → zapytanie SQL z wynikiem w schemacie
//! znormalizowanym (specyfikacja, sekcja 8.4; odpowiednik `bench/ops.py`).
//!
//! Argumenty funkcji `dist_*` odwzorowują SQL-e z `runner::spec()` 1:1 — te
//! same flagi strict/weak i ta sama odwrócona konwencja coverage — bo tamte są
//! zweryfikowane testami względem polars-bio. Różnice: dowolne ścieżki i nazwy
//! kolumn, jawne nazwy wyjściowe, brak ORDER BY (wynik konsumowany
//! strumieniowo, porównania nie zależą od kolejności wierszy).

use crate::dist_payload::DistOp;

/// Jedna operacja na wskazanych danych; `right` tylko dla operacji binarnych.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub op: DistOp,
    pub left: String,
    pub right: Option<String>,
    /// Nazwy kolumn przedziału: kontig, początek, koniec.
    pub cols: [String; 3],
}

/// Literał tekstowy SQL (apostrof podwojony — ścieżki bywają dowolne).
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Identyfikator SQL w cudzysłowie: nazwy kolumn bywają słowami kluczowymi
/// (`end`), a bez cudzysłowu DataFusion zamieniłby wielkie litery na małe.
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn col(source: &str, alias: &str) -> String {
    format!("{} AS {}", ident(source), ident(alias))
}

impl Scenario {
    pub fn validate(&self) -> Result<(), String> {
        match (self.op, self.right.is_some()) {
            (DistOp::Merge, true) => Err("merge działa na jednej tabeli — bez --right".to_string()),
            (DistOp::Merge, false) | (_, true) => Ok(()),
            (op, false) => Err(format!("{} wymaga --right", op.as_str())),
        }
    }

    pub fn sql(&self) -> String {
        let [c, s, e] = &self.cols;
        let cols = format!("{}, {}, {}", lit(c), lit(s), lit(e));
        let left = lit(&self.left);
        let right = lit(self.right.as_deref().unwrap_or_default());
        let side = |prefix: &str, n: u8| {
            [
                col(&format!("{prefix}_{c}"), &format!("chrom_{n}")),
                col(&format!("{prefix}_{s}"), &format!("start_{n}")),
                col(&format!("{prefix}_{e}"), &format!("end_{n}")),
            ]
            .join(", ")
        };
        let own = [col(c, "chrom"), col(s, "start"), col(e, "end")].join(", ");
        match self.op {
            DistOp::Overlap => format!(
                "SELECT {}, {} FROM dist_overlap('df1', {left}, 'df2', {right}, {cols}, 'strict')",
                side("left", 1),
                side("right", 2)
            ),
            // Konwencja dostawcy jak w coverage: lewa tabela jest indeksowana (i
            // broadcastowana), a wynik ma JEDEN wiersz na każdy wiersz PRAWEJ tabeli,
            // z najbliższym sąsiadem z lewej. pb.nearest(lewa, prawa) daje wiersz na
            // każdy wiersz lewej — więc strony są zamienione: 'df1' (nasza lewa) idzie
            // jako prawa (odpytywana), a kolumny _1 pochodzą z right_*, _2 z left_*.
            // 'strict' jak w pozostałych operacjach: dane są 0-based, półotwarte. Bez niego
            // NearestExec liczy w konwencji 1-based — od v0.22.2 odległość wychodzi o 1 mniejsza
            // niż w polars-bio (plan 3b-1, Zadanie 3).
            DistOp::Nearest => format!(
                "SELECT {}, {}, {} FROM dist_nearest('df2', {right}, 'df1', {left}, 1, true, true, {cols}, 'strict')",
                side("right", 1),
                side("left", 2),
                col("distance", "distance")
            ),
            // Odwrócona konwencja z runner::spec(): 'reads' = prawa, 'targets' = lewa,
            // więc wynik to wiersze lewej tabeli z pokryciem przez prawą — jak
            // pb.coverage(lewa, prawa).
            DistOp::Coverage => format!(
                "SELECT {own}, {} FROM dist_coverage('df2', {right}, 'df1', {left}, {cols}, 'strict')",
                col("coverage", "coverage")
            ),
            DistOp::Merge => format!(
                "SELECT {own}, {} FROM dist_merge('df1', {left}, {cols}, 0, 'strict')",
                col("n_intervals", "n_intervals")
            ),
            DistOp::Subtract => format!(
                "SELECT {own} FROM dist_subtract('df1', {left}, 'df2', {right}, {cols}, 'strict')"
            ),
        }
    }
}
