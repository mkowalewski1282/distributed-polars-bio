//! `DistBioProvider` — jeden, generyczny `TableProvider` owijający właściwy
//! provider z datafusion-bio-function-ranges.
//!
//! Po co owijanie: pola providerów vendora są prywatne, więc nie da się ich
//! odczytać przy serializacji. `DistBioProvider` trzyma obok nich jawny
//! `DistPayload` z parametrami konstrukcyjnymi, a `scan()` deleguje do
//! prawdziwego providera — czyli silnik algorytmiczny (COITrees) jest wciąż
//! używany, nic nie reimplementujemy.
//!
//! Uogólnienie względem Fazy A.4 (`DistOverlapProvider`): jeden typ zamiast
//! jednego typu na operację. Dodanie kolejnej operacji = jeden wariant enuma
//! + jedna gałąź `match` w `build()`.

use std::any::Any;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::{
    BioSessionExt, FilterOp, MergeProvider, NearestProvider, OverlapProvider, SubtractProvider,
};

use crate::coverage_node::DistCoverageProvider;
use crate::dist_payload::{DistPayload, TableRef};
use crate::runner::bio_session_config;

pub struct DistBioProvider {
    inner: Arc<dyn TableProvider>,
    payload: DistPayload,
}

impl fmt::Debug for DistBioProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DistBioProvider({:?})", self.payload)
    }
}

impl DistBioProvider {
    pub fn payload(&self) -> &DistPayload {
        &self.payload
    }

    /// Buduje własną, jednorazową sesję bio (nie dzieli stanu z sesją
    /// zapytania) — rejestruje źródła danych (Parquet albo CSV, patrz
    /// `register_source`) i konstruuje właściwy provider vendora.
    ///
    /// Samodzielne budowanie sesji przy każdej rekonstrukcji jest CELOWE: to
    /// dokładnie to, co musiałby zrobić prawdziwy executor w osobnym procesie
    /// (nie ma dostępu do pamięci klienta) — więc ten sam kod działa i w
    /// in-proc standalone, i w rozproszeniu wieloprocesowym.
    ///
    /// Sesja używa TEJ SAMEJ `bio_session_config()` co sesja klienta/schedulera.
    /// To istotne: ta sesja buduje plany fizyczne dzieci, więc decyduje o
    /// liczbie partycji, na podstawie której `EnforceDistribution` schedulera
    /// podejmuje potem decyzję o wstawieniu shuffle. Rozjazd konfiguracji
    /// między tymi dwoma miejscami cicho psuł dystrybucję (naprawione w Fazie H).
    pub async fn build(payload: DistPayload) -> Result<Self> {
        let session = Arc::new(DFSessionContext::new_with_bio(bio_session_config()));

        for t in payload.tables() {
            register_source(&session, t).await?;
        }

        let inner: Arc<dyn TableProvider> = match &payload {
            DistPayload::Overlap {
                left,
                right,
                cols,
                strict,
            } => {
                let left_schema = session.table(&left.name).await?.schema().as_arrow().clone();
                let right_schema = session.table(&right.name).await?.schema().as_arrow().clone();
                let filter_op = if *strict {
                    FilterOp::Strict
                } else {
                    FilterOp::Weak
                };
                Arc::new(OverlapProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    left_schema,
                    right_schema,
                    cols.as_vec(),
                    cols.as_vec(),
                    filter_op,
                ))
            }
            DistPayload::Merge {
                table,
                cols,
                min_dist,
                strict,
            } => Arc::new(MergeProvider::new(
                Arc::clone(&session),
                table.name.clone(),
                cols.as_tuple(),
                *min_dist,
                if *strict { FilterOp::Strict } else { FilterOp::Weak },
            )),
            DistPayload::Subtract {
                left,
                right,
                lcols,
                rcols,
                strict,
            } => {
                let left_schema = session.table(&left.name).await?.schema().as_arrow().clone();
                Arc::new(SubtractProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    lcols.as_tuple(),
                    rcols.as_tuple(),
                    if *strict { FilterOp::Strict } else { FilterOp::Weak },
                    left_schema,
                ))
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
                let left_schema = session.table(&left.name).await?.schema().as_arrow().clone();
                let right_schema = session.table(&right.name).await?.schema().as_arrow().clone();
                Arc::new(NearestProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    left_schema,
                    right_schema,
                    lcols.as_vec(),
                    rcols.as_vec(),
                    if *strict { FilterOp::Strict } else { FilterOp::Weak },
                    *include_overlaps,
                    *k as usize,
                    *compute_distance,
                ))
            }
            DistPayload::Coverage {
                left,
                right,
                lcols,
                rcols,
                strict,
                coverage,
            } => {
                let right_schema = session.table(&right.name).await?.schema().as_arrow().clone();
                Arc::new(DistCoverageProvider::new(
                    Arc::clone(&session),
                    left.name.clone(),
                    right.name.clone(),
                    right_schema,
                    lcols.as_tuple(),
                    rcols.as_tuple(),
                    if *strict { FilterOp::Strict } else { FilterOp::Weak },
                    *coverage,
                ))
            }
        };

        Ok(Self { inner, payload })
    }
}

/// Rejestruje źródło danych tabeli w sesji odbiorcy. Format rozpoznawany po
/// ścieżce: plik `*.parquet` albo katalog zawierający pliki `*.parquet` →
/// Parquet (zbiory databio-8p), wszystko inne → CSV (dotychczasowe dane
/// testowe). Ścieżka, której nie ma, to błąd z jej nazwą — wcześniej błąd
/// rejestracji był połykany, a zapytanie padało później na mylącym „table not
/// found”. Nazwa już zarejestrowana (ta sama tabela dwa razy w ładunku) jest
/// pomijana.
async fn register_source(session: &DFSessionContext, t: &TableRef) -> Result<()> {
    if session.table_exist(t.name.as_str())? {
        return Ok(());
    }
    let path = Path::new(&t.path);
    if !path.exists() {
        return Err(DataFusionError::Plan(format!(
            "brak danych tabeli {}: {}",
            t.name, t.path
        )));
    }
    if is_parquet_source(path) {
        session
            .register_parquet(&t.name, &t.path, ParquetReadOptions::default())
            .await
    } else {
        session
            .register_csv(&t.name, &t.path, CsvReadOptions::new())
            .await
    }
}

fn is_parquet_source(path: &Path) -> bool {
    let is_parquet = |p: &Path| p.extension().is_some_and(|ext| ext == "parquet");
    if path.is_dir() {
        std::fs::read_dir(path)
            .map(|entries| entries.flatten().any(|entry| is_parquet(&entry.path())))
            .unwrap_or(false)
    } else {
        is_parquet(path)
    }
}

#[async_trait]
impl TableProvider for DistBioProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}
