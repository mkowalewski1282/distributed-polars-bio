use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext as DFSessionContext};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_bio_function_ranges::{FilterOp, OverlapProvider};
use datafusion_proto::logical_plan::LogicalExtensionCodec;

// ---------------------------------------------------------------------------
// Faza A.4 (plan pracy magisterskiej): LogicalExtensionCodec dla operatora
// overlap, żeby prawdziwa Ballista standalone (Faza A.3) mogła faktycznie
// wykonać zapytanie, a nie tylko odtworzyć błąd serializacji.
//
// KLUCZOWE ODKRYCIE (ballista-core-53.0.0/src/extension.rs): nie trzeba omijać
// wygodnej funkcji SessionContextExt::standalone_with_state() ani dodawać
// ballista-core/-scheduler/-executor jako osobnych zależności. Wystarczy
// ustawić własny kodek na SessionConfig przez SessionConfigExt (re-eksportowany
// przez ballista::prelude) — Ballista sama odczytuje go z konfiguracji sesji
// przy starcie schedulera i executora (new_standalone_executor_from_state()
// czyta session_state.config().ballista_logical_extension_codec()).
//
// DistOverlapProvider to WŁASNY (nasz) TableProvider — nie da się bezpośrednio
// serializować pól OverlapProvider z bio-function-ranges, bo są prywatne dla
// tego crate'a. Zamiast tego owijamy go: DistOverlapProvider trzyma jawne,
// publiczne (dla nas) pola potrzebne do odtworzenia, a w środku deleguje
// TableProvider::scan() do prawdziwego, wewnętrznego OverlapProvider (którego
// konstruktory SĄ publiczne) — więc silnik COITrees jest wciąż używany.
//
// Kodek serializuje tylko parametry konstrukcyjne (nazwy tabel, ścieżki CSV,
// nazwy kolumn, tryb strict/weak) — nie plan fizyczny — zgodnie z odkryciem
// z Fazy A.3 (OverlapProvider::scan() buduje SQL dynamicznie w środku).
// ---------------------------------------------------------------------------

const DATA_LEFT_TABLE: &str = "intervals_a";
const DATA_LEFT_CSV: &str = "data/intervals_a.csv";
const DATA_RIGHT_TABLE: &str = "intervals_b";
const DATA_RIGHT_CSV: &str = "data/intervals_b.csv";

// ---------------------------------------------------------------------------
// DistOverlapProvider: nasz TableProvider, owijający prawdziwy silnik
// ---------------------------------------------------------------------------

struct DistOverlapProvider {
    inner: OverlapProvider,
    payload: Payload,
}

impl fmt::Debug for DistOverlapProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DistOverlapProvider({:?})", self.payload)
    }
}

impl DistOverlapProvider {
    /// Buduje własną, jednorazową sesję bio (nie dzieli stanu z sesją
    /// zapytania) — rejestruje CSV i konstruuje OverlapProvider. Samodzielne
    /// budowanie sesji przy każdej rekonstrukcji jest celowe: to dokładnie to,
    /// co musiałby zrobić PRAWDZIWY executor w osobnym procesie (nie ma
    /// dostępu do pamięci klienta) — więc ten sam kod działa i w in-proc
    /// standalone, i (docelowo) w prawdziwym rozproszeniu wieloprocesowym.
    async fn build(payload: Payload) -> Result<Self> {
        // FAZA A.4b: IntervalJoinExec (fizyczny węzeł produkowany przez
        // IntervalJoinPhysicalOptimizationRule, algorithm=Coitrees) ma prywatne
        // pola — nie da się dla niego napisać PhysicalExtensionCodec z zewnątrz
        // crate'a tak samo łatwo jak dla OverlapProvider (którego konstruktory
        // są publiczne).
        //
        // Próba #1 (nieudana, zostawiona jako udokumentowana ślepa uliczka):
        // BioConfig.prefer_interval_join=false NIE wystarcza — ta flaga jest
        // czytana tylko przez BioQueryPlanner (custom QueryPlanner), a
        // IntervalJoinPhysicalOptimizationRule to OSOBNY mechanizm (physical
        // optimizer rule), instalowany bezwarunkowo przez
        // BioSessionExt::new_with_bio()/with_config_rt_bio() — flaga configu go
        // nie wyłącza.
        //
        // Rozwiązanie: OverlapProvider::scan() (patrz Faza A.3) potrzebuje
        // sesji tylko do wykonania zwykłego SQL joina (join_query() w
        // bio-function-ranges nie odwołuje się do żadnych funkcji bio typu
        // overlap()/coverage()) — więc zwykła, NIE-bio-owa sesja DataFusion
        // wystarczy i nie instaluje w ogóle IntervalJoinPhysicalOptimizationRule.
        // Kosztem: ta w pełni rozproszona ścieżka wykonania nie używa COITrees
        // (dostaje standardowy HashJoinExec+filtr, serializowalny domyślnym
        // kodekiem Ballisty). Lokalna ścieżka (Faza A.2) nadal używa COITrees
        // bez zmian — to udokumentowane ograniczenie tylko tej jednej,
        // w pełni rozproszonej ścieżki, patrz OPIS.md.
        let session = Arc::new(DFSessionContext::new());

        // Rejestracja może się powtórzyć (ta sama sesja bio bywa budowana
        // wielokrotnie w jednym procesie w trybie in-proc standalone) — błąd
        // "already exists" jest tu nieszkodliwy i celowo ignorowany.
        let _ = session
            .register_csv(&payload.left_table, &payload.left_csv, CsvReadOptions::new())
            .await;
        let _ = session
            .register_csv(&payload.right_table, &payload.right_csv, CsvReadOptions::new())
            .await;

        let left_schema = session
            .table(&payload.left_table)
            .await?
            .schema()
            .as_arrow()
            .clone();
        let right_schema = session
            .table(&payload.right_table)
            .await?
            .schema()
            .as_arrow()
            .clone();

        let filter_op = if payload.strict {
            FilterOp::Strict
        } else {
            FilterOp::Weak
        };
        let cols = vec![
            payload.col_chrom.clone(),
            payload.col_start.clone(),
            payload.col_end.clone(),
        ];

        let inner = OverlapProvider::new(
            session,
            payload.left_table.clone(),
            payload.right_table.clone(),
            left_schema,
            right_schema,
            cols.clone(),
            cols,
            filter_op,
        );

        Ok(Self { inner, payload })
    }
}

#[async_trait]
impl TableProvider for DistOverlapProvider {
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

// ---------------------------------------------------------------------------
// Rejestracja SQL: dist_overlap(left_table, left_csv, right_table, right_csv,
//                                col_chrom, col_start, col_end [, 'strict'])
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct DistOverlapTableFunction;

fn expect_string_literal(arg: &Expr, idx: usize) -> Result<String> {
    match arg {
        Expr::Literal(ScalarValue::Utf8(Some(v)), _) => Ok(v.clone()),
        other => Err(DataFusionError::Plan(format!(
            "dist_overlap() argument {idx} must be a string literal, got: {other}"
        ))),
    }
}

impl TableFunctionImpl for DistOverlapTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() < 7 {
            return Err(DataFusionError::Plan(
                "dist_overlap() expects: left_table, left_csv, right_table, right_csv, \
                 col_chrom, col_start, col_end [, 'strict'|'weak']"
                    .to_string(),
            ));
        }

        let strict = match args.get(7) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) => v.to_lowercase() == "strict",
            _ => false,
        };

        let payload = Payload {
            left_table: expect_string_literal(&args[0], 0)?,
            left_csv: expect_string_literal(&args[1], 1)?,
            right_table: expect_string_literal(&args[2], 2)?,
            right_csv: expect_string_literal(&args[3], 3)?,
            col_chrom: expect_string_literal(&args[4], 4)?,
            col_start: expect_string_literal(&args[5], 5)?,
            col_end: expect_string_literal(&args[6], 6)?,
            strict,
        };

        // TableFunctionImpl::call jest synchroniczne, a budowa providera wymaga
        // async (rejestracja CSV, lookup schematu) — ten sam wzorzec
        // (block_in_place + block_on) jest już użyty wewnątrz samego
        // datafusion-bio-function-ranges (table_function.rs).
        let provider = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(DistOverlapProvider::build(payload))
        })?;

        Ok(Arc::new(provider))
    }
}

// ---------------------------------------------------------------------------
// Payload: dane potrzebne do odtworzenia DistOverlapProvider po drugiej
// stronie sieci — ręczne, minimalne kodowanie binarne (bez protobuf/serde,
// bo to tylko kilka stringów + jeden bool, patrz OPIS.md).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Payload {
    left_table: String,
    left_csv: String,
    right_table: String,
    right_csv: String,
    col_chrom: String,
    col_start: String,
    col_end: String,
    strict: bool,
}

const PAYLOAD_MAGIC: u32 = 0xD157_0001;

fn encode_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn decode_string(buf: &[u8], pos: &mut usize) -> Option<String> {
    let len = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
    *pos += 4;
    let s = String::from_utf8(buf.get(*pos..*pos + len)?.to_vec()).ok()?;
    *pos += len;
    Some(s)
}

fn encode_payload(p: &Payload) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&PAYLOAD_MAGIC.to_le_bytes());
    encode_string(&mut buf, &p.left_table);
    encode_string(&mut buf, &p.left_csv);
    encode_string(&mut buf, &p.right_table);
    encode_string(&mut buf, &p.right_csv);
    encode_string(&mut buf, &p.col_chrom);
    encode_string(&mut buf, &p.col_start);
    encode_string(&mut buf, &p.col_end);
    buf.push(p.strict as u8);
    buf
}

fn decode_payload(buf: &[u8]) -> Option<Payload> {
    if buf.len() < 4 || u32::from_le_bytes(buf[0..4].try_into().ok()?) != PAYLOAD_MAGIC {
        return None;
    }
    let mut pos = 4usize;
    Some(Payload {
        left_table: decode_string(buf, &mut pos)?,
        left_csv: decode_string(buf, &mut pos)?,
        right_table: decode_string(buf, &mut pos)?,
        right_csv: decode_string(buf, &mut pos)?,
        col_chrom: decode_string(buf, &mut pos)?,
        col_start: decode_string(buf, &mut pos)?,
        col_end: decode_string(buf, &mut pos)?,
        strict: *buf.get(pos)? != 0,
    })
}

// ---------------------------------------------------------------------------
// LogicalExtensionCodec: to jest to, czego brakowało w Fazie A.3
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct DistOverlapLogicalCodec {
    /// Domyślny kodek Ballisty (dla wszystkiego, co NIE jest naszym
    /// DistOverlapProvider) — pobrany jako trait object z pustego
    /// SessionConfig, żeby nie dodawać ballista-core jako osobnej zależności
    /// tylko po to, by nazwać konkretny typ BallistaLogicalExtensionCodec.
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl Default for DistOverlapLogicalCodec {
    fn default() -> Self {
        Self {
            inner: SessionConfig::new().ballista_logical_extension_codec(),
        }
    }
}

impl LogicalExtensionCodec for DistOverlapLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        match decode_payload(buf) {
            Some(payload) => {
                let provider = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(DistOverlapProvider::build(payload))
                })?;
                Ok(Arc::new(provider))
            }
            None => self.inner.try_decode_table_provider(buf, table_ref, schema, ctx),
        }
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        match node.as_any().downcast_ref::<DistOverlapProvider>() {
            Some(p) => {
                buf.extend_from_slice(&encode_payload(&p.payload));
                Ok(())
            }
            None => self.inner.try_encode_table_provider(table_ref, node, buf),
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("  Faza A.4: dist_overlap + LogicalExtensionCodec + Ballista standalone");
    println!("============================================================\n");

    // UWAGA (druga, subtelniejsza przyczyna IntervalJoinExec w planie
    // rozproszonym): nie wystarczy, że WEWNĘTRZNA sesja w DistOverlapProvider
    // jest "plain" — fizyczne reguły optymalizatora działają na CAŁYM drzewie
    // planu (transform_up/down), więc jeśli sesja SCHEDULERA (ta budowana
    // tutaj, przekazywana do standalone_with_state) jest bio-owa, jej
    // IntervalJoinPhysicalOptimizationRule i tak ponownie przepisze join
    // ukryty w poddrzewie zwróconym przez nasz TableProvider — niezależnie od
    // tego, jaka sesja go zbudowała. Sesja schedulera/klienta MUSI więc też
    // być zwykła (nie new_with_bio), skoro ta w pełni rozproszona ścieżka i
    // tak rezygnuje z COITrees (patrz komentarz w DistOverlapProvider::build).
    let logical_codec: Arc<dyn LogicalExtensionCodec> = Arc::new(DistOverlapLogicalCodec::default());
    let config = SessionConfig::new().with_ballista_logical_extension_codec(logical_codec);
    let bio_ctx = DFSessionContext::new_with_config(config);
    let state = bio_ctx.state();

    println!("Łączenie z Ballista standalone (scheduler + executor in-proc)...");
    let ctx = DFSessionContext::standalone_with_state(state).await?;
    println!("Klaster Ballista wystartował.\n");

    // register_udtf działa na SessionState (rejestr funkcji), ale rejestrujemy
    // jawnie też na `ctx` (obiekt zwrócony po stronie ballista-owej) na wszelki
    // wypadek, gdyby rejestr nie był w pełni współdzielony przez samo .state().
    ctx.register_udtf("dist_overlap", Arc::new(DistOverlapTableFunction));

    let t0 = Instant::now();
    let df = ctx
        .sql(&format!(
            "SELECT left_chrom AS chrom, \
                    left_start AS start_a, left_end AS end_a, left_name AS name_a, \
                    right_start AS start_b, right_end AS end_b, right_name AS name_b \
             FROM dist_overlap('{DATA_LEFT_TABLE}', '{DATA_LEFT_CSV}', \
                               '{DATA_RIGHT_TABLE}', '{DATA_RIGHT_CSV}', \
                               'chrom', 'start', 'end', 'strict') \
             ORDER BY chrom, start_a, start_b"
        ))
        .await?;
    let result = df.collect().await?;
    let elapsed = t0.elapsed();

    println!("Czas: {:.4}s", elapsed.as_secs_f64());
    for batch in &result {
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch.clone()])?
        );
    }
    println!("============================================================");

    Ok(())
}
