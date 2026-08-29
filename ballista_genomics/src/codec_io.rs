//! Wspólne prymitywy kodowania binarnego dla wszystkich kodeków tego crate'a.
//!
//! Konwencje (te same, których używał już `physical_codec.rs` w Fazie A.5):
//! - liczby little-endian,
//! - sekwencje bajtów i stringi length-prefiksowane `u32`,
//! - bool jako pojedynczy bajt 0/1.
//!
//! Formatu nie trzeba wersjonować: klient, scheduler i executor pochodzą z tego
//! samego procesu i tej samej binarki (Ballista standalone), więc ładunek żyje
//! dokładnie jedną sesję.

use datafusion::common::{DataFusionError, Result};

pub fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32> {
    let bytes = buf
        .get(*pos..*pos + 4)
        .ok_or_else(|| DataFusionError::Internal("codec_io: bufor obcięty (u32)".into()))?;
    *pos += 4;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

pub fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn read_i64(buf: &[u8], pos: &mut usize) -> Result<i64> {
    let bytes = buf
        .get(*pos..*pos + 8)
        .ok_or_else(|| DataFusionError::Internal("codec_io: bufor obcięty (i64)".into()))?;
    *pos += 8;
    Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
}

pub fn write_bool(buf: &mut Vec<u8>, v: bool) {
    buf.push(v as u8);
}

pub fn read_bool(buf: &[u8], pos: &mut usize) -> Result<bool> {
    let b = *buf
        .get(*pos)
        .ok_or_else(|| DataFusionError::Internal("codec_io: bufor obcięty (bool)".into()))?;
    *pos += 1;
    Ok(b != 0)
}

pub fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

pub fn read_u8(buf: &[u8], pos: &mut usize) -> Result<u8> {
    let b = *buf
        .get(*pos)
        .ok_or_else(|| DataFusionError::Internal("codec_io: bufor obcięty (u8)".into()))?;
    *pos += 1;
    Ok(b)
}

pub fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

pub fn read_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let len = read_u32(buf, pos)? as usize;
    let slice = buf
        .get(*pos..*pos + len)
        .ok_or_else(|| DataFusionError::Internal("codec_io: bufor obcięty (bytes)".into()))?;
    *pos += len;
    Ok(slice)
}

pub fn write_str(buf: &mut Vec<u8>, s: &str) {
    write_bytes(buf, s.as_bytes());
}

pub fn read_str(buf: &[u8], pos: &mut usize) -> Result<String> {
    let bytes = read_bytes(buf, pos)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| DataFusionError::Internal(format!("codec_io: niepoprawny UTF-8: {e}")))
}

/// Trójka nazw kolumn (kontig, start, koniec) — powtarza się w KAŻDEJ operacji
/// zakresowej z datafusion-bio-function-ranges, stąd osobny helper.
pub fn write_cols(buf: &mut Vec<u8>, c: &(String, String, String)) {
    write_str(buf, &c.0);
    write_str(buf, &c.1);
    write_str(buf, &c.2);
}

pub fn read_cols(buf: &[u8], pos: &mut usize) -> Result<(String, String, String)> {
    Ok((
        read_str(buf, pos)?,
        read_str(buf, pos)?,
        read_str(buf, pos)?,
    ))
}

/// Sprawdza prefiks MAGIC bez przesuwania pozycji. Zwraca `false` także dla
/// buforów krótszych niż 4 bajty — wtedy ładunek na pewno nie jest nasz.
pub fn has_magic(buf: &[u8], magic: u32) -> bool {
    buf.len() >= 4 && u32::from_le_bytes(buf[0..4].try_into().unwrap()) == magic
}

pub fn write_magic(buf: &mut Vec<u8>, magic: u32) {
    buf.extend_from_slice(&magic.to_le_bytes());
}

// --- schemat Arrow ---------------------------------------------------------
//
// Schemat wyjsciowy serializujemy ZAWSZE, przez protobuf datafusion-proto.
// Alternatywa (odtwarzanie go po stronie executora z parametrow) oznaczalaby
// powielenie logiki konstruktorow providerow vendora - a ta logika jest rozna
// dla kazdej operacji i moglaby sie z nia cicho rozjechac.

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion_proto::protobuf;
use prost::Message;
use std::sync::Arc;

pub fn write_schema(buf: &mut Vec<u8>, schema: &Schema) -> Result<()> {
    let proto = protobuf::Schema::try_from(schema)
        .map_err(|e| DataFusionError::Internal(format!("codec_io: schema -> proto: {e}")))?;
    write_bytes(buf, &proto.encode_to_vec());
    Ok(())
}

pub fn read_schema(buf: &[u8], pos: &mut usize) -> Result<SchemaRef> {
    let bytes = read_bytes(buf, pos)?;
    let proto = protobuf::Schema::decode(bytes)
        .map_err(|e| DataFusionError::Internal(format!("codec_io: proto decode: {e}")))?;
    let schema = Schema::try_from(&proto)
        .map_err(|e| DataFusionError::Internal(format!("codec_io: proto -> schema: {e}")))?;
    Ok(Arc::new(schema))
}

// --- RecordBatch przez Arrow IPC -------------------------------------------
//
// Uzywane przez operacje o wzorcu BROADCAST (nearest, coverage): lewa,
// indeksowana tabela jedzie w calosci w ladunku planu fizycznego do kazdego
// executora, ktory odbudowuje z niej indeks lokalnie.
//
// GRANICA SKALOWALNOSCI: ladunek planu idzie przez gRPC, a Ballista ustawia
// max_message_size = 16 MB (ballista-core/src/utils.rs). Przekroczenie tego
// limitu wywali zapytanie na poziomie transportu. To jest wlasciwe ograniczenie
// tego podejscia i nalezy je raportowac jako takie, a nie obchodzic.

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;

fn arrow_err(e: ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// `StreamWriter` zapisuje komunikat ze SCHEMATEM przy konstrukcji, wiec nawet
/// pusty batch (0 wierszy) round-trip'uje z poprawnym schematem.
pub fn write_batch_ipc(buf: &mut Vec<u8>, batch: &RecordBatch) -> Result<()> {
    let mut w = StreamWriter::try_new(Vec::<u8>::new(), batch.schema_ref()).map_err(arrow_err)?;
    w.write(batch).map_err(arrow_err)?;
    w.finish().map_err(arrow_err)?;
    let bytes = w.into_inner().map_err(arrow_err)?;
    write_bytes(buf, &bytes);
    Ok(())
}

pub fn read_batch_ipc(buf: &[u8], pos: &mut usize) -> Result<RecordBatch> {
    let bytes = read_bytes(buf, pos)?;
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
        .map_err(arrow_err)?;
    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(arrow_err)?;
    concat_batches(&schema, &batches).map_err(arrow_err)
}
