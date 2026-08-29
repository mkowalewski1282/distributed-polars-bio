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
