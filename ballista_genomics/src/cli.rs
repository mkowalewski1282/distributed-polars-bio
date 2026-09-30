//! Ścisłe parsowanie flag wiersza poleceń, wspólne dla binarek `ballista_node`
//! i `bench_client`. Nieznany albo powtórzony argument to błąd: literówka w nazwie
//! flagi nie może cicho uruchomić programu z wartością domyślną, a powtórzenie —
//! z inną wartością niż zamierzona.

use std::collections::HashMap;
use std::str::FromStr;

pub type Flags = HashMap<String, Option<String>>;

/// Parsuje `--klucz wartość` i flagi bez wartości.
pub fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    bool_flags: &[&str],
) -> Result<Flags, String> {
    let mut flags = Flags::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let value = if value_flags.contains(&arg.as_str()) {
            Some(it.next().ok_or_else(|| format!("brak wartości dla {arg}"))?.clone())
        } else if bool_flags.contains(&arg.as_str()) {
            None
        } else {
            return Err(format!("nieznany argument: {arg}"));
        };
        if flags.insert(arg.clone(), value).is_some() {
            return Err(format!("powtórzony argument: {arg}"));
        }
    }
    Ok(flags)
}

/// Wymagana flaga z wartością danego typu.
pub fn required<T: FromStr>(flags: &Flags, name: &str) -> Result<T, String> {
    optional(flags, name)?.ok_or_else(|| format!("brak wymaganej flagi {name}"))
}

/// Opcjonalna flaga z wartością danego typu; brak flagi → `None`.
pub fn optional<T: FromStr>(flags: &Flags, name: &str) -> Result<Option<T>, String> {
    match flags.get(name).and_then(|v| v.as_deref()) {
        None => Ok(None),
        Some(v) => v
            .parse::<T>()
            .map(Some)
            .map_err(|_| format!("niepoprawna wartość flagi {name}")),
    }
}
