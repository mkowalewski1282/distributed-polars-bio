//! Strict command-line flag parsing shared by the `ballista_node` and `bench_client`
//! binaries. An unknown or repeated argument is an error: a typo in a flag name must
//! not silently run the program with a default value, and a repetition must not
//! silently run it with an unintended value.

use std::collections::HashMap;
use std::str::FromStr;

pub type Flags = HashMap<String, Option<String>>;

/// Parses `--key value` flags and flags without a value.
pub fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    bool_flags: &[&str],
) -> Result<Flags, String> {
    let mut flags = Flags::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let value = if value_flags.contains(&arg.as_str()) {
            Some(it.next().ok_or_else(|| format!("missing value for {arg}"))?.clone())
        } else if bool_flags.contains(&arg.as_str()) {
            None
        } else {
            return Err(format!("unknown argument: {arg}"));
        };
        if flags.insert(arg.clone(), value).is_some() {
            return Err(format!("repeated argument: {arg}"));
        }
    }
    Ok(flags)
}

/// A required flag with a value of the given type.
pub fn required<T: FromStr>(flags: &Flags, name: &str) -> Result<T, String> {
    optional(flags, name)?.ok_or_else(|| format!("missing required flag {name}"))
}

/// An optional flag with a value of the given type; an absent flag -> `None`.
pub fn optional<T: FromStr>(flags: &Flags, name: &str) -> Result<Option<T>, String> {
    match flags.get(name).and_then(|v| v.as_deref()) {
        None => Ok(None),
        Some(v) => v
            .parse::<T>()
            .map(Some)
            .map_err(|_| format!("invalid value for flag {name}")),
    }
}
