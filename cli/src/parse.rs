//! Parsing of arguments with units: sizes, durations, fsync policy, headers.
//! Pure logic: each function returns a readable error message.

use std::time::Duration;

use mojay::FsyncPolicy;

/// A size in bytes: `4096`, `512B`, `64KiB`, `64MiB`, `1G`… in base 1024.
/// The French units (`o`, `Kio`, `Mio`…) are still accepted.
pub fn size(text: &str) -> Result<u64, String> {
    let (number, unit) = split_number(text)?;
    let factor: u64 = match unit.to_ascii_lowercase().as_str() {
        "" | "o" | "b" => 1,
        "k" | "ko" | "kb" | "kio" | "kib" => 1 << 10,
        "m" | "mo" | "mb" | "mio" | "mib" => 1 << 20,
        "g" | "go" | "gb" | "gio" | "gib" => 1 << 30,
        "t" | "to" | "tb" | "tio" | "tib" => 1 << 40,
        _ => {
            return Err(format!(
                "unknown size unit \"{unit}\" (B, KiB, MiB, GiB, TiB)"
            ));
        }
    };
    number
        .checked_mul(factor)
        .ok_or_else(|| format!("size too large: {text}"))
}

/// A duration: `250ms`, `30s`, `15m`, `12h`, `7d` (or `7j`).
pub fn duration(text: &str) -> Result<Duration, String> {
    let (number, unit) = split_number(text)?;
    let seconds = match unit.to_ascii_lowercase().as_str() {
        "ms" => return Ok(Duration::from_millis(number)),
        "s" => 1,
        "m" | "min" => 60,
        "h" => 3600,
        "d" | "j" => 86_400,
        "" => return Err(format!("missing unit in \"{text}\" (ms, s, m, h, d)")),
        _ => {
            return Err(format!("unknown duration unit \"{unit}\" (ms, s, m, h, d)"));
        }
    };
    number
        .checked_mul(seconds)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("duration too large: {text}"))
}

/// The fsync policy: `always`, `never`, or an interval such as `100ms`.
pub fn fsync(text: &str) -> Result<FsyncPolicy, String> {
    match text {
        "always" => Ok(FsyncPolicy::Always),
        "never" => Ok(FsyncPolicy::Never),
        interval => duration(interval).map(FsyncPolicy::Interval).map_err(|_| {
            format!("\"{text}\": expected always, never, or an interval such as 100ms")
        }),
    }
}

/// A `key=value` header.
pub fn header(text: &str) -> Result<(String, String), String> {
    match text.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => Err(format!("\"{text}\": expected key=value")),
    }
}

fn split_number(text: &str) -> Result<(u64, &str), String> {
    let text = text.trim();
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return Err(format!("\"{text}\": a number is expected"));
    }
    let number = text[..digits]
        .parse()
        .map_err(|_| format!("number too large: {text}"))?;
    Ok((number, text[digits..].trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_accept_french_and_english_units() {
        assert_eq!(size("4096"), Ok(4096));
        assert_eq!(size("512o"), Ok(512));
        assert_eq!(size("64Kio"), Ok(64 << 10));
        assert_eq!(size("64MiB"), Ok(64 << 20));
        assert_eq!(size("2 Go"), Ok(2 << 30));
        assert!(size("12 apples").is_err());
        assert!(size("Mio").is_err());
        assert!(size("99999999999T").is_err());
    }

    #[test]
    fn durations_need_a_unit() {
        assert_eq!(duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(duration("15m"), Ok(Duration::from_secs(900)));
        assert_eq!(duration("7d"), duration("7j"));
        assert_eq!(duration("12h"), Ok(Duration::from_secs(43_200)));
        assert!(
            duration("30").is_err(),
            "without a unit, the ambiguity is rejected"
        );
        assert!(duration("3 weeks").is_err());
    }

    #[test]
    fn fsync_policies_and_headers() {
        assert_eq!(fsync("always"), Ok(FsyncPolicy::Always));
        assert_eq!(fsync("never"), Ok(FsyncPolicy::Never));
        assert_eq!(
            fsync("100ms"),
            Ok(FsyncPolicy::Interval(Duration::from_millis(100)))
        );
        assert!(fsync("sometimes").is_err());
        assert_eq!(header("trace=a=b"), Ok(("trace".into(), "a=b".into())));
        assert!(header("=x").is_err());
        assert!(header("no-equals").is_err());
    }
}
