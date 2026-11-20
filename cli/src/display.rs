//! Human-readable formatting: bytes, sizes, durations, dates.
//! Pure logic.

use std::time::Duration;

use mojay::Header;

/// Values longer than this are truncated (unless `--full` is given).
const MAX_SHOWN_CHARS: usize = 120;

/// Readable bytes: quoted text if they are valid UTF-8,
/// hexadecimal otherwise. Control characters are escaped.
pub fn bytes(value: &[u8], full: bool) -> String {
    let shown = match std::str::from_utf8(value) {
        Ok(text) => format!("{text:?}"),
        Err(_) => format!("0x{}", hex(value)),
    };
    if full || shown.chars().count() <= MAX_SHOWN_CHARS {
        return shown;
    }
    let cut: String = shown.chars().take(MAX_SHOWN_CHARS).collect();
    format!("{cut}… ({})", size(value.len() as u64))
}

/// An optional key or value: `null` if absent.
pub fn optional_bytes(value: Option<&[u8]>, full: bool) -> String {
    value.map_or_else(|| "null".to_string(), |v| bytes(v, full))
}

pub fn headers(headers: &[Header], full: bool) -> String {
    headers
        .iter()
        .map(|h| format!("{}={}", h.key, bytes(&h.value, full)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|b| format!("{b:02x}")).collect()
}

/// A size in binary units: `512 B`, `1.5 KiB`, `64 MiB`.
pub fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    let text = if unit == 0 || value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    };
    format!("{text} {}", UNITS[unit])
}

/// A duration in the largest unit that represents it exactly: `7d`, `12h`,
/// `15min`, `90s`, `1500ms`.
pub fn duration(d: Duration) -> String {
    let ms = d.as_millis();
    for (unit, size) in [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("min", 60_000),
        ("s", 1000),
    ] {
        if ms >= size && ms.is_multiple_of(size) {
            return format!("{}{unit}", ms / size);
        }
    }
    format!("{ms}ms")
}

/// A timestamp in milliseconds since the epoch, formatted as ISO 8601 UTC.
pub fn timestamp(millis: i64) -> String {
    let seconds = millis.div_euclid(1000);
    let ms = millis.rem_euclid(1000);
    let (days, secs_of_day) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        secs_of_day / 3600,
        secs_of_day % 3600 / 60,
        secs_of_day % 60
    )
}

/// Days since 1970-01-01 → (year, month, day), Gregorian calendar.
/// Howard Hinnant's algorithm ("chrono-Compatible Low-Level Date Algorithms").
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_is_quoted_and_binary_is_hex() {
        assert_eq!(bytes(b"order", false), "\"order\"");
        assert_eq!(bytes(b"line\nnext", false), "\"line\\nnext\"");
        assert_eq!(bytes(&[0xff, 0x00, 0x10], false), "0xff0010");
        assert_eq!(optional_bytes(None, false), "null");
    }

    #[test]
    fn long_values_are_cut_unless_full() {
        let long = "x".repeat(500);
        let short = bytes(long.as_bytes(), false);
        assert!(short.ends_with("… (500 B)"), "{short}");
        assert_eq!(bytes(long.as_bytes(), true).len(), 502);
    }

    #[test]
    fn sizes_and_durations_read_naturally() {
        assert_eq!(size(512), "512 B");
        assert_eq!(size(1536), "1.5 KiB");
        assert_eq!(size(64 << 20), "64 MiB");
        assert_eq!(duration(Duration::from_secs(7 * 86_400)), "7d");
        assert_eq!(duration(Duration::from_millis(1500)), "1500ms");
        assert_eq!(duration(Duration::from_secs(90)), "90s");
    }

    #[test]
    fn timestamps_are_iso_8601_utc() {
        assert_eq!(timestamp(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(timestamp(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
        assert_eq!(timestamp(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(timestamp(-1), "1969-12-31T23:59:59.999Z");
    }
}
