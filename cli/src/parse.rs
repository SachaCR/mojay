//! Lecture des arguments à unités : tailles, durées, politique fsync, headers.
//! Logique pure : chaque fonction renvoie un message d'erreur lisible.

use std::time::Duration;

use mojay::FsyncPolicy;

/// Une taille en octets : `4096`, `512o`, `64Kio`, `64MiB`, `1G`… en base 1024.
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
                "unité de taille inconnue « {unit} » (o, Kio, Mio, Gio, Tio)"
            ));
        }
    };
    number
        .checked_mul(factor)
        .ok_or_else(|| format!("taille trop grande : {text}"))
}

/// Une durée : `250ms`, `30s`, `15m`, `12h`, `7d` (ou `7j`).
pub fn duration(text: &str) -> Result<Duration, String> {
    let (number, unit) = split_number(text)?;
    let seconds = match unit.to_ascii_lowercase().as_str() {
        "ms" => return Ok(Duration::from_millis(number)),
        "s" => 1,
        "m" | "min" => 60,
        "h" => 3600,
        "d" | "j" => 86_400,
        "" => return Err(format!("unité manquante dans « {text} » (ms, s, m, h, d)")),
        _ => {
            return Err(format!(
                "unité de durée inconnue « {unit} » (ms, s, m, h, d)"
            ));
        }
    };
    number
        .checked_mul(seconds)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("durée trop grande : {text}"))
}

/// La politique fsync : `always`, `never`, ou un intervalle comme `100ms`.
pub fn fsync(text: &str) -> Result<FsyncPolicy, String> {
    match text {
        "always" => Ok(FsyncPolicy::Always),
        "never" => Ok(FsyncPolicy::Never),
        interval => duration(interval).map(FsyncPolicy::Interval).map_err(|_| {
            format!("« {text} » : attendu always, never, ou un intervalle comme 100ms")
        }),
    }
}

/// Un header `clé=valeur`.
pub fn header(text: &str) -> Result<(String, String), String> {
    match text.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => Err(format!("« {text} » : attendu clé=valeur")),
    }
}

fn split_number(text: &str) -> Result<(u64, &str), String> {
    let text = text.trim();
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return Err(format!("« {text} » : un nombre est attendu"));
    }
    let number = text[..digits]
        .parse()
        .map_err(|_| format!("nombre trop grand : {text}"))?;
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
        assert!(size("12 pommes").is_err());
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
            "sans unité, l'ambiguïté est refusée"
        );
        assert!(duration("3 semaines").is_err());
    }

    #[test]
    fn fsync_policies_and_headers() {
        assert_eq!(fsync("always"), Ok(FsyncPolicy::Always));
        assert_eq!(fsync("never"), Ok(FsyncPolicy::Never));
        assert_eq!(
            fsync("100ms"),
            Ok(FsyncPolicy::Interval(Duration::from_millis(100)))
        );
        assert!(fsync("parfois").is_err());
        assert_eq!(header("trace=a=b"), Ok(("trace".into(), "a=b".into())));
        assert!(header("=x").is_err());
        assert!(header("sans-egal").is_err());
    }
}
