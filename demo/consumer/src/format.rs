//! Mise en forme d'un message de la démo pour la console. Logique pure.

use serde_json::Value;

/// `p1:42     order_paid       client-17     129,90 €`, ou le contenu brut si
/// le message n'est pas un événement de la démo.
pub fn record_line(partition: u32, offset: u64, value: Option<&[u8]>) -> String {
    let prefix = format!("p{partition}:{offset:<6}");
    let Some(bytes) = value else {
        return format!("{prefix} (valeur nulle)");
    };
    match serde_json::from_slice::<Value>(bytes) {
        Ok(event) if event.get("type").is_some() => format!(
            "{prefix} {:<16} {:<10} {:>11}",
            event["type"].as_str().unwrap_or("?"),
            event["customer"].as_str().unwrap_or("?"),
            event["amount_cents"].as_u64().map_or("?".into(), euros),
        ),
        _ => format!("{prefix} {}", String::from_utf8_lossy(bytes)),
    }
}

fn euros(cents: u64) -> String {
    format!("{},{:02} €", cents / 100, cents % 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_events_are_shown_as_columns() {
        let json = br#"{"id":3,"type":"order_paid","customer":"client-17","amount_cents":12990,"emitted_at":0}"#;
        let line = record_line(1, 42, Some(json));
        assert!(line.starts_with("p1:42 "), "{line}");
        assert!(line.contains("order_paid"), "{line}");
        assert!(line.contains("client-17"), "{line}");
        assert!(line.ends_with("129,90 €"), "{line}");
    }

    #[test]
    fn other_messages_are_shown_as_they_are() {
        assert_eq!(record_line(0, 7, Some(b"bonjour")), "p0:7      bonjour");
        assert_eq!(record_line(2, 1, None), "p2:1      (valeur nulle)");
    }
}
