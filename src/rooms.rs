use base64::{Engine as _, engine::general_purpose::STANDARD};
use url::Url;

/// Returns room id candidates parsed from a Webex room or space link.
///
/// Webex app links such as `webexteams://im?space=<uuid>` expose the room UUID,
/// while the public REST API expects the base64-encoded room URI form.
pub fn room_id_candidates_from_link(link: &str) -> Vec<String> {
    let Ok(url) = Url::parse(link.trim()) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    for segment in url.path_segments().into_iter().flatten() {
        push_room_candidate(&mut candidates, segment);
    }
    for (key, value) in url.query_pairs() {
        if key_contains_room_hint(&key) {
            push_room_candidate(&mut candidates, &value);
        }
    }
    if let Some(fragment) = url.fragment() {
        for (key, value) in url::form_urlencoded::parse(fragment.as_bytes()) {
            if key_contains_room_hint(&key) {
                push_room_candidate(&mut candidates, &value);
            }
        }
    }
    candidates
}

fn key_contains_room_hint(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key == "id" || key.contains("room") || key.contains("space") || key.contains("conversation")
}

fn push_room_candidate(candidates: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if let Some(rest_room_id) = rest_room_id_from_space_uuid(value) {
        push_unique(candidates, rest_room_id);
    }
    if value.len() >= 16
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '/' | '='))
    {
        push_unique(candidates, value.to_owned());
    }
}

fn rest_room_id_from_space_uuid(value: &str) -> Option<String> {
    let value = value.trim();
    if !is_hyphenated_uuid_like(value) {
        return None;
    }
    Some(STANDARD.encode(format!(
        "ciscospark://us/ROOM/{}",
        value.to_ascii_lowercase()
    )))
}

fn is_hyphenated_uuid_like(value: &str) -> bool {
    if value.len() != 36 {
        return false;
    }
    value.chars().enumerate().all(|(index, ch)| {
        matches!(index, 8 | 13 | 18 | 23) && ch == '-'
            || !matches!(index, 8 | 13 | 18 | 23) && ch.is_ascii_hexdigit()
    })
}

fn push_unique(candidates: &mut Vec<String>, value: String) {
    if !candidates.contains(&value) {
        candidates.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_room_link_candidates_from_query_and_fragment() {
        let candidates = room_id_candidates_from_link(
            "https://web.webex.com/rooms?id=Y2lzY29zcGFyazovL3VzL1JPT00vabc#spaceId=room-1234567890123456",
        );

        assert!(candidates.contains(&"Y2lzY29zcGFyazovL3VzL1JPT00vabc".to_owned()));
        assert!(candidates.contains(&"room-1234567890123456".to_owned()));
    }

    #[test]
    fn derives_rest_room_id_from_webexteams_space_uuid() {
        let candidates = room_id_candidates_from_link(
            "webexteams://im?space=ee5fc3f0-6036-11f1-8223-f90cc6ad4e70",
        );

        assert_eq!(
            candidates.first().map(String::as_str),
            Some("Y2lzY29zcGFyazovL3VzL1JPT00vZWU1ZmMzZjAtNjAzNi0xMWYxLTgyMjMtZjkwY2M2YWQ0ZTcw")
        );
        assert!(candidates.contains(&"ee5fc3f0-6036-11f1-8223-f90cc6ad4e70".to_owned()));
    }

    #[test]
    fn normalizes_uppercase_space_uuid_before_deriving_rest_room_id() {
        let candidates = room_id_candidates_from_link(
            "webexteams://im?space=EE5FC3F0-6036-11F1-8223-F90CC6AD4E70",
        );

        assert_eq!(
            candidates.first().map(String::as_str),
            Some("Y2lzY29zcGFyazovL3VzL1JPT00vZWU1ZmMzZjAtNjAzNi0xMWYxLTgyMjMtZjkwY2M2YWQ0ZTcw")
        );
    }
}
