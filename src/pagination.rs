use reqwest::header::{HeaderMap, LINK};
use serde::Deserialize;
use url::Url;

/// A single Webex collection page.
#[derive(Debug, Clone)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<Url>,
}

impl<T> Page<T> {
    pub fn has_next(&self) -> bool {
        self.next.is_some()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Collection<T> {
    pub items: Vec<T>,
}

pub(crate) fn next_link(headers: &HeaderMap) -> Option<Url> {
    let link = headers.get(LINK)?.to_str().ok()?;
    parse_next_link(link)
}

fn parse_next_link(link: &str) -> Option<Url> {
    for part in link.split(',') {
        let part = part.trim();
        let (url_part, params_part) = part.split_once(';')?;
        let url = url_part.trim().strip_prefix('<')?.strip_suffix('>')?.trim();
        if params_part
            .split(';')
            .any(|param| param.trim().eq_ignore_ascii_case("rel=\"next\""))
        {
            return Url::parse(url).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::parse_next_link;

    #[test]
    fn parses_next_link() {
        let link = r#"<https://webexapis.com/v1/messages?roomId=a&max=10&before=b>; rel="next", <https://webexapis.com/v1/messages?roomId=a>; rel="first""#;
        let next = parse_next_link(link).unwrap();
        assert_eq!(next.query().unwrap(), "roomId=a&max=10&before=b");
    }

    #[test]
    fn ignores_missing_next() {
        let link = r#"<https://webexapis.com/v1/messages?roomId=a>; rel="first""#;
        assert!(parse_next_link(link).is_none());
    }
}
