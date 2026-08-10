use serde::{Deserialize, Serialize};

use super::FetchResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageKind {
    ValidContent,
    AgeGate,
    LoginRequired,
    InteractionRequired,
    AccessDenied,
    RateLimited,
    TemporaryUnavailable,
    InvalidContent,
}

pub fn classify_transport(response: &FetchResponse) -> Option<PageKind> {
    match response.status {
        Some(401) => Some(PageKind::LoginRequired),
        Some(403) => Some(PageKind::AccessDenied),
        Some(429) => Some(PageKind::RateLimited),
        Some(500..=599) => Some(PageKind::TemporaryUnavailable),
        Some(200..=399) => None,
        Some(_) => Some(PageKind::InvalidContent),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use reqwest::{Url, header::HeaderMap};

    use super::*;
    use crate::fetch::FetchMode;

    fn response(status: u16) -> FetchResponse {
        FetchResponse {
            final_url: Url::parse("https://example.invalid/").unwrap(),
            status: Some(status),
            content_type: Some("text/html".into()),
            headers: HeaderMap::new(),
            body: String::new(),
            fetched_at: Utc::now(),
            fetch_mode: FetchMode::Http,
            elapsed_ms: 1,
        }
    }

    #[test]
    fn classifies_transport_failures_before_page_content() {
        assert_eq!(
            classify_transport(&response(403)),
            Some(PageKind::AccessDenied)
        );
        assert_eq!(
            classify_transport(&response(429)),
            Some(PageKind::RateLimited)
        );
        assert_eq!(
            classify_transport(&response(503)),
            Some(PageKind::TemporaryUnavailable)
        );
        assert_eq!(classify_transport(&response(200)), None);
    }
}
