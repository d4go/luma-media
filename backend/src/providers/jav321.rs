use crate::fetch::{FetchMode, FetchResponse, PageKind, classify_transport};

use super::common::{ProviderAdapter, contains_selector};

pub struct Jav321Adapter;

impl ProviderAdapter for Jav321Adapter {
    fn key(&self) -> &'static str {
        "jav321"
    }
    fn label(&self) -> &'static str {
        "Jav321"
    }
    fn default_fetch_mode(&self) -> FetchMode {
        FetchMode::Http
    }

    fn classify(&self, response: &FetchResponse) -> PageKind {
        if let Some(kind) = classify_transport(response) {
            return kind;
        }
        if contains_selector(
            &response.body,
            &["a[href*='/video/']", "video", ".panel-heading"],
        ) || response.body.to_ascii_lowercase().contains("jav321")
        {
            PageKind::ValidContent
        } else {
            PageKind::InvalidContent
        }
    }
}
