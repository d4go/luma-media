use crate::fetch::{FetchMode, FetchResponse, PageKind, classify_transport};

use super::common::{ProviderAdapter, contains_selector};

pub struct JavDbAdapter;

impl ProviderAdapter for JavDbAdapter {
    fn key(&self) -> &'static str {
        "javdb"
    }
    fn label(&self) -> &'static str {
        "JavDB"
    }
    fn default_fetch_mode(&self) -> FetchMode {
        FetchMode::Browser
    }

    fn classify(&self, response: &FetchResponse) -> PageKind {
        if let Some(kind) = classify_transport(response) {
            return kind;
        }
        let lower = response.body.to_ascii_lowercase();
        if lower.contains("challenge-platform") || lower.contains("just a moment") {
            return PageKind::InteractionRequired;
        }
        if lower.contains("age verification") || lower.contains("年齡確認") {
            return PageKind::AgeGate;
        }
        if contains_selector(
            &response.body,
            &["a[href*='/v/']", ".video-title", ".movie-panel-info"],
        ) {
            PageKind::ValidContent
        } else {
            PageKind::InvalidContent
        }
    }
}
