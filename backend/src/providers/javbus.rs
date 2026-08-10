use crate::fetch::{FetchMode, FetchResponse, PageKind, classify_transport};

use super::common::{ProviderAdapter, contains_selector};

pub struct JavBusAdapter;

impl ProviderAdapter for JavBusAdapter {
    fn key(&self) -> &'static str {
        "javbus"
    }
    fn label(&self) -> &'static str {
        "JavBus"
    }
    fn default_fetch_mode(&self) -> FetchMode {
        FetchMode::Http
    }

    fn classify(&self, response: &FetchResponse) -> PageKind {
        if let Some(kind) = classify_transport(response) {
            return kind;
        }
        let lower = response.body.to_ascii_lowercase();
        if lower.contains("driver-verify")
            && (lower.contains("age verification") || response.body.contains("你是否已經成年"))
        {
            return PageKind::AgeGate;
        }
        if contains_selector(
            &response.body,
            &[
                "a.movie-box",
                ".movie-box",
                "meta[property='og:title']",
                "a[href*='/ajax/uncledatoolsbyajax.php']",
            ],
        ) {
            PageKind::ValidContent
        } else if lower.contains("challenge-platform") || lower.contains("just a moment") {
            PageKind::InteractionRequired
        } else if lower.contains("javbus") {
            PageKind::ValidContent
        } else {
            PageKind::InvalidContent
        }
    }
}
