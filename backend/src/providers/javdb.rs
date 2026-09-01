use crate::fetch::{FetchMode, FetchResponse, PageKind, classify_transport};

use super::{
    ProviderContext, ProviderError, ProviderMediaRef, ResourceCandidate, ResourceProvider,
    common::{ProviderAdapter, contains_selector},
    resource_common::{detail_document, parse_magnet_candidates},
};

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
        if lower.contains("age verification") || lower.contains("年齡確認") {
            return PageKind::AgeGate;
        }
        if contains_selector(
            &response.body,
            &["a[href*='/v/']", ".video-title", ".movie-panel-info"],
        ) {
            PageKind::ValidContent
        } else if lower.contains("challenge-platform") || lower.contains("just a moment") {
            PageKind::InteractionRequired
        } else {
            PageKind::InvalidContent
        }
    }
}

#[async_trait::async_trait]
impl ResourceProvider for JavDbAdapter {
    fn key(&self) -> &'static str {
        "javdb"
    }

    async fn fetch_resources(
        &self,
        context: &ProviderContext,
        media: &ProviderMediaRef,
    ) -> Result<Vec<ResourceCandidate>, ProviderError> {
        let document = detail_document(context, media).await?;
        Ok(parse_magnet_candidates(
            &document.body,
            "JavDB 资源",
            &document.source_url,
        ))
    }
}
