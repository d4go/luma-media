use crate::fetch::{FetchMode, FetchResponse, PageKind, classify_transport};

use super::{
    ProviderContext, ProviderError, ProviderMediaRef, ResourceCandidate, ResourceProvider,
    common::{ProviderAdapter, contains_selector},
    resource_common::{detail_document, parse_magnet_candidates},
};

pub struct JavLibraryAdapter;

impl ProviderAdapter for JavLibraryAdapter {
    fn key(&self) -> &'static str {
        "javlibrary"
    }
    fn label(&self) -> &'static str {
        "JavLibrary"
    }
    fn default_fetch_mode(&self) -> FetchMode {
        FetchMode::Http
    }

    fn classify(&self, response: &FetchResponse) -> PageKind {
        if let Some(kind) = classify_transport(response) {
            return kind;
        }
        let lower = response.body.to_ascii_lowercase();
        if contains_selector(
            &response.body,
            &["#video_title", ".video", "a[href*='v=jav']"],
        ) {
            PageKind::ValidContent
        } else if lower.contains("challenge-platform") || lower.contains("just a moment") {
            PageKind::InteractionRequired
        } else if lower.contains("javlibrary") {
            PageKind::ValidContent
        } else {
            PageKind::InvalidContent
        }
    }
}

#[async_trait::async_trait]
impl ResourceProvider for JavLibraryAdapter {
    fn key(&self) -> &'static str {
        "javlibrary"
    }

    async fn fetch_resources(
        &self,
        context: &ProviderContext,
        media: &ProviderMediaRef,
    ) -> Result<Vec<ResourceCandidate>, ProviderError> {
        let document = detail_document(context, media).await?;
        Ok(parse_magnet_candidates(
            &document.body,
            "JavLibrary 资源",
            &document.source_url,
        ))
    }
}
