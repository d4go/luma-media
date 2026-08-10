use crate::fetch::{FetchMode, FetchResponse, PageKind, classify_transport};

use super::{
    ProviderContext, ProviderError, ProviderMediaRef, ResourceCandidate, ResourceProvider,
    common::{ProviderAdapter, contains_selector},
    resource_common::{detail_document, parse_magnet_candidates},
};

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

#[async_trait::async_trait]
impl ResourceProvider for Jav321Adapter {
    fn key(&self) -> &'static str {
        "jav321"
    }

    async fn fetch_resources(
        &self,
        context: &ProviderContext,
        media: &ProviderMediaRef,
    ) -> Result<Vec<ResourceCandidate>, ProviderError> {
        let document = detail_document(context, media).await?;
        Ok(parse_magnet_candidates(
            &document.body,
            "Jav321 资源",
            &document.source_url,
        ))
    }
}
