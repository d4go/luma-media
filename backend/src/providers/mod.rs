mod api;
mod common;
mod jav321;
mod javbus;
mod javdb;
mod javlibrary;
mod model;
mod registry;
pub mod runtime;

pub use api::router;
pub use common::ProviderAdapter;
pub use model::{
    DiscoverPage, DiscoverRequest, ProviderContext, ProviderError, ProviderMediaCandidate,
    ProviderMediaRef, RawProviderDocument, ResourceCandidate, SourceMedia, SourceProviderConfig,
};
pub use registry::ProviderRegistry;

use async_trait::async_trait;

#[async_trait]
pub trait MetadataProvider: Send + Sync {
    fn key(&self) -> &str;

    async fn discover(
        &self,
        context: &ProviderContext,
        request: DiscoverRequest,
    ) -> Result<DiscoverPage, ProviderError>;

    async fn search_by_code(
        &self,
        context: &ProviderContext,
        code: &str,
    ) -> Result<Vec<ProviderMediaCandidate>, ProviderError>;

    async fn fetch_detail(
        &self,
        context: &ProviderContext,
        item: &ProviderMediaRef,
    ) -> Result<RawProviderDocument, ProviderError>;
}

#[async_trait]
pub trait ResourceProvider: Send + Sync {
    fn key(&self) -> &str;

    async fn fetch_resources(
        &self,
        context: &ProviderContext,
        media: &ProviderMediaRef,
    ) -> Result<Vec<ResourceCandidate>, ProviderError>;
}
