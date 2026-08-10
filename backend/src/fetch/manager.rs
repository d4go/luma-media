use std::sync::Arc;

use super::{
    http::HttpFetcher,
    model::{FetchError, FetchFailureKind, FetchMode, FetchRequest, FetchResponse, FetchTransport},
};

#[derive(Debug, Clone)]
pub struct FetchManager {
    http: Arc<HttpFetcher>,
}

impl Default for FetchManager {
    fn default() -> Self {
        Self {
            http: Arc::new(HttpFetcher),
        }
    }
}

impl FetchManager {
    pub async fn fetch(
        &self,
        mode: FetchMode,
        request: FetchRequest,
        transport: &FetchTransport,
    ) -> Result<FetchResponse, FetchError> {
        match mode {
            FetchMode::Http | FetchMode::Auto => self.http.fetch(request, transport).await,
            FetchMode::Browser => Err(FetchError::new(
                request.provider_key,
                FetchFailureKind::BrowserUnavailable,
                "Chromium fetcher is not enabled in this build phase",
                None,
            )),
        }
    }

    pub async fn fetch_http(
        &self,
        request: FetchRequest,
        transport: &FetchTransport,
    ) -> Result<FetchResponse, FetchError> {
        self.http.fetch(request, transport).await
    }
}
