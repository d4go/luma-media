use std::sync::Arc;

use super::{
    browser::BrowserManager,
    http::HttpFetcher,
    model::{FetchError, FetchMode, FetchRequest, FetchResponse, FetchTransport},
};

#[derive(Debug, Clone)]
pub struct FetchManager {
    http: Arc<HttpFetcher>,
    browser: Arc<BrowserManager>,
}

impl Default for FetchManager {
    fn default() -> Self {
        Self {
            http: Arc::new(HttpFetcher),
            browser: Arc::new(BrowserManager::from_env()),
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
            FetchMode::Browser => self.browser.fetch(request, transport).await,
        }
    }

    pub async fn fetch_http(
        &self,
        request: FetchRequest,
        transport: &FetchTransport,
    ) -> Result<FetchResponse, FetchError> {
        self.http.fetch(request, transport).await
    }

    pub fn browser(&self) -> &BrowserManager {
        &self.browser
    }
}
