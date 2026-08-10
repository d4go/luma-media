use std::{collections::HashMap, sync::Arc};

use crate::fetch::{FetchMode, FetchResponse, PageKind};

use super::{
    ProviderAdapter, ResourceProvider, jav321::Jav321Adapter, javbus::JavBusAdapter,
    javdb::JavDbAdapter, javlibrary::JavLibraryAdapter,
};

#[derive(Clone)]
pub struct ProviderRegistry {
    adapters: Arc<HashMap<&'static str, Arc<dyn ProviderAdapter>>>,
    resource_providers: Arc<HashMap<&'static str, Arc<dyn ResourceProvider>>>,
}

impl std::fmt::Debug for ProviderRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRegistry")
            .field("keys", &self.adapters.keys().collect::<Vec<_>>())
            .field(
                "resource_keys",
                &self.resource_providers.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        let adapters: Vec<Arc<dyn ProviderAdapter>> = vec![
            Arc::new(JavDbAdapter),
            Arc::new(JavBusAdapter),
            Arc::new(Jav321Adapter),
            Arc::new(JavLibraryAdapter),
        ];
        let resource_providers: Vec<Arc<dyn ResourceProvider>> = vec![
            Arc::new(JavDbAdapter),
            Arc::new(JavBusAdapter),
            Arc::new(Jav321Adapter),
            Arc::new(JavLibraryAdapter),
        ];
        Self {
            adapters: Arc::new(
                adapters
                    .into_iter()
                    .map(|adapter| (adapter.key(), adapter))
                    .collect(),
            ),
            resource_providers: Arc::new(
                resource_providers
                    .into_iter()
                    .map(|provider| (provider.key(), provider))
                    .collect(),
            ),
        }
    }
}

impl ProviderRegistry {
    pub fn contains(&self, key: &str) -> bool {
        self.adapters.contains_key(key)
    }

    pub fn label(&self, key: &str) -> Option<&'static str> {
        self.adapters.get(key).map(|adapter| adapter.label())
    }

    pub fn default_fetch_mode(&self, key: &str) -> Option<FetchMode> {
        self.adapters
            .get(key)
            .map(|adapter| adapter.default_fetch_mode())
    }

    pub fn classify(&self, key: &str, response: &FetchResponse) -> PageKind {
        self.adapters
            .get(key)
            .map_or(PageKind::InvalidContent, |adapter| {
                adapter.classify(response)
            })
    }

    pub fn resource_provider(&self, key: &str) -> Option<Arc<dyn ResourceProvider>> {
        self.resource_providers.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use reqwest::{Url, header::HeaderMap};

    use super::*;

    fn response(body: &str) -> FetchResponse {
        FetchResponse {
            final_url: Url::parse("https://example.invalid/").unwrap(),
            status: Some(200),
            content_type: Some("text/html".into()),
            headers: HeaderMap::new(),
            body: body.into(),
            fetched_at: Utc::now(),
            fetch_mode: FetchMode::Http,
            elapsed_ms: 1,
        }
    }

    #[test]
    fn registry_has_all_builtin_adapters() {
        let registry = ProviderRegistry::default();
        for key in ["javdb", "javbus", "jav321", "javlibrary"] {
            assert!(registry.contains(key));
        }
        assert_eq!(registry.label("javdb"), Some("JavDB"));
        assert!(registry.resource_provider("javdb").is_some());
        assert!(registry.resource_provider("javbus").is_some());
        assert!(registry.resource_provider("javlibrary").is_some());
    }

    #[test]
    fn page_classifiers_require_positive_provider_evidence() {
        let registry = ProviderRegistry::default();
        assert_eq!(
            registry.classify("javdb", &response("<a href='/v/abc'>ABC-123</a>")),
            PageKind::ValidContent
        );
        assert_eq!(
            registry.classify("javdb", &response("<h1>Age Verification</h1>")),
            PageKind::AgeGate
        );
        assert_eq!(
            registry.classify("javdb", &response("<html>generic success page</html>")),
            PageKind::InvalidContent
        );
        assert_eq!(
            registry.classify(
                "javdb",
                &response(
                    "<script src='/challenge-platform/widget.js'></script><a href='/v/abc'>ABC-123</a>",
                ),
            ),
            PageKind::ValidContent
        );
    }
}
