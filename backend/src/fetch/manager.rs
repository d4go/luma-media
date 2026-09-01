use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use super::{
    browser::BrowserManager,
    classifier::looks_like_challenge,
    http::HttpFetcher,
    model::{
        AutoFetchPolicy, FetchDecision, FetchError, FetchMode, FetchRequest, FetchResponse,
        FetchTransport,
    },
};

#[derive(Debug, Clone)]
pub struct FetchManager {
    http: Arc<HttpFetcher>,
    browser: Arc<BrowserManager>,
    policy: AutoFetchPolicy,
    preferred: Arc<Mutex<HashMap<String, PreferredTransport>>>,
}

#[derive(Debug, Clone, Copy)]
struct PreferredTransport {
    mode: FetchMode,
    until: Instant,
}

impl Default for FetchManager {
    fn default() -> Self {
        Self {
            http: Arc::new(HttpFetcher),
            browser: Arc::new(BrowserManager::from_env()),
            policy: AutoFetchPolicy::default(),
            preferred: Arc::new(Mutex::new(HashMap::new())),
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
            FetchMode::Http => self.http.fetch(request, transport).await,
            FetchMode::Browser => self.browser.fetch(request, transport).await,
            FetchMode::Auto => self.fetch_auto(request, transport).await,
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

    /// The transport a provider is currently sticky to (only Browser is recorded).
    pub fn preferred_transport_for(&self, provider_key: &str) -> Option<FetchMode> {
        let mut map = self
            .preferred
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match map.get(provider_key) {
            Some(preferred) if preferred.until > Instant::now() => Some(preferred.mode),
            _ => {
                map.remove(provider_key);
                None
            }
        }
    }

    async fn fetch_auto(
        &self,
        request: FetchRequest,
        transport: &FetchTransport,
    ) -> Result<FetchResponse, FetchError> {
        let provider_key = request.provider_key.clone();
        // Transport sticky: once a provider has proven "HTTP blocked + Browser works",
        // skip the doomed HTTP attempt for a short TTL and go straight to Browser.
        if self.preferred_transport_for(&provider_key) == Some(FetchMode::Browser) {
            return self.browser.fetch(request, transport).await;
        }

        let http_result = self.http.fetch(request.clone(), transport).await;
        let decision = self
            .policy
            .decide(http_result.as_ref(), looks_like_challenge);
        match decision {
            FetchDecision::Return => http_result,
            FetchDecision::FallbackToBrowser => {
                match self.browser.fetch(request, transport).await {
                    Ok(response) => {
                        self.remember_browser_preference(&provider_key);
                        Ok(response)
                    }
                    Err(error) => Err(error),
                }
            }
            // Retry/Cooldown/InteractionRequired surface the original HTTP result;
            // the caller (runtime/circuit breaker/task engine) decides backoff.
            FetchDecision::Retry | FetchDecision::Cooldown | FetchDecision::InteractionRequired => {
                http_result
            }
        }
    }

    fn remember_browser_preference(&self, provider_key: &str) {
        let mut map = self
            .preferred
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        map.insert(
            provider_key.to_owned(),
            PreferredTransport {
                mode: FetchMode::Browser,
                until: Instant::now() + self.policy.sticky_ttl,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use reqwest::{Url, header::HeaderMap};

    use super::*;
    use crate::fetch::FetchFailureKind;

    fn response(body: &str) -> FetchResponse {
        FetchResponse {
            final_url: Url::parse("https://example.invalid/").unwrap(),
            status: Some(200),
            content_type: Some("text/html".into()),
            headers: HeaderMap::new(),
            body: body.to_owned(),
            fetched_at: Utc::now(),
            fetch_mode: FetchMode::Http,
            elapsed_ms: 1,
        }
    }

    fn error(kind: FetchFailureKind) -> FetchError {
        FetchError::new("test", kind, "boom", None)
    }

    #[test]
    fn auto_policy_returns_http_on_valid_content() {
        let policy = AutoFetchPolicy::default();
        let result = Ok(&response("<html><body><h1>movie</h1></body></html>"));
        assert_eq!(
            policy.decide(result, looks_like_challenge),
            FetchDecision::Return
        );
    }

    #[test]
    fn auto_policy_falls_back_to_browser_on_challenge_served_with_200() {
        let policy = AutoFetchPolicy::default();
        let result = Ok(&response(
            "<html><title>Just a moment...</title><div class=\"cf-challenge\"></div></html>",
        ));
        assert_eq!(
            policy.decide(result, looks_like_challenge),
            FetchDecision::FallbackToBrowser
        );
    }

    #[test]
    fn auto_policy_falls_back_to_browser_on_access_denied() {
        let policy = AutoFetchPolicy::default();
        assert_eq!(
            policy.decide(
                Err(&error(FetchFailureKind::AccessDenied)),
                looks_like_challenge
            ),
            FetchDecision::FallbackToBrowser
        );
        let result = Ok(&FetchResponse {
            status: Some(403),
            ..response("")
        });
        assert_eq!(
            policy.decide(result, looks_like_challenge),
            FetchDecision::FallbackToBrowser
        );
    }

    #[test]
    fn auto_policy_never_falls_back_on_rate_limit() {
        let policy = AutoFetchPolicy::default();
        assert_eq!(
            policy.decide(
                Err(&error(FetchFailureKind::RateLimited)),
                looks_like_challenge
            ),
            FetchDecision::Cooldown
        );
        let result = Ok(&FetchResponse {
            status: Some(429),
            ..response("")
        });
        assert_eq!(
            policy.decide(result, looks_like_challenge),
            FetchDecision::Cooldown
        );
    }

    #[test]
    fn auto_policy_retries_network_and_plain_5xx() {
        let policy = AutoFetchPolicy::default();
        assert_eq!(
            policy.decide(Err(&error(FetchFailureKind::Timeout)), looks_like_challenge),
            FetchDecision::Retry
        );
        assert_eq!(
            policy.decide(
                Err(&error(FetchFailureKind::TemporaryUnavailable)),
                looks_like_challenge
            ),
            FetchDecision::Retry
        );
        let result = Ok(&FetchResponse {
            status: Some(503),
            ..response("Service temporarily unavailable")
        });
        assert_eq!(
            policy.decide(result, looks_like_challenge),
            FetchDecision::Retry
        );
    }

    #[test]
    fn auto_policy_falls_back_on_5xx_challenge_page() {
        let policy = AutoFetchPolicy::default();
        let result = Ok(&FetchResponse {
            status: Some(503),
            ..response("<html><div class=\"cf-challenge\"></div></html>")
        });
        assert_eq!(
            policy.decide(result, looks_like_challenge),
            FetchDecision::FallbackToBrowser
        );
    }

    #[test]
    fn sticky_preference_respects_ttl() {
        let manager = FetchManager {
            policy: AutoFetchPolicy {
                sticky_ttl: Duration::from_secs(60),
            },
            ..FetchManager::default()
        };
        manager.remember_browser_preference("javdb");
        assert_eq!(
            manager.preferred_transport_for("javdb"),
            Some(FetchMode::Browser)
        );
        let manager = FetchManager {
            policy: AutoFetchPolicy {
                sticky_ttl: Duration::ZERO,
            },
            ..FetchManager::default()
        };
        manager.remember_browser_preference("javdb");
        assert_eq!(manager.preferred_transport_for("javdb"), None);
    }
}
