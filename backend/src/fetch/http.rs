use std::time::Instant;

use chrono::Utc;
use reqwest::header::{CONTENT_TYPE, COOKIE, REFERER, USER_AGENT};

use super::model::{
    FetchError, FetchFailureKind, FetchMethod, FetchMode, FetchRequest, FetchResponse,
    FetchTransport,
};

const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/127 Safari/537.36 Luma/1.0";

#[derive(Debug, Default)]
pub struct HttpFetcher;

impl HttpFetcher {
    pub async fn fetch(
        &self,
        request: FetchRequest,
        transport: &FetchTransport,
    ) -> Result<FetchResponse, FetchError> {
        let mut builder = reqwest::Client::builder()
            .timeout(request.timeout)
            .redirect(reqwest::redirect::Policy::limited(10));
        if let Some(proxy_url) = transport.proxy_url.as_deref() {
            let proxy = reqwest::Proxy::all(proxy_url).map_err(|error| {
                FetchError::new(
                    &request.provider_key,
                    FetchFailureKind::Network,
                    format!("invalid proxy URL: {error}"),
                    None,
                )
            })?;
            builder = builder.proxy(proxy);
        }
        let client = builder.build().map_err(|error| {
            FetchError::new(
                &request.provider_key,
                FetchFailureKind::Network,
                error.to_string(),
                None,
            )
        })?;
        let mut outbound = match request.method {
            FetchMethod::Get => client.get(request.url),
            FetchMethod::Post => client.post(request.url),
        }
        .headers(request.headers)
        .header(
            USER_AGENT,
            transport
                .user_agent
                .as_deref()
                .unwrap_or(DEFAULT_USER_AGENT),
        );
        if let Some(cookie) = transport.cookie.as_deref() {
            outbound = outbound.header(COOKIE, cookie);
        }
        if let Some(referer) = request.referer.as_ref() {
            outbound = outbound.header(REFERER, referer.as_str());
        }
        if let Some(body) = request.body {
            outbound = outbound
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(body);
        }

        let started = Instant::now();
        let response = outbound.send().await.map_err(|error| {
            let kind = if error.is_timeout() {
                FetchFailureKind::Timeout
            } else {
                FetchFailureKind::Network
            };
            FetchError::new(&request.provider_key, kind, error.to_string(), None)
        })?;
        let status = response.status();
        let kind = match status.as_u16() {
            401 | 403 => Some(FetchFailureKind::AccessDenied),
            429 => Some(FetchFailureKind::RateLimited),
            500..=599 => Some(FetchFailureKind::TemporaryUnavailable),
            200..=399 => None,
            _ => Some(FetchFailureKind::InvalidContent),
        };
        if let Some(kind) = kind {
            return Err(FetchError::new(
                &request.provider_key,
                kind,
                format!("HTTP {status}"),
                Some(status.as_u16()),
            ));
        }
        let final_url = response.url().clone();
        let headers = response.headers().clone();
        let content_type = headers
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response.text().await.map_err(|error| {
            FetchError::new(
                &request.provider_key,
                FetchFailureKind::InvalidContent,
                error.to_string(),
                Some(status.as_u16()),
            )
        })?;
        Ok(FetchResponse {
            final_url,
            status: Some(status.as_u16()),
            content_type,
            headers,
            body,
            fetched_at: Utc::now(),
            fetch_mode: FetchMode::Http,
            elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }
}
