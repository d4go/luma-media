use std::time::Duration;

use serde_json::json;

use crate::{
    fetch::{FetchMethod, FetchMode, FetchRequest, FetchResponse, PageKind, classify_transport},
    ingestion::SnapshotInput,
};

use super::{
    ProviderContext, ProviderError, ProviderMediaRef, RawProviderDocument, ResourceCandidate,
    ResourceProvider,
    common::{ProviderAdapter, contains_selector},
    resource_common::{extract_js_value, parse_magnet_candidates},
};

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

#[async_trait::async_trait]
impl ResourceProvider for JavBusAdapter {
    fn key(&self) -> &'static str {
        "javbus"
    }

    async fn fetch_resources(
        &self,
        context: &ProviderContext,
        media: &ProviderMediaRef,
    ) -> Result<Vec<ResourceCandidate>, ProviderError> {
        let document = if let Some(document) = &context.raw_document {
            document.clone()
        } else {
            fetch_detail_document(context, media).await?
        };
        let embedded = parse_magnet_candidates(&document.body, "JavBus 资源", &document.source_url);
        if !embedded.is_empty() {
            return Ok(embedded);
        }
        if !context.allow_resource_endpoint {
            return Ok(Vec::new());
        }
        let Some(gid) = extract_js_value(&document.body, "gid") else {
            return Ok(Vec::new());
        };
        let img = extract_js_value(&document.body, "img").unwrap_or_default();
        let uc = extract_js_value(&document.body, "uc").unwrap_or_else(|| "0".into());
        let mut ajax = reqwest::Url::parse(context.provider.base_url.trim_end_matches('/'))
            .map_err(|error| ProviderError::Fetch(error.to_string()))?;
        ajax.set_path("/ajax/uncledatoolsbyajax.php");
        ajax.query_pairs_mut()
            .append_pair("gid", &gid)
            .append_pair("lang", "zh")
            .append_pair("img", &img)
            .append_pair("uc", &uc)
            .append_pair("floor", "1");
        let mut transport = context.provider.transport();
        transport.cookie = Some(javbus_cookie(context, &[]));
        let mut request = FetchRequest::get(&context.provider.key, ajax);
        request.referer = reqwest::Url::parse(&document.source_url).ok();
        let response = context
            .state
            .fetch_manager
            .fetch(context.provider.fetch_mode, request, &transport)
            .await
            .map_err(|error| ProviderError::Fetch(error.to_string()))?;
        let resources =
            parse_magnet_candidates(&response.body, "JavBus 资源", response.final_url.as_str());
        context
            .state
            .snapshot_repository
            .store(SnapshotInput {
                provider_key: &context.provider.key,
                entity_type: "resource",
                provider_entity_id: Some(&media.provider_id),
                media_id: Some(context.media_id),
                source_url: response.final_url.as_str(),
                response: &response,
                raw_json: json!({
                    "resourceState": if resources.is_empty() { "resource_empty" } else { "resource_found" },
                    "resourceCount": resources.len(),
                }),
                parser_version: "2",
            })
            .await
            .map_err(|error| ProviderError::Fetch(error.to_string()))?;
        Ok(resources)
    }
}

async fn fetch_detail_document(
    context: &ProviderContext,
    media: &ProviderMediaRef,
) -> Result<RawProviderDocument, ProviderError> {
    let base = reqwest::Url::parse(context.provider.base_url.trim_end_matches('/'))
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    let url = reqwest::Url::parse(&media.source_url)
        .or_else(|_| base.join(&media.source_url))
        .or_else(|_| base.join(&media.provider_id))
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    let mut transport = context.provider.transport();
    transport.cookie = Some(javbus_cookie(context, &[]));
    let mut response = context
        .state
        .fetch_manager
        .fetch(
            context.provider.fetch_mode,
            FetchRequest::get(&context.provider.key, url.clone()),
            &transport,
        )
        .await
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    if context
        .state
        .provider_registry
        .classify("javbus", &response)
        == PageKind::AgeGate
    {
        let mut verify_url = base.clone();
        verify_url.set_path("/doc/driver-verify");
        verify_url
            .query_pairs_mut()
            .append_pair("referer", url.path());
        let verification = context
            .state
            .fetch_manager
            .fetch(
                context.provider.fetch_mode,
                FetchRequest {
                    provider_key: context.provider.key.clone(),
                    url: verify_url,
                    method: FetchMethod::Post,
                    headers: reqwest::header::HeaderMap::new(),
                    referer: Some(url.clone()),
                    body: Some("Submit=confirm".into()),
                    timeout: Duration::from_secs(20),
                },
                &transport,
            )
            .await
            .map_err(|error| ProviderError::Fetch(error.to_string()))?;
        let session = verification
            .headers
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .filter_map(|value| value.split(';').next())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        transport.cookie = Some(javbus_cookie(context, &session));
        response = context
            .state
            .fetch_manager
            .fetch(
                context.provider.fetch_mode,
                FetchRequest::get(&context.provider.key, url),
                &transport,
            )
            .await
            .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    }
    let kind = context
        .state
        .provider_registry
        .classify("javbus", &response);
    if kind != PageKind::ValidContent {
        return Err(ProviderError::InvalidContent(format!("{kind:?}")));
    }
    context
        .state
        .snapshot_repository
        .store(SnapshotInput {
            provider_key: &context.provider.key,
            entity_type: "detail",
            provider_entity_id: Some(&media.provider_id),
            media_id: Some(context.media_id),
            source_url: response.final_url.as_str(),
            response: &response,
            raw_json: json!({ "pageKind": "valid_content" }),
            parser_version: "2",
        })
        .await
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    Ok(RawProviderDocument {
        source_url: response.final_url.to_string(),
        body: response.body,
    })
}

fn javbus_cookie(context: &ProviderContext, session: &[String]) -> String {
    let mut parts = vec!["age=verified".to_owned(), "existmag=all".to_owned()];
    if !context.provider.secret.trim().is_empty() {
        parts.push(context.provider.secret.trim().to_owned());
    }
    parts.extend(session.iter().cloned());
    parts.join("; ")
}
