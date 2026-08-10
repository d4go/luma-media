use serde_json::json;

use crate::{
    fetch::{FetchRequest, PageKind},
    ingestion::SnapshotInput,
    qbittorrent::magnet_hash,
};

use super::{
    ProviderContext, ProviderError, ProviderMediaRef, RawProviderDocument, ResourceCandidate,
};

pub async fn detail_document(
    context: &ProviderContext,
    media: &ProviderMediaRef,
) -> Result<RawProviderDocument, ProviderError> {
    if let Some(document) = &context.raw_document {
        return Ok(document.clone());
    }
    let base = reqwest::Url::parse(context.provider.base_url.trim_end_matches('/'))
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    let url = reqwest::Url::parse(&media.source_url)
        .or_else(|_| base.join(&media.source_url))
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    let response = context
        .state
        .fetch_manager
        .fetch(
            context.provider.fetch_mode,
            FetchRequest::get(&context.provider.key, url),
            &context.provider.transport(),
        )
        .await
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    let kind = context
        .state
        .provider_registry
        .classify(&context.provider.adapter, &response);
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
            parser_version: "1",
        })
        .await
        .map_err(|error| ProviderError::Fetch(error.to_string()))?;
    Ok(RawProviderDocument {
        source_url: response.final_url.to_string(),
        body: response.body,
    })
}

pub fn parse_magnet_candidates(
    html: &str,
    default_label: &str,
    source_url: &str,
) -> Vec<ResourceCandidate> {
    let mut raw = Vec::<(String, String)>::new();
    let document = scraper::Html::parse_document(html);
    if let Ok(selector) = scraper::Selector::parse(
        "a[href*='magnet:?'], [data-clipboard-text*='magnet:?'], [data-magnet*='magnet:?'], [onclick*='magnet:?']",
    ) {
        for element in document.select(&selector) {
            let value = element
                .value()
                .attr("href")
                .or_else(|| element.value().attr("data-clipboard-text"))
                .or_else(|| element.value().attr("data-magnet"))
                .or_else(|| element.value().attr("onclick"))
                .unwrap_or_default();
            let Some(url) = extract_magnet_value(value) else {
                continue;
            };
            let title = element.text().collect::<Vec<_>>().join(" ");
            raw.push((url, title));
        }
    }
    let mut cursor = 0;
    while let Some(offset) = html[cursor..].find("magnet:?") {
        let start = cursor + offset;
        let end = html[start..]
            .find(['\"', '\'', '<', ' '])
            .unwrap_or(html.len() - start);
        let label_start = char_boundary_before(html, start.saturating_sub(500));
        let nearby = scraper::Html::parse_fragment(&html[label_start..start])
            .root_element()
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .split_whitespace()
            .rev()
            .take(8)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join(" ");
        raw.push((html[start..start + end].to_owned(), nearby));
        cursor = start + end.max(1);
    }

    let mut resources = Vec::new();
    for (url, title) in raw {
        let url = html_unescape(url.trim());
        let Some(info_hash) = magnet_hash(&url) else {
            continue;
        };
        let title = clean_resource_label(&title, default_label);
        if let Some(existing) = resources
            .iter_mut()
            .find(|candidate: &&mut ResourceCandidate| {
                candidate.info_hash.as_deref() == Some(&info_hash)
            })
        {
            if resource_label_score(&title, default_label)
                > resource_label_score(&existing.title, default_label)
            {
                let size_bytes = parse_size_bytes(&title);
                existing.title = title;
                if size_bytes.is_some() {
                    existing.size_bytes = size_bytes;
                }
            }
            continue;
        }
        let trackers = reqwest::Url::parse(&url)
            .ok()
            .map(|url| {
                url.query_pairs()
                    .filter(|(key, _)| key == "tr")
                    .map(|(_, value)| value.into_owned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        resources.push(ResourceCandidate {
            provider_resource_id: Some(format!("btih:{info_hash}")),
            download_url: url,
            size_bytes: parse_size_bytes(&title),
            title,
            info_hash: Some(info_hash),
            trackers,
            source_url: source_url.to_owned(),
            raw_json: json!({}),
            ..ResourceCandidate::default()
        });
    }
    resources
}

fn extract_magnet_value(value: &str) -> Option<String> {
    let value = html_unescape(value);
    let start = value.find("magnet:?")?;
    let tail = &value[start..];
    let end = tail.find(['\'', '"', '<', ' ', ')']).unwrap_or(tail.len());
    Some(tail[..end].to_owned())
}

fn clean_resource_label(value: &str, default_label: &str) -> String {
    let label = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = label.to_ascii_lowercase();
    if label.is_empty()
        || label.len() > 160
        || lower.contains("magnet:?")
        || lower.contains("onclick=")
        || lower.contains("text-align")
        || label.contains(['<', '>'])
    {
        default_label.to_owned()
    } else {
        label
    }
}

fn resource_label_score(label: &str, default_label: &str) -> usize {
    if label == default_label {
        return 0;
    }
    if ["download", "copy", "下载", "下載", "复制", "複製"]
        .iter()
        .any(|generic| label.eq_ignore_ascii_case(generic))
    {
        return 1;
    }
    10 + label.chars().take(100).count()
}

fn parse_size_bytes(label: &str) -> Option<i64> {
    for token in label.split_whitespace() {
        let token = token
            .trim_matches(|character: char| !character.is_ascii_alphanumeric() && character != '.')
            .to_ascii_uppercase();
        for (suffix, multiplier) in [
            ("TB", 1024_f64.powi(4)),
            ("GB", 1024_f64.powi(3)),
            ("MB", 1024_f64.powi(2)),
            ("KB", 1024_f64),
        ] {
            if let Some(number) = token.strip_suffix(suffix)
                && let Ok(value) = number.parse::<f64>()
                && value.is_finite()
                && value >= 0.0
            {
                return Some((value * multiplier).round() as i64);
            }
        }
    }
    None
}

pub fn extract_js_value(html: &str, name: &str) -> Option<String> {
    for marker in [
        format!("var {name}"),
        format!("{name} ="),
        format!("{name}="),
    ] {
        let Some(at) = html.find(&marker) else {
            continue;
        };
        let end = char_boundary_before(html, at + marker.len() + 800);
        let value = html[at + marker.len()..end]
            .trim_start_matches(|character: char| character.is_whitespace() || character == '=')
            .split([';', '\n', '\r', ','])
            .next()?
            .trim()
            .trim_matches(['\'', '\"']);
        if !value.is_empty() {
            return Some(html_unescape(value));
        }
    }
    None
}

fn char_boundary_before(value: &str, index: usize) -> usize {
    let mut index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn html_unescape(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&#38;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_magnets_from_links_and_clipboard_attributes() {
        let html = r#"<a href="magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&amp;tr=https%3A%2F%2Ftracker.test">1080p</a><button data-clipboard-text="magnet:?xt=urn:btih:89abcdef0123456789abcdef0123456789abcdef">copy</button>"#;
        let resources = parse_magnet_candidates(html, "resource", "https://source.test/item");
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].trackers, vec!["https://tracker.test"]);
    }

    #[test]
    fn extracts_javbus_ajax_values() {
        let html = "<script>var gid = 12345; var uc = 0; var img = '/cover.jpg';</script>";
        assert_eq!(extract_js_value(html, "gid").as_deref(), Some("12345"));
        assert_eq!(extract_js_value(html, "uc").as_deref(), Some("0"));
        assert_eq!(extract_js_value(html, "img").as_deref(), Some("/cover.jpg"));
    }

    #[test]
    fn extracts_javbus_onclick_magnet_without_html_in_title() {
        let html = r#"<a style="text-align:center;white-space:nowrap" onclick="window.open('magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&amp;dn=ABC-123','_self')">4.57GB</a>"#;
        let resources =
            parse_magnet_candidates(html, "JavBus resource", "https://source.test/item");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].title, "4.57GB");
        assert_eq!(resources[0].size_bytes, Some(4_907_000_136));
        assert_eq!(
            resources[0].download_url,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=ABC-123"
        );
    }
}
