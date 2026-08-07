use std::{collections::HashMap, time::Duration};

use anyhow::{Context, anyhow};
use reqwest::{Client, RequestBuilder, StatusCode, Url, redirect::Policy};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::models::Settings;

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct ProviderData {
    #[serde(default)]
    movie_providers: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MovieSearchResult {
    pub id: String,
    pub provider: String,
    pub title: String,
    #[serde(default)]
    pub number: String,
}

pub struct MetaTubeClient {
    client: Client,
    image_client: Client,
    base_url: Url,
    token: String,
}

#[derive(Debug)]
pub struct DownloadedImage {
    pub bytes: Vec<u8>,
    pub extension: &'static str,
}

impl MetaTubeClient {
    pub fn new(settings: &Settings) -> anyhow::Result<Self> {
        let base = settings.metatube_url.trim().trim_end_matches('/');
        let base_url = Url::parse(base).context("invalid MetaTube URL")?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(anyhow!("MetaTube URL must use http or https"));
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(8))
            .timeout(Duration::from_secs(45))
            .build()?;
        let image_client = Client::builder()
            .connect_timeout(Duration::from_secs(8))
            .timeout(Duration::from_secs(45))
            .redirect(Policy::none())
            .build()?;
        Ok(Self {
            client,
            image_client,
            base_url,
            token: settings.metatube_token.trim().to_owned(),
        })
    }

    pub async fn test_connection(&self) -> anyhow::Result<usize> {
        let response: ApiResponse<ProviderData> = self
            .send(self.client.get(self.endpoint(&["v1", "providers"])?))
            .await?;
        Ok(response.data.movie_providers.len())
    }

    pub async fn search_movie(&self, query: &str) -> anyhow::Result<MovieSearchResult> {
        let results = self.search_movies(query).await?;
        choose_best_match(query, results)
            .ok_or_else(|| anyhow!("MetaTube found no metadata for {query}"))
    }

    pub async fn search_movies(&self, query: &str) -> anyhow::Result<Vec<MovieSearchResult>> {
        let url = self.endpoint(&["v1", "movies", "search"])?;
        let response: ApiResponse<Vec<MovieSearchResult>> = self
            .send(self.client.get(url).query(&[("q", query)]))
            .await?;
        Ok(response.data)
    }

    pub async fn movie_info(&self, provider: &str, id: &str) -> anyhow::Result<Value> {
        let url = self.endpoint(&["v1", "movies", provider, id])?;
        let response: ApiResponse<Value> = self.send(self.client.get(url)).await?;
        Ok(response.data)
    }

    pub async fn download_image(&self, source: &str) -> anyhow::Result<DownloadedImage> {
        const MAX_IMAGE_SIZE: usize = 25 * 1024 * 1024;

        let url = Url::parse(source)
            .or_else(|_| self.base_url.join(source))
            .context("invalid image URL returned by MetaTube")?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(anyhow!("image URL must use http or https"));
        }

        let mut response = self
            .authorize(self.image_client.get(url))
            .send()
            .await
            .context("could not download metadata image")?;
        let status = response.status();
        if status != StatusCode::OK {
            return Err(anyhow!("image download returned HTTP {status}"));
        }
        if response
            .content_length()
            .is_some_and(|size| size > MAX_IMAGE_SIZE as u64)
        {
            return Err(anyhow!("metadata image is larger than 25 MiB"));
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let extension = match content_type.as_str() {
            "image/jpeg" | "image/jpg" => "jpg",
            "image/png" => "png",
            "image/webp" => "webp",
            _ => return Err(anyhow!("unsupported image content type: {content_type}")),
        };

        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("failed while downloading metadata image")?
        {
            if bytes.len() + chunk.len() > MAX_IMAGE_SIZE {
                return Err(anyhow!("metadata image is larger than 25 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(anyhow!("metadata image response was empty"));
        }

        Ok(DownloadedImage { bytes, extension })
    }

    fn endpoint(&self, segments: &[&str]) -> anyhow::Result<Url> {
        let mut url = self.base_url.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| anyhow!("MetaTube URL cannot be used as a base URL"))?;
            path.clear();
            for segment in segments {
                path.push(segment);
            }
        }
        Ok(url)
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        if self.token.is_empty() {
            request
        } else {
            request.bearer_auth(&self.token)
        }
    }

    async fn send<T: for<'de> Deserialize<'de>>(
        &self,
        request: RequestBuilder,
    ) -> anyhow::Result<T> {
        let response = self
            .authorize(request)
            .send()
            .await
            .context("could not connect to MetaTube")?;
        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            let message = message.chars().take(300).collect::<String>();
            return Err(anyhow!("MetaTube returned HTTP {status}: {message}"));
        }
        response
            .json::<T>()
            .await
            .context("MetaTube returned an invalid JSON response")
    }
}

fn choose_best_match(query: &str, results: Vec<MovieSearchResult>) -> Option<MovieSearchResult> {
    let normalized_query = normalize(query);
    results
        .iter()
        .find(|item| {
            normalize(&item.number) == normalized_query
                || normalize(&item.title) == normalized_query
        })
        .cloned()
        .or_else(|| results.into_iter().next())
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        http::{StatusCode, header},
        routing::get,
    };

    #[test]
    fn exact_number_is_preferred_over_first_result() {
        let results = vec![
            MovieSearchResult {
                id: "1".into(),
                provider: "first".into(),
                title: "Another title".into(),
                number: "ZZZ-999".into(),
            },
            MovieSearchResult {
                id: "2".into(),
                provider: "exact".into(),
                title: "Matched title".into(),
                number: "ABC-123".into(),
            },
        ];
        let selected = choose_best_match("abc_123", results).unwrap();
        assert_eq!(selected.provider, "exact");
    }

    #[tokio::test]
    async fn image_download_rejects_redirects_and_html() {
        let app = Router::new()
            .route(
                "/image",
                get(|| async { ([(header::CONTENT_TYPE, "image/jpeg")], vec![1_u8, 2, 3]) }),
            )
            .route(
                "/redirect",
                get(|| async {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, "/image")],
                        "redirect",
                    )
                }),
            )
            .route(
                "/html",
                get(|| async { ([(header::CONTENT_TYPE, "text/html")], "error page") }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let settings = Settings {
            metatube_url: format!("http://{address}"),
            metatube_token: String::new(),
            output_format: "nfo".into(),
            scan_interval: 60,
            overwrite_policy: "missing".into(),
            log_level: "info".into(),
        };
        let client = MetaTubeClient::new(&settings).unwrap();

        let image = client
            .download_image(&format!("http://{address}/image"))
            .await
            .unwrap();
        assert_eq!(image.extension, "jpg");
        assert_eq!(image.bytes, vec![1, 2, 3]);

        let redirect = client
            .download_image(&format!("http://{address}/redirect"))
            .await
            .unwrap_err()
            .to_string();
        assert!(redirect.contains("302"));

        let html = client
            .download_image(&format!("http://{address}/html"))
            .await
            .unwrap_err()
            .to_string();
        assert!(html.contains("text/html"));
        server.abort();
    }
}
