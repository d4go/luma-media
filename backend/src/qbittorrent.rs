use anyhow::{Context, bail};
use reqwest::header::{COOKIE, SET_COOKIE};
use serde::Deserialize;

use crate::models::Settings;

#[derive(Clone)]
pub struct QBittorrentClient {
    base_url: String,
    username: String,
    password: String,
    http: reqwest::Client,
}

impl QBittorrentClient {
    pub fn new(settings: &Settings) -> anyhow::Result<Self> {
        let base_url = settings.qbittorrent_url.trim().trim_end_matches('/');
        let url = reqwest::Url::parse(base_url).context("invalid qBittorrent URL")?;
        if !matches!(url.scheme(), "http" | "https") {
            bail!("qBittorrent URL must use http or https");
        }
        Ok(Self {
            base_url: base_url.into(),
            username: settings.qbittorrent_username.clone(),
            password: settings.qbittorrent_password.clone(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
        })
    }

    async fn login(&self) -> anyhow::Result<String> {
        let response = self
            .http
            .post(format!("{}/api/v2/auth/login", self.base_url))
            .form(&[
                ("username", self.username.as_str()),
                ("password", self.password.as_str()),
            ])
            .send()
            .await
            .context("could not reach qBittorrent")?;
        if !response.status().is_success() {
            bail!("qBittorrent login returned HTTP {}", response.status());
        }
        let cookie = response
            .headers()
            .get(SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .filter(|value| value.starts_with("SID="))
            .map(str::to_owned);
        let body = response.text().await.unwrap_or_default();
        if body.trim() != "Ok." {
            bail!("qBittorrent rejected the credentials");
        }
        cookie.context("qBittorrent did not return a session cookie")
    }

    pub async fn version(&self) -> anyhow::Result<String> {
        let cookie = self.login().await?;
        let response = self
            .http
            .get(format!("{}/api/v2/app/version", self.base_url))
            .header(COOKIE, cookie)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "qBittorrent version request returned HTTP {}",
                response.status()
            );
        }
        Ok(response.text().await?.trim().to_owned())
    }

    pub async fn add_download(
        &self,
        download_url: &str,
        trackers: &[String],
    ) -> anyhow::Result<Option<String>> {
        let cookie = self.login().await?;
        let response = self
            .http
            .post(format!("{}/api/v2/torrents/add", self.base_url))
            .header(COOKIE, &cookie)
            .form(&[("urls", download_url)])
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() || body.trim() != "Ok." {
            bail!("qBittorrent rejected the download: HTTP {status} {body}");
        }

        let hash = magnet_hash(download_url);
        if let Some(hash) = hash.as_deref()
            && !trackers.is_empty()
        {
            let mut last_error = None;
            for attempt in 0..20 {
                match self
                    .add_trackers_with_session(&cookie, hash, trackers)
                    .await
                {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
                if attempt < 19 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
            if let Some(error) = last_error {
                tracing::warn!(%error, hash, "download was accepted but its trackers could not be added");
            }
        }
        Ok(hash)
    }

    pub async fn update_all_trackers(&self, trackers: &[String]) -> anyhow::Result<usize> {
        if trackers.is_empty() {
            return Ok(0);
        }
        let cookie = self.login().await?;
        let response = self
            .http
            .get(format!("{}/api/v2/torrents/info", self.base_url))
            .header(COOKIE, &cookie)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "qBittorrent torrent list returned HTTP {}",
                response.status()
            );
        }
        let torrents: Vec<TorrentInfo> = response.json().await?;
        let mut updated = 0;
        for torrent in torrents {
            if self
                .add_trackers_with_session(&cookie, &torrent.hash, trackers)
                .await
                .is_ok()
            {
                updated += 1;
            }
        }
        Ok(updated)
    }

    async fn add_trackers_with_session(
        &self,
        cookie: &str,
        hash: &str,
        trackers: &[String],
    ) -> anyhow::Result<()> {
        let urls = trackers.join("\n");
        let response = self
            .http
            .post(format!("{}/api/v2/torrents/addTrackers", self.base_url))
            .header(COOKIE, cookie)
            .form(&[("hash", hash), ("urls", urls.as_str())])
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("adding trackers returned HTTP {}", response.status());
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct TorrentInfo {
    hash: String,
}

pub fn parse_tracker_list(text: &str) -> Vec<String> {
    let mut trackers = text
        .lines()
        .map(str::trim)
        .filter(|line| {
            !line.is_empty()
                && (line.starts_with("http://")
                    || line.starts_with("https://")
                    || line.starts_with("udp://"))
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    trackers.sort();
    trackers.dedup();
    trackers
}

fn magnet_hash(url: &str) -> Option<String> {
    if !url.starts_with("magnet:?") {
        return None;
    }
    url.split('&')
        .flat_map(|part| part.split('?'))
        .find_map(|part| part.strip_prefix("xt=urn:btih:"))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Json, Router,
        http::{StatusCode, header},
        routing::{get, post},
    };

    use super::*;

    #[test]
    fn parses_and_deduplicates_tracker_lines() {
        let trackers = parse_tracker_list(
            "\nhttps://one/announce\ninvalid\nudp://two:80/announce\nhttps://one/announce\n",
        );
        assert_eq!(
            trackers,
            vec!["https://one/announce", "udp://two:80/announce"]
        );
    }

    #[test]
    fn extracts_hash_from_magnet() {
        assert_eq!(
            magnet_hash("magnet:?xt=urn:btih:ABC123&dn=Example").as_deref(),
            Some("ABC123")
        );
    }

    #[tokio::test]
    async fn submits_download_and_updates_trackers_through_web_api() {
        let downloads = Arc::new(AtomicUsize::new(0));
        let tracker_updates = Arc::new(AtomicUsize::new(0));
        let download_counter = downloads.clone();
        let tracker_counter = tracker_updates.clone();
        let app = Router::new()
            .route(
                "/api/v2/auth/login",
                post(|| async { ([(header::SET_COOKIE, "SID=test; HttpOnly")], "Ok.") }),
            )
            .route(
                "/api/v2/torrents/add",
                post(move || {
                    let counter = download_counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        "Ok."
                    }
                }),
            )
            .route(
                "/api/v2/torrents/addTrackers",
                post(move || {
                    let counter = tracker_counter.clone();
                    async move {
                        let attempt = counter.fetch_add(1, Ordering::SeqCst);
                        if attempt < 2 {
                            (StatusCode::CONFLICT, "torrent not ready")
                        } else {
                            (StatusCode::OK, "Ok.")
                        }
                    }
                }),
            )
            .route(
                "/api/v2/torrents/info",
                get(|| async { Json(serde_json::json!([{"hash": "ABC123"}])) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let settings = Settings {
            qbittorrent_url: format!("http://{address}"),
            ..Settings::default()
        };
        let client = QBittorrentClient::new(&settings).unwrap();
        let hash = client
            .add_download(
                "magnet:?xt=urn:btih:ABC123&dn=Example",
                &["udp://tracker.example:80/announce".into()],
            )
            .await
            .unwrap();
        assert_eq!(hash.as_deref(), Some("ABC123"));
        assert_eq!(downloads.load(Ordering::SeqCst), 1);
        assert_eq!(tracker_updates.load(Ordering::SeqCst), 3);

        let updated = client
            .update_all_trackers(&["https://tracker.example/announce".into()])
            .await
            .unwrap();
        assert_eq!(updated, 1);
        assert_eq!(tracker_updates.load(Ordering::SeqCst), 4);
        server.abort();
    }
}
