use anyhow::{Context, bail};
use reqwest::header::{COOKIE, SET_COOKIE};
use serde::Deserialize;

use crate::models::{DownloadItem, Settings};

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

    pub async fn add_download_with_options(
        &self,
        download_url: &str,
        trackers: &[String],
        save_path: Option<&str>,
        category: Option<&str>,
        tags: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let cookie = self.login().await?;
        let hash = magnet_hash(download_url);
        let mut form = vec![("urls", download_url)];
        if let Some(value) = save_path.filter(|value| !value.trim().is_empty()) {
            form.push(("savepath", value));
        }
        if let Some(value) = category.filter(|value| !value.trim().is_empty()) {
            form.push(("category", value));
        }
        if let Some(value) = tags.filter(|value| !value.trim().is_empty()) {
            form.push(("tags", value));
        }
        let response = self
            .http
            .post(format!("{}/api/v2/torrents/add", self.base_url))
            .header(COOKIE, &cookie)
            .form(&form)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() || body.trim() != "Ok." {
            let already_present = if status.is_success() {
                match hash.as_deref() {
                    Some(hash) => self.torrent_exists_with_session(&cookie, hash).await?,
                    None => false,
                }
            } else {
                false
            };
            if !already_present {
                bail!("qBittorrent rejected the download: HTTP {status} {body}");
            }
            tracing::info!(
                hash = hash.as_deref(),
                "qBittorrent already has this download"
            );
        }

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

    async fn torrent_exists_with_session(
        &self,
        cookie: &str,
        expected_hash: &str,
    ) -> anyhow::Result<bool> {
        let response = self
            .http
            .get(format!("{}/api/v2/torrents/info", self.base_url))
            .header(COOKIE, cookie)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "qBittorrent torrent list returned HTTP {}",
                response.status()
            );
        }
        let expected_hash = normalize_hash(expected_hash);
        Ok(response
            .json::<Vec<TorrentInfo>>()
            .await?
            .iter()
            .any(|torrent| normalize_hash(&torrent.hash) == expected_hash))
    }

    pub async fn torrents(&self) -> anyhow::Result<Vec<DownloadItem>> {
        let cookie = self.login().await?;
        let response = self
            .http
            .get(format!("{}/api/v2/torrents/info", self.base_url))
            .header(COOKIE, cookie)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "qBittorrent torrent list returned HTTP {}",
                response.status()
            );
        }
        Ok(response
            .json::<Vec<TorrentInfo>>()
            .await?
            .into_iter()
            .map(DownloadItem::from)
            .collect())
    }

    pub async fn pause(&self, hash: &str) -> anyhow::Result<()> {
        self.post_hash_action("pause", hash).await
    }

    pub async fn resume(&self, hash: &str) -> anyhow::Result<()> {
        self.post_hash_action("resume", hash).await
    }

    pub async fn remove(&self, hash: &str) -> anyhow::Result<()> {
        let cookie = self.login().await?;
        let response = self
            .http
            .post(format!("{}/api/v2/torrents/delete", self.base_url))
            .header(COOKIE, cookie)
            .form(&[("hashes", hash), ("deleteFiles", "false")])
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("qBittorrent delete returned HTTP {}", response.status());
        }
        Ok(())
    }

    async fn post_hash_action(&self, action: &str, hash: &str) -> anyhow::Result<()> {
        let cookie = self.login().await?;
        let response = self
            .http
            .post(format!("{}/api/v2/torrents/{action}", self.base_url))
            .header(COOKIE, cookie)
            .form(&[("hashes", hash)])
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("qBittorrent {action} returned HTTP {}", response.status());
        }
        Ok(())
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
    #[serde(default)]
    name: String,
    #[serde(default)]
    size: i64,
    #[serde(default)]
    progress: f64,
    #[serde(default)]
    state: String,
    #[serde(default)]
    dlspeed: i64,
    #[serde(default)]
    upspeed: i64,
    #[serde(default)]
    eta: i64,
    #[serde(default)]
    save_path: String,
    #[serde(default)]
    added_on: i64,
    #[serde(default)]
    completion_on: i64,
}

impl From<TorrentInfo> for DownloadItem {
    fn from(torrent: TorrentInfo) -> Self {
        Self {
            hash: torrent.hash,
            name: torrent.name,
            size: torrent.size,
            progress: torrent.progress,
            state: torrent.state,
            download_speed: torrent.dlspeed,
            upload_speed: torrent.upspeed,
            eta: torrent.eta,
            save_path: torrent.save_path,
            added_on: torrent.added_on,
            completion_on: torrent.completion_on,
        }
    }
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

pub fn magnet_hash(url: &str) -> Option<String> {
    let url = reqwest::Url::parse(url).ok()?;
    if url.scheme() != "magnet" {
        return None;
    }
    url.query_pairs().find_map(|(key, value)| {
        (key == "xt")
            .then(|| value.strip_prefix("urn:btih:").and_then(normalize_hash))
            .flatten()
    })
}

pub fn normalize_hash(value: &str) -> Option<String> {
    let value = value.trim();
    if matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Some(value.to_ascii_lowercase());
    }
    if value.len() != 32 {
        return (!value.is_empty()).then(|| value.to_ascii_lowercase());
    }
    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    let mut decoded = Vec::with_capacity(20);
    for byte in value.bytes() {
        let digit = match byte.to_ascii_uppercase() {
            b'A'..=b'Z' => byte.to_ascii_uppercase() - b'A',
            b'2'..=b'7' => byte - b'2' + 26,
            _ => return None,
        };
        accumulator = (accumulator << 5) | u32::from(digit);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            decoded.push(((accumulator >> bits) & 0xff) as u8);
            accumulator &= (1_u32 << bits) - 1;
        }
    }
    (decoded.len() == 20).then(|| decoded.iter().map(|byte| format!("{byte:02x}")).collect())
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
            magnet_hash("magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01&dn=Example")
                .as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef01")
        );
        assert_eq!(
            normalize_hash("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").as_deref(),
            Some("0000000000000000000000000000000000000000")
        );
    }

    #[tokio::test]
    async fn submits_download_and_updates_trackers_through_web_api() {
        let downloads = Arc::new(AtomicUsize::new(0));
        let tracker_updates = Arc::new(AtomicUsize::new(0));
        let lifecycle_actions = Arc::new(AtomicUsize::new(0));
        let download_counter = downloads.clone();
        let tracker_counter = tracker_updates.clone();
        let pause_counter = lifecycle_actions.clone();
        let resume_counter = lifecycle_actions.clone();
        let delete_counter = lifecycle_actions.clone();
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
                get(|| async {
                    Json(serde_json::json!([{
                        "hash": "abcdef0123456789abcdef0123456789abcdef01",
                        "name": "Example Movie",
                        "size": 2147483648_i64,
                        "progress": 0.42,
                        "state": "downloading",
                        "dlspeed": 1048576,
                        "upspeed": 1024,
                        "eta": 120,
                        "save_path": "/downloads",
                        "added_on": 1_700_000_000_i64,
                        "completion_on": -1
                    }]))
                }),
            )
            .route(
                "/api/v2/torrents/pause",
                post(move || {
                    let counter = pause_counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/api/v2/torrents/resume",
                post(move || {
                    let counter = resume_counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/api/v2/torrents/delete",
                post(move || {
                    let counter = delete_counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
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
            .add_download_with_options(
                "magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01&dn=Example",
                &["udp://tracker.example:80/announce".into()],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            hash.as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef01")
        );
        assert_eq!(downloads.load(Ordering::SeqCst), 1);
        assert_eq!(tracker_updates.load(Ordering::SeqCst), 3);

        let updated = client
            .update_all_trackers(&["https://tracker.example/announce".into()])
            .await
            .unwrap();
        assert_eq!(updated, 1);
        assert_eq!(tracker_updates.load(Ordering::SeqCst), 4);

        let torrents = client.torrents().await.unwrap();
        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].name, "Example Movie");
        assert_eq!(torrents[0].download_speed, 1_048_576);
        let torrent_hash = "abcdef0123456789abcdef0123456789abcdef01";
        client.pause(torrent_hash).await.unwrap();
        client.resume(torrent_hash).await.unwrap();
        client.remove(torrent_hash).await.unwrap();
        assert_eq!(lifecycle_actions.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn treats_qbit_fails_as_success_when_torrent_already_exists() {
        let app = Router::new()
            .route(
                "/api/v2/auth/login",
                post(|| async { ([(header::SET_COOKIE, "SID=test; HttpOnly")], "Ok.") }),
            )
            .route("/api/v2/torrents/add", post(|| async { "Fails." }))
            .route(
                "/api/v2/torrents/info",
                get(|| async {
                    Json(serde_json::json!([{
                        "hash": "abcdef0123456789abcdef0123456789abcdef01",
                        "name": "Existing Movie",
                        "progress": 0.25,
                        "state": "downloading"
                    }]))
                }),
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
            .add_download_with_options(
                "magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01",
                &[],
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            hash.as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef01")
        );
        server.abort();
    }
}
