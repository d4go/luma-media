use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use chromiumoxide::{Browser, BrowserConfig};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use reqwest::Url;
use serde::Serialize;
use tokio::{
    process::{Child, Command},
    sync::{Mutex, Semaphore},
};
use uuid::Uuid;

use super::model::{
    FetchError, FetchFailureKind, FetchMode, FetchRequest, FetchResponse, FetchTransport,
};

const INTERACTIVE_SESSION_MINUTES: i64 = 15;
const CHROMIUM_PROFILE_LOCK_FILES: [&str; 3] =
    ["SingletonLock", "SingletonSocket", "SingletonCookie"];

#[derive(Debug, Clone)]
pub struct BrowserSettings {
    pub enabled: bool,
    pub executable: PathBuf,
    pub data_dir: PathBuf,
    pub headless: bool,
    pub session_port: u16,
}

impl BrowserSettings {
    pub fn from_env() -> Self {
        Self {
            enabled: env_bool("LUMA_BROWSER_ENABLED", false),
            executable: std::env::var_os("LUMA_CHROMIUM_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/usr/bin/chromium")),
            data_dir: std::env::var_os("LUMA_BROWSER_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data/browser-profiles")),
            headless: env_bool("LUMA_BROWSER_HEADLESS", true),
            session_port: std::env::var("LUMA_BROWSER_SESSION_PORT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(6080),
        }
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .and_then(|value| match value.as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserSessionView {
    pub session_id: String,
    pub provider_key: String,
    pub status: String,
    pub port: u16,
    pub password: Option<String>,
    pub started_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

struct InteractiveSession {
    session_id: String,
    provider_key: String,
    password: String,
    started_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    children: Vec<Child>,
}

impl InteractiveSession {
    fn view(&self, status: &str, port: u16, include_password: bool) -> BrowserSessionView {
        BrowserSessionView {
            session_id: self.session_id.clone(),
            provider_key: self.provider_key.clone(),
            status: status.to_owned(),
            port,
            password: include_password.then(|| self.password.clone()),
            started_at: self.started_at,
            expires_at: self.expires_at,
        }
    }
}

#[derive(Clone)]
pub struct BrowserManager {
    settings: BrowserSettings,
    browsers: Arc<Mutex<HashMap<String, Arc<Mutex<Browser>>>>>,
    provider_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    interactive: Arc<Mutex<Option<InteractiveSession>>>,
    interactive_start: Arc<Mutex<()>>,
}

impl std::fmt::Debug for BrowserManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserManager")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl BrowserManager {
    pub fn new(settings: BrowserSettings) -> Self {
        Self {
            settings,
            browsers: Arc::new(Mutex::new(HashMap::new())),
            provider_locks: Arc::new(Mutex::new(HashMap::new())),
            interactive: Arc::new(Mutex::new(None)),
            interactive_start: Arc::new(Mutex::new(())),
        }
    }

    pub fn from_env() -> Self {
        Self::new(BrowserSettings::from_env())
    }

    pub fn enabled(&self) -> bool {
        self.settings.enabled
    }

    pub fn executable(&self) -> &Path {
        &self.settings.executable
    }

    pub fn profile_path(&self, provider_key: &str) -> Result<PathBuf, FetchError> {
        validate_provider_key(provider_key)?;
        Ok(self.settings.data_dir.join(provider_key))
    }

    pub async fn fetch(
        &self,
        request: FetchRequest,
        _transport: &FetchTransport,
    ) -> Result<FetchResponse, FetchError> {
        self.validate_available(&request.provider_key)?;
        let provider_key = request.provider_key.clone();
        let lock = self.provider_lock(&provider_key).await;
        let _permit = lock.acquire_owned().await.map_err(|error| {
            FetchError::new(
                &provider_key,
                FetchFailureKind::BrowserUnavailable,
                error.to_string(),
                None,
            )
        })?;
        if self.active_session_for(&provider_key).await? {
            return Err(FetchError::new(
                provider_key,
                FetchFailureKind::InteractionRequired,
                "an interactive browser session is active for this provider",
                None,
            ));
        }
        let browser = self.browser(&provider_key, request.timeout).await?;
        let started = Instant::now();
        let page = browser
            .lock()
            .await
            .new_page("about:blank")
            .await
            .map_err(|error| {
                FetchError::new(
                    &provider_key,
                    FetchFailureKind::BrowserUnavailable,
                    error.to_string(),
                    None,
                )
            })?;
        let navigation = tokio::time::timeout(request.timeout, page.goto(request.url.as_str()))
            .await
            .map_err(|_| {
                FetchError::new(
                    &provider_key,
                    FetchFailureKind::Timeout,
                    "browser navigation timed out",
                    None,
                )
            })?
            .map_err(|error| {
                FetchError::new(
                    &provider_key,
                    FetchFailureKind::Network,
                    error.to_string(),
                    None,
                )
            });
        if let Err(error) = navigation {
            let _ = page.close().await;
            return Err(error);
        }
        let body = page.content().await.map_err(|error| {
            FetchError::new(
                &provider_key,
                FetchFailureKind::InvalidContent,
                error.to_string(),
                None,
            )
        })?;
        let final_url = page
            .url()
            .await
            .ok()
            .flatten()
            .and_then(|value| Url::parse(&value).ok())
            .unwrap_or(request.url);
        let _ = page.close().await;
        Ok(FetchResponse {
            final_url,
            status: None,
            content_type: Some("text/html".into()),
            headers: reqwest::header::HeaderMap::new(),
            body,
            fetched_at: Utc::now(),
            fetch_mode: FetchMode::Browser,
            elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }

    pub async fn start_interactive(
        &self,
        provider_key: &str,
        initial_url: &Url,
    ) -> Result<BrowserSessionView, FetchError> {
        self.validate_available(provider_key)?;
        validate_provider_key(provider_key)?;
        let _start_guard = self.interactive_start.lock().await;
        self.cleanup_expired_session().await;
        {
            let active = self.interactive.lock().await;
            if let Some(session) = active.as_ref() {
                return Err(FetchError::new(
                    provider_key,
                    FetchFailureKind::InteractionRequired,
                    format!(
                        "interactive session {} is already active for {}",
                        session.session_id, session.provider_key
                    ),
                    None,
                ));
            }
        }

        let lock = self.provider_lock(provider_key).await;
        let _permit = lock.acquire_owned().await.map_err(|error| {
            FetchError::new(
                provider_key,
                FetchFailureKind::BrowserUnavailable,
                error.to_string(),
                None,
            )
        })?;
        self.close_provider(provider_key).await;

        let profile_path = self.profile_path(provider_key)?;
        tokio::fs::create_dir_all(&profile_path)
            .await
            .map_err(|error| browser_error(provider_key, error))?;
        clear_stale_profile_locks(provider_key, &profile_path).await?;
        let session_id = Uuid::new_v4().to_string();
        let password = Uuid::new_v4().simple().to_string()[..12].to_owned();
        let started_at = Utc::now();
        let expires_at = started_at + chrono::Duration::minutes(INTERACTIVE_SESSION_MINUTES);
        let mut children = Vec::new();

        let launch_result = async {
            let mut xvfb = Command::new("Xvfb");
            xvfb.args([":99", "-screen", "0", "1280x800x24", "-nolisten", "tcp"]);
            children.push(spawn_silent(&mut xvfb)?);
            tokio::time::sleep(Duration::from_millis(300)).await;

            let mut vnc = Command::new("x11vnc");
            vnc.args([
                "-display",
                ":99",
                "-forever",
                "-shared",
                "-rfbport",
                "5900",
                "-passwd",
                &password,
                "-noxdamage",
            ]);
            children.push(spawn_silent(&mut vnc)?);

            let mut proxy = Command::new("websockify");
            proxy.args([
                "--web=/usr/share/novnc",
                &self.settings.session_port.to_string(),
                "127.0.0.1:5900",
            ]);
            children.push(spawn_silent(&mut proxy)?);

            let mut chromium = Command::new(&self.settings.executable);
            chromium
                .env("DISPLAY", ":99")
                .arg(format!("--user-data-dir={}", profile_path.display()))
                .args([
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--disable-background-networking",
                    initial_url.as_str(),
                ]);
            children.push(spawn_silent(&mut chromium)?);
            tokio::time::sleep(Duration::from_millis(700)).await;
            for child in &mut children {
                if let Some(status) = child
                    .try_wait()
                    .map_err(|error| browser_error(provider_key, error))?
                {
                    return Err(FetchError::new(
                        provider_key,
                        FetchFailureKind::BrowserUnavailable,
                        format!("interactive browser component exited early with {status}"),
                        None,
                    ));
                }
            }
            Ok::<(), FetchError>(())
        }
        .await;

        if let Err(error) = launch_result {
            stop_children(children).await;
            return Err(error);
        }

        let session = InteractiveSession {
            session_id,
            provider_key: provider_key.to_owned(),
            password,
            started_at,
            expires_at,
            children,
        };
        let view = session.view("active", self.settings.session_port, true);
        *self.interactive.lock().await = Some(session);
        let manager = self.clone();
        let expiring_session_id = view.session_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(
                u64::try_from(INTERACTIVE_SESSION_MINUTES).unwrap_or(15) * 60,
            ))
            .await;
            manager.expire_session(&expiring_session_id).await;
        });
        Ok(view)
    }

    pub async fn interactive_status(
        &self,
        provider_key: &str,
        session_id: &str,
    ) -> Result<BrowserSessionView, FetchError> {
        validate_provider_key(provider_key)?;
        self.cleanup_expired_session().await;
        let active = self.interactive.lock().await;
        let session = active
            .as_ref()
            .filter(|session| {
                session.provider_key == provider_key && session.session_id == session_id
            })
            .ok_or_else(|| session_not_found(provider_key))?;
        Ok(session.view("active", self.settings.session_port, false))
    }

    pub async fn complete_interactive(
        &self,
        provider_key: &str,
        session_id: &str,
    ) -> Result<BrowserSessionView, FetchError> {
        self.finish_interactive(provider_key, session_id, "completed")
            .await
    }

    pub async fn cancel_interactive(
        &self,
        provider_key: &str,
        session_id: &str,
    ) -> Result<BrowserSessionView, FetchError> {
        self.finish_interactive(provider_key, session_id, "cancelled")
            .await
    }

    pub async fn clear_profile(&self, provider_key: &str) -> Result<(), FetchError> {
        validate_provider_key(provider_key)?;
        self.cleanup_expired_session().await;
        if self.active_session_for(provider_key).await? {
            return Err(FetchError::new(
                provider_key,
                FetchFailureKind::InteractionRequired,
                "close the interactive session before clearing its browser profile",
                None,
            ));
        }
        let lock = self.provider_lock(provider_key).await;
        let _permit = lock.acquire_owned().await.map_err(|error| {
            FetchError::new(
                provider_key,
                FetchFailureKind::BrowserUnavailable,
                error.to_string(),
                None,
            )
        })?;
        self.close_provider(provider_key).await;
        let profile_path = self.profile_path(provider_key)?;
        match tokio::fs::remove_dir_all(&profile_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(browser_error(provider_key, error)),
        }
        tokio::fs::create_dir_all(profile_path)
            .await
            .map_err(|error| browser_error(provider_key, error))?;
        Ok(())
    }

    async fn browser(
        &self,
        provider_key: &str,
        timeout: Duration,
    ) -> Result<Arc<Mutex<Browser>>, FetchError> {
        if let Some(browser) = self.browsers.lock().await.get(provider_key).cloned() {
            return Ok(browser);
        }
        let profile_path = self.profile_path(provider_key)?;
        tokio::fs::create_dir_all(&profile_path)
            .await
            .map_err(|error| {
                FetchError::new(
                    provider_key,
                    FetchFailureKind::BrowserUnavailable,
                    format!("could not create browser profile: {error}"),
                    None,
                )
            })?;
        clear_stale_profile_locks(provider_key, &profile_path).await?;
        let mut builder = BrowserConfig::builder()
            .chrome_executable(&self.settings.executable)
            .user_data_dir(profile_path)
            .request_timeout(timeout)
            .launch_timeout(timeout)
            .no_sandbox()
            .arg("--disable-dev-shm-usage");
        if !self.settings.headless {
            builder = builder.with_head();
        }
        let config = builder.build().map_err(|error| {
            FetchError::new(
                provider_key,
                FetchFailureKind::BrowserUnavailable,
                error.to_string(),
                None,
            )
        })?;
        let (browser, mut handler) = Browser::launch(config).await.map_err(|error| {
            FetchError::new(
                provider_key,
                FetchFailureKind::BrowserUnavailable,
                error.to_string(),
                None,
            )
        })?;
        let key = provider_key.to_owned();
        tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                if let Err(error) = event {
                    tracing::warn!(provider_key = key, %error, "Chromium handler stopped");
                    break;
                }
            }
        });
        let browser = Arc::new(Mutex::new(browser));
        self.browsers
            .lock()
            .await
            .insert(provider_key.to_owned(), browser.clone());
        Ok(browser)
    }

    async fn close_provider(&self, provider_key: &str) {
        let Some(browser) = self.browsers.lock().await.remove(provider_key) else {
            return;
        };
        let mut browser = browser.lock().await;
        let _ = browser.close().await;
        if tokio::time::timeout(Duration::from_secs(5), browser.wait())
            .await
            .is_err()
        {
            let _ = browser.kill().await;
        }
    }

    async fn provider_lock(&self, provider_key: &str) -> Arc<Semaphore> {
        let mut locks = self.provider_locks.lock().await;
        locks
            .entry(provider_key.to_owned())
            .or_insert_with(|| Arc::new(Semaphore::new(1)))
            .clone()
    }

    fn validate_available(&self, provider_key: &str) -> Result<(), FetchError> {
        if !self.settings.enabled {
            return Err(FetchError::new(
                provider_key,
                FetchFailureKind::BrowserUnavailable,
                "browser fetching is disabled",
                None,
            ));
        }
        if !self.settings.executable.is_file() {
            return Err(FetchError::new(
                provider_key,
                FetchFailureKind::BrowserUnavailable,
                format!(
                    "Chromium executable does not exist: {}",
                    self.settings.executable.display()
                ),
                None,
            ));
        }
        Ok(())
    }

    async fn active_session_for(&self, provider_key: &str) -> Result<bool, FetchError> {
        self.cleanup_expired_session().await;
        Ok(self
            .interactive
            .lock()
            .await
            .as_ref()
            .is_some_and(|session| session.provider_key == provider_key))
    }

    async fn cleanup_expired_session(&self) {
        let expired = {
            let mut active = self.interactive.lock().await;
            if active
                .as_ref()
                .is_some_and(|session| session.expires_at <= Utc::now())
            {
                active.take()
            } else {
                None
            }
        };
        if let Some(session) = expired {
            stop_children(session.children).await;
        }
    }

    async fn expire_session(&self, session_id: &str) {
        let expired = {
            let mut active = self.interactive.lock().await;
            if active
                .as_ref()
                .is_some_and(|session| session.session_id == session_id)
            {
                active.take()
            } else {
                None
            }
        };
        if let Some(session) = expired {
            stop_children(session.children).await;
        }
    }

    async fn finish_interactive(
        &self,
        provider_key: &str,
        session_id: &str,
        status: &str,
    ) -> Result<BrowserSessionView, FetchError> {
        validate_provider_key(provider_key)?;
        let session = {
            let mut active = self.interactive.lock().await;
            let matches = active.as_ref().is_some_and(|session| {
                session.provider_key == provider_key && session.session_id == session_id
            });
            if !matches {
                return Err(session_not_found(provider_key));
            }
            active.take().expect("interactive session checked above")
        };
        let view = session.view(status, self.settings.session_port, false);
        stop_children(session.children).await;
        Ok(view)
    }
}

fn spawn_silent(command: &mut Command) -> Result<Child, FetchError> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            FetchError::new(
                "browser-session",
                FetchFailureKind::BrowserUnavailable,
                format!("could not start interactive browser component: {error}"),
                None,
            )
        })
}

async fn stop_children(mut children: Vec<Child>) {
    for child in children.iter_mut().rev() {
        if let Some(process_id) = child.id() {
            let _ = Command::new("kill")
                .args(["-TERM", &process_id.to_string()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;
        }
        if tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .is_err()
        {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
}

fn browser_error(provider_key: &str, error: impl std::fmt::Display) -> FetchError {
    FetchError::new(
        provider_key,
        FetchFailureKind::BrowserUnavailable,
        error.to_string(),
        None,
    )
}

fn session_not_found(provider_key: &str) -> FetchError {
    FetchError::new(
        provider_key,
        FetchFailureKind::InteractionRequired,
        "interactive browser session was not found or has expired",
        None,
    )
}

async fn clear_stale_profile_locks(
    provider_key: &str,
    profile_path: &Path,
) -> Result<(), FetchError> {
    for file_name in CHROMIUM_PROFILE_LOCK_FILES {
        let lock_path = profile_path.join(file_name);
        match tokio::fs::symlink_metadata(&lock_path).await {
            Ok(metadata) if metadata.file_type().is_symlink() || metadata.is_file() => {
                tokio::fs::remove_file(&lock_path)
                    .await
                    .map_err(|error| browser_error(provider_key, error))?;
            }
            Ok(_) => {
                return Err(FetchError::new(
                    provider_key,
                    FetchFailureKind::BrowserUnavailable,
                    format!(
                        "refusing to remove unexpected Chromium lock path: {}",
                        lock_path.display()
                    ),
                    None,
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(browser_error(provider_key, error)),
        }
    }
    Ok(())
}

fn validate_provider_key(provider_key: &str) -> Result<(), FetchError> {
    if provider_key.is_empty()
        || !provider_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(FetchError::new(
            provider_key,
            FetchFailureKind::BrowserUnavailable,
            "provider key is not safe for a profile path",
            None,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> BrowserSettings {
        BrowserSettings {
            enabled: false,
            executable: PathBuf::from("chromium"),
            data_dir: PathBuf::from("/data/browser-profiles"),
            headless: true,
            session_port: 6080,
        }
    }

    #[test]
    fn provider_profile_paths_reject_traversal() {
        let manager = BrowserManager::new(settings());
        assert_eq!(
            manager.profile_path("javdb").unwrap(),
            PathBuf::from("/data/browser-profiles/javdb")
        );
        assert!(manager.profile_path("../escape").is_err());
        assert!(manager.profile_path("javdb/other").is_err());
    }

    #[test]
    fn interactive_session_uses_a_dedicated_port() {
        let manager = BrowserManager::new(settings());
        assert_eq!(manager.settings.session_port, 6080);
    }

    #[test]
    fn only_chromium_singleton_locks_are_cleaned() {
        assert_eq!(
            CHROMIUM_PROFILE_LOCK_FILES,
            ["SingletonLock", "SingletonSocket", "SingletonCookie"]
        );
    }
}
