use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Folder {
    pub id: i64,
    pub name: String,
    pub path: String,
    pub media_type: String,
    pub output_format: String,
    pub scan_mode: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderInput {
    pub name: String,
    pub path: String,
    #[serde(rename = "type")]
    pub media_type: String,
    #[serde(default = "default_scan_mode")]
    pub scan_mode: String,
    #[serde(default = "default_output_format")]
    pub output_format: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_scan_mode() -> String {
    "manual".into()
}
fn default_output_format() -> String {
    "nfo".into()
}
fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: i64,
    pub media_id: Option<i64>,
    pub folder_id: Option<i64>,
    pub task_type: String,
    pub status: String,
    pub progress: i64,
    pub error_message: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
    pub record_count: i64,
    pub media: Option<TaskMedia>,
    pub folder: Option<TaskFolder>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMedia {
    pub id: i64,
    pub title: String,
    pub filename: String,
    pub path: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskFolder {
    pub id: i64,
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRecord {
    pub id: i64,
    pub task_id: i64,
    pub status: String,
    pub progress: i64,
    pub error_message: Option<String>,
    pub created_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetail {
    #[serde(flatten)]
    pub task: Task,
    pub records: Vec<TaskRecord>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaResourceState {
    pub status: String,
    pub source: Option<String>,
    pub path: Option<String>,
    pub checked_at: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaResources {
    pub nfo: MediaResourceState,
    pub poster: MediaResourceState,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaItem {
    pub id: i64,
    pub folder_id: Option<i64>,
    pub path: String,
    pub filename: String,
    pub hash: String,
    pub title: String,
    pub media_type: String,
    pub provider_id: Option<String>,
    pub status: String,
    pub scrape_task_id: Option<i64>,
    pub scrape_task_status: Option<String>,
    pub scrape_record_count: i64,
    pub resources: MediaResources,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScrapeOptions {
    #[serde(default = "default_enabled")]
    pub overwrite_nfo: bool,
    #[serde(default = "default_enabled")]
    pub overwrite_image: bool,
}

impl Default for ScrapeOptions {
    fn default() -> Self {
        Self {
            overwrite_nfo: true,
            overwrite_image: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchScrapeInput {
    pub media_ids: Vec<i64>,
    #[serde(flatten)]
    pub options: ScrapeOptions,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchScrapeResponse {
    pub queued: usize,
    pub skipped: usize,
    pub tasks: Vec<Task>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchTaskInput {
    pub task_ids: Vec<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchTaskResponse {
    pub processed: usize,
    pub skipped: usize,
    pub tasks: Vec<Task>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    pub metatube_url: String,
    #[serde(default)]
    pub metatube_token: String,
    pub output_format: String,
    pub scan_interval: u64,
    pub overwrite_policy: String,
    pub log_level: String,
    #[serde(default = "default_qbittorrent_url")]
    pub qbittorrent_url: String,
    #[serde(default = "default_qbittorrent_username")]
    pub qbittorrent_username: String,
    #[serde(default)]
    pub qbittorrent_password: String,
    #[serde(default)]
    pub qbittorrent_auto_update_trackers: bool,
    #[serde(default = "default_tracker_source_url")]
    pub qbittorrent_tracker_source_url: String,
    #[serde(default = "default_tracker_update_interval")]
    pub qbittorrent_tracker_update_interval: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            metatube_url: "http://127.0.0.1:8080".into(),
            metatube_token: String::new(),
            output_format: "nfo".into(),
            scan_interval: 60,
            overwrite_policy: "missing".into(),
            log_level: "info".into(),
            qbittorrent_url: default_qbittorrent_url(),
            qbittorrent_username: default_qbittorrent_username(),
            qbittorrent_password: String::new(),
            qbittorrent_auto_update_trackers: false,
            qbittorrent_tracker_source_url: default_tracker_source_url(),
            qbittorrent_tracker_update_interval: default_tracker_update_interval(),
        }
    }
}

fn default_qbittorrent_url() -> String {
    "http://127.0.0.1:8080".into()
}

fn default_qbittorrent_username() -> String {
    "admin".into()
}

fn default_tracker_source_url() -> String {
    "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt".into()
}

fn default_tracker_update_interval() -> u64 {
    1440
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetaTubeConnection {
    pub connected: bool,
    pub provider_count: usize,
    pub message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardStats {
    pub media_count: i64,
    pub task_count: i64,
    pub success_count: i64,
    pub failed_count: i64,
    pub recent_activity: Vec<Task>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    pub id: i64,
    pub level: String,
    pub module: String,
    pub message: String,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QBittorrentConnection {
    pub connected: bool,
    pub version: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceHealth {
    pub connected: bool,
    pub message: String,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    pub luma: ServiceHealth,
    pub meta_tube: ServiceHealth,
    pub qbittorrent: ServiceHealth,
    pub checked_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrawlerScript {
    pub id: i64,
    pub name: String,
    pub website_url: String,
    pub file_name: String,
    pub interval_minutes: u64,
    pub enabled: bool,
    pub auto_download: bool,
    pub last_started_at: Option<String>,
    pub last_finished_at: Option<String>,
    pub next_run_at: Option<String>,
    pub last_run_status: Option<String>,
    pub last_result_count: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrawlerRun {
    pub id: i64,
    pub script_id: i64,
    pub status: String,
    pub stdout: String,
    pub stderr: String,
    pub result_count: i64,
    pub error_message: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrawlerResult {
    pub id: i64,
    pub run_id: i64,
    pub script_id: i64,
    pub title: String,
    pub download_url: String,
    pub trackers: Vec<String>,
    pub raw: serde_json::Value,
    pub download_status: String,
    pub qbit_hash: Option<String>,
    pub error_message: Option<String>,
    pub created_at: String,
    pub downloaded_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchCrawlerResultInput {
    pub result_ids: Vec<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrape_options_default_to_overwrite() {
        let options: ScrapeOptions = serde_json::from_str("{}").unwrap();
        assert!(options.overwrite_nfo);
        assert!(options.overwrite_image);
    }

    #[test]
    fn batch_scrape_options_are_flattened() {
        let input: BatchScrapeInput = serde_json::from_str(
            r#"{"mediaIds":[1,2],"overwriteNfo":false,"overwriteImage":true}"#,
        )
        .unwrap();
        assert_eq!(input.media_ids, vec![1, 2]);
        assert!(!input.options.overwrite_nfo);
        assert!(input.options.overwrite_image);
    }
}
