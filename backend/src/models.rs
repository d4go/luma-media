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
    pub finished_at: Option<String>,
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
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScrapeOptions {
    #[serde(default)]
    pub overwrite_nfo: bool,
    #[serde(default)]
    pub overwrite_image: bool,
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
