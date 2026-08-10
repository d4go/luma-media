use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, sqlite::SqliteRow};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Pausing,
    Paused,
    Cancelling,
    Cancelled,
    Success,
    Failed,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "pausing" => Self::Pausing,
            "paused" => Self::Paused,
            "cancelling" => Self::Cancelling,
            "cancelled" => Self::Cancelled,
            "success" => Self::Success,
            "failed" => Self::Failed,
            _ => Self::Pending,
        }
    }

    /// Whether a runner may still claim new items for this run.
    pub fn accepts_new_items(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobItemStatus {
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
    Cancelled,
}

impl JobItemStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "success" => Self::Success,
            "failed" => Self::Failed,
            "skipped" => Self::Skipped,
            "cancelled" => Self::Cancelled,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobRun {
    pub id: i64,
    pub job_definition_id: Option<i64>,
    pub job_type: String,
    pub provider_key: Option<String>,
    pub status: JobStatus,
    pub idempotency_key: String,
    pub priority: i64,
    pub progress_current: i64,
    pub progress_total: Option<i64>,
    pub checkpoint: Value,
    pub error_message: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl JobRun {
    pub fn checkpoint(&self) -> Value {
        self.checkpoint.clone()
    }
}

#[derive(Debug, Clone)]
pub struct JobItem {
    pub id: i64,
    pub run_id: i64,
    pub item_key: String,
    pub status: JobItemStatus,
    pub retry_count: i64,
    pub checkpoint: Value,
    pub error_message: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CreateJobRun<'a> {
    pub job_type: &'a str,
    pub provider_key: Option<&'a str>,
    pub idempotency_key: &'a str,
    pub priority: i64,
    pub config: Value,
    pub job_definition_id: Option<i64>,
}

pub(crate) fn job_run_from_row(row: &SqliteRow) -> JobRun {
    JobRun {
        id: row.get("id"),
        job_definition_id: row.get("job_definition_id"),
        job_type: row.get("job_type"),
        provider_key: row.get("provider_key"),
        status: JobStatus::parse(&row.get::<String, _>("status")),
        idempotency_key: row.get("idempotency_key"),
        priority: row.get("priority"),
        progress_current: row.get("progress_current"),
        progress_total: row.get("progress_total"),
        checkpoint: parse_json(&row.get::<String, _>("checkpoint_json")),
        error_message: row.get("error_message"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn job_item_from_row(row: &SqliteRow) -> JobItem {
    JobItem {
        id: row.get("id"),
        run_id: row.get("run_id"),
        item_key: row.get("item_key"),
        status: JobItemStatus::parse(&row.get::<String, _>("status")),
        retry_count: row.get("retry_count"),
        checkpoint: parse_json(&row.get::<String, _>("checkpoint_json")),
        error_message: row.get("error_message"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn parse_json(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or(Value::Null)
}
