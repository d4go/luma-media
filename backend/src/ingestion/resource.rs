#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRefreshJobPayload {
    pub media_id: i64,
    pub provider_key: String,
    #[serde(default)]
    pub force: bool,
}
