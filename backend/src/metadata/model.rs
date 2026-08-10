use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LocalizedAlias {
    pub value: String,
    #[serde(default = "default_locale")]
    pub locale: String,
}

impl LocalizedAlias {
    pub fn new(value: impl Into<String>, locale: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            locale: locale.into(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceActor {
    pub provider_actor_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<LocalizedAlias>,
    pub avatar_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataSourceInput {
    pub media_id: i64,
    pub provider_key: String,
    pub provider_entity_id: String,
    pub source_url: Option<String>,
    pub record_kind: String,
    pub evidence_level: i64,
    pub priority: i64,
    pub normalized_code: String,
    pub title: Option<String>,
    pub original_title: Option<String>,
    pub summary: Option<String>,
    pub release_date: Option<String>,
    pub duration_minutes: Option<i64>,
    pub poster_url: Option<String>,
    pub backdrop_url: Option<String>,
    #[serde(default)]
    pub actors: Vec<SourceActor>,
    #[serde(default)]
    pub aliases: Vec<LocalizedAlias>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub raw_json: Value,
}

impl MetadataSourceInput {
    pub fn catalogue(
        media_id: i64,
        provider_key: impl Into<String>,
        provider_entity_id: impl Into<String>,
        normalized_code: impl Into<String>,
    ) -> Self {
        Self {
            media_id,
            provider_key: provider_key.into(),
            provider_entity_id: provider_entity_id.into(),
            source_url: None,
            record_kind: "catalogue".into(),
            evidence_level: 1,
            priority: 100,
            normalized_code: normalized_code.into(),
            title: None,
            original_title: None,
            summary: None,
            release_date: None,
            duration_minutes: None,
            poster_url: None,
            backdrop_url: None,
            actors: Vec::new(),
            aliases: Vec::new(),
            tags: Vec::new(),
            raw_json: Value::Object(Default::default()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionResult {
    pub media_id: i64,
    pub source_record_id: i64,
    pub actor_ids: Vec<i64>,
}

fn default_locale() -> String {
    "und".into()
}
