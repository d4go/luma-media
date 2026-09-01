use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};

use super::MetadataSourceInput;

pub(super) async fn upsert_source_record(
    pool: &SqlitePool,
    input: &MetadataSourceInput,
) -> anyhow::Result<i64> {
    let actors_json = serde_json::to_string(&input.actors)?;
    let aliases_json = serde_json::to_string(&input.aliases)?;
    let tags_json = serde_json::to_string(&input.tags)?;
    let raw_json = input.raw_json.to_string();
    let content_hash = content_hash(input)?;
    let row = sqlx::query(
        "INSERT INTO metadata_source_record(media_id,provider_key,provider_entity_id,source_url,record_kind,evidence_level,priority,normalized_code,title,original_title,summary,release_date,duration_minutes,poster_url,backdrop_url,actors_json,aliases_json,tags_json,raw_json,content_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(provider_key,provider_entity_id) DO UPDATE SET media_id=excluded.media_id,source_url=COALESCE(excluded.source_url,metadata_source_record.source_url),record_kind=CASE WHEN excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.record_kind ELSE metadata_source_record.record_kind END,evidence_level=MAX(metadata_source_record.evidence_level,excluded.evidence_level),priority=excluded.priority,normalized_code=excluded.normalized_code,title=CASE WHEN excluded.title IS NOT NULL AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.title ELSE COALESCE(metadata_source_record.title,excluded.title) END,original_title=CASE WHEN excluded.original_title IS NOT NULL AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.original_title ELSE COALESCE(metadata_source_record.original_title,excluded.original_title) END,summary=CASE WHEN excluded.summary IS NOT NULL AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.summary ELSE COALESCE(metadata_source_record.summary,excluded.summary) END,release_date=CASE WHEN excluded.release_date IS NOT NULL AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.release_date ELSE COALESCE(metadata_source_record.release_date,excluded.release_date) END,duration_minutes=CASE WHEN excluded.duration_minutes IS NOT NULL AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.duration_minutes ELSE COALESCE(metadata_source_record.duration_minutes,excluded.duration_minutes) END,poster_url=CASE WHEN excluded.poster_url IS NOT NULL AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.poster_url ELSE COALESCE(metadata_source_record.poster_url,excluded.poster_url) END,backdrop_url=CASE WHEN excluded.backdrop_url IS NOT NULL AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.backdrop_url ELSE COALESCE(metadata_source_record.backdrop_url,excluded.backdrop_url) END,actors_json=CASE WHEN excluded.evidence_level>=2 AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.actors_json WHEN json_array_length(metadata_source_record.actors_json)=0 THEN excluded.actors_json ELSE metadata_source_record.actors_json END,aliases_json=CASE WHEN json_array_length(excluded.aliases_json)>0 THEN excluded.aliases_json ELSE metadata_source_record.aliases_json END,tags_json=CASE WHEN excluded.evidence_level>=2 AND excluded.evidence_level>=metadata_source_record.evidence_level THEN excluded.tags_json WHEN json_array_length(metadata_source_record.tags_json)=0 THEN excluded.tags_json ELSE metadata_source_record.tags_json END,raw_json=excluded.raw_json,content_hash=excluded.content_hash,last_seen_at=datetime('now'),updated_at=CASE WHEN metadata_source_record.content_hash<>excluded.content_hash THEN datetime('now') ELSE metadata_source_record.updated_at END RETURNING id",
    )
    .bind(input.media_id)
    .bind(input.provider_key.trim())
    .bind(input.provider_entity_id.trim())
    .bind(trimmed(input.source_url.as_deref()))
    .bind(input.record_kind.trim())
    .bind(input.evidence_level.clamp(0, 10))
    .bind(input.priority.clamp(-1000, 1000))
    .bind(input.normalized_code.trim())
    .bind(trimmed(input.title.as_deref()))
    .bind(trimmed(input.original_title.as_deref()))
    .bind(trimmed(input.summary.as_deref()))
    .bind(trimmed(input.release_date.as_deref()))
    .bind(input.duration_minutes.filter(|value| *value > 0))
    .bind(trimmed(input.poster_url.as_deref()))
    .bind(trimmed(input.backdrop_url.as_deref()))
    .bind(actors_json)
    .bind(aliases_json)
    .bind(tags_json)
    .bind(raw_json)
    .bind(content_hash)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

fn trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn content_hash(input: &MetadataSourceInput) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(input)?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
