use serde_json::Value;
use sqlx::{Sqlite, Transaction};

pub(super) async fn select_field(
    transaction: &mut Transaction<'_, Sqlite>,
    media_id: i64,
    field_name: &str,
    source_record_id: i64,
    provider_key: &str,
    priority: i64,
    source_updated_at: &str,
    value: &Value,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO metadata_field_provenance(media_id,field_name,provider_key,source_record_id,priority,value_json,source_updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(media_id,field_name) DO UPDATE SET provider_key=excluded.provider_key,source_record_id=excluded.source_record_id,priority=excluded.priority,value_json=excluded.value_json,source_updated_at=excluded.source_updated_at,selected_at=datetime('now')")
        .bind(media_id)
        .bind(field_name)
        .bind(provider_key)
        .bind(source_record_id)
        .bind(priority)
        .bind(value.to_string())
        .bind(source_updated_at)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}
