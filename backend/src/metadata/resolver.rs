use std::collections::HashSet;

use serde_json::json;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use super::{LocalizedAlias, ResolutionResult, SourceActor, provenance};

#[derive(Debug)]
struct SourceRecord {
    id: i64,
    provider_key: String,
    priority: i64,
    normalized_code: String,
    title: Option<String>,
    original_title: Option<String>,
    summary: Option<String>,
    release_date: Option<String>,
    duration_minutes: Option<i64>,
    poster_url: Option<String>,
    backdrop_url: Option<String>,
    actors: Vec<SourceActor>,
    aliases: Vec<LocalizedAlias>,
    tags: Vec<String>,
    updated_at: String,
}

#[derive(Debug, Clone)]
struct Selection<T> {
    value: T,
    source_record_id: i64,
    provider_key: String,
    priority: i64,
    source_updated_at: String,
}

pub(super) async fn resolve_media(
    pool: &SqlitePool,
    media_id: i64,
    source_record_id: i64,
) -> anyhow::Result<ResolutionResult> {
    let rows = sqlx::query("SELECT id,provider_key,priority,normalized_code,title,original_title,summary,release_date,duration_minutes,poster_url,backdrop_url,actors_json,aliases_json,tags_json,updated_at FROM metadata_source_record WHERE media_id=? ORDER BY priority DESC,provider_key,id")
        .bind(media_id)
        .fetch_all(pool)
        .await?;
    let records = rows
        .into_iter()
        .map(|row| SourceRecord {
            id: row.get("id"),
            provider_key: row.get("provider_key"),
            priority: row.get("priority"),
            normalized_code: row.get("normalized_code"),
            title: row.get("title"),
            original_title: row.get("original_title"),
            summary: row.get("summary"),
            release_date: row.get("release_date"),
            duration_minutes: row.get("duration_minutes"),
            poster_url: row.get("poster_url"),
            backdrop_url: row.get("backdrop_url"),
            actors: parse_json_vec(&row.get::<String, _>("actors_json")),
            aliases: parse_json_vec(&row.get::<String, _>("aliases_json")),
            tags: parse_json_vec(&row.get::<String, _>("tags_json")),
            updated_at: row.get("updated_at"),
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(!records.is_empty(), "media has no metadata source records");

    let title = select_text(
        &records,
        |record| record.title.as_deref(),
        |record, value| {
            if normalize_key(value) == normalize_key(&record.normalized_code) {
                0
            } else {
                20 + value.chars().take(200).count() as i64
            }
        },
    );
    let original_title = select_text(
        &records,
        |record| record.original_title.as_deref(),
        |_, value| 10 + value.chars().take(200).count() as i64,
    );
    let summary = select_text(
        &records,
        |record| record.summary.as_deref(),
        |_, value| value.chars().take(2000).count() as i64,
    );
    let release_date = select_text(
        &records,
        |record| record.release_date.as_deref(),
        |_, value| if valid_iso_date(value) { 100 } else { 0 },
    );
    let poster_url = select_text(
        &records,
        |record| record.poster_url.as_deref(),
        |_, value| url_quality(value),
    );
    let backdrop_url = select_text(
        &records,
        |record| record.backdrop_url.as_deref(),
        |_, value| url_quality(value),
    );
    let duration = select_number(
        &records,
        |record| record.duration_minutes,
        |value| {
            if (1..=1440).contains(&value) { 100 } else { 0 }
        },
    );

    let mut transaction = pool.begin().await?;
    let has_actors = records.iter().any(|record| !record.actors.is_empty());
    let metadata_status =
        if title.is_some() && release_date.is_some() && poster_url.is_some() && has_actors {
            "full"
        } else {
            "partial"
        };
    sqlx::query("UPDATE media SET title=COALESCE(?,title),original_title=COALESCE(?,original_title),summary=COALESCE(?,summary),release_date=COALESCE(?,release_date),duration_minutes=COALESCE(?,duration_minutes),poster_url=COALESCE(?,poster_url),backdrop_url=COALESCE(?,backdrop_url),metadata_status=?,updated_at=datetime('now') WHERE id=?")
        .bind(title.as_ref().map(|selected| selected.value.as_str()))
        .bind(original_title.as_ref().map(|selected| selected.value.as_str()))
        .bind(summary.as_ref().map(|selected| selected.value.as_str()))
        .bind(release_date.as_ref().map(|selected| selected.value.as_str()))
        .bind(duration.as_ref().map(|selected| selected.value))
        .bind(poster_url.as_ref().map(|selected| selected.value.as_str()))
        .bind(backdrop_url.as_ref().map(|selected| selected.value.as_str()))
        .bind(metadata_status)
        .bind(media_id)
        .execute(&mut *transaction)
        .await?;

    persist_text_selection(&mut transaction, media_id, "title", title.as_ref()).await?;
    persist_text_selection(
        &mut transaction,
        media_id,
        "original_title",
        original_title.as_ref(),
    )
    .await?;
    persist_text_selection(&mut transaction, media_id, "summary", summary.as_ref()).await?;
    persist_text_selection(
        &mut transaction,
        media_id,
        "release_date",
        release_date.as_ref(),
    )
    .await?;
    persist_number_selection(
        &mut transaction,
        media_id,
        "duration_minutes",
        duration.as_ref(),
    )
    .await?;
    persist_text_selection(
        &mut transaction,
        media_id,
        "poster_url",
        poster_url.as_ref(),
    )
    .await?;
    persist_text_selection(
        &mut transaction,
        media_id,
        "backdrop_url",
        backdrop_url.as_ref(),
    )
    .await?;

    persist_aliases_and_tags(&mut transaction, media_id, &records).await?;
    let actor_ids = persist_actors(&mut transaction, media_id, &records).await?;
    refresh_search_documents(&mut transaction, media_id, &actor_ids).await?;
    transaction.commit().await?;
    Ok(ResolutionResult {
        media_id,
        source_record_id,
        actor_ids,
    })
}

fn select_text(
    records: &[SourceRecord],
    value: impl Fn(&SourceRecord) -> Option<&str>,
    quality: impl Fn(&SourceRecord, &str) -> i64,
) -> Option<Selection<String>> {
    let mut candidates = records
        .iter()
        .filter_map(|record| {
            let value = value(record)?.trim();
            (!value.is_empty()).then(|| (record, value.to_owned(), quality(record, value)))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .0
            .priority
            .cmp(&left.0.priority)
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| left.0.provider_key.cmp(&right.0.provider_key))
            .then_with(|| left.0.id.cmp(&right.0.id))
    });
    candidates
        .into_iter()
        .next()
        .map(|(record, value, _)| selection(record, value))
}

fn select_number(
    records: &[SourceRecord],
    value: impl Fn(&SourceRecord) -> Option<i64>,
    quality: impl Fn(i64) -> i64,
) -> Option<Selection<i64>> {
    let mut candidates = records
        .iter()
        .filter_map(|record| value(record).map(|value| (record, value, quality(value))))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .0
            .priority
            .cmp(&left.0.priority)
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| left.0.provider_key.cmp(&right.0.provider_key))
            .then_with(|| left.0.id.cmp(&right.0.id))
    });
    candidates
        .into_iter()
        .next()
        .map(|(record, value, _)| selection(record, value))
}

fn selection<T>(record: &SourceRecord, value: T) -> Selection<T> {
    Selection {
        value,
        source_record_id: record.id,
        provider_key: record.provider_key.clone(),
        priority: record.priority,
        source_updated_at: record.updated_at.clone(),
    }
}

async fn persist_text_selection(
    transaction: &mut Transaction<'_, Sqlite>,
    media_id: i64,
    field_name: &str,
    selected: Option<&Selection<String>>,
) -> anyhow::Result<()> {
    if let Some(selected) = selected {
        provenance::select_field(
            transaction,
            media_id,
            field_name,
            selected.source_record_id,
            &selected.provider_key,
            selected.priority,
            &selected.source_updated_at,
            &json!(selected.value),
        )
        .await?;
    }
    Ok(())
}

async fn persist_number_selection(
    transaction: &mut Transaction<'_, Sqlite>,
    media_id: i64,
    field_name: &str,
    selected: Option<&Selection<i64>>,
) -> anyhow::Result<()> {
    if let Some(selected) = selected {
        provenance::select_field(
            transaction,
            media_id,
            field_name,
            selected.source_record_id,
            &selected.provider_key,
            selected.priority,
            &selected.source_updated_at,
            &json!(selected.value),
        )
        .await?;
    }
    Ok(())
}

async fn persist_aliases_and_tags(
    transaction: &mut Transaction<'_, Sqlite>,
    media_id: i64,
    records: &[SourceRecord],
) -> anyhow::Result<()> {
    for record in records {
        sqlx::query("DELETE FROM metadata_alias_provenance WHERE source_record_id=?")
            .bind(record.id)
            .execute(&mut **transaction)
            .await?;
        let mut aliases = record.aliases.clone();
        if let Some(title) = record.title.as_deref() {
            aliases.push(LocalizedAlias::new(title, detect_locale(title)));
        }
        if let Some(title) = record.original_title.as_deref() {
            aliases.push(LocalizedAlias::new(title, detect_locale(title)));
        }
        let mut seen = HashSet::new();
        for localized_alias in aliases {
            let alias = localized_alias.value.trim();
            let normalized = normalize_key(alias);
            if normalized.is_empty() || !seen.insert(normalized.clone()) {
                continue;
            }
            let locale =
                valid_locale(&localized_alias.locale).unwrap_or_else(|| detect_locale(alias));
            let is_primary = record
                .title
                .as_deref()
                .is_some_and(|title| normalize_key(title) == normalized);
            sqlx::query("INSERT INTO media_title_alias(media_id,locale,alias,normalized_alias,source_key,is_primary) VALUES (?,?,?,?,?,?) ON CONFLICT(media_id,normalized_alias) DO UPDATE SET alias=excluded.alias,locale=CASE WHEN media_title_alias.locale='und' THEN excluded.locale ELSE media_title_alias.locale END,is_primary=MAX(media_title_alias.is_primary,excluded.is_primary),updated_at=datetime('now')")
                .bind(media_id)
                .bind(&locale)
                .bind(alias)
                .bind(&normalized)
                .bind(&record.provider_key)
                .bind(is_primary)
                .execute(&mut **transaction)
                .await?;
            sqlx::query("INSERT INTO metadata_alias_provenance(media_id,normalized_alias,provider_key,source_record_id,alias,locale,is_primary) VALUES (?,?,?,?,?,?,?) ON CONFLICT(media_id,normalized_alias,provider_key) DO UPDATE SET source_record_id=excluded.source_record_id,alias=excluded.alias,locale=excluded.locale,is_primary=excluded.is_primary,last_seen_at=datetime('now')")
                .bind(media_id)
                .bind(&normalized)
                .bind(&record.provider_key)
                .bind(record.id)
                .bind(alias)
                .bind(&locale)
                .bind(is_primary)
                .execute(&mut **transaction)
                .await?;
        }

        sqlx::query("DELETE FROM metadata_tag_provenance WHERE source_record_id=?")
            .bind(record.id)
            .execute(&mut **transaction)
            .await?;
        for tag in &record.tags {
            let tag = tag.trim();
            let normalized = normalize_key(tag);
            if normalized.is_empty() {
                continue;
            }
            sqlx::query("INSERT INTO media_tag(media_id,normalized_tag,tag) VALUES (?,?,?) ON CONFLICT(media_id,normalized_tag) DO UPDATE SET tag=excluded.tag,updated_at=datetime('now')")
                .bind(media_id)
                .bind(&normalized)
                .bind(tag)
                .execute(&mut **transaction)
                .await?;
            sqlx::query("INSERT INTO metadata_tag_provenance(media_id,normalized_tag,provider_key,source_record_id,tag) VALUES (?,?,?,?,?) ON CONFLICT(media_id,normalized_tag,provider_key) DO UPDATE SET source_record_id=excluded.source_record_id,tag=excluded.tag,last_seen_at=datetime('now')")
                .bind(media_id)
                .bind(&normalized)
                .bind(&record.provider_key)
                .bind(record.id)
                .bind(tag)
                .execute(&mut **transaction)
                .await?;
        }
    }
    sqlx::query("DELETE FROM media_tag WHERE media_id=? AND NOT EXISTS(SELECT 1 FROM metadata_tag_provenance provenance WHERE provenance.media_id=media_tag.media_id AND provenance.normalized_tag=media_tag.normalized_tag)")
        .bind(media_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn persist_actors(
    transaction: &mut Transaction<'_, Sqlite>,
    media_id: i64,
    records: &[SourceRecord],
) -> anyhow::Result<Vec<i64>> {
    let mut actor_ids = HashSet::new();
    for record in records {
        sqlx::query("DELETE FROM media_actor_source WHERE source_record_id=?")
            .bind(record.id)
            .execute(&mut **transaction)
            .await?;
        sqlx::query("DELETE FROM actor_alias_provenance WHERE source_record_id=?")
            .bind(record.id)
            .execute(&mut **transaction)
            .await?;
        for (order, source_actor) in record.actors.iter().enumerate() {
            let name = source_actor.name.trim();
            if name.is_empty() {
                continue;
            }
            let actor_id = resolve_actor(transaction, record, source_actor).await?;
            actor_ids.insert(actor_id);
            sqlx::query(
                "INSERT OR IGNORE INTO media_actor(media_id,actor_id,billing_order) VALUES (?,?,?)",
            )
            .bind(media_id)
            .bind(actor_id)
            .bind(order as i64)
            .execute(&mut **transaction)
            .await?;
            sqlx::query("INSERT INTO media_actor_source(media_id,actor_id,provider_key,provider_actor_id,source_record_id,source_name,billing_order) VALUES (?,?,?,?,?,?,?) ON CONFLICT(source_record_id,provider_actor_id,source_name) DO UPDATE SET actor_id=excluded.actor_id,billing_order=excluded.billing_order,last_seen_at=datetime('now')")
                .bind(media_id)
                .bind(actor_id)
                .bind(&record.provider_key)
                .bind(source_actor.provider_actor_id.as_deref().unwrap_or_default())
                .bind(record.id)
                .bind(name)
                .bind(order as i64)
                .execute(&mut **transaction)
                .await?;
        }
    }
    let mut actor_ids = actor_ids.into_iter().collect::<Vec<_>>();
    actor_ids.sort_unstable();
    Ok(actor_ids)
}

async fn resolve_actor(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &SourceRecord,
    source_actor: &SourceActor,
) -> anyhow::Result<i64> {
    let primary_key = normalize_key(&source_actor.name);
    let mapped = if let Some(provider_actor_id) = source_actor
        .provider_actor_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        sqlx::query_scalar("SELECT actor_id FROM provider_entity_mapping WHERE provider_key=? AND entity_type='actor' AND provider_entity_id=?")
            .bind(&record.provider_key)
            .bind(provider_actor_id)
            .fetch_optional(&mut **transaction)
            .await?
    } else {
        None
    };
    let by_primary = if mapped.is_none() {
        sqlx::query_scalar("SELECT id FROM actor WHERE normalized_name=?")
            .bind(&primary_key)
            .fetch_optional(&mut **transaction)
            .await?
    } else {
        None
    };
    let by_alias = if mapped.is_none() && by_primary.is_none() {
        let mut actor_id = None;
        for alias in std::iter::once(LocalizedAlias::new(&source_actor.name, "und"))
            .chain(source_actor.aliases.clone())
        {
            actor_id = sqlx::query_scalar("SELECT actor_id FROM actor_name_alias WHERE normalized_alias=? ORDER BY is_primary DESC,id LIMIT 1")
                .bind(normalize_key(&alias.value))
                .fetch_optional(&mut **transaction)
                .await?;
            if actor_id.is_some() {
                break;
            }
        }
        actor_id
    } else {
        None
    };
    let actor_id = if let Some(actor_id) = mapped.or(by_primary).or(by_alias) {
        sqlx::query("UPDATE actor SET avatar_url=COALESCE(avatar_url,?),updated_at=datetime('now') WHERE id=?")
            .bind(source_actor.avatar_url.as_deref())
            .bind(actor_id)
            .execute(&mut **transaction)
            .await?;
        actor_id
    } else {
        sqlx::query_scalar(
            "INSERT INTO actor(normalized_name,name,avatar_url) VALUES (?,?,?) RETURNING id",
        )
        .bind(&primary_key)
        .bind(source_actor.name.trim())
        .bind(source_actor.avatar_url.as_deref())
        .fetch_one(&mut **transaction)
        .await?
    };

    if let Some(provider_actor_id) = source_actor
        .provider_actor_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        sqlx::query("INSERT INTO provider_entity_mapping(provider_key,entity_type,provider_entity_id,actor_id) VALUES (?,'actor',?,?) ON CONFLICT(provider_key,entity_type,provider_entity_id) DO UPDATE SET actor_id=excluded.actor_id,last_seen_at=datetime('now')")
            .bind(&record.provider_key)
            .bind(provider_actor_id)
            .bind(actor_id)
            .execute(&mut **transaction)
            .await?;
    }

    let mut aliases = vec![LocalizedAlias::new(
        &source_actor.name,
        detect_locale(&source_actor.name),
    )];
    aliases.extend(source_actor.aliases.clone());
    let mut seen = HashSet::new();
    for (index, alias) in aliases.into_iter().enumerate() {
        let alias = alias.value.trim();
        let normalized = normalize_key(alias);
        if normalized.is_empty() || !seen.insert(normalized.clone()) {
            continue;
        }
        let locale = valid_locale(&alias_locale(&alias, &source_actor.aliases, index))
            .unwrap_or_else(|| detect_locale(alias));
        let is_primary = index == 0;
        sqlx::query("INSERT INTO actor_name_alias(actor_id,locale,alias,normalized_alias,source_key,is_primary) VALUES (?,?,?,?,?,?) ON CONFLICT(actor_id,normalized_alias) DO UPDATE SET alias=excluded.alias,locale=CASE WHEN actor_name_alias.locale='und' THEN excluded.locale ELSE actor_name_alias.locale END,is_primary=MAX(actor_name_alias.is_primary,excluded.is_primary),updated_at=datetime('now')")
            .bind(actor_id)
            .bind(&locale)
            .bind(alias)
            .bind(&normalized)
            .bind(&record.provider_key)
            .bind(is_primary)
            .execute(&mut **transaction)
            .await?;
        sqlx::query("INSERT INTO actor_alias_provenance(actor_id,normalized_alias,provider_key,source_record_id,alias,locale,is_primary) VALUES (?,?,?,?,?,?,?) ON CONFLICT(actor_id,normalized_alias,provider_key) DO UPDATE SET source_record_id=excluded.source_record_id,alias=excluded.alias,locale=excluded.locale,is_primary=excluded.is_primary,last_seen_at=datetime('now')")
            .bind(actor_id)
            .bind(&normalized)
            .bind(&record.provider_key)
            .bind(record.id)
            .bind(alias)
            .bind(&locale)
            .bind(is_primary)
            .execute(&mut **transaction)
            .await?;
    }
    let aliases_json: String = sqlx::query_scalar("SELECT COALESCE(json_group_array(alias),'[]') FROM actor_name_alias WHERE actor_id=? AND is_primary=0")
        .bind(actor_id)
        .fetch_one(&mut **transaction)
        .await?;
    sqlx::query("UPDATE actor SET aliases_json=?,updated_at=datetime('now') WHERE id=?")
        .bind(aliases_json)
        .bind(actor_id)
        .execute(&mut **transaction)
        .await?;
    Ok(actor_id)
}

fn alias_locale(_alias: &str, aliases: &[LocalizedAlias], index: usize) -> String {
    if index == 0 {
        "und".into()
    } else {
        aliases
            .get(index - 1)
            .map(|alias| alias.locale.clone())
            .unwrap_or_else(|| "und".into())
    }
}

async fn refresh_search_documents(
    transaction: &mut Transaction<'_, Sqlite>,
    media_id: i64,
    actor_ids: &[i64],
) -> anyhow::Result<()> {
    for actor_id in actor_ids {
        sqlx::query("INSERT INTO actor_search_document(actor_id,name,aliases,updated_at) SELECT actor.id,actor.name,COALESCE((SELECT group_concat(alias,' ') FROM actor_name_alias WHERE actor_id=actor.id),''),datetime('now') FROM actor WHERE actor.id=? ON CONFLICT(actor_id) DO UPDATE SET name=excluded.name,aliases=excluded.aliases,updated_at=datetime('now')")
            .bind(actor_id)
            .execute(&mut **transaction)
            .await?;
    }
    sqlx::query("INSERT INTO media_search_document(media_id,code,title,original_title,aliases,actors,resources,updated_at) SELECT media.id,media.normalized_code,media.title,COALESCE(media.original_title,''),COALESCE((SELECT group_concat(alias,' ') FROM media_title_alias WHERE media_id=media.id),''),COALESCE((SELECT group_concat(actor_text,' ') FROM (SELECT actor.name || ' ' || COALESCE((SELECT group_concat(alias,' ') FROM actor_name_alias WHERE actor_id=actor.id),'') AS actor_text FROM media_actor JOIN actor ON actor.id=media_actor.actor_id WHERE media_actor.media_id=media.id)),''),COALESCE((SELECT group_concat(title,' ') FROM resource WHERE media_id=media.id AND available=1),''),datetime('now') FROM media WHERE media.id=? ON CONFLICT(media_id) DO UPDATE SET code=excluded.code,title=excluded.title,original_title=excluded.original_title,aliases=excluded.aliases,actors=excluded.actors,resources=excluded.resources,updated_at=datetime('now')")
        .bind(media_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn parse_json_vec<T: serde::de::DeserializeOwned>(value: &str) -> Vec<T> {
    serde_json::from_str(value).unwrap_or_default()
}

fn normalize_key(value: &str) -> String {
    value
        .trim()
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn valid_iso_date(value: &str) -> bool {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

fn url_quality(value: &str) -> i64 {
    match reqwest::Url::parse(value) {
        Ok(url) if url.scheme() == "https" => 100,
        Ok(url) if url.scheme() == "http" => 50,
        _ => 0,
    }
}

fn valid_locale(value: &str) -> Option<String> {
    matches!(value, "ja" | "zh" | "en" | "und").then(|| value.to_owned())
}

fn detect_locale(value: &str) -> String {
    if value
        .chars()
        .any(|character| matches!(character, '\u{3040}'..='\u{30ff}' | '\u{31f0}'..='\u{31ff}'))
    {
        "ja".into()
    } else if value
        .chars()
        .any(|character| matches!(character, '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}'))
    {
        "zh".into()
    } else if value.chars().any(|character| character.is_alphabetic()) {
        "en".into()
    } else {
        "und".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MetadataSourceInput, record_and_resolve};

    async fn setup() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    async fn media(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO media(normalized_code,title) VALUES ('abc-123','ABC-123') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn lower_priority_provider_cannot_overwrite_canonical_fields() {
        let pool = setup().await;
        let media_id = media(&pool).await;
        let mut preferred =
            MetadataSourceInput::catalogue(media_id, "provider-a", "a-1", "abc-123");
        preferred.record_kind = "detail".into();
        preferred.evidence_level = 2;
        preferred.priority = 200;
        preferred.title = Some("Preferred title".into());
        preferred.summary = Some("Authoritative summary".into());
        record_and_resolve(&pool, &preferred).await.unwrap();

        let mut fallback = MetadataSourceInput::catalogue(media_id, "provider-b", "b-1", "abc-123");
        fallback.record_kind = "detail".into();
        fallback.evidence_level = 2;
        fallback.priority = 50;
        fallback.title = Some("A much longer but lower priority title".into());
        fallback.summary = Some("Fallback".into());
        record_and_resolve(&pool, &fallback).await.unwrap();

        let row = sqlx::query("SELECT title,summary FROM media WHERE id=?")
            .bind(media_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("title"), "Preferred title");
        assert_eq!(row.get::<String, _>("summary"), "Authoritative summary");
        let provider: String = sqlx::query_scalar("SELECT provider_key FROM metadata_field_provenance WHERE media_id=? AND field_name='title'")
            .bind(media_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(provider, "provider-a");
    }

    #[tokio::test]
    async fn aliases_tags_and_actors_are_merged_with_provenance() {
        let pool = setup().await;
        let media_id = media(&pool).await;
        let mut first = MetadataSourceInput::catalogue(media_id, "provider-a", "a-1", "abc-123");
        first.evidence_level = 2;
        first.title = Some("日本語タイトル".into());
        first.aliases = vec![LocalizedAlias::new("Japanese title", "en")];
        first.tags = vec!["Drama".into()];
        first.actors = vec![SourceActor {
            provider_actor_id: Some("actor-a".into()),
            name: "紗倉まな".into(),
            aliases: vec![LocalizedAlias::new("Mana Sakura", "en")],
            avatar_url: None,
        }];
        record_and_resolve(&pool, &first).await.unwrap();

        let mut second = MetadataSourceInput::catalogue(media_id, "provider-b", "b-1", "abc-123");
        second.evidence_level = 2;
        second.title = Some("中文标题".into());
        second.aliases = vec![LocalizedAlias::new("Chinese title", "en")];
        second.tags = vec!["4K".into()];
        second.actors = vec![SourceActor {
            provider_actor_id: Some("actor-b".into()),
            name: "Mana Sakura".into(),
            aliases: vec![LocalizedAlias::new("紗倉まな", "ja")],
            avatar_url: None,
        }];
        record_and_resolve(&pool, &second).await.unwrap();

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM actor")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM media_title_alias WHERE media_id=?")
                .bind(media_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            4
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM media_tag WHERE media_id=?")
                .bind(media_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM metadata_alias_provenance WHERE media_id=?"
            )
            .bind(media_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            4
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM media_actor_source WHERE media_id=?"
            )
            .bind(media_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            2
        );
    }
}
