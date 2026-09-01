CREATE TABLE metadata_source_record (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    provider_key TEXT NOT NULL,
    provider_entity_id TEXT NOT NULL,
    source_url TEXT,
    record_kind TEXT NOT NULL DEFAULT 'detail',
    evidence_level INTEGER NOT NULL DEFAULT 1,
    priority INTEGER NOT NULL DEFAULT 100,
    normalized_code TEXT NOT NULL,
    title TEXT,
    original_title TEXT,
    summary TEXT,
    release_date TEXT,
    duration_minutes INTEGER,
    poster_url TEXT,
    backdrop_url TEXT,
    actors_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(actors_json)),
    aliases_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(aliases_json)),
    tags_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(tags_json)),
    raw_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(raw_json)),
    content_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(provider_key, provider_entity_id),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE INDEX idx_metadata_source_media_priority
ON metadata_source_record(media_id, priority DESC, provider_key);

CREATE INDEX idx_metadata_source_code
ON metadata_source_record(normalized_code, provider_key);

CREATE TABLE metadata_field_provenance (
    media_id INTEGER NOT NULL,
    field_name TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    source_record_id INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    value_json TEXT NOT NULL CHECK(json_valid(value_json)),
    source_updated_at TEXT NOT NULL,
    selected_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (media_id, field_name),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (source_record_id) REFERENCES metadata_source_record(id) ON DELETE CASCADE
);

CREATE TABLE metadata_alias_provenance (
    media_id INTEGER NOT NULL,
    normalized_alias TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    source_record_id INTEGER NOT NULL,
    alias TEXT NOT NULL,
    locale TEXT NOT NULL DEFAULT 'und',
    is_primary INTEGER NOT NULL DEFAULT 0,
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (media_id, normalized_alias, provider_key),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (source_record_id) REFERENCES metadata_source_record(id) ON DELETE CASCADE
);

CREATE TABLE media_actor_source (
    media_id INTEGER NOT NULL,
    actor_id INTEGER NOT NULL,
    provider_key TEXT NOT NULL,
    provider_actor_id TEXT NOT NULL DEFAULT '',
    source_record_id INTEGER NOT NULL,
    source_name TEXT NOT NULL,
    billing_order INTEGER NOT NULL DEFAULT 0,
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source_record_id, provider_actor_id, source_name),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (actor_id) REFERENCES actor(id) ON DELETE CASCADE,
    FOREIGN KEY (source_record_id) REFERENCES metadata_source_record(id) ON DELETE CASCADE
);

CREATE INDEX idx_media_actor_source_actor
ON media_actor_source(actor_id, media_id);

CREATE TABLE actor_alias_provenance (
    actor_id INTEGER NOT NULL,
    normalized_alias TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    source_record_id INTEGER NOT NULL,
    alias TEXT NOT NULL,
    locale TEXT NOT NULL DEFAULT 'und',
    is_primary INTEGER NOT NULL DEFAULT 0,
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (actor_id, normalized_alias, provider_key),
    FOREIGN KEY (actor_id) REFERENCES actor(id) ON DELETE CASCADE,
    FOREIGN KEY (source_record_id) REFERENCES metadata_source_record(id) ON DELETE CASCADE
);

CREATE TABLE media_tag (
    media_id INTEGER NOT NULL,
    normalized_tag TEXT NOT NULL,
    tag TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (media_id, normalized_tag),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE TABLE metadata_tag_provenance (
    media_id INTEGER NOT NULL,
    normalized_tag TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    source_record_id INTEGER NOT NULL,
    tag TEXT NOT NULL,
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (media_id, normalized_tag, provider_key),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (source_record_id) REFERENCES metadata_source_record(id) ON DELETE CASCADE
);

INSERT OR IGNORE INTO metadata_source_record(
    media_id, provider_key, provider_entity_id, record_kind, evidence_level, priority,
    normalized_code, title, original_title, summary, release_date, poster_url, backdrop_url,
    actors_json, aliases_json, tags_json, raw_json, content_hash, first_seen_at, last_seen_at, updated_at
)
SELECT
    legacy.media_id,
    legacy.provider_key,
    COALESCE(NULLIF(legacy.provider_entity_id, ''), 'legacy:' || legacy.id),
    'legacy',
    1,
    50,
    media.normalized_code,
    json_extract(legacy.raw_json, '$.title'),
    COALESCE(json_extract(legacy.raw_json, '$.original_title'), json_extract(legacy.raw_json, '$.number')),
    COALESCE(json_extract(legacy.raw_json, '$.summary'), json_extract(legacy.raw_json, '$.plot')),
    COALESCE(json_extract(legacy.raw_json, '$.release_date'), json_extract(legacy.raw_json, '$.premiered')),
    COALESCE(json_extract(legacy.raw_json, '$.poster_url'), json_extract(legacy.raw_json, '$.cover_url')),
    COALESCE(json_extract(legacy.raw_json, '$.backdrop_url'), json_extract(legacy.raw_json, '$.thumb_url')),
    CASE WHEN json_type(legacy.raw_json, '$.actors') = 'array' THEN json_extract(legacy.raw_json, '$.actors') ELSE '[]' END,
    '[]',
    CASE WHEN json_type(legacy.raw_json, '$.genres') = 'array' THEN json_extract(legacy.raw_json, '$.genres') ELSE '[]' END,
    CASE WHEN json_valid(legacy.raw_json) THEN legacy.raw_json ELSE '{}' END,
    'legacy:' || legacy.id,
    legacy.created_at,
    legacy.created_at,
    legacy.created_at
FROM metadata_record_v2 legacy
JOIN media ON media.id = legacy.media_id
WHERE legacy.status = 'complete';
