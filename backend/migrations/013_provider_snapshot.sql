CREATE TABLE provider_raw_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_key TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    provider_entity_id TEXT,
    media_id INTEGER,
    source_url TEXT NOT NULL,
    final_url TEXT,
    fetch_mode TEXT NOT NULL CHECK(fetch_mode IN ('http','browser','auto')),
    http_status INTEGER,
    content_type TEXT,
    body_hash TEXT NOT NULL,
    body_path TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(raw_json)),
    parser_version TEXT NOT NULL DEFAULT '1',
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE SET NULL
);

CREATE INDEX idx_provider_raw_snapshot_entity
ON provider_raw_snapshot(provider_key, entity_type, provider_entity_id, fetched_at DESC);

CREATE INDEX idx_provider_raw_snapshot_media
ON provider_raw_snapshot(media_id, fetched_at DESC) WHERE media_id IS NOT NULL;

CREATE INDEX idx_provider_raw_snapshot_hash
ON provider_raw_snapshot(body_hash, fetched_at DESC);
