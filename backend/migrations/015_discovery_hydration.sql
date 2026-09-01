CREATE TABLE provider_discovery_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_key TEXT NOT NULL,
    provider_entity_id TEXT NOT NULL,
    normalized_code TEXT,
    source_url TEXT NOT NULL,
    release_date TEXT,
    title_hint TEXT,
    poster_hint TEXT,
    content_hash TEXT NOT NULL,
    last_hydrated_hash TEXT,
    discovered_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_hydrated_at TEXT,
    hydration_status TEXT NOT NULL DEFAULT 'pending'
        CHECK(hydration_status IN ('pending','running','hydrated','failed')),
    hydration_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    media_id INTEGER,
    UNIQUE(provider_key, provider_entity_id),
    FOREIGN KEY (provider_key) REFERENCES provider_config(provider_key) ON DELETE CASCADE,
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE SET NULL
);

CREATE INDEX idx_provider_discovery_hydration
ON provider_discovery_item(provider_key, hydration_status, last_seen_at DESC);

CREATE INDEX idx_provider_discovery_media
ON provider_discovery_item(media_id, last_seen_at DESC) WHERE media_id IS NOT NULL;

ALTER TABLE source_sync_state ADD COLUMN active_mode TEXT NOT NULL DEFAULT 'incremental'
CHECK(active_mode IN ('bootstrap','incremental','on_demand'));
ALTER TABLE source_sync_state ADD COLUMN overlap_days INTEGER NOT NULL DEFAULT 3;
ALTER TABLE source_sync_state ADD COLUMN bootstrap_paused INTEGER NOT NULL DEFAULT 0
CHECK(bootstrap_paused IN (0,1));
ALTER TABLE source_sync_state ADD COLUMN bootstrap_from TEXT;
ALTER TABLE source_sync_state ADD COLUMN bootstrap_to TEXT;
ALTER TABLE source_sync_state ADD COLUMN discovery_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_sync_state ADD COLUMN hydrated_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_sync_state ADD COLUMN hydration_failed_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_sync_state ADD COLUMN pending_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE source_sync_run ADD COLUMN sync_mode TEXT NOT NULL DEFAULT 'incremental'
CHECK(sync_mode IN ('bootstrap','incremental','on_demand'));
ALTER TABLE source_sync_run ADD COLUMN discovery_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_sync_run ADD COLUMN hydrated_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_sync_run ADD COLUMN hydration_failed_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_sync_run ADD COLUMN pending_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_sync_run ADD COLUMN window_from TEXT;
ALTER TABLE source_sync_run ADD COLUMN window_to TEXT;

CREATE INDEX idx_source_sync_run_mode
ON source_sync_run(provider_key, sync_mode, id DESC);

CREATE TABLE source_sync_page (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    page_number INTEGER NOT NULL,
    page_url TEXT NOT NULL,
    item_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    cursor_after_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(cursor_after_json)),
    completed_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(run_id, page_number),
    FOREIGN KEY (run_id) REFERENCES source_sync_run(id) ON DELETE CASCADE
);

CREATE INDEX idx_source_sync_page_run
ON source_sync_page(run_id, page_number);
