PRAGMA foreign_keys = ON;

CREATE TABLE media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    normalized_code TEXT NOT NULL,
    title TEXT NOT NULL,
    original_title TEXT,
    summary TEXT NOT NULL DEFAULT '',
    release_date TEXT,
    duration_minutes INTEGER,
    poster_url TEXT,
    backdrop_url TEXT,
    media_type TEXT NOT NULL DEFAULT 'movie',
    metadata_status TEXT NOT NULL DEFAULT 'partial',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_media_normalized_code ON media(normalized_code);
CREATE INDEX idx_media_updated ON media(updated_at DESC);

CREATE TABLE actor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    normalized_name TEXT NOT NULL,
    name TEXT NOT NULL,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    avatar_url TEXT,
    followed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_actor_normalized_name ON actor(normalized_name);
CREATE INDEX idx_actor_followed ON actor(followed, updated_at DESC);

CREATE TABLE media_actor (
    media_id INTEGER NOT NULL,
    actor_id INTEGER NOT NULL,
    role TEXT NOT NULL DEFAULT 'actor',
    billing_order INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (media_id, actor_id, role),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (actor_id) REFERENCES actor(id) ON DELETE CASCADE
);

CREATE TABLE provider_entity_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_key TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    provider_entity_id TEXT NOT NULL,
    media_id INTEGER,
    actor_id INTEGER,
    source_url TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}',
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (actor_id) REFERENCES actor(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_provider_mapping_identity
    ON provider_entity_mapping(provider_key, entity_type, provider_entity_id);

CREATE TABLE resource (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    provider_key TEXT NOT NULL,
    provider_resource_id TEXT,
    title TEXT NOT NULL,
    download_url TEXT NOT NULL,
    info_hash TEXT,
    size_bytes INTEGER,
    resolution TEXT,
    subtitle_languages_json TEXT NOT NULL DEFAULT '[]',
    trackers_json TEXT NOT NULL DEFAULT '[]',
    published_at TEXT,
    score REAL NOT NULL DEFAULT 0,
    score_reasons_json TEXT NOT NULL DEFAULT '[]',
    available INTEGER NOT NULL DEFAULT 1,
    raw_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE INDEX idx_resource_provider_identity
    ON resource(provider_key, provider_resource_id) WHERE provider_resource_id IS NOT NULL;
CREATE UNIQUE INDEX idx_resource_info_hash
    ON resource(info_hash) WHERE info_hash IS NOT NULL;
CREATE INDEX idx_resource_media_rank ON resource(media_id, available, score DESC, published_at DESC);

CREATE TABLE acquisition (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    resource_id INTEGER,
    requested_by TEXT NOT NULL DEFAULT 'manual',
    automation_execution_id INTEGER,
    state TEXT NOT NULL DEFAULT 'REQUESTED',
    state_message TEXT NOT NULL DEFAULT '获取请求已创建',
    qbit_hash TEXT,
    progress REAL NOT NULL DEFAULT 0,
    download_speed INTEGER NOT NULL DEFAULT 0,
    eta_seconds INTEGER,
    download_path TEXT,
    qbit_missing_since TEXT,
    library_item_id INTEGER,
    last_error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE RESTRICT,
    FOREIGN KEY (resource_id) REFERENCES resource(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX idx_acquisition_active_media ON acquisition(media_id)
WHERE state NOT IN ('COMPLETED', 'CANCELLED', 'NEEDS_ATTENTION');
CREATE INDEX idx_acquisition_state ON acquisition(state, updated_at DESC);

CREATE TABLE acquisition_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    acquisition_id INTEGER NOT NULL,
    event_key TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    message TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (acquisition_id) REFERENCES acquisition(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_acquisition_event_key ON acquisition_event(acquisition_id, event_key);
CREATE INDEX idx_acquisition_event_timeline ON acquisition_event(acquisition_id, id);

CREATE TABLE library_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    acquisition_id INTEGER,
    legacy_media_item_id INTEGER,
    video_path TEXT NOT NULL,
    nfo_path TEXT,
    poster_path TEXT,
    status TEXT NOT NULL DEFAULT 'ready',
    file_size INTEGER,
    added_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE RESTRICT,
    FOREIGN KEY (acquisition_id) REFERENCES acquisition(id) ON DELETE SET NULL,
    FOREIGN KEY (legacy_media_item_id) REFERENCES media_item(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX idx_library_video_path ON library_item(video_path);
CREATE INDEX idx_library_media ON library_item(media_id, added_at DESC);

CREATE TABLE metadata_record_v2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    provider_key TEXT NOT NULL,
    provider_entity_id TEXT,
    status TEXT NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}',
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE TABLE attention_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'warning',
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    acquisition_id INTEGER,
    media_id INTEGER,
    status TEXT NOT NULL DEFAULT 'open',
    actions_json TEXT NOT NULL DEFAULT '[]',
    resolution_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT,
    FOREIGN KEY (acquisition_id) REFERENCES acquisition(id) ON DELETE CASCADE,
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE INDEX idx_attention_open ON attention_item(status, severity, created_at DESC);
CREATE UNIQUE INDEX idx_attention_active_acquisition
    ON attention_item(acquisition_id, kind) WHERE status = 'open' AND acquisition_id IS NOT NULL;

CREATE TABLE automation_rule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    trigger_type TEXT NOT NULL,
    trigger_config_json TEXT NOT NULL DEFAULT '{}',
    conditions_json TEXT NOT NULL DEFAULT '[]',
    action_type TEXT NOT NULL DEFAULT 'ACQUIRE',
    action_config_json TEXT NOT NULL DEFAULT '{}',
    mode TEXT NOT NULL DEFAULT 'CONFIRM',
    last_run_at TEXT,
    next_run_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE automation_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER NOT NULL,
    event_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    explanation TEXT NOT NULL DEFAULT '',
    media_id INTEGER,
    resource_id INTEGER,
    acquisition_id INTEGER,
    input_json TEXT NOT NULL DEFAULT '{}',
    output_json TEXT NOT NULL DEFAULT '{}',
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    FOREIGN KEY (rule_id) REFERENCES automation_rule(id) ON DELETE CASCADE,
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE SET NULL,
    FOREIGN KEY (resource_id) REFERENCES resource(id) ON DELETE SET NULL,
    FOREIGN KEY (acquisition_id) REFERENCES acquisition(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX idx_automation_execution_event ON automation_execution(rule_id, event_key);
CREATE INDEX idx_automation_execution_rule ON automation_execution(rule_id, created_at DESC);

CREATE TABLE provider_config (
    provider_key TEXT PRIMARY KEY,
    provider_type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    base_url TEXT NOT NULL DEFAULT '',
    secret TEXT NOT NULL DEFAULT '',
    config_json TEXT NOT NULL DEFAULT '{}',
    last_status TEXT NOT NULL DEFAULT 'unknown',
    last_message TEXT NOT NULL DEFAULT '',
    last_checked_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO provider_config(provider_key, provider_type, display_name, enabled, base_url)
VALUES ('javdb', 'source', 'JavDB', 1, 'https://javdb.com')
ON CONFLICT(provider_key) DO NOTHING;
INSERT INTO provider_config(provider_key, provider_type, display_name, enabled, base_url)
SELECT 'metatube', 'metadata', 'MetaTube', 1, value FROM app_setting WHERE key = 'metatube_url'
ON CONFLICT(provider_key) DO NOTHING;
INSERT INTO provider_config(provider_key, provider_type, display_name, enabled, base_url)
SELECT 'qbittorrent', 'download', 'qBittorrent', 1, value FROM app_setting WHERE key = 'qbittorrent_url'
ON CONFLICT(provider_key) DO NOTHING;

INSERT OR IGNORE INTO app_setting(key, value) VALUES
    ('javdb_url', 'https://javdb.com'),
    ('javdb_cookie', ''),
    ('qbittorrent_save_path', '/downloads'),
    ('qbittorrent_category', 'luma'),
    ('qbittorrent_tags', 'luma'),
    ('download_root', '/downloads'),
    ('media_root', '/media'),
    ('organizer_mode', 'hardlink'),
    ('organizer_movie_template', '{code}/{code}.{ext}'),
    ('organizer_conflict_policy', 'attention');

INSERT OR IGNORE INTO media(normalized_code, title, media_type, metadata_status, created_at, updated_at)
SELECT
    lower(replace(replace(trim(title), ' ', ''), '_', '-')),
    title,
    media_type,
    CASE WHEN status = 'ready' THEN 'complete' ELSE 'partial' END,
    created_at,
    updated_at
FROM media_item
WHERE trim(title) <> '';

INSERT OR IGNORE INTO library_item(media_id, legacy_media_item_id, video_path, status, added_at, updated_at)
SELECT m.id, mi.id, mi.path, mi.status, mi.created_at, mi.updated_at
FROM media_item mi
JOIN media m ON m.normalized_code = lower(replace(replace(trim(mi.title), ' ', ''), '_', '-'));
