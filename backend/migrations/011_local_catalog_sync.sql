PRAGMA foreign_keys = ON;

CREATE TABLE source_sync_state (
    provider_key TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'idle' CHECK (status IN ('idle', 'running', 'success', 'failed')),
    last_started_at TEXT,
    last_finished_at TEXT,
    last_success_at TEXT,
    next_run_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_message TEXT NOT NULL DEFAULT '',
    failure_count INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    cursor_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(cursor_json)),
    watermark_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(watermark_json)),
    lease_owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (provider_key) REFERENCES provider_config(provider_key) ON DELETE CASCADE
);

CREATE INDEX idx_source_sync_due ON source_sync_state(status, next_run_at);
CREATE INDEX idx_source_sync_ready
    ON source_sync_state(next_run_at, provider_key) WHERE status != 'running';
CREATE INDEX idx_source_sync_lease
    ON source_sync_state(lease_expires_at) WHERE status = 'running';

CREATE TABLE source_sync_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_key TEXT NOT NULL,
    script_id INTEGER,
    status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    item_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    cursor_before_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(cursor_before_json)),
    cursor_after_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(cursor_after_json)),
    watermark_before_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(watermark_before_json)),
    watermark_after_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(watermark_after_json)),
    error_message TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    FOREIGN KEY (provider_key) REFERENCES provider_config(provider_key) ON DELETE CASCADE,
    FOREIGN KEY (script_id) REFERENCES crawler_script(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX idx_source_sync_run_active
    ON source_sync_run(provider_key) WHERE status = 'running';
CREATE INDEX idx_source_sync_run_provider
    ON source_sync_run(provider_key, id DESC);
CREATE INDEX idx_source_sync_run_script
    ON source_sync_run(script_id, id DESC) WHERE script_id IS NOT NULL;

INSERT OR IGNORE INTO source_sync_state(provider_key)
SELECT provider_key FROM provider_config WHERE provider_type = 'source';

-- Keep scheduler state in sync when a source is added after this migration.
CREATE TRIGGER provider_config_source_sync_ai
AFTER INSERT ON provider_config
WHEN new.provider_type = 'source'
BEGIN
    INSERT OR IGNORE INTO source_sync_state(provider_key) VALUES (new.provider_key);
END;

CREATE TRIGGER provider_config_source_sync_au
AFTER UPDATE OF provider_type ON provider_config
WHEN new.provider_type = 'source'
BEGIN
    INSERT OR IGNORE INTO source_sync_state(provider_key) VALUES (new.provider_key);
END;

-- A source may use more than one uploaded crawler script, and a reusable
-- script may serve more than one mirrored source.
CREATE TABLE source_sync_script (
    provider_key TEXT NOT NULL,
    script_id INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (provider_key, script_id),
    FOREIGN KEY (provider_key) REFERENCES provider_config(provider_key) ON DELETE CASCADE,
    FOREIGN KEY (script_id) REFERENCES crawler_script(id) ON DELETE CASCADE
);

CREATE INDEX idx_source_sync_script_script
    ON source_sync_script(script_id, enabled, priority DESC);

ALTER TABLE crawler_result ADD COLUMN fingerprint TEXT;
ALTER TABLE crawler_result ADD COLUMN first_seen_at TEXT;
ALTER TABLE crawler_result ADD COLUMN last_seen_at TEXT;
ALTER TABLE crawler_result ADD COLUMN seen_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE crawler_result ADD COLUMN media_id INTEGER REFERENCES media(id) ON DELETE SET NULL;
ALTER TABLE crawler_result ADD COLUMN resource_id INTEGER REFERENCES resource(id) ON DELETE SET NULL;

-- Keep historical duplicate rows for auditability. Only the newest occurrence
-- receives the stable fingerprint and becomes the target of future upserts.
-- The priority/order mirrors crawler::result_fingerprint for the identities
-- that SQLite can canonicalize safely during migration.
WITH candidates AS (
    SELECT
        id,
        script_id,
        created_at,
        CASE
            WHEN json_valid(raw_json)
                 AND trim(COALESCE(json_extract(raw_json, '$.fingerprint'), '')) != ''
                THEN 'fingerprint:' || lower(replace(replace(trim(json_extract(raw_json, '$.fingerprint')), ' ', ''), char(9), ''))
            WHEN json_valid(raw_json)
                 AND trim(COALESCE(
                     json_extract(raw_json, '$.infoHash'),
                     json_extract(raw_json, '$.info_hash'),
                     json_extract(raw_json, '$.btih'),
                     json_extract(raw_json, '$.hash'),
                     ''
                 )) != ''
                THEN 'btih:' || replace(lower(trim(COALESCE(
                    json_extract(raw_json, '$.infoHash'),
                    json_extract(raw_json, '$.info_hash'),
                    json_extract(raw_json, '$.btih'),
                    json_extract(raw_json, '$.hash')
                ))), 'urn:btih:', '')
            WHEN instr(lower(download_url), 'urn:btih:') > 0
                THEN 'btih:' || CASE
                    WHEN instr(substr(lower(download_url), instr(lower(download_url), 'urn:btih:') + 9), '&') > 0
                        THEN substr(
                            substr(lower(download_url), instr(lower(download_url), 'urn:btih:') + 9),
                            1,
                            instr(substr(lower(download_url), instr(lower(download_url), 'urn:btih:') + 9), '&') - 1
                        )
                    ELSE substr(lower(download_url), instr(lower(download_url), 'urn:btih:') + 9)
                END
            WHEN json_valid(raw_json)
                 AND trim(COALESCE(
                     json_extract(raw_json, '$.resourceId'),
                     json_extract(raw_json, '$.resource_id'),
                     json_extract(raw_json, '$.externalId'),
                     json_extract(raw_json, '$.external_id'),
                     json_extract(raw_json, '$.providerResourceId'),
                     json_extract(raw_json, '$.provider_resource_id'),
                     json_extract(raw_json, '$.guid'),
                     json_extract(raw_json, '$.id'),
                     ''
                 )) != ''
                THEN 'source:' || lower(replace(replace(trim(COALESCE(
                    json_extract(raw_json, '$.resourceId'),
                    json_extract(raw_json, '$.resource_id'),
                    json_extract(raw_json, '$.externalId'),
                    json_extract(raw_json, '$.external_id'),
                    json_extract(raw_json, '$.providerResourceId'),
                    json_extract(raw_json, '$.provider_resource_id'),
                    json_extract(raw_json, '$.guid'),
                    json_extract(raw_json, '$.id')
                )), ' ', ''), char(9), ''))
            ELSE 'url:' || lower(trim(download_url))
        END AS fingerprint
    FROM crawler_result
    WHERE trim(download_url) != ''
), grouped AS (
    SELECT
        script_id,
        fingerprint,
        MAX(id) AS keeper_id,
        MIN(created_at) AS first_seen_at,
        MAX(created_at) AS last_seen_at,
        COUNT(*) AS seen_count
    FROM candidates
    GROUP BY script_id, fingerprint
)
UPDATE crawler_result
SET fingerprint = (SELECT fingerprint FROM grouped WHERE keeper_id = crawler_result.id),
    first_seen_at = (SELECT first_seen_at FROM grouped WHERE keeper_id = crawler_result.id),
    last_seen_at = (SELECT last_seen_at FROM grouped WHERE keeper_id = crawler_result.id),
    seen_count = (SELECT seen_count FROM grouped WHERE keeper_id = crawler_result.id)
WHERE id IN (SELECT keeper_id FROM grouped);

CREATE UNIQUE INDEX idx_crawler_result_identity
    ON crawler_result(script_id, fingerprint) WHERE fingerprint IS NOT NULL;
CREATE INDEX idx_crawler_result_catalog
    ON crawler_result(media_id, resource_id, last_seen_at DESC);

CREATE INDEX idx_media_actor_actor_media
    ON media_actor(actor_id, media_id);
CREATE INDEX idx_provider_mapping_media_provider
    ON provider_entity_mapping(media_id, provider_key)
    WHERE media_id IS NOT NULL;
CREATE INDEX idx_provider_mapping_actor_provider
    ON provider_entity_mapping(actor_id, provider_key)
    WHERE actor_id IS NOT NULL;
CREATE INDEX idx_media_title_alias_owner_rank
    ON media_title_alias(media_id, is_primary DESC, updated_at DESC);
CREATE INDEX idx_actor_name_alias_owner_rank
    ON actor_name_alias(actor_id, is_primary DESC, updated_at DESC);

CREATE TABLE media_search_document (
    media_id INTEGER PRIMARY KEY,
    code TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    original_title TEXT NOT NULL DEFAULT '',
    aliases TEXT NOT NULL DEFAULT '',
    actors TEXT NOT NULL DEFAULT '',
    resources TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE VIRTUAL TABLE media_search_fts USING fts5(
    code,
    title,
    original_title,
    aliases,
    actors,
    resources,
    content = 'media_search_document',
    content_rowid = 'media_id',
    tokenize = 'trigram'
);

CREATE TRIGGER media_search_document_ai AFTER INSERT ON media_search_document BEGIN
    INSERT INTO media_search_fts(rowid, code, title, original_title, aliases, actors, resources)
    VALUES (new.media_id, new.code, new.title, new.original_title, new.aliases, new.actors, new.resources);
END;
CREATE TRIGGER media_search_document_ad AFTER DELETE ON media_search_document BEGIN
    INSERT INTO media_search_fts(media_search_fts, rowid, code, title, original_title, aliases, actors, resources)
    VALUES ('delete', old.media_id, old.code, old.title, old.original_title, old.aliases, old.actors, old.resources);
END;
CREATE TRIGGER media_search_document_au AFTER UPDATE ON media_search_document BEGIN
    INSERT INTO media_search_fts(media_search_fts, rowid, code, title, original_title, aliases, actors, resources)
    VALUES ('delete', old.media_id, old.code, old.title, old.original_title, old.aliases, old.actors, old.resources);
    INSERT INTO media_search_fts(rowid, code, title, original_title, aliases, actors, resources)
    VALUES (new.media_id, new.code, new.title, new.original_title, new.aliases, new.actors, new.resources);
END;

INSERT INTO media_search_document(media_id, code, title, original_title, aliases, actors, resources)
SELECT
    m.id,
    m.normalized_code,
    m.title,
    COALESCE(m.original_title, ''),
    COALESCE((SELECT group_concat(alias, ' ') FROM media_title_alias WHERE media_id = m.id), ''),
    COALESCE((
        SELECT group_concat(actor_text, ' ')
        FROM (
            SELECT a.name || ' ' || COALESCE((SELECT group_concat(alias, ' ') FROM actor_name_alias WHERE actor_id = a.id), '') AS actor_text
            FROM media_actor ma JOIN actor a ON a.id = ma.actor_id
            WHERE ma.media_id = m.id
        )
    ), ''),
    COALESCE((SELECT group_concat(title, ' ') FROM resource WHERE media_id = m.id AND available = 1), '')
FROM media m;

-- Build from the external content table after the complete snapshot exists.
-- This also makes the initial index independent of row-trigger execution order.
INSERT INTO media_search_fts(media_search_fts) VALUES ('rebuild');

CREATE TABLE actor_search_document (
    actor_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    aliases TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (actor_id) REFERENCES actor(id) ON DELETE CASCADE
);

CREATE VIRTUAL TABLE actor_search_fts USING fts5(
    name,
    aliases,
    content = 'actor_search_document',
    content_rowid = 'actor_id',
    tokenize = 'trigram'
);

CREATE TRIGGER actor_search_document_ai AFTER INSERT ON actor_search_document BEGIN
    INSERT INTO actor_search_fts(rowid, name, aliases)
    VALUES (new.actor_id, new.name, new.aliases);
END;
CREATE TRIGGER actor_search_document_ad AFTER DELETE ON actor_search_document BEGIN
    INSERT INTO actor_search_fts(actor_search_fts, rowid, name, aliases)
    VALUES ('delete', old.actor_id, old.name, old.aliases);
END;
CREATE TRIGGER actor_search_document_au AFTER UPDATE ON actor_search_document BEGIN
    INSERT INTO actor_search_fts(actor_search_fts, rowid, name, aliases)
    VALUES ('delete', old.actor_id, old.name, old.aliases);
    INSERT INTO actor_search_fts(rowid, name, aliases)
    VALUES (new.actor_id, new.name, new.aliases);
END;

INSERT INTO actor_search_document(actor_id, name, aliases)
SELECT
    a.id,
    a.name,
    COALESCE((SELECT group_concat(alias, ' ') FROM actor_name_alias WHERE actor_id = a.id), '')
FROM actor a;

INSERT INTO actor_search_fts(actor_search_fts) VALUES ('rebuild');
