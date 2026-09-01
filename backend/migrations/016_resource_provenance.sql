ALTER TABLE resource ADD COLUMN first_seen_at TEXT;
ALTER TABLE resource ADD COLUMN last_seen_at TEXT;
ALTER TABLE resource ADD COLUMN last_verified_at TEXT;
ALTER TABLE resource ADD COLUMN availability_status TEXT NOT NULL DEFAULT 'unknown'
CHECK(availability_status IN ('unknown','available','unavailable'));
ALTER TABLE resource ADD COLUMN codec TEXT;
ALTER TABLE resource ADD COLUMN source_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE resource ADD COLUMN resource_fingerprint TEXT;

UPDATE resource
SET first_seen_at = COALESCE(first_seen_at, created_at),
    last_seen_at = COALESCE(last_seen_at, updated_at),
    last_verified_at = COALESCE(last_verified_at, updated_at),
    availability_status = CASE WHEN available = 1 THEN 'available' ELSE 'unavailable' END,
    resource_fingerprint = CASE
        WHEN info_hash IS NOT NULL AND trim(info_hash) != '' THEN 'btih:' || lower(trim(info_hash))
        ELSE resource_fingerprint
    END;

CREATE UNIQUE INDEX idx_resource_fingerprint
ON resource(resource_fingerprint) WHERE resource_fingerprint IS NOT NULL;

CREATE INDEX idx_resource_refresh_due
ON resource(media_id, availability_status, last_verified_at);

CREATE TABLE resource_source_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_id INTEGER NOT NULL,
    provider_key TEXT NOT NULL,
    provider_resource_id TEXT,
    source_identity TEXT NOT NULL,
    source_url TEXT,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    raw_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(raw_json)),
    UNIQUE(provider_key, source_identity),
    FOREIGN KEY (resource_id) REFERENCES resource(id) ON DELETE CASCADE
);

CREATE INDEX idx_resource_source_resource
ON resource_source_mapping(resource_id, last_seen_at DESC);

INSERT OR IGNORE INTO resource_source_mapping(
    resource_id,
    provider_key,
    provider_resource_id,
    source_identity,
    source_url,
    first_seen_at,
    last_seen_at,
    raw_json
)
SELECT
    id,
    provider_key,
    provider_resource_id,
    CASE
        WHEN provider_resource_id IS NOT NULL AND trim(provider_resource_id) != ''
            THEN 'id:' || trim(provider_resource_id)
        WHEN info_hash IS NOT NULL AND trim(info_hash) != ''
            THEN 'btih:' || lower(trim(info_hash))
        ELSE 'legacy:' || id
    END,
    download_url,
    created_at,
    updated_at,
    raw_json
FROM resource;

UPDATE resource
SET source_count = MAX(1, (
    SELECT COUNT(*) FROM resource_source_mapping mapping WHERE mapping.resource_id = resource.id
));

CREATE TABLE media_resource_cache (
    media_id INTEGER NOT NULL,
    provider_key TEXT NOT NULL,
    last_refreshed_at TEXT,
    last_success_at TEXT,
    expires_at TEXT,
    resource_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (media_id, provider_key),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (provider_key) REFERENCES provider_config(provider_key) ON DELETE CASCADE
);

CREATE INDEX idx_media_resource_cache_due
ON media_resource_cache(expires_at, provider_key);
