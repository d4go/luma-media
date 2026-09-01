CREATE TABLE IF NOT EXISTS provider_runtime_state (
    provider_key TEXT PRIMARY KEY,
    runtime_state TEXT NOT NULL DEFAULT 'unavailable'
        CHECK(runtime_state IN ('ready','degraded','cooldown','interaction_required','unavailable')),
    active_fetch_mode TEXT NOT NULL DEFAULT 'http'
        CHECK(active_fetch_mode IN ('http','browser','auto')),
    last_success_at TEXT,
    last_failure_at TEXT,
    last_failure_kind TEXT,
    last_failure_message TEXT,
    failure_count INTEGER NOT NULL DEFAULT 0,
    cooldown_until TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (provider_key) REFERENCES provider_config(provider_key) ON DELETE CASCADE
);

UPDATE provider_config
SET config_json = json_set(
    CASE WHEN json_valid(config_json) THEN config_json ELSE '{}' END,
    '$.fetchMode',
    CASE
        WHEN lower(COALESCE(json_extract(config_json, '$.adapter'), '')) = 'javdb' THEN 'browser'
        ELSE 'http'
    END
)
WHERE provider_type = 'source'
  AND json_extract(CASE WHEN json_valid(config_json) THEN config_json ELSE '{}' END, '$.fetchMode') IS NULL;

INSERT OR IGNORE INTO provider_runtime_state(provider_key, active_fetch_mode)
SELECT
    provider_key,
    CASE lower(COALESCE(json_extract(config_json, '$.fetchMode'), 'http'))
        WHEN 'browser' THEN 'browser'
        WHEN 'auto' THEN 'auto'
        ELSE 'http'
    END
FROM provider_config
WHERE provider_type = 'source';

CREATE INDEX IF NOT EXISTS idx_provider_runtime_state_status
ON provider_runtime_state(runtime_state, cooldown_until);
