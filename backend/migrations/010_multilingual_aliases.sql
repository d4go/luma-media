PRAGMA foreign_keys = ON;

CREATE TABLE media_title_alias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    locale TEXT NOT NULL DEFAULT 'und' CHECK (locale IN ('ja', 'zh', 'en', 'und')),
    alias TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    source_key TEXT NOT NULL DEFAULT 'legacy',
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_media_title_alias_identity
    ON media_title_alias(media_id, normalized_alias);
CREATE INDEX idx_media_title_alias_search
    ON media_title_alias(normalized_alias, media_id);

CREATE TABLE actor_name_alias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id INTEGER NOT NULL,
    locale TEXT NOT NULL DEFAULT 'und' CHECK (locale IN ('ja', 'zh', 'en', 'und')),
    alias TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    source_key TEXT NOT NULL DEFAULT 'legacy',
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (actor_id) REFERENCES actor(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_actor_name_alias_identity
    ON actor_name_alias(actor_id, normalized_alias);
CREATE INDEX idx_actor_name_alias_search
    ON actor_name_alias(normalized_alias, actor_id);

INSERT OR IGNORE INTO media_title_alias(media_id, locale, alias, normalized_alias, source_key, is_primary)
SELECT id, 'und', trim(title), lower(replace(trim(title), ' ', '')), 'migration', 1
FROM media
WHERE trim(title) != '';

INSERT OR IGNORE INTO media_title_alias(media_id, locale, alias, normalized_alias, source_key, is_primary)
SELECT id, 'und', trim(original_title), lower(replace(trim(original_title), ' ', '')), 'migration', 0
FROM media
WHERE trim(COALESCE(original_title, '')) != '';

INSERT OR IGNORE INTO actor_name_alias(actor_id, locale, alias, normalized_alias, source_key, is_primary)
SELECT id, 'und', trim(name), lower(replace(trim(name), ' ', '')), 'migration', 1
FROM actor
WHERE trim(name) != '';

INSERT OR IGNORE INTO actor_name_alias(actor_id, locale, alias, normalized_alias, source_key, is_primary)
SELECT actor.id, 'und', trim(json_each.value), lower(replace(trim(json_each.value), ' ', '')), 'migration', 0
FROM actor, json_each(actor.aliases_json)
WHERE json_valid(actor.aliases_json)
  AND json_type(actor.aliases_json) = 'array'
  AND json_each.type = 'text'
  AND trim(json_each.value) != '';

-- The user requested every built-in source to participate in searches. This
-- one-time migration enables the two adapters that were historically seeded
-- as disabled; they can still be disabled again from Settings afterwards.
UPDATE provider_config
SET enabled = 1, updated_at = datetime('now')
WHERE provider_type = 'source'
  AND provider_key IN ('javbus', 'jav321', 'javdb', 'javlibrary');
