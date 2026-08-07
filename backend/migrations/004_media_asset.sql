PRAGMA foreign_keys = ON;

CREATE TABLE media_asset (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    source TEXT NOT NULL,
    url TEXT NOT NULL,
    local_path TEXT,
    status TEXT NOT NULL,
    checked_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media_item(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_media_asset_source
    ON media_asset(media_id, asset_type, source, url);

CREATE INDEX idx_media_asset_active
    ON media_asset(media_id, asset_type, status, updated_at DESC);
