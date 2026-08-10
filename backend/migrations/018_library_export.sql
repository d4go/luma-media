CREATE TABLE canonical_media_asset (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    asset_type TEXT NOT NULL CHECK(asset_type IN ('poster','backdrop')),
    source_url TEXT,
    local_path TEXT NOT NULL,
    content_hash TEXT,
    mime_type TEXT,
    status TEXT NOT NULL DEFAULT 'available' CHECK(status IN ('available','missing','failed')),
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_verified_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(media_id, asset_type, local_path),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE INDEX idx_canonical_media_asset_active
ON canonical_media_asset(media_id, asset_type, status, updated_at DESC);

CREATE TABLE library_export_state (
    library_item_id INTEGER PRIMARY KEY,
    media_id INTEGER NOT NULL,
    nfo_path TEXT,
    poster_path TEXT,
    metadata_updated_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','success','partial','failed')),
    last_error TEXT,
    last_exported_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (library_item_id) REFERENCES library_item(id) ON DELETE CASCADE,
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);

CREATE INDEX idx_library_export_stale
ON library_export_state(status, metadata_updated_at, last_exported_at);
