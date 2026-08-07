PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS media_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    media_type TEXT NOT NULL DEFAULT 'movie',
    output_format TEXT NOT NULL DEFAULT 'nfo',
    scan_mode TEXT NOT NULL DEFAULT 'manual',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS media_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folder_id INTEGER,
    path TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    hash TEXT NOT NULL,
    title TEXT NOT NULL,
    media_type TEXT NOT NULL,
    provider_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (folder_id) REFERENCES media_config(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS scrape_task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER,
    folder_id INTEGER,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    progress INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    FOREIGN KEY (media_id) REFERENCES media_item(id) ON DELETE SET NULL,
    FOREIGN KEY (folder_id) REFERENCES media_config(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS metadata_record (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    provider TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (media_id) REFERENCES media_item(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS system_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT NOT NULL,
    module TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS app_setting (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_media_status ON media_item(status);
CREATE INDEX IF NOT EXISTS idx_media_folder ON media_item(folder_id);
CREATE INDEX IF NOT EXISTS idx_task_status ON scrape_task(status);
CREATE INDEX IF NOT EXISTS idx_task_created ON scrape_task(created_at DESC);

INSERT OR IGNORE INTO app_setting (key, value) VALUES
    ('metatube_url', 'http://metatube:8080'),
    ('output_format', 'nfo'),
    ('scan_interval', '60'),
    ('overwrite_policy', 'missing'),
    ('log_level', 'info');

