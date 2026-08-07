CREATE TABLE IF NOT EXISTS crawler_script (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    website_url TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    interval_minutes INTEGER NOT NULL DEFAULT 60,
    enabled INTEGER NOT NULL DEFAULT 1,
    auto_download INTEGER NOT NULL DEFAULT 0,
    last_started_at TEXT,
    last_finished_at TEXT,
    next_run_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS crawler_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    stdout TEXT NOT NULL DEFAULT '',
    stderr TEXT NOT NULL DEFAULT '',
    result_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT,
    FOREIGN KEY (script_id) REFERENCES crawler_script(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS crawler_result (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    script_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    download_url TEXT NOT NULL,
    trackers_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL,
    download_status TEXT NOT NULL DEFAULT 'pending',
    qbit_hash TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    downloaded_at TEXT,
    FOREIGN KEY (run_id) REFERENCES crawler_run(id) ON DELETE CASCADE,
    FOREIGN KEY (script_id) REFERENCES crawler_script(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_crawler_due ON crawler_script(enabled, next_run_at);
CREATE INDEX IF NOT EXISTS idx_crawler_run_script ON crawler_run(script_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_crawler_run_active ON crawler_run(script_id)
WHERE status IN ('pending', 'running');
CREATE INDEX IF NOT EXISTS idx_crawler_result_script ON crawler_result(script_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crawler_result_download ON crawler_result(download_status);

INSERT OR IGNORE INTO app_setting (key, value) VALUES
    ('qbittorrent_url', 'http://127.0.0.1:8080'),
    ('qbittorrent_username', 'admin'),
    ('qbittorrent_password', ''),
    ('qbittorrent_auto_update_trackers', 'false'),
    ('qbittorrent_tracker_source_url', 'https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt'),
    ('qbittorrent_tracker_update_interval', '1440'),
    ('qbittorrent_tracker_last_run', '');

-- New installs and untouched legacy defaults use the deployment host inferred from the request.
UPDATE app_setting SET value = 'auto', updated_at = datetime('now')
WHERE key = 'metatube_url' AND value = 'http://metatube:8080';
