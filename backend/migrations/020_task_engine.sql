PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS job_definition (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    name TEXT NOT NULL,
    provider_key TEXT,
    schedule TEXT,
    config_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    next_run_at TEXT,
    last_run_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS job_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_definition_id INTEGER,
    job_type TEXT NOT NULL,
    provider_key TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','pausing','paused','cancelling','cancelled','success','failed')),
    idempotency_key TEXT NOT NULL UNIQUE,
    priority INTEGER NOT NULL DEFAULT 100,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER,
    checkpoint_json TEXT,
    error_message TEXT,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (job_definition_id) REFERENCES job_definition(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_job_run_status ON job_run(status, priority DESC, id);
CREATE INDEX IF NOT EXISTS idx_job_run_idempotency ON job_run(idempotency_key);

CREATE TABLE IF NOT EXISTS job_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    item_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','success','failed','skipped','cancelled')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    checkpoint_json TEXT,
    error_message TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES job_run(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_item_key ON job_item(run_id, item_key);
CREATE INDEX IF NOT EXISTS idx_job_item_status ON job_item(run_id, status);

CREATE TABLE IF NOT EXISTS job_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    event_key TEXT NOT NULL,
    message TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES job_run(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_event_run ON job_event(run_id, id);
