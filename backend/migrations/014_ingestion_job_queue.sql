CREATE TABLE ingestion_job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_key TEXT NOT NULL,
    job_type TEXT NOT NULL,
    priority INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending','running','succeeded','failed','cancelled')),
    payload_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(payload_json)),
    dedupe_key TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK(max_attempts > 0),
    available_at TEXT NOT NULL DEFAULT (datetime('now')),
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT,
    started_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    FOREIGN KEY (provider_key) REFERENCES provider_config(provider_key) ON DELETE CASCADE
);

CREATE INDEX idx_ingestion_job_claim
ON ingestion_job(status, available_at, priority DESC, id);

CREATE INDEX idx_ingestion_job_lease
ON ingestion_job(lease_expires_at) WHERE status = 'running';

CREATE INDEX idx_ingestion_job_provider
ON ingestion_job(provider_key, job_type, id DESC);

CREATE UNIQUE INDEX idx_ingestion_job_active_dedupe
ON ingestion_job(dedupe_key)
WHERE dedupe_key IS NOT NULL AND status IN ('pending','running');

ALTER TABLE source_sync_run ADD COLUMN ingestion_job_id INTEGER
REFERENCES ingestion_job(id) ON DELETE SET NULL;

CREATE INDEX idx_source_sync_run_ingestion_job
ON source_sync_run(ingestion_job_id) WHERE ingestion_job_id IS NOT NULL;
