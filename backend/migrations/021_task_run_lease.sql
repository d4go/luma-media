PRAGMA foreign_keys = ON;

ALTER TABLE job_run ADD COLUMN lease_owner TEXT;
ALTER TABLE job_run ADD COLUMN lease_expires_at TEXT;

ALTER TABLE job_item ADD COLUMN available_at TEXT;

CREATE INDEX IF NOT EXISTS idx_job_run_lease ON job_run(status, lease_expires_at);
