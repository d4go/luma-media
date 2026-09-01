PRAGMA foreign_keys = ON;

ALTER TABLE job_run ADD COLUMN config_json TEXT NOT NULL DEFAULT '{}';
