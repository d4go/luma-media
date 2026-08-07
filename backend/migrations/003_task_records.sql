PRAGMA foreign_keys = ON;

CREATE TABLE task_record (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    progress INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    FOREIGN KEY (task_id) REFERENCES scrape_task(id) ON DELETE CASCADE
);

-- Existing rows represented individual executions. Preserve every one of them as
-- a record, while grouping executions that target the same media or folder under
-- a single logical task.
INSERT INTO task_record (task_id, status, progress, error_message, created_at, finished_at)
SELECT (
        SELECT MAX(parent.id)
        FROM scrape_task parent
        WHERE parent.task_type = execution.task_type
          AND (
              (
                  execution.task_type = 'scrape'
                  AND execution.media_id IS NOT NULL
                  AND parent.media_id = execution.media_id
              )
              OR (
                  execution.task_type = 'scan'
                  AND execution.folder_id IS NOT NULL
                  AND parent.folder_id = execution.folder_id
              )
              OR (
                  execution.task_type = 'scrape'
                  AND execution.media_id IS NULL
                  AND parent.id = execution.id
              )
              OR (
                  execution.task_type = 'scan'
                  AND execution.folder_id IS NULL
                  AND parent.id = execution.id
              )
              OR (
                  execution.task_type NOT IN ('scrape', 'scan')
                  AND parent.media_id IS execution.media_id
                  AND parent.folder_id IS execution.folder_id
              )
          )
    ),
    execution.status,
    execution.progress,
    execution.error_message,
    execution.created_at,
    execution.finished_at
FROM scrape_task execution;

DELETE FROM scrape_task
WHERE id NOT IN (SELECT DISTINCT task_id FROM task_record);

ALTER TABLE scrape_task ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';

UPDATE scrape_task
SET created_at = (
        SELECT MIN(record.created_at)
        FROM task_record record
        WHERE record.task_id = scrape_task.id
    ),
    updated_at = COALESCE(
        finished_at,
        (SELECT MAX(record.created_at) FROM task_record record WHERE record.task_id = scrape_task.id),
        created_at
    );

CREATE UNIQUE INDEX idx_task_scrape_media
    ON scrape_task(media_id)
    WHERE task_type = 'scrape' AND media_id IS NOT NULL;

CREATE UNIQUE INDEX idx_task_scan_folder
    ON scrape_task(folder_id)
    WHERE task_type = 'scan' AND folder_id IS NOT NULL;

CREATE INDEX idx_task_record_task ON task_record(task_id, created_at DESC, id DESC);
CREATE INDEX idx_task_record_status ON task_record(status);
