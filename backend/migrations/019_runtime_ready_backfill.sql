PRAGMA foreign_keys = ON;

-- The runtime circuit breaker is new. Providers that were previously working
-- without any recorded failure must not start out blocked.
UPDATE provider_runtime_state
SET runtime_state = 'ready', updated_at = datetime('now')
WHERE runtime_state = 'unavailable'
  AND failure_count = 0
  AND last_failure_at IS NULL
  AND cooldown_until IS NULL;
