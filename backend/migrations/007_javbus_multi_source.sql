-- Replace the retired JavDB adapter with a JavBus source registry.
-- Historical mappings/resources are intentionally retained for auditability,
-- but the old provider can no longer be enabled or queried.
DELETE FROM provider_config WHERE provider_key = 'javdb';
DELETE FROM app_setting WHERE key IN ('javdb_url', 'javdb_cookie');

INSERT INTO provider_config(
    provider_key,
    provider_type,
    display_name,
    enabled,
    base_url,
    secret,
    config_json
)
VALUES (
    'javbus',
    'source',
    'JavBus',
    1,
    'https://www.javbus.com',
    '',
    '{"adapter":"javbus"}'
)
ON CONFLICT(provider_key) DO UPDATE SET
    provider_type = 'source',
    display_name = 'JavBus',
    base_url = CASE
        WHEN provider_config.base_url = '' THEN excluded.base_url
        ELSE provider_config.base_url
    END,
    config_json = '{"adapter":"javbus"}',
    updated_at = datetime('now');
