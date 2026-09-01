-- Built-in source adapters. Each adapter can have multiple provider instances;
-- only Jav321 is enabled by default because the official JavDB/JavLibrary sites
-- currently require a browser-issued Cloudflare cookie in many regions.
INSERT INTO provider_config(provider_key, provider_type, display_name, enabled, base_url, secret, config_json)
VALUES
    ('jav321', 'source', 'Jav321', 1, 'https://www.jav321.com', '', '{"adapter":"jav321"}'),
    ('javdb', 'source', 'JavDB', 0, 'https://javdb.com', '', '{"adapter":"javdb"}'),
    ('javlibrary', 'source', 'JavLibrary', 0, 'https://www.javlibrary.com', '', '{"adapter":"javlibrary"}')
ON CONFLICT(provider_key) DO UPDATE SET
    provider_type = 'source',
    config_json = excluded.config_json,
    base_url = CASE WHEN provider_config.base_url = '' THEN excluded.base_url ELSE provider_config.base_url END,
    updated_at = datetime('now');
