# Luma Media Database Design

## media_item

    id
    path
    filename
    hash
    title
    media_type
    provider_id
    status
    created_at
    updated_at

## media_config

    id
    name
    path
    media_type
    output_format
    scan_mode
    enabled

## scrape_task

    id
    media_id
    task_type
    status
    progress
    error_message
    created_at
    finished_at

## metadata_record

    id
    media_id
    provider
    raw_json
    created_at

## system_log

    id
    level
    module
    message
    created_at
