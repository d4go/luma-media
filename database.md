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
    folder_id
    task_type
    status
    progress
    error_message
    created_at
    updated_at
    finished_at

`scrape_task` 表示逻辑任务。每个媒体最多有一个 `scrape` 任务，每个目录最多有一个
`scan` 任务。任务上的状态与进度是最近一次执行的快照。

## task_record

    id
    task_id
    status
    progress
    error_message
    created_at
    finished_at

`task_record` 保存每一次执行。重试不会创建新的逻辑任务，只会在原任务下增加记录。

## metadata_record

    id
    media_id
    provider
    raw_json
    created_at

## media_asset

    id
    media_id
    asset_type
    source
    url
    local_path
    status
    checked_at
    created_at
    updated_at

`media_asset` 保存海报/封面的远程来源、健康状态与本地缓存路径。`ACTIVE` 表示当前可用，
`BROKEN` 表示重定向、HTTP 错误、非图片响应或缓存文件缺失。

## system_log

    id
    level
    module
    message
    created_at
