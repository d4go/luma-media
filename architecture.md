# Luma Media Architecture

## Technology Stack

Backend: - Rust - Axum - Tokio - SQLx

Frontend: - Vue3 - TypeScript - Naive UI

Database: - SQLite / PostgreSQL

Deployment: - Docker

## Architecture

    Vue3 + Naive UI
            |
            |
         REST API
            |
            |
     Rust Axum Server
            |
     ------------------------------------------------
     |        |          |          |                |
    Scanner Parser   Provider   Task Engine   Template
                                          |
                                      MetaTube

## Backend Modules

    luma-server

    ├── api
    ├── scanner
    ├── parser
    ├── provider
    ├── task
    ├── metadata
    ├── template
    ├── storage
    └── logger

Design principles:

-   modular architecture
-   provider abstraction
-   asynchronous task processing
-   configuration driven
-   Docker friendly

## Task model

`scrape_task` 是面向媒体或目录的逻辑任务，`task_record` 是单次执行。刮削重试复用原任务，
仅增加执行记录。所有入口共享一个 Tokio semaphore，最多同时执行 8 个远程刮削。

扫描器在排队远程刮削前检查本地 NFO 与海报；资料完整的媒体直接关联到本地元数据记录。

## Asset resolver

海报与封面 URL 在写入媒体目录或返回客户端前必须通过严格 HTTP 状态和 Content-Type 检查。
解析器拒绝重定向，按 Provider 优先级寻找备用资源，并将成功图片缓存到数据卷。定时健康任务
每天复查 `ACTIVE` 资源，失败后标记 `BROKEN` 并重新执行 Provider 回退。
