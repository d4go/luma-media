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
