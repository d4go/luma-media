# Luma

**English** | [中文](README.md)

> A media acquisition and ingestion orchestrator for home NAS setups: it chains discovery, resource selection, downloading, metadata scraping, file organization, and library submission into one **traceable and resumable** automated pipeline.

Luma solves the "last mile" problem of a home media library: no more manually searching across multiple sites, copying magnet links, babysitting qBittorrent downloads, and scraping and organizing files by hand. Luma orchestrates the whole chain — where each acquisition came from, what step it is at, how to recover after a failure, and whether it finally reached the library — all recorded and auditable.

## Table of Contents

- [Features](#features)
- [How It Works](#how-it-works)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [External Services](#external-services)
- [Python Source Scripts](#python-source-scripts)
- [Project Structure](#project-structure)
- [Documentation](#documentation)
- [Security Notes](#security-notes)
- [Disclaimer](#disclaimer)
- [License](#license)

## Features

- **Global search**: searches works and actors at once, with built-in adapters for Jav321, JavDB, JavBus, and JavLibrary. Each source can be configured with multiple instances that aggregate results concurrently, so a single source outage does not break the overall result.
- **Resource ranking**: the server scores resources uniformly by subtitles, quality, size, age, and source, and returns the reasoning behind each ranking — so you know *why* a resource was recommended.
- **Acquisition state machine**: every transition from request, queueing, downloading, organizing, metadata to library submission is a persisted event; after a restart, Luma reconciles with qBittorrent and continues processing.
- **qBittorrent integration**: download submission, progress reconciliation, pause, resume, cancel, and scheduled tracker updates.
- **MetaTube metadata**: metadata, actors, NFO files, and posters; defaults to the deployment machine's IP plus `8080`, configurable in Settings.
- **Automation rules**: described with WHEN / IF / THEN, supporting three modes — AUTO (execute automatically), CONFIRM (human confirmation required), and NOTIFY (notification only).
- **Pending issues**: offline external services, path mapping failures, and file conflicts all surface with explicit recovery actions instead of failing silently.
- **Media library**: shows only files that have been fully organized and committed, while keeping the originating Acquisition records for traceability.
- **Compatibility tools**: media directory watching (filesystem events), periodic scanning, and trusted Python source scripts live under the advanced diagnostics area in Settings.
- **Service status**: the health of Luma, MetaTube, and qBittorrent refreshes every 10 seconds.

## How It Works

The top-level navigation is fixed: **Home, Resources, Downloads, Library, Automation, Settings**. Product operations are uniformly called "acquisitions".

```text
Discovery (search / source scripts / automation rules)
        │
        ▼
Resource selection (server-side ranking + reasons)
        │
        ▼
Download (qBittorrent submission & progress reconciliation)
        │
        ▼
File organization (rename / conflict handling / media directory)
        │
        ▼
Metadata (MetaTube: NFO / posters / actors)
        │
        ▼
Library commit (visible in the library, Acquisition provenance kept)
```

Legacy tasks, crawlers, and raw qBittorrent listings are no longer top-level pages; they moved to the advanced diagnostics area in Settings.

## Tech Stack

| Layer | Technology |
| --- | --- |
| Backend | Rust 2024, Axum 0.8, Tokio, SQLx, SQLite |
| Frontend | Vue 3, TypeScript, Vite, Naive UI, Tabler Icons |
| Deployment | Multi-stage Docker build; a single container serves both API and static frontend |
| Integrations | MetaTube HTTP API, qBittorrent Web API, Python source scripts, Chromium browser fetching |

## Getting Started

### Prerequisites

- **Recommended**: Docker and Docker Compose.
- **Local development**: Rust 1.85+ (edition 2024), Node.js 22+, npm, Python 3.
- **External services**: qBittorrent (Web UI) and MetaTube — see [External Services](#external-services) below.

### Docker Deployment (Recommended)

Luma needs three persistent or shared mounts:

| Container path | Purpose |
| --- | --- |
| `/data` | SQLite database, poster cache, browser profiles, and script runtime data |
| `/downloads` | Must be the **same host download directory shared with qBittorrent**, so finished downloads can be organized into the library |
| `/media` | Final media library directory, needs read/write access (scraped results are written next to the video files) |

```bash
MEDIA_ROOT=/path/to/media \
DOWNLOAD_ROOT=/path/to/downloads \
LUMA_DATA_ROOT=/path/to/luma-data \
docker compose up -d --build
```

Open `http://localhost:3000`. Both qBittorrent's save path and Luma's mapped path default to `/downloads`.

> Create a `.env` file next to `docker-compose.yml` to persist host path configuration; see [`.env.example`](.env.example). In the Luma UI, enter the container path `/media` for the media directory — not the host path.

### Local Development

```bash
cd backend
DATABASE_URL='sqlite://luma.db?mode=rwc' cargo run
```

```bash
cd frontend
npm ci
npm run dev
```

Open `http://localhost:5173`. Vite proxies `/api` to `http://localhost:3000`.

### Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite://luma.db?mode=rwc` | SQLite connection string |
| `LUMA_BIND` | `0.0.0.0:3000` | HTTP listen address |
| `LUMA_DATA_DIR` | `data` | Runtime data directory (database, cache, scripts) |
| `LUMA_STATIC_DIR` | — | Frontend static files directory (built into the image at `/app/web`) |
| `LUMA_METATUBE_URL` | `http://<host-ip>:8080` | Override for the MetaTube service address |
| `LUMA_PYTHON_BIN` | `python3` | Python interpreter path (for source scripts) |
| `LUMA_BROWSER_ENABLED` | `true` | Enable Chromium browser fetching |
| `LUMA_CHROMIUM_PATH` | `/usr/bin/chromium` | Path to the Chromium executable |
| `LUMA_BROWSER_DATA_DIR` | `/data/browser-profiles` | Browser user-data directory |
| `LUMA_BROWSER_HEADLESS` | `true` | Headless mode |
| `LUMA_BROWSER_IDLE_SECONDS` | `300` | Reap each provider's Chromium after this many seconds of inactivity (frees the browser process and its renderers) |
| `LUMA_BROWSER_SESSION_PORT` | `6080` | Browser session debug port (VNC) |
| `RUST_LOG` | `luma_server=info,tower_http=info` | Log level |

Docker Compose host paths (put in `.env`):

| Variable | Description |
| --- | --- |
| `MEDIA_ROOT` | Host media library directory, mapped to container `/media` |
| `DOWNLOAD_ROOT` | Shared download directory with qBittorrent, mapped to container `/downloads` |
| `LUMA_DATA_ROOT` | Host runtime data directory, mapped to container `/data` |

## External Services

### qBittorrent

Luma uses the qBittorrent Web API to submit downloads, reconcile progress, and control tasks. Make sure:

1. The qBittorrent Web UI is enabled;
2. Its download directory is the **same host directory** as Luma's `/downloads` mount;
3. The Web UI address, port, and credentials are filled in under Luma Settings.

### MetaTube

MetaTube provides work metadata, actors, NFO files, and posters. It defaults to `http://<deployment-machine-ip>:8080`, configurable in Settings or overridable via the `LUMA_METATUBE_URL` environment variable.

## Python Source Scripts

The "Python source scripts" feature in Settings supports uploading `.py` files up to 1 MiB, binding them to a website, running them on a schedule, and persisting stdout, stderr, and structured results. Scripts should write to stdout or to `LUMA_RESULT_PATH`:

```json
{
  "results": [
    {
      "title": "ABC-123 1080p",
      "downloadUrl": "magnet:?xt=urn:btih:...",
      "trackers": ["udp://tracker.example:80/announce"]
    }
  ]
}
```

Script results are first normalized into Media and Resource records and then handed to the unified AcquisitionService — scripts never bypass the state machine to touch qBittorrent directly. **Only upload scripts you trust** — they run on the Luma host and are equivalent to arbitrary code execution.

Reference example: [`examples/sample_crawler.py`](examples/sample_crawler.py).

## Project Structure

```text
backend/          Rust backend (API, state machine, provider adapters, fetching, ingestion)
  migrations/     SQLite migrations
  src/providers/  Jav321 / JavDB / JavBus / JavLibrary adapters
frontend/         Vue 3 frontend (Home, Resources, Downloads, Library, Automation, Settings)
docs/             Design docs: architecture, API, database, providers, state machine, etc.
examples/         Sample Python source script
Dockerfile        Multi-stage build (frontend + backend → single container)
docker-compose.yml
```

## Documentation

- [Current State Audit](docs/current-state.md)
- [Architecture](docs/architecture.md)
- [API](docs/api.md)
- [Database](docs/database.md)
- [Providers](docs/providers.md)
- [Acquisition State Machine](docs/acquisition-state-machine.md)
- [Automation](docs/automation.md)
- [Recovery & Idempotency](docs/recovery.md)
- [Implementation & Acceptance Report](docs/luma-implementation-report.md)
- [NAS Update Steps](DEPLOY_NAS.md)

## Security Notes

- Uploaded Python scripts are equivalent to arbitrary code execution; run only scripts you trust.
- Before exposing Luma to anything beyond your local network, enforce access control (e.g., LAN-only access, or a reverse proxy with authentication).

## Disclaimer

This project is for personal study and research only. Make sure your usage complies with local laws and the terms of the websites you visit; downloaded content should respect copyright and be limited to personal, reasonable use.

## License

This project does not currently carry an open-source license. You may not copy, modify, or distribute it without the author's explicit permission.

## Contributing

Issues and pull requests are welcome. For code changes, please make sure `cargo test`, `cargo build`, `npm run typecheck`, and `npm run build` all pass.
