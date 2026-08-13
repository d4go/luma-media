# Luma

[English](README.en.md) | **中文**

> 面向家庭 NAS 的媒体获取与入库编排器：把作品发现、资源选择、下载、元数据刮削、文件整理和媒体库提交连接成一条**可追踪、可恢复**的自动化流程。

Luma 解决的是家庭媒体库的"最后一公里"问题：不再需要人工在多个站点之间搜索、复制磁力链接、盯 qBittorrent 下载、再手动刮削和整理文件。Luma 把整条链路编排起来——每一次获取从哪里来、进行到哪一步、失败后如何恢复、最终是否进入媒体库，都有据可查。

## 目录

- [功能特性](#功能特性)
- [工作原理](#工作原理)
- [技术栈](#技术栈)
- [快速开始](#快速开始)
- [外部服务](#外部服务)
- [Python 来源脚本](#python-来源脚本)
- [项目结构](#项目结构)
- [文档](#文档)
- [安全说明](#安全说明)
- [免责声明](#免责声明)
- [许可证](#许可证)

## 功能特性

- **全局搜索**：同时查找作品和演员，内置 Jav321、JavDB、JavBus、JavLibrary 适配器；每种来源都能配置多个实例并发聚合，单个来源故障不影响整体结果。
- **资源排序**：服务端按字幕、清晰度、大小、时间和来源统一评分，并返回排序理由，告诉用户"为什么推荐这个资源"。
- **获取状态机**：从请求、排队、下载、整理、元数据到入库，每次状态转换都有持久化事件；服务重启后自动与 qBittorrent 对账并继续处理。
- **qBittorrent 集成**：下载提交、进度对账、暂停、继续、取消和定时 Tracker 更新。
- **MetaTube 元数据**：负责元数据、演员、NFO 和海报；默认地址为当前部署机器 IP 加 `8080`，可在设置中修改。
- **自动化规则**：使用 WHEN / IF / THEN 描述规则，支持 AUTO（自动执行）、CONFIRM（人工确认）和 NOTIFY（仅通知）三种模式。
- **待处理问题**：外部服务离线、路径映射失败和文件冲突都会给出明确的恢复动作，而不是静默失败。
- **媒体库**：只展示已经完成整理和提交的文件，并保留来源 Acquisition 记录，方便回溯。
- **兼容工具**：媒体目录监听（文件系统事件）、周期扫描和可信 Python 来源脚本位于设置的高级诊断区域。
- **服务状态**：Luma、MetaTube 和 qBittorrent 的状态每 10 秒刷新一次。

## 工作原理

一级导航固定为：**首页、资源、下载、媒体库、自动化、设置**。产品操作统一称为"获取"。

```text
发现（搜索 / 来源脚本 / 自动化规则）
        │
        ▼
资源选择（服务端统一评分 + 排序理由）
        │
        ▼
下载（qBittorrent 提交与进度对账）
        │
        ▼
文件整理（重命名 / 冲突处理 / 媒体目录）
        │
        ▼
元数据（MetaTube：NFO / 海报 / 演员）
        │
        ▼
入库（媒体库可见，保留 Acquisition 来源）
```

旧的任务、爬虫和 qBittorrent 原始列表不再作为一级页面，移入设置的高级诊断区域。

## 技术栈

| 层 | 技术 |
| --- | --- |
| 后端 | Rust 2024、Axum 0.8、Tokio、SQLx、SQLite |
| 前端 | Vue 3、TypeScript、Vite、Naive UI、Tabler Icons |
| 部署 | 多阶段 Docker 构建，单容器同时提供 API 与静态前端 |
| 集成 | MetaTube HTTP API、qBittorrent Web API、Python 来源脚本、Chromium 浏览器抓取 |

## 快速开始

### 前置依赖

- **推荐方式**：Docker 与 Docker Compose。
- **本地开发**：Rust 1.85+（edition 2024）、Node.js 22+、npm、Python 3。
- **外部服务**：qBittorrent（Web UI）和 MetaTube，见下文[外部服务](#外部服务)。

### Docker 部署（推荐）

Luma 需要三个持久化或共享挂载：

| 容器路径 | 用途 |
| --- | --- |
| `/data` | SQLite 数据库、海报缓存、浏览器配置文件和脚本运行数据 |
| `/downloads` | 必须与 qBittorrent 共享**同一宿主机下载目录**，下载完成后才能整理入库 |
| `/media` | 最终媒体库目录，需要读写权限（刮削结果会写在视频文件旁边） |

```bash
MEDIA_ROOT=/path/to/media \
DOWNLOAD_ROOT=/path/to/downloads \
LUMA_DATA_ROOT=/path/to/luma-data \
docker compose up -d --build
```

访问 `http://localhost:3000`。qBittorrent 的保存路径和 Luma 的映射路径默认均为 `/downloads`。

> 在 `docker-compose.yml` 同级创建 `.env` 文件可以持久化宿主机路径配置，参考 [`.env.example`](.env.example)。页面中的媒体目录请填写容器路径 `/media`，而不是宿主机路径。

### 本地开发

```bash
cd backend
DATABASE_URL='sqlite://luma.db?mode=rwc' cargo run
```

```bash
cd frontend
npm ci
npm run dev
```

访问 `http://localhost:5173`。Vite 会把 `/api` 代理到 `http://localhost:3000`。

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite://luma.db?mode=rwc` | SQLite 连接串 |
| `LUMA_BIND` | `0.0.0.0:3000` | HTTP 服务监听地址 |
| `LUMA_DATA_DIR` | `data` | 运行数据目录（数据库、缓存、脚本） |
| `LUMA_STATIC_DIR` | — | 前端静态文件目录（Docker 镜像内置 `/app/web`） |
| `LUMA_METATUBE_URL` | `http://<主机IP>:8080` | MetaTube 服务地址覆盖 |
| `LUMA_PYTHON_BIN` | `python3` | Python 解释器路径（来源脚本用） |
| `LUMA_BROWSER_ENABLED` | `true` | 是否启用 Chromium 浏览器抓取 |
| `LUMA_CHROMIUM_PATH` | `/usr/bin/chromium` | Chromium 可执行文件路径 |
| `LUMA_BROWSER_DATA_DIR` | `/data/browser-profiles` | 浏览器用户数据目录 |
| `LUMA_BROWSER_HEADLESS` | `true` | 无头模式 |
| `LUMA_BROWSER_SESSION_PORT` | `6080` | 浏览器会话调试端口（VNC） |
| `RUST_LOG` | `luma_server=info,tower_http=info` | 日志级别 |

Docker Compose 宿主机路径（写入 `.env`）：

| 变量 | 说明 |
| --- | --- |
| `MEDIA_ROOT` | 媒体库宿主机目录，映射到容器 `/media` |
| `DOWNLOAD_ROOT` | qBittorrent 共享下载目录，映射到容器 `/downloads` |
| `LUMA_DATA_ROOT` | 运行数据宿主机目录，映射到容器 `/data` |

## 外部服务

### qBittorrent

Luma 通过 qBittorrent Web API 提交下载、对账进度并控制任务。请确保：

1. qBittorrent 已启用 Web UI；
2. 下载目录与 Luma 的 `/downloads` 挂载是**同一个宿主机目录**；
3. 在 Luma 设置中填写 Web UI 地址、端口和账号。

### MetaTube

MetaTube 负责作品元数据、演员、NFO 和海报。默认地址为 `http://<当前部署机器IP>:8080`，也可以在设置中修改，或用 `LUMA_METATUBE_URL` 环境变量覆盖。

## Python 来源脚本

设置中的"Python 来源脚本"支持上传不超过 1 MiB 的 `.py` 文件、绑定网站、定时循环执行并持久化 stdout、stderr 和结构化结果。脚本应向 stdout 或 `LUMA_RESULT_PATH` 输出：

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

脚本结果会先归一化为 Media 和 Resource，再调用统一的 AcquisitionService，不会直接绕过状态机操作 qBittorrent。**只上传你信任的脚本**——脚本运行在 Luma 所在机器上，等同于任意代码执行。

参考示例：[`examples/sample_crawler.py`](examples/sample_crawler.py)。

## 项目结构

```text
backend/          Rust 后端（API、状态机、Provider 适配器、抓取、入库）
  migrations/     SQLite 迁移
  src/providers/  Jav321 / JavDB / JavBus / JavLibrary 适配器
frontend/         Vue 3 前端（首页、资源、下载、媒体库、自动化、设置）
docs/             架构、API、数据库、Provider、状态机等设计文档
examples/         Python 来源脚本示例
Dockerfile        多阶段构建（前端 + 后端 → 单容器）
docker-compose.yml
```

## 文档

- [当前状态审计](docs/current-state.md)
- [架构](docs/architecture.md)
- [API](docs/api.md)
- [数据库](docs/database.md)
- [Provider](docs/providers.md)
- [获取状态机](docs/acquisition-state-machine.md)
- [自动化](docs/automation.md)
- [恢复与幂等](docs/recovery.md)
- [实现与验收报告](docs/luma-implementation-report.md)
- [NAS 更新步骤](DEPLOY_NAS.md)

## 安全说明

- 上传的 Python 脚本等同于任意代码执行，只运行你信任的脚本。
- 对外暴露服务前请做好网络访问控制（例如仅内网访问、反向代理 + 认证）。

## 免责声明

本项目仅用于个人学习与研究。请确保你的使用方式符合当地法律法规以及所访问网站的条款；下载的内容请遵守版权规定，仅限个人合理使用。

## 许可证

本项目目前未附带开源许可证。在获得作者明确许可之前，不得复制、修改或分发本项目。

## 贡献

欢迎提交 Issue 和 Pull Request。涉及代码变更时，请确保 `cargo test`、`cargo build`、`npm run typecheck` 和 `npm run build` 全部通过。
