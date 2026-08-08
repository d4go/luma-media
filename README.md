# Luma

Luma 是面向家庭 NAS 的媒体获取与入库编排器。它把作品发现、资源选择、qBittorrent 下载、MetaTube 元数据、文件整理和媒体库提交连接为一条可追踪、可恢复的流程。

## 产品能力

- 全局搜索：同时查找作品和演员，内置 Jav321、JavDB、JavBus、JavLibrary 适配器；每种来源都能配置多个实例并发聚合。
- 资源排序：服务端按字幕、清晰度、大小、时间和来源统一评分，并返回排序理由。
- 获取状态机：从请求、排队、下载、整理、元数据到入库，每次转换都有持久化事件。
- qBittorrent：下载提交、进度对账、暂停、继续、取消和定时 Tracker 更新。
- MetaTube：默认地址为当前部署机器 IP 加 `8080`，负责元数据、演员、NFO 和海报。
- 自动化：使用 WHEN / IF / THEN 描述规则，支持 AUTO、CONFIRM 和 NOTIFY 三种模式。
- 待处理问题：外部服务离线、路径映射失败和文件冲突都会给出明确恢复动作。
- 媒体库：只展示已经完成整理和提交的文件，并保留来源 Acquisition。
- 兼容工具：媒体目录监听、周期扫描和可信 Python 来源脚本位于设置的高级诊断区域。
- 服务状态：Luma、MetaTube 和 qBittorrent 每 10 秒刷新。

## 信息架构

一级导航固定为：首页、资源、下载、媒体库、自动化、设置。产品操作统一称为“获取”。旧任务、爬虫和 qBittorrent 原始列表不再作为一级页面。

## 本地开发

需要 Rust 1.85+、Node.js 22+、npm 和 Python 3。

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

## Docker

Luma 需要三个持久化或共享挂载：

- `/data`：SQLite、海报缓存和脚本运行数据。
- `/downloads`：必须与 qBittorrent 共享同一宿主机下载目录。
- `/media`：最终媒体库目录，需要读写权限。

```bash
MEDIA_ROOT=/path/to/media \
DOWNLOAD_ROOT=/path/to/downloads \
LUMA_DATA_ROOT=/path/to/luma-data \
docker compose up -d --build
```

访问 `http://localhost:3000`。qBittorrent 的保存路径和 Luma 的映射路径默认均为 `/downloads`。

## Python 来源脚本

设置中的“Python 来源脚本”支持上传不超过 1 MiB 的 `.py` 文件、绑定网站、定时循环执行并持久化 stdout、stderr 和结构化结果。脚本应向 stdout 或 `LUMA_RESULT_PATH` 输出：

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

脚本结果会先归一化为 Media 和 Resource，再调用统一的 AcquisitionService，不会直接绕过状态机操作 qBittorrent。只上传你信任的脚本。

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
