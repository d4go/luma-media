# Luma 架构

## 核心原则

`Media` 是跨来源、获取和媒体库的中心实体。任何手动操作、Python 脚本或自动化规则都只能通过 `AcquisitionService.request` 创建获取。Provider 只提供能力，不拥有业务状态。

```text
全局搜索 / 自动化 / Python 来源
              │
              ▼
 SourceProvider Registry ── multiple JavBus / Script adapters
              │
              ▼
        Media + Resource
              │
        ResourceRanker
              │
              ▼
    AcquisitionService.request
              │
              ▼
 qBittorrent ── 5 秒对账 ── 文件整理 ── MetaTube ── LibraryItem
              │
              └──────────── AttentionItem + 恢复动作
```

## 后端模块

- `product.rs`：新产品领域 API、ResourceRanker、Acquisition 状态机、Provider 配置、自动化、恢复和入库。
- `qbittorrent.rs`：Download Provider 的 Web API 客户端。
- `provider.rs`：MetaTube Metadata Provider 客户端。
- `crawler.rs`：兼容的脚本 Source Provider 适配器，结果进入统一获取服务。
- `scanner.rs` / `watcher.rs`：既有文件扫描与目录事件监听。
- `scheduler.rs`：5 秒 Acquisition 对账，以及目录、脚本、Tracker 和自动化周期任务。
- `storage.rs`：SQLite 连接、迁移和旧任务存储。

## 前端

Vue Router 固定六个一级入口。页面只通过 `/api/v1` 读取业务语义数据。SSE `/events` 用于刷新 Acquisition 和 Attention；10 秒轮询只用于三项服务健康状态。

旧目录、刮削任务和脚本页面保留在 `/settings/legacy/*`，用于兼容和诊断。

## 部署路径

- qBittorrent 容器和 Luma 容器应把同一宿主目录映射为 `/downloads`。
- Luma 将 qB 返回的 `qbittorrent_save_path` 前缀映射为 `download_root`。
- 目标只允许写入 `media_root`。文件名会清理非法路径字符，父目录和绝对路径片段会被拒绝。
- 默认使用硬链接；跨文件系统失败时自动复制。已有目标不会被覆盖。
