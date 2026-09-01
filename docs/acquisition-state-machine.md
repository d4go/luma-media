# Acquisition 状态机

## 主路径

```text
REQUESTED
  → RESOURCE_RESOLVING
  → QUEUED
  → DOWNLOADING
  → DOWNLOADED
  → PROCESSING
  → METADATA
  → LIBRARY_COMMIT
  → COMPLETED
```

任一非终态可进入 `NEEDS_ATTENTION` 或 `CANCELLED`。`NEEDS_ATTENTION` 可通过重试返回 `REQUESTED`；下载已完成但整理失败时，重试从 `DOWNLOADED` 恢复，避免重复下载。

## 不变量

- 同一 Media 同时最多存在一个活动 Acquisition，由 SQLite 部分唯一索引保证。
- 状态更新和 `acquisition_event` 在同一事务提交。
- 非法跨阶段转换会返回 400。
- `COMPLETED` 必须关联 `library_item_id`。
- qBittorrent 任务连续 90 秒缺失会进入待处理，不会静默停滞。
- 文件目标已存在时不覆盖，转入待处理。

## 事件

每条事件包含 `event_key`、前后状态、用户可读消息、JSON payload 和时间。SSE 会广播创建、进度、状态转换和待处理事件；数据库事件才是审计真相，SSE 只负责界面即时刷新。
