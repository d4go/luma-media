# 恢复、幂等与安全边界

## 启动恢复

- 尚未获得 qB hash 的 REQUESTED、RESOURCE_RESOLVING 和 QUEUED 会重置为 REQUESTED 并重新提交。
- DOWNLOADED、PROCESSING、METADATA 和 LIBRARY_COMMIT 会重置为 DOWNLOADED 并重新执行幂等处理。
- DOWNLOADING 在首次 5 秒对账时继续同步 qBittorrent。
- qB 任务连续缺失 90 秒后进入 Attention。
- Python 中断运行沿用旧恢复逻辑，标记失败并保留 stdout、stderr。

## 幂等

- 活动 Acquisition 对 Media 唯一。
- AcquisitionEvent 对 `(acquisition_id, event_key)` 唯一。
- AutomationExecution 对 `(rule_id, event_key)` 唯一。
- Media 使用 normalized_code 唯一，Resource 优先使用 info_hash 唯一。
- LibraryItem 的 video_path 唯一，目标文件存在时不覆盖。

## 凭据

设置读取会清空旧接口的 Token 和密码；Provider API 只返回 `hasSecret`。保存时空凭据表示保持原值。凭据不会写入日志、前端状态快照或部署脚本。

## 路径

下载源必须在 `download_root` 的 canonical 路径内；目标必须在 `media_root` 内，拒绝父目录、根目录和平台前缀片段。目录扫描不跟随整理阶段的符号链接。下载路径无法映射时进入 Attention。

## 网络

Provider 地址只接受 HTTP 或 HTTPS。MetaTube 图片客户端禁止自动重定向并限制为 25 MiB 和已知图片类型。生产前端同源访问 API，后端不再默认开放任意 CORS 来源。
