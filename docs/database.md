# 数据库

SQLite 迁移 001 至 005 保持不变。迁移 006 新增产品领域，并从旧媒体文件回填中心 Media 与 LibraryItem。

## 中心实体

- `media`：normalized_code 唯一，保存跨 Provider 的产品身份。
- `actor`、`media_actor`：演员与作品多对多关系，演员可关注。
- `provider_entity_mapping`：Provider 外部 ID 到 Media 或 Actor 的稳定映射。
- `resource`：资源地址、info hash、字幕、清晰度、大小、Tracker、评分和理由。

## 获取

- `acquisition`：当前状态、进度、qB hash、路径、错误和 LibraryItem 关联。
- `acquisition_event`：不可变时间线。
- `attention_item`：问题、严重度、动作和解决记录。

## 入库与元数据

- `library_item`：最终视频、NFO、海报和来源 Acquisition。
- `metadata_record_v2`：Media 级 Provider 原始响应或失败。
- 旧 `media_item` 继续服务目录扫描和旧刮削接口，新增入库也会建立兼容记录。

## 自动化与 Provider

- `automation_rule`：WHEN / IF / THEN 和模式。
- `automation_execution`：事件去重、解释和关联结果。
- `provider_config`：Provider 类型、启停、地址、凭据和健康状态。

## 关键约束

- `media.normalized_code` 唯一。
- `resource.info_hash` 在非空时唯一。
- 每个 Media 只能有一个非终态且非待处理的 Acquisition。
- `library_item.video_path` 唯一。
- Provider 映射、AcquisitionEvent 和 AutomationExecution 都有幂等唯一键。
