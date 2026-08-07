# 自动化

规则由三个可读部分组成：

- WHEN：新资源、关注演员的新资源或计划检查。
- IF：最低评分、Provider、关键词和字幕要求。
- THEN：当前为 ACQUIRE，并选择执行模式。

执行模式：

- `AUTO`：直接调用 `AcquisitionService.request`。
- `CONFIRM`：创建 AttentionItem，用户确认后调用同一服务。
- `NOTIFY`：创建可关闭通知，不创建获取。

`automation_execution` 使用 `(rule_id, event_key)` 唯一约束避免同一资源重复触发，并保存输入、解释、输出、错误和关联 Acquisition。没有任何规则时，手动搜索与获取完全可用。
