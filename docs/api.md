# HTTP API

基础路径为 `/api/v1`，JSON 字段使用 camelCase。

## 产品

- `GET /home`：首页摘要、最近获取和快捷入口。
- `GET /search?q=`：聚合作品、演员和 ProviderReport。
- `GET /catalog/media?q=`：中心 Media 列表。
- `GET /catalog/media/{id}`：作品、演员、排序后资源、最近获取和入库关联。
- `GET /catalog/media/{id}/resources`：服务端排序后的 Resource。
- `POST /catalog/media/{id}/acquire`：创建或复用活动 Acquisition。请求体可带 `resourceId`。
- `GET /actors`、`GET /actors/{id}`：演员聚合。
- `POST|DELETE /actors/{id}/follow`：关注或取消关注。

## 获取与待处理

- `GET|POST /acquisitions`
- `GET /acquisitions/{id}`
- `POST /acquisitions/{id}/pause|resume|retry|cancel`
- `GET /attention`、`GET /attention/{id}`
- `POST /attention/{id}/action`，请求体为 `{ "action": "retry" }` 等。
- `GET /events`：SSE。数据包包含 `event`、`data` 和 `at`。

## 媒体库

- `GET /library?q=`、`GET /library/{id}`
- `POST /library/{id}/reorganize`

## 自动化

- `GET|POST /automations`
- `PUT|DELETE /automations/{id}`
- `POST /automations/{id}/enabled`

规则输入示例：

```json
{
  "name": "关注演员的新资源",
  "enabled": true,
  "triggerType": "FOLLOWED_ACTOR_UPDATE",
  "triggerConfig": {},
  "conditions": { "minScore": 80, "requireSubtitle": true },
  "actionType": "ACQUIRE",
  "actionConfig": {},
  "mode": "CONFIRM"
}
```

## Provider 与设置

- `GET /providers`
- `POST /providers`（添加 JavBus 来源）
- `PUT /providers/{key}`
- `DELETE /providers/{key}`（仅 Source Provider）
- `POST /providers/{key}/enabled`
- `POST /providers/{key}/test`
- `GET|PUT /product-settings`
- `GET|PUT /settings`：兼容设置接口，响应不包含凭据正文。
- `GET /status`：Luma、MetaTube 和 qBittorrent 健康状态。

## 兼容接口

`/folders`、`/tasks`、`/media`、`/crawlers`、`/crawler-runs`、`/crawler-results`、`/downloads` 和 `/logs` 保留。`POST /crawler-results/{id}/download` 已改为创建 Acquisition，不再直接操作 qBittorrent。
