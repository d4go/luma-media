# Provider 设计

## 类型

- Source Provider：搜索作品、详情、演员和资源。首个内置实现为 JavDB；Python 脚本通过适配器提供资源。
- Download Provider：提交、暂停、继续、取消和查询下载。首个实现为 qBittorrent。
- Metadata Provider：搜索和获取结构化元数据、海报。首个实现为 MetaTube。

`provider_config` 保存启停、地址、配置、加密边界内的凭据字段和最后一次健康状态。API 只返回 `hasSecret`，不会返回凭据正文。

## JavDB

默认地址 `https://javdb.com`。搜索会归一化到 `media` 和 `provider_entity_mapping`；详情按需加载演员和磁力资源。受限环境可在设置中填写 Cookie。来源超时或失败时，统一搜索仍返回本地和其他来源结果，并附带 ProviderReport。

## MetaTube

默认使用当前部署主机 IP 与 8080 端口。Luma 使用 `/v1/providers`、`/v1/movies/search` 和 `/v1/movies/{provider}/{id}`。入库时优先补全标题、简介、日期、演员和海报；失败会写 `metadata_record_v2`，使用基础元数据继续入库，之后可重刮。

## qBittorrent

要求 Web UI 可从 Luma 容器访问。提交时传入保存路径、分类、标签和 Resource 自带 Tracker。后台每 5 秒读取 torrent 列表并更新业务进度。Tracker 订阅任务只追加有效 HTTP、HTTPS 和 UDP 地址。
