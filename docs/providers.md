# Provider 设计

## 类型

- Source Provider：搜索作品、详情、演员和资源。内置 Jav321、JavDB、JavBus、JavLibrary 适配器；每种适配器都支持多个主站、反代或镜像，Python 脚本也可提供资源。
- Download Provider：提交、暂停、继续、取消和查询下载。首个实现为 qBittorrent。
- Metadata Provider：搜索和获取结构化元数据、海报。首个实现为 MetaTube。

`provider_config` 保存启停、地址、配置、加密边界内的凭据字段和最后一次健康状态。API 只返回 `hasSecret`，不会返回凭据正文。

## JAV 多来源

首次部署会创建 Jav321、JavDB、JavBus 和 JavLibrary。当前默认启用 Jav321；JavDB 和 JavLibrary 官方站在部分网络触发 Cloudflare，默认关闭，填入浏览器取得的完整 Cookie 或换成兼容镜像后即可启用。JavBus 支持年龄验证 Cookie 与镜像地址。

设置页可以为任意适配器添加多个实例。每个来源拥有独立名称、地址、Cookie、启停和健康状态。搜索并发请求全部启用来源，将结果归一化到 `media` 和 `provider_entity_mapping` 后按番号去重；详情按需加载演员和磁力资源。来源超时、验证或单站失败时，统一搜索仍返回本地和其他来源结果，并附带独立的 `ProviderReport`。

解析行为参考了 [hyperq/jav](https://github.com/hyperq/jav)（MIT）的 JavBus 请求流程和 [sangokvip/JAV-Scraper](https://github.com/sangokvip/JAV-Scraper)（MIT）的 JavDB/Jav321 适配方式；多来源边界参考 [Yuukiy/JavSP](https://github.com/Yuukiy/JavSP)（GPL-3.0），实现代码为独立编写。

## MetaTube

默认使用当前部署主机 IP 与 8080 端口。Luma 使用 `/v1/providers`、`/v1/movies/search` 和 `/v1/movies/{provider}/{id}`。入库时优先补全标题、简介、日期、演员和海报；失败会写 `metadata_record_v2`，使用基础元数据继续入库，之后可重刮。

## qBittorrent

要求 Web UI 可从 Luma 容器访问。提交时传入保存路径、分类、标签和 Resource 自带 Tracker。后台每 5 秒读取 torrent 列表并更新业务进度。Tracker 订阅任务只追加有效 HTTP、HTTPS 和 UDP 地址。
