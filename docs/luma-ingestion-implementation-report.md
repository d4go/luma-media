# Luma 本地索引与采集闭环实施报告

日期：2026-08-10

本报告记录 `703c3f3` 起的采集闭环改造及 Phase 9 产品界面。报告以实际代码、SQLite 迁移、自动测试和 NAS 运行结果为准，不把外部站点可达性推断成成功。

## 1. 实际变更范围

主要新增或修改如下：

- `backend/migrations/011_local_catalog_sync.sql` 至 `018_library_export.sql`
- `backend/src/fetch/`：HTTP、Chromium、页面分类、自动路由和持久浏览器会话
- `backend/src/providers/`：JavDB、JavBus、Jav321、JavLibrary 适配器、运行状态和资源解析
- `backend/src/ingestion/`：持久任务队列、Discovery、Hydration、Snapshot 和资源写入
- `backend/src/metadata/`：来源记录、字段 provenance 和 canonical resolver
- `backend/src/resource/`：资源仓储、排序和来源映射
- `backend/src/export/`：DB-first NFO 与本地图片导出
- `backend/src/product.rs`、`crawler.rs`、`scheduler.rs`、`scanner.rs`、`main.rs`
- `frontend/src/api.ts`、`types.ts`、`styles.css`
- `frontend/src/views/ResourcesView.vue`、`MediaDetailView.vue`、`LibraryDetailView.vue`、`ProductSettingsView.vue`
- `Dockerfile`、`docker-compose.yml`

Phase 9 没有新增数据库迁移。它复用 011 至 018 的状态、任务、来源、资源和导出表，增加按番号查漏、资源刷新别名接口、媒体库重匹配接口及完整管理界面。

## 2. 数据库迁移

| 迁移 | 作用 |
| --- | --- |
| 011 | 来源同步状态与运行记录、本地目录项、规范化搜索文档、FTS5 trigram 索引和资源来源身份去重 |
| 012 | Provider runtime 状态及 `http/browser/auto` FetchMode |
| 013 | 原始响应快照，可从快照离线重新解析 |
| 014 | 可恢复的持久 ingestion job 队列、租约、优先级和活动任务去重 |
| 015 | Discovery 与 Hydration 分离、Bootstrap checkpoint、暂停与恢复、增量 overlap |
| 016 | Resource 完整字段、info hash 指纹去重和多来源 provenance |
| 017 | Metadata source record、字段级 provenance 和 canonical merge |
| 018 | canonical 本地图片与媒体库导出状态 |

已发布迁移没有被改写。迁移测试会从空库完整执行到 018；NAS 真实数据升级也保留原数据库和 WAL/SHM 的部署前备份。

## 3. Browser runtime

BrowserFetcher 使用镜像内真实 Chromium，不实现 CAPTCHA 绕过、隐身补丁或 challenge solver。每个 Provider 使用独立的 user data directory：

```text
/data/browser-profiles/<provider_key>
```

容器把 `/data` 挂载到 NAS 的 `/vol3/1000/docker/luma-data`，因此 Cookie、LocalStorage 和 Chromium profile 会跨容器重建保留。无桌面 NAS 通过 Luma 提供的浏览器会话入口建立人工会话；会话完成后由同一 profile 执行后台诊断和抓取。启动前会清除 Chromium 异常退出遗留的 profile lock，但不会清除登录数据。

当前真实配置中 JavDB 与 JavBus 的 active FetchMode 均为 `browser`。内置默认值是 JavDB `browser`、JavBus `http`，管理员可改成 `auto/http/browser`；运行时状态使用 `ready/degraded/cooldown/interaction_required/unavailable`。

## 4. Live Provider Access Gate

2026-08-10 20:16 至 20:17（Asia/Shanghai）在 NAS 容器中调用真实 Provider 诊断接口，结果如下：

| Provider | 结果 | FetchMode | status | PageKind | final URL | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| JavBus | PASS | browser | 浏览器接口无独立 HTTP status | `valid_content` | `https://www.javbus.com/` | 用户完成年龄确认后，持久 profile 可重新加载真实列表页；runtime 为 `ready` |
| JavDB | PASS | browser | 浏览器接口无独立 HTTP status | `valid_content` | `https://javdb.com/` | 用户完成人工交互后，持久 profile 可重新加载真实站点内容；runtime 为 `ready` |

容器更新到 `9f3601864144f0da1142260a617aabd28f9bef7a` 后再次诊断，两套持久 profile 均无需重新交互即可返回 `valid_content`：JavBus 用时 1904 ms，JavDB 用时 681 ms，runtime 均为 `ready`。这证明 `/data/browser-profiles/<provider_key>` 在容器重建后被重新加载。

同版本 NAS 上执行了真实番号 STARS-134 的 on-demand 闭环：

- 本地 `GET /search?q=STARS-134` 初始返回空结果，证明普通搜索没有访问外部 Provider。
- `POST /catalog/resolve` 创建 job 72 至 75，优先级为用户 on-demand 优先级。
- JavBus job 73 一次完成真实 `detail -> Raw Snapshot -> metadata/resource -> local DB`。
- JavDB job 74 第一次写入遇到 SQLite `database is locked`，持久队列 60 秒后自动重试并成功，没有丢任务。
- Jav321 对该番号无精确结果，最终独立失败；JavLibrary 返回真实 `HTTP 403 Forbidden`，最终独立失败。两者均未阻断 JavBus、JavDB 或本地搜索。
- canonical media id 为 292，收录 2 条 Metadata source record、36 个按 info hash 去重的 Resource、48 条 Resource source mapping。JavBus 与 JavDB 的字段 provenance 和来源页面均可在作品详情页查看。
- 媒体库 item 3 原来错误关联到 `STARS-00134`，现已重匹配到 media 292 / `STARS-134`；原 NFO 先备份到部署备份目录。
- 生成的 `/media/STARS-134.nfo` 通过 XML 解析，根节点为 `movie`，unique id 为 `STARS-134`。第二次重建前后 raw snapshot 均为 100 条、metadata source record 均为 31 条，证明 NFO 重建没有访问外部网站。

## 5. 同步、checkpoint 与优先级

三种入口共用 ingestion job 与后续解析管线：

- Bootstrap：历史回填，优先级 100；分页提交 checkpoint，可在当前批次后暂停并继续。
- Incremental：日常增量，优先级 800；默认使用最近 3 天 overlap window，重复运行依赖来源身份与内容 hash 幂等。
- On-demand：用户明确按完整番号查漏，优先级 1000；本地搜索未命中时才由用户触发，不由普通搜索隐式访问外网。

Bootstrap checkpoint 实际 JSON 结构示例：

```json
{
  "mode": "bootstrap",
  "nextUrl": null,
  "page": 2,
  "from": "2024-01-01",
  "to": "2026-08-10",
  "includeResources": false,
  "hasMore": false
}
```

服务启动时会回收过期 job lease，并从已提交的 checkpoint 继续；活动 `dedupe_key` 唯一索引避免同一批任务重复排队。On-demand 使用 `catalog-resolve:<provider>:<normalized_code>`，重复点击复用活动任务。

## 6. 本地搜索与离线重解析

`GET /search` 只查询 Luma SQLite，不再在读请求中访问 JavBus、JavDB 或其他外站。查询顺序以规范化番号精确命中为最高优先级，再匹配作品别名、演员别名和 FTS5 trigram。中日英作品名及演员名分别进入别名表和搜索文档。

每次成功抓取尽可能保存 `provider_raw_snapshot` 的 body hash、文件路径、来源 URL、最终 URL、FetchMode、页面状态和 parser version。`POST /providers/{key}/reparse` 从快照重新解析，不重新请求来源网站。

## 7. Resource 去重与 provenance

资源不再作为 Media 的单一 magnet 字段。一个 Media 可以有多个 Resource，一个 Resource 可以通过 `resource_source_mapping` 关联多个来源。

去重键优先级：

1. 磁力链接的 BTIH：`btih:<lowercase-info-hash>`
2. 稳定来源资源 ID：`source:<provider-identity>`
3. 规范化下载 URL 指纹

同一 torrent 从多个来源出现时复用 Resource，并合并标题、大小、字幕、清晰度、codec、tracker、可用性、首次/最近发现及验证时间；来源证据单独保留。资源刷新由显式 API 排队，不阻断已有资源展示和创建 Acquisition。

## 8. Metadata merge policy

每个来源详情先写入 `metadata_source_record`，再由 resolver 按字段选择 canonical 值。选择依据包含管理员配置的 `metadataPriority`、证据级别、值完整度和来源更新时间。`metadata_field_provenance` 记录每个 canonical 字段最终来自哪个 source record。低优先级或空值不会无条件覆盖高优先级有效值。

actors、作品别名和演员别名采用合并语义；Provider entity mapping 是身份主证据，普通同名别名只用于搜索候选，不单独作为强制合并依据。

## 9. Library 与 MetaTube 边界

Luma 直接从 canonical DB 生成 Kodi/Jellyfin 可读 NFO，并可从本地缓存同步 poster/backdrop。NFO 生成器不访问网络。媒体库扫描先从文件名提取番号并匹配 Luma DB；详情页可以手动重匹配，再离线重建 NFO 和图片。

MetaTube 只保留为兼容降级路径：当下载完成后 canonical media 缺少关键字段时可尝试补充。MetaTube 失败不会阻止 LibraryItem 提交，已在 Luma DB 明确识别的作品不会为了生成 NFO 再次依赖 MetaTube。

## 10. 已完成验证

- `cargo test -q`：80/80 通过。
- `cargo build -q`：通过。
- `npm run typecheck`：通过。
- `npm run build`：通过，Vite 生产包成功生成。
- 自动测试覆盖：本地多语言搜索不访问外网、Bootstrap checkpoint 暂停/继续与启动恢复、持久 worker 增量同步和幂等、按番号查漏高优先级与去重、Resource 多来源去重、Metadata 优先级解析、离线 NFO 导出、qBittorrent 状态对账和重复 Acquisition。
- NAS 镜像 build、SQLite migration 18、`/api/v1/health`、三个持久挂载和浏览器渲染均通过；资源页、作品详情、来源状态与同步进度、媒体库重匹配及 NFO 操作在真实数据上可见。
- Luma 重启期间 qBittorrent 未重启。STARS-123 在切换前约 41.82%，切换后约 41.93%，状态保持 `DOWNLOADING/downloading`。
- 当前编译仍有四项未接入 trait 数据模型的 `dead_code` 警告，不影响构建或运行；没有把它们记成零警告。

## 11. 已知限制

- Provider HTML 结构改变后，对应结构化 parser 仍需维护。单个 Provider 失败只更新其 runtime 状态和任务错误，不影响本地搜索及其他来源。
- 人工验证由真实 Chromium profile 保存，外站将来重新要求验证时仍会回到 `interaction_required`，需要用户再次建立会话。
- FTS5 trigram 对少于 3 个字符没有 token；1 至 2 字符查询使用受限的精确、前缀和 LIKE 回退。
- 历史媒体库中有旧版本把标题误当番号形成的错误关联，需要通过详情页“重新匹配”逐项修正；系统不会在没有可靠番号时自动猜测并覆盖。
- Jav321 对部分番号可能没有精确结果；JavLibrary 当前真实返回 `HTTP 403 Forbidden`。两项故障均会留在各自任务和 Provider 状态中，不会回退为前台搜索时实时并发访问。
