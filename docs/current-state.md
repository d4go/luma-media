# Luma 当前状态审计

审计基线：`luma-codex-full-product-spec.md`，审计日期：2026-08-08。

## 结论

当前仓库已经具备可运行的 Vue 3 管理端、Rust API、SQLite、MetaTube 刮削、qBittorrent 控制、Python 脚本调度、媒体目录扫描和监听能力，但仍是多个工具页面的组合，不是规格书要求的以 `Media` 和 `Acquisition` 为中心的完整产品。

最关键的结构性缺口是：爬虫结果会直接提交到 qBittorrent，系统没有持久化的获取状态机、资源统一排序、文件整理、入库事务、待处理问题和重启恢复链路。因此现有下载过程无法回答“为什么获取、进行到哪一步、失败后如何恢复、最终是否已进入媒体库”。

## 当前技术栈

- 后端：Rust 2024、Axum 0.8、Tokio、SQLx、SQLite、Reqwest。
- 前端：Vue 3、TypeScript、Vite、Naive UI、Tabler Icons。
- 部署：多阶段 Docker 构建，单容器同时提供 API 和静态前端。
- 集成：MetaTube HTTP API、qBittorrent Web API、用户上传的 Python 脚本。

## 已有能力

### 媒体目录

- 支持手动、周期和 `watch` 三种扫描模式。
- `watch` 使用操作系统文件事件，周期扫描作为漏事件兜底，目录监听本身可用。
- 扫描器递归发现视频文件，写入 `media_item`，并检测同目录的 NFO 和海报。
- MetaTube 刮削可以写入 NFO、JSON、海报和背景图。

### 脚本与下载

- 支持上传 Python 脚本、网站地址、循环周期、启停、立即执行。
- 脚本 stdout、stderr、运行记录和结构化结果会持久化。
- qBittorrent 支持登录、版本测试、提交磁力链接、暂停、继续、删除和 Tracker 更新。
- Tracker 订阅可按周期追加到现有种子。

### 运维

- MetaTube 默认地址会根据当前部署主机生成 `http://<host>:8080`。
- Luma、MetaTube 和 qBittorrent 状态可探测，前端每 10 秒刷新。
- Docker 数据库、资源缓存和脚本目录位于 `/data`，媒体目录位于 `/media`。

## 与目标产品的差距

### 领域模型

当前 `media_item` 表示已经被扫描到的本地文件，并不是跨来源、下载和入库阶段共享的中心 `Media`。仓库中不存在以下一等实体：

- `Media`、`Actor`、`MediaActor`、`ProviderEntityMapping`
- `Resource` 和后端统一资源排序
- `Acquisition`、`AcquisitionEvent` 和合法状态转换
- `LibraryItem` 和入库提交记录
- `AttentionItem` 和可执行恢复动作
- `AutomationRule`、`AutomationExecution`
- 可统一启停、测试和报告能力的 `ProviderConfig`

### Provider 边界

- MetaTube 和 qBittorrent 是具体客户端，没有实现统一 Provider 契约。
- 原实现没有内置可扩展的多 SourceProvider 注册表。
- Python 脚本是独立功能，不会先归一化为 Media 和 Resource。
- 搜索、详情、演员、资源和来源部分失败没有统一聚合协议。

### 获取主链路

- `crawler::download_result` 与脚本的 `auto_download` 直接调用 qBittorrent，绕过业务状态机。
- 没有防止同一 Media 重复活动获取的幂等约束。
- qBittorrent 进度没有与持久化业务对象同步。
- 下载完成后没有受控的硬链接或复制、命名、冲突处理、元数据和媒体库提交链路。
- 启动恢复只会把中断脚本标记失败，不会对账 qBittorrent 或恢复媒体处理。

### 自动化

现有“自动化”实质是脚本周期和 `auto_download` 开关。它不具备 WHEN / IF / THEN、AUTO / CONFIRM / NOTIFY、执行解释、去重键和人工确认。

### 前端信息架构

现有一级导航包含 Dashboard、资源发现、下载中心、任务中心、媒体库、自动化规则和系统设置。规格书要求固定为：首页、资源、下载、媒体库、自动化、设置，并由全局搜索贯穿。任务、爬虫和执行器只能作为详情或高级诊断出现。

页面目前直接展示 qBittorrent 下载和爬虫结果，尚未展示 Acquisition 阶段、来源失败、排序理由、演员聚合、待处理问题、自动化解释和恢复动作。

### 安全与边界

- 设置读取接口会返回 MetaTube Token 和 qBittorrent 密码明文。
- CORS 当前允许任意来源。
- 用户上传 Python 是任意代码执行能力，虽有限制文件大小、并发和超时，但仍应明确只用于可信脚本。
- Provider URL、远程资源 URL 和媒体路径需要更严格的 SSRF 与目录边界约束。

## 兼容迁移策略

本轮实现保留旧表和旧接口，新增独立领域表及服务层：

1. 从现有 `media_item` 回填中心 `media` 与 `library_item`。
2. 从 `crawler_result` 归一化 `media` 与 `resource`。
3. 所有手动获取、脚本自动获取和规则自动获取统一调用 `AcquisitionService.request`。
4. 旧 `/downloads` 接口继续作为 qBittorrent 诊断接口；产品页面改用 `/acquisitions`。
5. 旧任务、目录和脚本页面移入设置的高级区域，不再作为一级导航。
6. 只增加新迁移，不修改已部署的 001 至 005 迁移校验。

## 验收重点

- 没有任何自动化规则时，用户仍可搜索、查看详情、选择资源并完成获取。
- 服务重启后，活动 Acquisition 会与 qBittorrent 对账并继续处理或进入待处理。
- 下载完成后的文件只能写入配置允许的下载根目录和媒体根目录，冲突不得静默覆盖。
- Provider 离线或部分失败时，其余来源仍可返回，界面必须明确说明降级状态。
- 设置读取不再泄露已保存的密码、Token 或 Cookie。
