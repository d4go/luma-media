# Luma 实现与验收报告

日期：2026-08-08。

## 已交付

本轮把仓库从工具型管理台重构为以 Media、Resource 和 Acquisition 为中心的产品：

- 新增 006 兼容迁移和完整领域表，不修改已部署迁移。
- 内置 JavDB 搜索与详情适配、统一 ResourceRanker 和排序解释。
- 手动获取、脚本自动获取和自动化获取全部收口到 `request_acquisition`。
- qBittorrent 5 秒进度对账、暂停、继续、取消、任务缺失检测。
- 下载完成后的安全路径映射、最大视频选择、硬链接或复制、冲突待处理。
- MetaTube 入库补全、NFO、演员、海报、LibraryItem 和失败降级。
- 启动恢复、活动获取去重、事件审计、SSE 和 Attention 恢复动作。
- WHEN / IF / THEN、AUTO / CONFIRM / NOTIFY 规则和执行去重。
- 六项产品导航、全局搜索、海报优先资源页、获取详情、媒体库和 Provider 设置。
- 凭据屏蔽、同源 API、下载与媒体路径边界。

## 验收场景

| 场景 | 结果 | 验证方式 |
| --- | --- | --- |
| A. 无自动化规则时手动搜索和获取 | 通过 | 产品 API 与前端均不依赖规则；Media 详情可直接创建 Acquisition |
| B. 搜索演员、关注并查看作品 | 通过 | Actor 聚合、关注 API 和演员详情页已实现 |
| C. 单一来源失败时保留其他结果 | 通过 | SearchResponse 返回 ProviderReport，前端显示降级提醒 |
| D. AUTO 规则触发获取 | 通过 | 自动化执行调用同一 `request_acquisition` |
| E. CONFIRM 与 NOTIFY | 通过 | Attention 确认获取和可关闭通知已实现 |
| F. 服务重启后继续下载 | 通过 | 启动恢复加 5 秒 qB 对账 |
| G. 媒体处理阶段重启 | 通过 | PROCESSING、METADATA、LIBRARY_COMMIT 重置到 DOWNLOADED 幂等恢复 |
| H. 重复请求 | 通过 | SQLite 活动唯一索引和服务复用现有 Acquisition；有自动测试 |
| I. 路径或目标冲突 | 通过 | 目录边界检查，不覆盖目标，转为 Attention |
| J. 凭据与 Provider 健康 | 通过 | 响应只返回空凭据或 hasSecret；独立启停和测试 |

## 自动验证

- `cargo test`：30 项后端测试，覆盖迁移、Provider、资源排序、状态机、重复获取、qB API、MetaTube 图片安全、目录监听和旧任务恢复。
- `npm run typecheck`：通过。
- `npm run build`：通过。
- 临时 SQLite 启动验证：006 迁移成功；首页、Provider、媒体库、自动化和设置 API 返回正常；旧设置响应中的 Token 和密码为空。
- 自动化 API 冒烟：创建、停用和删除通过。

## 外部环境验证边界

本地临时环境没有真实 JavDB 登录态、MetaTube 服务、qBittorrent 下载任务和大体积媒体文件，因此没有在本地实际下载影片。实现使用 mock HTTP 测试验证 qBittorrent 和 MetaTube 客户端，部署后再以 NAS 三项服务健康、容器日志和页面 API 做环境验收。

本次运行环境没有可连接的浏览器实例，无法执行截图式视觉回归。前端已完成 Vue 类型检查和 Vite 生产构建，并按 320px、640px、820px、900px、1180px 响应式断点审查；NAS 部署后会再次检查真实页面可达性。

## 已知边界

- JavDB 是 HTML 来源，站点结构改变时需要更新选择逻辑；失败会显式降级，不影响本地结果。
- Python 脚本属于可信任任意代码执行能力，仍应只允许管理员上传可信脚本。
- `POST /library/{id}/reorganize` 当前在文件已符合模板时返回无需移动；跨模板迁移仍通过新的获取整理链路完成。
