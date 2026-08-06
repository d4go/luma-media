# Luma Media

Luma Media 是一个桌面优先的媒体元数据管理服务。项目按上级目录中的 UI、架构、数据库和 API 规格实现，包含 Vue 3 管理端、Rust Axum API、SQLite 存储与 Docker 部署。

## 已实现功能

- 概览：媒体数、任务数、成功数、失败数和最近活动
- 媒体目录：新增、编辑、删除、启停和立即扫描
- 扫描器：递归识别常见视频格式，生成文件指纹并更新媒体索引
- 任务中心：状态、进度、详情、取消与失败重试
- 媒体库：搜索、元数据状态与手动刮削任务
- 系统设置：MetaTube 地址、可选 Token、连接测试、输出格式、扫描间隔、覆盖策略和日志级别
- MetaTube：调用官方搜索与详情接口，写入 NFO/JSON、海报和背景图，并记录真实任务错误
- 调度器：按扫描间隔处理 `interval` 和 `watch` 模式目录
- 明暗主题、移动端导航、加载、空数据和错误状态

## 目录结构

```text
luma-media/
├── backend/       Rust、Axum、SQLx、SQLite migrations
├── frontend/      Vue 3、TypeScript、Naive UI
├── Dockerfile
└── docker-compose.yml
```

## 本地开发

需要 Node.js 22+、npm 和 Rust 1.85+。

终端一：

```bash
cd backend
DATABASE_URL='sqlite://luma-media.db?mode=rwc' cargo run
```

终端二：

```bash
cd frontend
npm install
npm run dev
```

访问 `http://localhost:5173`。Vite 会把 `/api` 请求代理到 `http://localhost:3000`。

## Docker

在项目目录运行 Compose。默认使用飞牛 NAS 路径 `/vol1/1000/a1` 作为媒体目录、`/vol3/1000/docker/luma-media-data` 保存数据库；也可以用环境变量改掉。媒体目录必须以读写方式挂载，因为刮削结果会落在视频旁边：

```bash
MEDIA_ROOT=/path/to/your/media \
LUMA_DATA_ROOT=/path/to/luma-data \
docker compose up -d --build
```

访问 `http://localhost:3000`。容器内的媒体路径以 `/media` 开头，例如 `/media/movies`。

以 `/media/movies/ABC-123.mp4` 为例，输出文件为：

```text
ABC-123.nfo
ABC-123.metadata.json    # 输出格式为 json 或 both 时
ABC-123-poster.jpg       # 扩展名取决于远程图片格式
ABC-123-fanart.jpg
```

覆盖策略为 `missing` 或 `never` 时保留已存在文件，只补缺失文件；选择 `always`，或在单次刮削时勾选覆盖选项，才会重写对应文件。

## API

所有接口以 `/api/v1` 开头：

- `GET /dashboard`
- `GET|POST /folders`
- `PUT|DELETE /folders/{id}`
- `POST /folders/{id}/scan`
- `GET /tasks`、`GET /tasks/{id}`
- `POST /tasks/{id}/retry`、`POST /tasks/{id}/cancel`
- `GET /media`、`POST /media/{id}/scrape`
- `GET|PUT /settings`
- `POST /settings/metatube/test`
- `GET /logs`
- `GET /health`

## MetaTube 连接

系统使用 MetaTube 官方 `/v1/providers`、`/v1/movies/search` 和 `/v1/movies/{provider}/{id}` 接口。若 MetaTube 使用 `-token` 启动，请在系统设置中填写 Token；请求会使用 Bearer 鉴权。

Luma Media 与 MetaTube 位于不同容器时，不要填写 `127.0.0.1`。可填写 NAS 局域网地址，例如 `http://192.168.1.20:8080`，或把两个容器加入同一个 Docker 网络后填写 `http://metatube:8080`。

你当前的 MetaTube 容器没有设置 `TOKEN`，因此 Luma Media 的 Token 留空即可。若以后为 MetaTube 设置 Token，两边填写相同值。

MetaTube 的 `DSN` 为空时使用内存数据库，容器重启后缓存会丢失。建议把宿主机目录挂载到 `/config`，并让 MetaTube 使用 `-dsn /config/metatube.db`；这不影响 Luma Media 连接，但能保留 MetaTube 数据。
