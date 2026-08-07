# Luma

Luma 是一个桌面优先的媒体元数据管理服务。项目包含 Vue 3 管理端、Rust Axum API、SQLite 存储与 Docker 部署。

## 已实现功能

- 开始：服务链路状态、首次配置步骤与常用入口
- 媒体目录：新增前验证 MetaTube，新增后自动扫描和刮削，并支持编辑、删除、启停和立即扫描
- 扫描器：递归识别常见视频格式，自动关联已有 NFO 与海报，仅排队仍缺元数据的媒体
- 任务中心：一个媒体对应一个逻辑任务，每次执行与重试都保留独立记录，并显示媒体关联
- 媒体库：搜索、元数据状态筛选、任务反向关联、单个刮削与批量刮削
- 系统设置：MetaTube 与 qBittorrent 连接、自动 Tracker 更新、输出格式、扫描间隔、覆盖策略和日志级别
- MetaTube：全局并发 8 个刮削执行，调用官方搜索与详情接口，写入 NFO/JSON、海报和背景图
- 资源回退：严格检查海报/封面 URL，自动切换 Provider，并把成功图片缓存到 `/data/assets`
- Python 爬虫：上传脚本、绑定目标网站、定时循环执行，并持久化 stdout、stderr 和结构化结果
- qBittorrent：手动或自动执行爬虫结果，附加结果内 Tracker，并定时向现有种子追加 Tracker 订阅列表
- 目录监听：`watch` 模式使用操作系统文件事件递归监听，周期扫描作为漏事件兜底
- Apple 风格明暗主题、移动端导航、加载、空数据和错误状态

## 目录结构

```text
luma/
├── backend/       Rust、Axum、SQLx、SQLite migrations
├── frontend/      Vue 3、TypeScript、Naive UI
├── Dockerfile
└── docker-compose.yml
```

## 本地开发

需要 Node.js 22+、npm、Rust 1.85+ 和 Python 3。可用 `LUMA_PYTHON_BIN` 指定 Python 可执行文件。

终端一：

```bash
cd backend
DATABASE_URL='sqlite://luma.db?mode=rwc' cargo run
```

终端二：

```bash
cd frontend
npm install
npm run dev
```

访问 `http://localhost:5173`。Vite 会把 `/api` 请求代理到 `http://localhost:3000`。

## Docker

在项目目录运行 Compose。默认使用飞牛 NAS 路径 `/vol1/1000/a1` 作为媒体目录、`/vol3/1000/docker/luma-data` 保存数据库；也可以用环境变量改掉。媒体目录必须以读写方式挂载，因为刮削结果会落在视频旁边：

```bash
MEDIA_ROOT=/path/to/your/media \
LUMA_DATA_ROOT=/path/to/luma-data \
docker compose up -d --build
```

访问 `http://localhost:3000`。容器内的媒体路径以 `/media` 开头，例如 `/media/movies`。

旧版数据库若仅因迁移文件换行符等非结构性变化出现校验不一致，启动时会先验证核心表结构，再自动修复 1/2 号历史迁移的校验记录；表或字段缺失时仍会停止启动并保留原始数据库。

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
- `GET /asset/poster/{mediaId}`、`GET /asset/cover/{mediaId}`
- `GET|PUT /settings`
- `POST /settings/metatube/test`
- `POST /settings/qbittorrent/test`
- `GET|POST /crawlers`、`PUT|DELETE /crawlers/{id}`
- `POST /crawlers/{id}/run`
- `GET /crawler-runs`、`GET /crawler-results`
- `POST /crawler-results/{id}/download`
- `GET /logs`
- `GET /health`

## MetaTube 连接

系统使用 MetaTube 官方 `/v1/providers`、`/v1/movies/search` 和 `/v1/movies/{provider}/{id}` 接口。首次打开设置页时，默认地址会根据当前部署主机地址生成 `http://<部署主机 IP>:8080`；也可通过 `LUMA_METATUBE_URL` 或设置页覆盖。若 MetaTube 使用 `-token` 启动，请在系统设置中填写 Token；请求会使用 Bearer 鉴权。

Luma 与 MetaTube 位于不同容器时，不要填写 `127.0.0.1`。可填写 NAS 局域网地址，例如 `http://192.168.1.20:8080`，或把两个容器加入同一个 Docker 网络后填写 `http://metatube:8080`。

你当前的 MetaTube 容器没有设置 `TOKEN`，因此 Luma 的 Token 留空即可。若以后为 MetaTube 设置 Token，两边填写相同值。

MetaTube 的 `DSN` 为空时使用内存数据库，容器重启后缓存会丢失。建议把宿主机目录挂载到 `/config`，并让 MetaTube 使用 `-dsn /config/metatube.db`；这不影响 Luma 连接，但能保留 MetaTube 数据。

## Python 爬虫与 qBittorrent

“爬虫与下载”页支持上传最大 1 MiB 的 `.py` 文件。服务将目标网站同时作为第一个命令行参数和 `LUMA_TARGET_WEBSITE` 环境变量传入；脚本应向 stdout 输出 JSON，或写入 `LUMA_RESULT_PATH` 指定的文件：

```json
{
  "results": [
    {
      "title": "Example",
      "downloadUrl": "magnet:?xt=urn:btih:...",
      "trackers": ["udp://tracker.example:80/announce"]
    }
  ]
}
```

根节点也可直接使用数组；下载字段还接受 `magnet`、`torrentUrl` 或 `url`。每次执行最长 15 分钟，最多同时运行 2 个脚本。脚本属于任意代码执行能力，只应上传你信任的脚本；可从 [示例脚本](examples/sample_crawler.py) 开始修改。

qBittorrent 需要启用 Web UI，并确保填写的地址可从 Luma 容器访问。自动 Tracker 更新从设置的文本订阅地址读取 HTTP、HTTPS 或 UDP Tracker，按设置周期追加到现有种子，不会删除已有 Tracker。
