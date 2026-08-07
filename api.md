# Luma API Design

Base path:

    /api/v1

## Folder

GET /folders

POST /folders

创建目录前会验证当前 MetaTube 配置；目录启用时，创建成功后自动扫描并为扫描到的媒体创建刮削任务。

POST /folders/{id}/scan

新建或更新目录时会校验容器内路径存在且是目录。所有成功扫描都会自动为状态为 `pending` 的媒体创建刮削任务。

Example:

``` json
{
  "name":"Movies",
  "path":"/media/movies",
  "type":"movie",
  "scanMode":"watch"
}
```

## Tasks

GET /tasks

GET /tasks/{id}

任务列表返回关联的 `media` 或 `folder` 摘要以及 `recordCount`。详情接口额外返回按时间倒序排列的
`records`，用于查看首次执行和每次重试。

POST /tasks/{id}/retry

POST /tasks/{id}/cancel

POST /tasks/retry

POST /tasks/cancel

重试复用原任务 ID，仅新增一条执行记录；取消只取消当前等待中或执行中的记录。

批量任务请求：

```json
{
  "taskIds": [1, 2, 3]
}
```

## Media

GET /media?search=keyword&status=pending

`status` 可选值为 `pending`、`ready`、`failed`。

每条媒体的 `resources.nfo` 与 `resources.poster` 分别返回真实资源状态：

- `status`: `ready`、`missing` 或 `failed`
- `source`: 本地资源为 `local`，远程封面为实际 Provider
- `path`: 已发现的旁车文件或缓存文件路径
- `checkedAt`: 远程封面最近一次检查时间，本地资源为 `null`

媒体整体 `status` 与资源状态相互独立，所以刮削任务成功时仍可以明确看出封面是否下载成功。

POST /media/{id}/scrape

POST /media/scrape

批量响应中的 `queued` 表示本次新增的执行记录数量。刮削执行器全局最多并发 8 个任务，
超过上限的记录保持等待状态。

批量请求示例：

```json
{
  "mediaIds": [1, 2, 3],
  "overwriteNfo": true,
  "overwriteImage": true
}
```

Options:

``` json
{
 "overwriteNfo":true,
 "overwriteImage":true
}
```

目录扫描会识别与视频同名的 `.nfo`，以及同名 `-poster`、同名图片、`poster` 或 `folder`
海报文件（支持 jpg、jpeg、png、webp、avif）。NFO 与海报同时存在时，媒体会自动关联本地
元数据并标记为已就绪，不再进入自动远程刮削队列。

## Media assets

GET /asset/poster/{mediaId}

GET /asset/cover/{mediaId}

两个地址返回同一份经过验证的海报/封面缓存。`mediaId` 支持媒体库内部数字 ID，也支持
Provider ID、标题或文件名中的影片编号。首次请求没有缓存时会查询 MetaTube，并按
FC2PPVDB、JavBus、JavLibrary、FC2、fc2hub 的顺序回退。

图片请求不跟随 301/302，仅接受 HTTP 200 与支持的图片 Content-Type。成功图片保存在
`$LUMA_DATA_DIR/assets/poster/`。每天执行一次健康检查，失效资源标记为 `BROKEN` 后重新解析。

## Settings

GET /settings

PUT /settings

Manage:

-   MetaTube address
-   output format
-   scan interval
-   overwrite policy
-   qBittorrent Web UI credentials
-   automatic tracker source and interval

MetaTube 未显式配置时，`GET /settings` 根据 `Host`/`X-Forwarded-Host` 返回部署主机的
`http://<host>:8080`，也可使用 `LUMA_METATUBE_URL` 覆盖。

POST /settings/metatube/test

POST /settings/qbittorrent/test

## Crawlers

GET /crawlers

POST /crawlers（`multipart/form-data`）

PUT /crawlers/{id}（`multipart/form-data`，更新时 script 可省略）

DELETE /crawlers/{id}

POST /crawlers/{id}/run

上传字段为 `name`、`websiteUrl`、`intervalMinutes`、`enabled`、`autoDownload` 和 `script`。
Python 结果使用 JSON 数组或 `{ "results": [] }`，每项需要 `downloadUrl`、`magnet`、
`torrentUrl` 或 `url` 之一，可附带 `title`/`name` 与 `trackers`。

GET /crawler-runs?scriptId={id}

GET /crawler-results?scriptId={id}

POST /crawler-results/{id}/download

POST /crawler-results/download

批量下载请求为 `{ "resultIds": [1, 2] }`。提交后结果保存 qBittorrent 状态、哈希（磁力链接
可直接解析时）与错误信息。
