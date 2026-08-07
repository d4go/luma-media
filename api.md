# Luma Media API Design

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

POST /tasks/{id}/retry

POST /tasks/retry

POST /tasks/cancel

批量任务请求：

```json
{
  "taskIds": [1, 2, 3]
}
```

## Media

GET /media?search=keyword&status=pending

`status` 可选值为 `pending`、`ready`、`failed`。

POST /media/{id}/scrape

POST /media/scrape

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

## Settings

GET /settings

PUT /settings

Manage:

-   MetaTube address
-   output format
-   scan interval
-   overwrite policy
