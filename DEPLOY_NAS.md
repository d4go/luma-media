# NAS 更新步骤

以下步骤会保留现有 SQLite 数据库和媒体文件。

## 1. 备份数据库

先停止旧容器，并备份数据库及可能存在的 WAL 文件：

```bash
docker stop luma-media

mkdir -p /vol3/1000/docker/luma-media-backup
cp -a /vol3/1000/docker/luma-media/luma-media.db* \
  /vol3/1000/docker/luma-media-backup/
```

## 2. 配置宿主机路径

在 `docker-compose.yml` 同级创建 `.env`：

```dotenv
MEDIA_ROOT=/vol1/1000/a1
LUMA_DATA_ROOT=/vol3/1000/docker/luma-media
```

对应的容器挂载必须是：

```text
/vol1/1000/a1                     -> /media（读写）
/vol3/1000/docker/luma-media      -> /data
```

Luma Media 页面中的媒体目录填写容器路径 `/media`，不能填写宿主机路径 `/vol1/1000/a1`。

## 3. 重新构建

在新项目目录执行：

```bash
docker compose down
docker compose up -d --build
docker compose logs --tail=100 luma-media
```

若旧数据库仅存在历史迁移文件校验差异，程序会在确认核心表和字段完整后自动修复校验并继续启动。日志会出现一次：

```text
repaired a legacy migration checksum after validating the database schema
```

若数据库结构确实缺表或缺字段，程序仍会停止启动，并在日志中明确指出缺失项；此时不要删除数据库，应使用第 1 步的备份恢复。
