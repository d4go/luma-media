# Luma - Media Asset Fallback 设计文档

## 1. 背景

当前 Luma 使用 MetaTube 作为媒体元数据来源。

MetaTube 返回：
- 标题
- 演员
- 描述
- 海报 poster URL
- 背景图 fanart URL

但是部分 Provider 返回的图片 URL 不稳定，例如：

```
https://storage200000.contents.fc2.com/file/xxx.jpg
```

可能出现：
- HTTP 302 跳转错误页
- HTTP 404
- 图片 CDN 失效
- 防盗链

导致：

```
failed to download poster image
```

---

## 2. 目标

在 Luma 增加媒体资源健康检查机制：

1. 不直接信任第三方 poster URL
2. 返回给客户端之前验证图片可用性
3. 图片失效自动寻找备用 Provider
4. 成功图片缓存
5. 后续请求优先使用缓存

架构：

```
Jellyfin
    |
    |
Luma
    |
    +---- Asset Resolver
              |
              +---- Cache
              |
              +---- MetaTube
              |
              +---- Provider Fallback
```

---

# 3. Poster 健康检查

新增：

```
PosterChecker
```

职责：

检查图片 URL 是否有效。

接口：

```java
public interface PosterChecker {

    boolean check(String url);

}
```

检查规则：

## HTTP 状态

允许：

```
200
```

拒绝：

```
301
302
403
404
500+
```

注意：

不能接受：

```
302 -> error page
```

---

## Content-Type

必须包含：

```
image
```

允许：

```
image/jpeg
image/png
```

拒绝：

```
text/html
```

---

# 4. Fallback 策略

当 poster 检查失败：

进入备用搜索。

例如：

```
FC2PPV-4792609
```

调用 MetaTube:

```
GET /v1/movies/search?q=FC2PPV-4792609
```

Provider 优先级：

```
1. FC2PPVDB

2. JavBus

3. JavLibrary

4. FC2

5. fc2hub
```

选择条件：

```
poster != null

poster检查成功
```

伪代码：

```java
for(provider : providers){

    Movie movie = search(provider);

    if(movie.poster != null
        && posterChecker.check(movie.poster)){

        return movie;

    }

}
```

---

# 5. 图片缓存

增加资源缓存。

表：

```sql
CREATE TABLE media_asset (

    id bigint primary key,

    media_id varchar(128),

    type varchar(32),

    source varchar(64),

    url text,

    local_path text,

    status varchar(20),

    checked_time datetime,

    create_time datetime

);
```

字段：

|字段|说明|
|-|-|
|media_id|影片ID|
|type|poster/fanart|
|source|来源|
|url|远程地址|
|local_path|本地缓存|
|status|ACTIVE/BROKEN|

---

# 6. 本地缓存策略

目录：

```
data/assets/poster/
```

例如：

```
data/assets/poster/
    FC2PPV-4792609.jpg
```

返回：

```
http://luma/assets/poster/FC2PPV-4792609.jpg
```

不要长期依赖第三方 CDN。

---

# 7. API

新增：

```
GET /asset/poster/{mediaId}
```

逻辑：

```
查询缓存

存在:
    返回缓存图片


不存在:

    查询MetaTube

    检查poster

    下载

    保存缓存

    返回
```

---

# 8. 定时健康检查

增加：

```
AssetHealthJob
```

每天执行：

扫描 media_asset：

检查：

```
ACTIVE资源
```

如果：

```
404
302
403
```

标记：

```
BROKEN
```

重新执行 fallback。

---

# 9. 日志要求

成功：

```
POSTER_RESOLVE_SUCCESS

mediaId=FC2PPV-4792609
source=FC2PPVDB
```

失败：

```
POSTER_RESOLVE_FAILED

mediaId=xxx
url=xxx
reason=302 redirect
```

Fallback：

```
POSTER_FALLBACK

from=fc2hub
to=FC2PPVDB
```

---

# 10. 非目标

本功能不修改：

- MetaTube
- Jellyfin
- Emby

Luma 作为媒体中间层负责：

- 元数据聚合
- 图片稳定化
- 缓存
- Provider切换

---

# 11. 验收标准

## Case 1

MetaTube返回正常图片：

结果：

```
直接缓存
返回
```

---

## Case 2

MetaTube返回：

```
302 -> error.fc2.com
```

结果：

```
检测失败

切换备用Provider

返回备用poster
```

---

## Case 3

所有Provider失败：

结果：

```
返回metadata

poster为空

记录日志
```

---

# 最终架构

```
             Jellyfin

                |

           Luma

                |

        Metadata Resolver

                |

        +---------------+

        |               |

    MetaTube       Asset Resolver

                        |

             +----------+---------+

             |                    |

       PosterChecker        Local Cache


             |

       Provider Fallback

             |

 FC2PPVDB / JavBus / FC2 / fc2hub
```
