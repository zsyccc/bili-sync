![bili-sync](https://socialify.git.ci/amtoaer/bili-sync/image?description=1&font=KoHo&issues=1&language=1&logo=https%3A%2F%2Fs2.loli.net%2F2023%2F12%2F02%2F9EwT2yInOu1d3zm.png&name=1&owner=1&pattern=Signal&pulls=1&stargazers=1&theme=Light)

## 简介

> [!NOTE]
> [查看文档](https://bili-sync.amto.cc/) ｜ [加入 Telegram 交流群](https://t.me/+nuYrt8q6uEo4MWI1)

bili-sync 是一款专为 NAS 用户编写的哔哩哔哩同步工具，由 Rust & Tokio 驱动。

## 效果演示

### 管理页
![管理页](./assets/webui.webp)
### 媒体库概览
![媒体库概览](./assets/overview.webp)
### 媒体库详情
![媒体库详情](./assets/detail.webp)
### 播放（使用 infuse）
![播放](./assets/play.webp)
### 文件排布
![文件](./assets/dir.webp)


## 功能与路线图

- [x] 使用用户填写的凭据认证，并在必要时自动刷新
- [x] 支持收藏夹与视频列表/视频合集的下载
- [x] 自动选择用户设置范围内最优的视频和音频流，并在下载完成后使用 FFmpeg 合并
- [x] 使用 Tokio 与 Reqwest，对视频、视频分页进行异步并发下载
- [x] 使用媒体服务器支持的文件命名，方便一键作为媒体库导入
- [x] 当前轮次下载失败会在下一轮下载时重试，失败次数过多自动丢弃
- [x] 使用数据库保存媒体信息，避免对同个视频的多次请求
- [x] 打印日志，并在请求出现风控时自动终止，等待下一轮执行
- [x] 提供多平台的二进制可执行文件，为 Linux 平台提供了立即可用的 Docker 镜像
- [x] 支持对“稍后再看”内视频的自动扫描与下载
- [x] 支持对 UP 主投稿视频的自动扫描与下载
- [x] 支持限制任务的并行度和接口请求频率
- [x] 支持单个文件的分块并行下载
- [x] 支持使用 Web UI 配置，查看并管理视频、视频源

## 跟踪同一 BV 的新增分 P

在 Docker Compose 的 `environment` 或进程环境中配置以下变量。程序在每轮普通下载开始前检查已收录的视频，并优先检查明确指定的 BV。变量为空时，继续使用原有的扫描流程。

| 环境变量 | 用途 |
| --- | --- |
| `BILI_SYNC_REFRESH_MULTIPART_BVIDS` | 逗号分隔的 BV 号。每轮检查已启用来源中的对应视频。 |
| `BILI_SYNC_REFRESH_REPLACED_FIRST_BVIDS` | 逗号分隔的 BV 号。允许这些视频从已下载的单 P 变为多 P 时，把替换后的 P1 加入下载队列。 |
| `BILI_SYNC_REFRESH_MULTIPART_UPLOADER_MIDS` | 逗号分隔的 UP 主数字 UID。轮流检查这些 UP 主在已启用来源里已收录且符合下载规则的视频。 |
| `BILI_SYNC_REFRESH_MULTIPART_UPLOADER_BUDGET` | 每轮按 UP 主检查的视频数上限，默认 10，范围 1–100。明确指定的 BV 号不占此额度。 |

当数据库已有分 P 的顺序和 CID 与 B 站列表开头完全一致时，程序会登记新增分 P 并交给正常下载流程。已有分 P 的路径和下载状态会保留。单 P 变为多 P 时，CID 一致的 P1 沿用原路径，新增分 P 使用 `Season 1` 目录。

单 P 变为多 P 且旧 CID 已从 B 站分 P 列表消失时，可将对应 BV 加入 `BILI_SYNC_REFRESH_REPLACED_FIRST_BVIDS`。程序会把当前 P1 和后续分集保存到 `Season 1`，旧 P1 文件留在原位置，视频详情显示新版 P1。旧 P1 已归档到其他位置时，也可根据数据库中已完成的下载记录执行。其他替换、重排或删除情况会在日志中显示，供人工核对。

按 UP 主检查的游标保存在进程内存中，重启后从最早的记录重新扫描。如果有正在追更的 BV，可单独填写该 BV，每轮直接检查。


## 参考与借鉴

该项目实现过程中主要参考借鉴了如下的项目，感谢他们的贡献：

+ [bilibili-API-collect](https://github.com/SocialSisterYi/bilibili-API-collect) B 站的第三方接口文档
+ [bilibili-api](https://github.com/Nemo2011/bilibili-api) 使用 Python 调用接口的参考实现
+ [danmu2ass](https://github.com/gwy15/danmu2ass) 本项目弹幕下载功能的缝合来源
