use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, Ordering};

use anyhow::{Result, ensure};
use bili_sync_entity::{page, video};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, QueryOrder, QuerySelect, Select, TransactionTrait};

use super::VIDEO_PAGE_STATUS_OFFSET;
use crate::adapter::{VideoSource, VideoSourceEnum};
use crate::bilibili::{BiliClient, BiliError, Credential, PageInfo, Video};
use crate::config::ARGS;
use crate::utils::model::create_pages;
use crate::utils::status::{STATUS_NOT_STARTED, VideoStatus};

// The task manager serializes download rounds. The cursor is process-local,
// so restarting safely starts the uploader scan at the first known video.
static UPLOADER_CURSOR: AtomicI32 = AtomicI32::new(0);

pub(crate) async fn select_uploader_audit_batch(
    connection: &DatabaseConnection,
    sources: &[VideoSourceEnum],
) -> Result<Vec<i32>> {
    if ARGS.refresh_multipart_uploader_mids.is_empty() || sources.is_empty() {
        return Ok(Vec::new());
    }
    let source_filter = sources
        .iter()
        .fold(Condition::any(), |scope, source| scope.add(source.filter_expr()));
    let ids = select_uploader_candidates(
        connection,
        source_filter,
        &ARGS.refresh_multipart_uploader_mids,
        &ARGS.refresh_multipart_bvids,
        ARGS.refresh_multipart_uploader_budget,
        UPLOADER_CURSOR.load(Ordering::Relaxed),
    )
    .await?;
    if let Some(&last_id) = ids.last() {
        UPLOADER_CURSOR.store(last_id, Ordering::Relaxed);
    }
    debug!("本轮计划检查 {} 个指定 UP 主的视频是否有新增分 P", ids.len());
    Ok(ids)
}

async fn select_uploader_candidates(
    connection: &DatabaseConnection,
    sources: Condition,
    mids: &[i64],
    exact_bvids: &[String],
    budget: u16,
    cursor: i32,
) -> Result<Vec<i32>> {
    let candidates = eligible_videos()
        .select_only()
        .column(video::Column::Id)
        .filter(sources)
        .filter(video::Column::UpperId.is_in(mids.iter().copied()))
        .filter(video::Column::Bvid.is_not_in(exact_bvids.iter().map(String::as_str)))
        .order_by_asc(video::Column::Id);
    let mut ids = candidates
        .clone()
        .filter(video::Column::Id.gt(cursor))
        .limit(u64::from(budget))
        .into_tuple::<i32>()
        .all(connection)
        .await?;
    let remaining = usize::from(budget) - ids.len();
    if remaining > 0 && cursor > 0 {
        ids.extend(
            candidates
                .filter(video::Column::Id.lte(cursor))
                .limit(remaining as u64)
                .into_tuple::<i32>()
                .all(connection)
                .await?,
        );
    }
    Ok(ids)
}

fn eligible_videos() -> Select<video::Entity> {
    video::Entity::find()
        .filter(video::Column::Valid.eq(true))
        .filter(video::Column::Category.eq(2))
        .filter(video::Column::ShouldDownload.eq(true))
        .filter(video::Column::SinglePage.is_not_null())
}

pub(super) async fn refresh_tracked_video_pages(
    bili_client: &BiliClient,
    video_source: &VideoSourceEnum,
    connection: &DatabaseConnection,
    credential: &Credential,
    bvids: &[String],
    uploader_ids: &[i32],
) -> Result<()> {
    if bvids.is_empty() && uploader_ids.is_empty() {
        return Ok(());
    }

    let mut candidates = eligible_videos()
        .select_only()
        .columns([video::Column::Id, video::Column::Bvid])
        .filter(video_source.filter_expr())
        .filter(
            Condition::any()
                .add(video::Column::Bvid.is_in(bvids.iter().map(String::as_str)))
                .add(video::Column::Id.is_in(uploader_ids.iter().copied())),
        )
        .into_tuple::<(i32, String)>()
        .all(connection)
        .await?;
    candidates.sort_by_key(|(id, bvid)| (!bvids.contains(bvid), *id));

    for (id, bvid) in candidates {
        let pages = match Video::new(bili_client, &bvid, credential).get_pages().await {
            Ok(pages) => pages,
            Err(error) => {
                if error
                    .downcast_ref::<BiliError>()
                    .is_some_and(BiliError::is_risk_control_related)
                {
                    return Err(error);
                }
                warn!("检查视频 {bvid} ({id}) 的新增分 P 失败：{error:#}");
                continue;
            }
        };
        let added = append_video_pages(connection, id, pages).await?;
        if added > 0 {
            info!("视频 {bvid} ({id}) 新增 {added} 个分 P，已加入下载队列");
        }
    }

    Ok(())
}

async fn append_video_pages(connection: &DatabaseConnection, video_id: i32, remote: Vec<PageInfo>) -> Result<usize> {
    let txn = connection.begin().await?;
    let Some(video) = eligible_videos()
        .filter(video::Column::Id.eq(video_id))
        .one(&txn)
        .await?
    else {
        return Ok(0);
    };
    let local = page::Entity::find()
        .filter(page::Column::VideoId.eq(video_id))
        .order_by_asc(page::Column::Pid)
        .all(&txn)
        .await?;

    let added = match appended_pages(&local, &remote) {
        Ok(0) => return Ok(0),
        Ok(added) => added,
        Err(error) => {
            warn!(
                "跳过视频 {} ({video_id}) 的分 P 更新（本地 {} P，远端 {} P）：{error:#}",
                video.bvid,
                local.len(),
                remote.len(),
            );
            return Ok(0);
        }
    };
    let new_pages = remote
        .into_iter()
        .skip(local.len())
        .map(|page| page.into_active_model(video_id))
        .collect();
    create_pages(new_pages, &txn).await?;

    let mut status = VideoStatus::from(video.download_status);
    status.set(VIDEO_PAGE_STATUS_OFFSET, STATUS_NOT_STARTED);
    let was_single = video.single_page == Some(true);
    let mut video: video::ActiveModel = video.into();
    if was_single {
        // 单页视频此前跳过了剧集级的封面和 NFO。
        status.set(0, STATUS_NOT_STARTED);
        status.set(1, STATUS_NOT_STARTED);
        video.single_page = Set(Some(false));
    }
    video.download_status = Set(status.into());
    video.update(&txn).await?;
    txn.commit().await?;
    Ok(added)
}

fn appended_pages(local: &[page::Model], remote: &[PageInfo]) -> Result<usize> {
    ensure!(!local.is_empty(), "本地没有可匹配的分 P");
    ensure!(remote.len() >= local.len(), "远端分 P 数量少于本地");

    let mut cids = HashSet::new();
    for (index, remote_page) in remote.iter().enumerate() {
        ensure!(
            usize::try_from(remote_page.page).ok() == Some(index + 1)
                && remote_page.cid > 0
                && cids.insert(remote_page.cid),
            "远端分 P 序号或 CID 无效（P{}，CID {}）",
            remote_page.page,
            remote_page.cid,
        );
        if let Some(local_page) = local.get(index) {
            ensure!(
                local_page.pid == remote_page.page && local_page.cid == remote_page.cid,
                "P{} 不匹配（本地 P{}，CID {}，远端 CID {}）",
                remote_page.page,
                local_page.pid,
                local_page.cid,
                remote_page.cid,
            );
        }
    }

    Ok(remote.len() - local.len())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bili_sync_migration::{Migrator, MigratorTrait};
    use sea_orm::{ConnectOptions, Database};

    use super::*;
    use crate::utils::status::{PageStatus, STATUS_OK};
    use crate::workflow::page_uses_single_layout;

    async fn test_database() -> Result<DatabaseConnection> {
        let mut connection = ConnectOptions::new("sqlite::memory:");
        connection.max_connections(1);
        let db = Database::connect(connection).await?;
        Migrator::up(&db, None).await?;
        Ok(db)
    }

    fn remote_pages(cids: &[i64]) -> Vec<PageInfo> {
        cids.iter()
            .enumerate()
            .map(|(index, &cid)| PageInfo {
                cid,
                page: i32::try_from(index + 1).unwrap(),
                name: format!("Part {}", index + 1),
                duration: 60,
                ..Default::default()
            })
            .collect()
    }

    #[tokio::test]
    async fn appends_pages_without_resetting_existing_downloads() -> Result<()> {
        let db = test_database().await?;
        let completed_video = u32::from(VideoStatus::from([STATUS_OK; 5]));
        let completed_page = u32::from(PageStatus::from([STATUS_OK; 5]));
        let video = video::ActiveModel::from(video::Model {
            id: 1,
            bvid: "BV-test".to_owned(),
            path: "/library/Video".to_owned(),
            category: 2,
            valid: true,
            should_download: true,
            single_page: Some(true),
            download_status: completed_video,
            ..Default::default()
        })
        .insert(&db)
        .await?;
        let mut first = remote_pages(&[101]).pop().unwrap().into_active_model(video.id);
        first.path = Set(Some("/library/Video/Original - upload.mp4".to_owned()));
        first.download_status = Set(completed_page);
        let first = first.insert(&db).await?;

        assert_eq!(
            append_video_pages(&db, video.id, remote_pages(&[101, 102, 103])).await?,
            2
        );
        let updated = video::Entity::find_by_id(video.id).one(&db).await?.unwrap();
        assert_eq!(updated.single_page, Some(false));
        assert_eq!(updated.path, video.path);
        assert!(!VideoStatus::from(updated.download_status).get_completed());
        assert_eq!(
            <[u32; 5]>::from(VideoStatus::from(updated.download_status)),
            [0, 0, 7, 7, 0]
        );
        assert_eq!(page::Entity::find_by_id(first.id).one(&db).await?, Some(first.clone()));
        let pages = page::Entity::find().order_by_asc(page::Column::Pid).all(&db).await?;
        assert_eq!(pages.len(), 3);
        assert_eq!((pages[2].pid, pages[2].cid, pages[2].duration), (3, 103, 60));
        assert_eq!(pages[2].download_status, 0);
        assert_eq!(pages[2].path, None);

        assert_eq!(
            append_video_pages(&db, video.id, remote_pages(&[101, 102, 103])).await?,
            0
        );
        assert_eq!(
            append_video_pages(&db, video.id, remote_pages(&[101, 999, 103, 104])).await?,
            0
        );
        assert_eq!(append_video_pages(&db, video.id, remote_pages(&[101, 102])).await?, 0);
        assert_eq!(
            append_video_pages(&db, video.id, remote_pages(&[101, 102, 103, 103])).await?,
            0
        );
        assert_eq!(
            page::Entity::find().order_by_asc(page::Column::Pid).all(&db).await?,
            pages
        );
        assert_eq!(
            video::Entity::find_by_id(video.id).one(&db).await?,
            Some(updated.clone())
        );

        let mut updated: video::ActiveModel = updated.into();
        updated.download_status = Set(completed_video);
        updated.update(&db).await?;
        assert_eq!(
            append_video_pages(&db, video.id, remote_pages(&[101, 102, 103, 104])).await?,
            1
        );
        let updated = video::Entity::find_by_id(video.id).one(&db).await?.unwrap();
        assert_eq!(
            <[u32; 5]>::from(VideoStatus::from(updated.download_status)),
            [7, 7, 7, 7, 0]
        );
        assert!(!VideoStatus::from(updated.download_status).get_completed());

        Ok(())
    }

    #[tokio::test]
    async fn rotates_uploader_checks_within_one_budget_for_all_enabled_sources() -> Result<()> {
        let db = test_database().await?;
        let known_video = |id| video::Model {
            id,
            bvid: format!("BV-audit-{id}"),
            favorite_id: Some(1),
            upper_id: 1234,
            category: 2,
            valid: true,
            should_download: true,
            single_page: Some(true),
            ..Default::default()
        };
        let videos = [
            video::Model {
                should_download: false,
                ..known_video(1)
            },
            known_video(2),
            known_video(3),
            video::Model {
                favorite_id: Some(3),
                ..known_video(4)
            },
            video::Model {
                favorite_id: Some(2),
                single_page: Some(false),
                ..known_video(5)
            },
            video::Model {
                upper_id: 1235,
                ..known_video(6)
            },
            video::Model {
                valid: false,
                ..known_video(7)
            },
            known_video(8),
            video::Model {
                single_page: None,
                ..known_video(9)
            },
        ];
        for video in videos {
            video::ActiveModel::from(video).insert(&db).await?;
        }

        let sources = Condition::any()
            .add(video::Column::FavoriteId.eq(1))
            .add(video::Column::FavoriteId.eq(2));
        let exact = vec!["BV-audit-3".to_owned()];
        let mids = [1234];
        assert_eq!(
            select_uploader_candidates(&db, sources.clone(), &mids, &exact, 2, 0).await?,
            [2, 5]
        );
        assert_eq!(
            select_uploader_candidates(&db, sources.clone(), &mids, &exact, 2, 5).await?,
            [8, 2]
        );
        assert_eq!(
            select_uploader_candidates(&db, sources.clone(), &mids, &exact, 8, 5).await?,
            [8, 2, 5]
        );
        assert_eq!(
            select_uploader_candidates(&db, sources, &mids, &[], 8, 0).await?,
            [2, 3, 5, 8]
        );
        Ok(())
    }

    #[test]
    fn keeps_existing_p1_file_layout_after_video_becomes_multipart() {
        let video_path = Path::new("/library/Video");
        let mut first = page::Model {
            pid: 1,
            path: Some("/library/Video/Original - upload.mp4".to_owned()),
            ..Default::default()
        };
        assert!(page_uses_single_layout(false, &first, video_path));

        first.path = Some("/library/Video/Season 1/Part 1 - S01E01.mp4".to_owned());
        assert!(!page_uses_single_layout(false, &first, video_path));

        first.path = None;
        assert!(!page_uses_single_layout(false, &first, video_path));
        assert!(page_uses_single_layout(true, &first, video_path));
    }
}
