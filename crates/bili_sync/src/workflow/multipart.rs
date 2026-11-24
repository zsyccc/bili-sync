use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};

use anyhow::{Context, Result, ensure};
use bili_sync_entity::{page, video};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, QueryOrder, QuerySelect, Select, TransactionTrait};

use super::{VIDEO_PAGE_STATUS_OFFSET, page_uses_single_layout};
use crate::adapter::{VideoSource, VideoSourceEnum};
use crate::bilibili::{BiliClient, BiliError, Credential, PageInfo, Video};
use crate::config::ARGS;
use crate::utils::model::create_pages;
use crate::utils::status::{PageStatus, STATUS_NOT_STARTED, STATUS_OK, VideoStatus};

// The task manager serializes download rounds. The cursor is process-local,
// so restarting safely starts the uploader scan at the first known video.
static UPLOADER_CURSOR: AtomicI32 = AtomicI32::new(0);

fn exact_bvids() -> impl Iterator<Item = &'static str> {
    ARGS.refresh_multipart_bvids
        .iter()
        .chain(&ARGS.refresh_replaced_first_bvids)
        .map(String::as_str)
}

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
        &exact_bvids().collect::<Vec<_>>(),
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
    exact_bvids: &[&str],
    budget: u16,
    cursor: i32,
) -> Result<Vec<i32>> {
    let candidates = eligible_videos()
        .select_only()
        .column(video::Column::Id)
        .filter(sources)
        .filter(video::Column::UpperId.is_in(mids.iter().copied()))
        .filter(video::Column::Bvid.is_not_in(exact_bvids.iter().copied()))
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
    uploader_ids: &[i32],
) -> Result<()> {
    let bvids = exact_bvids().collect::<HashSet<_>>();
    if bvids.is_empty() && uploader_ids.is_empty() {
        return Ok(());
    }

    let mut candidates = eligible_videos()
        .select_only()
        .columns([video::Column::Id, video::Column::Bvid])
        .filter(video_source.filter_expr())
        .filter(
            Condition::any()
                .add(video::Column::Bvid.is_in(bvids.iter().copied()))
                .add(video::Column::Id.is_in(uploader_ids.iter().copied())),
        )
        .into_tuple::<(i32, String)>()
        .all(connection)
        .await?;
    candidates.sort_by_key(|(id, bvid)| (!bvids.contains(bvid.as_str()), *id));

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
        let allow_replacement = ARGS.refresh_replaced_first_bvids.contains(&bvid);
        let added = reconcile_video_pages(connection, id, pages, allow_replacement).await?;
        if added > 0 {
            info!("视频 {bvid} ({id}) 有 {added} 个分 P 已加入下载队列");
        }
    }

    Ok(())
}

async fn reconcile_video_pages(
    connection: &DatabaseConnection,
    video_id: i32,
    remote: Vec<PageInfo>,
    allow_replacement: bool,
) -> Result<usize> {
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

    let replacement = allow_replacement
        .then(|| replaced_first_page(&video, &local, &remote))
        .flatten();
    let added = match if replacement.is_some() {
        validate_remote_pages(&remote).map(|()| remote.len())
    } else {
        appended_pages(&local, &remote)
    } {
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
    let mut remote_pages = remote.into_iter();
    if let Some(previous) = replacement {
        let mut current = remote_pages
            .next()
            .context("missing replacement P1")?
            .into_active_model(video_id);
        current.id = Set(previous.id);
        current.path = Set(None);
        current.update(&txn).await?;
    }
    let new_pages = remote_pages
        .skip(if replacement.is_some() { 0 } else { local.len() })
        .map(|page| page.into_active_model(video_id))
        .collect();
    create_pages(new_pages, &txn).await?;

    let mut status = VideoStatus::from(video.download_status);
    status.set(VIDEO_PAGE_STATUS_OFFSET, STATUS_NOT_STARTED);
    let was_single = video.single_page == Some(true);
    let bvid = video.bvid.clone();
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
    if let Some(previous) = replacement {
        info!(
            "视频 {bvid} ({video_id}) 的 P1 已替换（旧 CID {}），新 P1 已加入 Season 1 下载队列",
            previous.cid
        );
    }
    Ok(added)
}

fn replaced_first_page<'a>(
    video: &video::Model,
    local: &'a [page::Model],
    remote: &[PageInfo],
) -> Option<&'a page::Model> {
    let ([previous], [first, _, ..]) = (local, remote) else {
        return None;
    };
    let root = Path::new(&video.path);
    let page_status: [u32; 5] = PageStatus::from(previous.download_status).into();
    (video.single_page == Some(true)
        && root.is_absolute()
        && page_uses_single_layout(false, previous, root)
        && page_status[1] == STATUS_OK
        && previous.cid > 0
        && previous.cid != first.cid
        && !remote.iter().any(|page| page.cid == previous.cid))
    .then_some(previous)
}

fn appended_pages(local: &[page::Model], remote: &[PageInfo]) -> Result<usize> {
    ensure!(!local.is_empty(), "本地没有可匹配的分 P");
    ensure!(remote.len() >= local.len(), "远端分 P 数量少于本地");
    validate_remote_pages(remote)?;
    for (local_page, remote_page) in local.iter().zip(remote) {
        ensure!(
            local_page.pid == remote_page.page && local_page.cid == remote_page.cid,
            "P{} 不匹配（本地 P{}，CID {}，远端 CID {}）",
            remote_page.page,
            local_page.pid,
            local_page.cid,
            remote_page.cid,
        );
    }
    Ok(remote.len() - local.len())
}

fn validate_remote_pages(remote: &[PageInfo]) -> Result<()> {
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
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_tempfile::TempDir;
    use bili_sync_migration::{Migrator, MigratorTrait};
    use sea_orm::{ConnectOptions, ConnectionTrait, Database};
    use tokio::fs;

    use super::*;

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
            reconcile_video_pages(&db, video.id, remote_pages(&[101, 102, 103]), true).await?,
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
            reconcile_video_pages(&db, video.id, remote_pages(&[101, 102, 103]), false).await?,
            0
        );
        assert_eq!(
            reconcile_video_pages(&db, video.id, remote_pages(&[101, 999, 103, 104]), true).await?,
            0
        );
        assert_eq!(
            reconcile_video_pages(&db, video.id, remote_pages(&[101, 102]), true).await?,
            0
        );
        assert_eq!(
            reconcile_video_pages(&db, video.id, remote_pages(&[101, 102, 103, 103]), true).await?,
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
            reconcile_video_pages(&db, video.id, remote_pages(&[101, 102, 103, 104]), true).await?,
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
    async fn queues_replaced_first_page_only_for_an_opted_in_completed_single() -> Result<()> {
        for old_file_exists in [true, false] {
            let db = test_database().await?;
            let directory = TempDir::new().await?;
            let video_path = directory.dir_path();
            let old_path = video_path.join("Original.mp4");
            if old_file_exists {
                fs::write(&old_path, b"original media").await?;
            }
            let old_video = video::ActiveModel::from(video::Model {
                id: 1,
                bvid: "BV-replaced".to_owned(),
                path: video_path.to_string_lossy().into_owned(),
                category: 2,
                valid: true,
                should_download: true,
                single_page: Some(true),
                download_status: VideoStatus::from([STATUS_OK; 5]).into(),
                ..Default::default()
            })
            .insert(&db)
            .await?;
            let id = old_video.id;
            let mut old_first = remote_pages(&[100]).remove(0).into_active_model(id);
            old_first.path = Set(Some(old_path.to_string_lossy().into_owned()));
            old_first.download_status = Set(PageStatus::from([STATUS_OK; 5]).into());
            let old_first = old_first.insert(&db).await?;

            assert_eq!(
                reconcile_video_pages(&db, id, remote_pages(&[101, 102]), false).await?,
                0
            );
            assert_eq!(reconcile_video_pages(&db, id, remote_pages(&[101]), true).await?, 0);
            assert_eq!(
                reconcile_video_pages(&db, id, remote_pages(&[101, 100]), true).await?,
                0
            );
            let mut malformed = remote_pages(&[101, 102]);
            malformed[1].page = 3;
            assert_eq!(reconcile_video_pages(&db, id, malformed, true).await?, 0);

            let mut video: video::ActiveModel = old_video.clone().into();
            video.should_download = Set(false);
            let mut video: video::ActiveModel = video.update(&db).await?.into();
            assert_eq!(
                reconcile_video_pages(&db, id, remote_pages(&[101, 102]), true).await?,
                0
            );
            video.should_download = Set(true);
            video.update(&db).await?;

            db.execute_unprepared(
                "CREATE TRIGGER reject_p2 BEFORE INSERT ON page WHEN NEW.pid = 2 BEGIN SELECT RAISE(ABORT, 'test'); END",
            )
            .await?;
            assert!(
                reconcile_video_pages(&db, id, remote_pages(&[101, 102]), true)
                    .await
                    .is_err()
            );
            assert_eq!(page::Entity::find().all(&db).await?, [old_first.clone()]);
            assert_eq!(video::Entity::find_by_id(id).one(&db).await?, Some(old_video.clone()));
            db.execute_unprepared("DROP TRIGGER reject_p2").await?;

            assert_eq!(
                reconcile_video_pages(&db, id, remote_pages(&[101, 102, 103]), true).await?,
                3
            );
            let updated = video::Entity::find_by_id(id).one(&db).await?.unwrap();
            let pages = page::Entity::find().order_by_asc(page::Column::Pid).all(&db).await?;
            let new_first = &pages[0];
            assert_eq!(updated.single_page, Some(false));
            assert_eq!(updated.path, old_video.path);
            assert_eq!(
                <[u32; 5]>::from(VideoStatus::from(updated.download_status)),
                [0, 0, 7, 7, 0]
            );
            assert_eq!(
                pages.iter().map(|p| (p.pid, p.cid)).collect::<Vec<_>>(),
                [(1, 101), (2, 102), (3, 103)]
            );
            assert_eq!(new_first.id, old_first.id);
            assert!(pages.iter().all(|p| p.path.is_none() && p.download_status == 0));
            assert!(!page_uses_single_layout(false, new_first, video_path));

            assert_eq!(
                reconcile_video_pages(&db, id, remote_pages(&[101, 102, 103]), true).await?,
                0
            );
            assert_eq!(
                reconcile_video_pages(&db, id, remote_pages(&[999, 102, 103]), true).await?,
                0
            );
            assert_eq!(
                page::Entity::find().order_by_asc(page::Column::Pid).all(&db).await?,
                pages
            );
            if old_file_exists {
                assert_eq!(fs::read(&old_path).await?, b"original media");
            } else {
                assert!(!fs::try_exists(&old_path).await?);
            }
        }
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
        let exact = ["BV-audit-3"];
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
