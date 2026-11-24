use std::collections::{BTreeMap, HashMap};
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result};
use async_tempfile::TempFile;
use bili_sync_entity::{page, video};
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, QueryOrder, QuerySelect};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{eligible_videos, reconcile_video_pages};
use crate::bilibili::{BiliClient, BiliError, Credential, Video, get_batch_page_counts};
use crate::config::CONFIG_DIR;

const BATCH_SIZE: usize = 100;
const CURSOR_FILE: &str = "multipart-library.cursor";

struct LibraryCandidate {
    id: i32,
    bvid: String,
    stored_pages: usize,
}

struct LibraryBatch {
    candidates: Vec<LibraryCandidate>,
    next_cursor: i32,
}

#[derive(Default)]
struct BatchOutcome {
    queued_parts: usize,
    missing_counts: usize,
}

pub(super) async fn refresh_library(
    bili_client: &BiliClient,
    connection: &DatabaseConnection,
    credential: &Credential,
    scope: Condition,
    budget: usize,
) -> Result<()> {
    // Only the existing serialized download task advances this checkpoint.
    let cursor_path = CONFIG_DIR.join(CURSOR_FILE);
    let mut cursor = load_cursor(&cursor_path).await?;
    let mut checked = 0;
    let mut queued_parts = 0;
    let mut missing_counts = 0;
    while checked < budget {
        let limit = BATCH_SIZE.min(budget - checked);
        let batch = select_batch(connection, scope.clone(), cursor, limit).await?;
        if batch.candidates.is_empty() {
            if cursor != 0 {
                save_cursor(&cursor_path, 0).await?;
                cursor = 0;
            }
            break;
        }

        let outcome = refresh_batch(bili_client, connection, credential, &batch.candidates).await?;
        save_cursor(&cursor_path, batch.next_cursor).await?;
        checked += batch.candidates.len();
        queued_parts += outcome.queued_parts;
        missing_counts += outcome.missing_counts;
        cursor = batch.next_cursor;
        if cursor == 0 {
            break;
        }
    }

    if checked > 0 {
        info!(
            "全库分 P 检查：本轮检查 {checked} 条记录，登记 {queued_parts} 个分 P，{missing_counts} 条未取得分页数，下轮游标 {cursor}"
        );
    }
    Ok(())
}

async fn select_batch(
    connection: &DatabaseConnection,
    scope: Condition,
    cursor: i32,
    limit: usize,
) -> Result<LibraryBatch> {
    let mut rows = eligible_videos()
        .select_only()
        .columns([video::Column::Id, video::Column::Bvid])
        .filter(scope)
        .filter(video::Column::Id.gt(cursor))
        .order_by_asc(video::Column::Id)
        .limit(limit as u64 + 1)
        .into_tuple::<(i32, String)>()
        .all(connection)
        .await?;

    // Look ahead one record to checkpoint the wrap in this same round.
    let mut next_cursor = 0;
    if rows.len() > limit {
        rows.pop();
        next_cursor = rows.last().context("全库检查批次上限必须大于零")?.0;
    }
    if rows.is_empty() {
        return Ok(LibraryBatch {
            candidates: Vec::new(),
            next_cursor,
        });
    }

    let counts = page::Entity::find()
        .select_only()
        .column(page::Column::VideoId)
        .column_as(page::Column::Id.count(), "page_count")
        .filter(page::Column::VideoId.is_in(rows.iter().map(|(id, _)| *id)))
        .group_by(page::Column::VideoId)
        .into_tuple::<(i32, i64)>()
        .all(connection)
        .await?
        .into_iter()
        .collect::<HashMap<_, _>>();
    let candidates = rows
        .into_iter()
        .map(|(id, bvid)| LibraryCandidate {
            id,
            bvid,
            stored_pages: usize::try_from(counts.get(&id).copied().unwrap_or_default()).unwrap_or_default(),
        })
        .collect();
    Ok(LibraryBatch {
        candidates,
        next_cursor,
    })
}

async fn refresh_batch(
    bili_client: &BiliClient,
    connection: &DatabaseConnection,
    credential: &Credential,
    candidates: &[LibraryCandidate],
) -> Result<BatchOutcome> {
    let mut groups = BTreeMap::<&str, Vec<&LibraryCandidate>>::new();
    for candidate in candidates {
        groups.entry(&candidate.bvid).or_default().push(candidate);
    }
    let bvids = groups.keys().map(|&bvid| bvid.to_owned()).collect::<Vec<_>>();
    let counts = get_batch_page_counts(bili_client, credential, &bvids)
        .await
        .context("全库分 P 批量检查失败")?;
    let mut outcome = BatchOutcome::default();
    for (bvid, mut rows) in groups {
        let Some(&remote_count) = counts.get(bvid) else {
            outcome.missing_counts += rows.len();
            continue;
        };
        rows.retain(|row| row.stored_pages > 0 && remote_count > row.stored_pages);
        if rows.is_empty() {
            continue;
        }

        let pages = match Video::new(bili_client, bvid, credential).get_pages().await {
            Ok(pages) => pages,
            Err(error) => {
                if error
                    .downcast_ref::<BiliError>()
                    .is_some_and(BiliError::is_risk_control_related)
                {
                    return Err(error);
                }
                warn!("全库检查视频 {bvid} 的分 P 详情失败：{error:#}");
                continue;
            }
        };
        for candidate in rows {
            let added = reconcile_video_pages(connection, candidate.id, &pages, false).await?;
            outcome.queued_parts += added;
            if added > 0 {
                info!(
                    "全库检查发现视频 {bvid} ({}) 的 {added} 个新增分 P，已加入下载队列",
                    candidate.id
                );
            }
        }
    }
    Ok(outcome)
}

async fn load_cursor(path: &Path) -> Result<i32> {
    match fs::read_to_string(path).await {
        Ok(contents) => match contents.trim().parse::<i32>() {
            Ok(cursor) if cursor >= 0 => Ok(cursor),
            _ => {
                warn!("全库分 P 检查游标无效，将从头检查");
                Ok(0)
            }
        },
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).context("无法读取全库分 P 检查游标"),
    }
}

async fn save_cursor(path: &Path, cursor: i32) -> Result<()> {
    let directory = path.parent().context("全库分 P 检查游标缺少父目录")?;
    let mut temporary = TempFile::new_in(directory)
        .await
        .context("无法创建全库分 P 检查游标的临时文件")?;
    temporary.write_all(format!("{cursor}\n").as_bytes()).await?;
    temporary.flush().await?;
    temporary.sync_all().await?;
    fs::rename(temporary.file_path(), path)
        .await
        .context("无法更新全库分 P 检查游标")?;
    temporary.drop_async().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_tempfile::TempDir;

    use super::*;
    use crate::workflow::multipart::tests::{remote_pages, test_database};

    #[tokio::test]
    async fn library_scan_filters_rows_and_checkpoints_restart_and_wrap() -> Result<()> {
        let db = test_database().await?;
        let directory = TempDir::new().await?;
        let cursor_path = directory.dir_path().join(CURSOR_FILE);
        let candidate = |id| video::Model {
            id,
            bvid: format!("BV-library-{id}"),
            favorite_id: Some(1),
            category: 2,
            valid: true,
            should_download: true,
            single_page: Some(true),
            ..Default::default()
        };
        for video in [
            candidate(1),
            video::Model {
                favorite_id: Some(3),
                ..candidate(2)
            },
            video::Model {
                category: 12,
                ..candidate(3)
            },
            video::Model {
                valid: false,
                ..candidate(4)
            },
            video::Model {
                should_download: false,
                ..candidate(5)
            },
            video::Model {
                single_page: None,
                ..candidate(6)
            },
            video::Model {
                favorite_id: Some(2),
                single_page: Some(false),
                ..candidate(7)
            },
        ] {
            video::ActiveModel::from(video).insert(&db).await?;
        }
        for (id, cids) in [(1, &[100][..]), (7, &[101, 102][..])] {
            for page in remote_pages(cids) {
                page.into_active_model(id).insert(&db).await?;
            }
        }

        let scope = Condition::any()
            .add(video::Column::FavoriteId.eq(1))
            .add(video::Column::FavoriteId.eq(2));
        let initial = load_cursor(&cursor_path).await?;
        assert_eq!(initial, 0);
        let first = select_batch(&db, scope.clone(), initial, 1).await?;
        assert_eq!(first.candidates.len(), 1);
        assert_eq!(first.candidates[0].id, 1);
        assert_eq!(first.candidates[0].stored_pages, 1);
        assert_eq!(first.next_cursor, 1);
        save_cursor(&cursor_path, first.next_cursor).await?;

        let resumed = load_cursor(&cursor_path).await?;
        let last = select_batch(&db, scope.clone(), resumed, 1).await?;
        assert_eq!(last.candidates.len(), 1);
        assert_eq!(last.candidates[0].id, 7);
        assert_eq!(last.candidates[0].stored_pages, 2);
        assert_eq!(last.next_cursor, 0);
        save_cursor(&cursor_path, last.next_cursor).await?;

        let wrapped = select_batch(&db, scope, load_cursor(&cursor_path).await?, 100).await?;
        assert_eq!(
            wrapped
                .candidates
                .iter()
                .map(|candidate| candidate.id)
                .collect::<Vec<_>>(),
            [1, 7]
        );
        assert_eq!(wrapped.next_cursor, 0);
        let existing = wrapped.candidates[1].id;
        assert_eq!(
            reconcile_video_pages(&db, existing, &remote_pages(&[101, 900, 103]), false).await?,
            0
        );
        assert_eq!(
            reconcile_video_pages(&db, existing, &remote_pages(&[101, 102, 103]), false).await?,
            1
        );
        Ok(())
    }
}
