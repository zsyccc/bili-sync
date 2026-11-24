use bili_sync_entity::page;
use chrono::NaiveDate;
use sea_orm::DatabaseConnection;
use sea_orm::sqlx;

use super::PAGE_MEDIA_STATUS_OFFSET;
use crate::utils::status::PageStatus;

pub(super) fn has_pending_media_download(page_tasks_pending: bool, pages: &[page::Model]) -> bool {
    page_tasks_pending
        && pages
            .iter()
            .any(|page| PageStatus::from(page.download_status).should_run()[PAGE_MEDIA_STATUS_OFFSET])
}

pub(super) async fn claim_download(
    connection: &DatabaseConnection,
    submission_id: i32,
    day: impl FnOnce() -> NaiveDate,
    bvid: &str,
    limit: u32,
) -> Result<bool, sqlx::Error> {
    if limit == 0 {
        return Ok(true);
    }

    // Capture the local date after obtaining the write lock, including when
    // a task waits for the lock across midnight.
    let mut transaction = connection
        .get_sqlite_connection_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await?;
    let result = sqlx::query(
        "INSERT INTO submission_download_day (submission_id, day, bvid)
         SELECT ?1, ?2, ?3
         WHERE EXISTS (
             SELECT 1 FROM submission_download_day WHERE submission_id = ?1 AND day = ?2 AND bvid = ?3
         ) OR (
             SELECT COUNT(*) FROM submission_download_day WHERE submission_id = ?1 AND day = ?2
         ) < ?4
         ON CONFLICT (submission_id, day, bvid) DO UPDATE SET bvid = excluded.bvid",
    )
    .bind(submission_id)
    .bind(day())
    .bind(bvid)
    .bind(i64::from(limit))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(result.rows_affected() != 0)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::time::Duration;

    use async_tempfile::TempDir;
    use bili_sync_entity::submission;
    use bili_sync_migration::{MigrationTrait, Migrator, MigratorTrait};
    use sea_orm::{ConnectionTrait, Database, EntityTrait};

    use super::*;

    const ADD_SUBMISSIONS: &str = "INSERT INTO submission (id, upper_id, upper_name, path) VALUES (1, 101, 'one', '/one'), (2, 102, 'two', '/two')";

    struct PriorMigrator;

    impl MigratorTrait for PriorMigrator {
        fn migrations() -> Vec<Box<dyn MigrationTrait>> {
            let mut migrations = Migrator::migrations();
            migrations.pop();
            migrations
        }
    }

    #[test]
    fn metadata_only_retry_uses_no_media_quota() {
        let page_with_media = page::Model {
            download_status: PageStatus::from([0, crate::utils::status::STATUS_OK, 0, 0, 0]).into(),
            ..Default::default()
        };
        let page_without_media = page::Model::default();
        assert!(!has_pending_media_download(true, &[page_with_media]));
        assert!(!has_pending_media_download(
            false,
            std::slice::from_ref(&page_without_media)
        ));
        assert!(has_pending_media_download(true, &[page_without_media]));
        assert!(!has_pending_media_download(true, &[]));
    }

    #[tokio::test]
    async fn migrated_source_defaults_to_unlimited_and_quota_survives_reopen() {
        let dir = TempDir::new().await.unwrap();
        let path = dir.dir_path().join("quota.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Database::connect(&url).await.unwrap();
        PriorMigrator::up(&db, None).await.unwrap();
        db.execute_unprepared(ADD_SUBMISSIONS).await.unwrap();
        Migrator::up(&db, None).await.unwrap();

        let source = submission::Entity::find_by_id(1).one(&db).await.unwrap().unwrap();
        let day = NaiveDate::from_ymd_opt(2026, 11, 24).unwrap();
        assert_eq!(source.daily_download_limit, 0);
        assert!(
            claim_download(&db, source.id, || day, "unlimited-a", source.daily_download_limit)
                .await
                .unwrap()
        );
        assert!(
            claim_download(&db, source.id, || day, "unlimited-b", source.daily_download_limit)
                .await
                .unwrap()
        );
        assert!(claim_download(&db, source.id, || day, "first", 1).await.unwrap());
        db.execute_unprepared("UPDATE submission SET daily_download_limit = 1 WHERE id = 1")
            .await
            .unwrap();
        db.close().await.unwrap();

        let db = Database::connect(&url).await.unwrap();
        let migration = Migrator::migrations().pop().unwrap();
        sqlx::query("DELETE FROM seaql_migrations WHERE version = ?")
            .bind(migration.name())
            .execute(db.get_sqlite_connection_pool())
            .await
            .unwrap();
        PriorMigrator::up(&db, None).await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        let source = submission::Entity::find_by_id(1).one(&db).await.unwrap().unwrap();
        assert_eq!(source.daily_download_limit, 1);
        assert!(claim_download(&db, source.id, || day, "first", 1).await.unwrap());
        assert!(!claim_download(&db, source.id, || day, "second", 1).await.unwrap());
        assert!(claim_download(&db, 2, || day, "second", 1).await.unwrap());
        let next_day = day.succ_opt().unwrap();
        assert!(claim_download(&db, source.id, || next_day, "second", 1).await.unwrap());
    }

    #[tokio::test]
    async fn simultaneous_starts_share_the_last_slot() {
        let dir = TempDir::new().await.unwrap();
        let db = crate::database::setup_database(&dir.dir_path().join("quota.sqlite"))
            .await
            .unwrap();
        db.execute_unprepared(ADD_SUBMISSIONS).await.unwrap();
        let day = NaiveDate::from_ymd_opt(2026, 11, 24).unwrap();

        let (first, second, third) = tokio::try_join!(
            claim_download(&db, 1, || day, "first", 1),
            claim_download(&db, 1, || day, "second", 1),
            claim_download(&db, 1, || day, "third", 1),
        )
        .unwrap();
        assert_eq!([first, second, third].into_iter().filter(|result| *result).count(), 1);

        let admitted = if first {
            "first"
        } else if second {
            "second"
        } else {
            "third"
        };
        let (retry_one, retry_two) = tokio::try_join!(
            claim_download(&db, 1, || day, admitted, 1),
            claim_download(&db, 1, || day, admitted, 1),
        )
        .unwrap();
        assert!(retry_one && retry_two);
        assert!(!claim_download(&db, 1, || day, "fourth", 1).await.unwrap());
    }

    #[tokio::test]
    async fn waiting_for_the_write_lock_uses_the_new_day() {
        let dir = TempDir::new().await.unwrap();
        let db = crate::database::setup_database(&dir.dir_path().join("quota.sqlite"))
            .await
            .unwrap();
        db.execute_unprepared(ADD_SUBMISSIONS).await.unwrap();
        let day = NaiveDate::from_ymd_opt(2026, 11, 24).unwrap();
        let clock = Cell::new(day);
        assert!(claim_download(&db, 1, || clock.get(), "yesterday", 1).await.unwrap());

        let lock = db
            .get_sqlite_connection_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        let mut waiting = std::pin::pin!(claim_download(&db, 1, || clock.get(), "today", 1));
        tokio::select! {
            result = &mut waiting => panic!("claim completed before database became writable: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(30)) => {}
        }
        clock.set(day.succ_opt().unwrap());
        lock.commit().await.unwrap();
        assert!(waiting.await.unwrap());
        assert!(!claim_download(&db, 1, || clock.get(), "another", 1).await.unwrap());
    }
}
