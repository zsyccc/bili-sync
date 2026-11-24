use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::Statement;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(SubmissionDownloadDay::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(SubmissionDownloadDay::SubmissionId).integer().not_null())
                    .col(ColumnDef::new(SubmissionDownloadDay::Day).date().not_null())
                    .col(ColumnDef::new(SubmissionDownloadDay::Bvid).string().not_null())
                    .primary_key(
                        Index::create()
                            .col(SubmissionDownloadDay::SubmissionId)
                            .col(SubmissionDownloadDay::Day)
                            .col(SubmissionDownloadDay::Bvid),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(SubmissionDownloadDay::Table, SubmissionDownloadDay::SubmissionId)
                            .to(Submission::Table, Submission::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        if !has_daily_download_limit(manager).await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Submission::Table)
                        .add_column(
                            ColumnDef::new(Submission::DailyDownloadLimit)
                                .unsigned()
                                .not_null()
                                .default(0),
                        )
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(SubmissionDownloadDay::Table).if_exists().to_owned())
            .await?;
        if has_daily_download_limit(manager).await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Submission::Table)
                        .drop_column(Submission::DailyDownloadLimit)
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}

async fn has_daily_download_limit(manager: &SchemaManager<'_>) -> Result<bool, DbErr> {
    let connection = manager.get_connection();
    let column = connection
        .query_one(Statement::from_string(
            connection.get_database_backend(),
            "SELECT 1 FROM pragma_table_info('submission') WHERE name = 'daily_download_limit'",
        ))
        .await?;
    Ok(column.is_some())
}

#[derive(DeriveIden)]
enum Submission {
    Table,
    Id,
    DailyDownloadLimit,
}

#[derive(DeriveIden)]
enum SubmissionDownloadDay {
    Table,
    SubmissionId,
    Day,
    Bvid,
}
