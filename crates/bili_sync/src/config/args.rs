use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::LazyLock;

use clap::Parser;

pub static ARGS: LazyLock<Args> = LazyLock::new(Args::parse);

#[derive(Parser)]
#[command(name = "Bili-Sync", version = detail_version(), about, long_about = None)]
pub struct Args {
    #[arg(short, long, env = "SCAN_ONLY")]
    pub scan_only: bool,

    #[arg(short, long, default_value = "None,bili_sync=info", env = "RUST_LOG")]
    pub log_level: String,

    #[arg(short, long, env = "DISABLE_CREDENTIAL_REFRESH")]
    pub disable_credential_refresh: bool,

    #[arg(short, long, env = "BILI_SYNC_CONFIG_DIR")]
    pub config_dir: Option<PathBuf>,

    #[arg(short, long, env = "BILI_SYNC_FFMPEG_PATH")]
    pub ffmpeg_path: Option<String>,

    /// Comma-separated BVIDs to check for newly appended video pages.
    #[arg(long, env = "BILI_SYNC_REFRESH_MULTIPART_BVIDS", value_delimiter = ',')]
    pub refresh_multipart_bvids: Vec<String>,

    /// Comma-separated BVIDs allowed to queue a new first part on single-to-multipart conversion.
    #[arg(long, env = "BILI_SYNC_REFRESH_REPLACED_FIRST_BVIDS", value_delimiter = ',')]
    pub refresh_replaced_first_bvids: Vec<String>,

    /// Comma-separated uploader MIDs whose eligible videos are checked in rotation.
    #[arg(long, env = "BILI_SYNC_REFRESH_MULTIPART_UPLOADER_MIDS", value_delimiter = ',', value_parser = clap::value_parser!(i64).range(1..))]
    pub refresh_multipart_uploader_mids: Vec<i64>,

    /// Maximum uploader video checks per download round, across all enabled sources.
    #[arg(long, env = "BILI_SYNC_REFRESH_MULTIPART_UPLOADER_BUDGET", default_value_t = 10, value_parser = clap::value_parser!(u16).range(1..=100))]
    pub refresh_multipart_uploader_budget: u16,
}

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub fn version() -> Cow<'static, str> {
    if let (Some(git_version), Some(git_dirty)) = (built_info::GIT_VERSION, built_info::GIT_DIRTY) {
        Cow::Owned(format!("{}{}", git_version, if git_dirty { "-dirty" } else { "" }))
    } else {
        Cow::Borrowed(built_info::PKG_VERSION)
    }
}

fn detail_version() -> String {
    format!(
        "{}
Architecture: {}-{}
Author: {}
Built Time: {}
Rustc Version: {}",
        version(),
        built_info::CFG_OS,
        built_info::CFG_TARGET_ARCH,
        built_info::PKG_AUTHORS,
        built_info::BUILT_TIME_UTC,
        built_info::RUSTC_VERSION,
    )
}
