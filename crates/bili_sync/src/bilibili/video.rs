use anyhow::{Context, Result, ensure};
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use prost::Message;
use reqwest::Method;
use serde_json::Value;
use std::collections::HashMap;

use crate::bilibili::analyzer::PageAnalyzer;
use crate::bilibili::client::BiliClient;
use crate::bilibili::danmaku::{DanmakuElem, DanmakuWriter, DmSegMobileReply};
use crate::bilibili::subtitle::{SubTitle, SubTitleBody, SubTitleInfo, SubTitlesInfo};
use crate::bilibili::{Credential, ErrorForStatusExt, MIXIN_KEY, Validate, VideoInfo, WbiSign};

fn bvid_to_aid(bvid: &str) -> Result<u64> {
    const ALPHABET: &[u8] = b"FcwAPNKTMug3GV5Lj7EJnHpWsx4tb8haYeviqBz6rkCy12mUSDQX9RdoZf";
    let mut bytes = bvid.as_bytes().to_vec();
    ensure!(bytes.len() == 12 && bytes.starts_with(b"BV1"), "invalid BVID: {bvid}");
    bytes.swap(3, 9);
    bytes.swap(4, 7);
    let mut value = 0_u64;
    for byte in &bytes[3..] {
        let digit = ALPHABET
            .iter()
            .position(|c| c == byte)
            .with_context(|| format!("invalid BVID: {bvid}"))?;
        value = value
            .checked_mul(58)
            .and_then(|v| v.checked_add(digit as u64))
            .context("BVID overflow")?;
    }
    Ok((value & ((1_u64 << 51) - 1)) ^ 23_442_827_791_579)
}

/// Fetch page counts for up to 100 videos. Missing or invalid entries remain unknown.
pub async fn get_batch_page_counts(
    bili_client: &BiliClient,
    credential: &Credential,
    bvids: &[String],
) -> Result<HashMap<String, usize>> {
    ensure!(bvids.len() <= 100, "at most 100 BVIDs per request");
    let requested: HashMap<_, _> = bvids
        .iter()
        .filter_map(|bvid| bvid_to_aid(bvid).ok().map(|aid| (bvid.as_str(), aid)))
        .collect();
    if requested.is_empty() {
        return Ok(HashMap::new());
    }
    let resources = requested
        .values()
        .map(|aid| format!("{aid}:2"))
        .collect::<Vec<_>>()
        .join(",");
    let response = bili_client
        .request(
            Method::GET,
            "https://api.bilibili.com/x/v3/fav/resource/infos",
            credential,
        )
        .await
        .query(&[("resources", resources.as_str()), ("platform", "web")])
        .send()
        .await?
        .error_for_status_ext()?
        .json::<Value>()
        .await?
        .validate()?;
    let entries = response["data"]
        .as_array()
        .context("resource infos data is not an array")?;
    let mut counts = HashMap::new();
    for entry in entries {
        let Some(bvid) = entry["bvid"].as_str() else { continue };
        let Some(&aid) = requested.get(bvid) else { continue };
        if !entry["id"].is_null() && entry["id"].as_u64() != Some(aid) {
            continue;
        }
        let Some(count) = entry["page"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|&n| n > 0)
        else {
            continue;
        };
        counts.insert(bvid.to_owned(), count);
    }
    Ok(counts)
}

#[cfg(test)]
mod batch_page_count_tests {
    use super::bvid_to_aid;

    #[test]
    fn converts_bvids_to_aids() {
        for (bvid, aid) in [
            ("BV1AHxZ6vEEQ", 117559865251529),
            ("BV16mtU6kEu9", 117201587737721),
            ("BV1iCs26dEaE", 117636855760495),
            ("BV17x411w7KC", 170001),
        ] {
            assert_eq!(bvid_to_aid(bvid).unwrap(), aid);
        }
        assert!(bvid_to_aid("invalid").is_err());
    }
}

pub struct Video<'a> {
    client: &'a BiliClient,
    pub bvid: &'a str,
    credential: &'a Credential,
}

#[derive(Clone, Debug, serde::Deserialize, Default)]
pub struct PageInfo {
    pub cid: i64,
    pub page: i32,
    #[serde(rename = "part")]
    pub name: String,
    pub duration: u32,
    pub first_frame: Option<String>,
    pub dimension: Option<Dimension>,
}

#[derive(Clone, Debug, serde::Deserialize, Default)]
pub struct Dimension {
    pub width: u32,
    pub height: u32,
    pub rotate: u32,
}

impl<'a> Video<'a> {
    pub fn new(client: &'a BiliClient, bvid: &'a str, credential: &'a Credential) -> Self {
        Self {
            client,
            bvid,
            credential,
        }
    }

    /// 直接调用视频信息接口获取详细的视频信息，视频信息中包含了视频的分页信息
    pub async fn get_view_info(&self) -> Result<VideoInfo> {
        let mut res = self
            .client
            .request(
                Method::GET,
                "https://api.bilibili.com/x/web-interface/wbi/view",
                self.credential,
            )
            .await
            .query(&[("bvid", &self.bvid)])
            .wbi_sign(MIXIN_KEY.load().as_deref())?
            .send()
            .await?
            .error_for_status_ext()?
            .json::<serde_json::Value>()
            .await?
            .validate()?;
        Ok(serde_json::from_value(res["data"].take())?)
    }

    pub async fn get_pages(&self) -> Result<Vec<PageInfo>> {
        let mut res = self
            .client
            .request(
                Method::GET,
                "https://api.bilibili.com/x/player/pagelist",
                self.credential,
            )
            .await
            .query(&[("bvid", &self.bvid)])
            .send()
            .await?
            .error_for_status_ext()?
            .json::<serde_json::Value>()
            .await?
            .validate()?;
        Ok(serde_json::from_value(res["data"].take())?)
    }

    pub async fn get_tags(&self) -> Result<Vec<String>> {
        let mut res = self
            .client
            .request(
                Method::GET,
                "https://api.bilibili.com/x/web-interface/view/detail/tag",
                self.credential,
            )
            .await
            .query(&[("bvid", &self.bvid)])
            .send()
            .await?
            .error_for_status_ext()?
            .json::<serde_json::Value>()
            .await?
            .validate()?;
        Ok(res["data"]
            .as_array_mut()
            .context("tags is not an array")?
            .iter_mut()
            .filter_map(|v| if let Value::String(s) = v.take() { Some(s) } else { None })
            .collect())
    }

    pub async fn get_danmaku_writer(&self, page: &'a PageInfo) -> Result<DanmakuWriter<'a>> {
        let tasks = FuturesUnordered::new();
        for i in 1..=page.duration.div_ceil(360) {
            tasks.push(self.get_danmaku_segment(page, i as i64));
        }
        let result: Vec<Vec<DanmakuElem>> = tasks.try_collect().await?;
        let mut result: Vec<DanmakuElem> = result.into_iter().flatten().collect();
        result.sort_by_key(|d| d.progress);
        Ok(DanmakuWriter::new(page, result.into_iter().map(|x| x.into()).collect()))
    }

    async fn get_danmaku_segment(&self, page: &PageInfo, segment_idx: i64) -> Result<Vec<DanmakuElem>> {
        let mut res = self
            .client
            .request(
                Method::GET,
                "https://api.bilibili.com/x/v2/dm/wbi/web/seg.so",
                self.credential,
            )
            .await
            .query(&[("type", 1), ("oid", page.cid), ("segment_index", segment_idx)])
            .wbi_sign(MIXIN_KEY.load().as_deref())?
            .send()
            .await?
            .error_for_status_ext()?;
        let headers = std::mem::take(res.headers_mut());
        let content_type = headers.get("content-type");
        ensure!(
            content_type.is_some_and(|v| v == "application/octet-stream"),
            "unexpected content type: {:?}, body: {:?}",
            content_type,
            res.text().await
        );
        Ok(DmSegMobileReply::decode(res.bytes().await?)?.elems)
    }

    pub async fn get_page_analyzer(&self, page: &PageInfo) -> Result<PageAnalyzer> {
        let mut res = self
            .client
            .request(
                Method::GET,
                "https://api.bilibili.com/x/player/wbi/playurl",
                self.credential,
            )
            .await
            .query(&[
                ("bvid", self.bvid),
                ("qn", "127"),
                ("otype", "json"),
                ("fnval", "4048"),
                ("fourk", "1"),
            ])
            .query(&[("cid", page.cid)])
            .wbi_sign(MIXIN_KEY.load().as_deref())?
            .send()
            .await?
            .error_for_status_ext()?
            .json::<serde_json::Value>()
            .await?
            .validate()?;
        Ok(PageAnalyzer::new(res["data"].take()))
    }

    pub async fn get_subtitles(&self, page: &PageInfo) -> Result<Vec<SubTitle>> {
        let mut res = self
            .client
            .request(Method::GET, "https://api.bilibili.com/x/player/wbi/v2", self.credential)
            .await
            .query(&[("bvid", self.bvid)])
            .query(&[("cid", page.cid)])
            .wbi_sign(MIXIN_KEY.load().as_deref())?
            .send()
            .await?
            .error_for_status_ext()?
            .json::<serde_json::Value>()
            .await?
            .validate()?;
        // 接口返回的信息，包含了一系列的字幕，每个字幕包含了字幕的语言和 json 下载地址
        match serde_json::from_value::<Option<SubTitlesInfo>>(res["data"]["subtitle"].take())? {
            Some(subtitles_info) => {
                let tasks = subtitles_info
                    .subtitles
                    .into_iter()
                    .filter(|v| !v.is_ai_sub())
                    .map(|v| self.get_subtitle(v))
                    .collect::<FuturesUnordered<_>>();
                tasks.try_collect().await
            }
            None => Ok(vec![]),
        }
    }

    async fn get_subtitle(&self, info: SubTitleInfo) -> Result<SubTitle> {
        let mut res = self
            .client
            .client // 这里可以直接使用 inner_client，因为该请求不需要鉴权
            .request(Method::GET, format!("https:{}", &info.subtitle_url).as_str(), None)
            .send()
            .await?
            .error_for_status_ext()?
            .json::<serde_json::Value>()
            .await?;
        let body: SubTitleBody = serde_json::from_value(res["body"].take())?;
        Ok(SubTitle { lan: info.lan, body })
    }
}
