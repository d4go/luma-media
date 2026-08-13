use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::ingestion::SyncMode;

use super::handler::{JobContext, JobHandler};

const DEFAULT_MAX_PAGES: i64 = 200_000;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapConfig {
    provider_key: String,
}

fn default_mode() -> String {
    "bootstrap".into()
}

fn default_max_pages() -> i64 {
    DEFAULT_MAX_PAGES
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageCheckpoint {
    page: i64,
    page_url: String,
    from: String,
    to: String,
    include_resources: bool,
    #[serde(default = "default_mode")]
    mode: String,
    #[serde(default = "default_max_pages")]
    max_pages: i64,
}

/// Unified task-engine bootstrap/incremental handler.
///
/// Each JobItem is one catalogue page (master plan §41 Option A). The page is
/// fetched through the regular provider gate, candidates are upserted and
/// hydration jobs are enqueued through the existing ingestion queue, and the
/// next page is appended as a new JobItem so the run is observable and
/// pausable/resumable at page granularity.
pub struct BootstrapHandler;

#[async_trait]
impl JobHandler for BootstrapHandler {
    fn job_type(&self) -> &'static str {
        "bootstrap"
    }

    async fn run(&self, ctx: &mut JobContext<'_>) -> anyhow::Result<()> {
        let config: BootstrapConfig = serde_json::from_value(ctx.run_config().await?)?;
        let checkpoint: PageCheckpoint = serde_json::from_value(ctx.item_checkpoint().await?)?;
        let provider = crate::product::source_provider_by_key(ctx.state, &config.provider_key)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("source provider {} no longer exists", config.provider_key)
            })?;

        let page =
            crate::product::fetch_source_catalogue_page(ctx.state, &provider, &checkpoint.page_url)
                .await?;
        let reached_start = crate::ingestion::reached_window_start(&page.items, &checkpoint.from);
        let candidates = crate::ingestion::upsert_candidates(
            &ctx.state.pool,
            &provider.key,
            page.items
                .iter()
                .filter(|item| {
                    crate::ingestion::within_window(item, &checkpoint.from, &checkpoint.to)
                })
                .cloned()
                .collect(),
        )
        .await?;
        let mode = if checkpoint.mode == "incremental" {
            SyncMode::Incremental
        } else {
            SyncMode::Bootstrap
        };
        let pending = crate::product::enqueue_candidate_hydrations(
            ctx.state,
            &provider,
            &candidates,
            mode,
            checkpoint.include_resources,
            ctx.run_id,
        )
        .await?;

        let next_url = if reached_start { None } else { page.next_url };
        ctx.log(
            "page",
            &format!(
                "第 {} 页：发现 {} 项，待补全 {} 项",
                checkpoint.page,
                candidates.len(),
                pending
            ),
            json!({
                "page": checkpoint.page,
                "discovered": candidates.len(),
                "pendingHydrations": pending,
                "nextUrl": next_url,
            }),
        )
        .await?;
        ctx.report_progress(
            checkpoint.page,
            None,
            json!({
                "page": checkpoint.page,
                "nextUrl": next_url,
                "mode": checkpoint.mode,
            }),
        )
        .await?;

        if let Some(next_url) = next_url {
            let next_page = checkpoint.page + 1;
            if next_page <= checkpoint.max_pages {
                let next_checkpoint = json!({
                    "page": next_page,
                    "pageUrl": next_url,
                    "from": checkpoint.from,
                    "to": checkpoint.to,
                    "includeResources": checkpoint.include_resources,
                    "mode": checkpoint.mode,
                    "maxPages": checkpoint.max_pages,
                });
                ctx.engine
                    .create_item(ctx.run_id, &format!("page:{next_page}"), next_checkpoint)
                    .await?;
            }
        }
        Ok(())
    }
}

pub fn default_bootstrap_config(
    provider_key: &str,
    from: String,
    to: String,
    include_resources: bool,
    mode: &str,
) -> Value {
    json!({
        "providerKey": provider_key,
        "from": from,
        "to": to,
        "includeResources": include_resources,
        "mode": mode,
        "maxPages": DEFAULT_MAX_PAGES,
    })
}
