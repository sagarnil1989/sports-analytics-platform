# Cost and Purpose

## Resources

| Resource | Purpose | Approx monthly MVP cost |
|---|---|---:|
| Resource Group | Logical container | €0 |
| ADLS Gen2 Storage Account | Bronze/Silver/Gold lake storage | €2–5 |
| Function Storage Account | Required by Azure Functions runtime | €1 |
| Key Vault | Store API keys and secrets | ~€1 |
| Log Analytics + Application Insights | Monitoring and errors | €3–10 |
| Azure SQL Database Basic | Serving database for website/Power BI | ~€5 |
| Azure Functions Consumption | Bronze capture + HTTP API routes | often near €0 at MVP scale |
| Azure Data Factory | Orchestrates silver/gold Databricks jobs | ~€1–2 (activity runs only) |
| Azure Databricks Premium | Silver/gold pipeline processing via job clusters | ~€80–150 (job clusters, pay-per-run) |
| Static Web App Free | Frontend hosting | €0 |

Expected MVP total: roughly **€100–175/month**, dominated by Databricks job cluster runtime.

---

## Architecture Split

| Layer | Runs on | Schedule |
|---|---|---|
| Bronze capture (inplay + prematch) | Azure Function App (timer triggers) | Every 5–60 seconds |
| HTTP API routes | Azure Function App (HTTP triggers) | On demand |
| Silver + gold (ended matches) | ADF `pl_build_ended_match` Activity 1 + 2 → Databricks | Daily 02:00 CET |
| Silver + gold backfill (manual) | ADF `pl_backfill` Activity 1 + 2 → Databricks | Manual only |

`pl_build_ended_match` runs two sequential activities: Activity 1 parses bronze → silver for matches quiet >1 hour; Activity 2 rebuilds gold from silver (only if Activity 1 succeeds). Live match gold pages are out of scope.

---

## Databricks Cost Note

Databricks uses **job clusters** (new cluster per pipeline run) — no always-on cluster cost.
Each run incurs:
- ~2–3 minutes cluster startup time
- Runtime of the notebooks (silver: ~5–15 min, gold: ~2–5 min, depending on backlog)
- Cluster: single-node `Standard_DS4_v2` (28 GB RAM, 8 vCPUs), 128-thread parallel blob processing

One daily run for `pl_build_ended_match`; `pl_backfill` is manual only.
At IPL season peak, expect the lower end of the €80–150 range (single daily run vs. frequent polling).
Off-season cost drops to near zero.
