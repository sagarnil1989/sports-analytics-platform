# Architecture

## Component Overview

| Component | Runs on | Role |
|---|---|---|
| Bronze capture (inplay + prematch) | `func-ramanuj` — timer triggers only | Every 5s–1hr, calls BetsAPI, writes raw snapshots |
| HTTP API / display routes | `func-ramanuj-display` — HTTP triggers only | On-demand reads from gold/bronze; all UI pages |
| Silver + gold (ended matches) | ADF `pl_build_ended_match` → Databricks | Daily at 02:00 CET |
| Manual backfill | ADF `pl_backfill` → Databricks | Triggered manually in ADF Studio |

Live match gold pages are **out of scope** — pipelines process ended and inactive matches only.

---

## Why ADF + Databricks (not Function App)

The silver pipeline originally ran as an Azure Function App timer. Three problems caused repeated data gaps:

| Problem | Details |
|---|---|
| Consumption plan cold starts | Function App scales to zero when idle. Under load (9 concurrent IPL matches), the timer was absent for 73 minutes — zero snapshots processed. |
| 10-minute timeout ceiling | A single match with 2,000+ unprocessed snapshots takes 17+ minutes. Always timed out mid-run, leaving gaps. |
| Sequential marker checks | Old approach checked `blob_exists()` per manifest. With 10,000+ manifests accumulated, this exhausted the 9-minute budget on network round-trips before reaching unprocessed data. |

ADF + Databricks solves all three:
- No timeout ceiling — Databricks jobs run as long as needed
- ADF schedule triggers are guaranteed — never dropped by cold starts
- 128-thread parallel blob processing — IO-bound work scales with concurrency, not clock time
- Quiet-match detection (>60 min since last bronze snapshot) prevents touching active live matches

---

## Databricks Cluster Spec

Single-node job cluster — new cluster per pipeline run, no always-on cost.

| Setting | Value |
|---|---|
| Node type | `Standard_F4s_v2` (4 vCPU, 8 GB) |
| Driver node | Same (single-node mode) |
| Spark config | `spark.databricks.cluster.profile=singleNode`, `spark.master=local[*]` |
| Runtime | Databricks 15.4 LTS (Scala 2.12) |
| Parallelism | 128 threads (IO-bound — more cores provide no benefit) |
| Activity timeout | 4 hours per activity |

Cluster startup adds ~2–3 minutes per pipeline run.

---

## Module Reference

| File | Purpose | Runs on |
|---|---|---|
| `storage.py` | Env vars, blob client helpers, BetsAPI call util | Both |
| `leagues.py` | League preferences, allowed/blocked lists, league discovery | Both |
| `innings_tracker.py` | Innings accumulator logic | Both |
| `bronze.py` | `bronze_*` inplay capture functions | Function App |
| `prematch.py` | `bronze_*` prematch/upcoming capture functions | Function App |
| `silver.py` | `silver_*` parse functions | Databricks via ADF |
| `gold.py` | `gold_*` index build functions | Databricks via ADF |
| `views/` (package) | `view_*` HTTP handlers split across 10 modules | `func-ramanuj-display` (`cricket_display/`) |
| `function_app.py` (ingestion) | Entry point: 4 bronze timer triggers only — no HTTP routes | `func-ramanuj` (`cricket_ingestion/`) |
| `function_app.py` (display) | Entry point: all 35 HTTP routes — no timer triggers | `func-ramanuj-display` (`cricket_display/`) |

### Per-match HTTP routes

| Route | Handler | What it shows |
|---|---|---|
| `/api/matches/{event_id}/view` | `view_match_page_html` | Live match state, current markets, odds timeline |
| `/api/matches/{event_id}/innings-tracker/view` | `view_silver_innings_tracker_html` | Ball-by-ball innings rows from gold tracker |
| `/api/matches/{event_id}/heatmap` | `view_market_heatmap_html` | Betting market availability heatmap by delivery |
| `/api/matches/{event_id}/detailed-analysis` | `view_detailed_analysis_html` | Batting/bowling scorecards, over-by-over, phase breakdown (Powerplay/Middle/Death), chase analysis — reads gold tracker + silver team_scores in parallel |
| `/api/matches/{event_id}/lineage/view` | `view_match_lineage_html` | Data lineage and API call log |

The **Detailed Analysis** page (`view_detailed_analysis_html` in `views/detailed_analysis.py`) loads the gold tracker (single blob) then fetches silver `team_scores.json` for every snapshot in parallel (64 workers) to decode S6/S7/S8. It runs the same scorecard and phase logic as `analysis_match_data_explorer.py`.
| `cricwebsite_db.py` | PostgreSQL writer | Function App |

---

## Cost Breakdown

| Resource | Purpose | Approx monthly |
|---|---|---:|
| ADLS Gen2 Storage Account | Bronze/silver/gold lake | €2–5 |
| Function Storage Account | Required by Azure Functions runtime | €1 |
| Key Vault | API keys and Databricks PAT | ~€1 |
| Log Analytics + App Insights | Monitoring | €3–10 |
| Azure SQL Database Basic | Serving database | ~€5 |
| Azure Functions Consumption (`func-ramanuj`) | Bronze capture — timer triggers only | ~€0 at MVP scale |
| Azure Functions Consumption (`func-ramanuj-display`) | All HTTP/display routes | ~€0 at MVP scale |
| Display Function Storage Account (`stramanujdispweu`) | Required by `func-ramanuj-display` runtime | ~€1–2 |
| Azure Data Factory | Pipeline orchestration | ~€1–2 |
| Azure Databricks Premium | Silver/gold notebook execution | ~€80–150 |
| Static Web App Free | Frontend hosting | €0 |

**Expected total: ~€100–175/month**, dominated by Databricks job cluster runtime.

Databricks cost scales with run frequency. At IPL season peak (one daily run), expect the lower end. Off-season cost drops to near zero — clusters only spin up when a pipeline runs.

### Databricks networking — VNet injection

The workspace uses VNet injection (`infra/7.databricks/network.tf`) with `no_public_ip = false`. This means:
- Azure does **not** create a managed VNet or NAT gateway (saves ~€30/month)
- Cluster nodes have their own public IPs for outbound traffic — no NAT gateway needed
- The VNet (`10.139.0.0/16`) lives in the same resource group as all other resources
