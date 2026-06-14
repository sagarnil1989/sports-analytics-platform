# Databricks Notebooks — Pipeline Index

Each `.py` file here is a Databricks notebook uploaded to the workspace.
This index maps each notebook to its ADF pipeline (exact name as in Terraform), trigger schedule, and purpose.

---

## ADF-Triggered Notebooks

| Notebook | ADF Pipeline | Activity Name | Schedule | Purpose |
|---|---|---|---|---|
| `bronze_to_silver.py` | `pl_build_ended_match` (activity 1) | `bronze_to_silver` | Daily 02:00 CET | No `event_id` → processes all matches quiet ≥60 min. Also used by `pl_backfill` manually with an `event_id` to target one match. [→ Guide](bronze_to_silver.md) |
| `silver_to_gold.py` | `pl_build_ended_match` (activity 2) | `silver_to_gold` | Daily (chained after bronze_to_silver) | Reads silver, rebuilds `innings_1_from_silver.json` for every ended match |
| `discover_cricket_ended.py` | `pl_build_ended_match` (activity 3) | `discover_cricket_ended` | Daily (chained after silver_to_gold) | Scans gold for `innings_1_from_silver.json` files and writes the ended match index to bronze |
| `hypothesis_inn2_over6.py` | `pl_build_ended_match` (activity 4a) | `hypothesis_inn2_over6` | Daily (chained after discover) | Tests "innings-2 over-6 favourite wins" hypothesis, writes result to gold |
| `hypothesis_timeout_wicket.py` | `pl_build_ended_match` (activity 4b) | `hypothesis_timeout_wicket` | Daily (chained after discover) | Tests "wicket after strategic timeout" hypothesis, writes result to gold |

> **Notebooks not yet wired to an ADF pipeline** (no pipeline exists in Terraform yet):
> `gold_auto_rebuild.py`, `gold_match_pages.py`, `gold_build_prematch_pages.py`

---

## Manual / Analysis Notebooks (Databricks UI only, no ADF pipeline)

| Notebook | Purpose |
|---|---|
| `analysis_bronze_silver_counts.py` | Shows which matches have unprocessed bronze snapshots — use to check backlog |
| `analysis_match_data_explorer.py` | Full decoded match breakdown: batting/bowling scorecard, odds movement, ball analysis, chase data |
| `analysis_snapshot_timeline.py` | Declare an `event_id`, see every silver snapshot linked to match state, score progression, and dedup gaps |
| `bronze_dedup_cleanup.py` | One-off cleanup of duplicate bronze snapshots from the old MD5 dedup bug |

---

## Detailed Guides

- [bronze_to_silver.md](bronze_to_silver.md) — step-by-step for `bronze_to_silver.py` (covers both nightly and manual use)

---

## Pipeline Chain — `pl_build_ended_match` (daily 02:00 CET)

```
pl_build_ended_match
    │
    ├─ Activity 1: bronze_to_silver  (no event_id passed)
    │     → silver/cricket/inplay/.../match_state.json
    │     → silver/cricket/inplay/.../team_scores.json
    │     → silver/cricket/inplay/.../market_odds.json
    │
    ├─ Activity 2: silver_to_gold  (runs after Activity 1 succeeds)
    │     → gold/cricket/innings_tracker/event_id=*/innings_1_from_silver.json
    │
    ├─ Activity 3: discover_cricket_ended  (runs after Activity 2 succeeds)
    │     → bronze/cricket/ended/latest/index.json
    │
    ├─ Activity 4a: hypothesis_inn2_over6  (runs after Activity 3 succeeds)
    │     → gold/cricket/hypothesis/inn2_over6_favorite.json
    │
    └─ Activity 4b: hypothesis_timeout_wicket  (runs after Activity 3 succeeds)
          → gold/cricket/hypothesis/timeout_wicket.json
```

## Pipeline Chain — `pl_backfill` (manual)

```
pl_backfill  [parameter: event_id]
    │
    ├─ Activity 1: bronze_to_silver
    │     → silver layer for the specified match (or all quiet matches)
    │
    └─ Activity 2: silver_to_gold  (runs after Activity 1 succeeds)
          → gold/cricket/innings_tracker/event_id=*/innings_1_from_silver.json
```
