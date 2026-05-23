# Gold Rebuild for Ended Matches

> **Status (May 2026): Implemented** as Activity 2 inside ADF pipeline `pl_build_ended_match`,
> chained on Activity 1 (silver) success. The standalone `pl_gold_auto_rebuild` pipeline
> has been removed.

---

## How Gold Rebuild Works

Gold rebuild is **Activity 2** in `pl_build_ended_match` (and in `pl_backfill` for manual runs).
It runs only if Activity 1 (silver) succeeded, using ADF `dependsOn` with `dependencyConditions = ["Succeeded"]`.

### What it does

1. Scans silver for all events where a processed_snapshot marker exists
2. Compares the newest marker timestamp per event against `last_modified` of `gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json`
3. Rebuilds only where the gold file is missing or older than the newest silver marker
4. Reads from **silver only** — no bronze access
5. Writes `gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json`

This means gold rebuild is always driven by silver data that was just written in Activity 1, so it naturally picks up new matches without any watermark.

---

## The Original Problem (Historical)

When a cricket match ends, the system needs to rebuild `innings_1_from_silver.json` — a
gold-layer file that aggregates all historical silver snapshots into the full innings
timeline. Without this file a match never appears in `/api/ended/view`.

The original Function App timer (`auto_rebuild_ended_innings_tracker`, every 10 minutes)
worked for T20 matches but had a hard ceiling that made it unreliable:

| Match type | Approx snapshots | Rebuild time | Result |
|------------|-----------------|--------------|--------|
| T20 (IPL)  | ~800–1,000      | ~2–4 min     | OK |
| ODI (50-over) | ~3,000–5,000 | ~6–10 min | Marginal |
| Test match | 10,000–50,000+  | exceeds 10 min | Always times out |

The standalone `pl_gold_auto_rebuild` pipeline (every 15 min) that replaced it has since
been consolidated into Activity 2 of `pl_build_ended_match` to simplify orchestration.

---

## Infrastructure

| Resource | Location |
|---|---|
| Databricks notebook (daily) | `infra/7.databricks/notebooks/gold_build_ended_match.py` |
| Databricks notebook (backfill) | `infra/7.databricks/notebooks/gold_backfill.py` |
| Source Python | `src/functions/cricket_ingestion/views.py` — `gold_rebuild_ended_matches()` |
| ADF pipeline (daily) | `infra/8.adf-config/main.tf` — `azurerm_data_factory_pipeline.build_ended_match` — Activity 2 |
| ADF pipeline (manual) | `infra/8.adf-config/main.tf` — `azurerm_data_factory_pipeline.backfill` — Activity 2 |
| ADF trigger (daily) | `infra/8.adf-config/main.tf` — schedule trigger at 02:00 CET |

---

## Triggering a Manual Rebuild

For one-off operator-triggered rebuilds, use ADF Studio to trigger `pl_backfill`:

- Pass `event_id=<id>` to rebuild one specific match
- Leave `event_id` empty to rebuild all quiet matches

The backfill pipeline runs Activity 1 (silver) then Activity 2 (gold) in sequence.
`discover_cricket_ended` picks up the rebuilt gold file on its next hourly run, after which
the match appears in `/api/ended/view`.
