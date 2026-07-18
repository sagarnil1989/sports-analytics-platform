# Live ML Predictions — Feature Description & Feasibility

---

## ⚠️ Foundational Architecture Principle: Complete Isolation

**The live ML prediction system must be a fully separate process from the existing
pipeline. It must never write to, modify, or interfere with anything the current
ingestion, silver/gold, ended-match, or batch ML flow reads or depends on.**

The existing system has three independent flows that are all running correctly:

```
Flow 1: Ingestion (every 5s)
  capture_inplay_snapshot → bronze/betsapi/…

Flow 2: Ended match processing (daily 02:30 UTC + pl_build_ended_match)
  bronze → silver → gold/event_id=.../innings_tracker.json → ADF ML pipeline

Flow 3: Batch ML (weekly Saturday 03:00 UTC)
  gold/ml/over_under_training_data.csv → trained models → win_predictor_summary.json
```

The live prediction system is **Flow 4** — a completely new, independent consumer:

```
Flow 4: Live ML predictions (isolated, no pipeline dependency)
  CALLS:     BetsAPI /v3/events/inplay + /v1/bet365/event?stats=1  ← direct API, no bronze/silver
  READS:     gold/cricket/ml_features/t20/live_models/win_predictor/*.pkl  ← trained by Flow 3
  WRITES:    gold/cricket/inplay/live_accumulators/event_id=*.json   ← per-over history (grows each 60s)
             gold/event_id=*/live_predictions.json                   ← win predictor output
             gold/cricket/inplay/live_index.json                     ← display function reads this
             gold/cricket/inplay/notification_state.json             ← notifier state
             gold/cricket/config/notification_prefs.json             ← user preferences (also editable via web)
```

**Design note (2026-07):** Flow 4 was originally designed to read silver accumulators. This was
changed because the daily `bronze_to_silver.py` batch explicitly skips active (live) matches, so
silver accumulators for live matches were never written. Flow 4 now calls BetsAPI directly and
builds its own accumulator in gold, accumulating per-over rows every 60 seconds.

### Rules that enforce isolation

| Rule | Rationale |
|---|---|
| Flow 4 only **reads** silver/bronze — never writes | Prevents any risk of corrupting raw data that Flows 1–3 depend on |
| Flow 4 writes to a **new, dedicated gold path** (`live_predictions.json`) | No existing pipeline reads this path; breaking Flow 4 cannot cascade |
| Flow 4 runs in a **separate Azure Function** (`func-ramanuj-live-ml`) — not inside the existing ingestion function | A crash or bug in live inference cannot kill the inplay snapshot capture |
| Flow 4 has **no ADF dependency** — not part of pl_ml_and_hypothesis or any existing pipeline | Can be deployed, stopped, or rolled back independently |
| Model pkls written by Flow 3 to `live_models/` are in a **new blob path** — not the same as any path Flow 3's training reads back | Training loop cannot accidentally pick up stale live-inference artifacts |
| Flow 4's timer/trigger is **independent** of the existing 5-second snapshot timer | Even if Flow 4 is slow or hung, it cannot block Flow 1 |

### What Flow 4 is allowed to touch

```
READ (model artifacts — written by Flow 3 only, never modified by Flow 4):
  gold/cricket/ml_features/t20/live_models/win_predictor/*.pkl

WRITE (new paths, nothing else reads these except cricket_display):
  gold/event_id=*/live_predictions.json
  gold/cricket/inplay/live_index.json
  gold/cricket/inplay/live_accumulators/event_id=*.json
  gold/cricket/inplay/notification_state.json
  gold/cricket/config/notification_prefs.json

NEVER TOUCH:
  silver/**                                    (Flow 4 has no silver access)
  bronze/**                                    (Flow 4 has no bronze access)
  gold/event_id=*/innings_tracker.json         (written by Flow 2 only)
  gold/ml/over_under_training_data.csv         (Flow 3 training input)
  gold/cricket/ml_features/t20/win_predictor_summary.json  (Flow 3 output)
  betsapi/control/**                          (ingestion control files — read-only)
```

---

## What This Feature Does

Apply the two existing trained models — the **win predictor** and the **Over/Under
market models** — to **matches currently in progress**, producing predictions as each
over completes rather than waiting until the match is quiet (ended). The prediction
appears on the innings tracker page (or a dedicated live page) in near-real-time,
showing *"73% chance defending team wins"* or *"68% probability OVER on first-12-overs
market"* as the match evolves.

---

## Why This Is More Feasible Than It First Looks

The critical insight is that a **live data source already exists that uses the exact
same format the models expect.**

The silver writer (`silver_write_outputs`) builds
`silver/event_id={id}/innings_accumulator.json` during every live snapshot parse — a
growing array of per-over snapshot rows, same schema as the gold `innings_tracker.json`
rows the models were trained on. The feature engineering code in `ml_win_predictor.py`
(`per_over_breakdown`, `state_after_n_overs`, `chase_aggregate`) operates purely on
this rows array. At a 2nd-innings checkpoint (e.g. over 6 of the chase), innings 1 is
already complete — so all 20 overs of inn1 features are available in the accumulator,
plus inn2 rows up to the current over.

**No new ingestion required.** The data is already being captured every 5 seconds.

---

## Current Pipeline vs Live Pipeline

### Current (ended matches only)
```
Bronze snapshot (every 5s)
  → silver_write_outputs → innings_accumulator.json (LIVE, but never read by ML)
  → [match goes quiet, 1hr no new snapshot]
  → bronze_to_silver → silver_to_gold → innings_tracker.json (GOLD)
  → ml_win_predictor.py reads gold → trains model
  → win_predictor_summary.json has per-match test predictions
```

### Proposed (live matches)
```
Bronze snapshot (every 5s)
  → innings_accumulator.json updated (ALREADY HAPPENING)
      ↓  [on checkpoint trigger: over 2/6/10/16 of inn2, or inn1 complete]
  → LiveInferenceFunction reads accumulator, builds feature vector
  → loads model pkl from blob storage (new: model saved there at training time)
  → runs inference → writes gold/event_id={id}/live_predictions.json
      ↓
  → innings tracker page reads live_predictions.json and displays probability
```

---

## Two Models, Two Difficulty Levels

### Model 1: Win Predictor (Medium-High difficulty)

**What it predicts:** Which team wins, at 5 checkpoints:
- `innings1-only` — after inn1 complete (before chase begins)
- `innings2-2over` — after 2 overs of the chase
- `innings2-6over` — after 6 overs of the chase (powerplay complete)
- `innings2-10over` — after 10 overs of the chase
- `innings2-16over` — after 16 overs of the chase

**Feature extraction complexity:** The feature vector for the `innings2-6over` model
has ~90+ features (per-over runs/wickets/odds for all 20 overs of inn1 + composite
inn1 features + inn2 checkpoint state). All of these are derivable from the silver
`innings_accumulator.json` rows, but extracting them requires porting the feature
engineering functions (`per_over_breakdown`, `chase_aggregate`, composite feature
calculations) out of the Databricks notebook and into a Python module that the Azure
Function can import.

**Categorical encoding:** The model uses target-mean encodings for venue, batting team,
and bowling team (smoothed averages of win rate per category computed from training
data). These encodings must be stored alongside the model pkl at training time —
currently they're only in memory during the Databricks training run.

**Effort:** Significant — roughly 400–500 lines of feature engineering code to
extract/port + the model storage change.

---

### Model 2: Over/Under Markets (Low-Medium difficulty)

**What it predicts:** Over or Under on 5 betting markets (inn1 first-6, inn1 total,
inn1 first-12, inn2 first-6, inn2 first-12) at various checkpoint overs.

**Feature extraction complexity:** Much simpler — core features are:
`score`, `wickets`, `betting_line`, `over_odds`, `under_odds`, `run_rate`,
`runs_required`, `implied_prob_over`, `batting_team_win_odds`, plus per-over trajectory
features (ov{k}_runs, ov{k}_cumwkts, ov{k}_line, ov{k}_vs_pace). All available
directly from the current snapshot row and the accumulator's per-over history.

The `over_under_predictor.py` library already exists and partially implements live
inference for innings 1 markets. It has a known feature-vector bug (builds 11 features
vs the ~97 the model expects — see notes in this repo) but the logic structure is
correct. Fixing it + storing the model pkl in blob is the main work.

**Effort:** Moderate — fix existing predictor library + model storage + trigger logic.

---

## Implementation Plan

### Step 1: Store models in Blob Storage at training time

In `ml_win_predictor.py` and `ml_train_over_under.py`, after training, upload:

```
gold/cricket/ml_features/t20/live_models/win_predictor/<checkpoint>.pkl
gold/cricket/ml_features/t20/live_models/win_predictor/cat_encodings.json
gold/ml/live_models/over_under/<market>_cp<over>.pkl
gold/ml/live_models/over_under/<market>_pooled.pkl
```

The pkl contains the fitted model + feature list (already stored in the pickle dict
as `model_obj["features"]`). The cat_encodings.json stores the target-mean lookup
tables so the Function App can encode venue/team at inference time.

**Effort: Low** (10–20 lines per notebook).

---

### Step 2: Checkpoint detection trigger

A timer inside `func-ramanuj-live-ml` (every 60s) scans the active-inplay control file
(`bronze/betsapi/control/active_inplay_fi/latest.json` — read-only, written by
`func-ramanuj-ingestion`) to get the list of currently live matches. For each, it
reads the silver accumulator and checks whether a checkpoint was crossed since the last
run. This is **not** inserted into the existing 5-second inplay snapshot loop — it runs
completely independently on its own schedule.

```python
current_over = innings_accumulator[-1]["over"]  # e.g. "6.5" → just completed over 6
if innings == 2 and over_just_completed in {2, 6, 10, 16}:
    trigger_live_inference(event_id)
if innings == 1 and over_just_completed == 20:
    trigger_live_inference(event_id)  # inn1 complete → run innings1-only model
```

Alternatively, a timer-based approach (every 60 seconds) scans all active matches and
checks their accumulators for missed checkpoints. This avoids tight coupling to the
snapshot loop.

**Effort: Low.**

---

### Step 3: Live feature extraction module

A new shared library `infra/7.databricks/lib/live_feature_extractor.py` (deployed to
both Azure Functions and Databricks DBFS) that:

1. Reads `silver/event_id={id}/innings_accumulator.json`
2. Runs the same feature engineering as `ml_win_predictor.py` Steps 3–4 against
   the current rows array
3. Applies categorical target-mean encodings from `cat_encodings.json`
4. Returns a feature dict ready for model inference

For the Over/Under models, fixes the existing `over_under_predictor.py` library to
use `model_obj["features"]` and `model_obj["cat_encodings"]` from the stored pkl
instead of the hardcoded 11-feature `_FEATURES` list.

**Effort: High** (win predictor feature engineering port) / **Low** (O/U fix).

---

### Step 4: Dedicated Live Inference Azure Function App

A **brand-new, separate Azure Function App** (`func-ramanuj-live-ml`), not a new
route inside the existing ingestion or display function. This enforces the isolation
principle at the infrastructure level — a crash or deployment failure in this new app
cannot affect the ingestion pipeline or the display app. It has its own:

- Azure Function App resource (new Terraform resource in a new `infra/10.live-ml/`)
- App Service Plan (can share the same consumption plan as ingestion to keep costs low)
- Deployment slot in `deploy.yml` as a new, optional job

Internally it runs on a timer (every 60 seconds — NOT the same 5-second inplay
snapshot timer) and:

```
Reads: silver innings_accumulator + model pkl from blob
Writes: gold/event_id={id}/live_predictions.json
```

```json
{
  "event_id": "12345678",
  "generated_at_utc": "2026-07-03T15:30:00Z",
  "checkpoints": {
    "innings1-only": {
      "prob_defend": 0.61,
      "prob_chase": 0.39,
      "features_used": { "inn1_total_score": 180, "inn1_pp_rp_wkt": 22.0, ... }
    },
    "innings2-6over": {
      "prob_defend": 0.73,
      "prob_chase": 0.27,
      "features_used": { "inn2_ov6_score": 10, "inn2_ov6_rrr": 24.0, ... }
    }
  },
  "over_under": {
    "inn1_total_cp8": { "prob_over": 0.64, "betting_line": 175.5 },
    "inn2_first_6_cp4": { "prob_over": 0.48, "betting_line": 48.5 }
  }
}
```

**Effort: Medium.**

---

### Step 5: Display on innings tracker page

The existing `/api/innings-tracker/{event_id}` page reads
`gold/event_id={id}/over_under_predictions.json` already. Extend it to also read
`live_predictions.json` and display a prediction panel showing:

- Win probability bar (defend vs chase) for each completed checkpoint
- Over/Under probability per active market
- Last-updated timestamp

**Effort: Low–Medium** (UI addition to existing page).

---

## Summary Difficulty Table

| Component | Effort | Notes |
|---|---|---|
| `infra/10.live-ml/` Terraform module (new Function App) | **Low** | ~30 lines, mirrors existing infra pattern |
| Save model pkls + cat_encodings to blob at training time | **Low** | ~20 lines in Databricks notebooks |
| Checkpoint trigger in new live-ml Function App (read-only) | **Low** | Reads existing control file, writes nothing existing depends on |
| Fix `over_under_predictor.py` feature vector bug | **Medium** | Port to use stored pkl feature list |
| Win predictor feature extraction port | **High** | ~400 lines, must produce bit-identical features to training |
| Write `live_predictions.json` to isolated gold path | **Low** | New path, nothing else reads it |
| Display on innings tracker page | **Medium** | UI addition to existing page |
| `deploy.yml` new job for `func-ramanuj-live-ml` | **Low** | New optional job, no existing job dependency |
| **Total: Over/Under live only** | **~2 days** | Isolated Function App + O/U fix + display |
| **Total: Win predictor live only** | **~3–4 days** | Isolated Function App + feature port + display |
| **Total: Both models live** | **~4–5 days** | All of the above |

---

## Key Risks

1. **Isolation violation by accident.** The most important risk is a future developer
   adding a read or write in the wrong direction — e.g. having the live-ml app touch
   the silver accumulator in a way that conflicts with the ingestion writer, or writing
   a file that the batch ML training accidentally picks up. The `infra/10.live-ml/`
   module boundary, the separate Function App, and the dedicated `live_predictions.json`
   write path (documented in the "rules" table above) are the safeguards. A storage
   role assignment on `func-ramanuj-live-ml`'s managed identity that grants only
   `Storage Blob Data Reader` on the silver container (not Contributor) would enforce
   the read-only rule at the Azure RBAC level.

2. **Feature drift between training and live extraction.** The gold tracker rows are
   built by `tracker_writer.py` (silver layer) then used by `ml_win_predictor.py`
   (Databricks). The live extractor must produce identical field values for the same
   match state. Even small differences (e.g. how "over 6.0" vs "over 6.5" is
   interpreted when looking for the over-6 checkpoint row) would silently produce wrong
   features. Needs careful end-to-end testing against a known historical match.

2. **Snapshot timing: not every over boundary is clean.** Snapshots come every 5s but
   sometimes a checkpoint over row may be missing (dropped snapshot, ingestion gap).
   Need graceful handling (skip that checkpoint, use the nearest available row within
   tolerance).

3. **Model staleness.** Live predictions use the model trained on historical data. If
   the model was last trained a month ago, its cat-encodings don't include recently
   promoted teams or new venues. This is pre-existing — not a new risk, but worth
   noting for monitoring.

4. **No live match concept in current ML design.** The existing pipeline is deliberately
   scoped to finished matches. Live predictions are a NEW concern — they need their own
   trigger, their own write path, and careful isolation from the batch training/
   evaluation pipeline so they don't interfere.

---

## Verdict

**Doable.** The hardest single piece is porting the win predictor's feature engineering
to a portable module — but the data is already there (silver accumulator has the right
format), the model logic is well-understood, and the serving infrastructure (Azure
Functions reading blob storage) already exists. Over/Under live predictions are the
lower-risk starting point since the feature vector is simpler and the predictor library
is already partially written.

**Recommended sequence:**
1. Stand up the isolated `infra/10.live-ml/` Terraform module and empty
   `func-ramanuj-live-ml` Function App first — establishes the isolation boundary
   before any ML code goes in, and can be deployed without touching any existing system.
2. Over/Under live predictions — fix predictor library, store model pkls, add timer
   trigger and write path. Lower risk, immediate value.
3. Win predictor live predictions — port feature engineering, validate against known
   matches, add to the same isolated Function App.

At every step, the existing ingestion, ended-match, and batch ML flows remain
completely unmodified and unaffected.
