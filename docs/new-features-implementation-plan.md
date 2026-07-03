# New Features — Implementation Plan

Three features across six build phases.

- **Feature 1:** Odds Movement Analysis → [`docs/odds-movement-analysis.md`](odds-movement-analysis.md)
- **Feature 2:** Win Predictor What-If / Sensitivity Analysis → [`docs/win-predictor-whatif.md`](win-predictor-whatif.md)
- **Feature 3:** Live ML Predictions → [`docs/live-ml-predictions.md`](live-ml-predictions.md)

---

## Sequencing Logic

Two shared dependencies bind the three features:

- **Phase 2 (Model Storage)** is a prerequisite for Feature 2 (Phase 3) and Feature 3 (Phases 5–6). Do it once, unlock both.
- **Phase 4 (Live ML Infra)** must exist before any live ML code ships — the empty isolated Function App goes up first, functionality goes in after.

```
Phase 1 — Odds Movement Analysis    ← independent, ship first, fast value
Phase 2 — Model Storage             ← shared prerequisite for Phases 3, 5, 6
Phase 3 — What-If Sensitivity       ← uses Phase 2 models, no new infra
Phase 4 — Live ML Infra             ← empty isolated Function App
Phase 5 — Live ML: Over/Under       ← uses Phase 2 models + Phase 4 infra
Phase 6 — Live ML: Win Predictor    ← hardest, builds on everything above
```

Phases 1 and 4 are independent and can run in parallel with everything else. Phase 2
can be triggered manually once coded without waiting for the Saturday pipeline.
Phases 3 and 5 can run in parallel once Phase 2 is done.

---

## Phase 1 — Odds Movement Analysis

**~2 days | No dependencies | Feature 1**

### 1.1 — Databricks hypothesis notebook

File: `infra/7.databricks/notebooks/hypothesis_odds_movement.py`

- Scan all gold `innings_tracker.json` files — `batting_team_odds` / `bowling_team_odds`
  are already captured per snapshot row, no new ingestion needed.
- Per match compute:
  - Peak odds reached by each team at any point in the match
  - Swing magnitude — how far from 2.0 the peak went (extra return above even-money)
  - Double-opportunity flag — did both teams' odds exceed 2.0 at any point?
  - Net profit if both peak-odds bets were placed (winning side odds − 2.0)
  - Pre-match odds gap (from bronze prematch blobs where captured)
  - Over and innings where the biggest single-snapshot odds move occurred
- Aggregate by league and by team (home + away)
- Write: `gold/cricket/analysis/odds_movement_summary.json`

### 1.2 — ADF pipeline activity

File: `infra/8.adf-config/main.tf`

Add `OddsMovementAnalysis` activity to `pl_ml_and_hypothesis` — same Custom/Batch
pattern as `HypothesisInn2Over6` / `HypothesisTimeoutWicket` / `HypothesisInn1Prematch`.
No dependency on any other activity in the pipeline (runs in parallel at startup).

### 1.3 — Display page

Files:
- `src/functions/cricket_display/views/odds_movement.py` (new)
- `src/functions/cricket_display/views/__init__.py` (export)
- `src/functions/cricket_display/function_app.py` (route)

Route: `GET /api/analysis/odds-movement`

Four tabs on the page:
- **Tab 1 — Match Ranking:** Table of all matches sorted by swing magnitude, largest
  first. Columns: match, date, league, peak odds Team A, peak odds Team B, swing,
  double-opportunity flag (✓/✗), profit potential if both sides backed at peak.
- **Tab 2 — By League:** Aggregated stats per league/tournament — average swing,
  % of matches with a double-opportunity, highest single-match swing ever seen.
- **Tab 3 — By Team:** Which teams are most often involved in large-swing matches.
  Split by batting-first vs chasing, so patterns like "collapse as batting side" vs
  "collapse as chasing side" are visible.
- **Tab 4 — Starting Odds Gap vs Swing:** Scatter showing pre-match odds gap on the
  X-axis vs in-play swing on the Y-axis. Tests the hypothesis: do close pre-match
  odds predict bigger swings?

---

## Phase 2 — Model Storage

**~0.5 days | No dependencies | Shared prerequisite for Phases 3, 5, 6**

### 2.1 — Win predictor models to blob

File: `infra/9.ml/notebooks/ml_win_predictor.py`

After training each checkpoint model, upload to blob:
```
gold/cricket/ml_features/t20/live_models/win_predictor/innings1-only.pkl
gold/cricket/ml_features/t20/live_models/win_predictor/innings2-2over.pkl
gold/cricket/ml_features/t20/live_models/win_predictor/innings2-6over.pkl
gold/cricket/ml_features/t20/live_models/win_predictor/innings2-10over.pkl
gold/cricket/ml_features/t20/live_models/win_predictor/innings2-16over.pkl
gold/cricket/ml_features/t20/live_models/win_predictor/cat_encodings.json
```

Each pkl already contains `model_obj["features"]` (the pruned feature list) and
`model_obj["cat_encodings"]` (venue/team target-mean lookup tables built from training
data). `cat_encodings.json` is a human-readable copy of the same data for use by the
Azure Function at serve time without needing to unpickle.

~10 lines of change.

### 2.2 — Over/Under models to blob

File: `infra/7.databricks/notebooks/ml_train_over_under.py`

After training, upload LightGBM pooled and per-checkpoint models:
```
gold/ml/live_models/over_under/<market>_pooled.pkl
gold/ml/live_models/over_under/<market>_cp<over>.pkl
```

~10 lines of change.

> **Note:** Once coded, trigger `pl_ml_and_hypothesis` manually (or run the two
> notebooks directly in Databricks) to populate the model blobs — do not wait for the
> Saturday schedule to unblock Phase 3 and Phase 5.

---

## Phase 3 — Win Predictor What-If / Sensitivity Analysis ✓ DONE

**Implemented 2026-07-03 | Needs Phase 2**

### 3.1 — Backend inference endpoint

File: `src/functions/cricket_display/views/win_predictor_whatif.py` (new)

- On first request, download the pruned XGBoost model pkl for the requested checkpoint
  from `gold/cricket/ml_features/t20/live_models/win_predictor/` and cache in memory.
- Receive POST body:
  ```json
  { "checkpoint": "innings2-6over", "features": { "inn1_total_score": 160, ... } }
  ```
- Apply categorical target-mean encodings for venue, batting team, bowling team from
  `cat_encodings.json`.
- Run `model.predict_proba(feature_vector)` and return:
  ```json
  { "prob_defend": 0.61, "prob_chase": 0.39 }
  ```

Route: `POST /api/ml/win-predictor/whatif`

### 3.2 — What-If panel UI

File: `src/functions/cricket_display/views/win_predictor.py`

Add a **"What if?"** toggle on each test-set prediction row. Expanding it shows:

**Section 1 — 1st Innings Summary:** Total score, total wickets, powerplay
runs/wickets, middle-over runs/wickets, death runs/wickets, pressure metric, odds
swing across inn1. All editable.

**Section 2 — 2nd Innings State at Checkpoint:** Runs scored, wickets down, current
run rate, required run rate, runs needed, runs-per-wicket-in-hand, per-over runs and
wickets. All editable. Derived fields (CRR, RRR, runs-per-wkt) are recalculated by
JavaScript when raw inputs change and shown read-only with a `=` badge.

**Section 3 — Context:** Venue (dropdown), batting team, bowling team, women's match,
weekend match.

Alongside each input, display a feature-importance bar taken from the stored feature
importances in `win_predictor_summary.json`, so the user can immediately see which
factor contributes most to the prediction.

**Re-run button:** Posts the edited feature dict to `/api/ml/win-predictor/whatif`,
receives updated probability, updates the probability bar in-place with a diff badge
showing e.g. `↓ −12pp`.

**Reset all button:** Restores every value to the original match values.

---

## Phase 4 — Live ML Isolated Infrastructure

**~0.5 days | No dependencies | Prerequisite for Phases 5, 6**

This phase ships an **empty** Function App that does nothing except prove the isolation
boundary works before any ML code goes in.

### 4.1 — New Terraform module

Directory: `infra/10.live-ml/`

Resources:
- `azurerm_linux_function_app` named `func-ramanuj-live-ml`
- Shares the same consumption App Service Plan as `func-ramanuj-ingestion` (no added
  cost)
- Managed identity RBAC:
  - `Storage Blob Data Reader` on the **silver** container → enforces read-only at the
    Azure level; the function physically cannot corrupt silver data
  - `Storage Blob Data Contributor` on the **gold** container scoped to the
    `event_id=*/live_predictions.json` path only → cannot accidentally write to
    training data or model summaries

### 4.2 — New Function App source

Directory: `src/functions/cricket_live_ml/`

- `function_app.py` with a single timer trigger every **60 seconds** (not the existing
  5-second snapshot timer — completely independent)
- Body initially empty — just logs `"alive"` and exits
- `requirements.txt`: `xgboost`, `lightgbm`, `scikit-learn`, `azure-storage-blob`

### 4.3 — CI job

File: `.github/workflows/deploy.yml`

New optional job `deploy_live_ml` that deploys `func-ramanuj-live-ml`. Can be skipped
with a `skip_live_ml: true` workflow dispatch input so it never risks blocking
the existing deploy.

---

## Phase 5 — Live ML: Over/Under Predictions ✓ DONE

**Implemented 2026-07-03 | Needs Phases 2 + 4**

### 5.1 — Fix the over_under_predictor feature-vector bug

File: `infra/7.databricks/lib/over_under_predictor.py`

Currently builds a hardcoded 11-feature vector; the trained models expect ~97 features.
Fix: replace `_FEATURES` / `_build_vector` with reading `model_obj["features"]` and
`model_obj["cat_encodings"]` from the stored pkl (Phase 2). Load model from blob
storage (`gold/ml/live_models/over_under/`) instead of DBFS.

### 5.2 — Live Over/Under predictor

File: `src/functions/cricket_live_ml/live_ou_predictor.py` (new)

Timer logic (called every 60s):
1. Read `bronze/betsapi/control/active_inplay_fi/latest.json` **read-only** — list of
   currently live matches
2. For each match, read `silver/event_id={id}/innings_accumulator.json` **read-only**
3. Detect which O/U checkpoint overs have been completed since last run
   (overs 1–5 for first-6 market; overs 2/4/6/8 for first-12; etc.)
4. For each new checkpoint: build feature vector, load model from blob, run inference
5. Write results to `gold/event_id={id}/live_predictions.json` — **new isolated path,
   nothing in the existing pipeline reads this**

### 5.3 — Display

File: `src/functions/cricket_display/views/innings_tracker.py`

If `live_predictions.json` exists for the match being viewed, show a Live Predictions
panel above the innings tracker table — probability bars per O/U market with the
current checkpoint, betting line, and model AUC.

---

## Phase 6 — Live ML: Win Predictor Predictions

**~3 days | Hardest phase | Needs Phases 2 + 4 + 5**

### 6.1 — Live feature extraction module

File: `infra/7.databricks/lib/live_feature_extractor.py` (new, also deployed to
`func-ramanuj-live-ml` via `src/functions/cricket_live_ml/`)

Port the following from `ml_win_predictor.py` Steps 3–4 into a standalone module (~400
lines):
- `per_over_breakdown(rows, start, end)` — per-over runs, wickets, odds
- `state_after_n_overs(rows, n)` — match state snapshot at end of over N
- `chase_aggregate(rows, inn1_total, checkpoint)` — CRR, RRR, runs needed, etc.
- All composite inn1 features (pressure, powerplay/mid/death breakdowns, odds swing,
  bat dominance)
- All composite inn2 features per checkpoint
- Categorical encoding application from `cat_encodings.json`

Input: `innings_accumulator` rows (same schema as gold tracker rows — this is the
key reason it's feasible without new ingestion).

Output: feature dict matching `model_obj["features"]` for the requested checkpoint.

**Validation before shipping (mandatory):** Run the extractor against 10 known
historical matches. Compare its output field-by-field against the feature values the
training notebook produced for those same matches. Zero tolerance for differences — any
mismatch means live predictions would be on a different distribution from the training
data. Fix before proceeding.

### 6.2 — Live win predictor logic

File: `src/functions/cricket_live_ml/live_win_predictor.py` (new)

Called by the same 60-second timer as `live_ou_predictor.py`:
1. Reads silver accumulator (read-only)
2. Detects completed checkpoints: inn1 over 20 done → `innings1-only`; inn2 over 2,
   6, 10, 16 done → corresponding models
3. For each new checkpoint: calls `live_feature_extractor`, loads model pkl from blob,
   runs inference, merges win probability into `gold/event_id={id}/live_predictions.json`
   alongside the O/U predictions from Phase 5

### 6.3 — Display

File: `src/functions/cricket_display/views/innings_tracker.py`

Extend the live predictions panel from Phase 5 to also show:
- Win probability bar (defend vs chase) with the checkpoint label (e.g. "After 6
  overs of the chase")
- Updates each time a new checkpoint is reached — previous checkpoints remain visible
  showing how the probability evolved during the match

---

## Timeline Summary

| Phase | Feature | Effort | Can start when |
|---|---|---|---|
| 1 — Odds Movement | Feature 1 | ~2 days | Immediately |
| 2 — Model Storage | Shared | ~0.5 days | Immediately |
| 3 — What-If Sensitivity | Feature 2 | ~2–3 days | After Phase 2 |
| 4 — Live ML Infra | Feature 3 | ~0.5 days | Immediately |
| 5 — Live ML: O/U | Feature 3 | ~2 days | After Phases 2 + 4 |
| 6 — Live ML: Win Predictor | Feature 3 | ~3 days | After Phases 2 + 4 + 5 |
| **Total** | | **~10–11 days** | |

Phases 1, 2, and 4 can all start immediately and run in parallel.
Phases 3 and 5 can run in parallel once Phase 2 is done.
Phase 6 is last — highest risk, most complex, builds on all prior phases.
