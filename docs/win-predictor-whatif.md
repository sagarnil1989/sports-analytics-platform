# Win Predictor — What-If / Sensitivity Analysis Feature Description

## Overview

When the win predictor model produces a result for a match — for example,
*"Defending team wins, 73% probability"* at innings-2 over-6 — this feature lets the
user expand a panel showing every feature value that drove that prediction. Each feature
is displayed in an editable input box. The user can change any single value (e.g. adjust
the innings-1 total from 180 to 160) and immediately see how the prediction probability
shifts, with everything else held constant. This is a **counterfactual / sensitivity
analysis** tool applied to the live match prediction.

---

## User Experience Flow

### Step 1 — Arrive at a match prediction

The existing `/api/ml/win-predictor` page shows test-set match predictions per
checkpoint. Each row is one match at one model checkpoint (e.g. "innings2-6over"),
with a predicted probability.

**Example row:**
> MI vs CSK — Innings 2 @ Over 6  
> Model: innings2-6over | Probability: **73% defend** | Actual: Defend ✓

### Step 2 — Expand the "What-If" panel

A **"What if?"** toggle/button on each prediction row opens a panel below it. The panel
shows the feature values that were fed to the model for this specific match × checkpoint,
grouped into readable sections. Importantly, only the features that survived pruning for
this checkpoint's model (i.e. non-zero importance) are shown — not all raw inputs.

### Step 3 — Edit a feature

Each feature value is shown in an editable number input (or dropdown for categoricals).
An edit icon or pencil indicator signals that the value is changeable. All other values
dim slightly to indicate they are being held constant.

**Example panel (innings2-6over checkpoint):**

```
┌─ 1st Innings Summary ────────────────────────────────────────────────────────┐
│  Total score            [ 180 ]   Wickets lost   [ 4  ]                      │
│  Powerplay runs/wkt     [  22 ]   Powerplay wkts [ 1  ]                      │
│  Middle-overs runs/wkt  [  18 ]   Death runs      [ 54 ]                     │
│  Overall pressure (R/wkt) [19.5 ]                                             │
├─ 2nd Innings State @ Over 6 ─────────────────────────────────────────────────┤
│  Runs scored             [ 10 ]   Wickets lost   [ 0  ]                      │
│  Current run rate        [1.67]   Required run rate [24.0]                   │
│  Runs needed             [171]    RR difference   [-22.3]                    │
│  Runs needed / wkt in hand  [ 17.1 ]                                         │
├─ Context ────────────────────────────────────────────────────────────────────┤
│  Venue    [ Wankhede ]   Bat team (inn1) [ MI ]   Bowl team (inn1) [ CSK ]  │
│  Women's match [ No ]    Weekend match   [ Yes ]                              │
└──────────────────────────────────────────────────────────────────────────────┘

Current prediction:  ████████████████░░░░  73% DEFEND   →  UPDATE →  [re-run]
```

### Step 4 — See updated prediction

When the user changes a value (e.g. sets "Total score" from 180 to 160) and clicks
**Re-run** (or after a short debounce), the panel sends the modified feature vector to
an inference endpoint and updates the probability bar and label in-place. A small diff
badge shows the change:

> **Before:** 73% defend → **After:** 61% defend `↓ −12pp`  
> Changed: `inn1_total_score` 180 → 160

A **Reset all** button restores every value to the original match values.

---

## Feature Panel Groups

The features are logically grouped in the panel. Groups and their editable fields, for
the innings2-6over checkpoint:

### Group 1: 1st Innings Summary
| Feature | Description | Type |
|---|---|---|
| `inn1_total_score` | Final innings-1 runs | Number |
| `inn1_total_wickets` | Final innings-1 wickets | Number (0–10) |
| `inn1_pp_runs` *(per-over)* | Runs in each of overs 1–6 | Number × 6 |
| `inn1_pp_wickets` | Wickets in powerplay | Number |
| `inn1_pp_rp_wkt` | Powerplay runs per wicket in hand | Computed |
| `inn1_mid_wickets_only` | Wickets in middle overs 7–15 | Number |
| `inn1_mid_rp_wkt` | Middle-overs runs per wicket in hand | Computed |
| `inn1_death_runs` | Runs in death overs 16–20 | Number |
| `inn1_death_wickets` | Wickets in death overs | Number |
| `inn1_pressure` | Overall inn1 runs per wicket in hand | Computed |
| `inn1_odds_swing_full` | Market movement across all 20 overs | Number |
| `inn1_odds_swing_pp` | Market movement across powerplay | Number |
| `inn1_bat_dominance` | Max SR − avg SR (one player carrying?) | Number |

*Computed fields are recalculated automatically from the raw editable inputs and are
shown read-only with a `=` badge.*

### Group 2: 2nd Innings State (at the checkpoint over)
| Feature | Description | Type |
|---|---|---|
| `inn2_ov6_score` | Chase runs scored after 6 overs | Number |
| `inn2_ov6_wickets` | Wickets down at over 6 | Number (0–10) |
| `inn2_ov6_crr` | Current run rate | Computed from score + balls |
| `inn2_ov6_rrr` | Required run rate | Computed from runs needed + balls left |
| `inn2_ov6_runs_needed` | Runs still needed | Computed |
| `inn2_ov6_rr_diff` | CRR − RRR | Computed |
| `inn2_ov6_rp_wkt` | Chase runs per wicket in hand | Computed |
| `inn2_ov6_chase_difficulty` | RRR × (wickets_fallen + 1) | Computed |
| `inn2_ov{n}_runs/wkts` *(per over)* | Runs and wickets for overs 1–6 | Number × 6 |

### Group 3: Context (Categorical)
| Feature | Description | Type |
|---|---|---|
| `venue` | Ground | Dropdown |
| `inn1_bat_team` | Team batting first | Dropdown |
| `inn1_bowl_team` | Team bowling first | Dropdown |
| `is_womens_match` | Women's match flag | Checkbox |
| `is_weekend_match` | Weekend match flag | Checkbox |

---

## Feature Importance Badges

Alongside each editable input, display a **feature importance bar** (the XGBoost
feature importance value from the trained model) so the user intuitively understands
which inputs matter most to the prediction. High-importance features have a prominent
bar; low-importance ones are collapsed by default (expandable via "Show minor features").

This directly answers the user's question — *"which factor is contributing to the
result?"* — without requiring any new model computation.

---

## Technical Implementation

### Challenge: Model is on DBFS, Not Azure Blob Storage

The trained models are XGBoost `.pkl` files saved to
`/dbfs/FileStore/cricket-pipeline/models/` — on Databricks DBFS, not in Azure Blob
Storage. The `cricket_display` Azure Functions have no runtime access to DBFS.

Two implementation paths:

---

**Path A — Store model in Blob Storage at training time (Recommended)**

During `MlWinPredictor` (ml_win_predictor.py), after training, serialise the pruned
XGBoost model (and its categorical encodings) to Azure Blob Storage:

```
gold/cricket/ml_features/t20/win_predictor_models/<checkpoint>.pkl
```

The `cricket_display` Function App downloads this blob on first request, caches it
in memory, and runs inference locally using `xgboost` + `scikit-learn` (already
available in the Python environment). A new route serves what-if predictions:

```
POST /api/ml/win-predictor/whatif
Body: { "checkpoint": "innings2-6over", "features": { ... } }
Response: { "prob_defend": 0.61, "prob_chase": 0.39 }
```

The frontend sends the edited feature dict, receives the updated probability, and
updates the UI. No Databricks compute needed at serve time.

**Pros:** Exact model inference, real-time response, no approximate.  
**Cons:** Need to add blob-upload step to `ml_win_predictor.py`, add `xgboost` to
`cricket_display`'s requirements.txt, and handle the cat-encoding lookup.

---

**Path B — SHAP-based linear approximation (Simpler, lower fidelity)**

At training time, compute SHAP values for every test-set prediction and store them in
`win_predictor_summary.json` alongside the per-match predictions. For a given
prediction, the SHAP local linear model approximates:

```
Δ probability ≈ Σ (SHAP_i / feature_stdev_i) × Δfeature_i
```

The Function App reads the stored SHAP values and applies this approximation entirely
client-side (or in a trivial Python computation) — no model object needed at serve time.

**Pros:** No model deployment needed, no new dependencies, extremely fast.  
**Cons:** Approximation only — may be inaccurate for large feature changes (e.g.
changing inn1 total by 40 runs), since SHAP is a local linear explanation around the
original prediction point, not a full re-run.

---

### Recommended Approach

**Path A for the primary what-if re-run; Path B as the importance badge display.**

The feature importance bars (Path B / SHAP values stored at training time) can be
displayed immediately for every row with no extra infrastructure. The "Re-run with
edited features" button (Path A) triggers a real inference call, giving an accurate
updated probability.

---

## Difficulty Assessment

| Component | Effort | Notes |
|---|---|---|
| Store model pkl to blob at training time | Low | ~10 lines in ml_win_predictor.py |
| Add `xgboost` to cricket_display requirements | Low | Version-pin to match training |
| `POST /api/ml/win-predictor/whatif` endpoint | Medium | Cat-encoding lookup + feature vector construction + inference |
| Feature panel UI (HTML + JS) | Medium | Editable inputs, debounce, probability bar update |
| Computed-field recalculation in JS | Medium | e.g. re-compute RRR = runs_needed × 6 / balls_left when score changes |
| Feature importance badges from summary JSON | Low | Already stored in win_predictor_summary.json |
| "Reset all" button | Low | JS only |
| Grouping + collapsing minor features | Low | CSS/JS, no backend change |

**Overall: Medium effort.** Path A requires a backend inference endpoint and model
deployment change; the UI is straightforward. Biggest risk is getting the categorical
encoding (venue/team target-mean encodings built from training data) available at
serve time — these need to be stored alongside the model pkl or in the summary JSON.

---

## What This Is NOT

- Not a real-time live-match predictor (the model only runs on finished, quiet matches).
- Not a causal attribution — changing `inn1_total_score` doesn't recompute per-over
  features (e.g. `inn1_death_runs`) automatically, unless we add JS recalculation
  logic for derived features. Users should be informed that computed features (marked
  with `=`) are recalculated but raw per-over values are held constant unless edited
  individually.
