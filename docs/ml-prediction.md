# ML Prediction — Design and Architecture

## Scope

**T20 only.** ODI and Test matches are excluded from all training and inference. T20 has a fixed 20-over innings which makes score prediction well-defined and the over/under market liquid. ODI and Test have fundamentally different dynamics and would dilute the model.

Filter applied at feature extraction: `total_overs == 20`.

---

## What We're Building

Three distinct prediction tasks:

| Task | Input | Output | Type |
|---|---|---|---|
| **Score predictor** | Match state at over N (score, wickets, run rate) | Predicted final score | Regression |
| **Win probability** | State at any point across both innings | P(team A wins) | Binary classification |
| **Over/under vs bookmaker** | Current state + Bet365's current line | P(actual score > line) | Binary classification |

The over/under task is the most interesting — Bet365 already publishes a line and we have it in bronze. The model learns *when the market is wrong* based on match state signals.

---

## Training Data — Already in the Pipeline

**Everything needed for the MVP is already in gold. No bronze access required.**

Each ended match has `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json` which contains:

```
{
  "match_name": "...",
  "home_team_name": "...",
  "away_team_name": "...",
  "actual_total": 172,           ← label: final score
  "outcome": "over",             ← label: over/under vs bookmaker line ("over"/"under"/"push")
  "rows": [                      ← one entry per over-change throughout the match
    {
      "over": "7.3",
      "score": 68,
      "wickets": 2,
      "batting_team": "...",
      "batting_team_odds": 1.85,     ← live win odds for batting team
      "bowling_team_odds": 1.95,
      "predicted_total": 155.5,      ← bookmaker line at that snapshot
      "over_odds_at_line": 1.91,     ← Bet365 over odds at that line
      "under_odds_at_line": 1.91,
      "ball_window": ["4","W","1","0","6","2"],
      "innings": 1
    },
    ...
  ]
}
```

The labels (`actual_total`, `outcome`) and all per-over features are already computed by `innings_tracker.py`. Feature extraction is just flattening `rows[]` across all ended matches into a training DataFrame.

---

## Pipeline Architecture

```
Existing gold                         New steps
─────────────────────────────────────────────────────────────────
innings_1_from_silver.json    →  [1] ml_feature_extraction notebook
  (T20 matches only)                       ↓
                                 gold/cricket/ml_features/t20/
                                           ↓
                               [2] ml_model_training notebook (MLflow)
                                           ↓        ← runs weekly via ADF trigger
                                 MLflow Model Registry
                                           ↓
                               [3] ml_inference notebook
                                           ↓        ← runs as Activity 4 in pl_build_ended_match
                                 gold/cricket/predictions/event_id={eid}/
```

### Step 1 — Feature Extraction (new Databricks notebook)
Reads all ended-match `innings_1_from_silver.json` files. Filters to `total_overs == 20`. Flattens `rows[]` into one row per over-change.

Derived features computed at extraction time:
- `over_num` (integer part of `over`)
- `balls_elapsed` = `over_num * 6 + balls_in_over`
- `balls_remaining` = `120 - balls_elapsed` (fixed at 120 for T20)
- `run_rate` = `score / max(over_num, 1)`
- `implied_prob_batting` = `1 / batting_team_odds` (if odds available)
- `market_implied_total` = `predicted_total` (already in each row)
- `score_vs_line` = `score - predicted_total` (how far ahead/behind the line)

Label columns: `actual_total` (regression target), `outcome` (over/under, classification target).

Writes flat Parquet to `gold/cricket/ml_features/t20/`.

### Step 2 — Model Training (new Databricks notebook)
XGBoost or LightGBM on the feature table. MLflow already ships with Databricks — no extra setup. Logs experiments, registers best model in MLflow Model Registry.

**Retraining schedule:** Weekly (Sunday 03:00 UTC) via a dedicated ADF trigger. Not daily — with the current data volume adding 1–2 matches per day does not meaningfully change the model. Revisit when match count exceeds 200.

**Validation:** Group K-Fold cross-validation grouped by `event_id`. Rows from the same match must never appear in both train and test sets.

### Step 3 — Inference
**Batch only** (no real-time endpoint needed at this stage).

An `ml_inference` notebook runs as Activity 4 in `pl_build_ended_match` after Activity 3 succeeds. For each live match, loads current match state from gold, scores with the registered MLflow model, writes prediction JSON to `gold/cricket/predictions/event_id={eid}/latest.json`.

Real-time API (Azure ML Managed Online Endpoint) deferred — only needed if a sub-second response product is built.

---

## Azure Components

### MVP — no new components needed
| Component | Role |
|---|---|
| Databricks MLflow | Experiment tracking + model registry (already in cluster) |
| ADF | Weekly retraining trigger + inference as Activity 4 in `pl_build_ended_match` |
| Blob storage | Feature tables (`gold/cricket/ml_features/t20/`) + prediction outputs |

### Cost
| Item | Cost |
|---|---|
| Weekly retraining run (~10 min, Standard_F4s_v2) | ~$0.05/run → **~$0.20/month** |
| Batch inference per match (~5 min) | ~$0.02/run → **~$0.60/month** at 30 matches/month |
| Blob storage for features + predictions | negligible |
| **Total** | **~$1/month** |

### If a live prediction API is needed later
- **Azure Machine Learning Managed Online Endpoint** — always-on REST endpoint, ~$50–100/month fixed. Only needed for sub-second response use cases.

---

## Feature Sources — Gold Only

All ML features come from gold. No bronze or silver access during training or inference.

### Already in gold (`rows[]` per over-change)
| Field | Description |
|---|---|
| `over`, `score`, `wickets` | Match state at that snapshot |
| `batting_team_odds`, `bowling_team_odds` | Live win odds |
| `predicted_total` | Bookmaker's over/under line |
| `over_odds_at_line`, `under_odds_at_line` | Bet365 odds at that line |
| `ball_window` | Last 6 legal deliveries |
| `innings` | 1 or 2 |
| `actual_total` | Final score — regression label |
| `outcome` | `"over"` / `"under"` / `"push"` — classification label |

### To be added to gold (silver extension done — gold build backfill pending)

Bet365 `TE`-type records in `api_live_market_stats.json` are now decoded in silver. Field meanings confirmed from match `11658818` (LSG vs PBKS T20, 2026-05-23):

#### EV record (match-level)
| Field | Meaning |
|---|---|
| `NA` | Match name |
| `CT` | Competition/league name |
| `SS` | Current score "runs/wickets" |
| `S3` | 2nd innings target (empty in 1st innings) |
| `S5` | Total overs (20 = T20) |
| `PG` | Ball window + over state (already parsed) |
| `EX` | Context text e.g. "Punjab Kings require 178 from 110 balls" |
| `VC` | Venue code (numeric ID, not name — name comes from `event_view.extra.stadium_data.name`) |

#### TE record — batting team (PI="1")
| Field | Meaning |
|---|---|
| `S1` | Striker batsman's runs |
| `S4` | Non-striker batsman's runs |
| `S5` | "runs#wickets" team score string |
| `S6` | "striker_name:runs:balls#non_striker_name:runs:balls" |
| `S7` | "striker_name###" (# markers = on strike indicator) |
| `PI` | "1" = this team is batting |

#### TE record — bowling/fielding team (PI="0")
| Field | Meaning |
|---|---|
| `S7` | "bowler_name#over_num#balls#runs#wickets" — current bowler |
| `S8` | "over_num#bowler_name#runs#wickets" — previous over summary |
| `PI` | "0" = this team is fielding |

Silver now decodes these into structured fields `batsmen[]`, `current_bowler{}`, `prev_over{}` on each `team_scores` row. These will be promoted to gold tracker `rows[]` in a future gold build extension.

---

## Build Order

1. **Inspect silver `player_entries.json`** — confirm S-field meanings from a real file
2. **Extend silver parsing** — decode player stats, write `player_stats.json`
3. **Extend gold build** — add batsmen/bowler/partnership to `rows[]`
4. **Feature extraction notebook** — flatten gold `rows[]` into training DataFrame (T20 only)
5. **Score predictor** — regression, simplest to validate
6. **Win probability** — uses both innings
7. **Over/under vs market** — most commercially interesting

---

## Training Data Volume

As of 2026-05-24: **33 ended matches** in gold.

| Threshold | Status |
|---|---|
| 33 matches (~1,300 rows) | **Current** — enough to validate the approach and see accuracy curves |
| 100+ matches | Recommended before trusting predictions in production |
| 200+ matches | Reliable per-league and per-format breakdowns |

**Critical:** rows from the same match must not appear in both train and test sets. Use **Group K-Fold cross-validation** grouped by `event_id`. With 33 groups, leave-one-match-out CV gives a realistic accuracy estimate without data leakage.

Keep collecting. The pipeline already captures all the data needed — accuracy improves automatically as more matches complete.
