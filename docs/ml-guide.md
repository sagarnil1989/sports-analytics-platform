# ML Models — Complete Guide

A plain-language explanation of every ML model in the platform, what the results mean,
and where to find them. No prior ML knowledge assumed.

---

## Models at a Glance

| Model group | Notebook | Question answered |
|---|---|---|
| **Score Predictor** | `ml_model_training` | What will the 1st innings final score be? |
| **Over/Under Classifier** | `ml_model_training` | Will the final score beat the bookmaker's line? |
| **Win Predictor × 3** | `ml_win_predictor` | Who wins the match? (three variants, see below) |

---

## Part 1 — Score Predictor and Over/Under Classifier

These two models are trained together every Sunday by `pl_ml_retrain` (Activity 2).
Results are in MLflow under the experiment **`/Users/you/cricket-t20-models`**.

### What they predict

**Score Predictor** — given the match state at any over (score, wickets, run rate,
bookmaker line, odds), predict what the 1st innings will finish at.

**Over/Under Classifier** — given the same state, predict whether the actual final
score will be higher than the bookmaker's published over/under line. The model is
trying to find moments where the market is wrong.

### Where to read results

| Location | How to get there |
|---|---|
| MLflow UI | Databricks → Experiments → `cricket-t20-models` |
| Gold blob | `gold/cricket/ml_features/t20/model_accuracy.json` |

### Every metric explained

#### `cv_mae` — Mean Absolute Error (Score Predictor only)

On average, how many runs was the prediction off by?

- **Our result: ~22 runs** with 21 matches
- A random "always guess 160" baseline gets around 20–25 MAE on T20
- Target: below 12 runs once 100+ matches are collected

The model is barely beating random with 21 matches — this is expected and normal.

#### `cv_r2` — R-Squared (Score Predictor only)

How much of the variation in final scores does the model explain?

- **1.0** = perfect, knows exactly what score will happen
- **0.0** = no better than always guessing the average
- **< 0.0** = worse than guessing the average
- **Our result: ~0.06** — explains 6% of variation
- Target: above 0.40 at 100+ matches

#### `cv_accuracy` — Accuracy (Over/Under Classifier)

What fraction of over/under predictions were correct?

- **Our result: ~52%** — barely above a coin flip (50%)
- In betting markets, consistent 57–58% would be commercially valuable
- Target: above 57% at 100+ matches

#### `cv_roc_auc` — ROC-AUC (Over/Under Classifier)

Does the model's confidence score correctly rank outcomes?
A score of 0.5 = random; 1.0 = perfect.

- **Our result: ~0.53** — nearly random
- Target: above 0.60 at 100+ matches

#### `matches` and `training_rows`

How many matches and how many over-snapshots were used.
Each match contributes ~150–200 rows (one per snapshot captured every 5 seconds).

### Why the numbers look low

21 matches is a tiny dataset for ML. Expected milestones:

| Matches | What to expect |
|---|---|
| 21 (first run) | Barely beating random — establishing a baseline |
| 50 | Starts picking up real signals |
| 100 | Meaningful predictions, R² 0.3–0.5 |
| 200+ | Reliable enough to act on |

The pipeline retrains every Sunday automatically. Numbers will improve on their own.

---

## Part 2 — Win Predictor (three models)

Trained by `ml_win_predictor` notebook, also in `pl_ml_retrain` (Activity 3, runs
in parallel with Activity 2). Results visible in three places — see below.

### What it predicts

Binary: **did the chasing team (innings 2) win?** (1 = yes, 0 = no)

Three separate models are trained, each with more match information available:

| Model name | What it knows at prediction time |
|---|---|
| `innings1-only` | Full innings 1 complete — chase has not started yet |
| `innings2-2over` | Innings 1 + first 2 overs of the chase |
| `innings2-6over` | Innings 1 + full powerplay of the chase (6 overs) |

The idea: accuracy should increase as you move from `innings1-only` → `innings2-6over`
because you have more information. If it does not, the early overs are carrying real
predictive signal.

### Train / test split

Unlike the score predictor (which uses cross-validation), the win predictor uses a
hard date split:

- **Train**: matches before 2026-05-23
- **Test**: matches from 2026-05-23 onwards

This means the test set is genuinely unseen future matches — a stricter test than
cross-validation.

### Authoritative final score source

The win predictor reads final scores from the tracker's `score_summary_events` /
`score_summary_bet365` / `score_summary` field — the same source the ended/view
page uses. This comes from the Bet365 events API and reflects the actual final score,
not the last snapshot captured by the pipeline.

If that field is missing, it falls back to the last snapshot row. This means:
- A match that finished 203 will always be recorded as 203, even if the last
  snapshot fired at 201
- The win/loss label is derived from the same authoritative scores, not snapshot rows

If a match shows `inn1_total_score = 0`, the gold tracker for that match has
incomplete data and `pl_build_ended_match` should be re-run for that event ID.

### Features used

Every model uses:

**Team and venue**
- Which team batted in innings 1 (categorical)
- Which team bowled in innings 1 (categorical)
- Venue / stadium (categorical)

**Innings 1 — per over, for all 20 overs**
- Runs scored in that specific over
- Wickets lost in that specific over
- Batting team's betting odds at the end of that over
- Bowling team's betting odds at the end of that over

**Innings 1 — composite interaction features**
These pre-bake the relationships between score and wickets that the model
would otherwise have to discover on its own:

| Feature | What it captures |
|---|---|
| `inn1_pp_rp_wkt` | Powerplay runs per wicket in hand — 80/0 scores 8.0, 65/3 scores 9.3 |
| `inn1_pressure` | Overall innings quality: total runs per wicket in hand |
| `inn1_death_runs` | Runs scored in overs 16–20 |
| `inn1_odds_swing_full` | How far the market moved across all 20 overs of innings 1 |
| `inn1_odds_swing_pp` | Market movement across just the powerplay |

**Innings 2 additions** (`innings2-2over` and `innings2-6over` only)
- Per-over runs, wickets, batting odds, bowling odds through the model's horizon
- `inn2_ov{N}_rp_wkt` — chase runs per wicket in hand at over N
- `inn2_ov{N}_odds_swing` — how far the market shifted from end of innings 1 to over N
- `inn2_ov{N}_chase_difficulty` — `RRR × (wickets_fallen + 1)`. Encodes the joint severity of a chase: 51/5 chasing 254 at over 6 gives ~87 (near-impossible); 120/2 chasing 180 at over 10 gives ~9 (comfortable). Avoids over-relying on collinear `rrr`, `crr`, `rr_diff`, `runs_needed` individually.
- Cumulative chase state: CRR, RRR, run-rate differential, runs needed

### Why odds at every over matter

The betting odds at each over boundary encode everything the market knows at that
moment — pitch conditions, bowler form, batsman matchups, team history. A single
odds number replaces dozens of variables you would otherwise need to collect separately.

If the market moves from 1.9 → 1.3 across overs 15–18, something meaningful happened
in those overs that the raw score may not fully show. The model can learn that pattern.

### Algorithms

Two algorithms are trained and compared on every run:

| Algorithm | Why it's here |
|---|---|
| **XGBoost** | Handles correlated and interacting features via tree splits. Primary model. |
| **Random Forest** | More stable feature importances on small datasets. Used for cross-checking which features XGBoost ranked highly. |

**Feature pruning**: XGBoost trains once on all features, drops anything below 0.5%
of total importance (features it never used in any tree split), then retrains on the
reduced set. This keeps the model clean and prevents noise from irrelevant columns.

### Where to read results

**1. Website** (easiest, no login needed)

`https://func-ramanuj-display.azurewebsites.net/api/ml/win-predictor`

Shows: model comparison table, feature importance bars, LSTM status.
Also linked from the Home page.

**2. Databricks MLflow**

Experiments → `cricket-win-predictor` → 6 runs (XGBoost and Random Forest for each
of the 3 models).

Each run has:
- **Metrics tab**: accuracy, ROC-AUC, training/test match counts
- **Params tab**: all algorithm settings
- **Artifacts tab** → `feature_importance/` → CSV with every feature ranked

**3. Gold blob**

`gold/cricket/ml_features/t20/win_predictor_summary.json`

Contains accuracy, ROC-AUC, and full feature importance list for all models.
Download from Azure Storage Explorer or the portal.

### How to read the results

#### Test accuracy

What fraction of test-set matches the model predicted correctly.

- **0.50** = coin flip, no signal
- **0.60** = some signal, meaningful with enough test matches
- **0.70+** = strong signal

With a small test set (5–10 matches), even a perfect model will look noisy.
Watch the trend across weekly retrains, not the absolute number on any single run.

#### ROC-AUC

Does the model assign higher confidence to the predictions that turned out correct?

- **0.50** = random — confidence scores carry no information
- **0.60** = useful — model knows when it's more sure
- **0.70+** = strong — can be used to filter predictions by confidence

#### Feature importance table

Ranked list of which features the model relied on most. Shown as percentage of
total importance so they add up to 100%.

Colour coding on the website:
- **Blue** — odds features (market signals)
- **Purple** — composite interaction features (runs-per-wicket-in-hand etc.)
- **Green** — categorical (team names, venue)
- **Grey** — raw runs and wickets per over

If odds features dominate the top of the list, it means the market is the single
strongest predictor — which makes intuitive sense. If composite features rank highly,
the model is learning something beyond what the odds already know.

#### Comparison between the three models

The key question: does accuracy increase from `innings1-only` → `innings2-2over`
→ `innings2-6over`?

- **If yes** — the chase overs carry genuine predictive signal beyond what innings 1 alone tells you
- **If no** — innings 1 (specifically the final odds after innings 1 ends) already captures most of the story, and the chase overs add noise rather than signal

Neither outcome is wrong — both are informative.

### LSTM — future model

When training matches reach 500+, an LSTM neural network can be added. Unlike
XGBoost, an LSTM processes the innings as a time series — it reads over 1, then
over 2 building on what it saw in over 1, and so on. This allows it to learn that
a team that was 30/3 at over 3 but recovered to 80/3 by over 6 is in a different
position than a team that was 80/0 all along.

The full implementation is already written in `ml_win_predictor.py` (Step 12),
commented out. The website shows whether the 500-match threshold has been reached.

---

## Part 3 — Cross-validation vs date split

The score predictor and O/U classifier use **Group K-Fold cross-validation**.
The win predictor uses a **hard date split**.

| | Cross-validation | Date split |
|---|---|---|
| Used by | Score predictor, O/U classifier | Win predictor |
| How it works | Rotates which matches are test set across 5 rounds | Everything before a date = train, after = test |
| Strength | Uses all data efficiently — good with small datasets | Tests on genuinely future matches — most realistic |
| Weakness | Can be optimistic if there are time-based trends in the data | Wastes training data if the split date is early |

---

## Summary — what to watch each week

After each Sunday retrain, check the website at `/api/ml/win-predictor` and
MLflow at `cricket-t20-models`. The numbers to track:

| Metric | Now (baseline) | Target (100+ matches) |
|---|---|---|
| Score predictor MAE | ~22 runs | < 12 runs |
| Score predictor R² | ~0.06 | > 0.40 |
| O/U classifier accuracy | ~52% | > 57% |
| O/U classifier ROC-AUC | ~0.53 | > 0.60 |
| Win predictor accuracy | run notebook to see | > 65% |
| Win predictor ROC-AUC | run notebook to see | > 0.65 |
