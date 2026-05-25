# Your First ML Models — A Complete Layman's Guide

This document explains everything you see in the Databricks MLflow screens in plain language.
No prior ML knowledge assumed.

---

## What Did We Actually Build?

Think of it like this: you've been watching 21 cricket matches and taking detailed notes at every over — the score, how many wickets fell, how fast the run rate is, what the bookmaker thinks the final score will be. Now you hand all those notes to a student and say:

> "Based on everything you've seen in past matches, when a new match reaches over 7 with a score of 65/2, what do you think the final total will be?"

That student is the **machine learning model**. It learns patterns from your historical notes (training data) and uses them to make predictions about new situations.

We built **two separate students**, each answering a different question:

| Model | Question it answers |
|---|---|
| **Score Predictor** | "Given the match state right now, what will the final 1st innings score be?" |
| **Over/Under Classifier** | "Will the final score beat the bookmaker's line?" |

---

## The Two Screens You're Looking At

### Screen 1 — Score Predictor results
### Screen 2 — Over/Under Classifier results
### Screen 3 — Experiment overview (both runs listed)

These screens are from **MLflow** — a tool built into Databricks that acts like a lab notebook. Every time you train a model it automatically records:
- What settings you used
- How accurate it was
- The trained model itself (saved for future use)

---

## What Is "Training"?

Training is the process of the algorithm looking at your historical data and finding patterns. Think of it like this:

1. You show the algorithm 4,000+ rows of data (one per snapshot in a match)
2. For each row it can see: score, wickets, run rate, bookmaker line, ball window, etc.
3. It also knows the *answer* for each row — what the final score actually was
4. It adjusts its internal settings thousands of times until its guesses match the real answers as closely as possible

That's it. Training is just a very fast, very systematic trial-and-error process.

---

## What Is "Group K-Fold Cross-Validation"?

This is the most important concept for understanding why our numbers look the way they do.

**The problem:** If you train and test on the same data, the model looks great — but it's cheating. It's like giving a student the exam answers during study time. The score looks perfect but it learned nothing useful.

**The solution — Cross-Validation:**

Imagine you have 21 matches. You split them into 5 groups (called "folds"):
- Group 1: matches 1–4
- Group 2: matches 5–8
- Group 3: matches 9–12
- Group 4: matches 13–16
- Group 5: matches 17–21

You run 5 rounds of training:
- Round 1: train on groups 2–5, test on group 1
- Round 2: train on groups 1, 3–5, test on group 2
- ... and so on

Each match only ever appears in the *test* set once — never in the test set when it was also in the training set. The final accuracy is the average across all 5 rounds.

**Why "Group" K-Fold?** Because all rows from the same match must stay together. If match 5 is in the test group, *all 200 snapshots from match 5* must be in the test set — you can't have some of match 5 in training and some in testing. That would be like telling the student the first half of an exam answer during study time.

**The important implication:** Cross-validation gives you a *realistic* accuracy number — what you'd expect on a brand new match the model has never seen. It's deliberately harder than testing on training data.

---

## Every Metric Explained

### Score Predictor Metrics

#### `cv_mae` = 22.66
**MAE = Mean Absolute Error**

This is the simplest metric. It says: on average, how many runs was our prediction off by?

**Our number: 22.66 runs**

In plain English: when the model predicts a final score of 160, the real answer is somewhere between 137 and 183 on average. For context, a typical T20 score ranges from 130–210 (an 80-run range). Being off by 23 runs is a large fraction of that range.

> Is 22 runs good or bad? **Honest answer: it's not good yet.** A random guess of "always predict 160" would probably get you around 20–25 MAE on T20 matches. Our model is barely beating random. But this is completely expected with 21 matches — see the "Why the numbers look like this" section below.

---

#### `cv_r2` = 0.0604
**R² = R-Squared (also called "coefficient of determination")**

This is the trickiest metric to explain. It answers: *how much of the variation in final scores does our model actually explain?*

- **R² = 1.0** → Perfect. The model explains everything. If you knew the features, you'd know the exact final score.
- **R² = 0.0** → The model is no better than just guessing the average score every time.
- **R² < 0.0** → The model is actually worse than guessing the average.

**Our number: 0.0604** → The model explains about **6% of the variation** in final scores.

Think of it this way: if the average T20 score is 160, and scores typically vary between 130–190, our model can only explain 6% of why some matches end at 145 and others at 185. The other 94% is noise (to the model at least — a cricket expert watching the match would know more).

> **Honest assessment:** 0.06 is very low. A good model would be 0.5+. But again, 21 matches is a tiny dataset. This number will grow as you collect more data.

---

### Over/Under Classifier Metrics

#### `cv_accuracy` = 0.5206
**Accuracy = what fraction of predictions were correct**

For our binary "will the score beat the bookmaker's line?" question, the model needs to say yes or no. Accuracy counts how often it said the right thing.

- **1.0** → Got every prediction right
- **0.5** → Correct half the time (same as flipping a coin)
- **0.0** → Wrong every time

**Our number: 52.06%**

The model is correct slightly more than half the time — barely better than a coin flip.

> **Is this bad?** Yes and no. In betting markets, the bookmaker is already very good. Even 53–55% accuracy consistently would be commercially valuable (this is why professional sports bettors exist). Getting to 52% with 21 matches isn't surprising — the signal is there but drowned in noise.

---

#### `cv_roc_auc` = 0.5294
**ROC-AUC = Area Under the Receiver Operating Characteristic Curve**

This sounds terrifying. Here's the simple version:

Accuracy measures "how often were you right?" but it has a flaw: if 80% of matches end OVER the line, a model that always says "OVER" gets 80% accuracy without learning anything useful.

ROC-AUC fixes this by asking: **"Is the model better at ranking — does it give higher confidence to the correct prediction?"**

- **1.0** → Perfect ranking. The model always gives higher probability to the outcome that actually happened.
- **0.5** → Random. The model's confidence scores are essentially noise.
- **0.0** → Perfectly wrong (worse than random — you'd just flip its predictions).

**Our number: 0.5294**

Nearly random. The model's confidence scores carry almost no information about which way the match will go.

> **The hard truth:** This is barely above 0.5. With 21 matches, the model hasn't seen enough variety to learn when the bookmaker is likely to be wrong. This is the metric to watch as data accumulates.

---

### Common to both models

#### `matches` = 21
How many unique cricket matches were in the training data. 21 is very small for ML — typically you want 100–200+ before results become meaningful.

#### `training_rows` = 4123 (score predictor), 3571 (O/U classifier)
Total number of snapshots used for training. Each match contributes ~150–200 rows (one per gold tracker snapshot). The O/U classifier has fewer rows because some snapshots were missing the bookmaker line.

---

## The Parameters Panel

These are the "settings" you gave XGBoost before training. Think of them like cooking instructions.

| Parameter | What it controls | Our value |
|---|---|---|
| `n_estimators` | How many "trees" to build (more = more complex model) | 300 |
| `max_depth` | How detailed each tree can get (deeper = more complex, risk of memorising) | 4 |
| `learning_rate` | How big a step to take on each adjustment (smaller = slower but safer) | 0.05 |
| `subsample` | What fraction of training rows to use per tree (prevents memorising) | 0.8 |
| `colsample_bytree` | What fraction of features to consider per tree | 0.8 |
| `min_child_weight` | Minimum data points needed to make a split (higher = simpler trees) | 3 |

These settings are deliberately conservative (shallow trees, slow learning rate) to prevent **overfitting** — the model memorising the 21 training matches instead of learning general patterns.

---

## "Model Registered" — What Does That Mean?

When you see **"cricket-score-predictor v1"** and **"cricket-ou-classifier v1"** — this means the trained models have been saved into the **MLflow Model Registry**.

Think of the Registry as a version-controlled storage room for models:

- Every time the pipeline retrains, it creates a new version (v2, v3, ...)
- You can always go back to v1 if a newer version is worse
- Other code (like the web app) can load the latest registered model by name, without needing to know where the file lives

The "v1" label means this is the first training run. Next Sunday when the pipeline runs again with more data, it will create v2.

---

## Why Do the Numbers Look Like This?

The honest answer: **21 matches is not enough data for ML.**

Here's a comparison to set expectations:

| Matches | What to expect |
|---|---|
| 21 (now) | Barely beating random — establishing a baseline |
| 50 | Starts to pick up clear signals, R² might reach 0.15–0.25 |
| 100 | Meaningful predictions, R² 0.3–0.5 possible |
| 200+ | Reliable enough to inform decisions, per-league breakdowns |

This is not a failure. You have a working pipeline that:
1. Automatically collects data every 5 seconds during live matches
2. Retrains every Sunday with whatever new matches completed that week
3. Versions each model so you can track improvement over time

The accuracy will improve automatically as matches accumulate. You don't need to do anything — the pipeline handles it.

---

## How the Pipeline Works End-to-End

```
Every 5 seconds (live match)
  Function App captures a snapshot → bronze blob

Daily at 02:00 CET (after match ends)
  ADF pl_build_ended_match:
    Silver: parse bronze snapshots into structured data
    Gold:   build innings_1_from_silver.json (the tracker file)

Every Sunday at 03:00 UTC
  ADF pl_ml_retrain:
    Activity 1 — Feature Extraction:
      Read all 21 (then 22, 23...) gold tracker files
      Flatten into one big table (features.parquet)
    Activity 2 — Model Training:
      Load features.parquet
      Train score predictor → log to MLflow → register as v2, v3...
      Train O/U classifier → log to MLflow → register as v2, v3...
      Write accuracy summary → gold/cricket/ml_features/t20/model_accuracy.json
```

No manual action needed. Every new match that completes automatically becomes training data for the next Sunday's retrain.

---

## How to Read the MLflow Experiment Page (Screen 3)

The experiment page at `/Users/dasgupta.sagarnil@gmail.com/cricket-t20-models` shows all runs:

- **Green circle** = run completed successfully
- **Run name** = what we called it (`score-predictor`, `ou-classifier`)
- **Duration** = how long training took (15–18 seconds each — very fast)
- **Models column** = which registered model was created from this run

Each row is one training run. Every Sunday a new row appears. Over time you'll be able to see the accuracy improving — MAE going down, R² going up, accuracy climbing from 52% toward something meaningful.

---

## What to Watch Over the Next Months

| Metric | Now | Target (100+ matches) |
|---|---|---|
| Score predictor MAE | 22.6 runs | < 12 runs |
| Score predictor R² | 0.06 | > 0.40 |
| O/U classifier accuracy | 52% | > 57% |
| O/U classifier ROC-AUC | 0.53 | > 0.60 |

When O/U accuracy reaches ~57–58% consistently, the model is identifying situations where the market is genuinely mispriced — which is commercially useful.

---

## Summary in One Paragraph

We built two ML models that learn from past T20 cricket matches to predict future outcomes. The score predictor tries to guess the final innings total from the current match state. The over/under classifier tries to predict whether the final score will beat the bookmaker's line. Both models were trained on 21 matches and evaluated using a rigorous method (Group K-Fold cross-validation) that tests on matches the model has never seen. The current accuracy is low — barely above random — because 21 matches is a small dataset. The models will automatically retrain every Sunday as more matches complete, and accuracy will improve over time without any manual effort.
