# ML Model Selection — Win Predictor

**Last updated:** 2026-07-11  
**Author:** Ramanuj Analytics Platform

---

## The Core Problem

Predicting who wins a T20 cricket match at any point during the game. This is:

- A **binary classification** problem (`chasing_won = 1 or 0`)
- A **temporal / sequential** problem — the match evolves over 40 overs, and every ball changes the state
- A **small dataset** problem — ~170 matches as of mid-2026, growing weekly
- A **context-rich** problem — venue, teams, time of day, tournament stage all matter independently of the scoreline

The tension between these constraints is what drives every model choice below.

---

## Why XGBoost Was Chosen (Phase 1)

XGBoost was selected as the Phase 1 model for pragmatic reasons, not because it is the best fit conceptually.

| Constraint | Why XGBoost was chosen |
|---|---|
| ~170 matches | Deep learning needs 1,000+ rows; XGBoost works at 100 |
| Weekly retraining | Trains in seconds; LSTM takes minutes |
| Interpretable | Feature importance, SHAP, and decision tree visualisation work natively |
| Mixed feature types | Handles numeric + categorical without complex pipelines |
| Proven baseline | XGBoost dominates structured/tabular ML benchmarks |

The LSTM (Phase 2) has already been scaffolded in the notebook with a comment: *"activate at 500+ matches."*

---

## All Models Evaluated

### 1. XGBoost (current — Phase 1)

**What it does:** Trains an ensemble of ~100 decision trees on a flat row of features. Every over is just another column.

| ✓ Pros | ✗ Cons |
|---|---|
| Works well on 100–500 rows | Sees overs 1–20 as 20 separate columns, not a *sequence* |
| Interpretable via SHAP and tree plots | Can produce non-monotonic predictions (known issue) |
| Handles missing values natively | Cannot learn "momentum changed in over 14" |
| Fast to train and retrain weekly | Venue / team effects need many matches per category to emerge |

**Sequential blindness:** XGBoost cannot tell the difference between:
- "80/0 after 6 overs" (smooth start), and  
- "80/3 after 6 overs, then 0 for 4 overs" (wobbled, then recovered).  
Both produce the same flat-feature row if the over-6 totals are the same.

---

### 2. CatBoost

**What it does:** Same gradient-boosted tree ensemble as XGBoost but built specifically for categorical features using *ordered target statistics* instead of label encoding.

| ✓ Pros | ✗ Cons |
|---|---|
| Dramatically better venue and team encoding | Same sequential blindness as XGBoost |
| No manual label encoding needed | Slightly slower to train than XGBoost |
| Handles 50+ unique venues without cardinality explosion | Less community tooling than XGBoost |
| Often outperforms XGBoost on small datasets with many categories | — |

**Why this matters for cricket:** With label encoding (current), "Eden Gardens" becomes `int 7` and "Lord's" becomes `int 14`. CatBoost instead learns directly from how often teams chasing at Eden Gardens win — without needing enough matches per venue to stabilise a label code.

**Decision — run XGBoost and CatBoost in parallel, not as a replacement.** See section below.

---

### 3. LightGBM

**What it does:** Same family as XGBoost/CatBoost but optimised for speed on large datasets using histogram binning.

| ✓ Pros | ✗ Cons |
|---|---|
| Very fast, good on large datasets | No categorical advantage over XGBoost on small data |
| Already in use for the Over/Under predictor | Same sequential blindness |
| Low memory usage | Offers no unique benefit at 170 matches |

**Decision:** Keep for Over/Under. No reason to add to win predictor over CatBoost.

---

### 4. Random Forest (current — used as comparison)

**What it does:** Bagging ensemble of decision trees. Each tree sees a random subset of features and data.

| ✓ Pros | ✗ Cons |
|---|---|
| Very stable feature importances on small datasets | Lower accuracy than XGBoost on most benchmarks |
| Good cross-check when XGBoost disagrees | Same sequential blindness |
| Less prone to overfitting | — |

**Decision:** Keep as the third parallel model alongside XGBoost and CatBoost. Three models that disagree signals high uncertainty.

---

### 5. LSTM — Long Short-Term Memory (Active — small-data adapted)

**What it does:** Reads the match as an ordered time series. After processing over 6, it carries a memory of overs 1–5 into its decision for over 7.

| ✓ Pros | ✗ Cons |
|---|---|
| Naturally captures momentum and trajectory | Ideal threshold is 300–500 matches |
| "3 wickets in 2 overs" is a distinct sequence signal | Black box — harder to explain to stakeholders |
| Handles variable-length innings (rain, early all-out) | Requires careful per-over feature design |
| Directly addresses the "every moment matters" philosophy | Slow to train and tune |

**This is the right model conceptually.** A match at 80/3 after 6 overs following 0/0 after 4 overs is fundamentally different from 80/3 after 6 overs with wickets spread evenly. Only LSTM (and Transformer) can learn this.

**Current implementation (active):**
- Bidirectional LSTM, 16 units, Dropout 0.4, Early stopping patience=15
- Data augmentation: 2 noisy copies per training match to triple effective training set
- **With-odds variant** — 6D observation per over: `[inc_runs/20, inc_wkts/2, bat_odds/10, bowl_odds/10, crr/20, rrr/20]`. Market odds per over teach the LSTM how market confidence tracks match momentum.
- **No-odds variant** — stays at 4D: `[inc_runs/20, inc_wkts/2, total/200, pressure/20]` (no odds features)

**Decision:** Active in both notebooks. Expected to improve with more data.

---

### 6. Transformer / Attention Model (Phase 3, ≥ 1,000 matches)

**What it does:** Like LSTM but can directly attend to *any* pair of overs simultaneously — e.g. learns that the chase in overs 15–20 systematically mirrors what happened in overs 1–6.

| ✓ Pros | ✗ Cons |
|---|---|
| Best-in-class for sequence data globally | Needs 1,000+ matches |
| Attention weights show *which overs mattered most* | Complex to implement and tune |
| No "forgetting" problem that LSTMs have | Overkill at current data size |
| Powers the best sports prediction models in research | — |

**Decision:** Long-term target. Re-evaluate when dataset crosses 1,000 matches.

---

### 7. Bayesian / Probabilistic Model

**What it does:** Encodes prior cricket knowledge ("evening matches in India have 65% chase win rate historically") and updates it with in-match evidence using Bayes' theorem.

| ✓ Pros | ✗ Cons |
|---|---|
| Best for encoding dew factor, venue effects explicitly | Priors require manual research and encoding |
| Works well with small data by using domain knowledge priors | Less flexible than learned models |
| Uncertainty quantification built in | Hard to update with new match types automatically |

**Decision:** Use for the pre-match prior layer in the hybrid model (Phase 2). Not a standalone replacement.

---

### 8. Logistic Regression

**What it does:** Classic linear classification. The Duckworth-Lewis method is essentially this.

| ✓ Pros | ✗ Cons |
|---|---|
| Completely monotonic and interpretable | You do all the feature engineering manually |
| Works with 50 matches | Cannot discover unexpected interactions |
| Forces explicit encoding of cricket knowledge | Limited predictive power |

**Decision:** Good as a sanity-check baseline. Already implicitly covered by the feature engineering in the notebooks.

---

### 9. Survival Analysis (Cox / Kaplan-Meier)

**What it does:** Frames the chase as a survival problem — at each ball/over, what is the probability of the chasing team "surviving" to victory?

| ✓ Pros | ✗ Cons |
|---|---|
| Elegant for wicket-by-wicket modelling | Niche — limited ML tooling |
| Handles early all-out naturally | Not intuitive for cricket analysts |

**Decision:** Interesting research direction. Not worth the complexity at this stage.

---

## XGBoost vs CatBoost — Replace or Run in Parallel?

**Run in parallel.** Here is why:

We already run XGBoost + Random Forest in parallel. Adding CatBoost as a third algorithm costs almost nothing extra in training time and gives three concrete benefits:

1. **Model disagreement = uncertainty signal.** If XGBoost says Chase 65% but CatBoost says Chase 45%, the match is genuinely uncertain and neither prediction should be trusted blindly. Surfacing this disagreement is valuable.

2. **CatBoost learns venue/team effects better.** XGBoost will keep being better at numeric features (per-over runs, wickets, run rates). CatBoost will likely become better at venue and team encoding. Ensemble the two and you get both.

3. **No information loss.** Replacing XGBoost with CatBoost throws away XGBoost's proven numeric feature learning. Keeping both and averaging their probabilities is almost always strictly better than either alone.

**Practical change:** Add a `catboost_train_pruned` function identical in structure to `xgb_train_pruned`, run it for all 5 checkpoints, add the results to the summary JSON, and display them on the win predictor page alongside XGBoost and Random Forest.

---

## The Recommended Hybrid Architecture

```
Phase 2 onwards (300+ matches):

                ┌─────────────────────────────────────┐
                │         PRE-MATCH PRIOR LAYER        │
                │   (CatBoost or Logistic Regression)  │
                │                                     │
                │  Inputs known BEFORE first ball:    │
                │  • venue_region (Asia/UK/Aus/WI/SA) │
                │  • is_evening_match                 │
                │  • is_subcontinental                │
                │  • toss_winner                      │
                │  • team_a vs team_b h2h_win_rate    │
                │  • team_a_form_last5                │
                │  • team_b_form_last5                │
                │  • tournament_stage                 │
                │                                     │
                │  Output: prior_chase_prob           │
                └──────────────┬──────────────────────┘
                               │
                               ▼
                ┌─────────────────────────────────────┐
                │        IN-MATCH UPDATE LAYER         │
                │   XGBoost + CatBoost + Random Forest │
                │   (→ LSTM at 500+ matches)           │
                │                                     │
                │  Inputs known DURING the match:     │
                │  • prior_chase_prob (passed in)     │
                │  • per-over runs, wickets           │
                │  • run rates, required run rate     │
                │  • chase difficulty composites      │
                │                                     │
                │  Output: updated_chase_prob         │
                └─────────────────────────────────────┘
```

The pre-match layer handles everything that XGBoost currently under-weights (venue, teams, time of day, dew). The in-match layer handles the live scorecard.

---

## Missing Features That Must Be Added

These should be added to feature extraction regardless of model choice:

| Feature | Source | Why |
|---|---|---|
| `is_evening_match` | Match start time > 17:00 local | Dew factor in night matches |
| `venue_region` | Stadium → Asia / UK / Aus / WI / SA / Other | Encodes pitch and dew conditions |
| `is_subcontinental` | venue_region == "Asia" | Strongest single dew proxy |
| `toss_won_by_chaser` | BetsAPI event data | Fielding first in dew conditions is a huge edge |
| `team_form_last5` | Win rate in previous 5 matches | Recent momentum |
| `h2h_win_rate` | Historical head-to-head per team pair | Team A consistently beats Team B |
| `tournament_stage` | Group / Knockout / Final / Bilateral | Knockout matches behave differently |
| `is_neutral_venue` | Home/away from BetsAPI | Teams playing away from home |

### The Dew Factor Specifically

Evening matches in the Indian subcontinent (India, Pakistan, Sri Lanka, Bangladesh) consistently favour the chasing team due to dew settling on the outfield and ball in the second innings. The bowlers cannot grip the wet ball, making yorkers and cutters ineffective. This effect is not present in:
- Day matches (dew hasn't settled)
- Australian / UK / South African venues (climate)
- Covered or enclosed stadiums

The current model has no way to learn this because it treats "MA Chidambaram Stadium, Chennai" as an arbitrary integer label. Adding `is_subcontinental=1` and `is_evening_match=1` as explicit features would immediately give both XGBoost and CatBoost something to learn from.

---

## Phase Roadmap

| Phase | Matches | Models | New features |
|---|---|---|---|
| **1 — Now** | 170 | XGBoost + Random Forest | — |
| **1b — Next sprint** | 170 | + CatBoost in parallel | is_evening, venue_region, is_subcontinental, toss_winner |
| **2 — Short term** | 300+ | Pre-match prior layer added | team_form, h2h_win_rate, tournament_stage |
| **3 — Medium term** | 500+ | LSTM replaces XGBoost as primary in-match model | Full over-by-over sequence features |
| **4 — Long term** | 1000+ | Transformer end-to-end | Attention over ball-by-ball data |

---

## Key Takeaways

1. **XGBoost is the right Phase 1 model** — not the right long-term model.
2. **CatBoost should run alongside XGBoost** (not replace it) starting immediately.
3. **The dew factor, venue region, and time of day are genuinely important** and are currently missing from feature extraction entirely — not a model problem, a feature problem.
4. **LSTM is the philosophically correct model** for a problem where "every moment matters" — activate it at 300+ matches.
5. **A hybrid pre-match + in-match architecture** is the right long-term design. The pre-match layer handles what XGBoost can't (venue, teams, context), the in-match layer handles the live scorecard.
6. **Team identity matters less than you might think at small scale** — with 30+ teams and 170 matches, most pairs have met only once or twice. Team effects will become learnable around 500+ matches.
