# Databricks notebook: inn1 score predictor
#
# ── What this notebook does ──────────────────────────────────────────────────
# Predicts the FINAL innings-1 score (a regression target) using only
# information available at three different points in the innings:
#
#   at-over-6   — powerplay complete (overs 1–6 known)
#   at-over-10  — halfway point (overs 1–10 known)
#   at-over-16  — death overs approaching (overs 1–16 known)
#
# ── Why regression, not classification ──────────────────────────────────────
# The win predictor classifies who wins. This notebook predicts a number —
# the final runs scored by the batting side in innings 1. That number is
# then used to set expectations for the chase.
#
# ── Algorithm strategy ───────────────────────────────────────────────────────
# XGBoost Regressor  — primary (handles non-linear feature interactions)
# Random Forest Regressor — comparison (more stable importance on small sets)
# Feature pruning: same two-pass approach as win predictor
#
# ── Train/test split ─────────────────────────────────────────────────────────
# Train : match_date_utc < today − 5 days
# Test  : match_date_utc >= today − 5 days

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "pyarrow", "pandas",
    "xgboost", "scikit-learn",
], check=True)

# COMMAND ----------

import json, io, tempfile, os
import numpy as np
import pandas as pd
import mlflow
import mlflow.xgboost
import mlflow.sklearn
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc    = BlobServiceClient.from_connection_string(conn_str)
gold   = svc.get_container_client("gold")

from datetime import datetime, timedelta, timezone
TRAIN_CUTOFF         = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
IMPORTANCE_THRESHOLD = 0.005

def _dl(path):
    try:
        return json.loads(gold.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 1 — Load all T20 gold trackers
# ═══════════════════════════════════════════════════════════════════

blobs = [b.name for b in gold.list_blobs(name_starts_with="event_id=")
         if b.name.endswith("/innings_tracker.json")]

def _dl_tracker(path):
    t = _dl(path)
    if t is not None and not t.get("event_id"):
        ep = path.split("/")[0]  # "event_id=XXXXX"
        if ep.startswith("event_id="):
            t["event_id"] = ep.replace("event_id=", "")
    return t

trackers = []
with ThreadPoolExecutor(max_workers=32) as ex:
    futs = {ex.submit(_dl_tracker, b): b for b in blobs}
    for fut in as_completed(futs):
        t = fut.result()
        if t:
            trackers.append(t)

def _max_over(rows):
    mx = 0
    for r in rows:
        try:
            mx = max(mx, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    return mx

t20_trackers = [t for t in trackers if 15 <= _max_over(t.get("rows") or []) <= 20]

print(f"Total trackers loaded : {len(trackers)}")
print(f"T20 trackers          : {len(t20_trackers)}")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 2 — Feature extraction helpers
# ═══════════════════════════════════════════════════════════════════

def _parse_over(r):
    try:
        s = str(r.get("over") or "0")
        parts = s.split(".")
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    except Exception:
        return 0, 0

def state_after_n_overs(inn_rows, n):
    exact, prev_ov, any_ov = [], [], []
    for r in inn_rows:
        ov_int, ov_ball = _parse_over(r)
        if ov_int == n and ov_ball == 0:
            exact.append(r)
        if ov_int == n - 1:
            prev_ov.append(r)
        if ov_int <= n:
            any_ov.append(r)
    r = (exact[0] if exact
         else prev_ov[-1] if prev_ov
         else any_ov[-1]  if any_ov
         else None)
    if r is None:
        return None
    return {
        "score":     r.get("score")   or 0,
        "wickets":   r.get("wickets") or 0,
        "bat_odds":  r.get("batting_team_odds"),
        "bowl_odds": r.get("bowling_team_odds"),
    }

def per_over_breakdown(inn_rows, innings_num, from_over, to_over):
    result = {}
    prev_score = prev_wickets = 0
    # seed prev values from the over before from_over
    if from_over > 1:
        s0 = state_after_n_overs(inn_rows, from_over - 1)
        if s0:
            prev_score   = s0["score"]
            prev_wickets = s0["wickets"]
    for k in range(from_over, to_over + 1):
        s = state_after_n_overs(inn_rows, k)
        if s is not None:
            result[f"inn{innings_num}_ov{k}_runs"]      = max(0, s["score"]   - prev_score)
            result[f"inn{innings_num}_ov{k}_wkts"]      = max(0, s["wickets"] - prev_wickets)
            result[f"inn{innings_num}_ov{k}_bat_odds"]  = s["bat_odds"]
            result[f"inn{innings_num}_ov{k}_bowl_odds"] = s["bowl_odds"]
            prev_score   = s["score"]
            prev_wickets = s["wickets"]
        else:
            for suffix in ["_runs", "_wkts", "_bat_odds", "_bowl_odds"]:
                result[f"inn{innings_num}_ov{k}{suffix}"] = None
    return result

def parse_score_summary(tracker, inn1_bat_team):
    raw = (tracker.get("score_summary_events")
           or tracker.get("score_summary_bet365")
           or tracker.get("score_summary") or "")
    raw = raw.replace("-", ",").strip()
    if not raw or "," not in raw:
        return None, None
    parts = raw.split(",", 1)
    home = str(tracker.get("home_team_name") or "").strip()
    away = str(tracker.get("away_team_name") or "").strip()
    if inn1_bat_team and away and inn1_bat_team == away:
        parts = [parts[1].strip(), parts[0].strip()]
    try:
        return int(parts[0].split("/")[0].strip()), int(parts[1].split("/")[0].strip())
    except Exception:
        return None, None

def safe_div(a, b, default=0.0):
    try:
        return round(a / b, 4) if b and b != 0 else default
    except Exception:
        return default

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 3 — Build one row per match
#
# Label   : inn1_total_score (final authoritative runs)
# Features: everything known up to over 16 max
#           — each model will subset to its own cutoff columns
# ═══════════════════════════════════════════════════════════════════

records = []
skipped = 0

for t in t20_trackers:
    rows      = t.get("rows") or []
    inn1_rows = [r for r in rows if r.get("innings") == 1]
    if not inn1_rows:
        skipped += 1; continue

    inn1_bat_team = ""
    for r in inn1_rows:
        if r.get("batting_team"):
            inn1_bat_team = str(r["batting_team"]).strip(); break

    auth_inn1, _ = parse_score_summary(t, inn1_bat_team)
    inn1_final   = inn1_rows[-1]
    inn1_total_score   = auth_inn1 if auth_inn1 is not None else (inn1_final.get("score") or 0)
    inn1_total_wickets = inn1_final.get("wickets") or 0

    if inn1_total_score == 0:
        skipped += 1; continue

    home = str(t.get("home_team_name") or "").strip()
    away = str(t.get("away_team_name") or "").strip()
    inn1_bowl_team = away if inn1_bat_team == home else home

    _sd   = t.get("stadium_data") or {}
    _sd_name = _sd.get("name", "")
    _sd_city = _sd.get("city", "")
    if _sd_name:
        venue = f"{_sd_name}, {_sd_city}".strip(", ")
    else:
        venue = str(t.get("venue") or t.get("stadium") or t.get("league_name") or "unknown").strip() or "unknown"

    date_str = str(t.get("match_date_utc") or "")[:10]
    split    = "train" if date_str < TRAIN_CUTOFF else "test"

    # State snapshots at each model cutoff
    s6  = state_after_n_overs(inn1_rows, 6)
    s10 = state_after_n_overs(inn1_rows, 10)
    s16 = state_after_n_overs(inn1_rows, 16)
    s1  = state_after_n_overs(inn1_rows, 1)

    # Per-over breakdowns — one batch covering all overs we care about
    overs_1_6  = per_over_breakdown(inn1_rows, 1, 1, 6)
    overs_7_10 = per_over_breakdown(inn1_rows, 1, 7, 10)
    overs_11_16= per_over_breakdown(inn1_rows, 1, 11, 16)

    rec = {
        "event_id":    str(t.get("event_id") or ""),
        "match_name":  str(t.get("match_name") or ""),
        "match_date":  date_str,
        "split":       split,
        "venue":       venue,
        "inn1_bat_team":  inn1_bat_team  or "unknown",
        "inn1_bowl_team": inn1_bowl_team or "unknown",
        # cumulative state at each cutoff
        "inn1_score_at_6":   s6["score"]   if s6  else None,
        "inn1_wkts_at_6":    s6["wickets"] if s6  else None,
        "inn1_score_at_10":  s10["score"]  if s10 else None,
        "inn1_wkts_at_10":   s10["wickets"]if s10 else None,
        "inn1_score_at_16":  s16["score"]  if s16 else None,
        "inn1_wkts_at_16":   s16["wickets"]if s16 else None,
        # odds at key points
        "inn1_ov1_bat_odds": s1["bat_odds"]  if s1  else None,
        "inn1_ov6_bat_odds": s6["bat_odds"]  if s6  else None,
        "inn1_ov10_bat_odds":s10["bat_odds"] if s10 else None,
        "inn1_ov16_bat_odds":s16["bat_odds"] if s16 else None,
        # label
        "inn1_total_score":   inn1_total_score,
        "inn1_total_wickets": inn1_total_wickets,
    }
    rec.update(overs_1_6)
    rec.update(overs_7_10)
    rec.update(overs_11_16)
    records.append(rec)

df = pd.DataFrame(records)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 4 — Composite features
# ═══════════════════════════════════════════════════════════════════

df["inn1_pp_rp_wkt"] = df.apply(
    lambda r: safe_div(r["inn1_score_at_6"] or 0, 10 - (r["inn1_wkts_at_6"] or 0)), axis=1)

df["inn1_mid_rp_wkt"] = df.apply(
    lambda r: safe_div(
        (r["inn1_score_at_10"] or 0) - (r["inn1_score_at_6"] or 0),
        10 - (r["inn1_wkts_at_10"] or 0)
    ), axis=1)

df["inn1_mid_wkts_only"] = df.apply(
    lambda r: max(0, (r["inn1_wkts_at_10"] or 0) - (r["inn1_wkts_at_6"] or 0)), axis=1)

df["inn1_death_start_rp_wkt"] = df.apply(
    lambda r: safe_div(
        (r["inn1_score_at_16"] or 0) - (r["inn1_score_at_10"] or 0),
        10 - (r["inn1_wkts_at_16"] or 0)
    ), axis=1)

df["inn1_death_start_wkts_only"] = df.apply(
    lambda r: max(0, (r["inn1_wkts_at_16"] or 0) - (r["inn1_wkts_at_10"] or 0)), axis=1)

df["inn1_pp_odds_swing"] = df.apply(
    lambda r: safe_div(
        (r["inn1_ov6_bat_odds"] or 0) - (r["inn1_ov1_bat_odds"] or 0), 1, default=None
    ), axis=1)

df["inn1_mid_odds_swing"] = df.apply(
    lambda r: safe_div(
        (r["inn1_ov10_bat_odds"] or 0) - (r["inn1_ov6_bat_odds"] or 0), 1, default=None
    ), axis=1)

df["inn1_death_start_odds_swing"] = df.apply(
    lambda r: safe_div(
        (r["inn1_ov16_bat_odds"] or 0) - (r["inn1_ov10_bat_odds"] or 0), 1, default=None
    ), axis=1)

for col in ["venue", "inn1_bat_team", "inn1_bowl_team"]:
    df[col] = df[col].astype("category")

train_df = df[df["split"] == "train"].reset_index(drop=True)
test_df  = df[df["split"] == "test"].reset_index(drop=True)

print(f"\n{'='*60}")
print(f"  Total T20 matches      : {len(df)}")
print(f"  Skipped (incomplete)   : {skipped}")
print(f"  Train (before {TRAIN_CUTOFF}) : {len(train_df)}")
print(f"  Test  (from   {TRAIN_CUTOFF}) : {len(test_df)}")
print(f"{'='*60}\n")

display(df[["match_name", "match_date", "split",
            "inn1_bat_team", "inn1_score_at_6", "inn1_wkts_at_6",
            "inn1_score_at_10", "inn1_score_at_16",
            "inn1_pp_rp_wkt", "inn1_total_score"]])

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 5 — Feature sets per model cutoff
# ═══════════════════════════════════════════════════════════════════

CAT = ["venue", "inn1_bat_team", "inn1_bowl_team"]

# Model 1: at over 6 — powerplay only
AT6_FEATURES = (
    CAT +
    ["inn1_score_at_6", "inn1_wkts_at_6",
     "inn1_pp_rp_wkt", "inn1_ov1_bat_odds", "inn1_ov6_bat_odds", "inn1_pp_odds_swing"] +
    [f"inn1_ov{n}_runs"      for n in range(1, 7)] +
    [f"inn1_ov{n}_wkts"      for n in range(1, 7)] +
    [f"inn1_ov{n}_bat_odds"  for n in range(1, 7)] +
    [f"inn1_ov{n}_bowl_odds" for n in range(1, 7)]
)

# Model 2: at over 10 — halfway
AT10_FEATURES = (
    AT6_FEATURES +
    ["inn1_score_at_10", "inn1_wkts_at_10",
     "inn1_mid_rp_wkt", "inn1_mid_wkts_only", "inn1_ov10_bat_odds", "inn1_mid_odds_swing"] +
    [f"inn1_ov{n}_runs"      for n in range(7, 11)] +
    [f"inn1_ov{n}_wkts"      for n in range(7, 11)] +
    [f"inn1_ov{n}_bat_odds"  for n in range(7, 11)] +
    [f"inn1_ov{n}_bowl_odds" for n in range(7, 11)]
)

# Model 3: at over 16 — death overs approaching
AT16_FEATURES = (
    AT10_FEATURES +
    ["inn1_score_at_16", "inn1_wkts_at_16",
     "inn1_death_start_rp_wkt", "inn1_death_start_wkts_only",
     "inn1_ov16_bat_odds", "inn1_death_start_odds_swing"] +
    [f"inn1_ov{n}_runs"      for n in range(11, 17)] +
    [f"inn1_ov{n}_wkts"      for n in range(11, 17)] +
    [f"inn1_ov{n}_bat_odds"  for n in range(11, 17)] +
    [f"inn1_ov{n}_bowl_odds" for n in range(11, 17)]
)

print(f"at-over-6  : {len(AT6_FEATURES)} features")
print(f"at-over-10 : {len(AT10_FEATURES)} features")
print(f"at-over-16 : {len(AT16_FEATURES)} features")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 6 — Shared helpers
# ═══════════════════════════════════════════════════════════════════

def make_fi_df(importances, feature_cols):
    fi = pd.Series(importances, index=feature_cols).sort_values(ascending=False)
    total = fi.sum()
    return pd.DataFrame({
        "rank":         range(1, len(fi) + 1),
        "feature":      fi.index,
        "importance":   fi.values.round(5),
        "pct_of_total": (fi.values / total * 100).round(2) if total > 0 else [0.0] * len(fi),
    })

def regression_metrics(y_true, y_pred):
    mae  = round(float(mean_absolute_error(y_true, y_pred)), 2)
    rmse = round(float(mean_squared_error(y_true, y_pred) ** 0.5), 2)
    r2   = round(float(r2_score(y_true, y_pred)), 3) if len(y_true) > 1 else None
    return mae, rmse, r2

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 7 — XGBoost Regressor: train → prune → retrain
# ═══════════════════════════════════════════════════════════════════

XGB_PARAMS = {
    "n_estimators":      200,
    "max_depth":         3,
    "learning_rate":     0.05,
    "subsample":         0.8,
    "colsample_bytree":  0.6,
    "min_child_weight":  2,
    "random_state":      42,
    "n_jobs":            -1,
    "objective":         "reg:squarederror",
    # enable_categorical omitted — we label-encode categoricals in prepare_X
}

def _label_encode_cats(df_in, feature_cols, cat_encoders=None):
    """Label-encode category/object columns; median-fill numerics. Returns float DataFrame."""
    X = df_in[feature_cols].copy()
    encoders = cat_encoders or {}
    cat_cols = [c for c in feature_cols
                if pd.api.types.is_categorical_dtype(df_in[c]) or df_in[c].dtype == object]
    num_cols = [c for c in feature_cols if c not in cat_cols]

    for col in cat_cols:
        if col not in encoders:
            _, uniq = pd.factorize(X[col])
            encoders[col] = {v: i for i, v in enumerate(uniq)}
        X[col] = X[col].map(encoders[col]).fillna(-1).astype(int)

    num_medians = {}
    for col in num_cols:
        med = (cat_encoders or {}).get(f"_med_{col}", X[col].median())
        X[col] = X[col].fillna(med)
        num_medians[f"_med_{col}"] = med

    encoders.update(num_medians)
    return X.astype(float), encoders

def xgb_train_pruned(model_name, feature_cols, train_df, test_df):
    print(f"\n{'─'*60}")
    print(f"  XGBoost Regressor — {model_name}  ({len(feature_cols)} features)")

    if len(train_df) < 5:
        print(f"  SKIP: only {len(train_df)} training matches")
        return None, None, None, None, feature_cols, []

    X_train, enc = _label_encode_cats(train_df, feature_cols)
    y_train = train_df["inn1_total_score"]
    X_test,  _  = _label_encode_cats(test_df,  feature_cols, enc)
    y_test  = test_df["inn1_total_score"]

    # Pass 1 — full feature set
    m1 = xgb.XGBRegressor(**XGB_PARAMS)
    m1.fit(X_train.to_numpy(), y_train.to_numpy())
    fi1   = pd.Series(m1.feature_importances_, index=feature_cols)
    total1 = fi1.sum()
    pct1   = fi1 / total1 if total1 > 0 else fi1

    dropped = pct1[pct1 < IMPORTANCE_THRESHOLD].index.tolist()
    kept    = pct1[pct1 >= IMPORTANCE_THRESHOLD].index.tolist()
    print(f"  Pass 1: {len(dropped)} features dropped  |  {len(kept)} kept → retraining")
    if not kept:
        kept = feature_cols

    # Pass 2 — pruned feature set
    X_train2, enc2 = _label_encode_cats(train_df, kept)
    X_test2,  _    = _label_encode_cats(test_df,  kept, enc2)

    m2 = xgb.XGBRegressor(**XGB_PARAMS)
    m2.fit(X_train2.to_numpy(), y_train.to_numpy())

    y_pred = m2.predict(X_test2.to_numpy())
    mae, rmse, r2 = regression_metrics(y_test, y_pred)
    fi_df = make_fi_df(m2.feature_importances_, kept)

    print(f"  MAE={mae}  RMSE={rmse}  R²={r2}")
    print(f"\n  Top 10 features:")
    for _, row in fi_df.head(10).iterrows():
        print(f"    {int(row['rank']):<4} {row['feature']:<42} {row['pct_of_total']:>6.2f}%")
    display(fi_df)

    # Per-match prediction table — include score/wickets at each cutoff state
    state_cols = [c for c in [
        "inn1_score_at_6",  "inn1_wkts_at_6",
        "inn1_score_at_10", "inn1_wkts_at_10",
        "inn1_score_at_16", "inn1_wkts_at_16",
    ] if c in test_df.columns]
    preds = test_df[["match_name", "match_date", "inn1_bat_team",
                      "inn1_bowl_team", "inn1_total_score"] + state_cols].copy()
    preds["predicted_score"] = y_pred.round(1)
    preds["error"]           = (y_pred - y_test.values).round(1)
    preds["abs_error"]       = preds["error"].abs()
    display(preds)

    preds_records = preds.rename(columns={"inn1_total_score": "actual_score"}).to_dict("records")
    return m2, mae, rmse, r2, fi_df, kept, preds_records

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 8 — Random Forest Regressor
# ═══════════════════════════════════════════════════════════════════

RF_PARAMS = {
    "n_estimators":    300,
    "max_depth":       4,
    "min_samples_leaf":2,
    "random_state":    42,
    "n_jobs":          -1,
}

def rf_train(model_name, feature_cols, train_df, test_df):
    print(f"\n{'─'*60}")
    print(f"  Random Forest Regressor — {model_name}  ({len(feature_cols)} features)")

    if len(train_df) < 5:
        return None, None, None, None, None

    X_train, enc = _label_encode_cats(train_df, feature_cols)
    X_test,  _   = _label_encode_cats(test_df,  feature_cols, enc)
    y_train = train_df["inn1_total_score"]
    y_test  = test_df["inn1_total_score"]

    rf = RandomForestRegressor(**RF_PARAMS)
    rf.fit(X_train, y_train)

    y_pred = rf.predict(X_test)
    mae, rmse, r2 = regression_metrics(y_test, y_pred)
    fi_df = make_fi_df(rf.feature_importances_, feature_cols)

    print(f"  MAE={mae}  RMSE={rmse}  R²={r2}")
    display(fi_df)
    return rf, mae, rmse, r2, fi_df

if len(train_df) < 5:
    raise ValueError(f"Only {len(train_df)} training matches — need at least 5.")

xgb_6,  xgb_mae_6,  xgb_rmse_6,  xgb_r2_6,  xgb_fi_6,  pruned_6,  preds_6  = xgb_train_pruned("at-over-6",  AT6_FEATURES,  train_df, test_df)
xgb_10, xgb_mae_10, xgb_rmse_10, xgb_r2_10, xgb_fi_10, pruned_10, preds_10 = xgb_train_pruned("at-over-10", AT10_FEATURES, train_df, test_df)
xgb_16, xgb_mae_16, xgb_rmse_16, xgb_r2_16, xgb_fi_16, pruned_16, preds_16 = xgb_train_pruned("at-over-16", AT16_FEATURES, train_df, test_df)

rf_6,  rf_mae_6,  rf_rmse_6,  rf_r2_6,  rf_fi_6  = rf_train("at-over-6",  pruned_6,  train_df, test_df)
rf_10, rf_mae_10, rf_rmse_10, rf_r2_10, rf_fi_10 = rf_train("at-over-10", pruned_10, train_df, test_df)
rf_16, rf_mae_16, rf_rmse_16, rf_r2_16, rf_fi_16 = rf_train("at-over-16", pruned_16, train_df, test_df)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 9 — Summary table
# ═══════════════════════════════════════════════════════════════════

def _fmt(v):
    return f"{v:.2f}" if v is not None else "n/a"

print("\n" + "="*72)
print(f"  {'Model':<14}  {'Algorithm':<16}  {'MAE':>7}  {'RMSE':>7}  {'R²':>7}  {'Features':>8}")
print("  " + "─"*68)
rows_cmp = [
    ("at-over-6",  "XGBoost", xgb_mae_6,  xgb_rmse_6,  xgb_r2_6,  len(pruned_6)),
    ("at-over-6",  "RandForest", rf_mae_6,  rf_rmse_6,  rf_r2_6,   len(pruned_6)),
    ("at-over-10", "XGBoost", xgb_mae_10, xgb_rmse_10, xgb_r2_10, len(pruned_10)),
    ("at-over-10", "RandForest", rf_mae_10, rf_rmse_10, rf_r2_10,  len(pruned_10)),
    ("at-over-16", "XGBoost", xgb_mae_16, xgb_rmse_16, xgb_r2_16, len(pruned_16)),
    ("at-over-16", "RandForest", rf_mae_16, rf_rmse_16, rf_r2_16,  len(pruned_16)),
]
for nm, algo, mae, rmse, r2, nf in rows_cmp:
    print(f"  {nm:<14}  {algo:<16}  {_fmt(mae):>7}  {_fmt(rmse):>7}  {_fmt(r2):>7}  {nf:>8}")
print("="*72)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 10 — Log to MLflow
# ═══════════════════════════════════════════════════════════════════

current_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{current_user}/cricket-inn1-score-predictor")

def log_xgb(run_name, model, mae, rmse, r2, feature_cols, reg_name, fi_df):
    if model is None:
        return
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(XGB_PARAMS)
        mlflow.log_param("algorithm",     "XGBoost")
        mlflow.log_param("feature_count", len(feature_cols))
        mlflow.log_param("train_cutoff",  TRAIN_CUTOFF)
        mlflow.log_metric("test_mae",   mae)
        mlflow.log_metric("test_rmse",  rmse)
        if r2 is not None:
            mlflow.log_metric("test_r2", r2)
        mlflow.log_metric("train_matches", len(train_df))
        mlflow.log_metric("test_matches",  len(test_df))
        if fi_df is not None:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                             prefix=f"{reg_name}_fi_") as f:
                fi_df.to_csv(f, index=False); tmp = f.name
            mlflow.log_artifact(tmp, artifact_path="feature_importance")
            os.unlink(tmp)
        mlflow.xgboost.log_model(model, artifact_path="model",
                                 registered_model_name=reg_name)
    print(f"  Logged XGBoost {reg_name}  MAE={mae}")

def log_rf(run_name, model, mae, rmse, r2, feature_cols, reg_name, fi_df):
    if model is None:
        return
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(RF_PARAMS)
        mlflow.log_param("algorithm",     "RandomForest")
        mlflow.log_param("feature_count", len(feature_cols))
        mlflow.log_param("train_cutoff",  TRAIN_CUTOFF)
        mlflow.log_metric("test_mae",   mae)
        mlflow.log_metric("test_rmse",  rmse)
        if r2 is not None:
            mlflow.log_metric("test_r2", r2)
        mlflow.log_metric("train_matches", len(train_df))
        mlflow.log_metric("test_matches",  len(test_df))
        if fi_df is not None:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                             prefix=f"{reg_name}_rf_fi_") as f:
                fi_df.to_csv(f, index=False); tmp = f.name
            mlflow.log_artifact(tmp, artifact_path="feature_importance")
            os.unlink(tmp)
        mlflow.sklearn.log_model(model, artifact_path="model",
                                 registered_model_name=f"{reg_name}-rf")
    print(f"  Logged RF      {reg_name}-rf  MAE={mae}")

log_xgb("at-over-6-xgb",  xgb_6,  xgb_mae_6,  xgb_rmse_6,  xgb_r2_6,  pruned_6,  "inn1-score-at-6",  xgb_fi_6)
log_xgb("at-over-10-xgb", xgb_10, xgb_mae_10, xgb_rmse_10, xgb_r2_10, pruned_10, "inn1-score-at-10", xgb_fi_10)
log_xgb("at-over-16-xgb", xgb_16, xgb_mae_16, xgb_rmse_16, xgb_r2_16, pruned_16, "inn1-score-at-16", xgb_fi_16)

log_rf("at-over-6-rf",  rf_6,  rf_mae_6,  rf_rmse_6,  rf_r2_6,  pruned_6,  "inn1-score-at-6",  rf_fi_6)
log_rf("at-over-10-rf", rf_10, rf_mae_10, rf_rmse_10, rf_r2_10, pruned_10, "inn1-score-at-10", rf_fi_10)
log_rf("at-over-16-rf", rf_16, rf_mae_16, rf_rmse_16, rf_r2_16, pruned_16, "inn1-score-at-16", rf_fi_16)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 11 — Write summary to gold
# ═══════════════════════════════════════════════════════════════════

def _model_entry(name, desc, xgb_mae, xgb_rmse, xgb_r2, rf_mae, rf_rmse, rf_r2,
                 feats, fi_df, preds_records):
    return {
        "name":          name,
        "description":   desc,
        "feature_count": len(feats),
        "xgb": {
            "mae":  xgb_mae,
            "rmse": xgb_rmse,
            "r2":   xgb_r2,
        },
        "rf": {
            "mae":  rf_mae,
            "rmse": rf_rmse,
            "r2":   rf_r2,
        },
        "feature_importance": (
            fi_df[["rank","feature","importance","pct_of_total"]].to_dict("records")
            if fi_df is not None else []
        ),
        "test_predictions": preds_records,
    }

summary = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "train_cutoff":     TRAIN_CUTOFF,
    "train_matches":    int(len(train_df)),
    "test_matches":     int(len(test_df)),
    "models": [
        _model_entry("at-over-6",  "Powerplay complete — overs 1–6 known",
                     xgb_mae_6,  xgb_rmse_6,  xgb_r2_6,
                     rf_mae_6,   rf_rmse_6,   rf_r2_6,
                     pruned_6,  xgb_fi_6,  preds_6),
        _model_entry("at-over-10", "Halfway — overs 1–10 known",
                     xgb_mae_10, xgb_rmse_10, xgb_r2_10,
                     rf_mae_10,  rf_rmse_10,  rf_r2_10,
                     pruned_10, xgb_fi_10, preds_10),
        _model_entry("at-over-16", "Death approaching — overs 1–16 known",
                     xgb_mae_16, xgb_rmse_16, xgb_r2_16,
                     rf_mae_16,  rf_rmse_16,  rf_r2_16,
                     pruned_16, xgb_fi_16, preds_16),
    ],
}

gold.get_blob_client("cricket/ml_features/t20/score_predictor_summary.json").upload_blob(
    json.dumps(summary, indent=2).encode(), overwrite=True
)
print(f"Summary → gold/cricket/ml_features/t20/score_predictor_summary.json")
print("\nNotebook complete.")
