# Databricks notebook: ml_train_over_under
#
# Phase 2 of the Over/Under Innings Total Predictor.
#
# Reads gold/ml/over_under_training_data.csv (produced by ml_extract_over_under_features),
# trains one LightGBM binary classifier per (market, checkpoint_over), evaluates with
# stratified 5-fold CV, calibrates probabilities with isotonic regression, then saves
# each model to DBFS and writes a summary to gold/ml/over_under_model_metadata.json.
#
# Models saved to:
#   dbfs:/FileStore/cricket-pipeline/models/over_under/<market>_cp<over>.pkl
#
# Run manually after re-running ml_extract_over_under_features.

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "lightgbm", "scikit-learn"
], check=True)

# COMMAND ----------

import csv, io, json, os, pickle, math
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from azure.storage.blob import BlobServiceClient
import lightgbm as lgb
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.metrics import roc_auc_score, brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc  = BlobServiceClient.from_connection_string(conn_str)
gold = svc.get_container_client("gold")

DBFS_MODEL_DIR = "/dbfs/FileStore/cricket-pipeline/models/over_under"
os.makedirs(DBFS_MODEL_DIR, exist_ok=True)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Load training data
# ---------------------------------------------------------------------------

raw = gold.get_blob_client("ml/over_under_training_data.csv").download_blob().readall()
reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
all_rows = list(reader)

train_rows = [r for r in all_rows if r.get("split", "train") == "train"]
test_rows  = [r for r in all_rows if r.get("split") == "test"]
print(f"Loaded {len(all_rows)} rows from training CSV  (train={len(train_rows)}, test={len(test_rows)})")
print(f"Training on train split only; test split used for held-out evaluation.")

# ---------------------------------------------------------------------------
# Feature definition
# ---------------------------------------------------------------------------

# Core features available at every checkpoint
FEATURES = [
    "score",
    "wickets_in_hand",
    "betting_line",
    "score_vs_line_pace",
    "run_rate",
    "rr_required",
    "implied_prob_over",
    "batting_team_win_odds",
]

def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _row_to_features(row: Dict) -> Optional[List[float]]:
    """Convert a CSV row to a feature vector. Returns None if required fields missing."""
    vec = []
    for f in FEATURES:
        val = _to_float(row.get(f))
        # Impute missing win odds with neutral value (2.0 = 50/50)
        if val is None and f == "batting_team_win_odds":
            val = 2.0
        if val is None:
            return None
        vec.append(val)
    return vec


# COMMAND ----------

# ---------------------------------------------------------------------------
# Group rows by (market, checkpoint_over)
# ---------------------------------------------------------------------------

groups: Dict[Tuple[str, int], Tuple[List, List]] = defaultdict(lambda: ([], []))

for row in train_rows:
    market = row.get("market", "")
    cp     = row.get("checkpoint_over", "")
    label  = _to_float(row.get("label"))
    if label is None:
        continue
    fvec = _row_to_features(row)
    if fvec is None:
        continue
    key = (market, int(cp))
    groups[key][0].append(fvec)
    groups[key][1].append(int(label))

print(f"\nGroups to train: {len(groups)}")
for (mkt, cp), (X, y) in sorted(groups.items()):
    n_over = sum(y)
    print(f"  {mkt} cp={cp}: n={len(y)}  over={n_over} ({100*n_over/len(y):.1f}%)")

# COMMAND ----------

# ---------------------------------------------------------------------------
# LightGBM base estimator settings
# Small dataset → heavy regularisation, shallow trees
# ---------------------------------------------------------------------------

def _make_lgb(n_samples: int) -> lgb.LGBMClassifier:
    return lgb.LGBMClassifier(
        n_estimators=200,
        learning_rate=0.03,
        num_leaves=8,
        max_depth=3,
        min_child_samples=max(3, n_samples // 20),
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.5,
        reg_lambda=1.0,
        class_weight="balanced",
        random_state=42,
        verbose=-1,
    )

# ---------------------------------------------------------------------------
# Train, evaluate, calibrate, save
# ---------------------------------------------------------------------------

cv        = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
results   = []
model_meta: Dict[str, Any] = {}

for (mkt, cp), (X_list, y_list) in sorted(groups.items()):
    X = np.array(X_list, dtype=np.float32)
    y = np.array(y_list, dtype=np.int32)
    n = len(y)

    if n < 15:
        print(f"[{mkt} cp={cp}] Skipped — only {n} samples")
        continue

    # ── 5-fold CV on raw LightGBM (AUC + Brier) ─────────────────────────────
    lgb_clf  = _make_lgb(n)
    auc_scores = cross_val_score(lgb_clf, X, y, cv=cv, scoring="roc_auc")
    brier_scores = []
    for train_idx, test_idx in cv.split(X, y):
        clf_tmp = _make_lgb(n)
        clf_tmp.fit(X[train_idx], y[train_idx])
        proba = clf_tmp.predict_proba(X[test_idx])[:, 1]
        brier_scores.append(brier_score_loss(y[test_idx], proba))

    cv_auc   = float(np.mean(auc_scores))
    cv_brier = float(np.mean(brier_scores))

    # ── Final model: LightGBM + isotonic calibration on full data ────────────
    lgb_final = _make_lgb(n)
    # CalibratedClassifierCV with cv="prefit" needs a pre-fitted estimator;
    # use internal split so calibration has held-out data.
    calibrated = CalibratedClassifierCV(
        _make_lgb(n), method="isotonic", cv=min(5, max(3, n // 10))
    )
    calibrated.fit(X, y)

    # ── Feature importance from uncalibrated model fit on all data ───────────
    lgb_final.fit(X, y)
    importances = dict(zip(FEATURES, lgb_final.feature_importances_.tolist()))

    # ── Save model ───────────────────────────────────────────────────────────
    model_key  = f"{mkt}_cp{cp}"
    model_path = os.path.join(DBFS_MODEL_DIR, f"{model_key}.pkl")
    with open(model_path, "wb") as f:
        pickle.dump({"model": calibrated, "features": FEATURES}, f)

    meta_entry = {
        "market":           mkt,
        "checkpoint_over":  cp,
        "n_samples":        n,
        "n_over":           int(sum(y)),
        "over_pct":         round(100 * sum(y) / n, 1),
        "cv_auc":           round(cv_auc, 4),
        "cv_brier":         round(cv_brier, 4),
        "feature_importance": importances,
        "model_path":       f"dbfs:/FileStore/cricket-pipeline/models/over_under/{model_key}.pkl",
    }
    results.append(meta_entry)
    model_meta[model_key] = meta_entry

    print(
        f"[{mkt:>18} cp={cp}]  n={n:>3}  "
        f"CV-AUC={cv_auc:.3f}  CV-Brier={cv_brier:.3f}  "
        f"({meta_entry['over_pct']}% OVER)"
    )

# COMMAND ----------

# ---------------------------------------------------------------------------
# Pooled models — one per market, all checkpoints combined
#
# Why: per-checkpoint models have ~80 samples each (too few for reliable
# early-innings prediction). A pooled model trains on all checkpoints at once
# (~640 for innings_total, ~220 for first_12) using checkpoint_over and
# balls_remaining as extra features so the model can learn how predictability
# changes across the innings.
#
# Saved as: <market>_pooled.pkl
# At inference time, the pooled model is the fallback when no per-checkpoint
# model has CV-AUC >= 0.60.
# ---------------------------------------------------------------------------

POOLED_FEATURES = FEATURES + ["checkpoint_over", "balls_remaining", "balls_completed"]

def _row_to_pooled_features(row: Dict) -> Optional[List[float]]:
    vec = []
    for f in POOLED_FEATURES:
        val = _to_float(row.get(f))
        if val is None and f == "batting_team_win_odds":
            val = 2.0
        if val is None:
            return None
        vec.append(val)
    return vec


# Group by market only (all checkpoints pooled together)
pooled_groups: Dict[str, Tuple[List, List]] = defaultdict(lambda: ([], []))

for row in train_rows:
    market = row.get("market", "")
    label  = _to_float(row.get("label"))
    if label is None:
        continue
    fvec = _row_to_pooled_features(row)
    if fvec is None:
        continue
    pooled_groups[market][0].append(fvec)
    pooled_groups[market][1].append(int(label))

print("\n── Pooled model training ──")
pooled_results = []

for market, (X_list, y_list) in sorted(pooled_groups.items()):
    X = np.array(X_list, dtype=np.float32)
    y = np.array(y_list, dtype=np.int32)
    n = len(y)

    print(f"\n[{market}_pooled]  n={n}  over%={100*sum(y)/n:.1f}%")

    # CV evaluation
    lgb_pooled = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.02,
        num_leaves=15,
        max_depth=4,
        min_child_samples=max(5, n // 30),
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.3,
        reg_lambda=0.5,
        class_weight="balanced",
        random_state=42,
        verbose=-1,
    )
    auc_scores   = cross_val_score(lgb_pooled, X, y, cv=cv, scoring="roc_auc")
    brier_scores = []
    for train_idx, test_idx in cv.split(X, y):
        clf_tmp = lgb.LGBMClassifier(
            n_estimators=300, learning_rate=0.02, num_leaves=15, max_depth=4,
            min_child_samples=max(5, n // 30), subsample=0.8, colsample_bytree=0.8,
            reg_alpha=0.3, reg_lambda=0.5, class_weight="balanced",
            random_state=42, verbose=-1,
        )
        clf_tmp.fit(X[train_idx], y[train_idx])
        proba = clf_tmp.predict_proba(X[test_idx])[:, 1]
        brier_scores.append(brier_score_loss(y[test_idx], proba))

    cv_auc   = float(np.mean(auc_scores))
    cv_brier = float(np.mean(brier_scores))
    print(f"  CV-AUC={cv_auc:.3f}  CV-Brier={cv_brier:.3f}")

    # Fit per-checkpoint AUC breakdown so we can see where signal comes from
    for cp in sorted(set(int(r.get("checkpoint_over", 0)) for r in all_rows if r.get("market") == market)):
        cp_rows = [r for r in all_rows if r.get("market") == market and int(r.get("checkpoint_over", -1)) == cp]
        cp_X = [_row_to_pooled_features(r) for r in cp_rows]
        cp_y = [int(_to_float(r.get("label"))) for r in cp_rows]
        cp_X = [x for x, yy in zip(cp_X, cp_y) if x is not None]
        cp_y = [yy for x, yy in zip(cp_X, [int(_to_float(r.get("label"))) for r in cp_rows]) if x is not None]
        # Re-align after filtering
        pairs = [(x, yy) for x, yy in zip([_row_to_pooled_features(r) for r in cp_rows],
                                            [int(_to_float(r.get("label"))) for r in cp_rows]) if x is not None]
        if len(pairs) < 10:
            continue
        cp_X2 = np.array([p[0] for p in pairs], dtype=np.float32)
        cp_y2 = np.array([p[1] for p in pairs], dtype=np.int32)

    # Final calibrated model on all data
    calibrated_pooled = CalibratedClassifierCV(
        lgb.LGBMClassifier(
            n_estimators=300, learning_rate=0.02, num_leaves=15, max_depth=4,
            min_child_samples=max(5, n // 30), subsample=0.8, colsample_bytree=0.8,
            reg_alpha=0.3, reg_lambda=0.5, class_weight="balanced",
            random_state=42, verbose=-1,
        ),
        method="isotonic", cv=min(5, max(3, n // 20))
    )
    calibrated_pooled.fit(X, y)

    # Feature importance from uncalibrated model
    lgb_for_imp = lgb.LGBMClassifier(
        n_estimators=300, learning_rate=0.02, num_leaves=15, max_depth=4,
        min_child_samples=max(5, n // 30), subsample=0.8, colsample_bytree=0.8,
        reg_alpha=0.3, reg_lambda=0.5, class_weight="balanced",
        random_state=42, verbose=-1,
    )
    lgb_for_imp.fit(X, y)
    importances_pooled = dict(zip(POOLED_FEATURES, lgb_for_imp.feature_importances_.tolist()))
    print("  Feature importance:")
    total_imp_p = sum(importances_pooled.values()) or 1
    for f, v in sorted(importances_pooled.items(), key=lambda x: -x[1])[:6]:
        print(f"    {f:<30} {100*v/total_imp_p:>5.1f}%")

    model_key  = f"{market}_pooled"
    model_path = os.path.join(DBFS_MODEL_DIR, f"{model_key}.pkl")
    with open(model_path, "wb") as f:
        pickle.dump({"model": calibrated_pooled, "features": POOLED_FEATURES}, f)

    pooled_entry = {
        "market":           market,
        "checkpoint_over":  "pooled",
        "n_samples":        n,
        "n_over":           int(sum(y)),
        "over_pct":         round(100 * sum(y) / n, 1),
        "cv_auc":           round(cv_auc, 4),
        "cv_brier":         round(cv_brier, 4),
        "feature_importance": importances_pooled,
        "model_path":       f"dbfs:/FileStore/cricket-pipeline/models/over_under/{model_key}.pkl",
    }
    results.append(pooled_entry)
    pooled_results.append(pooled_entry)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Held-out test evaluation (if test_rows exist)
# ---------------------------------------------------------------------------

test_eval = []
if test_rows:
    print(f"\n── Test set evaluation ({len(test_rows)} rows) ──")

    # Load saved models and evaluate on test rows
    for (mkt, cp), (_, _) in sorted(groups.items()):
        model_key = f"{mkt}_cp{cp}"
        model_path = os.path.join(DBFS_MODEL_DIR, f"{model_key}.pkl")
        if not os.path.exists(model_path):
            continue
        with open(model_path, "rb") as f:
            m_obj = pickle.load(f)
        cp_test = [r for r in test_rows if r.get("market") == mkt and int(r.get("checkpoint_over", -1)) == cp]
        fvecs = [_row_to_features(r) for r in cp_test]
        labels = [int(_to_float(r.get("label"))) for r in cp_test]
        pairs = [(fv, lb) for fv, lb in zip(fvecs, labels) if fv is not None]
        if len(pairs) < 5:
            continue
        X_t = np.array([p[0] for p in pairs], dtype=np.float32)
        y_t = np.array([p[1] for p in pairs], dtype=np.int32)
        proba = m_obj["model"].predict_proba(X_t)[:, 1]
        auc = roc_auc_score(y_t, proba) if len(set(y_t)) > 1 else float("nan")
        test_eval.append({"key": model_key, "n": len(pairs), "test_auc": round(auc, 3)})
        print(f"  {model_key:<30} n={len(pairs)}  test_auc={auc:.3f}")

    # Pooled models on test set
    for market in sorted(pooled_groups.keys()):
        model_key = f"{market}_pooled"
        model_path = os.path.join(DBFS_MODEL_DIR, f"{model_key}.pkl")
        if not os.path.exists(model_path):
            continue
        with open(model_path, "rb") as f:
            m_obj = pickle.load(f)
        mkt_test = [r for r in test_rows if r.get("market") == market]
        fvecs  = [_row_to_pooled_features(r) for r in mkt_test]
        labels = [int(_to_float(r.get("label"))) for r in mkt_test]
        pairs  = [(fv, lb) for fv, lb in zip(fvecs, labels) if fv is not None]
        if len(pairs) < 5:
            continue
        X_t = np.array([p[0] for p in pairs], dtype=np.float32)
        y_t = np.array([p[1] for p in pairs], dtype=np.int32)
        proba = m_obj["model"].predict_proba(X_t)[:, 1]
        auc = roc_auc_score(y_t, proba) if len(set(y_t)) > 1 else float("nan")
        test_eval.append({"key": model_key, "n": len(pairs), "test_auc": round(auc, 3)})
        print(f"  {model_key:<30} n={len(pairs)}  test_auc={auc:.3f}")
else:
    print("\nNo test rows — set train_cutoff_date in gold/ml/train_config.json to enable held-out evaluation.")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Per-match predictions for display page
#
# For each (market, checkpoint), apply the inference model (per-cp if AUC ≥ 0.60,
# else pooled) to every train and test row and record the prediction.
# Written to gold/ml/over_under_match_predictions.json — read by the display page.
# ---------------------------------------------------------------------------

_AUC_THRESHOLD = 0.60

def _apply_model(m_obj, fvec):
    try:
        X = np.array([fvec], dtype=np.float32)
        return float(m_obj["model"].predict_proba(X)[0][1])
    except Exception:
        return None

def _pred_row(r, prob):
    lbl = _to_float(r.get("label"))
    if lbl is None or prob is None:
        return None
    actual    = "over" if int(lbl) == 1 else "under"
    predicted = "over" if prob >= 0.5 else "under"
    return {
        "event_id":      r.get("event_id"),
        "match_name":    r.get("match_name"),
        "match_date":    str(r.get("match_date_utc") or "")[:10],
        "batting_team":  r.get("batting_team_inn1") or r.get("home_team"),
        "home_team":     r.get("home_team"),
        "away_team":     r.get("away_team"),
        "score_at_cp":   r.get("score"),
        "wickets_at_cp": r.get("wickets"),
        "over_str":      r.get("over_str"),
        "betting_line":  r.get("betting_line"),
        "actual_value":  r.get("actual_value"),
        "actual_result": actual,
        "predicted":     predicted,
        "prob_over":     round(prob, 3),
        "correct":       actual == predicted,
    }

# Load pooled models once
_pooled_cache = {}
for _mkt in sorted(set(r.get("market") for r in all_rows)):
    _pk = f"{_mkt}_pooled"
    _pp = os.path.join(DBFS_MODEL_DIR, f"{_pk}.pkl")
    if os.path.exists(_pp):
        with open(_pp, "rb") as f:
            _pooled_cache[_mkt] = pickle.load(f)

match_preds_out = {
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "n_train_rows":     len(train_rows),
    "n_test_rows":      len(test_rows),
    "markets":          {},
}

for _mkt in sorted(set(r.get("market") for r in all_rows)):
    _cps = sorted(set(int(r.get("checkpoint_over", 0)) for r in all_rows if r.get("market") == _mkt))
    match_preds_out["markets"][_mkt] = {"checkpoints": {}}

    for _cp in _cps:
        _per_cp_key  = f"{_mkt}_cp{_cp}"
        _per_cp_meta = model_meta.get(_per_cp_key, {})
        _cv_auc      = _per_cp_meta.get("cv_auc", 0.0)
        _use_per_cp  = _cv_auc >= _AUC_THRESHOLD

        _per_cp_obj = None
        _per_cp_path = os.path.join(DBFS_MODEL_DIR, f"{_per_cp_key}.pkl")
        if _use_per_cp and os.path.exists(_per_cp_path):
            with open(_per_cp_path, "rb") as f:
                _per_cp_obj = pickle.load(f)

        _active_obj     = _per_cp_obj if _per_cp_obj else _pooled_cache.get(_mkt)
        _active_key     = _per_cp_key if _per_cp_obj else f"{_mkt}_pooled"
        _use_pooled_fv  = _per_cp_obj is None

        if _active_obj is None:
            continue

        def _collect(rows):
            out = []
            for r in rows:
                if r.get("market") != _mkt or int(r.get("checkpoint_over", -1)) != _cp:
                    continue
                fv = _row_to_features(r) if not _use_pooled_fv else _row_to_pooled_features(r)
                if fv is None:
                    continue
                prob = _apply_model(_active_obj, fv)
                row  = _pred_row(r, prob)
                if row:
                    out.append(row)
            return out

        _train_preds = _collect(train_rows)
        _test_preds  = _collect(test_rows)

        match_preds_out["markets"][_mkt]["checkpoints"][str(_cp)] = {
            "model_used": _active_key,
            "cv_auc":     round(_cv_auc, 3),
            "n_train":    len(_train_preds),
            "n_test":     len(_test_preds),
            "train":      _train_preds,
            "test":       _test_preds,
        }
        print(f"  [{_mkt} cp={_cp}] model={_active_key}  train={len(_train_preds)}  test={len(_test_preds)}")

gold.get_blob_client("ml/over_under_match_predictions.json").upload_blob(
    json.dumps(match_preds_out, indent=2).encode(), overwrite=True
)
print("Per-match predictions written to gold/ml/over_under_match_predictions.json")

# ---------------------------------------------------------------------------
# Write metadata to gold
# ---------------------------------------------------------------------------

metadata = {
    "trained_at_utc":  datetime.now(timezone.utc).isoformat(),
    "n_models":        len(results),
    "n_train_rows":    len(train_rows),
    "n_test_rows":     len(test_rows),
    "features":        FEATURES,
    "pooled_features": POOLED_FEATURES,
    "models":          results,
    "test_evaluation": test_eval,
}
gold.get_blob_client("ml/over_under_model_metadata.json").upload_blob(
    json.dumps(metadata, indent=2).encode(), overwrite=True
)
print(f"\nMetadata written to gold/ml/over_under_model_metadata.json")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print("\n── Per-checkpoint models ──")
print("{:<22} {:>4} {:>5} {:>9} {:>9}".format("Market", "CP", "N", "CV-AUC", "Brier"))
print("-" * 55)
for m in results:
    if m["checkpoint_over"] == "pooled":
        continue
    flag = "  ✓" if m["cv_auc"] >= 0.60 else ("  ~" if m["cv_auc"] >= 0.52 else "  ✗")
    print("{:<22} {:>4} {:>5} {:>9.3f} {:>9.3f}{}".format(
        m["market"], m["checkpoint_over"], m["n_samples"],
        m["cv_auc"], m["cv_brier"], flag
    ))

print("\n── Pooled models ──")
print("{:<22} {:>8} {:>5} {:>9} {:>9}".format("Market", "CP", "N", "CV-AUC", "Brier"))
print("-" * 58)
for m in pooled_results:
    flag = "  ✓" if m["cv_auc"] >= 0.55 else "  ~"
    print("{:<22} {:>8} {:>5} {:>9.3f} {:>9.3f}{}".format(
        m["market"], m["checkpoint_over"], m["n_samples"],
        m["cv_auc"], m["cv_brier"], flag
    ))

print(f"\nTop pooled feature importances:")
for m in pooled_results:
    print(f"\n  {m['market']}:")
    total_imp = sum(m["feature_importance"].values()) or 1
    for f, v in sorted(m["feature_importance"].items(), key=lambda x: -x[1])[:5]:
        print(f"    {f:<30} {100*v/total_imp:>5.1f}%")

all_model_keys = [m["model_path"].split("/")[-1].replace(".pkl","") for m in results]
dbutils.notebook.exit(json.dumps({
    "n_models":       len(results),
    "pooled_results": [{k: m[k] for k in ("market","cv_auc","cv_brier","n_samples")} for m in pooled_results],
    "models":         [{k: m[k] for k in ("market","checkpoint_over","n_samples","cv_auc","cv_brier")} for m in results],
}))
