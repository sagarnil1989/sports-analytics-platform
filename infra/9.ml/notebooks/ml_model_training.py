# Databricks notebook: ml — model training
# Reads gold/cricket/ml_features/t20/features.parquet (written by ml_feature_extraction).
# Trains two XGBoost models with Group K-Fold cross-validation:
#   1. Score predictor  — regression, predicts 1st innings final total
#   2. Over/Under model — classifier, predicts whether actual score > bookmaker line
#
# Logs experiments and registers best models in MLflow Model Registry.
# Writes accuracy summary to gold/cricket/ml_features/t20/model_accuracy.json
#
# Run manually or as Activity 2 in pl_ml_retrain (weekly, after feature extraction).

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "pyarrow", "pandas", "xgboost", "scikit-learn"
], check=True)

# COMMAND ----------

import io, json
import numpy as np
import pandas as pd
import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.model_selection import GroupKFold
from sklearn.metrics import mean_absolute_error, r2_score, accuracy_score, roc_auc_score
from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc  = BlobServiceClient.from_connection_string(conn_str)
gold = svc.get_container_client("gold")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 1 — Load feature Parquet
# ═══════════════════════════════════════════════════════════════════

raw = gold.get_blob_client("cricket/ml_features/t20/features.parquet").download_blob().readall()
df  = pd.read_parquet(io.BytesIO(raw))

print(f"Loaded {len(df):,} rows from {df['event_id'].nunique()} matches")
print(f"Outcome split: {dict(df['outcome'].value_counts())}")
display(df.head(5))

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 2 — Feature sets
# ═══════════════════════════════════════════════════════════════════

# Numeric features used by both models
# Identifiers (event_id, match_name, league, etc.) excluded — they would leak or overfit
FEATURES = [
    "over_num", "balls_elapsed", "balls_remaining",
    "score", "wickets",
    "run_rate", "projected_score",
    "bw_fours", "bw_sixes", "bw_wickets", "bw_dots", "bw_singles", "bw_doubles",
    # bookmaker signals — include so the O/U model can learn when market is wrong
    "predicted_total", "score_vs_line",
    "batting_team_odds", "bowling_team_odds", "implied_prob_bat",
    "over_odds", "under_odds",
]

def prepare(subset_df):
    X = subset_df[FEATURES].copy()
    # Fill missing market columns (some snapshots lack odds) with column median
    for col in X.columns:
        if X[col].isna().any():
            X[col] = X[col].fillna(X[col].median())
    return X

# ── Score predictor: 1st innings only ─────────────────────────────
# We predict the final 1st innings total from state at any given over.
# 2nd innings is excluded — a separate chase model would be its own task.
df_inn1 = df[df["innings"] == 1].reset_index(drop=True)
print(f"\nScore predictor training set: {len(df_inn1):,} rows from {df_inn1['event_id'].nunique()} matches")

# ── Over/Under classifier: 1st innings only ────────────────────────
# Predicts whether actual_total > bookmaker's predicted_total line.
# Only use rows where we actually have a bookmaker line.
df_ou = df_inn1[df_inn1["predicted_total"].notna()].reset_index(drop=True)
print(f"Over/Under classifier set   : {len(df_ou):,} rows  (after dropping rows without bookmaker line)")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 3 — Group K-Fold cross-validation helper
# Groups rows by event_id so a match never appears in both train and test.
# With ~33 matches, 5-fold CV leaves ~6–7 matches out per fold.
# ═══════════════════════════════════════════════════════════════════

def group_kfold_cv(model_factory, X, y, groups, n_splits=5):
    gkf    = GroupKFold(n_splits=n_splits)
    scores = []
    for fold, (tr_idx, va_idx) in enumerate(gkf.split(X, y, groups)):
        X_tr, X_va = X.iloc[tr_idx], X.iloc[va_idx]
        y_tr, y_va = y.iloc[tr_idx], y.iloc[va_idx]
        m = model_factory()
        m.fit(X_tr, y_tr)
        scores.append((m, X_va, y_va, va_idx))
    return scores

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 4 — Train Score Predictor (XGBoost Regressor)
# ═══════════════════════════════════════════════════════════════════

X_reg = prepare(df_inn1)
y_reg = df_inn1["actual_total"]
g_reg = df_inn1["event_id"]

xgb_reg_params = {
    "n_estimators":    300,
    "max_depth":       4,
    "learning_rate":   0.05,
    "subsample":       0.8,
    "colsample_bytree":0.8,
    "min_child_weight":3,
    "random_state":    42,
    "n_jobs":          -1,
}

cv_results_reg = group_kfold_cv(
    lambda: xgb.XGBRegressor(**xgb_reg_params),
    X_reg, y_reg, g_reg
)

# Aggregate CV metrics
mae_scores = []
r2_scores  = []
for m, X_va, y_va, _ in cv_results_reg:
    preds = m.predict(X_va)
    mae_scores.append(mean_absolute_error(y_va, preds))
    r2_scores.append(r2_score(y_va, preds))

cv_mae = float(np.mean(mae_scores))
cv_r2  = float(np.mean(r2_scores))
print(f"\nScore Predictor — Group K-Fold CV")
print(f"  MAE  : {cv_mae:.2f} runs  (avg across {len(mae_scores)} folds)")
print(f"  R²   : {cv_r2:.3f}")

# Train final model on all data
final_reg = xgb.XGBRegressor(**xgb_reg_params)
final_reg.fit(X_reg, y_reg)

# Feature importance
fi_reg = pd.Series(final_reg.feature_importances_, index=FEATURES).sort_values(ascending=False)
print(f"\nTop feature importances (score predictor):\n{fi_reg.head(10).to_string()}")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 5 — Train Over/Under Classifier (XGBoost Classifier)
# ═══════════════════════════════════════════════════════════════════

X_cls = prepare(df_ou)
y_cls = df_ou["outcome_bin"]
g_cls = df_ou["event_id"]

xgb_cls_params = {
    "n_estimators":     300,
    "max_depth":        4,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "random_state":     42,
    "n_jobs":           -1,
    "use_label_encoder": False,
    "eval_metric":      "logloss",
}

cv_results_cls = group_kfold_cv(
    lambda: xgb.XGBClassifier(**xgb_cls_params),
    X_cls, y_cls, g_cls
)

acc_scores = []
auc_scores = []
for m, X_va, y_va, _ in cv_results_cls:
    proba = m.predict_proba(X_va)[:, 1]
    preds = m.predict(X_va)
    acc_scores.append(accuracy_score(y_va, preds))
    if len(y_va.unique()) > 1:
        auc_scores.append(roc_auc_score(y_va, proba))

cv_acc = float(np.mean(acc_scores))
cv_auc = float(np.mean(auc_scores)) if auc_scores else None
print(f"\nOver/Under Classifier — Group K-Fold CV")
print(f"  Accuracy : {cv_acc:.3f}")
print(f"  ROC-AUC  : {cv_auc:.3f}" if cv_auc else "  ROC-AUC  : n/a (only 1 class in some folds)")

# Train final model on all data
final_cls = xgb.XGBClassifier(**xgb_cls_params)
final_cls.fit(X_cls, y_cls)

fi_cls = pd.Series(final_cls.feature_importances_, index=FEATURES).sort_values(ascending=False)
print(f"\nTop feature importances (O/U classifier):\n{fi_cls.head(10).to_string()}")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 6 — Log to MLflow and register models
# ═══════════════════════════════════════════════════════════════════

current_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{current_user}/cricket-t20-models")

# ── Score predictor ───────────────────────────────────────────────
with mlflow.start_run(run_name="score-predictor") as run_reg:
    mlflow.log_params(xgb_reg_params)
    mlflow.log_metric("cv_mae",      cv_mae)
    mlflow.log_metric("cv_r2",       cv_r2)
    mlflow.log_metric("training_rows", len(X_reg))
    mlflow.log_metric("matches",       g_reg.nunique())
    mlflow.xgboost.log_model(
        final_reg,
        artifact_path="model",
        registered_model_name="cricket-score-predictor",
    )
    reg_run_id = run_reg.info.run_id

print(f"Score predictor logged: run_id={reg_run_id}")

# ── Over/Under classifier ─────────────────────────────────────────
with mlflow.start_run(run_name="ou-classifier") as run_cls:
    mlflow.log_params(xgb_cls_params)
    mlflow.log_metric("cv_accuracy",   cv_acc)
    if cv_auc:
        mlflow.log_metric("cv_roc_auc", cv_auc)
    mlflow.log_metric("training_rows", len(X_cls))
    mlflow.log_metric("matches",       g_cls.nunique())
    mlflow.xgboost.log_model(
        final_cls,
        artifact_path="model",
        registered_model_name="cricket-ou-classifier",
    )
    cls_run_id = run_cls.info.run_id

print(f"O/U classifier logged : run_id={cls_run_id}")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 7 — Write accuracy summary to gold
# Human-readable JSON consumed by the web UI and future inference.
# ═══════════════════════════════════════════════════════════════════

from datetime import datetime, timezone
summary = {
    "trained_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "training_matches": int(g_reg.nunique()),
    "training_rows":    int(len(X_reg)),
    "features":         FEATURES,
    "score_predictor": {
        "description":    "Predicts 1st innings final score from match state at any over",
        "algorithm":      "XGBoost Regressor",
        "cv_mae_runs":    round(cv_mae, 2),
        "cv_r2":          round(cv_r2, 3),
        "mlflow_run_id":  reg_run_id,
        "top_features":   fi_reg.head(5).index.tolist(),
    },
    "ou_classifier": {
        "description":    "Predicts whether actual score will exceed bookmaker's over/under line",
        "algorithm":      "XGBoost Classifier",
        "cv_accuracy":    round(cv_acc, 3),
        "cv_roc_auc":     round(cv_auc, 3) if cv_auc else None,
        "mlflow_run_id":  cls_run_id,
        "top_features":   fi_cls.head(5).index.tolist(),
    },
}

blob_path = "cricket/ml_features/t20/model_accuracy.json"
gold.get_blob_client(blob_path).upload_blob(
    json.dumps(summary, indent=2).encode(), overwrite=True
)

print(f"\nAccuracy summary written → gold/{blob_path}")
print(json.dumps(summary, indent=2))
