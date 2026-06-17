# Why the ML Pipeline Cannot Move to Azure Batch

## Background

As part of the Databricks cost reduction initiative, five operational notebooks were
migrated to Azure Batch:

| Notebook | Pipeline | Frequency |
|---|---|---|
| `bronze_to_silver` | `pl_build_ended_match`, `pl_backfill` | Daily |
| `silver_to_gold` | `pl_build_ended_match`, `pl_backfill` | Daily |
| `discover_cricket_ended` | `pl_build_ended_match` | Daily |
| `hypothesis_inn2_over6` | `pl_hypothesis` | Weekly |
| `hypothesis_timeout_wicket` | `pl_hypothesis` | Weekly |

These notebooks are pure Python (blob I/O, `requests`, `pandas`) with no Spark
dependency, making them straightforward to run on a lightweight VM.

The four ML notebooks in `pl_ml_retrain` and `pl_ml_win_predictor` **cannot follow**
for the reasons below.

---

## Reason 1 — Hard `spark` dependency

Both `ml_model_training.py` and `ml_win_predictor.py` call:

```python
current_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{current_user}/cricket-t20-models")
```

`spark` is the Databricks SparkSession injected automatically into every notebook
environment. It does not exist outside Databricks. There is no lightweight substitute
— even starting a local PySpark session on Batch would add ~2 GB of JVM overhead and
still not provide `current_user()` (a Databricks-specific SQL function).

---

## Reason 2 — Databricks-managed MLflow

The ML notebooks use MLflow for experiment tracking and model storage:

```python
import mlflow, mlflow.xgboost

with mlflow.start_run(run_name="score-predictor"):
    mlflow.log_params(xgb_reg_params)
    mlflow.log_metric("cv_mae", cv_mae)
    mlflow.xgboost.log_model(model, artifact_path="model", ...)
```

When running inside Databricks, MLflow is pre-configured to talk to the
**workspace-embedded tracking server** — no connection string or API key needed.
This gives:

- Experiment run history (parameters, metrics, artifacts per run)
- Model versioning via the MLflow Model Registry
- Artifact storage backed by DBFS

Outside Databricks, you would need to either:

- Stand up a **self-hosted MLflow tracking server** (another VM/container + database)
- Or migrate to **Azure ML** (significant rework of training code)

Neither is worth the effort for a weekly job that runs for ~20 minutes.

---

## Reason 3 — `ml_feature_extraction` is fine, but it feeds the others

`ml_feature_extraction.py` **is** pure Python (pandas + blob) — it has no Spark or
MLflow dependency and could run on Batch without changes.

However, it is Activity 1 in `pl_ml_retrain`, and Activity 2 (`ml_model_training`)
writes its output to the Databricks-managed MLflow registry. Splitting the pipeline
across two compute targets (Batch for step 1, Databricks for step 2) adds operational
complexity for minimal cost saving, since feature extraction is fast (~5 min).

---

## Cost justification for keeping ML in Databricks

The ML pipeline runs **once per week** on Sunday at 03:00 UTC. Weekly runtime is
roughly 20–40 minutes total across four activities.

| | Monthly runtime | Monthly cost (est.) |
|---|---|---|
| Before migration | Daily ops + weekly ML | ~$17 |
| After migration | Weekly ML only | ~$3–5 |

The daily operational pipeline (the dominant cost driver) has already moved to Batch.
Databricks now only spins up clusters once a week, which is acceptable.

---

## What would be needed to move ML to Batch

For completeness, the work required would be:

1. **Remove `spark.sql(current_user())`** — replace with a hardcoded experiment name
   or an environment variable passed via ADF `extendedProperties`.
2. **Replace Databricks MLflow with a standalone tracking server** — options:
   - Self-hosted MLflow on a container app or VM
   - Azure ML workspace (with MLflow-compatible API)
3. **Replace model loading in `ml_win_predictor`** — currently loads from the MLflow
   Model Registry; would need to load from blob storage instead.
4. **Remove `dbutils.secrets`** — already solved pattern (same as operational notebooks).

This is a meaningful refactor. Given that the weekly Databricks cost is ~$3–5/month,
the return on investment does not justify the work at this stage.
