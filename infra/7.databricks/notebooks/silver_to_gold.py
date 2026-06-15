# Databricks notebook: silver_to_gold
# Reads silver innings data and rebuilds gold innings tracker files.
# Used by two ADF pipelines:
#   pl_build_ended_match (daily 02:00 CET) — no event_id, rebuilds all stale matches
#   pl_backfill (manual)                   — pass event_id for one match, or empty for all stale matches

# COMMAND ----------

dbutils.widgets.text("event_id", "")

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "azure-storage-file-datalake", "requests", "azure-functions"
], check=True)

# COMMAND ----------

import sys, os

sys.path.insert(0, "/dbfs/FileStore/cricket-pipeline/src/")

os.environ["DATA_STORAGE_CONNECTION_STRING"] = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
os.environ["SPORT_ID"]                       = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

# COMMAND ----------

try:
    event_id = dbutils.widgets.get("event_id").strip() or None
except Exception:
    event_id = None

print(f"[silver_to_gold] event_id={'ALL stale matches' if not event_id else event_id}")

# COMMAND ----------

from gold_rebuild import gold_rebuild_ended_matches
gold_rebuild_ended_matches(event_id=event_id)
