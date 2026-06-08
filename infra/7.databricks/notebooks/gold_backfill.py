# Databricks notebook: gold backfill
# Manual trigger only via ADF pl_gold_backfill.
# Pass event_id to rebuild innings tracker for one specific match.
# Leave event_id empty to rebuild all matches where silver has data
# newer than the existing gold file.

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

event_id = dbutils.widgets.get("event_id").strip() if dbutils.widgets.getAll().get("event_id") else ""
print(f"[gold_backfill] event_id={'ALL stale matches' if not event_id else event_id}")

# COMMAND ----------

from gold_rebuild import gold_rebuild_ended_matches
gold_rebuild_ended_matches(event_id=event_id or None)
