# Databricks notebook: gold — ended match innings builder
# Called by ADF pl_silver_build_ended_match on success (ExecutePipeline activity).
# Reads from silver only — no bronze access.
# Rebuilds innings_1_from_silver.json for every event where silver has data
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

from gold_rebuild import gold_rebuild_ended_matches
gold_rebuild_ended_matches()
