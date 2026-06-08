# Databricks notebook: silver — ended match processing
# Triggered by ADF pl_silver_build_ended_match once daily at 02:00 CET.
# Only processes bronze snapshots for matches that have received no new
# snapshot in the last 60 minutes — i.e. ended or inactive matches.
# Active live matches are never touched by this pipeline.

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

from snapshot_parser import silver_parse_ended_matches
silver_parse_ended_matches(quiet_threshold_minutes=60)
