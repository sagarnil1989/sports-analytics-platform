# Databricks notebook: gold — prematch page builder
# Called by ADF pl_build_prematch_pages (runs daily at 07:00 UTC).
# Reads bronze/betsapi/prematch_snapshot/ and writes gold/cricket/prematch/.
# No input parameters — always processes all recent bronze prematch snapshots.

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

from prematch_page_builder import gold_build_prematch_pages

gold_build_prematch_pages()
