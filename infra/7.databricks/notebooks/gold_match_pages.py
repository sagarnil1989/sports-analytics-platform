# Databricks notebook: gold match pages pipeline
# Triggered by ADF pl_gold_match_pages every 1 hour.
# Loops internally every 30 seconds for 55 minutes so a single cluster start
# amortises the cost across ~110 gold builds before ADF restarts it.

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "azure-storage-file-datalake", "requests", "azure-functions"
], check=True)

# COMMAND ----------

import sys, os, time

sys.path.insert(0, "/dbfs/FileStore/cricket-pipeline/src/")

os.environ["DATA_STORAGE_CONNECTION_STRING"] = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
os.environ["SPORT_ID"]                       = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

# COMMAND ----------

from gold import gold_build_match_pages

# Run for 55 minutes then exit cleanly; ADF triggers the next 1-hour run.
RUN_DURATION_SECONDS = 55 * 60
INTERVAL_SECONDS     = 30

run_start = time.monotonic()
iterations = 0

while time.monotonic() - run_start < RUN_DURATION_SECONDS:
    loop_start = time.monotonic()
    gold_build_match_pages()
    iterations += 1
    elapsed = time.monotonic() - loop_start
    sleep_for = max(0, INTERVAL_SECONDS - elapsed)
    if time.monotonic() - run_start + sleep_for >= RUN_DURATION_SECONDS:
        break
    time.sleep(sleep_for)

print(f"gold_match_pages loop finished: {iterations} iterations in {round(time.monotonic() - run_start)}s")
