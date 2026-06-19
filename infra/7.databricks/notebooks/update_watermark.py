# Databricks notebook: update_watermark
# Reads landing/{run_id}/index.json and advances the watermark.
# Must be the LAST step in the success path of the pipeline.
# Skipped (exits cleanly) in backfill mode (event_id widget is set).

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import sys, os, json, types
from datetime import datetime, timezone

def _load_from_dbfs(module_name, dbfs_path):
    content = dbutils.fs.head(dbfs_path, 500000)
    import types as _t
    mod = _t.ModuleType(module_name)
    mod.__file__ = dbfs_path
    sys.modules[module_name] = mod
    exec(compile(content, dbfs_path, "exec"), mod.__dict__)
    return mod

_src = "dbfs:/FileStore/cricket-pipeline/src"
landing_index = _load_from_dbfs("landing_index", f"{_src}/landing_index.py")

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")

svc              = BlobServiceClient.from_connection_string(conn_str)
landing_container = svc.get_container_client("landing")

# COMMAND ----------

try:
    run_id = dbutils.widgets.get("run_id").strip()
except Exception:
    run_id = "manual-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

try:
    event_id_filter = dbutils.widgets.get("event_id").strip()
except Exception:
    event_id_filter = ""

print(f"[update_watermark] run_id={run_id}  event_id_filter={event_id_filter or '(none)'}")

# COMMAND ----------

if event_id_filter:
    print("[update_watermark] Backfill mode — watermark NOT updated.")
else:
    landing_index.update_watermark_from_index(landing_container, run_id)
    print("[update_watermark] Done.")
