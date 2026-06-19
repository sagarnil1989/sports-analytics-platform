# Databricks notebook: cleanup_landing
# Deletes landing/{run_id}/index.json on pipeline failure so the next run
# re-scans from the unchanged watermark.
# ADF triggers this with dependencyConditions = ["Failed", "Skipped"] on update_watermark.

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

print(f"[cleanup_landing] run_id={run_id}")

# COMMAND ----------

landing_index.delete_landing_index(landing_container, run_id)
print("[cleanup_landing] Done.")
