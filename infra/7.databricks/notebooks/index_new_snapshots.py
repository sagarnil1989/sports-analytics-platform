# Databricks notebook: index_new_snapshots
# Scans bronze for new manifests since the last watermark and writes
# a per-run landing index to landing/{run_id}/index.json.
# Activity 0 in pl_build_ended_match_databricks pipeline.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import sys, os, json, time, types
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
sport_id = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

svc     = BlobServiceClient.from_connection_string(conn_str)
bronze  = svc.get_container_client("bronze")
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

print(f"[index_new_snapshots] run_id={run_id}  event_id_filter={event_id_filter or '(all)'}")

# COMMAND ----------

script_start_utc = datetime.now(timezone.utc)

scan_start_utc, new_entries, blobs_scanned = landing_index.scan_bronze_to_landing(
    bronze, landing_container, sport_id, run_id, event_id_filter=event_id_filter or None
)

# Watermark is NOT updated here. The update_watermark notebook runs last in the pipeline.
# On failure, cleanup_landing deletes the index so the next run re-scans from the unchanged watermark.
print(f"[index_new_snapshots] Landing index written — watermark update deferred to update_watermark step")

elapsed = (datetime.now(timezone.utc) - script_start_utc).total_seconds()
print(f"\n── Done ── {elapsed:.1f}s  |  {blobs_scanned} blobs scanned  |  {new_entries} entries written")
