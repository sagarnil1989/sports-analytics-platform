# Databricks notebook: delete_processed_markers
# Replaces update_watermark. After a successful pipeline run, deletes pending
# markers for all events processed in this run and writes last-successful-run.json.
# Skipped (exits cleanly) in backfill mode (event_id widget is set).

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import sys, json, types
from datetime import datetime, timezone

def _load_from_dbfs(module_name, dbfs_path):
    content = dbutils.fs.head(dbfs_path, 500000)
    mod = types.ModuleType(module_name)
    mod.__file__ = dbfs_path
    sys.modules[module_name] = mod
    exec(compile(content, dbfs_path, "exec"), mod.__dict__)
    return mod

_src = "dbfs:/FileStore/cricket-pipeline/src"

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")

svc = BlobServiceClient.from_connection_string(conn_str)
pq  = svc.get_container_client("process-queue")

# COMMAND ----------

dbutils.widgets.text("run_id",   "")
dbutils.widgets.text("event_id", "")

try:
    run_id = dbutils.widgets.get("run_id").strip()
except Exception:
    run_id = ""
if not run_id:
    run_id = "manual-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

try:
    event_id_filter = dbutils.widgets.get("event_id").strip()
except Exception:
    event_id_filter = ""

print(f"[delete_processed_markers] run_id={run_id}  event_id_filter={event_id_filter or '(none)'}")

# COMMAND ----------

if event_id_filter:
    print("[delete_processed_markers] Backfill mode — marker cleanup skipped.")
    dbutils.notebook.exit("skipped: backfill mode")

_load_from_dbfs("process_queue", f"{_src}/process_queue.py")
from process_queue import read_in_progress, delete_pending_for_events, write_last_successful_run

events_dict   = read_in_progress(pq, run_id)
all_event_ids = list(events_dict.keys())
n_processed   = sum(len(v) for v in events_dict.values())

print(f"[delete_processed_markers] {len(all_event_ids)} events, {n_processed} snapshots processed this run")

n_deleted = delete_pending_for_events(pq, all_event_ids)
write_last_successful_run(pq, run_id, n_processed)

print(f"[delete_processed_markers] Done — {n_deleted} markers deleted")
dbutils.notebook.exit(json.dumps({
    "events_processed":  len(all_event_ids),
    "markers_deleted":   n_deleted,
}))
