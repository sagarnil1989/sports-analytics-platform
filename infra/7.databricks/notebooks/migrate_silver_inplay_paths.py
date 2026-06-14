# Databricks notebook: migrate_silver_inplay_paths
# One-off migration: moves silver inplay snapshot blobs from time-partitioned paths to
# flat event_id paths.
#
# Old: silver/cricket/inplay/year=YYYY/month=MM/day=DD/hour=HH/event_id={id}/snapshot_id={id}/...
# New: silver/cricket/inplay/event_id={id}/snapshot_id={id}/...
#
# Safe to re-run — copies before deleting, skips blobs already at the new path.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc    = BlobServiceClient.from_connection_string(conn_str)
silver = svc.get_container_client("silver")

PARALLEL_WORKERS = 64

# COMMAND ----------
# ── STEP 1: Scan old-format blobs ───────────────────────────────────────────

print("Scanning silver/cricket/inplay/year= ...")
t0 = time.monotonic()

blobs_to_migrate = []  # (old_path, new_path)

for blob in silver.list_blobs(name_starts_with="cricket/inplay/year="):
    name = blob.name
    parts = name.split("/")

    event_part    = next((p for p in parts if p.startswith("event_id=")),   None)
    snapshot_part = next((p for p in parts if p.startswith("snapshot_id=")), None)
    filename      = parts[-1]

    if not event_part or not snapshot_part or not filename:
        continue

    new_path = f"cricket/inplay/{event_part}/{snapshot_part}/{filename}"
    blobs_to_migrate.append((name, new_path))

print(f"  {len(blobs_to_migrate)} blobs to migrate  ({time.monotonic()-t0:.1f}s)")

# COMMAND ----------
# ── STEP 2: Copy to new paths ────────────────────────────────────────────────

def copy_blob(item):
    old_path, new_path = item
    source_url = silver.get_blob_client(old_path).url
    dest = silver.get_blob_client(new_path)
    dest.start_copy_from_url(source_url)
    return old_path, new_path

print(f"\nCopying {len(blobs_to_migrate)} blobs with {PARALLEL_WORKERS} workers...")
t0 = time.monotonic()
copied = 0
copy_errors = []

with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
    futures = {pool.submit(copy_blob, item): item for item in blobs_to_migrate}
    for future in as_completed(futures):
        try:
            future.result()
            copied += 1
            if copied % 1000 == 0 or copied == len(blobs_to_migrate):
                elapsed = time.monotonic() - t0
                rate = copied / elapsed if elapsed > 0 else 0
                print(f"  {copied}/{len(blobs_to_migrate)}  |  {rate:.0f}/s")
        except Exception as ex:
            copy_errors.append((futures[future][0], str(ex)))

print(f"\n  copied : {copied}")
print(f"  errors : {len(copy_errors)}")
if copy_errors:
    for old, err in copy_errors[:10]:
        print(f"    {old}  →  {err}")

# COMMAND ----------
# ── STEP 3: Delete old blobs (only if copy had no errors) ────────────────────

if copy_errors:
    print("\nSkipping delete — copy had errors. Fix and re-run.")
    dbutils.notebook.exit("aborted: copy errors, old blobs retained")

def delete_blob(path):
    silver.get_blob_client(path).delete_blob()
    return path

old_paths = [old for old, _ in blobs_to_migrate]
print(f"\nDeleting {len(old_paths)} old blobs with {PARALLEL_WORKERS} workers...")
t0 = time.monotonic()
deleted = 0
delete_errors = []

with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
    futures = {pool.submit(delete_blob, p): p for p in old_paths}
    for future in as_completed(futures):
        try:
            future.result()
            deleted += 1
            if deleted % 1000 == 0 or deleted == len(old_paths):
                elapsed = time.monotonic() - t0
                rate = deleted / elapsed if elapsed > 0 else 0
                print(f"  {deleted}/{len(old_paths)}  |  {rate:.0f}/s")
        except Exception as ex:
            delete_errors.append((futures[future], str(ex)))

print(f"\n  deleted : {deleted}")
print(f"  errors  : {len(delete_errors)}")
if delete_errors:
    for path, err in delete_errors[:10]:
        print(f"    {path}  →  {err}")

print("\nMigration complete.")
