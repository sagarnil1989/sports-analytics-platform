# Databricks notebook: bronze vs silver snapshot counts per match
# Self-contained — uses azure-storage-blob directly, no pipeline source deps.
# Run manually in the workspace to see which matches have unprocessed bronze snapshots.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import os, json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
sport_id = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

svc      = BlobServiceClient.from_connection_string(conn_str)
bronze   = svc.get_container_client("bronze")
silver   = svc.get_container_client("silver")

# COMMAND ----------
# ── 1. Scan bronze — count manifests and collect fi per event_id ─────────────

print("Scanning bronze...")

bronze_counts  = defaultdict(int)
fi_lookup      = {}
manifest_paths = defaultdict(list)   # event_id -> [manifest blob path]

for blob in bronze.list_blobs(name_starts_with=f"betsapi/inplay_snapshot/sport_id={sport_id}/"):
    if not blob.name.endswith("/manifest.json"):
        continue
    parts    = blob.name.split("/")
    eid_part = next((p for p in parts if p.startswith("event_id=")), None)
    fi_part  = next((p for p in parts if p.startswith("fi=")), None)
    if not eid_part:
        continue
    eid = eid_part[9:]
    bronze_counts[eid] += 1
    manifest_paths[eid].append(blob.name)
    if fi_part and eid not in fi_lookup:
        fi_lookup[eid] = fi_part[3:]

print(f"Found {len(bronze_counts)} events in bronze")

# COMMAND ----------
# ── 2. Scan silver — count match_state.json per event_id ────────────────────

print("Scanning silver...")

silver_counts    = defaultdict(int)
silver_blobs_map = defaultdict(list)   # event_id -> [blob]

for blob in silver.list_blobs(name_starts_with="event_id="):
    if not blob.name.endswith("/match_state.json"):
        continue
    parts    = blob.name.split("/")
    eid_part = next((p for p in parts if p.startswith("event_id=")), None)
    if not eid_part:
        continue
    eid = eid_part[9:]
    silver_counts[eid] += 1
    silver_blobs_map[eid].append(blob)

print(f"Found {len(silver_counts)} events in silver")

# COMMAND ----------
# ── 3. Count bronze error snapshots (parallel manifest downloads) ─────────────
# A snapshot is counted as an error if event_view or bet365_event API failed.

print("Counting bronze API errors (parallel manifest download)...")

bronze_errors = defaultdict(int)

def _check_manifest(eid_path):
    eid, path = eid_path
    try:
        data = json.loads(bronze.get_blob_client(path).download_blob().readall())
        status = data.get("status") or {}
        has_error = (
            not status.get("event_view_success", True)
            or not status.get("bet365_event_success", True)
        )
        return eid, has_error
    except Exception:
        return eid, False   # unreadable manifest — don't count as error

all_manifest_items = [(eid, path) for eid, paths in manifest_paths.items() for path in paths]
total = len(all_manifest_items)
done  = 0

with ThreadPoolExecutor(max_workers=64) as pool:
    futures = {pool.submit(_check_manifest, item): item for item in all_manifest_items}
    for future in as_completed(futures):
        eid, has_error = future.result()
        if has_error:
            bronze_errors[eid] += 1
        done += 1
        if done % 500 == 0 or done == total:
            print(f"  {done}/{total} manifests checked")

print(f"Error manifest check complete")

# COMMAND ----------
# ── 4. Get match name from latest silver match_state.json per event ──────────

print("Fetching match names...")

match_names  = {}
league_ids   = {}
start_dates  = {}

for eid, blobs in silver_blobs_map.items():
    latest = max(blobs, key=lambda b: b.last_modified)
    try:
        data = json.loads(silver.get_blob_client(latest.name).download_blob().readall())
        home = data.get("home_team_name") or data.get("home_name") or ""
        away = data.get("away_team_name") or data.get("away_name") or ""
        match_names[eid] = data.get("match_name") or (f"{home} vs {away}" if home else "—")
        league_ids[eid]  = str(data.get("league_id") or "—")
        raw_ts           = data.get("event_time_utc") or ""
        start_dates[eid] = raw_ts[:10] if raw_ts else "—"   # YYYY-MM-DD
    except Exception:
        pass

print(f"Resolved {len(match_names)} match names")

# COMMAND ----------
# ── 5. Display results ───────────────────────────────────────────────────────

import pandas as pd

all_eids = sorted(
    set(list(bronze_counts.keys()) + list(silver_counts.keys())),
    key=lambda e: bronze_counts.get(e, 0),
    reverse=True
)

rows = []
for eid in all_eids:
    b      = bronze_counts.get(eid, 0)
    s      = silver_counts.get(eid, 0)
    errors = bronze_errors.get(eid, 0)
    rows.append({
        "event_id":         eid,
        "bet365_id":        fi_lookup.get(eid, "—"),
        "league_id":        league_ids.get(eid, "—"),
        "start_date":       start_dates.get(eid, "—"),
        "match_name":       match_names.get(eid, "—"),
        "bronze_snapshots": b,
        "bronze_errors":    errors,
        "silver_snapshots": s,
        "unprocessed":      b - s,
    })

df = pd.DataFrame(rows)
display(df)
