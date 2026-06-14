# Databricks notebook: analysis_ended_match_table
# Lists all ended matches available in bronze event_final, with event_id, match name,
# date, league, and score. Use this to pick specific event_ids for manual testing
# before running pl_backfill on all events.
#
# Usage:
#   Run with no parameters to see all ended matches.
#   Filter by league_name or match_name using the widgets below.
#
# To manually test a specific event:
#   1. Note the event_id from the table
#   2. Run pl_backfill with that event_id to process bronze → silver → gold
#   3. Check the output at gold/event_id={id}/innings_tracker.json

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

dbutils.widgets.text("filter_league", "", "Filter by league name (substring, case-insensitive)")
dbutils.widgets.text("filter_team",   "", "Filter by team name (substring, case-insensitive)")
dbutils.widgets.text("filter_date",   "", "Filter by date prefix (e.g. 2026-05)")
dbutils.widgets.text("limit",         "200", "Max rows to show")

filter_league = dbutils.widgets.get("filter_league").strip().lower()
filter_team   = dbutils.widgets.get("filter_team").strip().lower()
filter_date   = dbutils.widgets.get("filter_date").strip()
try:
    row_limit = int(dbutils.widgets.get("limit").strip())
except Exception:
    row_limit = 200

# COMMAND ----------

import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
sport_id = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
silver = svc.get_container_client("silver")

def _dl(container, path):
    try:
        return json.loads(container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

def _detect_format(match_name="", league_name="", extra_length=None, max_over=0, score_ss=""):
    combined = f"{match_name} {league_name}".lower()
    if "t20" in combined or "twenty20" in combined:
        return "T20"
    if "odi" in combined or "one day" in combined:
        return "ODI"
    try:
        length = int(str(extra_length))
        if length == 20: return "T20"
        if length == 50: return "ODI"
    except (TypeError, ValueError):
        pass
    if max_over > 0:
        return "T20" if max_over <= 20 else "ODI"
    if score_ss:
        import re as _re
        overs_in_ss = [float(m) for m in _re.findall(r'\((\d+(?:\.\d+)?)\)', score_ss)]
        if overs_in_ss:
            return "T20" if max(overs_in_ss) <= 20 else "ODI"
    return ""

# COMMAND ----------
# ── 1. Scan bronze event_final for all ended event_ids ───────────────────────

print("Scanning bronze/betsapi/event_final/...")
event_final_blobs = []
for blob in bronze.list_blobs(name_starts_with="betsapi/event_final/event_id="):
    if blob.name.endswith("/event_view.json"):
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if eid_part:
            event_final_blobs.append({
                "event_id": eid_part[9:],
                "path": blob.name,
                "last_modified": blob.last_modified,
            })

print(f"Found {len(event_final_blobs)} event_final files")

# ── Also collect silver complete markers (shows which are already processed) ──
silver_complete = set()
for blob in silver.list_blobs(name_starts_with="control/complete/"):
    fname = blob.name.rsplit("/", 1)[-1]
    if fname.startswith("event_id=") and fname.endswith(".json"):
        silver_complete.add(fname[9:-5])

print(f"Silver complete markers: {len(silver_complete)}")

# COMMAND ----------
# ── 2. Load event_view payloads in parallel ───────────────────────────────────

def _load_event(item):
    raw = _dl(bronze, item["path"])
    if not raw:
        return None
    # File format: {"response": {"body": {"results": [...]}}}
    body = (raw.get("response") or {}).get("body") or {}
    results = body.get("results") or raw.get("results") or []
    ev = results[0] if results else {}
    home = (ev.get("home") or {}).get("name") or ""
    away = (ev.get("away") or {}).get("name") or ""
    league = (ev.get("league") or {}).get("name") or ""
    score = str(ev.get("ss") or "").replace("-", ",")
    time_unix = ev.get("time")
    try:
        date_utc = datetime.fromtimestamp(int(time_unix), tz=timezone.utc).strftime("%Y-%m-%d") if time_unix else ""
    except Exception:
        date_utc = ""
    extra = ev.get("extra") or {}
    format_str = _detect_format(
        match_name=f"{home} vs {away}",
        league_name=league,
        extra_length=extra.get("length"),
        score_ss=score,
    )
    return {
        "event_id":   item["event_id"],
        "match_name": f"{home} vs {away}" if home and away else "",
        "home_team":  home,
        "away_team":  away,
        "league":     league,
        "score":      score,
        "date_utc":   date_utc,
        "format":     format_str,
        "silver_done": item["event_id"] in silver_complete,
    }

print(f"Loading {len(event_final_blobs)} event_view files...")
records = []
with ThreadPoolExecutor(max_workers=64) as ex:
    futs = {ex.submit(_load_event, b): b for b in event_final_blobs}
    for fut in as_completed(futs):
        r = fut.result()
        if r:
            records.append(r)

records.sort(key=lambda r: r["date_utc"], reverse=True)
print(f"Loaded {len(records)} matches")

# COMMAND ----------
# ── 3. Apply filters ──────────────────────────────────────────────────────────

filtered = records
if filter_league:
    filtered = [r for r in filtered if filter_league in r["league"].lower()]
if filter_team:
    filtered = [r for r in filtered
                if filter_team in r["home_team"].lower() or filter_team in r["away_team"].lower()]
if filter_date:
    filtered = [r for r in filtered if r["date_utc"].startswith(filter_date)]

filtered = filtered[:row_limit]
print(f"Displaying {len(filtered)} matches (limit={row_limit})")

# COMMAND ----------
# ── 4. Display table ──────────────────────────────────────────────────────────

import pandas as pd

if filtered:
    df = pd.DataFrame([{
        "event_id":    r["event_id"],
        "date":        r["date_utc"],
        "format":      r["format"],
        "match_name":  r["match_name"],
        "league":      r["league"],
        "score":       r["score"],
        "silver_done": str(r["silver_done"]),
    } for r in filtered])
    display(spark.createDataFrame(df))
else:
    print("No matches found with the given filters.")

# COMMAND ----------
# ── 5. Summary stats ──────────────────────────────────────────────────────────

total = len(records)
done  = sum(1 for r in records if r["silver_done"])
t20   = sum(1 for r in records if r["format"] == "T20")
odi   = sum(1 for r in records if r["format"] == "ODI")

print(f"\n── Summary ──")
print(f"  Total ended matches in bronze : {total}")
print(f"  Already in silver (complete)  : {done}")
print(f"  Pending silver processing     : {total - done}")
print(f"  T20 matches                   : {t20}")
print(f"  ODI matches                   : {odi}")
print(f"\nTo process a specific match:")
print(f"  Run pl_backfill with event_id=<id>")
print(f"  Then check gold/event_id=<id>/innings_tracker.json")
