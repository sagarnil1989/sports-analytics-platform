# Databricks notebook: discover_cricket_ended
# Replaces the Function App timer of the same name — no 10-minute timeout here.
# Scans gold for innings_1_from_silver.json files and writes the ended match index to bronze.
# Schedule: hourly via ADF trigger pl_discover_cricket_ended.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import json, sys
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
sport_id = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
gold   = svc.get_container_client("gold")

now = datetime.now(timezone.utc)

def _dl(container, path):
    try:
        return json.loads(container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

def _ul(container, path, data):
    container.get_blob_client(path).upload_blob(
        json.dumps(data, ensure_ascii=False, indent=2).encode(),
        overwrite=True,
    )

# COMMAND ----------
# ── 1. Blocked events ────────────────────────────────────────────────────────

blocked_events = set()
blocked_doc = _dl(gold, "cricket/config/blocked_event_ids.json") or {}
blocked_events = {str(e) for e in blocked_doc.get("blocked_event_ids", [])}
print(f"Blocked events: {len(blocked_events)}")

# COMMAND ----------
# ── 2. Currently-live event IDs (exclude from ended) ─────────────────────────

live_eids = set()
live_idx = _dl(gold, "cricket/matches/latest/index.json") or {}
for m in (live_idx.get("matches") or []):
    eid = str(m.get("event_id") or "")
    if eid:
        live_eids.add(eid)
print(f"Live events (excluded): {len(live_eids)}")

# COMMAND ----------
# ── 3. Collect all event_ids with innings_1_from_silver.json in gold ─────────

silver_eids = {}  # eid -> blob_name
for blob in gold.list_blobs(name_starts_with="cricket/innings_tracker/event_id="):
    if not blob.name.endswith("innings_1_from_silver.json"):
        continue
    parts = blob.name.split("/")
    eid_part = next((p for p in parts if p.startswith("event_id=")), None)
    if eid_part:
        silver_eids[eid_part.replace("event_id=", "")] = blob.name

print(f"Gold tracker files found: {len(silver_eids)}")

# COMMAND ----------
# ── 4. FI lookup — one bronze listing per event_id ───────────────────────────

fi_lookup = {}
for eid in silver_eids:
    for prefix in (
        f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={eid}/",
        f"betsapi/prematch_snapshot/sport_id={sport_id}/event_id={eid}/",
    ):
        for blob in bronze.list_blobs(name_starts_with=prefix):
            fi_part = next((p for p in blob.name.split("/") if p.startswith("fi=")), None)
            if fi_part:
                fi_lookup[eid] = fi_part.replace("fi=", "")
                break
        if eid in fi_lookup:
            break

print(f"FI resolved: {len(fi_lookup)} / {len(silver_eids)}")

# COMMAND ----------
# ── 5. Build ended index ──────────────────────────────────────────────────────

matches = []
skipped_live = skipped_blocked = skipped_no_fi = skipped_no_name = 0

for eid, tracker_blob_name in silver_eids.items():
    if eid in blocked_events:
        skipped_blocked += 1
        continue
    if eid in live_eids:
        skipped_live += 1
        continue

    tracker = _dl(gold, tracker_blob_name)
    if not tracker:
        continue

    league_id  = str(tracker.get("league_id") or "")
    home_name  = str(tracker.get("home_team_name") or "")
    away_name  = str(tracker.get("away_team_name") or "")
    match_name = tracker.get("match_name") or f"{home_name} vs {away_name}"
    fi         = fi_lookup.get(eid)

    if not match_name:
        skipped_no_name += 1
        print(f"  SKIP no match_name: event_id={eid}")
        continue
    if not fi:
        skipped_no_fi += 1
        print(f"  SKIP no fi: event_id={eid}")
        continue

    score = (
        tracker.get("score_summary_events")
        or tracker.get("score_summary_bet365")
        or tracker.get("score_summary")
        or ""
    )
    score = score.replace("-", ",") if score else score

    # Order by 1st-innings batting team
    rows = tracker.get("rows") or []
    inn1_bat = None
    for r in rows:
        if r.get("innings") == 1 and r.get("batting_team"):
            inn1_bat = str(r["batting_team"]).strip()
            break
    if inn1_bat and away_name and inn1_bat == away_name.strip():
        if score and "," in score:
            p = score.split(",", 1)
            score = f"{p[1].strip()},{p[0].strip()}"
        match_name = f"{away_name} vs {home_name}"

    # Detect format from the highest over seen across all rows
    max_over = 0
    for r in rows:
        try:
            max_over = max(max_over, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    match_format = "T20" if max_over <= 20 else "ODI"

    matches.append({
        "event_id":       eid,
        "fi":             fi,
        "league_id":      league_id,
        "league_name":    tracker.get("league_name"),
        "home_team_name": home_name,
        "away_team_name": away_name,
        "match_name":     match_name,
        "score":          score,
        "event_time_utc": tracker.get("match_date_utc"),
        "time_status":    "3",
        "format":         match_format,
    })

matches.sort(key=lambda m: str(m.get("event_time_utc") or ""), reverse=True)

ended_index = {
    "generated_at_utc":  now.isoformat(),
    "sport_id":          sport_id,
    "ended_match_count": len(matches),
    "matches":           matches,
}
_ul(bronze, "cricket/ended/latest/index.json", ended_index)

print(f"\n── Done ──")
print(f"  Ended matches written : {len(matches)}")
print(f"  Skipped (live)        : {skipped_live}")
print(f"  Skipped (blocked)     : {skipped_blocked}")
print(f"  Skipped (no fi)       : {skipped_no_fi}")
print(f"  Skipped (no name)     : {skipped_no_name}")
