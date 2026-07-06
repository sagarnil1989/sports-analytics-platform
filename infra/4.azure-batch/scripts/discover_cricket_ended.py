"""
Azure Batch script: discover_cricket_ended
Scans gold for innings_tracker.json files and writes the ended match index to bronze.

Environment variables set by ADF Custom activity extendedProperties:
  KEY_VAULT_URI  — e.g. https://kv-ramanuj.vault.azure.net/

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
  BET365-API-TOKEN
"""

import os, sys, json, time
import requests as _requests
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_activity_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "activity.json")
if os.path.exists(_activity_json_path):
    with open(_activity_json_path) as _f:
        _ext = json.load(_f).get("typeProperties", {}).get("extendedProperties", {})
    for _k, _v in _ext.items():
        os.environ.setdefault(_k, str(_v))

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

_kv_uri    = os.environ["KEY_VAULT_URI"]
_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
_cred      = ManagedIdentityCredential(client_id=_client_id)
_kv        = SecretClient(vault_url=_kv_uri, credential=_cred)

conn_str      = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
sport_id      = _kv.get_secret("SPORT-ID").value
betsapi_token = _kv.get_secret("BET365-API-TOKEN").value

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
gold   = svc.get_container_client("gold")

now              = datetime.now(timezone.utc)
script_start_utc = now
run_start        = time.monotonic()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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

def _detect_format(match_name="", league_name="", extra_length=None, max_over=0, score_ss=""):
    combined = f"{match_name} {league_name}".lower()
    # Test must come before ODI/T20 — "test match" contains neither "t20" nor "odi"
    if "test" in combined or "test match" in combined:
        return "Test"
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
    if max_over > 50:
        return "Test"
    if max_over > 0:
        return "T20" if max_over <= 20 else "ODI"
    if score_ss:
        import re as _re
        overs_in_ss = [float(m) for m in _re.findall(r'\((\d+(?:\.\d+)?)\)', score_ss)]
        if overs_in_ss:
            if max(overs_in_ss) > 50: return "Test"
            return "T20" if max(overs_in_ss) <= 20 else "ODI"
        # Test scores often have 4 comma-separated parts (4 innings)
        parts = [p.strip() for p in score_ss.replace("-", ",").split(",") if p.strip()]
        if len(parts) >= 3:
            return "Test"
    return ""

def _refresh_event_final(event_id) -> tuple:
    """
    Always call /v1/event/view for event_id and overwrite the event_final blob.
    Returns (ss, wrote) where ss is the score string and wrote is True if the
    blob was updated.  Falls back to whatever is already in the blob on API error.
    """
    blob_path = f"betsapi/event_final/event_id={event_id}/event_view.json"
    try:
        r = _requests.get(
            "https://api.b365api.com/v1/event/view",
            params={"event_id": event_id, "token": betsapi_token},
            timeout=8,
        )
        payload = r.json()
        results = payload.get("results") or []
        if results and str(results[0].get("time_status") or "") == "3" and results[0].get("ss"):
            _ul(bronze, blob_path, payload)
            return results[0]["ss"], True
    except Exception as exc:
        print(f"  [WARN] event/view API failed for {event_id}: {exc}")
    # API failed or returned incomplete data — fall back to existing blob
    existing = _dl(bronze, blob_path)
    if existing:
        ef_body  = (existing.get("response") or {}).get("body") or {}
        results  = ef_body.get("results") or existing.get("results") or []
        if results:
            return results[0].get("ss") or None, False
    return None, False

# ---------------------------------------------------------------------------
# 1. Blocked events
# ---------------------------------------------------------------------------

blocked_doc    = _dl(gold, "cricket/config/blocked_event_ids.json") or {}
blocked_events = {str(e) for e in blocked_doc.get("blocked_event_ids", [])}
print(f"Blocked events: {len(blocked_events)}")

# ---------------------------------------------------------------------------
# 2. Currently-live event IDs (exclude from ended)
# ---------------------------------------------------------------------------

live_eids = set()
live_idx  = _dl(gold, "cricket/matches/latest/index.json") or {}
for m in (live_idx.get("matches") or []):
    eid = str(m.get("event_id") or "")
    if eid:
        live_eids.add(eid)
print(f"Live events (excluded): {len(live_eids)}")

# ---------------------------------------------------------------------------
# 3. Collect all event_ids with innings_tracker.json in gold
# ---------------------------------------------------------------------------

silver_eids = {}
for blob in gold.list_blobs(name_starts_with="event_id="):
    if not blob.name.endswith("/innings_tracker.json"):
        continue
    parts   = blob.name.split("/")
    eid_part = next((p for p in parts if p.startswith("event_id=")), None)
    if eid_part:
        silver_eids[eid_part[9:]] = blob.name

print(f"Gold tracker files found: {len(silver_eids)}")

# ---------------------------------------------------------------------------
# 4. FI lookup
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# 5. Build ended index
# ---------------------------------------------------------------------------

matches = []
skipped_live = skipped_blocked = skipped_no_fi = skipped_no_name = score_patched = 0

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

    # Always refresh event_final from BetsAPI — the match is over (snapshot >1hr old)
    # so this is the guaranteed authoritative fetch. Overwrites any stale blob.
    time.sleep(0.15)
    score, wrote = _refresh_event_final(eid)
    if score:
        score_patched += 1

    if not score:
        score = (
            tracker.get("score_summary_events")
            or tracker.get("score_summary_bet365")
            or tracker.get("score_summary")
            or ""
        )

    score = score.replace("-", ",") if score else score

    rows    = tracker.get("rows") or []
    inn1_bat = None
    for r in rows:
        if r.get("innings") == 1 and r.get("batting_team"):
            inn1_bat = str(r["batting_team"]).strip()
            break
    if inn1_bat and away_name and inn1_bat == away_name.strip():
        if score and "," in score:
            p = score.split(",", 1)
            score = f"{p[1].strip()},{p[0].strip()}"
        swapped    = match_name.replace(f"{home_name} vs {away_name}", f"{away_name} vs {home_name}", 1)
        match_name = swapped if swapped != match_name else f"{away_name} vs {home_name}"

    ef_extra_length = None
    if event_final_data:
        ef_body2    = (event_final_data.get("response") or {}).get("body") or {}
        ef_results2 = ef_body2.get("results") or event_final_data.get("results") or []
        if ef_results2:
            ef_extra_length = ef_results2[0].get("extra", {}).get("length")
    max_over = 0
    for r in rows:
        try:
            max_over = max(max_over, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    match_format = _detect_format(
        match_name=match_name,
        league_name=str(tracker.get("league_name") or ""),
        extra_length=ef_extra_length,
        max_over=max_over,
        score_ss=score or "",
    )

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

elapsed             = time.monotonic() - run_start
script_finished_utc = datetime.now(timezone.utc)

print(f"\n── Done ──")
print(f"  Ended matches written    : {len(matches)}")
print(f"  Scores from event_final  : {score_patched}")
print(f"  Skipped (live)           : {skipped_live}")
print(f"  Skipped (blocked)        : {skipped_blocked}")
print(f"  Skipped (no fi)          : {skipped_no_fi}")
print(f"  Skipped (no name)        : {skipped_no_name}")

try:
    log_date = script_start_utc.strftime("%Y%m%d")
    log_time = script_start_utc.strftime("%H%M%S")
    log_path = f"logs/pl_build_ended_match/{log_date}/{log_time}_discover_cricket_ended.json"
    run_log  = {
        "script":                    "discover_cricket_ended",
        "run_date":                  script_start_utc.strftime("%Y-%m-%d"),
        "started_at_utc":            script_start_utc.isoformat(),
        "finished_at_utc":           script_finished_utc.isoformat(),
        "duration_seconds":          round(elapsed, 2),
        "status":                    "ok",
        "ended_matches_written":     len(matches),
        "scores_from_event_final":   score_patched,
        "skipped_live":              skipped_live,
        "skipped_blocked":           skipped_blocked,
        "skipped_no_fi":             skipped_no_fi,
        "skipped_no_name":           skipped_no_name,
    }
    _ul(gold, log_path, run_log)
    print(f"\n  run log written: {log_path}")
except Exception as log_ex:
    print(f"\n  [log write failed — non-fatal]: {log_ex}")
