import json
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

from util import (
    blob_exists,
    call_betsapi,
    download_json,
    extract_results,
    get_bronze_container_client,
    get_env,
    get_named_container_client,
    upload_json,
    utc_now,
)
from league_config import load_blocked_event_ids


def bronze_discover_cricket_ended() -> None:
    """Build the ended match index from gold innings_1_from_silver.json files.

    A match appears here if and only if:
      1. innings_1_from_silver.json exists in gold for that event_id
      2. The match is NOT currently live (not in the live matches index)
      3. The event is not in the admin block list

    League filtering is intentionally NOT applied here — if a match reached gold,
    it already passed the league filter at bronze capture time. Filtering again
    caused silent gaps when tracker metadata was null.

    No BetsAPI call is made — all data comes from gold/silver/bronze blobs.
    Score and match name are ordered by batting order (1st innings team first).
    """
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    bronze = get_bronze_container_client()
    gold = get_named_container_client("gold")

    blocked_events = load_blocked_event_ids()

    # ── 1. Get currently-live event IDs ──────────────────────────────────────
    live_eids: set = set()
    try:
        live_idx = download_json(gold, "cricket/matches/latest/index.json") or {}
        for m in (live_idx.get("matches") or []):
            eid = str(m.get("event_id") or "")
            if eid:
                live_eids.add(eid)
    except Exception:
        pass

    # ── 2. Collect all event_ids with innings_tracker.json in gold ───────────
    silver_eids: Dict[str, str] = {}  # eid -> blob_name
    for blob in gold.list_blobs(name_starts_with="event_id="):
        if not blob.name.endswith("/innings_tracker.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        silver_eids[eid_part[9:]] = blob.name

    # ── 3. FI lookup from bronze path scan ───────────────────────────────────
    fi_lookup: Dict[str, str] = {}
    for eid in silver_eids:
        for prefix in (
            f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={eid}/",
            f"betsapi/prematch_snapshot/sport_id={sport_id}/event_id={eid}/",
        ):
            for blob in bronze.list_blobs(name_starts_with=prefix):
                fi_part = next(
                    (p for p in blob.name.split("/") if p.startswith("fi=")), None
                )
                if fi_part:
                    fi_lookup[eid] = fi_part.replace("fi=", "")
                    break
            if eid in fi_lookup:
                break

    # ── 4. Build ended index ──────────────────────────────────────────────────
    matches: List[Dict[str, Any]] = []
    for eid, tracker_blob_name in silver_eids.items():
        if eid in blocked_events:
            continue
        if eid in live_eids:
            logging.info(json.dumps({"event": "ended_skip_live", "event_id": eid}))
            continue

        tracker = download_json(gold, tracker_blob_name)
        if not tracker:
            continue

        league_id = str(tracker.get("league_id") or "")
        home_name = str(tracker.get("home_team_name") or "")
        away_name = str(tracker.get("away_team_name") or "")
        match_name = tracker.get("match_name") or f"{home_name} vs {away_name}"
        fi = fi_lookup.get(eid)
        if not match_name:
            logging.info(json.dumps({"event": "ended_skip_no_match_name", "event_id": eid}))
            continue
        if not fi:
            logging.info(json.dumps({"event": "ended_skip_no_fi", "event_id": eid}))
            continue

        # Prefer event_final bronze (authoritative ss with overs) over snapshot-derived scores.
        score = None
        ef = download_json(bronze, f"betsapi/event_final/event_id={eid}/event_view.json")
        if ef:
            ef_body = (ef.get("response") or {}).get("body") or {}
            ef_results = ef_body.get("results") or ef.get("results") or []
            if ef_results:
                score = ef_results[0].get("ss") or None
        if not score:
            score = (
                tracker.get("score_summary_events")
                or tracker.get("score_summary_bet365")
                or tracker.get("score_summary")
                or ""
            )
        score = score.replace("-", ",") if score else score

        # Correct ordering: score_summary is home,away but display wants
        # 1st-innings team first. Detect via batting_team on innings==1 rows.
        rows = tracker.get("rows") or []
        inn1_bat = None
        for r in rows:
            if r.get("innings") == 1 and r.get("batting_team"):
                inn1_bat = str(r["batting_team"]).strip()
                break
        away_batted_first = bool(inn1_bat and away_name and inn1_bat == away_name.strip())
        if away_batted_first:
            if score and "," in score:
                p = score.split(",", 1)
                score = f"{p[1].strip()},{p[0].strip()}"
            swapped = match_name.replace(f"{home_name} vs {away_name}", f"{away_name} vs {home_name}", 1)
            match_name = swapped if swapped != match_name else f"{away_name} vs {home_name}"

        record: Dict[str, Any] = {
            "event_id": eid,
            "fi": fi,
            "league_id": league_id,
            "league_name": tracker.get("league_name"),
            "home_team_name": home_name,
            "away_team_name": away_name,
            "match_name": match_name,
            "score": score,
            "stadium": (tracker.get("stadium_data") or {}).get("name") or None,
            "event_time_utc": tracker.get("match_date_utc"),
            "time_status": "3",
        }
        matches.append(record)

    matches.sort(key=lambda m: str(m.get("event_time_utc") or ""), reverse=True)

    ended_index = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "ended_match_count": len(matches),
        "matches": matches,
    }
    upload_json(bronze, "cricket/ended/latest/index.json", ended_index, overwrite=True)

    logging.info(json.dumps({
        "event": "bronze_discover_cricket_ended_completed",
        "ended_match_count": len(matches),
        "silver_eid_count": len(silver_eids),
        "live_excluded": len(live_eids & set(silver_eids.keys())),
    }))


def _event_final_is_valid(payload: Optional[Dict]) -> bool:
    """Return True only if the stored event_final has time_status=3 AND a non-empty ss."""
    if not payload:
        return False
    ef_body   = (payload.get("response") or {}).get("body") or {}
    results   = ef_body.get("results") or payload.get("results") or []
    if not results:
        return False
    r = results[0]
    return str(r.get("time_status") or "") == "3" and bool(r.get("ss"))


def bronze_capture_ended_event_view() -> None:
    """Capture /v1/event/view for events whose latest inplay snapshot is > 1 hr old.

    Scans bronze inplay_snapshot for all tracked event_ids. For each candidate:
      - If no event_final blob exists → fetch and write.
      - If event_final exists but has empty ss or time_status != 3 (stale/wrong
        data written mid-match) → re-fetch and OVERWRITE with the correct score.
      - If event_final exists and is valid (time_status=3 + non-empty ss) → skip.

    This replaces the old simple idempotency check that let stale blobs persist
    permanently and blocked future corrections.
    """
    bronze = get_bronze_container_client()
    sport_id = get_env("SPORT_ID", "3")
    now = utc_now()
    cutoff = now - timedelta(hours=1)

    prefix = f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id="
    latest_ts: Dict[str, Any] = {}
    for blob in bronze.list_blobs(name_starts_with=prefix):
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        eid = eid_part[9:]
        if eid not in latest_ts or blob.last_modified > latest_ts[eid]:
            latest_ts[eid] = blob.last_modified

    candidates = [eid for eid, ts in latest_ts.items() if ts <= cutoff]

    logging.info(json.dumps({
        "event": "bronze_capture_ended_event_view_start",
        "total_tracked_events": len(latest_ts),
        "candidates_over_1hr": len(candidates),
    }))

    created = repaired = skipped = not_ended = failed = 0

    for eid in sorted(candidates):
        final_blob = f"betsapi/event_final/event_id={eid}/event_view.json"

        # Check existing blob validity — only skip if data is good
        existing = download_json(bronze, final_blob) if blob_exists(bronze, final_blob) else None
        if existing is not None and _event_final_is_valid(existing):
            skipped += 1
            continue

        # Existing blob is missing, empty, or has wrong data — (re-)fetch
        is_repair = existing is not None
        try:
            payload = call_betsapi("/v1/event/view", {"event_id": eid})
            results = extract_results(payload)
            result = results[0] if results else {}
            time_status = str(result.get("time_status") or "")
            ss = result.get("ss") or ""

            if not results or time_status != "3":
                not_ended += 1
                continue

            upload_json(bronze, final_blob, payload, overwrite=True)
            if is_repair:
                repaired += 1
                logging.warning(json.dumps({
                    "event": "bronze_event_final_repaired",
                    "event_id": eid, "ss": ss,
                }))
            else:
                created += 1
                logging.info(json.dumps({
                    "event": "bronze_event_final_created",
                    "event_id": eid, "ss": ss,
                }))

        except Exception:
            failed += 1
            logging.exception(json.dumps({
                "event": "bronze_event_final_failed", "event_id": eid,
            }))

    logging.warning(json.dumps({
        "event": "bronze_capture_ended_event_view_done",
        "created": created,
        "repaired_stale": repaired,
        "skipped_valid": skipped,
        "not_ended_or_not_found": not_ended,
        "failed": failed,
    }))

    bronze_discover_cricket_ended()


def bronze_repair_event_finals() -> Dict[str, Any]:
    """One-time (and on-demand) repair of ALL existing event_final blobs.

    Scans the entire bronze/betsapi/event_final/ prefix — not just recent
    inplay_snapshot candidates — so older matches with stale blobs are also
    corrected.  For each blob:
      - If valid (time_status=3 + non-empty ss) → skip.
      - If invalid → re-fetch /v1/event/view; overwrite only if the API now
        returns time_status=3 with a non-empty ss.

    Returns a summary dict for the admin HTTP response.
    Calls bronze_discover_cricket_ended() at the end to rebuild the ended index.
    """
    import time as _time

    bronze   = get_bronze_container_client()
    prefix   = "betsapi/event_final/event_id="

    total = skipped = repaired = api_not_ended = failed = 0
    repaired_list: List[str] = []

    for blob in bronze.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith("/event_view.json"):
            continue
        parts    = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        eid        = eid_part[9:]
        final_blob = f"betsapi/event_final/event_id={eid}/event_view.json"
        total += 1

        existing = download_json(bronze, final_blob)
        if _event_final_is_valid(existing):
            skipped += 1
            continue

        # Bad data — re-fetch
        try:
            _time.sleep(0.15)   # gentle BetsAPI rate-limit respect
            payload    = call_betsapi("/v1/event/view", {"event_id": eid})
            results    = extract_results(payload)
            result     = results[0] if results else {}
            time_status = str(result.get("time_status") or "")
            ss         = result.get("ss") or ""

            if not results or time_status != "3" or not ss:
                api_not_ended += 1
                continue

            upload_json(bronze, final_blob, payload, overwrite=True)
            repaired += 1
            repaired_list.append(eid)
            logging.warning(json.dumps({
                "event": "bronze_event_final_bulk_repaired",
                "event_id": eid, "ss": ss,
            }))

        except Exception:
            failed += 1
            logging.exception(json.dumps({
                "event": "bronze_event_final_bulk_repair_failed", "event_id": eid,
            }))

    logging.warning(json.dumps({
        "event": "bronze_repair_event_finals_done",
        "total_scanned": total, "skipped_valid": skipped,
        "repaired": repaired, "api_not_ended": api_not_ended, "failed": failed,
    }))

    # Rebuild the ended index so all corrected scores propagate immediately
    bronze_discover_cricket_ended()

    return {
        "total_scanned": total,
        "skipped_valid": skipped,
        "repaired": repaired,
        "repaired_event_ids": repaired_list,
        "api_not_ended_or_no_ss": api_not_ended,
        "failed": failed,
    }
