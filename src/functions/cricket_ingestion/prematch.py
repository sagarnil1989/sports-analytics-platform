import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from storage import (
    call_betsapi,
    download_json,
    download_required_json,
    extract_results,
    format_unix_ts,
    get_bool_env,
    get_bronze_container_client,
    get_env,
    get_int_env,
    get_named_container_client,
    safe_float,
    ts_compact,
    upload_json,
    utc_now,
)
from leagues import load_allowed_league_ids, load_blocked_event_ids


# ------------------------------------------------------------------
# Event summarization helpers (used by both upcoming and ended)
# ------------------------------------------------------------------

def summarize_event_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a BetsAPI event row into our standard small match record."""
    event_id = str(item.get("id")) if item.get("id") is not None else None
    fi = str(item.get("bet365_id")) if item.get("bet365_id") is not None else None
    league = item.get("league") or {}
    home = item.get("home") or {}
    away = item.get("away") or {}

    return {
        "event_id": event_id,
        "fi": fi,
        "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
        "event_time_unix": item.get("time"),
        "event_time_utc": format_unix_ts(item.get("time")),
        "time_status": item.get("time_status"),
        "league": league,
        "league_id": str(league.get("id")) if league.get("id") is not None else None,
        "league_name": league.get("name"),
        "home": home,
        "home_team_id": str(home.get("id")) if home.get("id") is not None else None,
        "home_team_name": home.get("name"),
        "away": away,
        "away_team_id": str(away.get("id")) if away.get("id") is not None else None,
        "away_team_name": away.get("name"),
        "match_name": f"{home.get('name', '')} vs {away.get('name', '')}".strip(" vs "),
        "score": item.get("ss"),
        "raw_item": item,
    }


def summarize_event_items(items: List[Dict[str, Any]], max_events: int, require_bet365_id: bool = False) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        match = summarize_event_item(item)
        if not match.get("event_id"):
            continue
        if require_bet365_id and not match.get("fi"):
            continue
        matches.append(match)
    if max_events <= 0:
        return matches
    return matches[:max_events]


# ------------------------------------------------------------------
# Bronze ingestion
# ------------------------------------------------------------------

def bronze_discover_cricket_upcoming() -> None:
    """Find upcoming cricket matches and store a small control file."""
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_upcoming = get_int_env("MAX_UPCOMING_MATCHES", 100)
    only_bet365 = get_bool_env("UPCOMING_REQUIRE_BET365_ID", True)
    container = get_bronze_container_client()

    api_payload = call_betsapi(path="/v3/events/upcoming", params={"sport_id": sport_id})

    raw_path = (
        f"betsapi/upcoming/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"events_upcoming_{ts_compact(now)}.json"
    )
    upload_json(container, raw_path, api_payload)

    upcoming_matches = summarize_event_items(extract_results(api_payload), max_upcoming, require_bet365_id=only_bet365)
    control_payload = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "max_upcoming_matches": max_upcoming,
        "require_bet365_id": only_bet365,
        "upcoming_match_count": len(upcoming_matches),
        "upcoming_matches": upcoming_matches,
        "source_raw_path": f"bronze/{raw_path}",
    }
    upload_json(container, "betsapi/control/upcoming_cricket/latest.json", control_payload, overwrite=True)

    logging.info(json.dumps({
        "event": "bronze_discover_cricket_upcoming_completed",
        "success": api_payload["response"]["success"],
        "upcoming_match_count": len(upcoming_matches),
        "raw_path": f"bronze/{raw_path}",
    }))


def bronze_capture_cricket_prematch_odds() -> None:
    """Capture prematch odds for upcoming matches using /v4/bet365/prematch."""
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_PREMATCH_ODDS_PER_RUN", 100)
    container = get_bronze_container_client()

    control = download_json(container, "betsapi/control/upcoming_cricket/latest.json")
    if not control or not control.get("upcoming_matches"):
        logging.info(json.dumps({"event": "bronze_capture_cricket_prematch_odds_skipped", "reason": "no_upcoming_matches"}))
        return

    allowed_leagues = load_allowed_league_ids()
    upcoming_matches = [
        m for m in control.get("upcoming_matches", [])
        if m.get("fi") and str(m.get("league_id") or "") in allowed_leagues
    ]
    if max_per_run > 0:
        upcoming_matches = upcoming_matches[:max_per_run]

    processed = failed = skipped = 0
    for match in upcoming_matches:
        try:
            snapshot_time = utc_now()
            snapshot_id = ts_compact(snapshot_time)
            event_id = str(match.get("event_id"))
            fi = str(match.get("fi"))
            if not event_id or not fi:
                skipped += 1
                continue

            prematch_payload = call_betsapi(path="/v4/bet365/prematch", params={"FI": fi})

            base_path = f"betsapi/prematch_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}"
            manifest = {
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time.isoformat(),
                "sport_id": sport_id,
                "event_id": event_id,
                "fi": fi,
                "match_from_upcoming": match,
                "files": {
                    "api_prematch_odds": f"bronze/{base_path}/api_prematch_odds.json",
                },
                "status": {
                    "prematch_success": prematch_payload["response"]["success"],
                    "prematch_error": prematch_payload["response"].get("error"),
                    "prematch_error_detail": prematch_payload["response"].get("error_detail"),
                },
            }

            upload_json(container, f"{base_path}/api_prematch_odds.json", prematch_payload)
            upload_json(container, f"{base_path}/manifest.json", manifest)
            processed += 1

        except Exception:
            failed += 1
            logging.exception("Failed to capture prematch odds")

    logging.info(json.dumps({
        "event": "bronze_capture_cricket_prematch_odds_completed",
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "checked": len(upcoming_matches),
    }))


def bronze_discover_cricket_ended() -> None:
<<<<<<< HEAD
    """Build the ended match index from gold innings_1_from_silver.json files.

    A match is considered ended when its innings_1.json tracker has not been
    updated for more than 1 hour AND it is not present in the live index.
    When that condition is first met, innings_1_from_silver.json is written
    automatically (if it doesn't already exist) to mark the match as ended.

    No BetsAPI call is made — all data comes from gold/silver/bronze blobs.
=======
    """Build the ended match index entirely from gold tracker files.

    A match appears here if and only if:
      1. innings_1_from_silver.json exists in gold for that event_id
      2. The match is NOT currently live (not in the live matches index)
      3. The league is not excluded and the event is not blocked

    The BetsAPI ended endpoint is called only for metadata enrichment (final
    score, fi) of matches already known via gold tracker files.
>>>>>>> refs/remotes/origin/main
    """
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    bronze = get_bronze_container_client()
    gold = get_named_container_client("gold")

    allowed_leagues = load_allowed_league_ids()
    blocked_events = load_blocked_event_ids()

<<<<<<< HEAD
    # ── 1. Get currently-live event IDs ──────────────────────────────────────
=======
    # ── 1. Scan gold for all event_ids that have innings_1_from_silver.json ───
    tracker_prefix = "cricket/innings_tracker/event_id="
    silver_eids: Dict[str, str] = {}  # eid -> blob_name
    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if not blob.name.endswith("innings_1_from_silver.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        eid = eid_part.replace("event_id=", "")
        silver_eids[eid] = blob.name

    # ── 2. Get currently-live event IDs so we can exclude them ───────────────
>>>>>>> refs/remotes/origin/main
    live_eids: set = set()
    try:
        live_idx = download_json(gold, "cricket/matches/latest/index.json") or {}
        for m in (live_idx.get("matches") or []):
            eid = str(m.get("event_id") or "")
            if eid:
                live_eids.add(eid)
    except Exception:
        pass
<<<<<<< HEAD

    # ── 2. Find matches that have gone quiet for >1 hour ─────────────────────
    # Scan innings_1.json blobs; use blob.last_modified without downloading.
    one_hour_ago = now - timedelta(hours=1)
    tracker_prefix = "cricket/innings_tracker/event_id="

    # Collect existing innings_1_from_silver.json paths to avoid re-writing.
    existing_silver_eids: set = set()
    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if blob.name.endswith("innings_1_from_silver.json"):
            parts = blob.name.split("/")
            eid_part = next((p for p in parts if p.startswith("event_id=")), None)
            if eid_part:
                existing_silver_eids.add(eid_part.replace("event_id=", ""))

    newly_written = 0
    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if not blob.name.endswith("innings_1.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        eid = eid_part.replace("event_id=", "")

        # Skip if still live or already promoted to silver
        if eid in live_eids:
            continue
        if eid in existing_silver_eids:
            continue
        if eid in blocked_events:
            continue

        # Only promote if innings_1.json hasn't been touched for >1 hour
        last_mod = blob.last_modified
        if last_mod and last_mod > one_hour_ago:
            continue

        # Download tracker to check league and build the silver file
        tracker = download_json(gold, blob.name)
=======

    # ── 3. Call BetsAPI ended for metadata enrichment ────────────────────────
    api_payload = call_betsapi(path="/v3/events/ended", params={"sport_id": sport_id})
    raw_path = (
        f"betsapi/ended/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"events_ended_{ts_compact(now)}.json"
    )
    upload_json(bronze, raw_path, api_payload)

    api_by_eid: Dict[str, Dict] = {}
    for m in summarize_event_items(extract_results(api_payload), 200, require_bet365_id=False):
        eid = str(m.get("event_id") or "")
        if eid:
            api_by_eid[eid] = m

    # ── 4. Build FI lookup: API response first, then bronze path scan ─────────
    fi_lookup: Dict[str, str] = {}
    for eid, m in api_by_eid.items():
        if m.get("fi"):
            fi_lookup[eid] = str(m["fi"])
    # Scan bronze snapshot paths for event_ids still missing FI
    for eid in silver_eids:
        if eid in fi_lookup:
            continue
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

    # ── 5. Build ended index from gold tracker files only ────────────────────
    matches: List[Dict[str, Any]] = []
    for eid, tracker_blob_name in silver_eids.items():
        if eid in blocked_events:
            continue
        if eid in live_eids:
            # Match is still live — do not include in ended index
            logging.info(json.dumps({"event": "ended_skip_live", "event_id": eid}))
            continue

        tracker = download_json(gold, tracker_blob_name)
>>>>>>> refs/remotes/origin/main
        if not tracker:
            continue

        league_id = str(tracker.get("league_id") or "")
        if league_id not in allowed_leagues:
            continue

<<<<<<< HEAD
        silver_blob = f"cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json"
        upload_json(gold, silver_blob, tracker, overwrite=True)
        existing_silver_eids.add(eid)
        newly_written += 1
        logging.info(json.dumps({
            "event": "ended_auto_promote",
=======
        record: Dict[str, Any] = {
>>>>>>> refs/remotes/origin/main
            "event_id": eid,
            "innings_1_last_modified": last_mod.isoformat() if last_mod else None,
        }))

    # ── 3. Collect all event_ids with innings_1_from_silver.json ─────────────
    silver_eids: Dict[str, str] = {}  # eid -> blob_name
    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if not blob.name.endswith("innings_1_from_silver.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        eid = eid_part.replace("event_id=", "")
        silver_eids[eid] = blob.name

    # ── 4. FI lookup from bronze path scan ───────────────────────────────────
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

    # ── 5. Build ended index ──────────────────────────────────────────────────
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
        if league_id not in allowed_leagues:
            continue

        match_name = tracker.get("match_name") or ""
        fi = fi_lookup.get(eid)
        if not match_name:
            logging.info(json.dumps({"event": "ended_skip_no_match_name", "event_id": eid}))
            continue
        if not fi:
            logging.info(json.dumps({"event": "ended_skip_no_fi", "event_id": eid}))
            continue

        score = (
            tracker.get("score_summary_events")
            or tracker.get("score_summary_bet365")
            or tracker.get("score_summary")
            or ""
        )
        record: Dict[str, Any] = {
            "event_id": eid,
            "fi": fi,
            "league_id": league_id,
            "league_name": tracker.get("league_name"),
            "home_team_name": tracker.get("home_team_name"),
            "away_team_name": tracker.get("away_team_name"),
            "match_name": match_name,
            "score": score,
            "event_time_utc": tracker.get("match_date_utc"),
            "time_status": "3",
        }
        matches.append(record)

<<<<<<< HEAD
=======
        # Enrich final score from API if available
        if eid in api_by_eid:
            api_m = api_by_eid[eid]
            if api_m.get("ss"):
                record["ss"] = api_m["ss"]
            if api_m.get("fi") and not record.get("fi"):
                record["fi"] = api_m["fi"]

        matches.append(record)

>>>>>>> refs/remotes/origin/main
    matches.sort(key=lambda m: str(m.get("event_time_utc") or ""), reverse=True)

    ended_index = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "ended_match_count": len(matches),
        "matches": matches,
<<<<<<< HEAD
=======
        "source_raw_path": f"bronze/{raw_path}",
>>>>>>> refs/remotes/origin/main
    }
    upload_json(gold, "cricket/ended/latest/index.json", ended_index, overwrite=True)

    logging.info(json.dumps({
        "event": "bronze_discover_cricket_ended_completed",
        "ended_match_count": len(matches),
        "silver_eid_count": len(silver_eids),
<<<<<<< HEAD
        "newly_promoted": newly_written,
        "live_excluded": len(live_eids & set(silver_eids.keys())),
=======
        "live_excluded": len(live_eids & set(silver_eids.keys())),
        "raw_path": f"bronze/{raw_path}",
>>>>>>> refs/remotes/origin/main
    }))


# ------------------------------------------------------------------
# Prematch parsing helpers
# ------------------------------------------------------------------

def _list_prematch_manifest_paths(bronze_container, sport_id: str, limit: int) -> List[str]:
    prefix = f"betsapi/prematch_snapshot/sport_id={sport_id}/"
    manifests = []
    for blob in bronze_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/manifest.json"):
            manifests.append((getattr(blob, "last_modified", None), name))
    manifests.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in manifests[:limit]]


def _extract_prematch_result(prematch_payload: Dict[str, Any]) -> Dict[str, Any]:
    body = prematch_payload.get("response", {}).get("body", {})
    results = body.get("results", []) if isinstance(body, dict) else []
    if isinstance(results, list) and results and isinstance(results[0], dict):
        return results[0]
    if isinstance(results, dict):
        return results
    return {}


def parse_prematch_markets(
    prematch_payload: Dict[str, Any],
    snapshot_id: str,
    snapshot_time_utc: str,
    event_id: str,
    fi: str,
) -> List[Dict[str, Any]]:
    """Flatten /v4/bet365/prematch into one row per odds selection."""
    root = _extract_prematch_result(prematch_payload)
    rows: List[Dict[str, Any]] = []

    for category_key, category_value in root.items():
        if not isinstance(category_value, dict):
            continue
        sp = category_value.get("sp")
        if not isinstance(sp, dict):
            continue
        category_updated_at = category_value.get("updated_at")
        for market_key, market_value in sp.items():
            if not isinstance(market_value, dict):
                continue
            odds_list = market_value.get("odds", [])
            if not isinstance(odds_list, list):
                continue
            for odds_item in odds_list:
                if not isinstance(odds_item, dict):
                    continue
                odds_value = odds_item.get("odds")
                rows.append({
                    "snapshot_id": snapshot_id,
                    "snapshot_time_utc": snapshot_time_utc,
                    "event_id": event_id,
                    "fi": fi,
                    "category_key": str(category_key),
                    "category_updated_at": category_updated_at,
                    "category_updated_at_utc": format_unix_ts(category_updated_at),
                    "market_key": str(market_key),
                    "market_id": str(market_value.get("id")) if market_value.get("id") is not None else None,
                    "market_name": market_value.get("name"),
                    "selection_id": str(odds_item.get("id")) if odds_item.get("id") is not None else None,
                    "selection_name": odds_item.get("name"),
                    "selection_header": odds_item.get("header"),
                    "handicap": odds_item.get("handicap"),
                    "odds": odds_value,
                    "odds_decimal": safe_float(odds_value),
                    "raw": odds_item,
                })

    return rows


def build_prematch_snapshot(manifest: Dict[str, Any], prematch_payload: Dict[str, Any]) -> Dict[str, Any]:
    event_id = str(manifest.get("event_id"))
    fi = str(manifest.get("fi"))
    match = manifest.get("match_from_upcoming", {}) or {}
    league = match.get("league") or {}
    home = match.get("home") or {}
    away = match.get("away") or {}
    markets = parse_prematch_markets(
        prematch_payload,
        manifest["snapshot_id"],
        manifest["snapshot_time_utc"],
        event_id,
        fi,
    )

    market_keys = {
        f"{row.get('category_key')}::{row.get('market_key')}"
        for row in markets
        if row.get("category_key") or row.get("market_key")
    }

    return {
        "generated_at_utc": utc_now().isoformat(),
        "snapshot": {
            "snapshot_id": manifest.get("snapshot_id"),
            "snapshot_time_utc": manifest.get("snapshot_time_utc"),
            "event_id": event_id,
            "fi": fi,
        },
        "match_header": {
            "league_id": str(league.get("id")) if league.get("id") is not None else match.get("league_id"),
            "league_name": league.get("name") or match.get("league_name"),
            "match_name": match.get("match_name"),
            "home_team": {
                "id": str(home.get("id")) if home.get("id") is not None else match.get("home_team_id"),
                "name": home.get("name") or match.get("home_team_name"),
            },
            "away_team": {
                "id": str(away.get("id")) if away.get("id") is not None else match.get("away_team_id"),
                "name": away.get("name") or match.get("away_team_name"),
            },
            "event_time_unix": match.get("event_time_unix"),
            "event_time_utc": match.get("event_time_utc"),
            "time_status": match.get("time_status"),
        },
        "prematch_markets": {
            "selection_count": len(markets),
            "market_count": len(market_keys),
            "records": markets,
        },
        "source": {
            "bronze_manifest_path": (
                f"bronze/betsapi/prematch_snapshot/sport_id={manifest.get('sport_id', '3')}/"
                f"event_id={event_id}/fi={fi}/snapshot_id={manifest.get('snapshot_id')}/manifest.json"
            )
        },
    }


def gold_write_prematch_indexes(gold_container, pages: List[Dict[str, Any]]) -> None:
    existing_index = download_json(gold_container, "cricket/prematch/latest/index.json") or {}
    latest_by_event: Dict[str, Dict[str, Any]] = {}

    if isinstance(existing_index, dict):
        for row in existing_index.get("matches", []):
            event_id = str(row.get("event_id") or "")
            if event_id:
                latest_by_event[event_id] = row

    for page in pages:
        snapshot = page.get("snapshot", {}) or {}
        header = page.get("match_header", {}) or {}
        markets = page.get("prematch_markets", {}) or {}
        event_id = str(snapshot.get("event_id") or "")
        if not event_id:
            continue

        home_name = (header.get("home_team") or {}).get("name")
        away_name = (header.get("away_team") or {}).get("name")
        match_name = header.get("match_name") or (f"{home_name} vs {away_name}" if home_name and away_name else None)
        row = {
            "event_id": event_id,
            "fi": snapshot.get("fi"),
            "snapshot_id": snapshot.get("snapshot_id"),
            "snapshot_time_utc": snapshot.get("snapshot_time_utc"),
            "league_id": header.get("league_id"),
            "league_name": header.get("league_name"),
            "match_name": match_name,
            "home_team_name": home_name,
            "away_team_name": away_name,
            "event_time_unix": header.get("event_time_unix"),
            "event_time_utc": header.get("event_time_utc"),
            "prematch_market_count": markets.get("market_count"),
            "prematch_selection_count": markets.get("selection_count"),
            "latest_gold_path": f"gold/cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json",
        }

        current = latest_by_event.get(event_id)
        if current is None or (row.get("snapshot_time_utc") or "") >= (current.get("snapshot_time_utc") or ""):
            latest_by_event[event_id] = row

    matches = list(latest_by_event.values())
    matches.sort(key=lambda x: x.get("event_time_unix") or "", reverse=False)

    upload_json(
        gold_container,
        "cricket/prematch/latest/index.json",
        {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches), "matches": matches},
        overwrite=True,
    )

    leagues: Dict[str, Dict[str, Any]] = {}
    for match in matches:
        league_id = str(match.get("league_id") or "unknown")
        league_name = match.get("league_name") or "Unknown League"
        if league_id not in leagues:
            leagues[league_id] = {"league_id": league_id, "league_name": league_name, "match_count": 0, "matches": []}
        leagues[league_id]["matches"].append(match)
        leagues[league_id]["match_count"] += 1

    league_rows = []
    for league in leagues.values():
        league["matches"].sort(key=lambda x: x.get("event_time_unix") or "", reverse=False)
        upload_json(
            gold_container,
            f"cricket/prematch/leagues/{league['league_id']}/matches.json",
            {
                "generated_at_utc": utc_now().isoformat(),
                "league_id": league["league_id"],
                "league_name": league["league_name"],
                "match_count": league["match_count"],
                "matches": league["matches"],
            },
            overwrite=True,
        )
        league_rows.append({
            "league_id": league["league_id"],
            "league_name": league["league_name"],
            "match_count": league["match_count"],
            "matches_path": f"gold/cricket/prematch/leagues/{league['league_id']}/matches.json",
        })

    league_rows.sort(key=lambda x: x.get("league_name") or "")
    upload_json(
        gold_container,
        "cricket/prematch/leagues/index.json",
        {"generated_at_utc": utc_now().isoformat(), "league_count": len(league_rows), "leagues": league_rows},
        overwrite=True,
    )


def gold_build_cricket_prematch_pages() -> None:
    """Timer trigger body: build fast gold pages from captured prematch odds."""
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_PREMATCH_SNAPSHOTS_PER_RUN", 200)
    bronze = get_bronze_container_client()
    gold = get_named_container_client("gold")

    manifest_paths = _list_prematch_manifest_paths(bronze, sport_id, max_per_run)
    built_pages: List[Dict[str, Any]] = []
    processed = failed = 0

    for manifest_path in manifest_paths:
        try:
            manifest = download_required_json(bronze, manifest_path)
            base_path = manifest_path.removesuffix("/manifest.json")
            prematch_payload = (
                download_json(bronze, f"{base_path}/api_prematch_odds.json")
                or download_required_json(bronze, f"{base_path}/prematch.json")
            )
            page = build_prematch_snapshot(manifest, prematch_payload)

            event_id = page["snapshot"]["event_id"]
            snapshot_id = page["snapshot"]["snapshot_id"]
            upload_json(gold, f"cricket/prematch/history/event_id={event_id}/snapshot_id={snapshot_id}/prematch_dashboard.json", page, overwrite=True)
            upload_json(gold, f"cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json", page, overwrite=True)
            built_pages.append(page)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to build prematch page")

    if built_pages:
        gold_write_prematch_indexes(gold, built_pages)

    logging.info(json.dumps({
        "event": "gold_build_cricket_prematch_pages_completed",
        "processed": processed,
        "failed": failed,
        "checked": len(manifest_paths),
    }))
