import json
import logging
import os
from typing import Any, Dict, List, Optional

from util import (
    call_betsapi,
    download_json,
    extract_results,
    format_unix_ts,
    get_bool_env,
    get_bronze_container_client,
    get_named_container_client,
    get_env,
    get_int_env,
    ts_compact,
    upload_json,
    utc_now,
)
from league_config import load_allowed_league_ids

_KNOWN_LEAGUES_PATH = "cricket/config/known_leagues.json"


def _upsert_known_leagues_gold(matches: List[Dict[str, Any]], source: str) -> None:
    """Merge leagues seen in matches into gold/cricket/config/known_leagues.json."""
    gold = get_named_container_client("gold")
    existing = download_json(gold, _KNOWN_LEAGUES_PATH) or {}
    leagues: Dict[str, Dict[str, Any]] = {
        str(lg["league_id"]): lg
        for lg in (existing.get("leagues") or [])
        if lg.get("league_id")
    }
    now_str = utc_now().isoformat()
    for m in matches:
        lid = str(m.get("league_id") or "").strip()
        lname = str(m.get("league_name") or "").strip()
        if not lid:
            continue
        if lid not in leagues:
            leagues[lid] = {"league_id": lid, "league_name": lname or lid, "sources": [], "first_seen_utc": now_str, "last_seen_utc": now_str}
        if source not in leagues[lid]["sources"]:
            leagues[lid]["sources"].append(source)
        if lname and not leagues[lid].get("league_name"):
            leagues[lid]["league_name"] = lname
        leagues[lid]["last_seen_utc"] = now_str
    upload_json(gold, _KNOWN_LEAGUES_PATH, {
        "updated_at_utc": now_str,
        "leagues": sorted(leagues.values(), key=lambda x: x.get("league_name") or ""),
    }, overwrite=True)


# ------------------------------------------------------------------
# Shared event summarization helpers
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
# Timer trigger bodies
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

    _upsert_known_leagues_gold(upcoming_matches, source="upcoming")

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
