import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from util import (
    call_betsapi,
    download_json,
    extract_results,
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

SNAPSHOT_HEARTBEAT_SECONDS = 300  # force-write every 5 min even if payload unchanged


# ------------------------------------------------------------------
# Inplay item helpers
# ------------------------------------------------------------------

def _get_event_id(item: Dict[str, Any]) -> Optional[str]:
    for key in ["our_event_id", "event_id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    if item.get("bet365_id") is not None:
        value = item.get("id")
        if value is not None and str(value).strip():
            return str(value)
    value = item.get("id")
    if value is not None and str(value).strip():
        return str(value)
    return None


def _get_fi(item: Dict[str, Any]) -> Optional[str]:
    for key in ["bet365_id", "FI", "fi", "id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def summarize_inplay_items(items: List[Dict[str, Any]], max_live_matches: int) -> List[Dict[str, Any]]:
    live = []
    for item in items:
        fi = _get_fi(item)
        event_id = _get_event_id(item)
        if not fi:
            continue
        if str(item.get("time_status", "1")) != "1":
            continue
        _league = item.get("league") or {}
        live.append({
            "fi": fi,
            "event_id": event_id or fi,
            "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
            "league_id": str(_league.get("id")) if _league.get("id") is not None else None,
            "league_name": _league.get("name"),
            "league": _league,
            "home": item.get("home"),
            "away": item.get("away"),
            "time_status": item.get("time_status"),
            "raw_item": item,
        })
    if max_live_matches <= 0:
        return live
    return live[:max_live_matches]


# ------------------------------------------------------------------
# Lineage helpers
# ------------------------------------------------------------------

def _get_api_result_count(api_payload: Optional[Dict[str, Any]]) -> Optional[int]:
    if not api_payload:
        return None
    body = api_payload.get("response", {}).get("body")
    if not isinstance(body, dict):
        return None
    results = body.get("results")
    if isinstance(results, list):
        return len(results)
    if isinstance(results, dict):
        return len(results)
    return None


def _build_api_call_lineage(
    api_name: str,
    api_payload: Optional[Dict[str, Any]],
    id_used: Dict[str, Any],
    bronze_path: Optional[str],
    purpose: str,
) -> Dict[str, Any]:
    response = (api_payload or {}).get("response", {})
    request = (api_payload or {}).get("request", {})
    return {
        "api_name": api_name,
        "purpose": purpose,
        "id_used": id_used,
        "path": request.get("url"),
        "params_without_token": request.get("params_without_token"),
        "called_at_utc": request.get("called_at_utc"),
        "http_status_code": response.get("http_status_code"),
        "success": response.get("success"),
        "elapsed_ms": response.get("elapsed_ms"),
        "error": response.get("error"),
        "error_detail": response.get("error_detail"),
        "result_count": _get_api_result_count(api_payload),
        "bronze_path": bronze_path,
    }


def _build_snapshot_lineage(
    sport_id: str,
    event_id: str,
    fi: str,
    base_path: str,
    payloads: Dict[str, Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    bronze_base = f"bronze/{base_path}"
    calls = [
        _build_api_call_lineage("events_inplay", payloads.get("events_inplay"), {"sport_id": sport_id}, f"{bronze_base}/api_inplay_event_list.json", "Find live cricket matches and current score/status from /v3/events/inplay."),
        _build_api_call_lineage("event_view", payloads.get("event_view"), {"event_id": event_id}, f"{bronze_base}/api_event_view.json", "Get event scoreboard/details from /v1/event/view using event_id."),
        _build_api_call_lineage("event_odds_summary", payloads.get("event_odds_summary"), {"event_id": event_id}, f"{bronze_base}/api_event_odds_summary.json", "Get compact odds summary from /v2/event/odds/summary using event_id."),
        _build_api_call_lineage("event_odds", payloads.get("event_odds"), {"event_id": event_id}, f"{bronze_base}/api_event_odds.json", "Get event odds history from /v2/event/odds using event_id."),
        _build_api_call_lineage("bet365_event", payloads.get("bet365_event"), {"FI": fi}, f"{bronze_base}/api_live_market_odds.json", "Get live Bet365 market stream from /v1/bet365/event using FI/bet365_id."),
    ]
    return {
        "generated_at_utc": utc_now().isoformat(),
        "sport_id": sport_id,
        "event_id": event_id,
        "fi": fi,
        "id_mapping": {
            "event_id": {"value": event_id, "used_for": ["/v1/event/view", "/v2/event/odds/summary", "/v2/event/odds"]},
            "fi": {"value": fi, "also_called": "bet365_id", "used_for": ["/v1/bet365/event"]},
        },
        "bronze_base_path": f"bronze/{base_path}",
        "api_calls": calls,
    }


# ------------------------------------------------------------------
# Timer trigger bodies
# ------------------------------------------------------------------

def _extract_pg(bet365_stats_payload: Any) -> Optional[str]:
    """Return the PG field from the EV record in a bet365/event?stats=1 payload.

    PG encodes the current game state as innings:over:ball:... e.g. '1:1:1:1:1:1#8:5:4'.
    It only changes when a new ball is bowled, making it the ideal dedup key.
    """
    body = (bet365_stats_payload or {}).get("response", {}).get("body") or {}
    results = body.get("results", [])
    records = results[0] if results and isinstance(results[0], list) else (results if isinstance(results, list) else [])
    for r in records:
        if isinstance(r, dict) and r.get("type") == "EV" and r.get("PG"):
            return str(r["PG"])
    return None


def _pg_to_snapshot_segment(pg: Optional[str]) -> str:
    """Sanitize PG value for embedding in a blob path segment (removes : and #)."""
    if not pg:
        return "nopg"
    return pg.replace(":", "-").replace("#", "_")


def bronze_discover_cricket_inplay() -> None:
    """Fetch live cricket matches from /v3/events/inplay and write the control file."""
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_live_matches = get_int_env("MAX_LIVE_MATCHES", 50)
    container = get_bronze_container_client()

    api_payload = call_betsapi(path="/v3/events/inplay", params={"sport_id": sport_id})

    raw_path = (
        f"betsapi/inplay_filter/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"events_inplay_{ts_compact(now)}.json"
    )
    upload_json(container, raw_path, api_payload, overwrite=True)

    active_matches = summarize_inplay_items(extract_results(api_payload), max_live_matches)
    control_payload = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "max_live_matches": max_live_matches,
        "active_match_count": len(active_matches),
        "active_matches": active_matches,
        "source_raw_path": f"bronze/{raw_path}",
    }
    upload_json(container, "betsapi/control/active_inplay_fi/latest.json", control_payload, overwrite=True)

    _upsert_known_leagues_gold(active_matches, source="live")

    logging.info(json.dumps({
        "event": "bronze_discover_cricket_inplay_completed",
        "success": api_payload["response"]["success"],
        "active_match_count": len(active_matches),
        "raw_path": f"bronze/{raw_path}",
    }))


def bronze_capture_cricket_inplay_snapshot() -> None:
    """Capture one full live snapshot per active match.

    Bronze is intentionally complete and verbose. We store every important live
    API response separately so future silver/gold logic can be changed without
    losing raw data.

    APIs captured per snapshot:
    1. /v3/events/inplay                     -> list/status/score (sport_id)
    2. /v1/event/view                        -> event details (event_id)
    3. /v2/event/odds/summary                -> odds summary (event_id)
    4. /v2/event/odds                        -> full odds history (event_id)
    5. /v1/event/history                     -> ball-by-ball timeline (event_id)
    6. /v1/event/stats_trend                 -> rolling stats trend (event_id)
    7. /v1/bet365/event                      -> Bet365 live markets (FI)
    8. /v1/bet365/event?stats=1              -> Bet365 live stats/PG field (FI)
    9. /v1/bet365/event?lineup=1             -> Bet365 lineup data (FI)
    10. /v1/bet365/event?raw=1               -> Bet365 raw event feed (FI)
    """
    import time as _time
    run_start = _time.monotonic()
    budget_seconds = 480  # exit at 8 min — well inside the 10-min Azure Functions ceiling

    sport_id = get_env("SPORT_ID", "3")
    container = get_bronze_container_client()

    control = download_json(container, "betsapi/control/active_inplay_fi/latest.json")
    if not control or not control.get("active_matches"):
        logging.info(json.dumps({"event": "bronze_capture_cricket_inplay_snapshot_skipped", "reason": "no_active_matches"}))
        return

    active_matches = control["active_matches"]
    allowed_leagues = load_allowed_league_ids()

    events_inplay_payload = call_betsapi(path="/v3/events/inplay", params={"sport_id": sport_id})

    captured = skipped = 0
    for match in active_matches:
        if _time.monotonic() - run_start > budget_seconds:
            logging.warning(json.dumps({"event": "bronze_capture_budget_reached", "captured": captured, "remaining": len(active_matches) - captured - skipped}))
            break
        if str(match.get("league_id") or "") not in allowed_leagues:
            logging.info(json.dumps({"event": "bronze_inplay_snapshot_skipped_league", "league_id": match.get("league_id"), "league_name": match.get("league_name")}))
            skipped += 1
            continue
        snapshot_time = utc_now()
        fi = str(match["fi"])
        event_id = str(match.get("event_id") or fi)

        event_view_payload = call_betsapi(path="/v1/event/view", params={"event_id": event_id})
        event_odds_summary_payload = call_betsapi(path="/v2/event/odds/summary", params={"event_id": event_id})
        event_odds_payload = call_betsapi(path="/v2/event/odds", params={"event_id": event_id})
        event_history_payload = call_betsapi(path="/v1/event/history", params={"event_id": event_id})
        event_stats_trend_payload = call_betsapi(path="/v1/event/stats_trend", params={"event_id": event_id})
        bet365_event_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi})
        bet365_event_stats_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi, "stats": 1})
        bet365_event_lineup_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi, "lineup": 1})
        bet365_event_raw_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi, "raw": 1})

        # Compare only the PG field from the stats payload — it encodes innings:over:ball
        # and changes on every new delivery. Odds are excluded because they fluctuate
        # every 5s even on a static game state.
        new_pg = _extract_pg(bet365_event_stats_payload)

        hash_control_path = f"betsapi/control/snapshot_hash/event_id={event_id}.json"
        stored = download_json(container, hash_control_path) or {}

        last_written_str = stored.get("last_written_utc")
        if last_written_str:
            last_written_dt = datetime.fromisoformat(last_written_str)
            if last_written_dt.tzinfo is None:
                last_written_dt = last_written_dt.replace(tzinfo=timezone.utc)
            seconds_since_write = (snapshot_time - last_written_dt).total_seconds()
        else:
            seconds_since_write = 9999

        if new_pg is None:
            logging.info(json.dumps({
                "event": "bronze_snapshot_skipped_no_pg",
                "event_id": event_id,
            }))
            skipped += 1
            continue

        if (
            new_pg == stored.get("pg")
            and seconds_since_write < SNAPSHOT_HEARTBEAT_SECONDS
        ):
            logging.info(json.dumps({
                "event": "bronze_snapshot_skipped_duplicate",
                "event_id": event_id,
                "pg": new_pg,
                "seconds_since_last_write": round(seconds_since_write),
            }))
            skipped += 1
            continue

        # Embed the PG value in the snapshot_id so the blob path is self-describing
        snapshot_id = f"{ts_compact(snapshot_time)}_{_pg_to_snapshot_segment(new_pg)}"
        base_path = f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}"

        lineage = _build_snapshot_lineage(
            sport_id=sport_id,
            event_id=event_id,
            fi=fi,
            base_path=base_path,
            payloads={
                "events_inplay": events_inplay_payload,
                "event_view": event_view_payload,
                "event_odds_summary": event_odds_summary_payload,
                "event_odds": event_odds_payload,
                "bet365_event": bet365_event_payload,
            },
        )

        manifest = {
            "snapshot_id": snapshot_id,
            "snapshot_time_utc": snapshot_time.isoformat(),
            "sport_id": sport_id,
            "event_id": event_id,
            "fi": fi,
            "match_from_filter": match,
            "files": {
                "api_inplay_event_list": f"bronze/{base_path}/api_inplay_event_list.json",
                "api_event_view": f"bronze/{base_path}/api_event_view.json",
                "api_event_odds_summary": f"bronze/{base_path}/api_event_odds_summary.json",
                "api_event_odds": f"bronze/{base_path}/api_event_odds.json",
                "api_event_history": f"bronze/{base_path}/api_event_history.json",
                "api_event_stats_trend": f"bronze/{base_path}/api_event_stats_trend.json",
                "api_live_market_odds": f"bronze/{base_path}/api_live_market_odds.json",
                "api_live_market_stats": f"bronze/{base_path}/api_live_market_stats.json",
                "api_live_market_lineup": f"bronze/{base_path}/api_live_market_lineup.json",
                "api_live_market_raw": f"bronze/{base_path}/api_live_market_raw.json",
                "lineage": f"bronze/{base_path}/lineage.json",
            },
            "api_lineage": lineage,
            "status": {
                "events_inplay_success": events_inplay_payload["response"]["success"],
                "event_view_success": event_view_payload["response"]["success"],
                "event_view_error": event_view_payload["response"].get("error"),
                "event_view_error_detail": event_view_payload["response"].get("error_detail"),
                "event_odds_summary_success": event_odds_summary_payload["response"]["success"],
                "event_odds_summary_error": event_odds_summary_payload["response"].get("error"),
                "event_odds_summary_error_detail": event_odds_summary_payload["response"].get("error_detail"),
                "event_odds_success": event_odds_payload["response"]["success"],
                "event_odds_error": event_odds_payload["response"].get("error"),
                "event_odds_error_detail": event_odds_payload["response"].get("error_detail"),
                "event_history_success": event_history_payload["response"]["success"],
                "event_stats_trend_success": event_stats_trend_payload["response"]["success"],
                "bet365_event_success": bet365_event_payload["response"]["success"],
                "bet365_event_error": bet365_event_payload["response"].get("error"),
                "bet365_event_error_detail": bet365_event_payload["response"].get("error_detail"),
                "bet365_event_stats_success": bet365_event_stats_payload["response"]["success"],
                "bet365_event_lineup_success": bet365_event_lineup_payload["response"]["success"],
                "bet365_event_raw_success": bet365_event_raw_payload["response"]["success"],
            },
        }

        upload_json(container, f"{base_path}/api_inplay_event_list.json", events_inplay_payload)
        upload_json(container, f"{base_path}/api_event_view.json", event_view_payload)
        upload_json(container, f"{base_path}/api_event_odds_summary.json", event_odds_summary_payload)
        upload_json(container, f"{base_path}/api_event_odds.json", event_odds_payload)
        upload_json(container, f"{base_path}/api_event_history.json", event_history_payload)
        upload_json(container, f"{base_path}/api_event_stats_trend.json", event_stats_trend_payload)
        upload_json(container, f"{base_path}/api_live_market_odds.json", bet365_event_payload)
        upload_json(container, f"{base_path}/api_live_market_stats.json", bet365_event_stats_payload)
        upload_json(container, f"{base_path}/api_live_market_lineup.json", bet365_event_lineup_payload)
        upload_json(container, f"{base_path}/api_live_market_raw.json", bet365_event_raw_payload)
        upload_json(container, f"{base_path}/lineage.json", lineage)
        upload_json(container, f"{base_path}/manifest.json", manifest)

        upload_json(container, hash_control_path, {
            "pg":               new_pg,
            "last_written_utc": snapshot_time.isoformat(),
            "last_snapshot_id": snapshot_id,
        }, overwrite=True)

        captured += 1
