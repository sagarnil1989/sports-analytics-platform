import json
import logging
from typing import Any, Dict, List

from storage import (
    call_betsapi,
    extract_results,
    get_bronze_container_client,
    get_env,
    get_int_env,
    summarize_inplay_items,
    ts_compact,
    upload_json,
    utc_now,
    build_live_snapshot_lineage,
)
from leagues import load_excluded_league_ids


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
    upload_json(container, raw_path, api_payload)

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
    from storage import download_json
    sport_id = get_env("SPORT_ID", "3")
    container = get_bronze_container_client()

    control = download_json(container, "betsapi/control/active_inplay_fi/latest.json")
    if not control or not control.get("active_matches"):
        logging.info(json.dumps({"event": "bronze_capture_cricket_inplay_snapshot_skipped", "reason": "no_active_matches"}))
        return

    active_matches = control["active_matches"]
    excluded_leagues = load_excluded_league_ids()

    events_inplay_payload = call_betsapi(path="/v3/events/inplay", params={"sport_id": sport_id})

    for match in active_matches:
        if excluded_leagues and str(match.get("league_id") or "") in excluded_leagues:
            logging.info(json.dumps({"event": "bronze_inplay_snapshot_skipped_league", "league_id": match.get("league_id"), "league_name": match.get("league_name")}))
            continue
        snapshot_time = utc_now()
        snapshot_id = ts_compact(snapshot_time)
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

        base_path = f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}"

        lineage = build_live_snapshot_lineage(
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
