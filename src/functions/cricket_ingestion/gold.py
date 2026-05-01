import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from azure.core.exceptions import ResourceNotFoundError

from storage import (
    download_json,
    download_required_json,
    get_int_env,
    get_named_container_client,
    upload_json,
    utc_now,
)
from innings_tracker import gold_write_innings_tracker_from_silver


def gold_list_latest_silver_match_snapshots(silver_container, limit: int) -> List[str]:
    prefix = "cricket/inplay/"
    snapshots = []
    for blob in silver_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/match_state.json"):
            snapshots.append((getattr(blob, "last_modified", None), name))
    snapshots.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in snapshots[:limit]]


def _base_path_from_match_snapshot_path(match_snapshot_path: str) -> str:
    return match_snapshot_path.removesuffix("/match_state.json")


def gold_build_match_page(
    match_snapshot: Dict[str, Any],
    team_scores: Dict[str, Any],
    player_entries: Dict[str, Any],
    odds_records: Dict[str, Any],
    current_markets: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    teams = team_scores.get("rows", []) if isinstance(team_scores, dict) else []
    players = player_entries.get("rows", []) if isinstance(player_entries, dict) else []
    odds = odds_records.get("rows", []) if isinstance(odds_records, dict) else []
    markets = current_markets.get("rows", []) if isinstance(current_markets, dict) else []
    unique_market_keys = {
        m.get("market_group_id") or m.get("market_group_name") or m.get("market_id") or m.get("market_name")
        for m in markets
        if m.get("market_group_id") or m.get("market_group_name") or m.get("market_id") or m.get("market_name")
    }
    return {
        "generated_at_utc": utc_now().isoformat(),
        "snapshot": {
            "snapshot_id": match_snapshot.get("snapshot_id"),
            "snapshot_time_utc": match_snapshot.get("snapshot_time_utc"),
            "event_id": match_snapshot.get("event_id"),
            "fi": match_snapshot.get("fi"),
        },
        "match_header": {
            "league_id": match_snapshot.get("league_id"),
            "league_name": match_snapshot.get("league_name"),
            "match_name": match_snapshot.get("match_name"),
            "home_team": {"id": match_snapshot.get("home_team_id"), "name": match_snapshot.get("home_team_name")},
            "away_team": {"id": match_snapshot.get("away_team_id"), "name": match_snapshot.get("away_team_name")},
            "time_status": match_snapshot.get("time_status"),
            "venue": match_snapshot.get("venue"),
            "event_time_unix": match_snapshot.get("event_time_unix"),
            "event_time_utc": match_snapshot.get("event_time_utc"),
        },
        "score": {
            "summary_from_events": match_snapshot.get("score_summary_events"),
            "summary_from_bet365": match_snapshot.get("score_summary_bet365"),
            "team_scores": teams,
        },
        "players": players,
        "odds": {"count": len(odds), "records": odds},
        "current_markets": {
            "selection_count": len(markets),
            "market_count": len(unique_market_keys),
            "records": markets,
        },
        "source": {
            "silver_match_snapshot_path": match_snapshot.get("source_silver_match_snapshot_path"),
            "silver_lineage_path": match_snapshot.get("source_silver_lineage_path"),
            "bronze_manifest_path": match_snapshot.get("source_bronze_manifest_path"),
            "bronze_lineage_path": match_snapshot.get("source_bronze_lineage_path"),
        },
        "data_lineage": match_snapshot.get("api_lineage"),
    }


def gold_write_match_page(gold_container, match_page: Dict[str, Any]) -> None:
    event_id = str(match_page["snapshot"]["event_id"])
    snapshot_id = str(match_page["snapshot"]["snapshot_id"])
    snapshot_time = match_page["snapshot"].get("snapshot_time_utc")
    dt = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00")) if snapshot_time else utc_now()
    history_base = (
        f"cricket/matches/history/event_id={event_id}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/snapshot_id={snapshot_id}"
    )
    latest_base = f"cricket/matches/latest/event_id={event_id}"
    upload_json(gold_container, f"{history_base}/match_dashboard.json", match_page, overwrite=True)
    upload_json(gold_container, f"{latest_base}/match_dashboard.json", match_page, overwrite=True)
    if match_page.get("data_lineage"):
        upload_json(gold_container, f"{history_base}/lineage.json", match_page["data_lineage"], overwrite=True)
        upload_json(gold_container, f"{latest_base}/lineage.json", match_page["data_lineage"], overwrite=True)


def _build_match_index_row(page: Dict[str, Any]) -> Dict[str, Any]:
    header = page.get("match_header", {}) or {}
    snapshot = page.get("snapshot", {}) or {}
    score = page.get("score", {}) or {}
    current_markets = page.get("current_markets", {}) or {}
    event_id = snapshot.get("event_id")
    home_name = (header.get("home_team") or {}).get("name")
    away_name = (header.get("away_team") or {}).get("name")
    match_name = header.get("match_name") or (f"{home_name} vs {away_name}" if home_name and away_name else None)
    return {
        "event_id": event_id,
        "fi": snapshot.get("fi"),
        "snapshot_id": snapshot.get("snapshot_id"),
        "snapshot_time_utc": snapshot.get("snapshot_time_utc"),
        "league_id": header.get("league_id"),
        "league_name": header.get("league_name"),
        "match_name": match_name,
        "home_team_name": home_name,
        "away_team_name": away_name,
        "score_summary": score.get("summary_from_events") or score.get("summary_from_bet365"),
        "odds_count": (page.get("odds") or {}).get("count"),
        "time_status": header.get("time_status"),
        "current_market_count": current_markets.get("market_count"),
        "current_market_selection_count": current_markets.get("selection_count"),
        "latest_gold_path": f"gold/cricket/matches/latest/event_id={event_id}/match_dashboard.json",
    }


def gold_write_index(gold_container, pages: List[Dict[str, Any]]) -> None:
    """Merge new pages into the existing latest index; prune stale/ended entries."""
    stale_hours = get_int_env("LIVE_MATCH_STALE_HOURS", 4)
    cutoff = utc_now() - timedelta(hours=stale_hours)

    existing_index = download_json(gold_container, "cricket/matches/latest/index.json") or {}
    latest_by_event: Dict[str, Dict[str, Any]] = {}

    if isinstance(existing_index, dict):
        for row in existing_index.get("matches", []):
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            if str(row.get("time_status") or "") == "3":
                continue
            snapshot_time = row.get("snapshot_time_utc")
            if snapshot_time:
                try:
                    dt = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00"))
                    if dt < cutoff:
                        continue
                except Exception:
                    pass
            latest_by_event[event_id] = row

    for page in pages:
        row = _build_match_index_row(page)
        event_id = str(row.get("event_id") or "")
        if not event_id:
            continue
        if str(row.get("time_status") or "") == "3":
            latest_by_event.pop(event_id, None)
            continue
        snapshot_time = row.get("snapshot_time_utc")
        if snapshot_time:
            try:
                dt = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00"))
                if dt < cutoff:
                    latest_by_event.pop(event_id, None)
                    continue
            except Exception:
                pass
        current = latest_by_event.get(event_id)
        if current is None or (row.get("snapshot_time_utc") or "") >= (current.get("snapshot_time_utc") or ""):
            latest_by_event[event_id] = row

    matches = list(latest_by_event.values())
    matches.sort(key=lambda x: x.get("snapshot_time_utc") or "", reverse=True)

    upload_json(
        gold_container,
        "cricket/matches/latest/index.json",
        {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches), "matches": matches},
        overwrite=True,
    )
    gold_write_league_indexes(gold_container, matches)


def gold_write_league_indexes(gold_container, matches: List[Dict[str, Any]]) -> None:
    leagues: Dict[str, Dict[str, Any]] = {}

    for match in matches:
        league_id = str(match.get("league_id") or "unknown")
        league_name = match.get("league_name") or "Unknown League"
        if league_id not in leagues:
            leagues[league_id] = {
                "league_id": league_id,
                "league_name": league_name,
                "match_count": 0,
                "latest_snapshot_time_utc": None,
                "matches": [],
            }
        leagues[league_id]["matches"].append(match)
        leagues[league_id]["match_count"] += 1
        snapshot_time = match.get("snapshot_time_utc") or ""
        if not leagues[league_id]["latest_snapshot_time_utc"] or snapshot_time > leagues[league_id]["latest_snapshot_time_utc"]:
            leagues[league_id]["latest_snapshot_time_utc"] = snapshot_time

    league_rows = []
    for league in leagues.values():
        league["matches"].sort(key=lambda x: x.get("snapshot_time_utc") or "", reverse=True)
        upload_json(
            gold_container,
            f"cricket/leagues/{league['league_id']}/matches.json",
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
            "latest_snapshot_time_utc": league["latest_snapshot_time_utc"],
            "matches_path": f"gold/cricket/leagues/{league['league_id']}/matches.json",
        })

    league_rows.sort(key=lambda x: x.get("latest_snapshot_time_utc") or "", reverse=True)
    upload_json(
        gold_container,
        "cricket/leagues/index.json",
        {"generated_at_utc": utc_now().isoformat(), "league_count": len(league_rows), "leagues": league_rows},
        overwrite=True,
    )


def gold_build_match_pages() -> None:
    """Timer trigger body: build gold match pages from newest silver snapshots."""
    max_events = get_int_env("MAX_GOLD_EVENTS_PER_RUN", 100)
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")

    all_paths = gold_list_latest_silver_match_snapshots(silver, max_events * 20)
    seen_events: Dict[str, str] = {}
    for path in all_paths:
        m = re.search(r"/event_id=([^/]+)/", path)
        if m:
            eid = m.group(1)
            if eid not in seen_events:
                seen_events[eid] = path
    match_snapshot_paths = list(seen_events.values())[:max_events]

    built_pages = []
    processed = failed = 0

    for match_snapshot_path in match_snapshot_paths:
        try:
            base_path = _base_path_from_match_snapshot_path(match_snapshot_path)
            match_snapshot = download_required_json(silver, f"{base_path}/match_state.json")
            match_snapshot["source_silver_match_snapshot_path"] = f"silver/{base_path}/match_state.json"
            match_snapshot["source_silver_lineage_path"] = f"silver/{base_path}/lineage.json"
            if not match_snapshot.get("api_lineage"):
                lineage_from_silver = download_json(silver, f"{base_path}/lineage.json")
                if lineage_from_silver:
                    match_snapshot["api_lineage"] = lineage_from_silver
            team_scores = download_required_json(silver, f"{base_path}/team_scores.json")
            player_entries = download_required_json(silver, f"{base_path}/player_entries.json")
            odds_records = download_required_json(silver, f"{base_path}/market_odds.json")
            try:
                current_markets = download_required_json(silver, f"{base_path}/active_markets.json")
            except ResourceNotFoundError:
                current_markets = {"rows": []}
            if not current_markets.get("rows"):
                event_id_for_control = match_snapshot.get("event_id")
                control_path = f"cricket/inplay/control/event_id={event_id_for_control}/last_known_markets.json"
                fallback = download_json(silver, control_path)
                if fallback and fallback.get("rows"):
                    current_markets = fallback
            match_page = gold_build_match_page(match_snapshot, team_scores, player_entries, odds_records, current_markets)
            gold_write_match_page(gold, match_page)
            try:
                gold_write_innings_tracker_from_silver(gold, silver, match_page)
            except Exception:
                logging.exception("Failed to write gold innings tracker")
            built_pages.append(match_page)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to build gold match page")

    if built_pages:
        gold_write_index(gold, built_pages)

    logging.info(json.dumps({
        "event": "gold_build_match_pages_completed",
        "processed": processed,
        "failed": failed,
        "checked": len(match_snapshot_paths),
    }))
