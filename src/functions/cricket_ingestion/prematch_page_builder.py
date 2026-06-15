import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from util import (
    download_json,
    download_required_json,
    format_unix_ts,
    get_bronze_container_client,
    get_env,
    get_int_env,
    get_named_container_client,
    safe_float,
    upload_json,
    utc_now,
)


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


# ------------------------------------------------------------------
# Gold index writers
# ------------------------------------------------------------------

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


def gold_build_prematch_pages(sport_id: Optional[str] = None, max_per_run: int = 500) -> None:
    """Read bronze prematch snapshots and write gold/cricket/prematch/ index files.

    Called by the Databricks notebook gold_build_prematch_pages.py via ADF
    pipeline pl_build_prematch_pages. Runs daily after capture_cricket_prematch_odds
    has written bronze snapshots.
    """
    sport_id = sport_id or get_env("SPORT_ID", "3")
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

    print(json.dumps({
        "event": "gold_build_prematch_pages_completed",
        "processed": processed,
        "failed": failed,
        "checked": len(manifest_paths),
    }))
