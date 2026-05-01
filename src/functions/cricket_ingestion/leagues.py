from typing import Any, Dict, List, Optional

from storage import download_json, get_named_container_client, upload_json, utc_now


_LEAGUE_PREFS_PATH = "cricket/config/league_preferences.json"


def load_excluded_league_ids() -> set:
    """Return a set of league_id strings that are excluded from capture."""
    gold = get_named_container_client("gold")
    prefs = download_json(gold, _LEAGUE_PREFS_PATH) or {}
    return set(str(lid) for lid in prefs.get("excluded_league_ids", []))


def save_league_preferences(excluded_ids: set) -> None:
    gold = get_named_container_client("gold")
    upload_json(
        gold,
        _LEAGUE_PREFS_PATH,
        {"excluded_league_ids": sorted(excluded_ids), "updated_at_utc": utc_now().isoformat()},
        overwrite=True,
    )


def collect_known_leagues() -> List[Dict[str, Any]]:
    """Gather all leagues seen across live, prematch, ended, league, and innings-tracker indexes."""
    gold = get_named_container_client("gold")
    leagues: Dict[str, Dict[str, Any]] = {}

    def merge(lid: Optional[str], lname: Optional[str], source: str) -> None:
        lid = str(lid or "").strip()
        if not lid:
            return
        if lid not in leagues:
            leagues[lid] = {"league_id": lid, "league_name": lname or lid, "sources": []}
        if source not in leagues[lid]["sources"]:
            leagues[lid]["sources"].append(source)
        if lname and lname != lid and not leagues[lid].get("league_name"):
            leagues[lid]["league_name"] = lname

    for idx_path, source in [
        ("cricket/matches/latest/index.json", "live"),
        ("cricket/prematch/latest/index.json", "prematch"),
        ("cricket/ended/latest/index.json", "ended"),
    ]:
        try:
            idx = download_json(gold, idx_path) or {}
            for m in (idx.get("matches") or []):
                merge(m.get("league_id"), m.get("league_name"), source)
        except Exception:
            pass

    for idx_path, source in [
        ("cricket/leagues/index.json", "live_leagues"),
        ("cricket/prematch/leagues/index.json", "prematch_leagues"),
    ]:
        try:
            idx = download_json(gold, idx_path) or {}
            for lg in (idx.get("leagues") or []):
                merge(lg.get("league_id"), lg.get("league_name"), source)
        except Exception:
            pass

    try:
        tracker_idx = download_json(gold, "cricket/innings_tracker/index.json") or {}
        for m in (tracker_idx.get("matches") or []):
            merge(m.get("league_id"), m.get("league_name"), "innings_tracker")
    except Exception:
        pass

    return sorted(leagues.values(), key=lambda x: x.get("league_name") or "")
