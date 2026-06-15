from typing import Any, Dict, List, Optional

from storage import download_json, get_named_container_client, upload_json, utc_now


_LEAGUE_PREFS_PATH = "cricket/config/league_preferences.json"
_BLOCKED_EVENTS_PATH = "cricket/config/blocked_event_ids.json"


def load_allowed_league_ids() -> set:
    """Return set of league_id strings explicitly allowed for capture.

    Opt-in model: any league NOT in this set is skipped at bronze capture time.
    An empty set means no leagues are allowed yet.
    """
    gold = get_named_container_client("gold")
    prefs = download_json(gold, _LEAGUE_PREFS_PATH) or {}
    return set(str(lid) for lid in prefs.get("allowed_league_ids", []))


def load_excluded_league_ids() -> set:
    """Legacy — kept so ended/prematch index builders can still use it.
    Returns an empty set (nothing explicitly excluded; opt-in now controls capture).
    """
    return set()


def save_league_preferences(allowed_ids: set) -> None:
    gold = get_named_container_client("gold")
    upload_json(
        gold,
        _LEAGUE_PREFS_PATH,
        {"allowed_league_ids": sorted(allowed_ids), "updated_at_utc": utc_now().isoformat()},
        overwrite=True,
    )


def load_blocked_event_ids() -> set:
    """Return a set of event_id strings permanently blocked from the ended index."""
    gold = get_named_container_client("gold")
    data = download_json(gold, _BLOCKED_EVENTS_PATH) or {}
    return set(str(eid) for eid in data.get("blocked_event_ids", []))


def block_event_ids(event_ids: set) -> None:
    """Add event_ids to the permanent block list."""
    gold = get_named_container_client("gold")
    existing = load_blocked_event_ids()
    merged = existing | {str(e) for e in event_ids}
    upload_json(
        gold,
        _BLOCKED_EVENTS_PATH,
        {"blocked_event_ids": sorted(merged), "updated_at_utc": utc_now().isoformat()},
        overwrite=True,
    )


_KNOWN_LEAGUES_PATH = "cricket/config/known_leagues.json"


def collect_known_leagues() -> List[Dict[str, Any]]:
    """Return leagues from gold/cricket/config/known_leagues.json.

    This file is maintained by the ingestion function (capture_inplay and
    capture_prematch) which writes to gold after each discovery run.
    Each entry carries league_id, league_name, sources (live/upcoming),
    first_seen_utc, and last_seen_utc.
    """
    gold = get_named_container_client("gold")
    data = download_json(gold, _KNOWN_LEAGUES_PATH) or {}
    leagues = [lg for lg in (data.get("leagues") or []) if lg.get("league_id")]
    return sorted(leagues, key=lambda x: x.get("league_name") or "")
