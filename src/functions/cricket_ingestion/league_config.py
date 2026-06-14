from typing import Any, Dict, List, Optional

from util import download_json, get_named_container_client, upload_json, utc_now


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


def load_blocked_event_ids() -> set:
    """Return a set of event_id strings permanently blocked from the ended index."""
    gold = get_named_container_client("gold")
    data = download_json(gold, _BLOCKED_EVENTS_PATH) or {}
    return set(str(eid) for eid in data.get("blocked_event_ids", []))
