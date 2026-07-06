"""
odds_movement — core logic for the Odds Movement Analysis hypothesis.

Scans all gold innings_tracker.json files and computes per-match odds swing
metrics:
  - Peak odds reached by the home and away team at any point
  - Swing magnitude — how far the biggest peak went above 2.0 (even-money)
  - Double-opportunity flag — did BOTH teams ever exceed 2.0 in the same match?
  - Net profit potential if both peak-odds bets were placed on the winning side
  - Opening odds gap (from the first snapshot, as a proxy for pre-match proximity)
  - The over / innings where the biggest single-snapshot swing occurred

Aggregates by league and team, then returns a single output dict written to:
  gold/cricket/analysis/odds_movement_summary.json
"""

import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from util import download_json, get_named_container_client, upload_json


# Odds threshold below which we treat a team as effectively eliminated
# (very occasional spike near match end does not represent a real opportunity).
_MIN_ODDS_TO_COUNT = 1.05     # odds below this are almost certainties — ignore
_DOUBLE_OPP_THRESHOLD = 2.0  # both teams must exceed this to count as double-opportunity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scan_event_ids(gold) -> List[str]:
    return [
        b.name.split("/")[0].replace("event_id=", "")
        for b in gold.list_blobs(name_starts_with="event_id=")
        if b.name.endswith("/innings_tracker.json")
    ]


def _match_format(rows: List[Dict]) -> str:
    """Infer match format from the max completed over across all innings."""
    max_over = 0
    for r in rows:
        try:
            max_over = max(max_over, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    if max_over > 50:
        return "Test"
    if max_over > 20:
        return "ODI"
    if max_over >= 15:
        return "T20"
    return "Unknown"


def _is_t20(rows: List[Dict]) -> bool:
    """Legacy alias kept for callers outside this module."""
    return _match_format(rows) == "T20"


def _infer_winner(rows: List[Dict], home_team: str, away_team: str) -> Optional[str]:
    """
    Infer match winner from the final snapshot's odds.
    At match end the winning team has very low odds (near 1.0).
    Returns home_team, away_team, or None if undetermined.
    """
    if not rows:
        return None
    last = rows[-1]
    home_odds = last.get("home_team_odds")
    away_odds = last.get("away_team_odds")
    if home_odds is None or away_odds is None:
        return None
    if home_odds < 1.3 and away_odds > 3.0:
        return home_team
    if away_odds < 1.3 and home_odds > 3.0:
        return away_team
    return None


def _process_match(tracker: Dict) -> Optional[Dict]:
    """Compute odds swing metrics for one match. Returns None if unusable."""
    rows: List[Dict] = tracker.get("rows") or []
    fmt = _match_format(rows)
    if not rows or fmt == "Unknown":
        return None   # too few overs captured to be useful

    home_team  = tracker.get("home_team_name") or ""
    away_team  = tracker.get("away_team_name") or ""
    if not home_team or not away_team:
        return None

    # Collect all valid home/away odds snapshots
    home_peak = 1.0
    away_peak = 1.0
    home_open = None   # opening odds (first snapshot with valid odds)
    away_open = None
    swing_over = None
    swing_inn  = None
    prev_home  = None
    peak_home_row: Optional[Dict] = None   # row where home peaked
    peak_away_row: Optional[Dict] = None   # row where away peaked

    for r in rows:
        h = r.get("home_team_odds")
        a = r.get("away_team_odds")
        if h is None or a is None or h < _MIN_ODDS_TO_COUNT or a < _MIN_ODDS_TO_COUNT:
            continue

        if home_open is None:
            home_open = h
            away_open = a

        # Track peak odds per team and capture the row for context
        if h > home_peak:
            if prev_home is not None and h - prev_home > 0.5:
                swing_over = r.get("over")
                swing_inn  = r.get("innings", 1)
            home_peak     = h
            peak_home_row = r

        if a > away_peak:
            away_peak     = a
            peak_away_row = r

        prev_home = h

    if home_open is None:
        return None  # no valid odds captured at all

    def _row_state(r: Optional[Dict]) -> Dict:
        if r is None:
            return {"innings": None, "over": None, "score": None, "wickets": None}
        return {
            "innings": r.get("innings", 1),
            "over":    r.get("over"),
            "score":   r.get("score"),
            "wickets": r.get("wickets"),
        }

    # Final scores — handle both int and string innings field, and both
    # "score" (older trackers) and "runs" (newer trackers) field names.
    def _inn_num(r: Dict) -> Optional[int]:
        v = r.get("innings")
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    def _score_of(r: Dict) -> Optional[int]:
        """Return cumulative score from row, trying both field names."""
        v = r.get("runs") if r.get("runs") is not None else r.get("score")
        try:
            return int(str(v).split("/")[0]) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _wickets_of(r: Dict) -> Optional[int]:
        v = r.get("wickets")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    inn1_rows = [r for r in rows if _inn_num(r) == 1]
    inn2_rows = [r for r in rows if _inn_num(r) == 2]
    final_inn1_score   = _score_of(inn1_rows[-1])   if inn1_rows else None
    final_inn1_wickets = _wickets_of(inn1_rows[-1]) if inn1_rows else None
    final_inn2_score   = _score_of(inn2_rows[-1])   if inn2_rows else None
    final_inn2_wickets = _wickets_of(inn2_rows[-1]) if inn2_rows else None

    # Venue
    _sd    = tracker.get("stadium_data") or {}
    venue  = str(tracker.get("venue") or _sd.get("name") or "").strip()

    # Swing = how far the biggest peak went above even-money
    max_peak  = max(home_peak, away_peak)
    swing_mag = round(max(0.0, max_peak - _DOUBLE_OPP_THRESHOLD), 3)

    double_opp = home_peak > _DOUBLE_OPP_THRESHOLD and away_peak > _DOUBLE_OPP_THRESHOLD

    winner = _infer_winner(rows, home_team, away_team)
    net_profit: Optional[float] = None
    if double_opp and winner is not None:
        winning_peak = home_peak if winner == home_team else away_peak
        net_profit = round(winning_peak - 2.0, 3)

    opening_gap = round(abs(home_open - away_open), 3) if home_open and away_open else None

    return {
        "event_id":         tracker.get("event_id") or "",
        "match_name":       tracker.get("match_name") or "",
        "match_date_utc":   (tracker.get("match_date_utc") or "")[:10],
        "league_id":        tracker.get("league_id"),
        "league_name":      tracker.get("league_name") or "",
        "venue":            venue,
        "format":           fmt,
        "home_team":        home_team,
        "away_team":        away_team,
        "winner":           winner,
        # Final scores
        "final_inn1_score":   final_inn1_score,
        "final_inn1_wickets": final_inn1_wickets,
        "final_inn2_score":   final_inn2_score,
        "final_inn2_wickets": final_inn2_wickets,
        # Peak odds
        "peak_home_odds":   round(home_peak, 3),
        "peak_away_odds":   round(away_peak, 3),
        # State when peak odds occurred
        "peak_home_at":     _row_state(peak_home_row),
        "peak_away_at":     _row_state(peak_away_row),
        # Opening odds
        "opening_home_odds": round(home_open, 3) if home_open else None,
        "opening_away_odds": round(away_open, 3) if away_open else None,
        "opening_odds_gap": opening_gap,
        "max_swing":        swing_mag,
        "double_opportunity": double_opp,
        "net_profit_if_both_backed": net_profit,
        "swing_at_over":    swing_over,
        "swing_innings":    swing_inn,
    }


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _aggregate_by_league(matches: List[Dict]) -> List[Dict]:
    stats: Dict[str, Dict] = defaultdict(lambda: {
        "match_count": 0, "swing_total": 0.0,
        "double_opp_count": 0, "max_swing": 0.0,
    })
    for m in matches:
        ln = m["league_name"] or "Unknown"
        s  = stats[ln]
        s["match_count"]      += 1
        s["swing_total"]      += m["max_swing"]
        s["max_swing"]         = max(s["max_swing"], m["max_swing"])
        if m["double_opportunity"]:
            s["double_opp_count"] += 1

    result = []
    for league, s in sorted(stats.items(), key=lambda x: -x[1]["match_count"]):
        n = s["match_count"]
        result.append({
            "league_name":             league,
            "match_count":             n,
            "avg_swing":               round(s["swing_total"] / n, 3) if n else 0,
            "max_swing":               round(s["max_swing"], 3),
            "double_opp_count":        s["double_opp_count"],
            "pct_double_opportunity":  round(100 * s["double_opp_count"] / n, 1) if n else 0,
        })
    return sorted(result, key=lambda x: -x["avg_swing"])


def _aggregate_by_team(matches: List[Dict]) -> List[Dict]:
    stats: Dict[str, Dict] = defaultdict(lambda: {
        "match_count": 0, "swing_total": 0.0,
        "double_opp_count": 0,
        "peak_as_home_total": 0.0, "peak_as_away_total": 0.0,
        "as_home_count": 0, "as_away_count": 0,
    })
    for m in matches:
        for team, role in [(m["home_team"], "home"), (m["away_team"], "away")]:
            if not team:
                continue
            s = stats[team]
            s["match_count"]   += 1
            s["swing_total"]   += m["max_swing"]
            if m["double_opportunity"]:
                s["double_opp_count"] += 1
            if role == "home":
                s["peak_as_home_total"] += m["peak_home_odds"]
                s["as_home_count"]      += 1
            else:
                s["peak_as_away_total"] += m["peak_away_odds"]
                s["as_away_count"]      += 1

    result = []
    for team, s in stats.items():
        n = s["match_count"]
        result.append({
            "team_name":           team,
            "match_count":         n,
            "avg_swing":           round(s["swing_total"] / n, 3) if n else 0,
            "double_opp_count":    s["double_opp_count"],
            "pct_double_opp":      round(100 * s["double_opp_count"] / n, 1) if n else 0,
            "avg_peak_odds_as_home": round(
                s["peak_as_home_total"] / s["as_home_count"], 3
            ) if s["as_home_count"] else None,
            "avg_peak_odds_as_away": round(
                s["peak_as_away_total"] / s["as_away_count"], 3
            ) if s["as_away_count"] else None,
        })
    return sorted(result, key=lambda x: -x["avg_swing"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_odds_movement(event_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Main entry point. Returns the full summary dict:
      - matches:      list of per-match metrics sorted by max_swing desc
      - by_league:    aggregated by league
      - by_team:      aggregated by team
      - metadata:     counts, max_swing ever, avg_swing, generated_at
    """
    gold = get_named_container_client("gold")

    if event_id:
        eids = [event_id]
    else:
        eids = _scan_event_ids(gold)

    def _load_and_process(eid: str) -> Optional[Dict]:
        tracker = download_json(gold, f"event_id={eid}/innings_tracker.json") or {}
        if not tracker:
            return None
        return _process_match(tracker)

    matches: List[Dict] = []
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(_load_and_process, eid): eid for eid in eids}
        for fut in as_completed(futs):
            result = fut.result()
            if result is not None:
                matches.append(result)

    matches.sort(key=lambda m: -m["max_swing"])

    n = len(matches)
    double_opp_count = sum(1 for m in matches if m["double_opportunity"])
    avg_swing  = round(sum(m["max_swing"] for m in matches) / n, 3) if n else 0.0
    max_swing  = round(max((m["max_swing"] for m in matches), default=0.0), 3)

    return {
        "generated_at_utc":       datetime.now(timezone.utc).isoformat(),
        "total_matches":          n,
        "double_opportunity_count": double_opp_count,
        "pct_double_opportunity": round(100 * double_opp_count / n, 1) if n else 0,
        "avg_swing":              avg_swing,
        "max_swing_ever":         max_swing,
        "matches":                matches,
        "by_league":              _aggregate_by_league(matches),
        "by_team":                _aggregate_by_team(matches),
    }
