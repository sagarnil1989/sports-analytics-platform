import re
from typing import Any, Dict, List, Optional

from storage import (
    _is_innings_market,
    download_json,
    upload_json,
    utc_now,
)


_OVERS_RE = re.compile(r'(?<!\d)(\d+)\.([0-5])(?!\d)')


def _over_key(over: Optional[str]) -> float:
    """Normalize over for dedup so None, '0', '0.0' all compare equal."""
    if over is None:
        return 0.0
    try:
        return round(float(over), 1)
    except (ValueError, TypeError):
        return 0.0


def parse_over_from_pg(pg: str) -> Optional[str]:
    """Extract current over from Bet365 PG field.

    Format: 'B1:B2:B3:B4:B5:B6#N:W:B'
      N = current over (1-indexed)
      W = wickets visible in ball window (ignored for over calculation)
      B = legal balls bowled in current over (excludes wides/no-balls)

    current_over = (N-1).B
    e.g. #7:5:3 → N=7, B=3 → over 6.3
    e.g. #16:6:5 → over 15.5
    e.g. #5:1:0  → over 4.0 (over just completed)
    """
    if not pg:
        return None
    try:
        parts = pg.split("#")
        if len(parts) < 2:
            return None
        stats = parts[-1].split(":")
        if len(stats) < 3:
            return None
        n = int(stats[0])  # current over, 1-indexed
        b = int(stats[2])  # legal balls in current over
        completed_overs = n - 1
        if completed_overs < 0 or b < 0:
            return None
        return f"{completed_overs}.{b}"
    except (ValueError, IndexError):
        return None


def parse_overs_string(text: Optional[str]) -> Optional[str]:
    """Return overs like '17.3' from text like '185/5 (17.3 ov)'. Returns None if not found."""
    if not text:
        return None
    m = _OVERS_RE.search(str(text))
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return None


def extract_innings_snapshot(
    match_snapshot: Dict[str, Any],
    team_score_rows: List[Dict[str, Any]],
    current_market_rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build one innings tracker data point from the current match state.

    Returns None if there is not enough data (no over or score).
    """
    home_team = str(match_snapshot.get("home_team_name") or "")
    away_team = str(match_snapshot.get("away_team_name") or "")

    current_over: Optional[str] = None
    current_score: Optional[int] = None
    current_wickets: Optional[int] = None
    batting_team: Optional[str] = None

    for row in team_score_rows:
        over_str = str(row.get("pg_over") or "").strip() or None
        if not over_str:
            continue
        current_over = over_str
        batting_team = str(row.get("name") or "")
        current_score = row.get("runs")
        current_wickets = row.get("wickets")
        break

    if current_score is None:
        return None

    bowling_team = (away_team if batting_team == home_team else home_team) if batting_team else away_team

    innings_rows = [
        r for r in current_market_rows
        if r.get("market_group_name") and _is_innings_market(r["market_group_name"])
    ]

    _SIMPLE_OVER_RE = re.compile(r'^[Oo]ver\s+([\d.]+)$')
    _SIMPLE_UNDER_RE = re.compile(r'^[Uu]nder\s+([\d.]+)$')

    by_line: Dict[str, Dict[str, Optional[float]]] = {}
    for r in innings_rows:
        sel_name = str(r.get("display_selection_name") or r.get("selection_name") or "").strip()
        m_ov = _SIMPLE_OVER_RE.match(sel_name)
        m_un = _SIMPLE_UNDER_RE.match(sel_name)
        if m_ov:
            line_key = m_ov.group(1)
            by_line.setdefault(line_key, {})["over"] = r.get("odds_decimal")
        elif m_un:
            line_key = m_un.group(1)
            by_line.setdefault(line_key, {})["under"] = r.get("odds_decimal")

    predicted_total: Optional[float] = None
    over_odds_at_line: Optional[float] = None
    under_odds_at_line: Optional[float] = None
    best_diff = float("inf")
    best_line_val = -1.0
    for line_str, ln_odds in by_line.items():
        ov = ln_odds.get("over")
        un = ln_odds.get("under")
        if ov is None or un is None:
            continue
        diff = abs(ov - un)
        try:
            line_val = float(line_str)
        except Exception:
            continue
        if diff < best_diff or (diff == best_diff and line_val > best_line_val):
            best_diff = diff
            best_line_val = line_val
            predicted_total = line_val
            over_odds_at_line = ov
            under_odds_at_line = un

    winner_keywords = ["full time", "match betting", "match winner", "to win the match", "win match"]
    winner_rows = [
        r for r in current_market_rows
        if r.get("market_group_name") and any(
            kw in r["market_group_name"].lower() for kw in winner_keywords
        ) and r.get("odds_decimal") and not r.get("suspended")
    ]

    home_odds: Optional[float] = None
    away_odds: Optional[float] = None
    for r in winner_rows:
        sel = str(r.get("selection_name") or "").strip().lower()
        od = r.get("odds_decimal")
        if home_team and home_team.lower() in sel:
            home_odds = od
        elif away_team and away_team.lower() in sel:
            away_odds = od
    if home_odds is None and len(winner_rows) >= 1:
        home_odds = winner_rows[0].get("odds_decimal")
    if away_odds is None and len(winner_rows) >= 2:
        away_odds = winner_rows[1].get("odds_decimal")

    batting_odds = (home_odds if batting_team == home_team else away_odds) if batting_team else home_odds
    bowling_odds = (away_odds if batting_team == home_team else home_odds) if batting_team else away_odds

    innings_market_name = innings_rows[0].get("market_group_name") if innings_rows else None
    return {
        "over": current_over,
        "over_float": current_over,
        "score": current_score,
        "wickets": current_wickets,
        "batting_team": batting_team or home_team,
        "bowling_team": bowling_team or away_team,
        "batting_team_odds": batting_odds,
        "bowling_team_odds": bowling_odds,
        "home_team_odds": home_odds,
        "away_team_odds": away_odds,
        "predicted_total": predicted_total,
        "over_odds_at_line": over_odds_at_line,
        "under_odds_at_line": under_odds_at_line,
        "innings_market_name": innings_market_name,
        "snapshot_id": match_snapshot.get("snapshot_id"),
        "snapshot_time_utc": match_snapshot.get("snapshot_time_utc"),
    }


def gold_write_innings_tracker_from_silver(
    gold_container,
    silver_container,
    match_page: Dict[str, Any],
) -> None:
    """Promote the silver innings accumulator to gold, adding outcome detection."""
    snapshot = match_page.get("snapshot") or {}
    header = match_page.get("match_header") or {}
    event_id = str(snapshot.get("event_id") or "")
    if not event_id:
        return

    acc_path = f"cricket/inplay/control/event_id={event_id}/innings_accumulator.json"
    acc = download_json(silver_container, acc_path) or {}
    rows: List[Dict[str, Any]] = acc.get("rows", [])

    time_status = str(header.get("time_status") or "")
    score = match_page.get("score") or {}
    score_summary = score.get("summary_from_events") or score.get("summary_from_bet365") or ""
    outcome: Optional[str] = acc.get("outcome")
    actual_total: Optional[int] = acc.get("actual_total")
    if time_status == "3" and outcome is None and rows:
        import re as _re
        m_ss = _re.match(r'^(\d+)', str(score_summary))
        if m_ss:
            actual_total = int(m_ss.group(1))
            last_predicted = rows[-1].get("predicted_total")
            if last_predicted is not None:
                if actual_total > last_predicted:
                    outcome = "over"
                elif actual_total < last_predicted:
                    outcome = "under"
                else:
                    outcome = "push"

    match_name = acc.get("match_name") or header.get("match_name")
    venue = acc.get("venue") or header.get("venue")
    tracker = {
        "event_id": event_id,
        "match_name": match_name,
        "venue": venue,
        "match_date_utc": acc.get("match_date_utc") or header.get("event_time_utc"),
        "league_id": acc.get("league_id") or header.get("league_id"),
        "league_name": acc.get("league_name") or header.get("league_name"),
        "home_team_name": acc.get("home_team_name") or (header.get("home_team") or {}).get("name"),
        "away_team_name": acc.get("away_team_name") or (header.get("away_team") or {}).get("name"),
        "rows": rows,
        "outcome": outcome,
        "actual_total": actual_total,
        "last_updated_utc": utc_now().isoformat(),
    }

    tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
    upload_json(gold_container, tracker_path, tracker, overwrite=True)

    index_path = "cricket/innings_tracker/index.json"
    idx = download_json(gold_container, index_path) or {}
    matches_idx: Dict[str, Dict] = {str(m.get("event_id") or ""): m for m in idx.get("matches", [])}
    matches_idx[event_id] = {
        "event_id": event_id,
        "match_name": match_name,
        "venue": venue,
        "match_date_utc": tracker["match_date_utc"],
        "league_id": tracker["league_id"],
        "league_name": tracker["league_name"],
        "home_team_name": tracker["home_team_name"],
        "away_team_name": tracker["away_team_name"],
        "row_count": len(rows),
        "has_outcome": outcome is not None,
        "outcome": outcome,
        "actual_total": actual_total,
        "tracker_path": f"gold/{tracker_path}",
    }
    upload_json(
        gold_container,
        index_path,
        {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches_idx), "matches": list(matches_idx.values())},
        overwrite=True,
    )


def update_innings_tracker(
    gold_container,
    match_page: Dict[str, Any],
    team_score_rows: List[Dict[str, Any]],
    current_market_rows: List[Dict[str, Any]],
) -> None:
    """Append a tracker row when over/score changed. Resolve outcome when match ends."""
    snapshot = match_page.get("snapshot") or {}
    header = match_page.get("match_header") or {}
    event_id = str(snapshot.get("event_id") or "")
    if not event_id:
        return

    tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
    existing = download_json(gold_container, tracker_path) or {}
    rows: List[Dict[str, Any]] = existing.get("rows", [])

    time_status = str(header.get("time_status") or "")
    score = match_page.get("score") or {}
    score_summary = score.get("summary_from_events") or score.get("summary_from_bet365") or ""

    outcome = existing.get("outcome")
    actual_total = existing.get("actual_total")
    if time_status == "3" and outcome is None and rows:
        import re as _re
        m_ss = _re.match(r'^(\d+)', str(score_summary))
        if m_ss:
            actual_total = int(m_ss.group(1))
            last_predicted = rows[-1].get("predicted_total")
            if last_predicted is not None and actual_total is not None:
                if actual_total > last_predicted:
                    outcome = "over"
                elif actual_total < last_predicted:
                    outcome = "under"
                else:
                    outcome = "push"

    if time_status != "3":
        ms_for_tracker = {
            "snapshot_id": snapshot.get("snapshot_id"),
            "snapshot_time_utc": snapshot.get("snapshot_time_utc"),
            "event_id": event_id,
            "home_team_name": (header.get("home_team") or {}).get("name"),
            "away_team_name": (header.get("away_team") or {}).get("name"),
            "score_summary_events": score.get("summary_from_events"),
            "score_summary_bet365": score.get("summary_from_bet365"),
            "venue": header.get("venue"),
        }
        data_point = extract_innings_snapshot(ms_for_tracker, team_score_rows, current_market_rows)
        if data_point is not None:
            last = rows[-1] if rows else None
            if last is None or _over_key(last.get("over")) != _over_key(data_point.get("over")) or last.get("score") != data_point.get("score"):
                rows.append(data_point)

    tracker = {
        "event_id": event_id,
        "match_name": header.get("match_name"),
        "venue": header.get("venue"),
        "match_date_utc": header.get("event_time_utc"),
        "league_id": header.get("league_id"),
        "league_name": header.get("league_name"),
        "home_team_name": (header.get("home_team") or {}).get("name"),
        "away_team_name": (header.get("away_team") or {}).get("name"),
        "rows": rows,
        "outcome": outcome,
        "actual_total": actual_total,
        "last_updated_utc": utc_now().isoformat(),
    }
    upload_json(gold_container, tracker_path, tracker, overwrite=True)

    index_path = "cricket/innings_tracker/index.json"
    idx = download_json(gold_container, index_path) or {}
    matches_idx: Dict[str, Dict] = {str(m.get("event_id") or ""): m for m in idx.get("matches", [])}
    matches_idx[event_id] = {
        "event_id": event_id,
        "match_name": tracker["match_name"],
        "venue": tracker["venue"],
        "match_date_utc": tracker["match_date_utc"],
        "league_id": tracker["league_id"],
        "league_name": tracker["league_name"],
        "home_team_name": tracker["home_team_name"],
        "away_team_name": tracker["away_team_name"],
        "row_count": len(rows),
        "has_outcome": outcome is not None,
        "outcome": outcome,
        "actual_total": actual_total,
        "tracker_path": f"gold/{tracker_path}",
    }
    upload_json(
        gold_container,
        index_path,
        {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches_idx), "matches": list(matches_idx.values())},
        overwrite=True,
    )
