"""Cricket hypothesis data extraction.

Hypothesis 1 — Inn2 Over-6 Favourite Wins:
  During a chase, whichever team is the match-winner odds favourite after the
  end of the 6th over of the 2nd innings, does that team always win?

Hypothesis 2 — Strategic Timeout Wicket:
  After a strategic timeout (game state unchanged for > 2 minutes), a wicket
  falls in the next over when play resumes.

Hypothesis 3 — Inn1 Pre-Match Score Over/Under:
  The bet365 pre-match "1st Innings Score" Over/Under line (set before a ball
  is bowled) — does the actual innings-1 total tend to land on one side of it?
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from util import (
    download_json,
    get_named_container_client,
    upload_json,
    utc_now,
)


def _is_t20_tracker(tracker: Dict[str, Any]) -> bool:
    """Detect T20 by max over in tracker rows (15–20), not match name.

    IPL, BBL etc. don't include 't20' in their match names so name-matching
    silently excludes them. Over-count is the ground truth.
    """
    rows = tracker.get("rows") or []
    max_over = 0
    for r in rows:
        try:
            max_over = max(max_over, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    return 15 <= max_over <= 20


def _is_womens_match(match_name: str) -> bool:
    n = match_name.lower()
    return "women" in n or "(w)" in n


def _scan_ended_event_ids(gold) -> List[str]:
    """Return all event_ids that have an innings_tracker.json in gold."""
    event_ids = []
    for blob in gold.list_blobs(name_starts_with="event_id="):
        if not blob.name.endswith("/innings_tracker.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if eid_part:
            event_ids.append(eid_part[9:])
    return event_ids


def extract_inn2_over6_favorite(event_id: Optional[str] = None) -> Dict[str, Any]:
    """Process T20 matches and write per-event hypothesis_inn2_over6.json to gold.

    If event_id is given, processes only that event. Otherwise scans all ended events.
    Returns aggregate summary.
    """
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")

    if event_id:
        event_ids_to_process = [event_id]
    else:
        all_ids = _scan_ended_event_ids(gold)
        event_ids_to_process = []
        for eid in all_ids:
            tracker = download_json(gold, f"event_id={eid}/innings_tracker.json") or {}
            if _is_t20_tracker(tracker):
                event_ids_to_process.append(eid)

    logging.info(json.dumps({"event": "hypothesis_inn2_over6_start", "event_count": len(event_ids_to_process)}))

    results = []
    for eid in event_ids_to_process:
        try:
            row = _process_event(eid, silver, gold)
            if row:
                upload_json(
                    gold,
                    f"event_id={eid}/hypothesis_inn2_over6.json",
                    {"generated_at_utc": utc_now().isoformat(), **row},
                    overwrite=True,
                )
                results.append(row)
        except Exception:
            logging.exception(json.dumps({"event": "hypothesis_event_error", "event_id": eid}))

    results.sort(key=lambda r: r.get("match_name", ""))

    favoured = sum(1 for r in results if r.get("favorite_won") is True)
    not_favoured = sum(1 for r in results if r.get("favorite_won") is False)
    no_odds = sum(1 for r in results if r.get("favorite_won") is None)
    eligible = favoured + not_favoured

    output: Dict[str, Any] = {
        "generated_at_utc": utc_now().isoformat(),
        "hypothesis": "During a chase, the team with lower match-winner odds after 6 overs of the 2nd innings wins the match.",
        "total_matches": len(results),
        "eligible_matches": eligible,
        "favorite_won_count": favoured,
        "favorite_lost_count": not_favoured,
        "no_odds_data_count": no_odds,
        "favorite_win_pct": round(100.0 * favoured / eligible, 1) if eligible > 0 else None,
        "results": results,
    }

    logging.info(json.dumps({
        "event": "hypothesis_inn2_over6_done",
        "total": len(results),
        "favorite_won": favoured,
        "favorite_lost": not_favoured,
        "win_pct": output["favorite_win_pct"],
    }))
    return output


def _load_accumulator_rows(event_id: str, silver) -> List[Dict]:
    """Load innings rows from silver accumulator."""
    acc = download_json(silver, f"event_id={event_id}/innings_accumulator.json") or {}
    return acc.get("rows") or []


def _process_event(
    event_id: str,
    silver,
    gold,
) -> Optional[Dict[str, Any]]:
    """Extract hypothesis row for one ended match. Returns None if data is insufficient."""
    import re as _re

    tracker = download_json(gold, f"event_id={event_id}/innings_tracker.json") or {}

    match_name = tracker.get("match_name") or f"event_{event_id}"
    tracker_home = tracker.get("home_team_name") or ""
    tracker_away = tracker.get("away_team_name") or ""

    tracker_rows = tracker.get("rows") or []
    inn1_tracker_rows = [r for r in tracker_rows if r.get("innings") == 1]
    batting_first_team = next((r["batting_team"] for r in inn1_tracker_rows if r.get("batting_team")), "") or ""

    rows = _load_accumulator_rows(event_id, silver)

    inn1_rows = [r for r in rows if r.get("innings") == 1]
    inn2_rows_raw = [r for r in rows if r.get("innings") == 2]

    raw_summary = (tracker.get("score_summary_events") or tracker.get("score_summary") or "").replace("-", ",").strip()
    actual_total: Optional[int] = None
    final_innings2_score: Optional[int] = None
    final_innings2_wickets: Optional[int] = None
    inn1_final_display: Optional[str] = None
    if raw_summary and "," in raw_summary:
        parts = [p.strip() for p in raw_summary.split(",", 1)]
        if batting_first_team and tracker_away and batting_first_team == tracker_away:
            parts = [parts[1], parts[0]]
        try:
            m0 = _re.match(r"(\d+)(?:/(\d+))?\(?(\d+\.?\d*)?\)?", parts[0])
            m1 = _re.match(r"(\d+)(?:/(\d+))?", parts[1])
            if m0 and m1:
                actual_total = int(m0.group(1))
                final_innings2_score = int(m1.group(1))
                final_innings2_wickets = int(m1.group(2)) if m1.group(2) else None
                inn1_wkts_str = f"/{m0.group(2)}" if m0.group(2) else ""
                inn1_overs_str = m0.group(3) if m0.group(3) else None
                inn1_final_display = (
                    f"{actual_total}{inn1_wkts_str} ({inn1_overs_str} ov)"
                    if inn1_overs_str else f"{actual_total}{inn1_wkts_str}"
                )
        except Exception:
            pass

    if not actual_total:
        return None

    target = actual_total + 1
    match_date = str(tracker.get("match_date_utc") or "")[:10]

    home_team = tracker_home
    away_team = tracker_away
    chasing_team = away_team if batting_first_team == home_team else home_team

    inn1_sorted = sorted(inn1_rows, key=lambda r: float(r.get("over") or 0))

    genuine_found = False
    inn2: List[Dict] = []
    for r in sorted(inn2_rows_raw, key=lambda r: r.get("snapshot_time_utc") or ""):
        score = r.get("score") or 0
        over_f = float(r.get("over") or 0)
        if not genuine_found:
            if over_f < 0.1 and score == 0:
                genuine_found = True
                inn2.append(r)
        else:
            inn2.append(r)

    if not inn2:
        return None

    inn1_over6 = next((r for r in inn1_sorted if float(r.get("over") or 0) >= 6.0), None)
    inn1_score_at_over6 = inn1_over6.get("score") if inn1_over6 else None
    inn1_wickets_at_over6 = inn1_over6.get("wickets") if inn1_over6 else None

    inn2_sorted = sorted(inn2, key=lambda r: float(r.get("over") or 0))
    over6_row = next((r for r in inn2_sorted if float(r.get("over") or 0) >= 6.0), None)
    if not over6_row:
        return None

    home_odds = over6_row.get("home_team_odds")
    away_odds = over6_row.get("away_team_odds")

    favorite_at_over6: Optional[str] = None
    if home_odds and away_odds:
        if home_odds < away_odds:
            favorite_at_over6 = home_team
        elif away_odds < home_odds:
            favorite_at_over6 = away_team
        else:
            favorite_at_over6 = "Even"

    if final_innings2_wickets is not None and final_innings2_wickets >= 10:
        actual_winner = chasing_team if (final_innings2_score or 0) > actual_total else batting_first_team
    else:
        actual_winner = chasing_team if (final_innings2_score or 0) >= actual_total else batting_first_team

    favorite_won: Optional[bool] = None
    if favorite_at_over6 and favorite_at_over6 != "Even":
        favorite_won = (favorite_at_over6 == actual_winner)

    chasing_odds = home_odds if chasing_team == home_team else away_odds
    batting_first_odds = home_odds if batting_first_team == home_team else away_odds

    return {
        "event_id": event_id,
        "match_date": match_date,
        "match_name": match_name,
        "league_name": tracker.get("league_name") or "",
        "is_womens_match": _is_womens_match(match_name),
        "batting_first_team": batting_first_team,
        "chasing_team": chasing_team,
        "target": target,
        "inn1_final_score": actual_total,
        "inn1_final_display": inn1_final_display,
        "inn1_score_at_over6": inn1_score_at_over6,
        "inn1_wickets_at_over6": inn1_wickets_at_over6,
        "over_reached": over6_row.get("over"),
        "score_at_over6": over6_row.get("score"),
        "wickets_at_over6": over6_row.get("wickets"),
        "chasing_team_odds_at_over6": chasing_odds,
        "batting_first_team_odds_at_over6": batting_first_odds,
        "favorite_at_over6": favorite_at_over6,
        "final_innings2_wickets": final_innings2_wickets,
        "actual_winner": actual_winner,
        "favorite_won": favorite_won,
        "final_innings2_score": final_innings2_score,
    }


# ---------------------------------------------------------------------------
# Hypothesis 2 — Strategic Timeout Wicket
# ---------------------------------------------------------------------------

TIMEOUT_THRESHOLD_SECONDS = 180  # 3 min gap = strategic timeout (wicket falls take ~2–2.5 min)


def extract_timeout_wicket(event_id: Optional[str] = None) -> Dict[str, Any]:
    """Process T20 matches and write per-event hypothesis_timeout_wicket.json to gold.

    If event_id is given, processes only that event. Otherwise scans all ended events.
    Returns aggregate summary.
    """
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")

    if event_id:
        event_ids_to_process = [event_id]
    else:
        all_ids = _scan_ended_event_ids(gold)
        event_ids_to_process = []
        for eid in all_ids:
            tracker_check = download_json(gold, f"event_id={eid}/innings_tracker.json") or {}
            if _is_t20_tracker(tracker_check):
                event_ids_to_process.append(eid)

    all_timeouts: List[Dict[str, Any]] = []

    for eid in event_ids_to_process:
        tracker = download_json(gold, f"event_id={eid}/innings_tracker.json") or {}
        match_name = tracker.get("match_name") or f"event_{eid}"
        rows = _load_accumulator_rows(eid, silver)
        if not rows:
            continue
        try:
            timeouts = _find_timeouts(eid, match_name, rows)
            if timeouts:
                upload_json(
                    gold,
                    f"event_id={eid}/hypothesis_timeout_wicket.json",
                    {
                        "generated_at_utc": utc_now().isoformat(),
                        "event_id": eid,
                        "match_name": match_name,
                        "timeouts": timeouts,
                    },
                    overwrite=True,
                )
            all_timeouts.extend(timeouts)
        except Exception:
            logging.exception(json.dumps({"event": "timeout_hypothesis_error", "event_id": eid}))

    total = len(all_timeouts)
    wicket_yes = sum(1 for t in all_timeouts if t.get("wicket_in_resumed_over") is True)
    wicket_no = sum(1 for t in all_timeouts if t.get("wicket_in_resumed_over") is False)
    unknown = sum(1 for t in all_timeouts if t.get("wicket_in_resumed_over") is None)
    eligible = wicket_yes + wicket_no

    output: Dict[str, Any] = {
        "generated_at_utc": utc_now().isoformat(),
        "hypothesis": "After a strategic timeout (game paused > 2 min), a wicket falls in the over that immediately resumes.",
        "timeout_threshold_seconds": TIMEOUT_THRESHOLD_SECONDS,
        "total_timeouts_detected": total,
        "eligible_timeouts": eligible,
        "wicket_in_resumed_over_count": wicket_yes,
        "no_wicket_count": wicket_no,
        "unknown_count": unknown,
        "wicket_pct": round(100.0 * wicket_yes / eligible, 1) if eligible > 0 else None,
        "results": all_timeouts,
    }

    logging.info(json.dumps({
        "event": "timeout_wicket_hypothesis_done",
        "total_timeouts": total,
        "wicket_yes": wicket_yes,
        "wicket_no": wicket_no,
        "wicket_pct": output["wicket_pct"],
    }))
    return output


def _find_timeouts(
    event_id: str,
    match_name: str,
    rows: List[Dict],
) -> List[Dict[str, Any]]:
    """Find strategic timeouts and check for wickets in the resumed over."""
    all_snaps: List[Dict] = []
    for row in rows:
        if row.get("innings") not in (1, 2):
            continue
        ts_raw = str(row.get("snapshot_time_utc") or "")
        try:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except Exception:
            continue
        all_snaps.append({
            "ts": ts,
            "innings": row.get("innings"),
            "over": str(row.get("over") or "0"),
            "wickets": row.get("wickets") or 0,
        })

    all_snaps.sort(key=lambda s: s["ts"])
    match_date = all_snaps[0]["ts"].strftime("%Y-%m-%d") if all_snaps else ""
    timeouts: List[Dict[str, Any]] = []

    for innings_no in (1, 2):
        snaps = [s for s in all_snaps if s["innings"] == innings_no]
        if len(snaps) < 2:
            continue

        quiet_start_idx = 0

        for i in range(1, len(snaps)):
            curr = snaps[i]
            prev = snaps[i - 1]
            state_changed = (curr["over"] != prev["over"] or curr["wickets"] != prev["wickets"])

            if not state_changed:
                continue

            quiet_duration = (prev["ts"] - snaps[quiet_start_idx]["ts"]).total_seconds()

            if quiet_duration >= TIMEOUT_THRESHOLD_SECONDS:
                resumed_over_str = curr["over"]
                try:
                    resumed_over_f = float(resumed_over_str)
                except ValueError:
                    quiet_start_idx = i
                    continue

                paused_over_str = snaps[quiet_start_idx]["over"]
                try:
                    paused_over_f = float(paused_over_str)
                except ValueError:
                    quiet_start_idx = i
                    continue

                if paused_over_f < 0.1 and resumed_over_f < 0.2:
                    quiet_start_idx = i
                    continue

                resumed_over_int = int(resumed_over_f)
                wickets_start = curr["wickets"]

                wickets_end: Optional[int] = None
                for j in range(i + 1, len(snaps)):
                    try:
                        fj_over_f = float(snaps[j]["over"])
                    except ValueError:
                        continue
                    if int(fj_over_f) > resumed_over_int:
                        wickets_end = snaps[j - 1]["wickets"]
                        break

                if wickets_end is None:
                    wickets_end = snaps[-1]["wickets"]

                wicket_in_resumed_over = (wickets_end > wickets_start) if wickets_end is not None else None

                timeouts.append({
                    "event_id": event_id,
                    "match_name": match_name,
                    "match_date": match_date,
                    "innings": innings_no,
                    "over_when_paused": snaps[quiet_start_idx]["over"],
                    "pause_duration_sec": round(quiet_duration),
                    "pause_duration_min": round(quiet_duration / 60, 1),
                    "over_resumed": resumed_over_str,
                    "wickets_at_resume": wickets_start,
                    "wickets_end_of_resumed_over": wickets_end,
                    "wicket_in_resumed_over": wicket_in_resumed_over,
                })

            quiet_start_idx = i

    return timeouts


# ---------------------------------------------------------------------------
# Hypothesis 3 — Inn1 Pre-Match Score Over/Under
# ---------------------------------------------------------------------------

_INN1_PREMATCH_CATEGORY = "innings_1"
_INN1_PREMATCH_MARKET   = "1st Innings Score"


def extract_inn1_prematch_over(event_id: Optional[str] = None) -> Dict[str, Any]:
    """Process T20 matches and write per-event hypothesis_inn1_prematch.json to gold.

    Compares the bet365 pre-match "1st Innings Score" Over/Under line (captured
    before the match started, via gold/cricket/prematch/latest/) against the
    actual innings-1 total from the gold tracker.

    If event_id is given, processes only that event. Otherwise scans all ended events.
    Returns aggregate summary.
    """
    gold = get_named_container_client("gold")

    if event_id:
        event_ids_to_process = [event_id]
    else:
        all_ids = _scan_ended_event_ids(gold)
        event_ids_to_process = []
        for eid in all_ids:
            tracker = download_json(gold, f"event_id={eid}/innings_tracker.json") or {}
            if _is_t20_tracker(tracker):
                event_ids_to_process.append(eid)

    logging.info(json.dumps({"event": "hypothesis_inn1_prematch_start", "event_count": len(event_ids_to_process)}))

    results = []
    no_prematch = 0
    for eid in event_ids_to_process:
        try:
            row = _process_inn1_prematch_event(eid, gold)
            if row is None:
                no_prematch += 1
                continue
            upload_json(
                gold,
                f"event_id={eid}/hypothesis_inn1_prematch.json",
                {"generated_at_utc": utc_now().isoformat(), **row},
                overwrite=True,
            )
            results.append(row)
        except Exception:
            logging.exception(json.dumps({"event": "hypothesis_inn1_prematch_event_error", "event_id": eid}))

    results.sort(key=lambda r: r.get("match_date", ""), reverse=True)

    over_count  = sum(1 for r in results if r.get("result") == "OVER")
    under_count = sum(1 for r in results if r.get("result") == "UNDER")
    push_count  = sum(1 for r in results if r.get("result") == "PUSH")
    eligible    = over_count + under_count

    output: Dict[str, Any] = {
        "generated_at_utc": utc_now().isoformat(),
        "hypothesis": "The actual innings-1 total lands OVER the bet365 pre-match '1st Innings Score' line more often than under.",
        "total_t20_matches": len(event_ids_to_process),
        "no_prematch_market_count": no_prematch,
        "eligible_matches": eligible,
        "over_count": over_count,
        "under_count": under_count,
        "push_count": push_count,
        "over_pct": round(100.0 * over_count / eligible, 1) if eligible > 0 else None,
        "results": results,
    }

    logging.info(json.dumps({
        "event": "hypothesis_inn1_prematch_done",
        "total": len(results),
        "over": over_count,
        "under": under_count,
        "over_pct": output["over_pct"],
    }))
    return output


def _process_inn1_prematch_event(event_id: str, gold) -> Optional[Dict[str, Any]]:
    """Extract hypothesis row for one ended T20 match. Returns None if no pre-match market available."""
    tracker = download_json(gold, f"event_id={event_id}/innings_tracker.json") or {}
    actual_total = tracker.get("actual_total")
    if actual_total is None:
        return None

    dashboard = download_json(gold, f"cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json")
    if not dashboard:
        return None

    records = (dashboard.get("prematch_markets") or {}).get("records") or []
    inn1_recs = [
        r for r in records
        if r.get("category_key") == _INN1_PREMATCH_CATEGORY and r.get("market_name") == _INN1_PREMATCH_MARKET
    ]
    if not inn1_recs:
        return None

    line: Optional[float] = None
    over_odds: Optional[float] = None
    under_odds: Optional[float] = None
    for r in inn1_recs:
        try:
            line = float(r.get("selection_name"))
        except (TypeError, ValueError):
            continue
        header = str(r.get("selection_header") or "").strip().lower()
        odds = r.get("odds_decimal")
        if header == "over":
            over_odds = odds
        elif header == "under":
            under_odds = odds

    if line is None:
        return None

    if actual_total > line:
        result = "OVER"
    elif actual_total < line:
        result = "UNDER"
    else:
        result = "PUSH"

    match_header = dashboard.get("match_header") or {}
    inn1_rows = [r for r in (tracker.get("rows") or []) if r.get("innings") == 1]
    batting_team_inn1 = next((r.get("batting_team") for r in inn1_rows if r.get("batting_team")), None)

    return {
        "event_id":            event_id,
        "match_date":          str(tracker.get("match_date_utc") or "")[:10],
        "match_name":          tracker.get("match_name") or match_header.get("match_name") or f"event_{event_id}",
        "league_name":         tracker.get("league_name") or match_header.get("league_name") or "",
        "is_womens_match":     _is_womens_match(tracker.get("match_name") or ""),
        "batting_team_inn1":   batting_team_inn1,
        "prematch_line":       line,
        "prematch_over_odds":  over_odds,
        "prematch_under_odds": under_odds,
        "actual_inn1_total":   actual_total,
        "margin":              round(actual_total - line, 1),
        "result":              result,
    }
