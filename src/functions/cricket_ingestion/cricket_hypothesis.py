"""Cricket hypothesis data extraction.

Hypothesis 1 — Inn2 Over-6 Favourite Wins:
  During a chase, whichever team is the match-winner odds favourite after the
  end of the 6th over of the 2nd innings, does that team always win?

Hypothesis 2 — Strategic Timeout Wicket:
  After a strategic timeout (game state unchanged for > 2 minutes), a wicket
  falls in the next over when play resumes.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from storage import (
    download_json,
    get_named_container_client,
    upload_json,
    utc_now,
)


def _is_t20_match(match_name: str) -> bool:
    n = match_name.lower()
    return "t20" in n or "twenty20" in n or "t-20" in n


def _is_womens_match(match_name: str) -> bool:
    n = match_name.lower()
    return "women" in n or "(w)" in n


def extract_inn2_over6_favorite() -> Dict[str, Any]:
    """Scan all ended T20 matches and write inn2_over6_favorite.json to gold."""
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")

    # Collect all ended event_ids from gold innings tracker files (T20 only)
    event_ids: List[str] = []
    for blob in gold.list_blobs(name_starts_with="cricket/innings_tracker/event_id="):
        if not blob.name.endswith("innings_1_from_silver.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        tracker = download_json(gold, blob.name) or {}
        if not _is_t20_match(tracker.get("match_name") or ""):
            continue
        event_ids.append(eid_part.replace("event_id=", ""))

    logging.info(json.dumps({"event": "hypothesis_inn2_over6_start", "event_count": len(event_ids)}))

    results = []
    for event_id in event_ids:
        try:
            row = _process_event(event_id, silver, gold)
            if row:
                results.append(row)
        except Exception:
            logging.exception(json.dumps({"event": "hypothesis_event_error", "event_id": event_id}))

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

    upload_json(gold, "cricket/hypothesis/inn2_over6_favorite.json", output, overwrite=True)
    logging.info(json.dumps({
        "event": "hypothesis_inn2_over6_done",
        "total": len(results),
        "favorite_won": favoured,
        "favorite_lost": not_favoured,
        "win_pct": output["favorite_win_pct"],
    }))
    return output


def _load_accumulator_rows(event_id: str, silver) -> List[Dict]:
    """Load innings rows from silver accumulator — one file per event, no blob scanning."""
    acc = download_json(silver, f"cricket/inplay/control/event_id={event_id}/innings_accumulator.json") or {}
    return acc.get("rows") or []


def _process_event(
    event_id: str,
    silver,
    gold,
) -> Optional[Dict[str, Any]]:
    """Extract hypothesis row for one ended match. Returns None if data is insufficient."""
    import re as _re

    # Load gold tracker for authoritative final scores and team metadata.
    tracker = download_json(gold, f"cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json") or {}

    match_name = tracker.get("match_name") or f"event_{event_id}"
    tracker_home = tracker.get("home_team_name") or ""
    tracker_away = tracker.get("away_team_name") or ""

    # Determine batting_first_team from the tracker's own rows — the same
    # source the ML predictor uses. The accumulator may lack batting_team for
    # older matches, but the tracker rows always have it.
    tracker_rows = tracker.get("rows") or []
    inn1_tracker_rows = [r for r in tracker_rows if r.get("innings") == 1]
    batting_first_team = next((r["batting_team"] for r in inn1_tracker_rows if r.get("batting_team")), "") or ""

    # Load accumulator rows from silver for inn2 over-6 odds/score state.
    # One HTTP request per event; no blob listing.
    rows = _load_accumulator_rows(event_id, silver)

    inn1_rows = [r for r in rows if r.get("innings") == 1]
    inn2_rows_raw = [r for r in rows if r.get("innings") == 2]

    # Chasing team from accumulator inn2 rows
    chasing_team_from_acc = next((r["batting_team"] for r in inn2_rows_raw if r.get("batting_team")), "") or ""

    # Authoritative final scores from score_summary_events (Bet365 API).
    # Format is home-team-first. Swap when away team batted first.
    raw_summary = (tracker.get("score_summary_events") or tracker.get("score_summary") or "").replace("-", ",").strip()
    actual_total: Optional[int] = None
    final_innings2_score: Optional[int] = None
    final_innings2_wickets: Optional[int] = None
    if raw_summary and "," in raw_summary:
        parts = [p.strip() for p in raw_summary.split(",", 1)]
        if batting_first_team and tracker_away and batting_first_team == tracker_away:
            parts = [parts[1], parts[0]]
        try:
            m0 = _re.match(r"(\d+)(?:/(\d+))?", parts[0])
            m1 = _re.match(r"(\d+)(?:/(\d+))?", parts[1])
            if m0 and m1:
                actual_total = int(m0.group(1))
                final_innings2_score = int(m1.group(1))
                final_innings2_wickets = int(m1.group(2)) if m1.group(2) else None
        except Exception:
            pass

    if not actual_total:
        return None

    target = actual_total + 1

    # Match date from tracker
    match_date = str(tracker.get("match_date_utc") or "")[:10]

    # Team names from accumulator rows (most reliable source)
    home_team = tracker_home
    away_team = tracker_away
    chasing_team = chasing_team_from_acc or (away_team if batting_first_team == home_team else home_team)

    # Sort inn1 by over for snapshot lookups
    inn1_sorted = sorted(inn1_rows, key=lambda r: float(r.get("over") or 0))

    # Ghost-filter inn2: skip rows where innings==2 but score > 0 at over 0.0
    # (transition ghost snapshots from innings break)
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

    # Inn1 over-6 snapshot
    inn1_over6 = next((r for r in inn1_sorted if float(r.get("over") or 0) >= 6.0), None)
    inn1_score_at_over6 = inn1_over6.get("score") if inn1_over6 else None
    inn1_wickets_at_over6 = inn1_over6.get("wickets") if inn1_over6 else None

    # Inn2 over-6 snapshot
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

    # Winner detection using authoritative final scores from score_summary.
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
        "is_womens_match": _is_womens_match(match_name),
        "batting_first_team": batting_first_team,
        "chasing_team": chasing_team,
        "target": target,
        "inn1_final_score": actual_total,
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


def extract_timeout_wicket() -> Dict[str, Any]:
    """Scan all ended matches and write timeout_wicket.json to gold.

    A 'strategic timeout' is detected when the game state (over + wickets) is
    unchanged across snapshots for more than TIMEOUT_THRESHOLD_SECONDS.
    Normal delivery gaps are 30-60s; over-change breaks ~90s; a strategic
    timeout is ~180s — so a 3-minute threshold reliably separates them.

    For each timeout, records whether a wicket fell during the over that
    immediately resumed after the pause ended.
    """
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")

    event_ids: List[str] = []
    for blob in gold.list_blobs(name_starts_with="cricket/innings_tracker/event_id="):
        if not blob.name.endswith("innings_1_from_silver.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if not eid_part:
            continue
        tracker_check = download_json(gold, blob.name) or {}
        if not _is_t20_match(tracker_check.get("match_name") or ""):
            continue
        event_ids.append(eid_part.replace("event_id=", ""))

    all_timeouts: List[Dict[str, Any]] = []

    for event_id in event_ids:
        tracker = download_json(gold, f"cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json") or {}
        match_name = tracker.get("match_name") or f"event_{event_id}"
        rows = _load_accumulator_rows(event_id, silver)
        if not rows:
            continue
        try:
            timeouts = _find_timeouts(event_id, match_name, rows)
            all_timeouts.extend(timeouts)
        except Exception:
            logging.exception(json.dumps({"event": "timeout_hypothesis_error", "event_id": event_id}))

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

    upload_json(gold, "cricket/hypothesis/timeout_wicket.json", output, overwrite=True)
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
    """Find strategic timeouts and check for wickets in the resumed over.

    rows — accumulator rows for the event (already loaded, no blob scanning).
    Each row must have: innings, over, wickets, snapshot_time_utc.
    """
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

        quiet_start_idx = 0  # index where the current static period began

        for i in range(1, len(snaps)):
            curr = snaps[i]
            prev = snaps[i - 1]
            state_changed = (curr["over"] != prev["over"] or curr["wickets"] != prev["wickets"])

            if not state_changed:
                continue

            # State changed at snapshot i — measure how long it was static before
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

                # Skip innings-break pauses: pause at over 0.0, resume at first delivery
                # (over 0.1). The gap between innings is always long but is not a timeout.
                if paused_over_f < 0.1 and resumed_over_f < 0.2:
                    quiet_start_idx = i
                    continue

                resumed_over_int = int(resumed_over_f)
                wickets_start = curr["wickets"]

                # Find wickets at the END of the resumed over
                # (when the over number advances past resumed_over_int)
                wickets_end: Optional[int] = None
                for j in range(i + 1, len(snaps)):
                    try:
                        fj_over_f = float(snaps[j]["over"])
                    except ValueError:
                        continue
                    if int(fj_over_f) > resumed_over_int:
                        wickets_end = snaps[j - 1]["wickets"]
                        break

                # If over never ended in data, use last known wicket count
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
