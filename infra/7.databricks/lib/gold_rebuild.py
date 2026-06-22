"""
Gold rebuild helpers — uploaded to Databricks DBFS and imported by notebooks.

Uses direct imports from the DBFS-resident modules (storage, innings_tracker, leagues)
rather than the Azure Functions _common wrapper used by cricket_display.

Functions exported:
  gold_rebuild_ended_matches(event_id=None)  — rebuild all stale or one specific match
  auto_rebuild_ended_innings()               — detect ended matches and auto-rebuild (opt-out league model)
"""
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from util import call_betsapi, download_json, extract_results, get_named_container_client, upload_json
from tracker_writer import extract_innings_snapshot
from league_config import load_disabled_league_ids

_OVER_ORD   = re.compile(r'^\d+(?:st|nd|rd|th)?\s+over', re.I)
_PLAYER_RUN = re.compile(r'.+\s+innings\s+runs?$', re.I)
_PLAYER_MIL = re.compile(r'.+\s+milestones?$', re.I)
_TOP_BATTER = re.compile(r'.+\s+top\s+batter$', re.I)
_TOP_BOWL   = re.compile(r'.+\s+top\s+bowl', re.I)
_INN_TOTAL  = re.compile(r'.+\s+\d+\s+overs?\s+runs?$', re.I)
_BALL_DEL   = re.compile(r'runs?\s+off\s+\d+\w*\s+delivery', re.I)
_FALL_WKT   = re.compile(r'runs?\s+at\s+fall\s+of', re.I)


def _categorise_market(name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Map a market_group_name to (canonical_name, type, color). Returns (None, None, None) if unknown."""
    g  = name.strip()
    gl = g.lower()
    if 'match winner' in gl:
        return 'Match Winner', 'match', '#2563eb'
    if _INN_TOTAL.match(g):
        return 'Innings Total (O/U)', 'match', '#2563eb'
    if _OVER_ORD.match(g):
        if 'odd' in gl or 'even' in gl:
            return 'Over Runs – Odd/Even', 'over', '#d97706'
        if 'wicket' in gl:
            return 'Over – Wicket', 'over', '#d97706'
        return 'Over Total Runs', 'over', '#d97706'
    if 'dismissal method' in gl or 'method of dismissal' in gl:
        return 'Dismissal Method', 'ball', '#16a34a'
    if _BALL_DEL.search(gl):
        return 'Ball Delivery Runs', 'ball', '#16a34a'
    if 'next batter out' in gl:
        return 'Next Batter Out', 'player', '#9333ea'
    if _PLAYER_RUN.match(g):
        return 'Batter Innings Runs', 'player', '#9333ea'
    if _PLAYER_MIL.match(g) or 'batter milestones' in gl:
        return 'Batter Milestones', 'player', '#9333ea'
    if _TOP_BATTER.match(g):
        return 'Top Batter', 'match', '#2563eb'
    if _TOP_BOWL.match(g):
        return 'Top Bowler', 'match', '#2563eb'
    if 'to score most runs' in gl:
        return 'Top Scorer', 'match', '#2563eb'
    if "6's" in gl or 'sixes' in gl:
        return 'Team Sixes (O/U)', 'match', '#2563eb'
    if 'highest' in gl and 'partnership' in gl:
        return 'Opening Partnership', 'player', '#9333ea'
    if _FALL_WKT.search(gl):
        return 'Fall of Wicket Runs', 'player', '#9333ea'
    if 'session runs' in gl:
        return 'Session Runs', 'over', '#d97706'
    if 'runs in first' in gl:
        return 'Powerplay Runs', 'over', '#d97706'
    return None, None, None


def _utc_now():
    return datetime.now(timezone.utc)


def _silver_snapshot_paths_for_event(silver_container, event_id: str) -> List[str]:
    """List all silver match_state.json paths for one event, sorted oldest-first."""
    paths = []
    seen_sids: set = set()
    for blob in silver_container.list_blobs(name_starts_with=f"event_id={event_id}/"):
        if not blob.name.endswith("/match_state.json"):
            continue
        parts = blob.name.split("/")
        sid = next((p for p in parts if p.startswith("snapshot_id=")), None)
        if not sid or sid in seen_sids:
            continue
        seen_sids.add(sid)
        paths.append(blob.name)
    paths.sort()
    return paths


def _rebuild_innings_core(event_id: str, snapshot_paths: Optional[List[str]] = None) -> Dict[str, Any]:
    """Rebuild innings_tracker.json for one event from silver data."""
    silver = get_named_container_client("silver")
    gold   = get_named_container_client("gold")

    if snapshot_paths is None:
        snapshot_paths = _silver_snapshot_paths_for_event(silver, event_id)

    if not snapshot_paths:
        return {"event_id": event_id, "message": "no silver snapshots found", "rows_written": 0,
                "silver_snapshots_scanned": 0, "raw_points_extracted": 0, "errors": 0}

    snapshot_paths.sort()

    gold_tracker_path = f"event_id={event_id}/innings_tracker.json"
    existing_gold = download_json(gold, gold_tracker_path) or {}

    silver_score = stadium_data = silver_match_name = None
    silver_league_id = silver_league_name = silver_home_team = silver_away_team = silver_match_date = None
    silver_fi = None
    for ms_path in reversed(snapshot_paths[-50:]):
        ms = download_json(silver, ms_path)
        if ms:
            if not silver_score and ms.get("score_summary_events"):
                silver_score = ms["score_summary_events"]
            if not stadium_data and ms.get("stadium_data"):
                stadium_data = ms["stadium_data"]
            if not silver_match_name and ms.get("match_name"):
                silver_match_name = ms["match_name"]
            if not silver_league_id and ms.get("league_id"):
                silver_league_id = str(ms["league_id"])
            if not silver_league_name and ms.get("league_name"):
                silver_league_name = ms["league_name"]
            if not silver_home_team and ms.get("home_team_name"):
                silver_home_team = ms["home_team_name"]
            if not silver_away_team and ms.get("away_team_name"):
                silver_away_team = ms["away_team_name"]
            if not silver_match_date and ms.get("event_time_utc"):
                silver_match_date = ms["event_time_utc"]
            if not silver_fi and ms.get("fi"):
                silver_fi = str(ms["fi"])
        if silver_score and stadium_data and silver_match_name and silver_league_id and silver_home_team and silver_fi:
            break
    if not stadium_data:
        stadium_data = existing_gold.get("stadium_data") or None

    def _norm_score(s):
        return s.replace("-", ",") if s else s

    silver_score = _norm_score(silver_score)
    gold_score   = _norm_score(existing_gold.get("score_summary_events") or None)

    def _2nd_runs(ss):
        if not ss:
            return -1
        parts = ss.split(",", 1)
        if len(parts) == 2:
            m = re.match(r'(\d+)', parts[1].strip())
            return int(m.group(1)) if m else 0
        return 0

    final_score = silver_score if _2nd_runs(silver_score) >= _2nd_runs(gold_score) else gold_score

    raw_points: List[Dict[str, Any]] = []
    heatmap_balls: List[Dict[str, Any]] = []
    all_heatmap_cats: Dict[str, Dict] = {}
    errors = 0
    for ms_path in snapshot_paths:
        base = ms_path.removesuffix("/match_state.json")
        try:
            # Always re-derive from raw silver data so that batting_team is resolved
            # from the innings market name (most reliable) rather than from cached
            # innings_snapshot.json which may have wrong batting_team from the PI bug.
            match_state = download_json(silver, ms_path)
            if not match_state:
                continue
            team_scores_doc    = download_json(silver, f"{base}/team_scores.json") or {}
            active_markets_doc = download_json(silver, f"{base}/active_markets.json") or {}
            point = extract_innings_snapshot(
                match_state,
                team_scores_doc.get("rows", []),
                active_markets_doc.get("rows", []),
            )
            if point is not None and (point.get("home_team_odds") is None or point.get("away_team_odds") is None):
                mkt_odds_doc = download_json(silver, f"{base}/market_odds.json") or {}
                for mr in mkt_odds_doc.get("rows", []):
                    if mr.get("market_key") == "3_1":
                        h = mr.get("home_odds_decimal")
                        a = mr.get("away_odds_decimal")
                        if h and a:
                            point["home_team_odds"] = h
                            point["away_team_odds"] = a
                            break
            if point is not None:
                raw_points.append(point)

            # Collect heatmap categories from active_markets
            open_cats: Set[str] = set()
            for mkt_row in active_markets_doc.get("rows", []):
                grp_name = str(mkt_row.get("market_group_name") or "")
                if not grp_name:
                    continue
                canon, mtype, mcolor = _categorise_market(grp_name)
                if canon is None:
                    continue
                if canon not in all_heatmap_cats:
                    all_heatmap_cats[canon] = {"type": mtype, "color": mcolor}
                open_cats.add(canon)

            if point is not None and open_cats:
                over_str = str(point.get("over") or "0")
                try:
                    ov_f       = float(over_str)
                    over_num   = int(ov_f)
                    ball_in_ov = round((ov_f - over_num) * 10)
                except Exception:
                    over_num = 0
                    ball_in_ov = 0
                heatmap_balls.append({
                    "over":     over_str,
                    "over_num": over_num,
                    "ball":     ball_in_ov,
                    "innings":  point.get("innings", 1),
                    "score":    f"{point.get('score', 0)}/{point.get('wickets', 0)}",
                    "cats":     sorted(open_cats),
                })
        except Exception:
            errors += 1

    raw_points.sort(key=lambda p: str(p.get("snapshot_time_utc") or ""))
    seen: Dict[tuple, Dict[str, Any]] = {}
    for p in raw_points:
        key = (p.get("innings", 1), p.get("over"), p.get("score"))
        seen[key] = p
    deduped = sorted(seen.values(), key=lambda p: str(p.get("snapshot_time_utc") or ""))

    # Fetch authoritative final score, home/away, league, and stadium from the
    # bronze master file written by bronze_capture_ended_event_view (daily function).
    # That file stores the full /v1/event/view response including overs in ss.
    # Falls back to a live BetsAPI call if the master file hasn't been written yet.
    bronze = get_named_container_client("bronze")
    master_path   = f"betsapi/event_final/event_id={event_id}/event_view.json"
    master_raw    = download_json(bronze, master_path)
    master_result = (extract_results(master_raw) or [{}])[0] if master_raw else {}

    if not master_result:
        try:
            ev_payload    = call_betsapi("/v1/event/view", {"event_id": event_id})
            master_result = (extract_results(ev_payload) or [{}])[0]
        except Exception:
            master_result = {}

    ev_ss = str(master_result.get("ss") or "").strip()
    if ev_ss:
        # Store home-first (same as BetsAPI ss). Do NOT swap here — display layers
        # (innings_tracker.py, bronze_discover_cricket_ended) already have their own
        # batting-order swap for presentation. Swapping here causes double-swap.
        ev_ss = ev_ss.replace("-", ",")
        final_score = ev_ss

    master_home    = (master_result.get("home")   or {}).get("name") or None
    master_away    = (master_result.get("away")   or {}).get("name") or None
    master_league  = (master_result.get("league") or {}).get("name") or None
    master_stadium = (master_result.get("extra")  or {}).get("stadium_data") or None

    acc_path = f"event_id={event_id}/innings_accumulator.json"
    old_acc  = download_json(silver, acc_path) or {}
    fi       = str(silver_fi or old_acc.get("fi") or existing_gold.get("fi") or "")
    new_acc  = {
        **old_acc,
        "event_id": event_id,
        "rows": deduped,
        "last_updated_utc": _utc_now().isoformat(),
        "rebuilt_from_snapshot_count": len(snapshot_paths),
    }
    if deduped and not new_acc.get("home_team_name"):
        p0 = deduped[0]
        new_acc["home_team_name"] = new_acc.get("home_team_name") or p0.get("batting_team")
        new_acc["away_team_name"] = new_acc.get("away_team_name") or p0.get("bowling_team")
    upload_json(silver, acc_path, new_acc, overwrite=True)

    outcome = actual_total = None
    if final_score and "," in str(final_score):
        # final_score (ev_ss) is stored home-first. innings 1 is not always the
        # home team's innings — swap to inn1's half if the away team batted first.
        score_parts = str(final_score).split(",", 1)
        inn1_batting_team = next(
            (r.get("batting_team") for r in deduped if r.get("innings", 1) == 1 and r.get("batting_team")),
            None,
        )
        away_name = master_away or existing_gold.get("away_team_name") or silver_away_team
        if inn1_batting_team and away_name and str(inn1_batting_team).strip() == str(away_name).strip():
            score_parts = [score_parts[1].strip(), score_parts[0].strip()]
        m_score = re.match(r'^(\d+)', score_parts[0].strip())
        if m_score:
            actual_total = int(m_score.group(1))
            last_pred = next(
                (r["predicted_total"] for r in reversed(deduped)
                 if r.get("innings", 1) == 1 and r.get("predicted_total") is not None),
                None,
            )
            if last_pred is not None:
                outcome = "over" if actual_total > last_pred else ("under" if actual_total < last_pred else "push")

    tracker = {**existing_gold, "event_id": event_id, "fi": fi, "rows": deduped, "last_updated_utc": _utc_now().isoformat()}
    if final_score:
        tracker["score_summary_events"] = final_score
    if outcome is not None:
        tracker["outcome"] = outcome
    if actual_total is not None:
        tracker["actual_total"] = actual_total
    if stadium_data:
        tracker["stadium_data"] = stadium_data
    if not tracker.get("match_name")     and silver_match_name:  tracker["match_name"]     = silver_match_name
    if not tracker.get("league_id")      and silver_league_id:   tracker["league_id"]      = silver_league_id
    if not tracker.get("match_date_utc") and silver_match_date:  tracker["match_date_utc"] = silver_match_date
    # Master data from bronze event_final overrides silver for names and stadium.
    tracker["league_name"]    = master_league    or tracker.get("league_name")    or silver_league_name
    tracker["home_team_name"] = master_home      or tracker.get("home_team_name") or silver_home_team
    tracker["away_team_name"] = master_away      or tracker.get("away_team_name") or silver_away_team
    if master_stadium:
        tracker["stadium_data"] = master_stadium
    if not tracker.get("stadium_data"):
        stadium_overrides = download_json(gold, "overrides/stadium_overrides.json") or {}
        override_name = stadium_overrides.get(event_id)
        if override_name:
            tracker["stadium_data"] = {"name": override_name}
    _combined = f"{tracker.get('match_name') or ''} {tracker.get('league_name') or ''}".lower()
    tracker["gender"] = "W" if ("women" in _combined or "(w)" in _combined) else "M"

    upload_json(gold, gold_tracker_path, tracker, overwrite=True)

    # Heatmap — write pre-computed market availability grid to gold
    if heatmap_balls and all_heatmap_cats:
        try:
            upload_json(gold, f"event_id={event_id}/heatmap.json", {
                "event_id":         event_id,
                "generated_at_utc": _utc_now().isoformat(),
                "categories":       all_heatmap_cats,
                "balls":            heatmap_balls,
            }, overwrite=True)
        except Exception:
            pass

    return {
        "event_id": event_id,
        "silver_snapshots_scanned": len(snapshot_paths),
        "raw_points_extracted": len(raw_points),
        "rows_written": len(deduped),
        "errors": errors,
        "message": "accumulator rebuilt — reload the innings tracker page",
    }


def gold_rebuild_ended_matches(event_id: Optional[str] = None) -> None:
    """Build/rebuild innings_tracker.json in gold from silver data.

    Finds events that have a silver complete marker but are missing or have
    a stale innings_tracker.json in gold, then rebuilds each one.

    Args:
        event_id: If provided, process only this event. Otherwise process all stale events.
    """
    silver = get_named_container_client("silver")
    gold   = get_named_container_client("gold")

    # Read event-level complete markers — one file per fully-processed event.
    silver_complete: Dict[str, Any] = {}
    for blob in silver.list_blobs(name_starts_with="control/complete/"):
        fname = blob.name.rsplit("/", 1)[-1]
        if fname.startswith("event_id=") and fname.endswith(".json"):
            eid = fname[9:-5]
            silver_complete[eid] = blob.last_modified

    if event_id:
        if event_id not in silver_complete:
            logging.warning(json.dumps({
                "event": "gold_rebuild_ended_matches_skipped",
                "reason": "no silver complete marker found",
                "event_id": event_id,
            }))
            return
        to_rebuild = [event_id]
    else:
        gold_ts: Dict[str, Any] = {}
        for blob in gold.list_blobs(name_starts_with="event_id="):
            if blob.name.endswith("/innings_tracker.json"):
                bparts = blob.name.split("/")
                ep = next((p for p in bparts if p.startswith("event_id=")), None)
                if ep:
                    gold_ts[ep[9:]] = blob.last_modified

        to_rebuild = []
        for eid, silver_lm in silver_complete.items():
            gold_lm = gold_ts.get(eid)
            if gold_lm is None or (silver_lm and silver_lm > gold_lm):
                to_rebuild.append(eid)

    logging.info(json.dumps({
        "event": "gold_rebuild_ended_matches_started",
        "total_silver_events": len(silver_complete),
        "to_rebuild": len(to_rebuild),
    }))

    rebuilt = failed = 0
    for eid in to_rebuild:
        paths = _silver_snapshot_paths_for_event(silver, eid)
        try:
            _rebuild_innings_core(eid, snapshot_paths=paths)
            rebuilt += 1
        except Exception:
            failed += 1
            logging.exception(json.dumps({"event": "gold_rebuild_failed", "event_id": eid}))

    logging.warning(json.dumps({
        "event": "gold_rebuild_ended_matches_done",
        "rebuilt": rebuilt,
        "failed": failed,
    }))


def auto_rebuild_ended_innings() -> None:
    """Detect matches that ended and auto-rebuild their innings tracker.

    Processes up to 2 matches per run to stay within Databricks job timeout.
    """
    now = _utc_now()
    one_hour_ago = now - timedelta(hours=1)
    gold = get_named_container_client("gold")
    disabled_leagues = load_disabled_league_ids()

    live_eids: set = set()
    try:
        live_idx = download_json(gold, "cricket/matches/latest/index.json") or {}
        for m in (live_idx.get("matches") or []):
            eid = str(m.get("event_id") or "")
            if eid:
                live_eids.add(eid)
    except Exception:
        pass

    gold_tracker_ts: Dict[str, Any] = {}
    for blob in gold.list_blobs(name_starts_with="event_id="):
        if blob.name.endswith("/innings_tracker.json"):
            ep = next((p for p in blob.name.split("/") if p.startswith("event_id=")), None)
            if ep:
                gold_tracker_ts[ep[9:]] = blob.last_modified

    silver = get_named_container_client("silver")
    acc_last_mod: Dict[str, Any] = {}
    for blob in silver.list_blobs(name_starts_with="event_id="):
        if not blob.name.endswith("/innings_accumulator.json"):
            continue
        ep = next((p for p in blob.name.split("/") if p.startswith("event_id=")), None)
        if ep:
            acc_last_mod[ep[9:]] = blob.last_modified

    candidates: List[str] = []
    seen: set = set()

    for eid, gold_lm in gold_tracker_ts.items():
        if eid in live_eids or eid in seen:
            continue
        if gold_lm and gold_lm > one_hour_ago:
            continue
        acc_ts = acc_last_mod.get(eid)
        if acc_ts is None or acc_ts <= gold_lm:
            continue
        candidates.append(eid)
        seen.add(eid)

    for eid, acc_ts in acc_last_mod.items():
        if eid in live_eids or eid in seen:
            continue
        if acc_ts and acc_ts > one_hour_ago:
            continue
        if gold_tracker_ts.get(eid) is not None:
            continue
        candidates.append(eid)
        seen.add(eid)

    if not candidates:
        logging.info(json.dumps({"event": "auto_rebuild_ended_innings_no_candidates"}))
        return

    rebuilt = 0
    for eid in candidates:
        if rebuilt >= 2:
            break
        tracker = (
            download_json(gold, f"event_id={eid}/innings_tracker.json")
            or download_json(silver, f"event_id={eid}/innings_accumulator.json")
        )
        if not tracker:
            continue
        league_id = str(tracker.get("league_id") or "")
        if league_id in disabled_leagues:
            logging.info(json.dumps({"event": "auto_rebuild_skip_league", "event_id": eid, "league_id": league_id}))
            continue
        try:
            result = _rebuild_innings_core(eid)
            logging.warning(json.dumps({"event": "auto_rebuild_ended_innings_done", **result}))
            rebuilt += 1
        except Exception:
            logging.exception(json.dumps({"event": "auto_rebuild_ended_innings_failed", "event_id": eid}))

    logging.warning(json.dumps({
        "event": "auto_rebuild_ended_innings_completed",
        "candidates_found": len(candidates),
        "rebuilt_this_run": rebuilt,
    }))
