"""
Gold rebuild helpers — uploaded to Databricks DBFS and imported by notebooks.

Uses direct imports from the DBFS-resident modules (storage, innings_tracker, leagues)
rather than the Azure Functions _common wrapper used by cricket_display.

Functions exported:
  gold_rebuild_ended_matches(event_id=None)  — rebuild all stale or one specific match
  auto_rebuild_ended_innings()               — detect ended matches and auto-rebuild
"""
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from api_and_blob import call_betsapi, download_json, get_named_container_client, upload_json
from tracker_writer import extract_innings_snapshot
from league_config import load_allowed_league_ids


def _utc_now():
    return datetime.now(timezone.utc)


def _silver_snapshot_paths_for_event(silver_container, event_id: str) -> List[str]:
    """List all silver match_state.json paths for one event, sorted oldest-first."""
    paths = []
    seen_sids: set = set()
    for blob in silver_container.list_blobs(name_starts_with="cricket/inplay/year="):
        if not blob.name.endswith("/match_state.json"):
            continue
        if f"/event_id={event_id}/" not in blob.name:
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
    """Rebuild innings_1.json and innings_1_from_silver.json for one event from silver data."""
    silver = get_named_container_client("silver")
    gold   = get_named_container_client("gold")

    if snapshot_paths is None:
        snapshot_paths = _silver_snapshot_paths_for_event(silver, event_id)

    if not snapshot_paths:
        return {"event_id": event_id, "message": "no silver snapshots found", "rows_written": 0,
                "silver_snapshots_scanned": 0, "raw_points_extracted": 0, "errors": 0}

    snapshot_paths.sort()

    gold_tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
    existing_gold = download_json(gold, gold_tracker_path) or {}

    silver_score = stadium_data = silver_match_name = None
    silver_league_id = silver_league_name = silver_home_team = silver_away_team = silver_match_date = None
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
        if silver_score and stadium_data and silver_match_name and silver_league_id and silver_home_team:
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
        except Exception:
            errors += 1

    raw_points.sort(key=lambda p: str(p.get("snapshot_time_utc") or ""))
    seen: Dict[tuple, Dict[str, Any]] = {}
    for p in raw_points:
        key = (p.get("innings", 1), p.get("over"), p.get("score"))
        seen[key] = p
    deduped = sorted(seen.values(), key=lambda p: str(p.get("snapshot_time_utc") or ""))

    # Fetch authoritative final score with overs from BetsAPI event/view.
    # This is the single source of truth — stored in score_summary_events so all
    # consumers (ended/view, ML, hypothesis) read from one place.
    # Falls back to silver/gold score if BetsAPI no longer has the match.
    try:
        ev_payload = call_betsapi("/v1/event/view", {"event_id": event_id})
        ev_result  = (ev_payload.get("results") or [{}])[0]
        ev_ss      = str(ev_result.get("ss") or "").strip()
        if ev_ss:
            ev_ss = ev_ss.replace("-", ",")
            # Reorder to innings sequence: score_summary is home-first, but we
            # want batting-first team first.
            first_bat  = next(
                (r.get("batting_team") for r in deduped if r.get("innings") == 1 and r.get("batting_team")),
                None,
            )
            ev_home = (
                (existing_gold.get("home_team_name") or silver_home_team or "").strip()
            )
            if first_bat and ev_home and first_bat != ev_home and "," in ev_ss:
                p0, p1 = ev_ss.split(",", 1)
                ev_ss = f"{p1.strip()},{p0.strip()}"
            final_score = ev_ss
    except Exception:
        pass

    acc_path = f"cricket/inplay/control/event_id={event_id}/innings_accumulator.json"
    old_acc  = download_json(silver, acc_path) or {}
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
    m_score = re.match(r'^(\d+)', str(final_score or ""))
    if m_score:
        actual_total = int(m_score.group(1))
        last_pred = next(
            (r["predicted_total"] for r in reversed(deduped)
             if r.get("innings", 1) == 1 and r.get("predicted_total") is not None),
            None,
        )
        if last_pred is not None:
            outcome = "over" if actual_total > last_pred else ("under" if actual_total < last_pred else "push")

    tracker = {**existing_gold, "event_id": event_id, "rows": deduped, "last_updated_utc": _utc_now().isoformat()}
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
    if not tracker.get("league_name")    and silver_league_name: tracker["league_name"]    = silver_league_name
    if not tracker.get("home_team_name") and silver_home_team:   tracker["home_team_name"] = silver_home_team
    if not tracker.get("away_team_name") and silver_away_team:   tracker["away_team_name"] = silver_away_team
    if not tracker.get("match_date_utc") and silver_match_date:  tracker["match_date_utc"] = silver_match_date

    upload_json(gold, gold_tracker_path, tracker, overwrite=True)

    if deduped:
        silver_tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json"
        upload_json(gold, silver_tracker_path, tracker, overwrite=True)

    return {
        "event_id": event_id,
        "silver_snapshots_scanned": len(snapshot_paths),
        "raw_points_extracted": len(raw_points),
        "rows_written": len(deduped),
        "errors": errors,
        "message": "accumulator rebuilt — reload the innings tracker page",
    }


def gold_rebuild_ended_matches(event_id: Optional[str] = None) -> None:
    """Build/rebuild innings_1_from_silver.json from silver data only.

    Finds events that have silver-processed snapshots but are missing or have
    a stale innings_1_from_silver.json, then rebuilds each one.

    Args:
        event_id: If provided, process only this event. Otherwise process all stale events.
    """
    silver = get_named_container_client("silver")
    gold   = get_named_container_client("gold")

    marker_prefix = "cricket/control/processed_snapshots/"
    silver_newest: Dict[str, Any] = {}
    for blob in silver.list_blobs(name_starts_with=marker_prefix):
        fname = blob.name.rsplit("/", 1)[-1]
        if not fname.endswith(".json"):
            continue
        parts = fname[:-5].rsplit("_", 2)
        if len(parts) != 3:
            continue
        eid = parts[1]
        lm = blob.last_modified
        if lm and (eid not in silver_newest or lm > silver_newest[eid]):
            silver_newest[eid] = lm

    if event_id:
        if event_id not in silver_newest:
            logging.warning(json.dumps({
                "event": "gold_rebuild_ended_matches_skipped",
                "reason": "no silver processed snapshots found",
                "event_id": event_id,
            }))
            return
        to_rebuild = [event_id]
    else:
        gold_ts: Dict[str, Any] = {}
        for blob in gold.list_blobs(name_starts_with="cricket/innings_tracker/event_id="):
            if blob.name.endswith("innings_1_from_silver.json"):
                bparts = blob.name.split("/")
                ep = next((p for p in bparts if p.startswith("event_id=")), None)
                if ep:
                    gold_ts[ep.replace("event_id=", "")] = blob.last_modified

        to_rebuild = []
        for eid, silver_lm in silver_newest.items():
            gold_lm = gold_ts.get(eid)
            if gold_lm is None or (silver_lm and silver_lm > gold_lm):
                to_rebuild.append(eid)

    logging.info(json.dumps({
        "event": "gold_rebuild_ended_matches_started",
        "total_silver_events": len(silver_newest),
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
    allowed_leagues = load_allowed_league_ids()

    live_eids: set = set()
    try:
        live_idx = download_json(gold, "cricket/matches/latest/index.json") or {}
        for m in (live_idx.get("matches") or []):
            eid = str(m.get("event_id") or "")
            if eid:
                live_eids.add(eid)
    except Exception:
        pass

    silver_file_written_at: Dict[str, Any] = {}
    tracker_prefix = "cricket/innings_tracker/event_id="
    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if blob.name.endswith("innings_1_from_silver.json"):
            parts = blob.name.split("/")
            ep = next((p for p in parts if p.startswith("event_id=")), None)
            if ep:
                silver_file_written_at[ep.replace("event_id=", "")] = blob.last_modified

    silver = get_named_container_client("silver")
    acc_prefix = "cricket/inplay/control/event_id="
    acc_last_mod: Dict[str, Any] = {}
    for blob in silver.list_blobs(name_starts_with=acc_prefix):
        if not blob.name.endswith("innings_accumulator.json"):
            continue
        parts = blob.name.split("/")
        ep = next((p for p in parts if p.startswith("event_id=")), None)
        if ep:
            acc_last_mod[ep.replace("event_id=", "")] = blob.last_modified

    candidates: List[str] = []
    seen: set = set()

    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if not blob.name.endswith("innings_1.json"):
            continue
        parts = blob.name.split("/")
        ep = next((p for p in parts if p.startswith("event_id=")), None)
        if not ep:
            continue
        eid = ep.replace("event_id=", "")
        if eid in live_eids or eid in seen:
            continue
        last_mod = blob.last_modified
        if last_mod and last_mod > one_hour_ago:
            continue
        silver_ts = silver_file_written_at.get(eid)
        if silver_ts is not None:
            acc_ts = acc_last_mod.get(eid)
            if acc_ts is None or acc_ts <= silver_ts:
                continue
        candidates.append(eid)
        seen.add(eid)

    for eid, acc_ts in acc_last_mod.items():
        if eid in live_eids or eid in seen:
            continue
        if acc_ts and acc_ts > one_hour_ago:
            continue
        silver_ts = silver_file_written_at.get(eid)
        if silver_ts is not None and acc_ts is not None and acc_ts <= silver_ts:
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
            download_json(gold, f"cricket/innings_tracker/event_id={eid}/innings_1.json")
            or download_json(silver, f"cricket/inplay/control/event_id={eid}/innings_accumulator.json")
        )
        if not tracker:
            continue
        league_id = str(tracker.get("league_id") or "")
        if league_id not in allowed_leagues:
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
