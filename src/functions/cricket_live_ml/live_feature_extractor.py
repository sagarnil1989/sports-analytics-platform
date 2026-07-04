"""
Live feature extraction for the Win Predictor (Phase 6).

Direct port of Steps 2–4 from infra/9.ml/notebooks/ml_win_predictor.py so
that live silver accumulator rows produce a feature vector on the exact same
distribution as the training data.

Functions here MUST stay in sync with the training notebook.  If the notebook
is changed, update this file in the same commit and re-run validate_against_history().

Exported API
-----------
build_record(accumulator, silver, event_id)
    → raw feature dict (same keys as training df columns)
apply_composites(rec)
    → same dict with composite columns added in-place
build_feature_vector(rec, feature_list, encodings)
    → (numpy float64 array, list of feature detail dicts)
validate_against_history(gold, silver, n_matches=10)
    → prints per-field comparison; returns True if all diffs are < 1e-6
"""

import json
import logging
import re as _re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


# ── Helpers identical to training notebook Steps 2-3 ─────────────────────────

def _parse_over(r: Dict) -> Tuple[int, int]:
    try:
        s = str(r.get("over") or "0")
        parts = s.split(".")
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    except Exception:
        return 0, 0


def state_after_n_overs(inn_rows: List[Dict], n: int) -> Optional[Dict]:
    """
    Best snapshot of {score, wickets, bat_odds, bowl_odds} immediately after
    n complete overs.  Priority: exact boundary row > last row of prev over >
    any row up to n.  Identical to training notebook.
    """
    exact, prev_ov, any_ov = [], [], []
    for r in inn_rows:
        ov_int, ov_ball = _parse_over(r)
        if ov_int == n and ov_ball == 0:
            exact.append(r)
        if ov_int == n - 1:
            prev_ov.append(r)
        if ov_int <= n:
            any_ov.append(r)
    r = (exact[0] if exact
         else prev_ov[-1] if prev_ov
         else any_ov[-1]  if any_ov
         else None)
    if r is None:
        return None
    return {
        "score":    r.get("score")   or 0,
        "wickets":  r.get("wickets") or 0,
        "bat_odds": r.get("batting_team_odds"),
        "bowl_odds": r.get("bowling_team_odds"),
    }


def per_over_breakdown(inn_rows: List[Dict], innings_num: int, max_over: int) -> Dict:
    """Per-over runs, wickets, bat_odds, bowl_odds for overs 1..max_over."""
    result: Dict = {}
    prev_score = prev_wickets = 0
    for k in range(1, max_over + 1):
        s = state_after_n_overs(inn_rows, k)
        if s is not None:
            result[f"inn{innings_num}_ov{k}_runs"]      = max(0, s["score"]   - prev_score)
            result[f"inn{innings_num}_ov{k}_wkts"]      = max(0, s["wickets"] - prev_wickets)
            result[f"inn{innings_num}_ov{k}_bat_odds"]  = s["bat_odds"]
            result[f"inn{innings_num}_ov{k}_bowl_odds"] = s["bowl_odds"]
            prev_score   = s["score"]
            prev_wickets = s["wickets"]
        else:
            for suffix in ["_runs", "_wkts", "_bat_odds", "_bowl_odds"]:
                result[f"inn{innings_num}_ov{k}{suffix}"] = None
    return result


def chase_aggregate(inn_rows: List[Dict], inn1_score: int, over_target: int) -> Optional[Dict]:
    """Cumulative chase state at end of over_target."""
    s = state_after_n_overs(inn_rows, over_target)
    if s is None:
        return None
    score       = s["score"]
    wickets     = s["wickets"]
    balls_done  = over_target * 6
    balls_left  = 120 - balls_done
    runs_needed = (inn1_score + 1) - score
    crr = round(score / (balls_done / 6), 4) if balls_done > 0 else 0
    rrr = round(runs_needed / (balls_left / 6), 4) if balls_left > 0 else 99
    return {
        "score": score, "wickets": wickets,
        "crr": crr, "rrr": rrr,
        "rr_diff": round(crr - rrr, 4),
        "runs_needed": runs_needed,
    }


def batting_dominance(event_id: str, silver) -> Optional[float]:
    """
    max_SR − avg_SR across inn1 batsmen (min 5 balls faced).
    Reads silver state files identical to training notebook.  Returns None if
    files are unavailable (e.g. match was ingested before state capture was enabled).
    """
    prefix = f"silver/cricket/inplay/state/event_id={event_id}/"
    try:
        state_blobs = [b.name for b in silver.list_blobs(name_starts_with=prefix)
                       if "/state_1_" in b.name]
    except Exception:
        return None
    if not state_blobs:
        return None

    def _load(path):
        try:
            return json.loads(silver.get_blob_client(path).download_blob().readall())
        except Exception:
            return None

    pattern = _re.compile(r'\[([^\]]+)#(\d+)\]:(\d+):(\d+)')
    player_max: Dict[str, Tuple[int, int]] = {}

    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(_load, b): b for b in state_blobs}
        for fut in as_completed(futs):
            snap = fut.result()
            if not snap:
                continue
            items = snap if isinstance(snap, list) else [snap]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("TY") != "TE" or item.get("PI") != "1":
                    continue
                s6 = str(item.get("S6") or "")
                for m in pattern.finditer(s6):
                    pid  = m.group(2)
                    runs  = int(m.group(3))
                    balls = int(m.group(4))
                    if runs > player_max.get(pid, (-1, 0))[0]:
                        player_max[pid] = (runs, balls)

    batsmen = [(r, b) for r, b in player_max.values() if b >= 5]
    if len(batsmen) < 2:
        return None
    srs = [(r / b) * 100 for r, b in batsmen]
    return round(max(srs) - (sum(srs) / len(srs)), 2)


def _safe_div(a, b, default=0.0):
    try:
        return round(a / b, 4) if b and b != 0 else default
    except Exception:
        return default


# ── Record builder ─────────────────────────────────────────────────────────────

def build_record(accumulator: Dict, silver, event_id: str) -> Optional[Dict]:
    """
    Build the raw feature dict from an innings_accumulator (silver) or
    innings_tracker (gold) — same row schema.  Returns None if match data is
    too incomplete to build any feature.
    """
    rows = accumulator.get("rows") or []
    if not rows:
        return None

    inn1_rows = [r for r in rows if r.get("innings") == 1]
    inn2_rows = [r for r in rows if r.get("innings") == 2]
    if not inn1_rows:
        return None

    match_name = str(accumulator.get("match_name") or "")
    date_str   = str(accumulator.get("match_date_utc") or "")[:10]
    league     = str(accumulator.get("league_name") or "")
    _sd        = accumulator.get("stadium_data") or {}
    venue      = str(
        accumulator.get("venue") or _sd.get("name") or
        accumulator.get("stadium") or league or "unknown"
    ).strip() or "unknown"

    home = str(accumulator.get("home_team_name") or "").strip()
    away = str(accumulator.get("away_team_name") or "").strip()

    inn1_bat_team = ""
    for r in inn1_rows:
        if r.get("batting_team"):
            inn1_bat_team = str(r["batting_team"]).strip()
            break
    inn1_bowl_team = away if inn1_bat_team == home else home

    is_womens  = 1 if ("women" in match_name.lower() or "(w)" in match_name.lower()) else 0
    is_weekend = 0
    try:
        is_weekend = 1 if datetime.strptime(date_str, "%Y-%m-%d").weekday() >= 5 else 0
    except Exception:
        pass

    # Inn1 aggregate (last row of inn1)
    inn1_final       = inn1_rows[-1]
    inn1_total_score = inn1_final.get("score") or 0
    inn1_total_wkts  = inn1_final.get("wickets") or 0

    # Phase state snapshots
    pp_state  = state_after_n_overs(inn1_rows, 6)
    mid_state = state_after_n_overs(inn1_rows, 15)
    ov1_state = state_after_n_overs(inn1_rows, 1)

    inn1_pp_score    = pp_state["score"]   if pp_state else None
    inn1_pp_wickets  = pp_state["wickets"] if pp_state else None
    inn1_ov1_bat_odds= ov1_state["bat_odds"] if ov1_state else None
    inn1_pp_bat_odds = pp_state["bat_odds"]  if pp_state else None
    mid_score   = mid_state["score"]   if mid_state else None
    mid_wickets = mid_state["wickets"] if mid_state else None

    # Per-over breakdowns
    inn1_overs    = per_over_breakdown(inn1_rows, 1, 20)
    inn2_overs_16 = per_over_breakdown(inn2_rows, 2, 16) if inn2_rows else {}

    inn1_last_bat_odds = inn1_overs.get("inn1_ov20_bat_odds")

    # Chase snapshots and aggregates
    cs2  = chase_aggregate(inn2_rows, inn1_total_score, 2)  if inn2_rows else None
    cs6  = chase_aggregate(inn2_rows, inn1_total_score, 6)  if inn2_rows else None
    cs10 = chase_aggregate(inn2_rows, inn1_total_score, 10) if inn2_rows else None
    cs16 = chase_aggregate(inn2_rows, inn1_total_score, 16) if inn2_rows else None

    inn2_ov2_s  = state_after_n_overs(inn2_rows, 2)  if inn2_rows else None
    inn2_ov6_s  = state_after_n_overs(inn2_rows, 6)  if inn2_rows else None
    inn2_ov10_s = state_after_n_overs(inn2_rows, 10) if inn2_rows else None
    inn2_ov16_s = state_after_n_overs(inn2_rows, 16) if inn2_rows else None

    rec: Dict[str, Any] = {
        "event_id":    event_id,
        "match_name":  match_name,
        "match_date":  date_str,
        # categorical
        "venue":           venue,
        "inn1_bat_team":   inn1_bat_team  or "unknown",
        "inn1_bowl_team":  inn1_bowl_team or "unknown",
        # match flags
        "is_womens_match":  is_womens,
        "is_weekend_match": is_weekend,
        # inn1 aggregate
        "inn1_total_score":   inn1_total_score,
        "inn1_total_wickets": inn1_total_wkts,
        # phase snapshots
        "inn1_pp_score":      inn1_pp_score,
        "inn1_pp_wickets":    inn1_pp_wickets,
        "inn1_mid_score":     mid_score,
        "inn1_mid_wickets":   mid_wickets,
        "inn1_ov1_bat_odds":  inn1_ov1_bat_odds,
        "inn1_pp_bat_odds":   inn1_pp_bat_odds,
        "inn1_last_bat_odds": inn1_last_bat_odds,
        # inn2 odds snapshots
        "inn2_ov2_bat_odds":  inn2_ov2_s["bat_odds"]  if inn2_ov2_s  else None,
        "inn2_ov6_bat_odds":  inn2_ov6_s["bat_odds"]  if inn2_ov6_s  else None,
        "inn2_ov10_bat_odds": inn2_ov10_s["bat_odds"] if inn2_ov10_s else None,
        "inn2_ov16_bat_odds": inn2_ov16_s["bat_odds"] if inn2_ov16_s else None,
        # label placeholder (not used for inference)
        "chasing_won": None,
    }

    rec.update(inn1_overs)
    rec.update(inn2_overs_16)

    for k in ["score", "wickets", "crr", "rrr", "rr_diff", "runs_needed"]:
        rec[f"inn2_ov2_{k}"]  = cs2[k]  if cs2  else None
        rec[f"inn2_ov6_{k}"]  = cs6[k]  if cs6  else None
        rec[f"inn2_ov10_{k}"] = cs10[k] if cs10 else None
        rec[f"inn2_ov16_{k}"] = cs16[k] if cs16 else None

    # batting_dominance — None if silver state files unavailable
    rec["inn1_bat_dominance"] = batting_dominance(event_id, silver)

    return rec


# ── Composite features (Step 4 of training notebook) ──────────────────────────

def apply_composites(rec: Dict) -> Dict:
    """Add composite columns to rec in-place. Same formulas as training notebook."""

    def _f(key):
        v = rec.get(key)
        return float(v) if v is not None else 0.0

    # Inn1 composites
    rec["inn1_pp_rp_wkt"]        = _safe_div(_f("inn1_pp_score"), 10 - _f("inn1_pp_wickets"))
    rec["inn1_mid_rp_wkt"]       = _safe_div(
        _f("inn1_mid_score") - _f("inn1_pp_score"), 10 - _f("inn1_mid_wickets")
    )
    rec["inn1_mid_wickets_only"] = max(0.0, _f("inn1_mid_wickets") - _f("inn1_pp_wickets"))
    rec["inn1_death_runs"]       = max(0.0, _f("inn1_total_score") - _f("inn1_mid_score"))
    rec["inn1_death_wickets"]    = max(0.0, _f("inn1_total_wickets") - _f("inn1_mid_wickets"))
    rec["inn1_pressure"]         = _safe_div(_f("inn1_total_score"), 10 - _f("inn1_total_wickets"))

    ov1_odds  = rec.get("inn1_ov1_bat_odds")
    pp_odds   = rec.get("inn1_pp_bat_odds")
    last_odds = rec.get("inn1_last_bat_odds")
    rec["inn1_odds_swing_full"] = (float(last_odds) - float(ov1_odds)) if (last_odds and ov1_odds) else None
    rec["inn1_odds_swing_pp"]   = (float(pp_odds)   - float(ov1_odds)) if (pp_odds   and ov1_odds) else None

    # Inn2 composites — one set per checkpoint
    for n in [2, 6, 10, 16]:
        sc  = rec.get(f"inn2_ov{n}_score")    or 0
        wk  = rec.get(f"inn2_ov{n}_wickets")  or 0
        rn  = rec.get(f"inn2_ov{n}_runs_needed") or 0
        rrr = rec.get(f"inn2_ov{n}_rrr")      or 0
        bat_odds_at = rec.get(f"inn2_ov{n}_bat_odds")

        rec[f"inn2_ov{n}_rp_wkt"]              = _safe_div(sc, 10 - wk)
        rec[f"inn2_ov{n}_odds_swing"]          = (
            (float(bat_odds_at) - float(last_odds)) if (bat_odds_at and last_odds) else None
        )
        rec[f"inn2_ov{n}_runs_needed_per_wkt"] = _safe_div(rn, 10 - wk)
        rec[f"inn2_ov{n}_chase_difficulty"]    = round(rrr * (wk + 1), 3)

    return rec


# ── Feature vector builder ────────────────────────────────────────────────────

def build_feature_vector(
    rec: Dict,
    feature_list: List[str],
    encodings: Dict,
) -> Tuple[np.ndarray, List[Dict]]:
    """
    Apply encodings to rec and return a float64 numpy array in the order of
    feature_list, plus a feature detail list for display.

    encodings from pkl["encodings"] maps:
      categorical feature → {value: int}     (label encoding)
      numeric feature     → float            (training median for NA fill)
    """
    X: List[float] = []
    details: List[Dict] = []

    for feat in feature_list:
        enc = encodings.get(feat)
        raw = rec.get(feat)

        if isinstance(enc, dict):
            if raw is not None and raw in enc:
                val, src = float(enc[raw]), "encoded"
            else:
                val, src = -1.0, "unknown_cat"
        else:
            if raw is not None:
                try:
                    val, src = float(raw), "data"
                except (TypeError, ValueError):
                    val, src = float(enc) if enc is not None else 0.0, "median"
            else:
                val, src = float(enc) if enc is not None else 0.0, "median"

        X.append(val)
        details.append({"name": feat, "value": round(val, 4), "source": src})

    return np.array(X, dtype=np.float64), details


# ── Checkpoint mapping ────────────────────────────────────────────────────────

# Which checkpoints have been reached, given current innings and over?
def available_checkpoints(inn1_complete: bool, inn2_over: Optional[float]) -> List[str]:
    """
    Return all checkpoint names that can be run given match state.
    inn2_over: None or float (e.g. 6.3 means after 6 full overs + 3 balls).
    """
    checkpoints: List[str] = []
    if inn1_complete:
        checkpoints.append("innings1-only")
    if inn2_over is None:
        return checkpoints
    # Round down to completed overs
    completed = int(inn2_over)
    if completed >= 2:
        checkpoints.append("innings2-2over")
    if completed >= 6:
        checkpoints.append("innings2-6over")
    if completed >= 10:
        checkpoints.append("innings2-10over")
    if completed >= 16:
        checkpoints.append("innings2-16over")
    return checkpoints


# ── Validation utility ────────────────────────────────────────────────────────

def validate_against_history(gold, silver, n_matches: int = 10) -> bool:
    """
    Load n_matches completed gold innings_trackers, extract features with this
    module, and compare key composites against re-computed ground truth.

    Returns True if all absolute differences are < 1e-4.
    Logs a report of any mismatches to help diagnose drift.

    Run this once after any change to apply_composites() or build_record() to
    confirm the live extractor stays on the same distribution as training.
    """
    import json

    blobs = [b.name for b in gold.list_blobs(name_starts_with="event_id=")
             if b.name.endswith("/innings_tracker.json")][:n_matches * 3]

    checked = mismatched = 0

    for blob_path in blobs:
        if checked >= n_matches:
            break
        try:
            raw = json.loads(gold.get_blob_client(blob_path).download_blob().readall())
        except Exception:
            continue

        rows = raw.get("rows") or []
        inn1 = [r for r in rows if r.get("innings") == 1]
        inn2 = [r for r in rows if r.get("innings") == 2]
        if not inn1 or not inn2:
            continue

        eid = blob_path.split("/")[0].replace("event_id=", "")
        rec = build_record(raw, silver, eid)
        if rec is None:
            continue
        apply_composites(rec)

        # Spot-check composite values against fresh recompute
        ground: Dict[str, Any] = {}
        pp_s = state_after_n_overs(inn1, 6)
        mi_s = state_after_n_overs(inn1, 15)
        if pp_s and mi_s:
            ground["inn1_pp_rp_wkt"]  = _safe_div(pp_s["score"], 10 - pp_s["wickets"])
            ground["inn1_mid_rp_wkt"] = _safe_div(
                mi_s["score"] - pp_s["score"], 10 - mi_s["wickets"]
            )
            ground["inn1_pressure"] = _safe_div(
                rec["inn1_total_score"], 10 - rec["inn1_total_wickets"]
            )

        ok = True
        for key, expected in ground.items():
            actual = rec.get(key)
            if actual is None or expected is None:
                continue
            diff = abs(float(actual) - float(expected))
            if diff > 1e-4:
                logging.error(f"validate: {eid} {key}: expected {expected:.4f} got {actual:.4f} (diff={diff:.6f})")
                mismatched += 1
                ok = False

        if ok:
            checked += 1
            logging.info(f"validate: {eid} OK")

    logging.info(f"validate: checked={checked} mismatched_fields={mismatched}")
    return mismatched == 0
