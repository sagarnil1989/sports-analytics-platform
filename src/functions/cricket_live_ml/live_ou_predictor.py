"""
Live Over/Under Predictions — Phase 5.

Reads silver innings_accumulator.json for recently active matches, builds the
Over/Under feature vector (same logic as ml_extract_over_under_features.py),
loads the calibrated pkl from gold, runs inference, and writes:
  gold/event_id={id}/live_predictions.json

ISOLATION RULE: only writes to gold/event_id=*/live_predictions.json.
Reads from silver (read-only). Never touches bronze or any shared gold path.

The "recently active" heuristic: silver accumulator blob modified within the
last 10 minutes — avoids needing bronze container access (which this Function
App does not have RBAC for).
"""

import io
import json
import logging
import pickle
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


# ── Constants ──────────────────────────────────────────────────────────────────

_ACCUM_PREFIX  = "cricket/inplay/event_id="
_ACCUM_SUFFIX  = "/innings_accumulator.json"
_MODEL_PREFIX  = "ml/live_models/over_under"
_LIVE_PRED_KEY = "event_id={eid}/live_predictions.json"
_BALL_TOL      = 5      # ±5 balls from checkpoint target
_LIVE_TTL_MIN  = 10     # accumulators modified within 10 minutes are "live"

# Market definitions: (name, checkpoints, market_total_balls, innings)
_MARKETS = [
    ("innings_total",     [2, 4, 6, 8, 10, 12, 14, 16], 120, 1),
    ("first_12_overs",    [2, 4, 6, 8],                   72, 1),
    ("first_6_overs",     [1, 2, 3, 4, 5],                36, 1),
    ("inn2_first_6",      [1, 2, 3, 4, 5],                36, 2),
    ("inn2_first_12",     [2, 4, 6, 8],                   72, 2),
]


# ── Helpers matching ml_extract_over_under_features.py ────────────────────────

def _over_to_balls(over_str: Optional[str]) -> Optional[int]:
    if not over_str:
        return None
    try:
        parts = str(over_str).split(".")
        return int(parts[0]) * 6 + (int(parts[1]) if len(parts) > 1 else 0)
    except Exception:
        return None


def _find_checkpoint_row(rows: List[Dict], cp: int, innings: int = 1) -> Optional[Dict]:
    target = cp * 6
    best, best_dist = None, _BALL_TOL + 1
    for r in rows:
        if r.get("innings") != innings:
            continue
        balls = _over_to_balls(r.get("over"))
        if balls is None:
            continue
        dist = abs(balls - target)
        cur_best_balls = _over_to_balls(best.get("over")) if best else -1
        if dist < best_dist or (dist == best_dist and balls > cur_best_balls):
            best_dist, best = dist, r
    return best


def _linear_slope(xs: List[float], ys: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 2:
        return None
    sx, sy = sum(xs), sum(ys)
    sxy = sum(x * y for x, y in zip(xs, ys))
    sxx = sum(x * x for x in xs)
    d = n * sxx - sx * sx
    return round((n * sxy - sx * sy) / d, 4) if d != 0 else 0.0


def _extract_trajectory_features(rows: List[Dict], cp: int, market_total_balls: int, innings: int) -> Dict:
    """Port of ml_extract_over_under_features._extract_trajectory_features."""
    result: Dict = {}
    prev_score: Optional[int] = 0
    lines: List[Tuple[int, float]] = []
    vs_pace_vals: List[Tuple[int, float]] = []
    over_runs: List[float] = []

    for k in range(1, cp + 1):
        row_k = _find_checkpoint_row(rows, k, innings=innings)
        if row_k is None:
            result[f"ov{k}_runs"] = result[f"ov{k}_cumwkts"] = result[f"ov{k}_line"] = result[f"ov{k}_vs_pace"] = None
            prev_score = None
            continue

        score_k   = row_k.get("score")
        wickets_k = row_k.get("wickets") or 0
        balls_k   = _over_to_balls(row_k.get("over")) or (k * 6)
        line_k    = row_k.get("predicted_total")

        runs_k: Optional[float] = None
        if score_k is not None and prev_score is not None:
            runs_k = max(0.0, score_k - prev_score)
            over_runs.append(float(runs_k))

        result[f"ov{k}_runs"]    = runs_k
        result[f"ov{k}_cumwkts"] = wickets_k
        result[f"ov{k}_line"]    = line_k

        vs_pace_k: Optional[float] = None
        if line_k is not None and score_k is not None and balls_k > 0:
            expected  = float(line_k) * balls_k / market_total_balls
            vs_pace_k = round(score_k - expected, 2)
            vs_pace_vals.append((k, vs_pace_k))
        result[f"ov{k}_vs_pace"] = vs_pace_k

        if line_k is not None:
            lines.append((k, float(line_k)))
        prev_score = score_k

    # Line trajectory
    if lines:
        lkeys = [k for k, _ in lines]
        lvals = [v for _, v in lines]
        result["line_ov1"]         = lvals[0]
        result["line_drift_total"] = round(lvals[-1] - lvals[0], 2) if len(lvals) >= 2 else 0.0
        result["line_trend_slope"] = _linear_slope(lkeys, lvals)
        diffs = [lvals[i + 1] - lvals[i] for i in range(len(lvals) - 1)]
        result["pct_overs_line_up"] = round(sum(1 for d in diffs if d > 0) / len(diffs), 3) if diffs else None
        result["max_line_jump"]     = round(max((d for d in diffs if d > 0), default=0.0), 2)
        result["line_accel"]        = round((lvals[-1] - lvals[len(lvals) // 2]) - (lvals[len(lvals) // 2] - lvals[0]), 2) if len(lvals) >= 4 else None
    else:
        for fk in ("line_ov1", "line_drift_total", "line_trend_slope", "pct_overs_line_up", "max_line_jump", "line_accel"):
            result[fk] = None

    result["score_vs_pace_at_ov2"] = next((v for k, v in vs_pace_vals if k == 2), None)
    result["score_vs_pace_trend"]  = _linear_slope([k for k, _ in vs_pace_vals], [v for _, v in vs_pace_vals]) if len(vs_pace_vals) >= 2 else None
    result["recent_rr_2"]   = round(sum(over_runs[-2:]) / 2, 2) if len(over_runs) >= 2 else None
    result["recent_rr_4"]   = round(sum(over_runs[-4:]) / 4, 2) if len(over_runs) >= 4 else None
    result["max_over_runs"] = max(over_runs) if over_runs else None
    result["min_over_runs"] = min(over_runs) if over_runs else None

    if len(over_runs) >= 4:
        fh_avg = sum(over_runs[:len(over_runs) // 2]) / (len(over_runs) // 2)
        result["rr_trend"] = round(result["recent_rr_2"] - fh_avg, 2) if result.get("recent_rr_2") is not None else None
    else:
        result["rr_trend"] = None

    # First wicket over
    first_wkt = cp + 1
    for k in range(1, cp + 1):
        if (result.get(f"ov{k}_cumwkts") or 0) > 0:
            first_wkt = k
            break
    result["first_wkt_over"] = first_wkt

    # Powerplay (only available for cp >= 6)
    if cp >= 6:
        row_pp = _find_checkpoint_row(rows, 6, innings=innings)
        result["pp_score"]   = row_pp.get("score")           if row_pp else None
        result["pp_wickets"] = (row_pp.get("wickets") or 0)  if row_pp else None
    else:
        result["pp_score"] = result["pp_wickets"] = None

    return result


def _build_ou_feature_vector(
    rows: List[Dict],
    cp: int,
    market_total_balls: int,
    innings: int,
    meta: Dict,           # match-level metadata: league, venue, batting_team, bowling_team, gender
    cat_encodings: Dict,  # from model_obj["cat_encodings"]
    feature_list: List[str],
    stored_medians: Dict, # from model_obj["cat_encodings"] — numeric medians for missing features
) -> Optional[List[float]]:
    """Build the feature vector for inference at checkpoint cp."""
    cp_row = _find_checkpoint_row(rows, cp, innings=innings)
    if cp_row is None:
        return None

    score   = cp_row.get("score")
    wickets = cp_row.get("wickets") or 0
    balls   = _over_to_balls(cp_row.get("over")) or (cp * 6)

    if score is None:
        return None

    line    = cp_row.get("predicted_total")
    batting_win_odds = cp_row.get("batting_team_odds")

    # Base features
    wickets_in_hand = 10 - wickets
    score_vs_line_pace = (score - float(line) * balls / market_total_balls) if line else None
    run_rate          = round(score * 6 / balls, 3) if balls > 0 else None
    rr_required       = None
    if line is not None and score is not None and balls < market_total_balls:
        balls_left  = market_total_balls - balls
        rr_required = round((float(line) - score) / (balls_left / 6), 3) if balls_left > 0 else None

    over_odds  = cp_row.get("over_odds_at_line")
    implied_prob_over = (1.0 / float(over_odds)) if over_odds and float(over_odds) > 0 else 0.5

    is_weekend = meta.get("is_weekend", 0)
    balls_done      = balls
    balls_remaining = max(0, market_total_balls - balls_done)
    balls_completed = balls_done

    # Categorical encodings (target-mean style: category → float mean label)
    def _enc(key, val, default=0.5):
        enc_map = cat_encodings.get(key) or {}
        if isinstance(enc_map, dict):
            return enc_map.get(str(val), default)
        return default

    global_mean = cat_encodings.get("__global_mean__", 0.5)
    venue_enc       = _enc("venue_enc",        meta.get("venue", ""),        global_mean)
    league_enc      = _enc("league_enc",       meta.get("league_name", ""),  global_mean)
    gender_enc      = _enc("gender_enc",       meta.get("gender", "male"),   global_mean)
    bat_team_enc    = _enc("batting_team_enc", meta.get("batting_team", ""), global_mean)
    bowl_team_enc   = _enc("bowling_team_enc", meta.get("bowling_team", ""), global_mean)

    # Trajectory features
    traj = _extract_trajectory_features(rows, cp, market_total_balls=market_total_balls, innings=innings)

    # Build feature dict
    feat_dict: Dict[str, Any] = {
        "score":               score,
        "wickets_in_hand":     wickets_in_hand,
        "betting_line":        line,
        "score_vs_line_pace":  score_vs_line_pace,
        "run_rate":            run_rate,
        "rr_required":         rr_required,
        "implied_prob_over":   implied_prob_over,
        "batting_team_win_odds": batting_win_odds,
        "is_weekend_match":    is_weekend,
        "venue_enc":           venue_enc,
        "league_enc":          league_enc,
        "gender_enc":          gender_enc,
        "batting_team_enc":    bat_team_enc,
        "bowling_team_enc":    bowl_team_enc,
        "checkpoint_over":     cp,
        "balls_remaining":     balls_remaining,
        "balls_completed":     balls_completed,
        **traj,
    }

    # Build vector in feature_list order, filling unknowns from stored_medians
    X = []
    for feat in feature_list:
        val = feat_dict.get(feat)
        if val is None:
            med = stored_medians.get(feat)
            val = float(med) if med is not None else 0.0
        else:
            try:
                val = float(val)
            except (TypeError, ValueError):
                val = 0.0
        X.append(val)

    return X


# ── Silver scanning ────────────────────────────────────────────────────────────

def _list_live_event_ids(silver) -> List[str]:
    """Return event IDs whose accumulator blob was modified in the last _LIVE_TTL_MIN minutes."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=_LIVE_TTL_MIN)
    eids = []
    try:
        for blob in silver.list_blobs(name_starts_with=_ACCUM_PREFIX):
            if not blob.name.endswith(_ACCUM_SUFFIX):
                continue
            last_mod = blob.last_modified
            if last_mod and last_mod.replace(tzinfo=timezone.utc) >= cutoff:
                # Extract event_id from path: cricket/inplay/event_id=XXX/innings_accumulator.json
                part = blob.name.replace(_ACCUM_PREFIX, "").split("/")[0]
                eids.append(part)
    except Exception as e:
        logging.warning(f"live_ou: failed scanning silver: {e}")
    return eids


def _load_accumulator(silver, event_id: str) -> Optional[Dict]:
    path = f"{_ACCUM_PREFIX}{event_id}{_ACCUM_SUFFIX}"
    try:
        return json.loads(silver.get_blob_client(path).download_blob().readall())
    except Exception:
        return None


def _load_model(gold, model_key: str) -> Optional[Dict]:
    path = f"{_MODEL_PREFIX}/{model_key}.pkl"
    try:
        raw = gold.get_blob_client(path).download_blob().readall()
        return pickle.loads(raw)
    except Exception:
        return None


def _current_innings_over(rows: List[Dict]) -> Tuple[int, float]:
    """Return (current_innings, current_over_float) from the latest accumulator row."""
    if not rows:
        return 1, 0.0
    last = rows[-1]
    innings = last.get("innings") or 1
    try:
        over_f = float(str(last.get("over") or "0"))
    except Exception:
        over_f = 0.0
    return innings, over_f


def _is_gender_womens(match_name: str) -> str:
    ml = match_name.lower()
    return "female" if ("women" in ml or "(w)" in ml) else "male"


def _extract_match_meta(accumulator: Dict) -> Dict:
    rows = accumulator.get("rows") or []
    batting_team = bowling_team = ""
    for r in rows:
        if r.get("batting_team"):
            batting_team = str(r["batting_team"]).strip()
        if r.get("bowling_team"):
            bowling_team = str(r["bowling_team"]).strip()
        if batting_team and bowling_team:
            break

    match_name = accumulator.get("match_name") or ""
    league_name = accumulator.get("league_name") or ""
    venue = accumulator.get("venue") or accumulator.get("stadium") or ""
    match_date = str(accumulator.get("match_date_utc") or "")[:10]
    is_weekend = 0
    try:
        wd = datetime.strptime(match_date, "%Y-%m-%d").weekday()
        is_weekend = 1 if wd >= 5 else 0
    except Exception:
        pass

    return {
        "batting_team":  batting_team,
        "bowling_team":  bowling_team,
        "venue":         venue,
        "league_name":   league_name,
        "gender":        _is_gender_womens(match_name),
        "is_weekend":    is_weekend,
        "match_name":    match_name,
    }


# ── Per-event prediction ───────────────────────────────────────────────────────

def _predict_for_event(gold, event_id: str, accumulator: Dict) -> Optional[Dict]:
    rows = accumulator.get("rows") or []
    if not rows:
        return None

    meta = _extract_match_meta(accumulator)
    current_innings, current_over = _current_innings_over(rows)

    predictions = []

    for market_name, checkpoints, market_total_balls, innings in _MARKETS:
        # Only run markets for innings that have started
        if innings > current_innings:
            continue
        if innings == current_innings and current_over < 1.0:
            continue

        # Determine which checkpoints have been reached
        inn_rows = [r for r in rows if r.get("innings") == innings]
        if not inn_rows:
            continue

        for cp in checkpoints:
            # Skip checkpoints not yet reached
            if innings == current_innings:
                if current_over < cp - 0.5:
                    continue

            model_key = f"{market_name}_cp{cp}"
            model_obj = _load_model(gold, model_key)

            # Also try pooled model as fallback
            if model_obj is None:
                # Try single-cp model under the exact checkpoint key (different naming?)
                pass  # continue to next

            if model_obj is None:
                continue

            feature_list  = model_obj.get("features") or []
            cat_encodings = model_obj.get("cat_encodings") or {}
            # stored_medians: the cat_encodings dict also contains numeric medians for per-over features
            # (same dict, numeric medians stored alongside cat dicts for the pooled model)
            stored_medians = {}
            for k, v in cat_encodings.items():
                if isinstance(v, (int, float)):
                    stored_medians[k] = v

            X = _build_ou_feature_vector(
                rows=inn_rows,
                cp=cp,
                market_total_balls=market_total_balls,
                innings=innings,
                meta=meta,
                cat_encodings=cat_encodings,
                feature_list=feature_list,
                stored_medians=stored_medians,
            )
            if X is None:
                continue

            try:
                model = model_obj["model"]
                X_arr = np.array(X, dtype=float).reshape(1, -1)
                proba = model.predict_proba(X_arr)[0]
                prob_over  = round(float(proba[1]), 4)
                prob_under = round(float(proba[0]), 4)
            except Exception as e:
                logging.warning(f"live_ou: inference failed for {event_id}/{model_key}: {e}")
                continue

            # State at checkpoint
            cp_row = _find_checkpoint_row(inn_rows, cp, innings=innings)
            predictions.append({
                "market":           market_name,
                "innings":          innings,
                "checkpoint_over":  cp,
                "prob_over":        prob_over,
                "prob_under":       prob_under,
                "betting_line":     cp_row.get("predicted_total") if cp_row else None,
                "score":            cp_row.get("score") if cp_row else None,
                "wickets":          cp_row.get("wickets") if cp_row else None,
                "model_key":        model_key,
            })

    if not predictions:
        return None

    return {
        "event_id":       event_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "current_innings":  current_innings,
        "current_over":     current_over,
        "match_name":       meta.get("match_name", ""),
        "source":           "live_ml_phase5",
        "ou_predictions":   predictions,
    }


# ── Public entry point ────────────────────────────────────────────────────────

def run_live_ou_predictions(gold, silver) -> int:
    """
    Main entry point called by the live_ml_tick timer.
    Returns the number of events that received predictions.
    """
    event_ids = _list_live_event_ids(silver)
    if not event_ids:
        logging.info("live_ou: no recently-active accumulators in silver")
        return 0

    logging.info(f"live_ou: found {len(event_ids)} live event(s): {event_ids}")
    written = 0

    for eid in event_ids:
        try:
            accum = _load_accumulator(silver, eid)
            if not accum:
                continue
            result = _predict_for_event(gold, eid, accum)
            if result is None:
                continue
            blob_path = _LIVE_PRED_KEY.format(eid=eid)
            gold.get_blob_client(blob_path).upload_blob(
                json.dumps(result, indent=2).encode(), overwrite=True
            )
            written += 1
            logging.info(f"live_ou: wrote {len(result['ou_predictions'])} prediction(s) for {eid}")
        except Exception as e:
            logging.exception(f"live_ou: unexpected error for event {eid}: {e}")

    return written
