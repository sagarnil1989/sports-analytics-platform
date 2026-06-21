"""
over_under_predictor — Over/Under inference for innings_total and first_12_overs.

Called from gold_rebuild._rebuild_innings_core() after the innings tracker is written.
Runs only on Databricks (models live on DBFS at /dbfs/FileStore/cricket-pipeline/models/).

Writes gold/event_id={eid}/over_under_predictions.json with one prediction row per
(market, checkpoint_over).

Model selection:
  - Per-checkpoint model used when its CV-AUC >= AUC_THRESHOLD (currently 0.60).
  - Pooled model used for all other checkpoints.
  - If DBFS models are not present (e.g. local dev), the call is a silent no-op.
"""

import json
import os
import pickle
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MODEL_DIR    = "/dbfs/FileStore/cricket-pipeline/models/over_under"
_AUC_THRESHOLD = 0.60   # use per-checkpoint model only if CV-AUC >= this

_IT_CHECKPOINTS  = [2, 4, 6, 8, 10, 12, 14, 16]
_F12_CHECKPOINTS = [4, 6, 8]

# First-12-Overs market identifiers in silver active_markets rows
_F12_GROUP_ID    = "29"
_F12_TMPL_ID     = "30171"
_F12_NAME_SUBSTR = "first 12"

# Feature lists — must match training exactly
_FEATURES = [
    "score",
    "wickets_in_hand",
    "betting_line",
    "score_vs_line_pace",
    "run_rate",
    "rr_required",
    "implied_prob_over",
    "batting_team_win_odds",
]
_POOLED_FEATURES = _FEATURES + ["checkpoint_over", "balls_remaining", "balls_completed"]

# Module-level model cache so models are loaded once per cluster lifetime
_model_cache: Dict[str, Any] = {}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _models_available() -> bool:
    return os.path.isdir(_MODEL_DIR)


def _load_model(key: str) -> Optional[Any]:
    if key in _model_cache:
        return _model_cache[key]
    path = os.path.join(_MODEL_DIR, f"{key}.pkl")
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        obj = pickle.load(f)
    _model_cache[key] = obj
    return obj


def _over_to_balls(over_str: Optional[str]) -> Optional[int]:
    if not over_str:
        return None
    try:
        parts = str(over_str).split(".")
        return int(parts[0]) * 6 + (int(parts[1]) if len(parts) > 1 else 0)
    except Exception:
        return None


def _find_checkpoint_row(rows: List[Dict], checkpoint_over: int, innings: int = 1) -> Optional[Dict]:
    """Return the tracker row closest to checkpoint_over * 6 balls in the given innings."""
    target = checkpoint_over * 6
    best_row  = None
    best_dist = 6   # tolerance = 1 over

    for r in rows:
        if r.get("innings") != innings:
            continue
        balls = _over_to_balls(r.get("over"))
        if balls is None:
            continue
        dist = abs(balls - target)
        if dist < best_dist or (
            dist == best_dist
            and balls > (_over_to_balls(best_row.get("over")) or 0 if best_row else 0)
        ):
            best_dist = dist
            best_row  = r
    return best_row


def _find_score_at_over(rows: List[Dict], target_over: int, innings: int = 1) -> Optional[int]:
    row = _find_checkpoint_row(rows, target_over, innings=innings)
    return row.get("score") if row else None


def _get_first12_line(silver_container, event_id: str, snapshot_id: str
                      ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Read silver active_markets.json and return (line, over_odds, under_odds)
    for the "Runs in First 12 Overs" market.  Returns (None, None, None) if absent.
    """
    try:
        path = f"event_id={event_id}/snapshot_id={snapshot_id}/active_markets.json"
        raw  = json.loads(silver_container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None, None, None

    rows = raw.get("rows", [])
    f12  = [
        r for r in rows
        if str(r.get("market_group_id")      or "") == _F12_GROUP_ID
        and str(r.get("market_template_id")  or "") == _F12_TMPL_ID
        and _F12_NAME_SUBSTR in str(r.get("market_group_name") or "").lower()
    ]
    if not f12:
        return None, None, None

    by_line: Dict[str, Dict] = {}
    for r in f12:
        sel  = str(r.get("selection_name") or "").strip().lower()
        odds = r.get("odds_decimal")
        line = r.get("handicap") or r.get("line")
        if not line or not odds:
            continue
        try:
            lv = float(line)
        except Exception:
            continue
        key = str(lv)
        by_line.setdefault(key, {})
        if sel == "over":
            by_line[key]["over"] = (float(odds), lv)
        elif sel == "under":
            by_line[key]["under"] = (float(odds), lv)

    best_line = best_ov = best_un = None
    best_diff = float("inf")
    for sides in by_line.values():
        if "over" not in sides or "under" not in sides:
            continue
        ov_o, lv = sides["over"]
        un_o, _  = sides["under"]
        diff = abs(ov_o - un_o)
        if diff < best_diff:
            best_diff = diff
            best_line = lv
            best_ov   = ov_o
            best_un   = un_o
    return best_line, best_ov, best_un


def _build_vector(
    score: int,
    wickets: int,
    balls: int,
    betting_line: float,
    over_odds: Optional[float],
    under_odds: Optional[float],
    batting_win_odds: Optional[float],
    checkpoint_over: int,
    total_balls: int,   # 120 for inn_total, 72 for first_12
) -> Optional[np.ndarray]:
    """Build feature vector for the core FEATURES list."""
    if batting_win_odds is None:
        batting_win_odds = 2.0   # neutral fallback (50/50)

    balls_remaining = max(0, total_balls - balls)
    run_rate   = (score * 6 / balls) if balls > 0 else 0.0
    rr_req     = ((betting_line - score) * 6 / balls_remaining) if balls_remaining > 0 else 0.0
    pace_diff  = score - betting_line * balls / total_balls
    imp_over   = (1.0 / over_odds) if over_odds and over_odds > 0 else 0.5

    vec = [
        float(score),
        float(10 - (wickets or 0)),
        float(betting_line),
        round(pace_diff, 3),
        round(run_rate, 3),
        round(rr_req, 3),
        round(imp_over, 4),
        float(batting_win_odds),
    ]
    return np.array(vec, dtype=np.float32).reshape(1, -1)


def _build_pooled_vector(
    base_vec: np.ndarray,
    checkpoint_over: int,
    balls: int,
    total_balls: int,
) -> np.ndarray:
    """Append pooled-only features to a base feature vector."""
    balls_remaining = max(0, total_balls - balls)
    extra = np.array([float(checkpoint_over), float(balls_remaining), float(balls)],
                     dtype=np.float32).reshape(1, -1)
    return np.concatenate([base_vec, extra], axis=1)


def _predict(model_obj: Dict, X: np.ndarray) -> Optional[float]:
    """Run model and return predicted P(OVER)."""
    try:
        model = model_obj["model"]
        prob  = model.predict_proba(X)[0][1]
        return round(float(prob), 4)
    except Exception:
        return None


def _load_model_metadata(gold_container) -> Dict[str, Dict]:
    """Load CV-AUC per model key from gold metadata JSON."""
    try:
        raw  = json.loads(
            gold_container.get_blob_client("ml/over_under_model_metadata.json")
            .download_blob().readall()
        )
        return {
            f"{m['market']}_cp{m['checkpoint_over']}": m.get("cv_auc", 0.0)
            for m in raw.get("models", [])
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_over_under_predictions(
    silver_container,
    gold_container,
    event_id: str,
    tracker: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Compute Over/Under predictions for all checkpoint overs and write
    gold/event_id={event_id}/over_under_predictions.json.

    Returns the predictions dict, or None if models are unavailable.
    """
    if not _models_available():
        return None

    rows         = tracker.get("rows") or []
    actual_total = tracker.get("actual_total")
    inn1_rows    = [r for r in rows if r.get("innings") == 1]
    if not inn1_rows:
        return None

    # T20 check: max completed over in inn1 must be 15–20
    max_over = max(
        (int(str(r.get("over") or "0").split(".")[0]) for r in inn1_rows),
        default=0,
    )
    if not (15 <= max_over <= 20):
        return None

    # Load AUC metadata to decide which model to use per checkpoint
    auc_by_key = _load_model_metadata(gold_container)

    predictions_it  = []
    predictions_f12 = []
    now_str = datetime.now(timezone.utc).isoformat()

    # ── Innings Total ─────────────────────────────────────────────────────────
    actual_first12 = _find_score_at_over(inn1_rows, 12, innings=1)

    for cp in _IT_CHECKPOINTS:
        row = _find_checkpoint_row(inn1_rows, cp, innings=1)
        if row is None:
            continue

        line     = row.get("predicted_total")
        ov_odds  = row.get("over_odds_at_line")
        un_odds  = row.get("under_odds_at_line")
        score    = row.get("score")
        wickets  = row.get("wickets")
        bat_odds = row.get("batting_team_odds")

        if line is None or score is None:
            continue

        balls = _over_to_balls(row.get("over")) or (cp * 6)
        base_vec = _build_vector(score, wickets or 0, balls, line,
                                  ov_odds, un_odds, bat_odds, cp, 120)
        if base_vec is None:
            continue

        # Choose model: per-checkpoint if AUC ≥ threshold, else pooled
        per_cp_key = f"innings_total_cp{cp}"
        per_cp_auc = auc_by_key.get(per_cp_key, 0.0)
        if per_cp_auc >= _AUC_THRESHOLD:
            model_obj  = _load_model(per_cp_key)
            model_used = per_cp_key
            model_auc  = per_cp_auc
            X = base_vec
        else:
            model_obj  = _load_model("innings_total_pooled")
            model_used = "innings_total_pooled"
            model_auc  = auc_by_key.get("innings_total_cpooled", 0.631)
            X = _build_pooled_vector(base_vec, cp, balls, 120)

        if model_obj is None:
            continue

        prob_over = _predict(model_obj, X)
        if prob_over is None:
            continue

        entry: Dict[str, Any] = {
            "checkpoint_over":   cp,
            "snapshot_id":       row.get("snapshot_id"),
            "over_str":          row.get("over"),
            "score":             score,
            "wickets_in_hand":   10 - (wickets or 0),
            "betting_line":      line,
            "over_odds":         ov_odds,
            "under_odds":        un_odds,
            "prob_over":         prob_over,
            "prob_under":        round(1 - prob_over, 4),
            "model_used":        model_used,
            "model_auc":         round(model_auc, 3),
        }
        if actual_total is not None:
            entry["actual_total"]  = actual_total
            entry["actual_margin"] = round(actual_total - line, 1)
            entry["outcome"]       = ("over" if actual_total > line
                                      else "under" if actual_total < line else "push")
        predictions_it.append(entry)

    # ── First 12 Overs ────────────────────────────────────────────────────────
    for cp in _F12_CHECKPOINTS:
        row = _find_checkpoint_row(inn1_rows, cp, innings=1)
        if row is None:
            continue

        snapshot_id = row.get("snapshot_id")
        if not snapshot_id:
            continue

        line, ov_odds, un_odds = _get_first12_line(silver_container, event_id, snapshot_id)
        if line is None:
            continue

        score    = row.get("score")
        wickets  = row.get("wickets")
        bat_odds = row.get("batting_team_odds")
        if score is None:
            continue

        balls    = _over_to_balls(row.get("over")) or (cp * 6)
        base_vec = _build_vector(score, wickets or 0, balls, line,
                                  ov_odds, un_odds, bat_odds, cp, 72)
        if base_vec is None:
            continue

        model_obj  = _load_model("first_12_overs_pooled")
        model_used = "first_12_overs_pooled"
        model_auc  = auc_by_key.get("first_12_overs_cpooled", 0.525)
        X = _build_pooled_vector(base_vec, cp, balls, 72)

        if model_obj is None:
            continue

        prob_over = _predict(model_obj, X)
        if prob_over is None:
            continue

        entry = {
            "checkpoint_over":   cp,
            "snapshot_id":       snapshot_id,
            "over_str":          row.get("over"),
            "score":             score,
            "wickets_in_hand":   10 - (wickets or 0),
            "betting_line":      line,
            "over_odds":         ov_odds,
            "under_odds":        un_odds,
            "prob_over":         prob_over,
            "prob_under":        round(1 - prob_over, 4),
            "model_used":        model_used,
            "model_auc":         round(model_auc, 3),
        }
        if actual_first12 is not None:
            entry["actual_first12"] = actual_first12
            entry["actual_margin"]  = round(actual_first12 - line, 1)
            entry["outcome"]        = ("over" if actual_first12 > line
                                       else "under" if actual_first12 < line else "push")
        predictions_f12.append(entry)

    if not predictions_it and not predictions_f12:
        return None

    output = {
        "event_id":         event_id,
        "generated_at_utc": now_str,
        "innings_total":    predictions_it,
        "first_12_overs":   predictions_f12,
    }

    try:
        from util import upload_json
        upload_json(
            gold_container,
            f"event_id={event_id}/over_under_predictions.json",
            output,
            overwrite=True,
        )
    except Exception:
        pass

    return output
