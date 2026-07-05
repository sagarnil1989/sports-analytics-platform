"""
Live Win Predictor — Phase 6.

Reads silver innings_accumulator.json for recently-active matches, builds the
win predictor feature vector using live_feature_extractor.py (same logic as the
Databricks training notebook), loads the XGBoost pkl from gold, and appends
win_predictions to gold/event_id={id}/live_predictions.json.

ISOLATION RULE: only writes to gold/event_id=*/live_predictions.json.
"""

import json
import logging
import pickle
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

from live_feature_extractor import (
    available_checkpoints,
    apply_composites,
    build_feature_vector,
    build_record,
)

_MODEL_PREFIX  = "cricket/ml_features/t20/live_models/win_predictor"
_LIVE_PRED_KEY = "event_id={eid}/live_predictions.json"
_ACCUM_PREFIX  = "event_id="
_ACCUM_SUFFIX  = "/innings_accumulator.json"

_CHECKPOINT_INN2_OVER = {
    "innings1-only":   None,
    "innings2-2over":  2,
    "innings2-6over":  6,
    "innings2-10over": 10,
    "innings2-16over": 16,
}


# ── Model cache (in-process, warm for the lifetime of the worker) ─────────────

_model_cache: Dict[str, Dict] = {}


def _load_model(gold, checkpoint: str) -> Optional[Dict]:
    if checkpoint in _model_cache:
        return _model_cache[checkpoint]
    path = f"{_MODEL_PREFIX}/{checkpoint}.pkl"
    try:
        raw = gold.get_blob_client(path).download_blob().readall()
        obj = pickle.loads(raw)
        _model_cache[checkpoint] = obj
        return obj
    except Exception as e:
        logging.warning(f"live_win: could not load model {checkpoint}: {e}")
        return None


# ── Match state detection ─────────────────────────────────────────────────────

def _is_inn1_complete(rows: List[Dict]) -> bool:
    """Innings 1 is complete if any row with innings=2 exists."""
    return any(r.get("innings") == 2 for r in rows)


def _inn2_current_over(rows: List[Dict]) -> Optional[float]:
    """Float over of the latest inn2 row (e.g. 6.3), or None."""
    inn2 = [r for r in rows if r.get("innings") == 2]
    if not inn2:
        return None
    last = inn2[-1]
    try:
        return float(str(last.get("over") or "0"))
    except Exception:
        return 0.0


# ── Per-checkpoint inference ──────────────────────────────────────────────────

def _predict_checkpoint(
    gold,
    checkpoint: str,
    rec: Dict,
) -> Optional[Dict]:
    model_obj = _load_model(gold, checkpoint)
    if model_obj is None:
        return None

    features  = model_obj.get("features") or []
    encodings = model_obj.get("encodings") or {}

    X, details = build_feature_vector(rec, features, encodings)
    try:
        proba = model_obj["model"].predict_proba(X.reshape(1, -1))[0]
    except Exception as e:
        logging.warning(f"live_win: inference error for {checkpoint}: {e}")
        return None

    prob_chase  = round(float(proba[1]), 4)
    prob_defend = round(float(proba[0]), 4)

    inn2_over = _CHECKPOINT_INN2_OVER.get(checkpoint)
    if inn2_over:
        s = rec.get(f"inn2_ov{inn2_over}_score")
        w = rec.get(f"inn2_ov{inn2_over}_wickets")
    else:
        s = rec.get("inn1_total_score")
        w = rec.get("inn1_total_wickets")

    return {
        "checkpoint":        checkpoint,
        "prob_chase_wins":   prob_chase,
        "prob_defends":      prob_defend,
        "inn1_total_score":  rec.get("inn1_total_score"),
        "inn2_score_at_cp":  s,
        "inn2_wickets_at_cp": w,
        "bat_team":          rec.get("inn1_bat_team"),
        "bowl_team":         rec.get("inn1_bowl_team"),
    }


# ── Per-event prediction ──────────────────────────────────────────────────────

def _predict_for_event(gold, silver, event_id: str, accumulator: Dict) -> Optional[List[Dict]]:
    rows = accumulator.get("rows") or []
    inn1_complete = _is_inn1_complete(rows)
    inn2_over     = _inn2_current_over(rows)

    checkpoints = available_checkpoints(inn1_complete, inn2_over)
    if not checkpoints:
        return None  # inn1 not yet complete — nothing to predict

    rec = build_record(accumulator, silver, event_id)
    if rec is None:
        return None
    apply_composites(rec)

    results = []
    for cp in checkpoints:
        pred = _predict_checkpoint(gold, cp, rec)
        if pred is not None:
            results.append(pred)

    return results if results else None


# ── Public entry point ────────────────────────────────────────────────────────

def run_live_win_predictions(gold, silver, event_ids: List[str]) -> int:
    """
    Run win predictions for the given event IDs.
    Appends win_predictions to each event's live_predictions.json.
    If live_predictions.json doesn't exist yet, creates it.
    Returns the number of events written.
    """
    written = 0

    for eid in event_ids:
        try:
            # Load accumulator
            path = f"{_ACCUM_PREFIX}{eid}{_ACCUM_SUFFIX}"
            try:
                accum = json.loads(silver.get_blob_client(path).download_blob().readall())
            except Exception:
                continue

            win_preds = _predict_for_event(gold, silver, eid, accum)
            if not win_preds:
                continue

            # Load existing live_predictions.json (written by Phase 5 O/U predictor)
            # and merge win_predictions in.
            blob_path = _LIVE_PRED_KEY.format(eid=eid)
            try:
                existing = json.loads(gold.get_blob_client(blob_path).download_blob().readall())
            except Exception:
                existing = {
                    "event_id":         eid,
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "match_name":       accum.get("match_name", ""),
                    "source":           "live_ml",
                }

            existing["win_predictions"]        = win_preds
            existing["win_pred_generated_utc"] = datetime.now(timezone.utc).isoformat()

            gold.get_blob_client(blob_path).upload_blob(
                json.dumps(existing, indent=2).encode(), overwrite=True
            )
            written += 1
            logging.info(f"live_win: {eid} — {len(win_preds)} checkpoint(s) predicted")

        except Exception as e:
            logging.exception(f"live_win: unexpected error for {eid}: {e}")

    return written
