"""
cricket_live_ml — live ML inference Function App.

Architecture:
  - Reads live match state directly from BetsAPI every 60 seconds (no silver needed).
  - Builds per-over accumulators in gold (gold/cricket/inplay/live_accumulators/).
  - Runs Win Predictor at each available checkpoint (inn1-only, inn2-2over, …16over).
  - Writes per-event live_predictions.json and a live_index.json summary.
  - Sends email + SMS notifications for new matches and new checkpoint predictions.

ISOLATION RULE: this function may ONLY write to:
  gold/event_id=*/live_predictions.json
  gold/cricket/inplay/live_index.json
  gold/cricket/inplay/live_accumulators/event_id=*.json
  gold/cricket/inplay/notification_state.json
  gold/cricket/config/notification_prefs.json
It must never touch silver, bronze, or any shared gold ML/training paths.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from betsapi_live_parser import fetch_live_accumulators
from live_win_predictor import run_live_win_predictions_from_accumulators
from live_ou_predictor import run_live_ou_predictions_from_accumulators
from live_notifier import process_notifications

_LIVE_INDEX_PATH = "cricket/inplay/live_index.json"
_LIVE_PRED_KEY   = "event_id={eid}/live_predictions.json"

app = func.FunctionApp()


def _get_container(account_url: str, container_name: str):
    cred   = DefaultAzureCredential()
    client = BlobServiceClient(account_url=account_url, credential=cred)
    return client.get_container_client(container_name)


def _write_event_predictions(
    gold, eid: str, accum: Dict,
    win_preds: List[Dict],
    ou_result: Optional[Dict],
) -> None:
    blob_path = _LIVE_PRED_KEY.format(eid=eid)
    rows      = accum.get("rows") or []
    last_row  = rows[-1] if rows else {}

    payload = {
        "event_id":               eid,
        "match_name":             accum.get("match_name", ""),
        "home_team_name":         accum.get("home_team_name", ""),
        "away_team_name":         accum.get("away_team_name", ""),
        "league_name":            accum.get("league_name", ""),
        "current_innings":        last_row.get("innings"),
        "current_over":           last_row.get("over"),
        "current_score":          last_row.get("score"),
        "current_wickets":        last_row.get("wickets"),
        "batting_team":           last_row.get("batting_team"),
        "batting_team_odds":      last_row.get("batting_team_odds"),
        "bowling_team_odds":      last_row.get("bowling_team_odds"),
        "generated_at_utc":       datetime.now(timezone.utc).isoformat(),
        "source":                 "live_ml_betsapi",
        "win_predictions":        win_preds,
        "ou_predictions":         (ou_result or {}).get("ou_predictions") or [],
    }
    gold.get_blob_client(blob_path).upload_blob(
        json.dumps(payload, indent=2).encode(), overwrite=True
    )


_OU_MARKET_PRIORITY = [
    "innings_total", "inn2_first_12", "first_12_overs", "inn2_first_6", "first_6_overs",
]


def _pick_latest_ou(ou_preds: List[Dict]) -> Optional[Dict]:
    """Return the most informative O/U prediction: highest checkpoint for the priority market."""
    by_market: Dict[str, Dict] = {}
    for p in ou_preds:
        m = p.get("market", "")
        if m not in by_market or (p.get("checkpoint_over") or 0) > (by_market[m].get("checkpoint_over") or 0):
            by_market[m] = p
    return next((by_market[m] for m in _OU_MARKET_PRIORITY if m in by_market), None)


def _write_live_index(gold, event_ids: List[str]) -> None:
    """Build and write gold/cricket/inplay/live_index.json for the display function."""
    entries = []
    for eid in event_ids:
        try:
            raw  = gold.get_blob_client(_LIVE_PRED_KEY.format(eid=eid)).download_blob().readall()
            preds = json.loads(raw)
        except Exception:
            preds = {}

        # Pick the most informative win prediction checkpoint
        win_order = [
            "innings2-16over", "innings2-10over", "innings2-6over",
            "innings2-2over", "innings1-only",
        ]
        win_map    = {p["checkpoint"]: p for p in (preds.get("win_predictions") or [])}
        latest_win = next((win_map[cp] for cp in win_order if cp in win_map), None)
        latest_ou  = _pick_latest_ou(preds.get("ou_predictions") or [])

        entries.append({
            "event_id":       eid,
            "match_name":     preds.get("match_name", ""),
            "home_team_name": preds.get("home_team_name", ""),
            "away_team_name": preds.get("away_team_name", ""),
            "current_innings": preds.get("current_innings"),
            "current_over":    preds.get("current_over"),
            "current_score":   preds.get("current_score"),
            "current_wickets": preds.get("current_wickets"),
            "batting_team":    preds.get("batting_team"),
            "batting_team_odds":  preds.get("batting_team_odds"),
            "bowling_team_odds":  preds.get("bowling_team_odds"),
            "updated_utc":     preds.get("generated_at_utc", ""),
            "latest_win":      latest_win,
            "latest_ou":       latest_ou,
        })

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "active_count":     len(entries),
        "matches":          entries,
    }
    gold.get_blob_client(_LIVE_INDEX_PATH).upload_blob(
        json.dumps(payload, indent=2).encode(), overwrite=True
    )


@app.timer_trigger(
    schedule="0 */1 * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def live_ml_tick(timer: func.TimerRequest) -> None:
    """
    Runs every 60 seconds.
      1. Fetches live cricket events from BetsAPI; builds/updates per-match
         accumulators in gold.
      2. Runs Win Predictor at all available checkpoints.
      3. Writes per-event live_predictions.json.
      4. Sends email/SMS notifications for new matches and new predictions.
      5. Writes live_index.json for the cricket_display live view page.
    """
    endpoint = os.environ.get("DATA_LAKE_BLOB_ENDPOINT", "")
    if not endpoint:
        logging.error("live_ml_tick: DATA_LAKE_BLOB_ENDPOINT not set — skipping")
        return

    try:
        gold = _get_container(endpoint, "gold")

        # 1. Fetch live events → build/update accumulators
        accumulators = fetch_live_accumulators(gold)
        if not accumulators:
            logging.info("live_ml_tick: no live events")
            _write_live_index(gold, [])
            return

        logging.info(f"live_ml_tick: {len(accumulators)} live event(s)")

        # 2. Run win predictor and O/U predictor
        win_results = run_live_win_predictions_from_accumulators(gold, accumulators)
        ou_results  = run_live_ou_predictions_from_accumulators(gold, accumulators)
        logging.info(
            f"live_ml_tick: win={len(win_results)} event(s), ou={len(ou_results)} event(s)"
        )

        # 3. Write per-event live_predictions.json
        for eid, accum in accumulators.items():
            win_preds = win_results.get(eid) or []
            ou_result = ou_results.get(eid)
            try:
                _write_event_predictions(gold, eid, accum, win_preds, ou_result)
            except Exception as exc:
                logging.exception(f"live_ml_tick: failed writing predictions for {eid}: {exc}")

        # 4. Notifications
        try:
            process_notifications(gold, accumulators, win_results)
        except Exception as exc:
            logging.exception(f"live_ml_tick: notification error: {exc}")

        # 5. Write live_index.json
        _write_live_index(gold, list(accumulators.keys()))
        logging.info("live_ml_tick: live_index.json updated")

    except Exception as exc:
        logging.exception(f"live_ml_tick: unexpected error: {exc}")
