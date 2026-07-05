"""
cricket_live_ml — isolated live ML inference Function App.

Reads from silver (read-only) to apply trained Over/Under and Win Predictor
models to currently live matches.  Per-event results go to:
  gold/event_id={id}/live_predictions.json

A live index summary (read by the cricket_display live view page) goes to:
  gold/cricket/inplay/live_index.json

ISOLATION RULE: this function may ONLY write to:
  gold/event_id=*/live_predictions.json
  gold/cricket/inplay/live_index.json
It must never touch silver, bronze, or any shared gold ML/training paths.

Phase 5: Over/Under live predictions (live_ou_predictor.py).
Phase 6: Win predictor live predictions (live_win_predictor.py).
"""
import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from live_ou_predictor import run_live_ou_predictions, _list_live_event_ids
from live_win_predictor import run_live_win_predictions

_LIVE_INDEX_PATH = "cricket/inplay/live_index.json"

app = func.FunctionApp()


def _get_container(account_url: str, container_name: str):
    cred = DefaultAzureCredential()
    client = BlobServiceClient(account_url=account_url, credential=cred)
    return client.get_container_client(container_name)


def _write_live_index(gold, event_ids: list) -> None:
    """
    Build and write gold/cricket/inplay/live_index.json — a lightweight summary
    of every currently-live event's latest predictions.  The cricket_display
    Function App reads this single blob to render the /api/live/view page.
    """
    entries = []
    for eid in event_ids:
        try:
            raw = gold.get_blob_client(
                f"event_id={eid}/live_predictions.json"
            ).download_blob().readall()
            preds = json.loads(raw)
        except Exception:
            preds = {}

        # Summarise O/U: pick the highest-checkpoint innings_total prediction
        latest_ou = None
        for p in sorted(
            (p for p in (preds.get("ou_predictions") or []) if p.get("market") == "innings_total"),
            key=lambda x: x.get("checkpoint_over", 0),
        ):
            latest_ou = p  # last = highest checkpoint

        # Summarise win predictor: pick the most informative checkpoint
        latest_win = None
        win_order = ["innings2-16over", "innings2-10over", "innings2-6over",
                     "innings2-2over", "innings1-only"]
        win_map   = {p["checkpoint"]: p for p in (preds.get("win_predictions") or [])}
        for cp in win_order:
            if cp in win_map:
                latest_win = win_map[cp]
                break

        entries.append({
            "event_id":       eid,
            "match_name":     preds.get("match_name", ""),
            "current_innings": preds.get("current_innings"),
            "current_over":    preds.get("current_over"),
            "updated_utc":     preds.get("generated_at_utc", ""),
            "latest_ou":       latest_ou,
            "latest_win":      latest_win,
        })

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "active_count":     len(entries),
        "matches":          entries,
    }
    gold.get_blob_client(_LIVE_INDEX_PATH).upload_blob(
        json.dumps(payload, indent=2).encode(), overwrite=True
    )


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def live_ml_tick(timer: func.TimerRequest) -> None:
    """
    Runs every 60 seconds.
    Phase 5: O/U predictions for currently live matches.
    Phase 6: Win predictor predictions for currently live matches.
    Also writes live_index.json so cricket_display can list live matches.
    """
    endpoint = os.environ.get("DATA_LAKE_BLOB_ENDPOINT", "")
    if not endpoint:
        logging.error("live_ml_tick: DATA_LAKE_BLOB_ENDPOINT not set — skipping")
        return

    try:
        gold   = _get_container(endpoint, "gold")
        silver = _get_container(endpoint, "silver")

        event_ids = _list_live_event_ids(silver)
        if not event_ids:
            logging.info("live_ml_tick: no live events")
            return

        logging.info(f"live_ml_tick: {len(event_ids)} live event(s)")

        ou_written  = run_live_ou_predictions(gold, silver)
        logging.info(f"live_ml_tick: phase5 O/U wrote {ou_written} file(s)")

        win_written = run_live_win_predictions(gold, silver, event_ids)
        logging.info(f"live_ml_tick: phase6 win wrote {win_written} file(s)")

        _write_live_index(gold, event_ids)
        logging.info("live_ml_tick: live_index.json updated")

    except Exception as e:
        logging.exception(f"live_ml_tick: unexpected error: {e}")
