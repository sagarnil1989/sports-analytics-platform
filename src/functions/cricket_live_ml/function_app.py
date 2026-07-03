"""
cricket_live_ml — isolated live ML inference Function App.

Reads from silver (read-only) to apply trained Over/Under models to currently
live matches, writing predictions to gold/event_id={id}/live_predictions.json.

ISOLATION RULE: this function may ONLY write to gold/event_id=*/live_predictions.json.
It must never write to silver, bronze, or any existing gold paths used by
the ingestion or batch ML pipelines.

Phase 5: Over/Under live predictions (live_ou_predictor.py).
Phase 6: Win predictor live predictions (future).
"""
import logging
import os

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from live_ou_predictor import run_live_ou_predictions

app = func.FunctionApp()


def _get_container(account_url: str, container_name: str):
    cred = DefaultAzureCredential()
    client = BlobServiceClient(account_url=account_url, credential=cred)
    return client.get_container_client(container_name)


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def live_ml_tick(timer: func.TimerRequest) -> None:
    """
    Runs every 60 seconds.
    Phase 5: O/U predictions for currently live matches.
    Phase 6 (future): Win predictor live predictions.
    """
    endpoint = os.environ.get("DATA_LAKE_BLOB_ENDPOINT", "")
    if not endpoint:
        logging.error("live_ml_tick: DATA_LAKE_BLOB_ENDPOINT not set — skipping")
        return

    try:
        gold   = _get_container(endpoint, "gold")
        silver = _get_container(endpoint, "silver")
        written = run_live_ou_predictions(gold, silver)
        logging.info(f"live_ml_tick: phase5 O/U wrote {written} prediction file(s)")
    except Exception as e:
        logging.exception(f"live_ml_tick: unexpected error: {e}")
