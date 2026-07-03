"""
cricket_live_ml — isolated live ML inference Function App.

Reads from silver/bronze (read-only) to apply trained win-predictor and
over/under models to currently live matches, writing predictions to
gold/event_id={id}/live_predictions.json.

See docs/live-ml-predictions.md for the full isolation architecture and
implementation plan. This file is intentionally empty for Phase 4 — the
timer trigger below just confirms the Function App is alive and the
isolation boundary is working. Prediction logic is added in Phases 5 and 6.

ISOLATION RULE: this function may ONLY write to gold/event_id=*/live_predictions.json.
It must never write to silver, bronze, or any existing gold paths used by
the ingestion or batch ML pipelines.
"""
import logging
import azure.functions as func

app = func.FunctionApp()


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def live_ml_tick(timer: func.TimerRequest) -> None:
    """Runs every 60 seconds. Phase 4: no-op heartbeat. Prediction logic added in Phases 5+6."""
    logging.info("live_ml_tick: alive — prediction logic not yet wired (Phase 4)")
