import azure.functions as func

from bronze import bronze_discover_cricket_inplay, bronze_capture_cricket_inplay_snapshot
from prematch import (
    bronze_discover_cricket_upcoming,
    bronze_capture_cricket_prematch_odds,
)

app = func.FunctionApp()


# ---------------------------------------------------------------------------
# Timer triggers — Bronze
# ---------------------------------------------------------------------------

@app.timer_trigger(schedule="*/5 * * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def discover_cricket_inplay(timer: func.TimerRequest) -> None:
    bronze_discover_cricket_inplay()


@app.timer_trigger(schedule="*/5 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_inplay_snapshot(timer: func.TimerRequest) -> None:
    bronze_capture_cricket_inplay_snapshot()


@app.timer_trigger(schedule="0 0 */1 * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def discover_cricket_upcoming(timer: func.TimerRequest) -> None:
    bronze_discover_cricket_upcoming()


@app.timer_trigger(schedule="0 10 */1 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_prematch_odds(timer: func.TimerRequest) -> None:
    bronze_capture_cricket_prematch_odds()


# discover_cricket_ended moved to ADF (pl_discover_cricket_ended) — no Function App timeout there.

# silver (parse_cricket_bronze_to_silver) and gold (build_cricket_gold_match_pages,
# build_cricket_prematch_pages, auto_rebuild_ended_innings_tracker) moved to ADF + Databricks.
