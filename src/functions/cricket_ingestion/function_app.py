import azure.functions as func

from capture_inplay import bronze_discover_cricket_inplay, bronze_capture_cricket_inplay_snapshot
from capture_prematch import bronze_discover_cricket_upcoming, bronze_capture_cricket_prematch_odds
from capture_ended import bronze_capture_ended_event_view

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


@app.timer_trigger(schedule="0 0 6 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def discover_cricket_upcoming(timer: func.TimerRequest) -> None:
    """Runs at 06:00 UTC daily. Writes upcoming match control file to bronze."""
    bronze_discover_cricket_upcoming()


@app.timer_trigger(schedule="0 10 6 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_prematch_odds(timer: func.TimerRequest) -> None:
    """Runs at 06:10 UTC daily — after upcoming discover. Writes prematch odds snapshots to bronze.
    ADF pipeline pl_build_prematch_pages then reads bronze and writes gold/cricket/prematch/."""
    bronze_capture_cricket_prematch_odds()


@app.timer_trigger(schedule="0 30 2 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_ended_event_view(timer: func.TimerRequest) -> None:
    """Runs at 02:30 UTC daily. Writes bronze/betsapi/event_final/event_id={id}/event_view.json
    for every ended match that does not yet have a master final-score file."""
    bronze_capture_ended_event_view()
