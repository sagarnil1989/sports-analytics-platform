import azure.functions as func

from capture_inplay import bronze_discover_cricket_inplay, bronze_capture_cricket_inplay_snapshot
from capture_prematch import bronze_discover_cricket_upcoming, bronze_capture_cricket_prematch_odds
from capture_ended import bronze_capture_ended_event_view, bronze_repair_event_finals
from prematch_page_builder import gold_build_prematch_pages

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


@app.timer_trigger(schedule="0 */30 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def discover_cricket_upcoming(timer: func.TimerRequest) -> None:
    """Runs every 30 min (:00 and :30). Writes upcoming match control file to bronze."""
    bronze_discover_cricket_upcoming()


@app.timer_trigger(schedule="0 10,40 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_prematch_odds(timer: func.TimerRequest) -> None:
    """Runs every 30 min (:10 and :40) — 10 min after each discover. Writes prematch odds snapshots to bronze."""
    bronze_capture_cricket_prematch_odds()


@app.timer_trigger(schedule="0 20,50 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def build_prematch_pages(timer: func.TimerRequest) -> None:
    """Runs every 30 min (:20 and :50) — 10 min after capture. Builds gold/cricket/prematch/ index."""
    gold_build_prematch_pages()


@app.timer_trigger(schedule="0 30 2 * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_ended_event_view(timer: func.TimerRequest) -> None:
    """Runs at 02:30 UTC daily. Writes/repairs bronze event_final blobs.
    Now re-fetches any blob that has empty ss or time_status != 3."""
    bronze_capture_ended_event_view()


@app.route(route="admin/repair-event-finals", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def repair_event_finals(req: func.HttpRequest) -> func.HttpResponse:
    """Admin: scan ALL event_final blobs, re-fetch any with wrong/missing scores,
    then rebuild the ended index. Safe to call multiple times — skips valid blobs.
    Takes ~30-60 s per 100 bad matches due to BetsAPI rate-limit sleep."""
    import json as _json
    try:
        result = bronze_repair_event_finals()
        return func.HttpResponse(
            _json.dumps(result, indent=2),
            mimetype="application/json",
        )
    except Exception as e:
        import traceback
        return func.HttpResponse(
            _json.dumps({"error": str(e), "trace": traceback.format_exc()}),
            mimetype="application/json",
            status_code=500,
        )
