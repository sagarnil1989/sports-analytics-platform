import azure.functions as func

from bronze import bronze_discover_cricket_inplay, bronze_capture_cricket_inplay_snapshot
from silver import silver_parse_bronze_to_silver
from gold import gold_build_match_pages
from prematch import (
    bronze_discover_cricket_upcoming,
    bronze_capture_cricket_prematch_odds,
    bronze_discover_cricket_ended,
    gold_build_cricket_prematch_pages,
)
from views import (
    view_prematch_matches,
    view_prematch_matches_html,
    view_prematch_match_json,
    view_save_prematch_results,
    view_prematch_match_html,
    view_prematch_leagues,
    view_prematch_leagues_html,
    view_prematch_league_matches,
    view_prematch_league_matches_html,
    view_ended_matches,
    view_ended_matches_html,
    view_latest_matches,
    view_match_page,
    view_matches_list_html,
    view_leagues,
    view_leagues_html,
    view_league_matches,
    view_league_matches_html,
    view_match_page_html,
    view_match_lineage_json,
    view_match_lineage_html,
    view_innings_tracker_html,
    view_silver_innings_tracker_html,
    view_innings_tracker_analytics,
    view_market_heatmap_html,
    view_match_live_markets,
    view_match_markets_raw,
    view_admin_rebuild_innings,
    view_admin_leagues,
    view_admin_league_toggle,
    view_home,
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


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def discover_cricket_upcoming(timer: func.TimerRequest) -> None:
    bronze_discover_cricket_upcoming()


@app.timer_trigger(schedule="10 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_prematch_odds(timer: func.TimerRequest) -> None:
    bronze_capture_cricket_prematch_odds()


@app.timer_trigger(schedule="30 */5 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def discover_cricket_ended(timer: func.TimerRequest) -> None:
    bronze_discover_cricket_ended()


# ---------------------------------------------------------------------------
# Timer triggers — Silver
# ---------------------------------------------------------------------------

@app.timer_trigger(schedule="*/10 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def parse_cricket_bronze_to_silver(timer: func.TimerRequest) -> None:
    silver_parse_bronze_to_silver()


# ---------------------------------------------------------------------------
# Timer triggers — Gold
# ---------------------------------------------------------------------------

@app.timer_trigger(schedule="*/10 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def build_cricket_gold_match_pages(timer: func.TimerRequest) -> None:
    gold_build_match_pages()


@app.timer_trigger(schedule="30 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def build_cricket_prematch_pages(timer: func.TimerRequest) -> None:
    gold_build_cricket_prematch_pages()


# ---------------------------------------------------------------------------
# HTTP routes — Prematch
# ---------------------------------------------------------------------------

@app.route(route="prematch", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_matches(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_matches(req)


@app.route(route="prematch/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_matches_html(req)


@app.route(route="prematch/{event_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_match_json(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_match_json(req)


@app.route(route="prematch/{event_id}/results", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def save_prematch_results(req: func.HttpRequest) -> func.HttpResponse:
    return view_save_prematch_results(req)


@app.route(route="prematch/{event_id}/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_match_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_match_html(req)


@app.route(route="prematch/leagues", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_leagues(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_leagues(req)


@app.route(route="prematch/leagues/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_leagues_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_leagues_html(req)


@app.route(route="prematch/leagues/{league_id}/matches", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_league_matches(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_league_matches(req)


@app.route(route="prematch/leagues/{league_id}/matches/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_league_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_prematch_league_matches_html(req)


# ---------------------------------------------------------------------------
# HTTP routes — Ended matches
# ---------------------------------------------------------------------------

@app.route(route="ended", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ended_matches(req: func.HttpRequest) -> func.HttpResponse:
    return view_ended_matches(req)


@app.route(route="ended/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ended_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ended_matches_html(req)


# ---------------------------------------------------------------------------
# HTTP routes — Live matches
# ---------------------------------------------------------------------------

@app.route(route="matches", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_latest_matches(req: func.HttpRequest) -> func.HttpResponse:
    return view_latest_matches(req)


@app.route(route="matches/{event_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_page(req: func.HttpRequest) -> func.HttpResponse:
    return view_match_page(req)


@app.route(route="matches/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_matches_list_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_matches_list_html(req)


@app.route(route="matches/{event_id}/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_page_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_match_page_html(req)


@app.route(route="matches/{event_id}/lineage", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_lineage_json(req: func.HttpRequest) -> func.HttpResponse:
    return view_match_lineage_json(req)


@app.route(route="matches/{event_id}/lineage/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_lineage_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_match_lineage_html(req)


@app.route(route="matches/{event_id}/innings-tracker/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_innings_tracker_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_silver_innings_tracker_html(req)


@app.route(route="matches/{event_id}/heatmap", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_market_heatmap_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_market_heatmap_html(req)


@app.route(route="matches/{event_id}/markets/live", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_live_markets(req: func.HttpRequest) -> func.HttpResponse:
    return view_match_live_markets(req)


@app.route(route="matches/{event_id}/markets/raw", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_markets_raw(req: func.HttpRequest) -> func.HttpResponse:
    return view_match_markets_raw(req)


# ---------------------------------------------------------------------------
# HTTP routes — Leagues
# ---------------------------------------------------------------------------

@app.route(route="leagues", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_leagues(req: func.HttpRequest) -> func.HttpResponse:
    return view_leagues(req)


@app.route(route="leagues/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_leagues_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_leagues_html(req)


@app.route(route="leagues/{league_id}/matches", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_league_matches(req: func.HttpRequest) -> func.HttpResponse:
    return view_league_matches(req)


@app.route(route="leagues/{league_id}/matches/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_league_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_league_matches_html(req)


# ---------------------------------------------------------------------------
# HTTP routes — Innings tracker
# ---------------------------------------------------------------------------

@app.route(route="innings-tracker", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_innings_tracker_analytics(req: func.HttpRequest) -> func.HttpResponse:
    return view_innings_tracker_analytics(req)


# ---------------------------------------------------------------------------
# HTTP routes — Admin
# ---------------------------------------------------------------------------

@app.route(route="run-prematch-now", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def run_prematch_now(req: func.HttpRequest) -> func.HttpResponse:
    gold_build_cricket_prematch_pages()
    return func.HttpResponse("Prematch build triggered.", status_code=200)


@app.route(route="mgmt/innings/{event_id}/rebuild", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def admin_rebuild_innings_accumulator(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_rebuild_innings(req)


@app.route(route="mgmt/leagues/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_admin_leagues_view(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_leagues(req)


@app.route(route="mgmt/leagues/toggle", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_admin_league_toggle(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_league_toggle(req)


# ---------------------------------------------------------------------------
# HTTP routes — Home / root
# ---------------------------------------------------------------------------

@app.route(route="home", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_home(req: func.HttpRequest) -> func.HttpResponse:
    return view_home(req)


@app.route(route="", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def root(req: func.HttpRequest) -> func.HttpResponse:
    return view_home(req)
