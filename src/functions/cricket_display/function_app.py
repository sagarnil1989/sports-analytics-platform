import azure.functions as func

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
    view_silver_innings_tracker_html,
    view_admin_leagues,
    view_admin_league_toggle,
    view_admin_rebuild_innings,
    view_admin_stadium_override_get,
    view_admin_stadium_override_save,
    view_admin_match_override_save,
    view_admin_adf_logs,
    view_detailed_analysis_html,
    view_home,
    view_model_insights_html,
    view_model_insights_no_odds_html,
    view_model_insights_image,
    view_ml_win_predictor_html,
    view_ml_win_predictor_config_post,
    view_ml_win_predictor_no_odds_html,
    view_ml_feature_matrix_html,
    view_ml_feature_matrix_no_odds_html,
    view_ml_score_predictor_html,
    view_ml_score_matrix_html,
    view_glossary_html,
    view_hypothesis_inn2_over6,
    view_hypothesis_timeout_wicket,
    view_hypothesis_inn1_prematch,
    view_ml_over_under_html,
    view_ml_over_under_config_get,
    view_ml_over_under_config_post,
    view_ml_over_under_market_html,
    view_ml_retrain_summary_html,
    view_odds_movement_html,
    view_win_predictor_whatif_html,
    view_win_predictor_whatif_post,
    view_live_matches_html,
    view_notification_settings_get,
    view_notification_settings_post,
)

app = func.FunctionApp()


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
# HTTP routes — Innings tracker (per match, reads gold)
# ---------------------------------------------------------------------------

@app.route(route="matches/{event_id}/innings-tracker/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_innings_tracker_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_silver_innings_tracker_html(req)


# ---------------------------------------------------------------------------
# HTTP routes — Per-match analysis
# ---------------------------------------------------------------------------

@app.route(route="matches/{event_id}/detailed-analysis", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_detailed_analysis_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_detailed_analysis_html(req)


# ---------------------------------------------------------------------------
# HTTP routes — Admin
# ---------------------------------------------------------------------------

@app.route(route="mgmt/leagues/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_admin_leagues_view(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_leagues(req)


@app.route(route="mgmt/leagues/toggle", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_admin_league_toggle(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_league_toggle(req)


@app.route(route="mgmt/rebuild-innings/{event_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_admin_rebuild_innings(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_rebuild_innings(req)


@app.route(route="mgmt/stadium-override", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_admin_stadium_override(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_stadium_override_get(req)


@app.route(route="mgmt/stadium-override", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_admin_stadium_override(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_stadium_override_save(req)


@app.route(route="mgmt/match-override", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_admin_match_override(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_match_override_save(req)


@app.route(route="mgmt/adf-logs/{activity_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_admin_adf_logs(req: func.HttpRequest) -> func.HttpResponse:
    return view_admin_adf_logs(req)


# ---------------------------------------------------------------------------
# HTTP routes — Home / root
# ---------------------------------------------------------------------------

@app.route(route="home", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_home(req: func.HttpRequest) -> func.HttpResponse:
    return view_home(req)


@app.route(route="", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def root(req: func.HttpRequest) -> func.HttpResponse:
    return view_home(req)


@app.route(route="live/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_live_matches(req: func.HttpRequest) -> func.HttpResponse:
    return view_live_matches_html(req)


@app.route(route="ml/win-predictor", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_win_predictor_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_win_predictor_html(req)


@app.route(route="ml/win-predictor/config", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_ml_win_predictor_config(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_win_predictor_config_post(req)


@app.route(route="ml/win-predictor/whatif", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_win_predictor_whatif(req: func.HttpRequest) -> func.HttpResponse:
    return view_win_predictor_whatif_html(req)


@app.route(route="ml/win-predictor/whatif", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_ml_win_predictor_whatif(req: func.HttpRequest) -> func.HttpResponse:
    return view_win_predictor_whatif_post(req)


@app.route(route="ml/win-predictor-no-odds", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_win_predictor_no_odds_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_win_predictor_no_odds_html(req)


@app.route(route="ml/model-insights", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_model_insights_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_model_insights_html(req)


@app.route(route="ml/model-insights-no-odds", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_model_insights_no_odds_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_model_insights_no_odds_html(req)


@app.route(route="ml/model-insights/image", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_model_insights_image(req: func.HttpRequest) -> func.HttpResponse:
    return view_model_insights_image(req)


@app.route(route="ml/feature-matrix", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_feature_matrix_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_feature_matrix_html(req)


@app.route(route="ml/feature-matrix-no-odds", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_feature_matrix_no_odds_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_feature_matrix_no_odds_html(req)


@app.route(route="ml/score-predictor", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_score_predictor_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_score_predictor_html(req)


@app.route(route="ml/score-matrix", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_score_matrix_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_score_matrix_html(req)


@app.route(route="ml/retrain-summary", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_retrain_summary_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_retrain_summary_html(req)


@app.route(route="analysis/odds-movement", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_odds_movement_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_odds_movement_html(req)


@app.route(route="ml/glossary", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_glossary_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_glossary_html(req)


# ---------------------------------------------------------------------------
# HTTP routes — Hypothesis
# ---------------------------------------------------------------------------

@app.route(route="ml/over-under", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_over_under_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_over_under_html(req)


@app.route(route="ml/over-under/config", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_over_under_config(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_over_under_config_get(req)


@app.route(route="ml/over-under/config", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_ml_over_under_config(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_over_under_config_post(req)


@app.route(route="ml/over-under/{slug}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ml_over_under_market_html(req: func.HttpRequest) -> func.HttpResponse:
    return view_ml_over_under_market_html(req)


@app.route(route="hypothesis/inn2-over6", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_hypothesis_inn2_over6(req: func.HttpRequest) -> func.HttpResponse:
    return view_hypothesis_inn2_over6(req)


@app.route(route="hypothesis/timeout-wicket", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_hypothesis_timeout_wicket(req: func.HttpRequest) -> func.HttpResponse:
    return view_hypothesis_timeout_wicket(req)


@app.route(route="hypothesis/inn1-prematch", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_hypothesis_inn1_prematch(req: func.HttpRequest) -> func.HttpResponse:
    return view_hypothesis_inn1_prematch(req)


# ---------------------------------------------------------------------------
# HTTP routes — Notifications
# ---------------------------------------------------------------------------

@app.route(route="notification/settings", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_notification_settings(req: func.HttpRequest) -> func.HttpResponse:
    return view_notification_settings_get(req)


@app.route(route="notification/settings", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_notification_settings(req: func.HttpRequest) -> func.HttpResponse:
    return view_notification_settings_post(req)
