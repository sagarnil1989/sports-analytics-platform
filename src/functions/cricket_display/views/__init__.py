"""views package — each module is isolated so a syntax error in one breaks only that module."""
from .prematch import (
    view_prematch_matches,
    view_prematch_matches_html,
    view_prematch_match_json,
    view_save_prematch_results,
    view_prematch_match_html,
    view_prematch_leagues,
    view_prematch_leagues_html,
    view_prematch_league_matches,
    view_prematch_league_matches_html,
)
from .ended import (
    view_ended_matches,
    view_ended_matches_html,
)
from .innings_tracker import (
    view_silver_innings_tracker_html,
)
from .mgmt import (
    view_admin_leagues,
    view_admin_league_toggle,
    view_admin_rebuild_innings,
    view_admin_stadium_override_get,
    view_admin_stadium_override_save,
    view_admin_match_override_save,
    view_admin_adf_logs,
)
from .match_analysis import (
    view_detailed_analysis_html,
)
from .home import (
    view_home,
)
from .win_predictor import (
    view_ml_win_predictor_html,
    view_ml_win_predictor_config_post,
)
from .win_predictor_no_odds import view_ml_win_predictor_no_odds_html
from .model_insights import (
    view_model_insights_html,
    view_model_insights_no_odds_html,
    view_model_insights_image,
)
from .feature_matrix import (
    view_ml_feature_matrix_html,
    view_ml_feature_matrix_no_odds_html,
)
from .score_predictor import (
    view_ml_score_predictor_html,
)
from .score_matrix import (
    view_ml_score_matrix_html,
)
from .glossary import (
    view_glossary_html,
)
from .hypothesis import (
    view_hypothesis_inn2_over6,
    view_hypothesis_timeout_wicket,
    view_hypothesis_inn1_prematch,
)
from .over_under_view import (
    view_ml_over_under_html,
    view_ml_over_under_config_get,
    view_ml_over_under_config_post,
    view_ml_over_under_market_html,
)
from .retrain_summary import (
    view_ml_retrain_summary_html,
)
from .odds_movement import (
    view_odds_movement_html,
)
from .win_predictor_whatif import (
    view_win_predictor_whatif_html,
    view_win_predictor_whatif_post,
)
from .live_matches import (
    view_live_matches_html,
)
from .notification_settings import (
    view_notification_settings_get,
    view_notification_settings_post,
)

__all__ = [
    "view_prematch_matches", "view_prematch_matches_html", "view_prematch_match_json",
    "view_save_prematch_results", "view_prematch_match_html", "view_prematch_leagues",
    "view_prematch_leagues_html", "view_prematch_league_matches", "view_prematch_league_matches_html",
    "view_ended_matches", "view_ended_matches_html",
    "view_silver_innings_tracker_html",
    "view_admin_leagues", "view_admin_league_toggle", "view_admin_rebuild_innings",
    "view_admin_stadium_override_get", "view_admin_stadium_override_save",
    "view_admin_match_override_save", "view_admin_adf_logs",
    "view_detailed_analysis_html",
    "view_home",
    "view_model_insights_html",
    "view_model_insights_no_odds_html",
    "view_model_insights_image",
    "view_ml_win_predictor_html",
    "view_ml_win_predictor_config_post",
    "view_ml_win_predictor_no_odds_html",
    "view_ml_feature_matrix_html",
    "view_ml_feature_matrix_no_odds_html",
    "view_ml_score_predictor_html",
    "view_ml_score_matrix_html",
    "view_glossary_html",
    "view_hypothesis_inn2_over6",
    "view_hypothesis_timeout_wicket",
    "view_hypothesis_inn1_prematch",
    "view_ml_over_under_html",
    "view_ml_over_under_config_get",
    "view_ml_over_under_config_post",
    "view_ml_over_under_market_html",
    "view_ml_retrain_summary_html",
    "view_odds_movement_html",
    "view_win_predictor_whatif_html",
    "view_win_predictor_whatif_post",
    "view_live_matches_html",
    "view_notification_settings_get",
    "view_notification_settings_post",
]
