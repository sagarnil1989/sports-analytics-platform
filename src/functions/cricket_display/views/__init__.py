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
from .feature_matrix import (
    view_ml_feature_matrix_html,
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
)

__all__ = [
    "view_prematch_matches", "view_prematch_matches_html", "view_prematch_match_json",
    "view_save_prematch_results", "view_prematch_match_html", "view_prematch_leagues",
    "view_prematch_leagues_html", "view_prematch_league_matches", "view_prematch_league_matches_html",
    "view_ended_matches", "view_ended_matches_html",
    "view_silver_innings_tracker_html",
    "view_admin_leagues", "view_admin_league_toggle", "view_admin_rebuild_innings",
    "view_detailed_analysis_html",
    "view_home",
    "view_ml_win_predictor_html",
    "view_ml_win_predictor_config_post",
    "view_ml_feature_matrix_html",
    "view_ml_score_predictor_html",
    "view_ml_score_matrix_html",
    "view_glossary_html",
    "view_hypothesis_inn2_over6",
    "view_hypothesis_timeout_wicket",
]
