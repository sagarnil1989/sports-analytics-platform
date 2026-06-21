from .common import (
    func,
)


def view_home(req: func.HttpRequest) -> func.HttpResponse:
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Cricket Data Platform</title>
        <style>
            body { font-family: Arial; background:#f7f7f7; padding:40px; }
            h1 { margin-bottom:10px; }
            .card { background:white; padding:20px; margin:15px 0; border-radius:10px; box-shadow:0 2px 8px #ddd; }
            a { font-size:18px; font-weight:bold; color:#0066cc; text-decoration:none; }
            p { margin:8px 0 0; color:#555; }
        </style>
    </head>
    <body>
        <h1>🏏 Cricket Analytics Platform</h1>
        <p>Browse upcoming and historical betting data</p>
        <div class="card"><a href="/api/prematch/view">Upcoming Matches</a><p>Prematch odds and markets before the game starts</p></div>
        <div class="card"><a href="/api/ended/view">Ended Matches</a><p>Recently finished matches with final results</p></div>
        <h2 style="margin:24px 0 4px; color:#333;">T20</h2>
        <h3 style="margin:16px 0 4px; color:#555; font-size:15px; text-transform:uppercase; letter-spacing:1px;">Model</h3>
        <div class="card"><a href="/api/ml/win-predictor">ML Win Predictor</a><p>Model performance, feature importances and algorithm comparison across all three prediction windows</p></div>
        <div class="card"><a href="/api/ml/over-under">ML Over/Under Predictor</a><p>LightGBM models predicting innings total and first-12-overs Over/Under at checkpoint overs during inn1 — per-checkpoint and pooled models with CV-AUC and held-out test evaluation</p></div>
        <div class="card"><a href="/api/ml/feature-matrix">ML Feature Matrix</a><p>All matches × all features in one table — train/test split highlighted, selected features marked per model</p></div>
        <div class="card"><a href="/api/ml/score-predictor">Inn1 Score Predictor</a><p>Predicts final innings-1 score at over 6, 10 and 16 — MAE, RMSE, R² and per-match test predictions</p></div>
        <div class="card"><a href="/api/ml/score-matrix">Score Feature Matrix</a><p>All matches × score-predictor features — three cutoff tabs (over 6 / 10 / 16), train/test highlighted</p></div>
        <div class="card"><a href="/api/mgmt/leagues/view">League Filter</a><p>Select which leagues to capture — excluded leagues skip bronze, silver and gold entirely</p></div>
        <div class="card"><a href="/api/mgmt/stadium-override">Stadium Overrides</a><p>Manually set a stadium name for a match when venue data is missing</p></div>
        <h3 style="margin:20px 0 4px; color:#555; font-size:15px; text-transform:uppercase; letter-spacing:1px;">Hypothesis</h3>
        <div class="card"><a href="/api/hypothesis/inn2-over6">Inn2 Over-6 Favourite Wins</a><p>Does the match-winner odds favourite after 6 overs of the chase always win? T20 only — with score, odds and actual result.</p></div>
        <div class="card"><a href="/api/hypothesis/timeout-wicket">Wicket After Strategic Timeout</a><p>After a strategic timeout (game paused &gt;2 min), does a wicket always fall in the very next over? T20 only — timeout detected from gaps in game state.</p></div>
        <h2 style="margin:24px 0 4px; color:#333;">Reference</h2>
        <div class="card"><a href="/api/ml/glossary">Glossary</a><p>Plain-English explanations of all ML metrics (MAE, RMSE, R², Accuracy, ROC-AUC), algorithms (XGBoost, Random Forest), and cricket analytics terms (CRR, RRR, bat dominance, chase difficulty, odds)</p></div>
    </body>
    </html>
    """
    return func.HttpResponse(html, mimetype="text/html")
