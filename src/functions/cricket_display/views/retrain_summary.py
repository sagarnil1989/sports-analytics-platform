"""ML retrain summary page — results of RunMLFeatureExtraction + RunMLModelTraining
(cricket-score-predictor + cricket-ou-classifier, trained with GroupKFold CV on the
train split, evaluated held-out on the test split)."""
from .common import json, logging, escape, func, get_named_container_client, adf_activity_badge


def view_ml_retrain_summary_html(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")

    try:
        raw     = gold.get_blob_client("cricket/ml_features/t20/model_accuracy.json").download_blob().readall()
        summary = json.loads(raw)
    except Exception:
        summary = None

    def _mae_color(v):
        if v is None: return "#999"
        if v <= 10:   return "#2d7a2d"
        if v <= 18:   return "#cc7700"
        return "#cc2200"

    def _r2_color(v):
        if v is None: return "#999"
        if v >= 0.6:  return "#2d7a2d"
        if v >= 0.3:  return "#cc7700"
        return "#cc2200"

    def _acc_color(v):
        if v is None: return "#999"
        if v >= 0.70: return "#2d7a2d"
        if v >= 0.60: return "#cc7700"
        return "#cc2200"

    if summary is None:
        body = "<p style='color:#c00'>No results found. Run <b>pl_ml_and_hypothesis</b> in ADF first.</p>"
    else:
        trained_at  = summary.get("trained_at_utc", "unknown")
        cutoff      = summary.get("train_cutoff", "?")
        train_n     = summary.get("training_matches", "?")
        train_rows  = summary.get("training_rows", "?")
        test_rows   = summary.get("test_rows", "?")
        sp          = summary.get("score_predictor", {})
        ou          = summary.get("ou_classifier", {})

        body = f"""
        <div class="meta-grid">
            <div class="meta-box"><div class="meta-label">Last trained</div><div class="meta-val">{escape(str(trained_at))}</div></div>
            <div class="meta-box"><div class="meta-label">Training matches</div><div class="meta-val">{train_n}</div></div>
            <div class="meta-box"><div class="meta-label">Training rows</div><div class="meta-val">{train_rows}</div></div>
            <div class="meta-box"><div class="meta-label">Test rows</div><div class="meta-val">{test_rows}</div></div>
            <div class="meta-box"><div class="meta-label">Train / test cutoff</div><div class="meta-val">{escape(str(cutoff))}</div></div>
        </div>
        <div class="algo-note">
            Fit on the <strong>train</strong> split only (rows with match_date_utc before the cutoff above).
            Group K-Fold CV (grouped by match) reported alongside a genuine held-out evaluation on the
            <strong>test</strong> split — rows the model never saw during fitting or CV.
        </div>

        <div class="metric-legend">
            <div class="metric-legend-title">What do the metrics mean?</div>
            <div class="metric-row">
                <span class="metric-name">MAE</span>
                <span class="metric-desc">Mean Absolute Error — average runs the score predictor was off by. <strong>Lower is better.</strong></span>
            </div>
            <div class="metric-row">
                <span class="metric-name">R²</span>
                <span class="metric-desc">How much of the variation in final scores the model explains. 1.0 = perfect, 0.0 = no better than guessing the average. <strong>Higher is better.</strong></span>
            </div>
            <div class="metric-row">
                <span class="metric-name">Accuracy / ROC-AUC</span>
                <span class="metric-desc">For the Over/Under classifier — accuracy is % of correct over/under calls; ROC-AUC measures how well it ranks over-outcomes above under-outcomes (0.5 = random, 1.0 = perfect). <strong>Higher is better.</strong></span>
            </div>
            <div class="metric-row">
                <span class="metric-name">CV vs Test</span>
                <span class="metric-desc">CV = cross-validated score on the train split (model never saw each fold's held-out matches during that fold's fit). Test = the held-out split after the cutoff date — the most honest measure of real-world performance.</span>
            </div>
        </div>

        <h2>Score Predictor</h2>
        <p style="color:#555;margin-bottom:12px">{escape(sp.get("description", ""))} &nbsp;|&nbsp; <strong>Algorithm:</strong> {escape(sp.get("algorithm", ""))}</p>
        <table class="cmp-table">
            <thead><tr><th></th><th>MAE (runs)</th><th>R²</th></tr></thead>
            <tbody>
                <tr>
                    <td><strong>Cross-validation (train)</strong></td>
                    <td style="color:{_mae_color(sp.get('cv_mae_runs'))};font-weight:bold">{sp.get('cv_mae_runs', '—')}</td>
                    <td style="color:{_r2_color(sp.get('cv_r2'))}">{sp.get('cv_r2', '—')}</td>
                </tr>
                <tr>
                    <td><strong>Held-out test</strong></td>
                    <td style="color:{_mae_color(sp.get('test_mae_runs'))};font-weight:bold">{sp.get('test_mae_runs', '—')}</td>
                    <td style="color:{_r2_color(sp.get('test_r2'))}">{sp.get('test_r2', '—')}</td>
                </tr>
            </tbody>
        </table>
        <p class="hint">MLflow run: <code>{escape(str(sp.get('mlflow_run_id', '')))}</code></p>
        <p class="hint">Top features: {escape(', '.join(sp.get('top_features', [])) or '—')}</p>

        <h2>Over/Under Classifier</h2>
        <p style="color:#555;margin-bottom:12px">{escape(ou.get("description", ""))} &nbsp;|&nbsp; <strong>Algorithm:</strong> {escape(ou.get("algorithm", ""))}</p>
        <table class="cmp-table">
            <thead><tr><th></th><th>Accuracy</th><th>ROC-AUC</th></tr></thead>
            <tbody>
                <tr>
                    <td><strong>Cross-validation (train)</strong></td>
                    <td style="color:{_acc_color(ou.get('cv_accuracy'))};font-weight:bold">{ou.get('cv_accuracy', '—')}</td>
                    <td>{ou.get('cv_roc_auc', '—')}</td>
                </tr>
                <tr>
                    <td><strong>Held-out test</strong></td>
                    <td style="color:{_acc_color(ou.get('test_accuracy'))};font-weight:bold">{ou.get('test_accuracy', '—')}</td>
                    <td>{ou.get('test_roc_auc', '—')}</td>
                </tr>
            </tbody>
        </table>
        <p class="hint">MLflow run: <code>{escape(str(ou.get('mlflow_run_id', '')))}</code></p>
        <p class="hint">Top features: {escape(', '.join(ou.get('top_features', [])) or '—')}</p>
        """

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>ML Retrain Summary</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; background:#f5f5f5; margin:0; padding:30px; }}
        h1 {{ margin-bottom:6px; }}
        h2 {{ margin-top:36px; margin-bottom:12px; border-bottom:2px solid #ddd; padding-bottom:6px; }}
        .meta-grid {{ display:flex; gap:16px; flex-wrap:wrap; margin:20px 0; }}
        .meta-box {{ background:white; padding:16px 20px; border-radius:8px;
                     box-shadow:0 1px 4px #ccc; min-width:160px; }}
        .meta-label {{ font-size:12px; color:#888; margin-bottom:4px; }}
        .meta-val {{ font-size:16px; font-weight:bold; color:#222; }}
        .algo-note {{ background:#fff8e1; border-left:4px solid #f0a000;
                      padding:12px 16px; border-radius:4px; margin-bottom:16px;
                      font-size:14px; color:#555; }}
        .metric-legend {{ background:white; border:1px solid #e0e0e0; border-radius:8px;
                          padding:16px 20px; margin-bottom:24px; box-shadow:0 1px 4px #ccc; }}
        .metric-legend-title {{ font-weight:bold; font-size:14px; color:#333;
                                 margin-bottom:12px; border-bottom:1px solid #eee; padding-bottom:6px; }}
        .metric-row {{ display:flex; gap:12px; margin-bottom:10px; font-size:13px; }}
        .metric-row:last-child {{ margin-bottom:0; }}
        .metric-name {{ font-family:monospace; font-weight:bold; color:#0055aa;
                        min-width:140px; padding-top:1px; }}
        .metric-desc {{ color:#444; line-height:1.5; }}
        .cmp-table {{ border-collapse:collapse; width:100%; background:white;
                      border-radius:8px; overflow:hidden; box-shadow:0 1px 4px #ccc;
                      margin-bottom:12px; }}
        .cmp-table th {{ background:#333; color:white; padding:10px 14px;
                         text-align:left; font-size:13px; }}
        .cmp-table td {{ padding:10px 14px; border-bottom:1px solid #eee; font-size:14px; }}
        .cmp-table tr:last-child td {{ border-bottom:none; }}
        .cmp-table tr:hover td {{ background:#f9f9f9; }}
        .hint {{ color:#777; font-size:13px; margin-top:6px; }}
        nav {{ margin-bottom:24px; font-size:14px; }}
        nav a {{ color:#0066cc; text-decoration:none; margin-right:16px; }}
        nav a:hover {{ text-decoration:underline; }}
    </style>
</head>
<body>
    <nav>
        <a href="/api/home">Home</a>
        <a href="/api/ml/win-predictor">Win Predictor</a>
        <a href="/api/ml/score-predictor">Score Predictor</a>
        <a href="/api/ml/retrain-summary">ML Retrain Summary</a>
        <a href="/api/ml/over-under">T20 Over/Under Runs</a>
        <a href="/api/ml/glossary">Glossary</a>
    </nav>
    <h1>ML Retrain Summary</h1>
    {adf_activity_badge("MlRetrainSummaryFeatureExtraction", "MlRetrainSummaryModelTraining")}
    <p style="color:#666">The cricket-score-predictor (XGBoost Regressor) and cricket-ou-classifier (XGBoost Classifier), retrained weekly in pl_ml_and_hypothesis.</p>
    {body}
</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")
