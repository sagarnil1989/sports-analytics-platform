"""Inn1 score predictor page — MAE/RMSE per model + per-match test predictions."""
from .common import json, logging, escape, func, get_named_container_client


def view_ml_score_predictor_html(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")

    try:
        raw     = gold.get_blob_client("cricket/ml_features/t20/score_predictor_summary.json").download_blob().readall()
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

    def _bar(pct):
        w = min(int(pct * 3), 100)
        return f'<div style="background:#0066cc;height:10px;width:{w}px;border-radius:3px;display:inline-block;vertical-align:middle;margin-left:8px"></div>'

    if summary is None:
        body = "<p style='color:#c00'>No results found. Run <b>pl_ml_retrain</b> in ADF first.</p>"
    else:
        trained_at = summary.get("generated_at_utc", "unknown")
        train_n    = summary.get("train_matches", "?")
        test_n     = summary.get("test_matches",  "?")
        cutoff     = summary.get("train_cutoff",  "?")

        body = f"""
        <div class="meta-grid">
            <div class="meta-box"><div class="meta-label">Last trained</div><div class="meta-val">{escape(trained_at)}</div></div>
            <div class="meta-box"><div class="meta-label">Training matches</div><div class="meta-val">{train_n}</div></div>
            <div class="meta-box"><div class="meta-label">Test matches</div><div class="meta-val">{test_n}</div></div>
            <div class="meta-box"><div class="meta-label">Train / test split date</div><div class="meta-val">{escape(cutoff)}</div></div>
        </div>
        <div class="algo-note">
            <strong>Target:</strong> Predicted innings-1 final score (runs) &nbsp;|&nbsp;
            <strong>Algorithms:</strong> XGBoost Regressor (XGB) + Random Forest Regressor (RF)
        </div>

        <div class="metric-legend">
            <div class="metric-legend-title">What do the metrics mean?</div>
            <div class="metric-row">
                <span class="metric-name">MAE</span>
                <span class="metric-desc">Mean Absolute Error — average number of runs the prediction was off by.
                A MAE of 12 means on average the model predicted within ±12 runs of the actual final score.
                <strong>Lower is better.</strong></span>
            </div>
            <div class="metric-row">
                <span class="metric-name">RMSE</span>
                <span class="metric-desc">Root Mean Squared Error — like MAE but large errors are penalised more heavily.
                A prediction that is off by 30 runs hurts the RMSE far more than three predictions off by 10.
                RMSE is always ≥ MAE. A big gap between them means the model has occasional large misses.
                <strong>Lower is better.</strong></span>
            </div>
            <div class="metric-row">
                <span class="metric-name">R²</span>
                <span class="metric-desc">R-squared — how much of the variation in final scores the model explains.
                1.0 = perfect. 0.0 = no better than always predicting the average score.
                Negative = worse than just guessing the average. With 30 matches, 0.3–0.6 is realistic.
                <strong>Higher is better.</strong></span>
            </div>
            <div class="metric-row">
                <span class="metric-name">XGB vs RF</span>
                <span class="metric-desc">XGBoost (XGB) is the primary model — it learns complex interactions between features using boosted trees.
                Random Forest (RF) is trained in parallel as a cross-check — it tends to be more stable on small datasets
                and helps confirm whether XGB results are genuine or overfit.</span>
            </div>
        </div>
        """

        # ── model comparison ──────────────────────────────────────
        body += """
        <h2>Model Comparison</h2>
        <table class="cmp-table">
            <thead><tr>
                <th>Model</th><th>What it knows</th>
                <th>XGB MAE</th><th>XGB RMSE</th><th>XGB R²</th>
                <th>RF MAE</th><th>RF RMSE</th><th>RF R²</th>
                <th>Features</th>
            </tr></thead><tbody>
        """
        descriptions = {
            "at-over-6":  "Powerplay complete — overs 1–6 known",
            "at-over-10": "Halfway — overs 1–10 known",
            "at-over-16": "Death approaching — overs 1–16 known",
        }
        for m in summary.get("models", []):
            name = m.get("name", "")
            desc = descriptions.get(name, "")
            xgb  = m.get("xgb", {});  rf = m.get("rf", {})
            xmae = xgb.get("mae");    xrmse = xgb.get("rmse"); xr2 = xgb.get("r2")
            rmae = rf.get("mae");     rrmse = rf.get("rmse");   rr2 = rf.get("r2")
            nf   = m.get("feature_count", "?")
            body += f"""<tr>
                <td><strong>{escape(name)}</strong></td>
                <td style="color:#555;font-size:13px">{escape(desc)}</td>
                <td style="color:{_mae_color(xmae)};font-weight:bold">{f"{xmae:.1f}" if xmae else "—"}</td>
                <td>{f"{xrmse:.1f}" if xrmse else "—"}</td>
                <td style="color:{_r2_color(xr2)}">{f"{xr2:.3f}" if xr2 is not None else "—"}</td>
                <td style="color:{_mae_color(rmae)};font-weight:bold">{f"{rmae:.1f}" if rmae else "—"}</td>
                <td>{f"{rrmse:.1f}" if rrmse else "—"}</td>
                <td style="color:{_r2_color(rr2)}">{f"{rr2:.3f}" if rr2 is not None else "—"}</td>
                <td style="text-align:center">{nf}</td>
            </tr>"""
        body += "</tbody></table>"
        body += """
        <p class="hint">
            MAE colour: <span style="color:#2d7a2d">■ ≤10 runs</span>
            &nbsp;<span style="color:#cc7700">■ 11–18 runs</span>
            &nbsp;<span style="color:#cc2200">■ &gt;18 runs</span>
            &nbsp;— with 30 matches, 12–18 run MAE is realistic; improves with more data.
        </p>
        """

        # ── test match predictions ────────────────────────────────
        body += "<h2>Test Match Predictions (XGBoost)</h2>"
        body += "<p style='color:#555;margin-bottom:20px'>Each row is a test match. <b>Score @ cutoff</b> = actual runs/wickets at the point the model sees. <b>Error</b> = predicted − actual (positive = over-estimated).</p>"

        _state_col = {
            "at-over-6":  ("Score @ ov6",  "inn1_score_at_6",  "inn1_wkts_at_6"),
            "at-over-10": ("Score @ ov10", "inn1_score_at_10", "inn1_wkts_at_10"),
            "at-over-16": ("Score @ ov16", "inn1_score_at_16", "inn1_wkts_at_16"),
        }

        for m in summary.get("models", []):
            name  = m.get("name", "")
            preds = m.get("test_predictions", [])
            lbl, sc_key, wk_key = _state_col.get(name, ("Score @ cutoff", "inn1_score_at_6", "inn1_wkts_at_6"))
            body += f"<h3>{escape(name)}</h3>"
            if not preds:
                body += "<p style='color:#999'>No test predictions yet — run pl_ml_retrain.</p>"
                continue
            body += f"""<table class="cmp-table"><thead>
                <tr><th>Match</th><th>Date</th><th>Inn1 batting</th>
                    <th>{lbl}</th>
                    <th>Predicted final</th><th>Actual final</th><th>Error</th></tr>
            </thead><tbody>"""
            for p in preds:
                pred   = p.get("predicted_score")
                actual = p.get("actual_score")
                err    = p.get("error")
                sc     = p.get(sc_key)
                wk     = p.get(wk_key)
                sc_str = f"{int(sc)}/{int(wk)}" if sc is not None and wk is not None else (str(int(sc)) if sc is not None else "—")
                err_s  = f"+{err:.1f}" if err and err > 0 else (f"{err:.1f}" if err is not None else "—")
                err_c  = "#cc2200" if err and abs(err) > 20 else ("#cc7700" if err and abs(err) > 10 else "#2d7a2d")
                body += f"""<tr>
                    <td>{escape(str(p.get("match_name",""))[:50])}</td>
                    <td style="color:#666">{escape(str(p.get("match_date",""))[:10])}</td>
                    <td style="font-size:13px">{escape(str(p.get("inn1_bat_team","")))}</td>
                    <td style="font-family:monospace;font-weight:bold">{escape(sc_str)}</td>
                    <td style="font-weight:bold;font-family:monospace">{f"{pred:.0f}" if pred is not None else "—"}</td>
                    <td style="font-family:monospace">{f"{actual:.0f}" if actual is not None else "—"}</td>
                    <td style="font-weight:bold;color:{err_c}">{err_s}</td>
                </tr>"""
            body += "</tbody></table><br>"

        # ── feature importances ───────────────────────────────────
        body += "<h2>Feature Importances (XGBoost)</h2>"
        body += "<p style='color:#555;margin-bottom:20px'>Only features kept after pruning. Colour: <span style='color:#0055aa'>■ odds</span> <span style='color:#6600aa'>■ composite</span> <span style='color:#007755'>■ categorical</span></p>"

        for m in summary.get("models", []):
            name = m.get("name", "")
            fi   = m.get("feature_importance", [])
            if not fi:
                body += f"<h3>{escape(name)}</h3><p style='color:#999'>No data yet.</p>"
                continue
            body += f"<h3>{escape(name)} <span style='font-weight:normal;color:#888;font-size:14px'>({len(fi)} features)</span></h3>"
            body += """<table class="fi-table"><thead>
                <tr><th>Rank</th><th>Feature</th><th>% of total</th><th></th></tr>
            </thead><tbody>"""
            for row in fi:
                feat = row.get("feature", "")
                pct  = row.get("pct_of_total", 0)
                if "_bat_odds" in feat or "_bowl_odds" in feat or "odds_swing" in feat:
                    fc = "#0055aa"
                elif "_rp_wkt" in feat or "_wkts_only" in feat:
                    fc = "#6600aa"
                elif "bat_team" in feat or "bowl_team" in feat or "venue" in feat:
                    fc = "#007755"
                else:
                    fc = "#333"
                body += f"""<tr>
                    <td style="color:#999;text-align:center">{row.get('rank','')}</td>
                    <td style="font-family:monospace;color:{fc}">{escape(str(feat))}</td>
                    <td style="text-align:right;font-weight:bold">{pct:.2f}%</td>
                    <td>{_bar(pct)}</td>
                </tr>"""
            body += "</tbody></table><br>"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Inn1 Score Predictor</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; background:#f5f5f5; margin:0; padding:30px; }}
        h1 {{ margin-bottom:6px; }}
        h2 {{ margin-top:36px; margin-bottom:12px; border-bottom:2px solid #ddd; padding-bottom:6px; }}
        h3 {{ margin-top:24px; margin-bottom:8px; color:#333; }}
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
                        min-width:60px; padding-top:1px; }}
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
        .fi-table {{ border-collapse:collapse; width:100%; background:white;
                     border-radius:8px; overflow:hidden; box-shadow:0 1px 4px #ccc; }}
        .fi-table th {{ background:#f0f0f0; color:#444; padding:8px 12px;
                        text-align:left; font-size:12px; font-weight:600; }}
        .fi-table td {{ padding:7px 12px; border-bottom:1px solid #f3f3f3; font-size:13px; }}
        .fi-table tr:last-child td {{ border-bottom:none; }}
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
        <a href="/api/ml/score-matrix">Score Feature Matrix</a>
        <a href="/api/ml/glossary">Glossary</a>
    </nav>
    <h1>Inn1 Score Predictor</h1>
    <p style="color:#666">Predicts final innings-1 score at three points in the innings — powerplay, halfway, and over 16</p>
    {body}
</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")
