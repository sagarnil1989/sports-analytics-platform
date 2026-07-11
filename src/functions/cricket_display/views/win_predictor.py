from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    get_named_container_client,
    build_simple_table_page, adf_activity_badge,
)

# Shared train/test cutoff config — same blob used by ml_extract_over_under_features,
# ml_win_predictor, and inn1_score_predictor, so every ML notebook splits on one date.
_CONFIG_BLOB = "ml/train_config.json"


def _load_config(gold) -> dict:
    try:
        return json.loads(gold.get_blob_client(_CONFIG_BLOB).download_blob().readall())
    except Exception:
        return {}


def view_ml_win_predictor_config_post(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    try:
        body = req.get_json()
        cutoff = str(body.get("train_cutoff", "")).strip()
        if len(cutoff) != 10 or cutoff[4] != "-" or cutoff[7] != "-":
            return func.HttpResponse(
                json.dumps({"ok": False, "error": "Invalid date format — use YYYY-MM-DD"}),
                mimetype="application/json", status_code=400,
            )
        cfg = _load_config(gold)
        cfg["train_cutoff_date"] = cutoff
        gold.get_blob_client(_CONFIG_BLOB).upload_blob(
            json.dumps(cfg, indent=2).encode(), overwrite=True
        )
        return func.HttpResponse(json.dumps({"ok": True, "train_cutoff": cutoff}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"ok": False, "error": str(e)}), mimetype="application/json", status_code=500)


def view_ml_win_predictor_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        return _view_ml_win_predictor_html_inner(req)
    except Exception as _ex:
        import traceback
        _tb = traceback.format_exc()
        logging.exception("win_predictor page error")
        return func.HttpResponse(
            f"<pre style='color:red;padding:20px'>Win Predictor error:\n{escape(str(_ex))}\n\n{escape(_tb)}</pre>",
            mimetype="text/html", status_code=200,
        )


def _view_ml_win_predictor_html_inner(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")

    config = _load_config(gold)
    saved_cutoff = config.get("train_cutoff_date", "")

    try:
        raw = gold.get_blob_client("cricket/ml_features/t20/win_predictor_summary.json").download_blob().readall()
        summary = json.loads(raw)
    except Exception:
        summary = None

    def _pct_bar(pct):
        w = min(int(pct * 3), 100)
        return f'<div style="background:#0066cc;height:10px;width:{w}px;border-radius:3px;display:inline-block;vertical-align:middle;margin-left:8px"></div>'

    def _acc_color(v):
        if v is None: return "#999"
        if v >= 0.70: return "#2d7a2d"
        if v >= 0.60: return "#cc7700"
        return "#cc2200"

    if summary is None:
        body = "<p style='color:#c00'>No model results found. Run the win predictor notebook in Databricks first.</p>"
    else:
        trained_at  = summary.get("generated_at_utc", "unknown")
        train_n     = summary.get("train_matches", "?")
        test_n      = summary.get("test_matches",  "?")
        cutoff      = summary.get("train_cutoff",  "?")
        algos_now   = ", ".join(summary.get("algorithms", {}).get("current", []))
        algos_future= summary.get("algorithms", {}).get("future", [""])[0]

        # ── header stats ──────────────────────────────────────────
        body = f"""
        <div class="meta-grid">
            <div class="meta-box"><div class="meta-label">Last trained</div><div class="meta-val">{escape(trained_at)}</div></div>
            <div class="meta-box"><div class="meta-label">Training matches</div><div class="meta-val">{train_n}</div></div>
            <div class="meta-box"><div class="meta-label">Test matches</div><div class="meta-val">{test_n}</div></div>
            <div class="meta-box"><div class="meta-label">Train / test split date</div><div class="meta-val">{escape(cutoff)}</div></div>
        </div>
        <div class="algo-note">
            <strong>Current algorithms:</strong> {escape(algos_now)}<br>
            <strong>Future (≥500 matches):</strong> {escape(algos_future)}
        </div>

        <div class="metric-legend">
            <div class="metric-legend-title">What do the metrics mean?</div>
            <div class="metric-row">
                <span class="metric-name">Accuracy</span>
                <span class="metric-desc">Percentage of test matches where the model predicted the correct result —
                either "chase won" or "defended". For example, 75% accuracy means 3 out of 4 test matches
                were predicted correctly. <strong>Higher is better.</strong> With small datasets (under 10 test matches)
                even one wrong prediction moves this by 10–15%, so treat it as a guide not a verdict.</span>
            </div>
            <div class="metric-row">
                <span class="metric-name">ROC-AUC</span>
                <span class="metric-desc">Area Under the ROC Curve — measures how well the model separates the two outcomes
                regardless of which threshold you use. 0.5 = random guessing (coin flip), 1.0 = perfect separation.
                Unlike accuracy, this uses the model's confidence score (0–100%) not just the final prediction,
                so it is more informative on small datasets. Above 0.70 is good; above 0.80 is strong.
                <strong>Higher is better.</strong></span>
            </div>
            <div class="metric-row">
                <span class="metric-name">XGB vs RF</span>
                <span class="metric-desc">XGBoost (XGB) is the primary model — it learns complex interactions between
                features using boosted decision trees. Random Forest (RF) is trained in parallel as a cross-check.
                If XGB and RF agree on which features matter, the signal is genuine. If they disagree strongly,
                the dataset may be too small to draw conclusions yet.</span>
            </div>
        </div>
        """

        # ── model comparison table ────────────────────────────────
        body += """
        <h2>Model Comparison</h2>
        <p style="color:#555;margin-bottom:12px">
            <strong>Train</strong> = matches the model learned from &nbsp;|&nbsp;
            <strong>Test</strong> = unseen matches held back for evaluation.
            A large Train→Test gap means the model memorised training data rather than generalising.
        </p>
        <table class="cmp-table">
            <thead>
                <tr>
                    <th rowspan="2">Model</th>
                    <th rowspan="2">What it knows</th>
                    <th colspan="2" style="text-align:center;border-bottom:1px solid #ccc;">XGBoost — Accuracy</th>
                    <th colspan="2" style="text-align:center;border-bottom:1px solid #ccc;">XGBoost — ROC-AUC</th>
                    <th colspan="2" style="text-align:center;border-bottom:1px solid #ccc;">Random Forest — Accuracy</th>
                    <th colspan="2" style="text-align:center;border-bottom:1px solid #ccc;">Random Forest — ROC-AUC</th>
                    <th rowspan="2">Features</th>
                </tr>
                <tr>
                    <th style="color:#888;font-weight:normal">Train</th><th>Test</th>
                    <th style="color:#888;font-weight:normal">Train</th><th>Test</th>
                    <th style="color:#888;font-weight:normal">Train</th><th>Test</th>
                    <th style="color:#888;font-weight:normal">Train</th><th>Test</th>
                </tr>
            </thead>
            <tbody>
        """
        descriptions = {
            "innings1-only":   "Full innings 1 complete — chase not yet started",
            "innings2-2over":  "Innings 1 + first 2 overs of chase",
            "innings2-6over":  "Innings 1 + full powerplay of chase (6 overs)",
            "innings2-10over": "Innings 1 + chase through over 10 (halfway)",
            "innings2-16over": "Innings 1 + chase through over 16 (death approaching)",
        }

        def _acc_cell(v, bold=False):
            col = _acc_color(v)
            val = f"{v:.1%}" if v else "—"
            return f'<td style="color:{col};{"font-weight:bold;" if bold else ""}">{val}</td>'

        def _auc_cell(v):
            return f'<td>{f"{v:.3f}" if v else "—"}</td>'

        for m in summary.get("models", []):
            name  = m.get("name", "")
            desc  = descriptions.get(name, "")
            xgb   = m.get("xgb", {})
            rf    = m.get("rf",  {})
            # support both old key names (accuracy) and new (test_accuracy)
            x_tr_acc = xgb.get("train_accuracy")
            x_te_acc = xgb.get("test_accuracy") or xgb.get("accuracy")
            x_tr_auc = xgb.get("train_roc_auc")
            x_te_auc = xgb.get("test_roc_auc")  or xgb.get("roc_auc")
            r_tr_acc = rf.get("train_accuracy")
            r_te_acc = rf.get("test_accuracy")  or rf.get("accuracy")
            r_tr_auc = rf.get("train_roc_auc")
            r_te_auc = rf.get("test_roc_auc")   or rf.get("roc_auc")
            nfeat = m.get("feature_count", "?")
            body += f"""
            <tr>
                <td><strong>{escape(name)}</strong></td>
                <td style="color:#555;font-size:13px">{escape(desc)}</td>
                {_acc_cell(x_tr_acc)}{_acc_cell(x_te_acc, bold=True)}
                {_auc_cell(x_tr_auc)}{_auc_cell(x_te_auc)}
                {_acc_cell(r_tr_acc)}{_acc_cell(r_te_acc, bold=True)}
                {_auc_cell(r_tr_auc)}{_auc_cell(r_te_auc)}
                <td style="text-align:center">{nfeat}</td>
            </tr>"""
        body += "</tbody></table>"
        body += """
        <p class="hint">Test accuracy colour: <span style="color:#2d7a2d">■ ≥70%</span>
        &nbsp;<span style="color:#cc7700">■ 60–70%</span>
        &nbsp;<span style="color:#cc2200">■ &lt;60%</span>
        &nbsp;— Train accuracy near 100% with low test accuracy = overfitting.</p>
        """

        # ── score context columns per model ───────────────────────
        _score_ctx = {
            "innings1-only":   [("Inn1 final", "inn1_score", None),
                                ("Inn2 final", "inn2_score", None)],
            "innings2-2over":  [("Inn1 final", "inn1_score", None),
                                ("Inn2 @ ov2", "inn2_ov2_score", "inn2_ov2_wickets"),
                                ("Inn2 final", "inn2_score", None)],
            "innings2-6over":  [("Inn1 final", "inn1_score", None),
                                ("Inn2 @ ov6", "inn2_ov6_score", "inn2_ov6_wickets"),
                                ("Inn2 final", "inn2_score", None)],
            "innings2-10over": [("Inn1 final", "inn1_score", None),
                                ("Inn2 @ ov10", "inn2_ov10_score", "inn2_ov10_wickets"),
                                ("Inn2 final", "inn2_score", None)],
            "innings2-16over": [("Inn1 final", "inn1_score", None),
                                ("Inn2 @ ov16", "inn2_ov16_score", "inn2_ov16_wickets"),
                                ("Inn2 final", "inn2_score", None)],
        }

        # odds feature keys at each model's checkpoint (from feature_values dict)
        _odds_ctx = {
            "innings1-only":   ("inn1_ov20_bat_odds",  "inn1_ov20_bowl_odds"),
            "innings2-2over":  ("inn2_ov2_bat_odds",   "inn2_ov2_bowl_odds"),
            "innings2-6over":  ("inn2_ov6_bat_odds",   "inn2_ov6_bowl_odds"),
            "innings2-10over": ("inn2_ov10_bat_odds",  "inn2_ov10_bowl_odds"),
            "innings2-16over": ("inn2_ov16_bat_odds",  "inn2_ov16_bowl_odds"),
        }

        def _pred_table(preds, ctx, split_label, split_color, model_name="", fi_list=None, odds_keys=None):
            if not preds:
                return f"<p style='color:#999'>No {split_label.lower()} predictions — run pl_ml_retrain.</p>"

            # Feature importance lookup: {feature_name: pct_of_total}
            fi_map = {row["feature"]: row["pct_of_total"] for row in (fi_list or [])}

            ctx_headers = "".join(f"<th>{lbl}</th>" for lbl, _, _ in ctx)
            odds_headers = '<th>Bat Odds</th><th>Bowl Odds</th>' if odds_keys else ''
            ncols = 8 + len(ctx) + (2 if odds_keys else 0)
            html = f"""<table class="cmp-table"><thead>
                <tr><th>Event ID</th><th>Match</th><th>Date</th><th>Inn1 batting</th>
                    {ctx_headers}
                    {odds_headers}
                    <th>Predicted</th><th>Actual</th><th>Confidence</th><th></th></tr>
            </thead><tbody>"""

            def _score_cell(p, score_key, wkt_key):
                display = p.get(f"{score_key}_display")
                if display:
                    return str(display)
                s = p.get(score_key)
                w = p.get(wkt_key) if wkt_key else None
                if s is None: return "—"
                try:
                    si = int(s)
                except (ValueError, TypeError):
                    return "—"  # NaN or unconvertible
                if w is not None:
                    try:
                        return f"{si}/{int(w)}"
                    except (ValueError, TypeError):
                        return str(si)
                return str(si)

            def _wi_panel(p, panel_id, model_nm):
                """Build the collapsible What-If panel for one prediction row."""
                feat_vals = p.get("feature_values") or {}
                if not feat_vals:
                    return '<tr id="{}" style="display:none"><td colspan="{}"><em style="color:#999;font-size:12px">No feature values stored — re-run pl_ml_and_hypothesis.</em></td></tr>'.format(panel_id, ncols)

                orig_json = json.dumps(feat_vals)
                orig_conf = p.get("confidence_pct", 50.0) or 50.0
                orig_pred = "Chase won" if p.get("predicted") == 1 else "Defended"

                # Sort features: those with importance first, then rest alphabetically
                sorted_feats = sorted(feat_vals.keys(),
                    key=lambda f: -fi_map.get(f, 0))

                rows_html = ""
                for feat in sorted_feats:
                    val = feat_vals[feat]
                    pct = fi_map.get(feat, 0)
                    bar_w = min(int(pct * 6), 80)
                    bar_html = f'<div style="display:inline-block;background:#0066cc;height:8px;width:{bar_w}px;border-radius:2px;vertical-align:middle;margin-right:4px"></div>'
                    val_str = "" if val is None else str(val)
                    input_type = "text" if isinstance(val, str) else "number"
                    step_attr  = 'step="0.01"' if isinstance(val, float) else ""
                    rows_html += f"""<tr>
                        <td style="font-family:monospace;font-size:12px;color:#333">{escape(feat)}</td>
                        <td style="white-space:nowrap">{bar_html}<span style="font-size:11px;color:#666">{pct:.1f}%</span></td>
                        <td><input type="{input_type}" {step_attr}
                                   class="wi-input" data-feat="{escape(feat)}"
                                   data-panel="{panel_id}"
                                   value="{escape(val_str)}"
                                   style="width:90px;padding:3px 6px;border:1px solid #ccc;border-radius:3px;font-size:12px;font-family:monospace;"
                                   oninput="wiSchedule('{panel_id}','{escape(model_nm)}')">
                        </td>
                    </tr>"""

                return f"""<tr id="{panel_id}" data-orig="{escape(orig_json)}" data-model="{escape(model_nm)}" style="display:none;background:#f8f8ff;">
                  <td colspan="{ncols}" style="padding:0;">
                    <div class="wi-panel">
                      <div class="wi-panel-header">
                        <strong>What-If Analysis</strong>
                        <span style="font-size:12px;color:#666;margin-left:10px">
                          Edit values → prediction updates in real-time
                        </span>
                        <button class="wi-reset-btn" onclick="wiReset('{panel_id}')">↺ Reset</button>
                      </div>

                      <div style="display:flex;gap:24px;align-items:flex-start;flex-wrap:wrap;">
                        <div>
                          <table class="wi-feat-table">
                            <thead><tr>
                              <th>Feature</th><th>Importance</th><th>Value</th>
                            </tr></thead>
                            <tbody>{rows_html}</tbody>
                          </table>
                        </div>

                        <div class="wi-result-col" id="{panel_id}-result">
                          <div class="wi-result-label">Current prediction</div>
                          <div class="wi-prob-bar" id="{panel_id}-bar">
                            <div class="wi-prob-chase"  id="{panel_id}-chase"
                                 style="width:{round(orig_conf if p.get('predicted')==1 else 100-orig_conf)}%">
                              Chase {round(orig_conf if p.get('predicted')==1 else 100-orig_conf)}%
                            </div>
                            <div class="wi-prob-defend" id="{panel_id}-defend"
                                 style="width:{round(orig_conf if p.get('predicted')==0 else 100-orig_conf)}%">
                              Defend {round(orig_conf if p.get('predicted')==0 else 100-orig_conf)}%
                            </div>
                          </div>
                          <div id="{panel_id}-delta" class="wi-delta"></div>
                          <div id="{panel_id}-status" class="wi-status"></div>
                          <div style="font-size:11px;color:#999;margin-top:8px;">
                            Original: <b>{orig_pred}</b> @ {orig_conf:.1f}%
                          </div>
                        </div>
                      </div>
                    </div>
                  </td>
                </tr>"""

            row_num = 0
            for p in sorted(preds, key=lambda p: p.get("match_date") or "", reverse=True):
                predicted = p.get("predicted")
                actual    = p.get("chasing_won")
                correct   = p.get("correct")
                conf      = p.get("confidence_pct")
                pred_lbl  = "Chase won" if predicted == 1 else "Defended"
                act_lbl   = "Chase won" if actual    == 1 else "Defended"
                tick      = "✓" if correct else "✗"
                tick_col  = "#2d7a2d" if correct else "#cc2200"
                conf_bar  = f'<div style="background:#0066cc;height:8px;width:{int((conf or 0) * 1.2)}px;border-radius:3px;display:inline-block;vertical-align:middle;margin-left:6px"></div>' if conf else ""
                ctx_cells = "".join(
                    f"<td style='font-family:monospace'>{_score_cell(p, sk, wk)}</td>"
                    for _, sk, wk in ctx
                )
                if odds_keys:
                    fv = p.get("feature_values") or {}
                    bat_o = fv.get(odds_keys[0])
                    bowl_o = fv.get(odds_keys[1])
                    def _fmt_odds(v):
                        if v is None: return "—"
                        try: return f"{float(v):.2f}"
                        except: return "—"
                    odds_cells = f"<td style='font-family:monospace;text-align:right'>{_fmt_odds(bat_o)}</td><td style='font-family:monospace;text-align:right'>{_fmt_odds(bowl_o)}</td>"
                else:
                    odds_cells = ""
                event_id_val = escape(str(p.get("event_id") or ""))
                safe_model   = model_name.replace("-", "_")
                panel_id     = f"wi_{safe_model}_{split_label}_{row_num}"
                has_feat_vals = bool(p.get("feature_values"))

                html += f"""<tr style="cursor:default">
                    <td style="font-family:monospace;color:#888;font-size:12px">{event_id_val}</td>
                    <td>{escape(str(p.get("match_name","")))}</td>
                    <td style="color:#666">{escape(str(p.get("match_date",""))[:10])}</td>
                    <td style="font-size:13px">{escape(str(p.get("inn1_bat_team","")))}</td>
                    {ctx_cells}
                    {odds_cells}
                    <td style="font-weight:bold">{pred_lbl}</td>
                    <td style="color:#555">{act_lbl}</td>
                    <td style="font-weight:bold">{f"{conf:.1f}%" if conf else "—"}{conf_bar}</td>
                    <td style="color:{tick_col};font-size:18px;font-weight:bold;text-align:center">{tick}</td>
                    <td><button class="wi-toggle-btn" onclick="wiToggle('{panel_id}')"
                                title="What-If: edit features and see how prediction changes"
                                {'style="opacity:0.35;cursor:not-allowed"' if not has_feat_vals else ''}>▼</button></td>
                </tr>"""
                html += _wi_panel(p, panel_id, model_name)
                row_num += 1

            html += "</tbody></table><br>"
            return html

        # ── test predictions ──────────────────────────────────────
        body += "<h2>Test Match Predictions (XGBoost)</h2>"
        body += "<p style='color:#555;margin-bottom:20px'>Matches <strong>after</strong> the train/test cutoff — the model never saw these during training. <b>Predicted</b> and <b>Actual</b> refer to whether the chasing team (batting 2nd) won. Click <b>▼</b> on any row to open the What-If panel.</p>"
        for m in summary.get("models", []):
            name = m.get("name", "")
            ctx  = _score_ctx.get(name, [("Inn1 final", "inn1_score", None)])
            body += f"<h3>{escape(name)}</h3>"
            body += _pred_table(m.get("test_predictions", []), ctx, "Test", "#0066cc",
                                model_name=name, fi_list=m.get("feature_importance", []),
                                odds_keys=_odds_ctx.get(name))

        # ── train predictions ─────────────────────────────────────
        body += "<h2>Training Match Predictions (XGBoost)</h2>"
        body += "<p style='color:#555;margin-bottom:20px'>Matches <strong>before</strong> the cutoff — the model <em>was trained on these</em>. High accuracy here is expected and does not mean the model is good. Compare with Test accuracy above to see the generalisation gap.</p>"
        for m in summary.get("models", []):
            name = m.get("name", "")
            ctx  = _score_ctx.get(name, [("Inn1 final", "inn1_score", None)])
            body += f"<h3>{escape(name)}</h3>"
            body += _pred_table(m.get("train_predictions", []), ctx, "Train", "#888",
                                model_name=name, fi_list=m.get("feature_importance", []),
                                odds_keys=_odds_ctx.get(name))

        # ── feature importance per model ──────────────────────────
        body += "<h2>Feature Importances (XGBoost)</h2>"
        body += "<p style='color:#555;margin-bottom:20px'>Only features the model actually used after pruning are shown. Features with zero importance were automatically dropped before retraining.</p>"

        for m in summary.get("models", []):
            name = m.get("name", "")
            fi   = m.get("feature_importance", [])
            if not fi:
                body += f"<h3>{escape(name)}</h3><p style='color:#999'>No importance data yet.</p>"
                continue

            body += f"<h3>{escape(name)} <span style='font-weight:normal;color:#888;font-size:14px'>({len(fi)} features)</span></h3>"
            body += """<table class="fi-table"><thead>
                <tr><th>Rank</th><th>Feature</th><th>% of total</th><th></th></tr>
            </thead><tbody>"""
            for row in fi:
                rank = row.get("rank", "")
                feat = row.get("feature", "")
                pct  = row.get("pct_of_total", 0)
                bar  = _pct_bar(pct)

                # colour-code feature groups
                if "_bat_odds" in feat or "_bowl_odds" in feat or "odds_swing" in feat:
                    fc = "#0055aa"   # odds — blue
                elif "_rp_wkt" in feat or "_pressure" in feat:
                    fc = "#6600aa"   # composite — purple
                elif "bat_team" in feat or "bowl_team" in feat or "venue" in feat:
                    fc = "#007755"   # categorical — green
                else:
                    fc = "#333"      # raw runs/wickets — default

                body += f"""<tr>
                    <td style="color:#999;text-align:center">{rank}</td>
                    <td style="font-family:monospace;color:{fc}">{escape(str(feat))}</td>
                    <td style="text-align:right;font-weight:bold">{pct:.2f}%</td>
                    <td>{bar}</td>
                </tr>"""
            body += "</tbody></table><br>"

        # ── LSTM status ───────────────────────────────────────────
        lstm_threshold = 500
        lstm_ready = train_n != "?" and int(train_n) >= lstm_threshold
        lstm_color = "#2d7a2d" if lstm_ready else "#cc7700"
        lstm_icon  = "✓" if lstm_ready else "✗"
        lstm_msg   = (
            "LSTM threshold reached — uncomment Step 12 in the win predictor notebook."
            if lstm_ready else
            f"LSTM not yet active. Have {train_n} training matches, need {lstm_threshold}. "
            f"Continue collecting data — XGBoost and Random Forest are the right tools for now."
        )
        body += f"""
        <h2>LSTM Status</h2>
        <div class="lstm-box" style="border-left:4px solid {lstm_color}">
            <span style="color:{lstm_color};font-size:20px;font-weight:bold">{lstm_icon}</span>
            &nbsp; {escape(lstm_msg)}
        </div>
        """

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>ML Win Predictor</title>
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
                        min-width:80px; padding-top:1px; }}
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
        .fi-table tr:hover td {{ background:#fafafa; }}
        .lstm-box {{ background:white; padding:16px 20px; border-radius:8px;
                     box-shadow:0 1px 4px #ccc; font-size:14px; color:#444; }}
        nav {{ margin-bottom:24px; font-size:14px; }}
        nav a {{ color:#0066cc; text-decoration:none; margin-right:16px; }}
        nav a:hover {{ text-decoration:underline; }}

        /* What-If panel */
        .wi-toggle-btn {{
            background:#f0f4ff; border:1px solid #99b; border-radius:4px;
            padding:3px 8px; cursor:pointer; font-size:12px; color:#336;
        }}
        .wi-toggle-btn:hover {{ background:#dde8ff; }}
        .wi-panel {{
            background:#f8f8ff; border-top:2px solid #99b; padding:16px 20px 14px;
        }}
        .wi-panel-header {{
            display:flex; align-items:center; gap:8px; margin-bottom:12px;
            font-size:14px; color:#333;
        }}
        .wi-reset-btn {{
            margin-left:auto; background:white; border:1px solid #ccc;
            border-radius:4px; padding:3px 10px; cursor:pointer; font-size:12px;
            color:#555;
        }}
        .wi-reset-btn:hover {{ background:#f0f0f0; }}
        .wi-feat-table {{ border-collapse:collapse; font-size:13px; }}
        .wi-feat-table th {{
            background:#eef; padding:5px 10px; text-align:left;
            font-size:11px; color:#446; font-weight:600;
        }}
        .wi-feat-table td {{ padding:4px 10px; border-bottom:1px solid #eee; }}
        .wi-result-col {{
            min-width:240px; padding:8px 0;
        }}
        .wi-result-label {{
            font-size:12px; color:#666; font-weight:600;
            text-transform:uppercase; letter-spacing:.5px; margin-bottom:8px;
        }}
        .wi-prob-bar {{
            display:flex; height:32px; border-radius:6px;
            overflow:hidden; width:100%; margin-bottom:8px;
        }}
        .wi-prob-chase {{
            background:#0066cc; color:white; font-size:12px; font-weight:bold;
            display:flex; align-items:center; justify-content:center;
            transition:width .3s ease;
        }}
        .wi-prob-defend {{
            background:#7b2d00; color:white; font-size:12px; font-weight:bold;
            display:flex; align-items:center; justify-content:center;
            transition:width .3s ease;
        }}
        .wi-delta {{
            font-size:13px; font-weight:bold; min-height:18px; margin-bottom:4px;
        }}
        .wi-status {{ font-size:11px; color:#999; min-height:16px; }}
        .cutoff-card {{ background:white; border:1px solid #e0e0e0; border-radius:8px;
                        padding:16px 20px; margin-bottom:24px; box-shadow:0 1px 4px #ccc;
                        display:flex; align-items:center; gap:16px; flex-wrap:wrap; }}
        .cutoff-label {{ font-size:13px; color:#555; }}
        .cutoff-input {{ font-size:14px; padding:6px 10px; border:1px solid #ccc;
                         border-radius:4px; font-family:monospace; }}
        .cutoff-btn {{ background:#0066cc; color:white; border:none; border-radius:4px;
                       padding:7px 18px; font-size:14px; cursor:pointer; }}
        .cutoff-btn:hover {{ background:#0052a3; }}
        .cutoff-status {{ font-size:13px; margin-left:4px; }}
    </style>
</head>
<body>
    <nav>
        <a href="/api/home">Home</a>
        <a href="/api/live/view" style="color:#c00;font-weight:bold;">🔴 Live</a>
        <a href="/api/ended/view">Ended Matches</a>
        <a href="/api/ml/win-predictor">ML Win Predictor</a>
        <a href="/api/ml/win-predictor/whatif">What-If Analysis</a>
        <a href="/api/ml/glossary">Glossary</a>
    </nav>
    <h1>ML Win Predictor</h1>
    {adf_activity_badge("MlWinPredictor")}
    <p style="color:#666">T20 match outcome prediction — three models trained with progressively more match information</p>

    <div class="cutoff-card">
        <span class="cutoff-label"><strong>Train / test cutoff date</strong><br>
            Matches before this date → training set. On or after → test set.<br>
            Save here, then re-run <code>pl_ml_retrain</code> in ADF to rebuild models.</span>
        <input type="date" id="cutoffInput" class="cutoff-input" value="{escape(saved_cutoff)}">
        <button class="cutoff-btn" onclick="saveCutoff()">Save</button>
        <span id="cutoffStatus" class="cutoff-status"></span>
    </div>
    <script>
    function saveCutoff() {{
        var d = document.getElementById('cutoffInput').value;
        var s = document.getElementById('cutoffStatus');
        if (!d) {{ s.style.color='#c00'; s.textContent='Pick a date first.'; return; }}
        s.style.color='#888'; s.textContent='Saving…';
        fetch('/api/ml/win-predictor/config', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{train_cutoff: d}})
        }})
        .then(r => r.json())
        .then(j => {{
            if (j.ok) {{ s.style.color='#2d7a2d'; s.textContent='Saved ✓ — re-run pl_ml_retrain to apply.'; }}
            else       {{ s.style.color='#c00';    s.textContent='Error: ' + (j.error || 'unknown'); }}
        }})
        .catch(e => {{ s.style.color='#c00'; s.textContent='Network error: ' + e; }});
    }}

    /* ── What-If panel JS ─────────────────────────────────── */
    var _wiTimers = {{}};
    var _wiOrigConf = {{}};  // panelId → {{prob_chase, prob_defend}}

    function wiToggle(panelId) {{
        var row = document.getElementById(panelId);
        if (!row) return;
        var open = row.style.display !== 'none';
        row.style.display = open ? 'none' : 'table-row';
        // Store original probs on first open
        if (!open && !_wiOrigConf[panelId]) {{
            var ch = document.getElementById(panelId + '-chase');
            var de = document.getElementById(panelId + '-defend');
            if (ch && de) {{
                _wiOrigConf[panelId] = {{
                    chaseW: ch.style.width, chaseT: ch.textContent,
                    defendW: de.style.width, defendT: de.textContent
                }};
            }}
        }}
    }}

    function wiSchedule(panelId, model) {{
        clearTimeout(_wiTimers[panelId]);
        document.getElementById(panelId + '-status').textContent = 'Calculating…';
        _wiTimers[panelId] = setTimeout(function() {{
            wiRun(panelId, model);
        }}, 450);
    }}

    function wiRun(panelId, model) {{
        // Collect current values from all inputs in this panel
        var inputs = document.querySelectorAll('[data-panel="' + panelId + '"]');
        var rawFeatures = {{}};
        inputs.forEach(function(inp) {{
            var feat = inp.getAttribute('data-feat');
            var v = inp.value.trim();
            if (v === '') {{ rawFeatures[feat] = null; return; }}
            var n = Number(v);
            rawFeatures[feat] = isNaN(n) ? v : n;
        }});

        fetch('/api/ml/win-predictor/whatif', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{ model: model, raw_features: rawFeatures }})
        }})
        .then(function(r) {{ return r.json(); }})
        .then(function(d) {{
            document.getElementById(panelId + '-status').textContent = '';
            if (!d.ok) {{
                document.getElementById(panelId + '-status').textContent = 'Error: ' + (d.error || '?');
                return;
            }}
            var pChase  = Math.round(d.prob_chase_wins * 100);
            var pDefend = Math.round(d.prob_defends * 100);
            var ch = document.getElementById(panelId + '-chase');
            var de = document.getElementById(panelId + '-defend');
            ch.style.width = pChase  + '%';
            de.style.width = pDefend + '%';
            ch.textContent = pChase  > 8 ? 'Chase '  + pChase  + '%' : '';
            de.textContent = pDefend > 8 ? 'Defend ' + pDefend + '%' : '';

            // Delta badge
            var orig = _wiOrigConf[panelId];
            if (orig) {{
                var origChase = parseInt(orig.chaseW);
                var delta = pChase - origChase;
                var deltaEl = document.getElementById(panelId + '-delta');
                if (Math.abs(delta) < 1) {{
                    deltaEl.textContent = '— no change';
                    deltaEl.style.color = '#999';
                }} else {{
                    var sign = delta > 0 ? '+' : '';
                    deltaEl.textContent = sign + delta + 'pp vs original';
                    deltaEl.style.color = delta > 0 ? '#0066cc' : '#7b2d00';
                }}
            }}
        }})
        .catch(function(e) {{
            document.getElementById(panelId + '-status').textContent = 'Network error: ' + e;
        }});
    }}

    function wiReset(panelId) {{
        var row = document.getElementById(panelId);
        if (!row) return;
        var origVals = JSON.parse(row.getAttribute('data-orig') || '{{}}');
        var model    = row.getAttribute('data-model') || '';
        var inputs = document.querySelectorAll('[data-panel="' + panelId + '"]');
        inputs.forEach(function(inp) {{
            var feat = inp.getAttribute('data-feat');
            var v = origVals[feat];
            inp.value = (v === null || v === undefined) ? '' : v;
        }});
        document.getElementById(panelId + '-delta').textContent = '';
        document.getElementById(panelId + '-status').textContent = 'Recalculating…';
        // Re-run inference from restored values so bar matches exactly
        wiRun(panelId, model);
    }}
    </script>

    {body}
</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")
