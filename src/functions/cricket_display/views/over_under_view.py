from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    download_json, upload_json, get_named_container_client,
)

_CONFIG_BLOB   = "ml/train_config.json"
_METADATA_BLOB = "ml/over_under_model_metadata.json"

_AUC_THRESHOLD = 0.60   # same as over_under_predictor.py


def _load_config(gold) -> dict:
    try:
        return json.loads(gold.get_blob_client(_CONFIG_BLOB).download_blob().readall())
    except Exception:
        return {}


def view_ml_over_under_config_post(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    try:
        body = req.get_json()
        cutoff = str(body.get("train_cutoff_date", "")).strip()
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
        return func.HttpResponse(
            json.dumps({"ok": True, "train_cutoff_date": cutoff}),
            mimetype="application/json",
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"ok": False, "error": str(e)}),
            mimetype="application/json", status_code=500,
        )


def view_ml_over_under_html(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")

    config       = _load_config(gold)
    saved_cutoff = config.get("train_cutoff_date", "")

    try:
        raw      = gold.get_blob_client(_METADATA_BLOB).download_blob().readall()
        metadata = json.loads(raw)
    except Exception:
        metadata = None

    def _auc_color(v):
        if v is None:
            return "#999"
        if v >= 0.65:
            return "#2d7a2d"
        if v >= 0.55:
            return "#cc7700"
        return "#cc2200"

    def _auc_badge(v):
        if v is None:
            return "—"
        col  = _auc_color(v)
        flag = "✓" if v >= _AUC_THRESHOLD else ("~" if v >= 0.55 else "✗")
        return f'<span style="color:{col};font-weight:bold">{v:.3f} {flag}</span>'

    def _bar(pct, color="#0066cc"):
        w = min(int(pct * 2.5), 150)
        return f'<div style="background:{color};height:10px;width:{w}px;border-radius:3px;display:inline-block;vertical-align:middle;margin-left:8px"></div>'

    if metadata is None:
        body = """
        <div style="background:#fff3cd;border-left:4px solid #f0a000;padding:16px 20px;
                    border-radius:4px;margin:20px 0;font-size:14px;color:#555;">
            No model results found yet.<br>
            Run <strong>pl_ml_retrain</strong> in ADF to train the Over/Under models.
        </div>"""
    else:
        trained_at  = metadata.get("trained_at_utc", "?")[:19].replace("T", " ")
        n_train     = metadata.get("n_train_rows", "?")
        n_test      = metadata.get("n_test_rows",  0)
        n_models    = metadata.get("n_models", 0)
        models      = metadata.get("models", [])
        test_eval   = {e["key"]: e for e in (metadata.get("test_evaluation") or [])}

        # Split into per-checkpoint and pooled
        per_cp  = [m for m in models if m.get("checkpoint_over") != "pooled"]
        pooled  = [m for m in models if m.get("checkpoint_over") == "pooled"]

        body = f"""
        <div class="meta-grid">
            <div class="meta-box"><div class="meta-label">Last trained</div>
                <div class="meta-val">{escape(trained_at)} UTC</div></div>
            <div class="meta-box"><div class="meta-label">Models trained</div>
                <div class="meta-val">{n_models}</div></div>
            <div class="meta-box"><div class="meta-label">Training rows</div>
                <div class="meta-val">{n_train}</div></div>
            <div class="meta-box"><div class="meta-label">Test rows</div>
                <div class="meta-val">{n_test if n_test else "—"}</div></div>
        </div>

        <div class="info-note">
            <strong>How it works:</strong> At each checkpoint over (e.g. after over 8), the model uses
            current score, wickets in hand, betting line, run rate, and team odds to predict whether
            the innings total will go <strong>OVER</strong> or <strong>UNDER</strong> the live betting line.
            Models with CV-AUC &ge; {_AUC_THRESHOLD} are used as per-checkpoint models; others fall back to the pooled model.
        </div>

        <h2>Per-Checkpoint Models — Innings Total</h2>
        <p style="color:#555;margin-bottom:12px">
            One model per checkpoint over, trained on ~80 samples each.
            CP8 has the strongest signal (score/pace visible by mid-innings).
        </p>
        <table class="cmp-table">
            <thead>
                <tr>
                    <th>Market</th><th>Checkpoint</th><th>N (train)</th>
                    <th>OVER %</th><th>CV-AUC</th>
                    <th>Test-AUC</th><th>Used?</th>
                </tr>
            </thead>
            <tbody>"""

        for m in per_cp:
            mkt   = m.get("market", "")
            cp    = m.get("checkpoint_over", "?")
            n     = m.get("n_samples", 0)
            ov_pct = m.get("over_pct", 0)
            cv_auc = m.get("cv_auc")
            key    = f"{mkt}_cp{cp}"
            te     = test_eval.get(key, {})
            te_auc = te.get("test_auc") if te else None
            te_n   = te.get("n", 0)
            used   = cv_auc is not None and cv_auc >= _AUC_THRESHOLD

            body += f"""
            <tr>
                <td style="font-family:monospace;font-size:13px">{escape(mkt)}</td>
                <td style="text-align:center;font-weight:bold">over {cp}</td>
                <td style="text-align:center">{n}</td>
                <td style="text-align:center">{ov_pct:.1f}%</td>
                <td>{_auc_badge(cv_auc)}</td>
                <td>{'<span style="color:#999">n/a</span>' if te_auc is None else f'<span style="color:{_auc_color(te_auc)};font-weight:bold">{te_auc:.3f}</span> <span style="color:#aaa;font-size:11px">n={te_n}</span>'}</td>
                <td style="text-align:center">{'<span style="color:#2d7a2d;font-weight:bold">✓ per-cp</span>' if used else '<span style="color:#888">→ pooled</span>'}</td>
            </tr>"""

        body += "</tbody></table>"

        body += """
        <h2>Pooled Models</h2>
        <p style="color:#555;margin-bottom:12px">
            One model per market across all checkpoints (~640 innings_total rows, ~220 first_12 rows).
            Used as fallback when a per-checkpoint model's CV-AUC &lt; 0.60.
            Extra features: <code>checkpoint_over</code>, <code>balls_remaining</code>, <code>balls_completed</code>.
        </p>
        <table class="cmp-table">
            <thead>
                <tr><th>Market</th><th>N (train)</th><th>OVER %</th>
                    <th>CV-AUC</th><th>Test-AUC</th></tr>
            </thead>
            <tbody>"""

        for m in pooled:
            mkt    = m.get("market", "")
            n      = m.get("n_samples", 0)
            ov_pct = m.get("over_pct", 0)
            cv_auc = m.get("cv_auc")
            key    = f"{mkt}_pooled"
            te     = test_eval.get(key, {})
            te_auc = te.get("test_auc") if te else None
            te_n   = te.get("n", 0)

            body += f"""
            <tr>
                <td style="font-family:monospace">{escape(mkt)}_pooled</td>
                <td style="text-align:center">{n}</td>
                <td style="text-align:center">{ov_pct:.1f}%</td>
                <td>{_auc_badge(cv_auc)}</td>
                <td>{'<span style="color:#999">n/a</span>' if te_auc is None else f'<span style="color:{_auc_color(te_auc)};font-weight:bold">{te_auc:.3f}</span> <span style="color:#aaa;font-size:11px">n={te_n}</span>'}</td>
            </tr>"""

        body += "</tbody></table>"

        # Feature importance for pooled models
        body += """
        <h2>Feature Importances</h2>
        <p style="color:#555;margin-bottom:12px">
            Blue = odds-derived &nbsp;|&nbsp; Purple = composite &nbsp;|&nbsp; Default = score/pace
        </p>"""

        for m in pooled + per_cp[:2]:  # show pooled first, then CP8 and CP10 as examples
            fi = m.get("feature_importance")
            if not fi:
                continue
            cp_label  = m.get("checkpoint_over", "pooled")
            mkt_label = m.get("market", "")
            total_imp = sum(fi.values()) or 1
            body += f'<h3 style="margin-top:20px">{escape(mkt_label)} — {escape(str(cp_label))}</h3>'
            body += '<table class="fi-table"><thead><tr><th>Rank</th><th>Feature</th><th>%</th><th></th></tr></thead><tbody>'
            for rank, (feat, imp) in enumerate(sorted(fi.items(), key=lambda x: -x[1])[:8], 1):
                pct = 100 * imp / total_imp
                col = "#0055aa" if "odds" in feat or "implied" in feat else ("#6600aa" if "pace" in feat or "rr_" in feat else "#333")
                body += f'<tr><td style="color:#999;text-align:center">{rank}</td><td style="font-family:monospace;color:{col}">{escape(feat)}</td><td style="text-align:right;font-weight:bold">{pct:.1f}%</td><td>{_bar(pct, col)}</td></tr>'
            body += "</tbody></table>"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>ML Over/Under Predictor</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; background:#f5f5f5; margin:0; padding:30px; }}
        h1 {{ margin-bottom:6px; }}
        h2 {{ margin-top:36px; margin-bottom:12px; border-bottom:2px solid #ddd; padding-bottom:6px; }}
        h3 {{ margin-top:20px; margin-bottom:8px; color:#333; }}
        .meta-grid {{ display:flex; gap:16px; flex-wrap:wrap; margin:20px 0; }}
        .meta-box {{ background:white; padding:16px 20px; border-radius:8px;
                     box-shadow:0 1px 4px #ccc; min-width:160px; }}
        .meta-label {{ font-size:12px; color:#888; margin-bottom:4px; }}
        .meta-val {{ font-size:16px; font-weight:bold; color:#222; }}
        .info-note {{ background:#eff6ff; border-left:4px solid #2563eb;
                      padding:12px 16px; border-radius:4px; margin-bottom:24px;
                      font-size:14px; color:#333; line-height:1.6; }}
        .cmp-table {{ border-collapse:collapse; width:100%; background:white;
                      border-radius:8px; overflow:hidden; box-shadow:0 1px 4px #ccc;
                      margin-bottom:12px; }}
        .cmp-table th {{ background:#333; color:white; padding:10px 14px;
                         text-align:left; font-size:13px; }}
        .cmp-table td {{ padding:10px 14px; border-bottom:1px solid #eee; font-size:14px; }}
        .cmp-table tr:last-child td {{ border-bottom:none; }}
        .cmp-table tr:hover td {{ background:#f9f9f9; }}
        .fi-table {{ border-collapse:collapse; width:100%; max-width:600px; background:white;
                     border-radius:8px; overflow:hidden; box-shadow:0 1px 4px #ccc;
                     margin-bottom:8px; }}
        .fi-table th {{ background:#f0f0f0; color:#444; padding:8px 12px;
                        text-align:left; font-size:12px; font-weight:600; }}
        .fi-table td {{ padding:7px 12px; border-bottom:1px solid #f3f3f3; font-size:13px; }}
        .fi-table tr:last-child td {{ border-bottom:none; }}
        nav {{ margin-bottom:24px; font-size:14px; }}
        nav a {{ color:#0066cc; text-decoration:none; margin-right:16px; }}
        nav a:hover {{ text-decoration:underline; }}
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
        .auc-legend {{ font-size:12px; color:#666; margin-top:8px; }}
    </style>
</head>
<body>
    <nav>
        <a href="/api/home">Home</a>
        <a href="/api/ended/view">Ended Matches</a>
        <a href="/api/ml/win-predictor">ML Win Predictor</a>
        <a href="/api/ml/over-under">ML Over/Under</a>
        <a href="/api/ml/glossary">Glossary</a>
    </nav>

    <h1>ML Over/Under Predictor</h1>
    <p style="color:#666">LightGBM models predicting innings total and first-12-overs Over/Under at checkpoint overs during inn1.</p>

    <div class="cutoff-card">
        <div class="cutoff-label">
            <strong>Train / test cutoff date</strong><br>
            Matches on or before this date → training set. After this date → held-out test set.<br>
            Save here, then run <code>pl_ml_retrain</code> in ADF to rebuild models with the new split.
        </div>
        <input type="date" id="cutoffInput" class="cutoff-input" value="{escape(saved_cutoff)}">
        <button class="cutoff-btn" onclick="saveCutoff()">Save</button>
        <span id="cutoffStatus" class="cutoff-status"></span>
    </div>
    <div class="auc-legend">
        AUC colour: <span style="color:#2d7a2d">■ ≥0.65 good</span>
        &nbsp;<span style="color:#cc7700">■ 0.55–0.65 marginal</span>
        &nbsp;<span style="color:#cc2200">■ &lt;0.55 weak</span>
        &nbsp;— CV-AUC = cross-validated on train rows, Test-AUC = held-out rows only.
    </div>

    <script>
    function saveCutoff() {{
        var d = document.getElementById('cutoffInput').value;
        var s = document.getElementById('cutoffStatus');
        if (!d) {{ s.style.color='#c00'; s.textContent='Pick a date first.'; return; }}
        s.style.color='#888'; s.textContent='Saving…';
        fetch('/api/ml/over-under/config', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{train_cutoff_date: d}})
        }})
        .then(r => r.json())
        .then(j => {{
            if (j.ok) {{ s.style.color='#2d7a2d'; s.textContent='Saved ✓ — re-run pl_ml_retrain in ADF to apply.'; }}
            else       {{ s.style.color='#c00';    s.textContent='Error: ' + (j.error || 'unknown'); }}
        }})
        .catch(e => {{ s.style.color='#c00'; s.textContent='Network error: ' + e; }});
    }}
    </script>

    {body}
</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")
