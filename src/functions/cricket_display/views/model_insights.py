"""
Model Insights — SHAP summary, PDP, decision tree visualisations.
Served for both the odds and no-odds win predictor models.

Routes:
  GET /api/ml/model-insights           — odds model insights page
  GET /api/ml/model-insights-no-odds   — no-odds model insights page
  GET /api/ml/model-insights/image     — serves a single PNG from gold blob
"""
import base64
from .common import json, logging, escape, func, get_named_container_client

_CHECKPOINTS  = ["innings1-only", "innings2-2over", "innings2-6over",
                 "innings2-10over", "innings2-16over"]
_PLOT_TYPES   = [
    ("shap_summary", "SHAP Summary",
     "Shows which features push the prediction toward Chase (right) or Defend (left). "
     "Red = high feature value, blue = low. Width shows how often that feature mattered."),
    ("pdp",          "Partial Dependence",
     "For each top feature: how does changing it (while holding others at their median) "
     "affect P(Chase wins)? Y-axis rises = feature favours chasing team."),
    ("tree0",        "Decision Tree #0",
     "The first tree in the XGBoost ensemble. Shows the most general splits the model learned. "
     "Leaf value > 0 → Chase wins, < 0 → Defended."),
]


def _load_history_index(gold, prefix):
    try:
        return json.loads(
            gold.get_blob_client(f"{prefix}/history_index.json").download_blob().readall()
        )
    except Exception:
        return {"runs": []}


def _load_image_b64(gold, path):
    try:
        data = gold.get_blob_client(path).download_blob().readall()
        return base64.b64encode(data).decode("ascii")
    except Exception:
        return None


def _render_page(gold, prefix, title, variant_key):
    history = _load_history_index(gold, prefix)
    runs    = history.get("runs") or []

    # Build history dropdown options
    hist_opts = '<option value="latest">Latest</option>'
    for r in runs:
        d   = escape(r.get("date", "?"))
        tr  = r.get("train_matches", "?")
        te  = r.get("test_matches",  "?")
        # best accuracy across checkpoints
        acc_vals = [v.get("xgb_test_accuracy") for v in r.get("accuracy", {}).values()
                    if v.get("xgb_test_accuracy") is not None]
        best_acc = f"{max(acc_vals):.0%}" if acc_vals else "?"
        hist_opts += f'<option value="{d}">{d} — {tr} train / {te} test — best acc {best_acc}</option>'

    # Build tabs per checkpoint
    tabs_html  = ""
    panels_html = ""
    for i, cp in enumerate(_CHECKPOINTS):
        active = "active" if i == 0 else ""
        tabs_html += f'<button class="tab-btn {active}" onclick="switchTab(this,\'{cp}\')">{cp}</button>'
        panels_html += f'<div id="panel-{cp}" class="tab-panel" style="{"" if i==0 else "display:none"}">'

        for plot_key, plot_label, plot_desc in _PLOT_TYPES:
            panels_html += f"""
            <div class="plot-section">
              <h3>{escape(plot_label)}</h3>
              <p class="plot-desc">{escape(plot_desc)}</p>
              <div class="img-wrap" id="img-{cp}-{plot_key}">
                <span class="img-loading">Loading…</span>
              </div>
            </div>"""

        panels_html += "</div>"

    # Build history comparison table
    hist_table = ""
    if runs:
        hist_table = """
        <h2>Training History</h2>
        <table class="hist-table">
          <thead><tr>
            <th>Date</th><th>Train</th><th>Test</th><th>Cutoff</th>
            <th>inn1-only acc</th><th>inn2-6over acc</th><th>inn2-16over acc</th>
          </tr></thead><tbody>"""
        for r in runs:
            acc = r.get("accuracy") or {}
            def _a(k): return f"{acc.get(k,{}).get('xgb_test_accuracy',0):.1%}" if acc.get(k,{}).get('xgb_test_accuracy') else "—"
            hist_table += f"""<tr>
              <td>{escape(r.get('date','?'))}</td>
              <td>{r.get('train_matches','?')}</td>
              <td>{r.get('test_matches','?')}</td>
              <td>{escape(r.get('train_cutoff','?'))}</td>
              <td>{_a('innings1-only')}</td>
              <td>{_a('innings2-6over')}</td>
              <td>{_a('innings2-16over')}</td>
            </tr>"""
        hist_table += "</tbody></table>"

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{escape(title)}</title>
<style>
  body {{ font-family: Arial, sans-serif; background:#f7f7f7; padding:24px; margin:0; }}
  h1   {{ font-size:22px; margin-bottom:4px; }}
  h2   {{ font-size:17px; margin:28px 0 8px; color:#333; }}
  h3   {{ font-size:14px; margin:16px 0 4px; color:#444; }}
  .subtitle {{ color:#555; font-size:13px; margin-bottom:20px; }}
  .controls {{ display:flex; gap:12px; align-items:center; margin-bottom:20px; flex-wrap:wrap; }}
  .controls label {{ font-size:13px; color:#555; }}
  select {{ padding:6px 10px; border-radius:6px; border:1px solid #ccc; font-size:13px; }}
  .tabs {{ display:flex; gap:6px; flex-wrap:wrap; margin-bottom:20px; }}
  .tab-btn {{ padding:8px 14px; border:1px solid #ccc; border-radius:6px; cursor:pointer;
              background:#fff; font-size:13px; }}
  .tab-btn.active {{ background:#0066cc; color:#fff; border-color:#0066cc; font-weight:bold; }}
  .tab-panel {{ }}
  .plot-section {{ background:#fff; border-radius:10px; padding:20px; margin-bottom:20px;
                   box-shadow:0 2px 6px #ddd; }}
  .plot-desc {{ font-size:13px; color:#555; margin:0 0 14px; }}
  .img-wrap img {{ max-width:100%; border-radius:6px; border:1px solid #eee; }}
  .img-loading {{ color:#999; font-size:13px; }}
  .img-error   {{ color:#c00; font-size:13px; }}
  .hist-table  {{ border-collapse:collapse; width:100%; font-size:13px; }}
  .hist-table th,td {{ border:1px solid #ddd; padding:8px 12px; text-align:left; }}
  .hist-table th {{ background:#f0f0f0; }}
  .back {{ font-size:13px; color:#0066cc; text-decoration:none; }}
</style>
</head>
<body>
<a class="back" href="/api/home">← Home</a>
<h1>{escape(title)}</h1>
<p class="subtitle">Generated by <code>pl_ml_and_hypothesis</code> · Re-run the pipeline to refresh all plots.</p>

<div class="controls">
  <label>History run:</label>
  <select id="hist-sel" onchange="reloadAll()">
    {hist_opts}
  </select>
  <span style="font-size:12px;color:#888">{len(runs)} run(s) stored</span>
</div>

<div class="tabs">{tabs_html}</div>
{panels_html}

{hist_table}

<script>
var _variant   = {json.dumps(variant_key)};
var _prefix    = {json.dumps(prefix)};
var _checkpoints = {json.dumps(_CHECKPOINTS)};
var _plotTypes   = {json.dumps([p[0] for p in _PLOT_TYPES])};
var _currentCp   = _checkpoints[0];
var _loading     = {{}};

function switchTab(btn, cp) {{
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _checkpoints.forEach(c => {{
    document.getElementById('panel-' + c).style.display = c === cp ? '' : 'none';
  }});
  _currentCp = cp;
  loadImages(cp);
}}

function reloadAll() {{
  loadImages(_currentCp);
}}

function loadImages(cp) {{
  var date = document.getElementById('hist-sel').value;
  _plotTypes.forEach(function(pk) {{
    var key  = cp + '-' + pk + '-' + date;
    if (_loading[key]) return;
    _loading[key] = true;
    var wrap = document.getElementById('img-' + cp + '-' + pk);
    if (!wrap) return;
    wrap.innerHTML = '<span class="img-loading">Loading…</span>';
    var folder = date === 'latest' ? 'latest' : 'history/' + date;
    var imgPath = _prefix + '/' + folder + '/' + pk + '_' + cp + '.png';
    fetch('/api/ml/model-insights/image?path=' + encodeURIComponent(imgPath))
      .then(function(r) {{ return r.json(); }})
      .then(function(d) {{
        if (d.ok) {{
          wrap.innerHTML = '<img src="data:image/png;base64,' + d.b64 + '" alt="' + pk + '">';
        }} else {{
          wrap.innerHTML = '<span class="img-error">Not generated yet — run pl_ml_and_hypothesis first.</span>';
        }}
        _loading[key] = false;
      }})
      .catch(function() {{
        wrap.innerHTML = '<span class="img-error">Load error.</span>';
        _loading[key] = false;
      }});
  }});
}}

// Load first tab on page load
window.onload = function() {{ loadImages(_currentCp); }};
</script>
</body>
</html>"""


def view_model_insights_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        prefix = "cricket/ml_features/t20/model_insights/win_predictor"
        html = _render_page(gold, prefix,
                            "Model Insights — Win Predictor (With Odds)", "win_predictor")
        return func.HttpResponse(html, mimetype="text/html")
    except Exception as e:
        logging.exception("model_insights error")
        return func.HttpResponse(f"Error: {e}", status_code=500)


def view_model_insights_no_odds_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        prefix = "cricket/ml_features/t20/model_insights/win_predictor_no_odds"
        html = _render_page(gold, prefix,
                            "Model Insights — Win Predictor (No Market Odds)", "win_predictor_no_odds")
        return func.HttpResponse(html, mimetype="text/html")
    except Exception as e:
        logging.exception("model_insights_no_odds error")
        return func.HttpResponse(f"Error: {e}", status_code=500)


def view_model_insights_image(req: func.HttpRequest) -> func.HttpResponse:
    """Serves a single insight PNG from the gold blob as base64 JSON."""
    path = req.params.get("path", "").strip()
    if not path or ".." in path or not path.startswith("cricket/ml_features/t20/model_insights/"):
        return func.HttpResponse(
            json.dumps({"ok": False, "error": "invalid path"}),
            mimetype="application/json", status_code=400,
        )
    try:
        gold = get_named_container_client("gold")
        b64  = _load_image_b64(gold, path)
        if b64 is None:
            return func.HttpResponse(json.dumps({"ok": False}), mimetype="application/json")
        return func.HttpResponse(json.dumps({"ok": True, "b64": b64}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"ok": False, "error": str(e)}), mimetype="application/json")
