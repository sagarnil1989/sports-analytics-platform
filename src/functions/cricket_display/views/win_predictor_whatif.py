"""
Win Predictor — What-If Sensitivity Analysis (Phase 3).

GET  /api/ml/win-predictor/whatif  — interactive form page
POST /api/ml/win-predictor/whatif  — inference endpoint (returns JSON)

Loads the XGBoost model pkl from gold blob, builds the feature vector from
user-supplied aggregate inputs (venue, teams, phase scores, odds), fills
per-over granular features with their training-set medians, and returns
P(chase wins).
"""
from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    get_named_container_client,
    adf_activity_badge,
)

_MODEL_PREFIX = "cricket/ml_features/t20/live_models/win_predictor"
_CAT_ENCODINGS_BLOB = f"{_MODEL_PREFIX}/cat_encodings.json"

_VALID_CHECKPOINTS = [
    "innings1-only",
    "innings2-2over",
    "innings2-6over",
    "innings2-10over",
    "innings2-16over",
]

_CHECKPOINT_OVER = {
    "innings1-only":   None,
    "innings2-2over":  2,
    "innings2-6over":  6,
    "innings2-10over": 10,
    "innings2-16over": 16,
}


def _load_model_obj(gold, checkpoint: str) -> Optional[Dict]:
    try:
        import pickle
        raw = gold.get_blob_client(f"{_MODEL_PREFIX}/{checkpoint}.pkl").download_blob().readall()
        return pickle.loads(raw)
    except Exception:
        return None


def _load_cat_encodings(gold) -> Dict:
    try:
        return json.loads(gold.get_blob_client(_CAT_ENCODINGS_BLOB).download_blob().readall())
    except Exception:
        return {}


def _safe_div(a, b, default=0.0):
    try:
        return round(a / b, 4) if b and b != 0 else default
    except Exception:
        return default


def _build_feature_vector(inputs: Dict, features: List[str], encodings: Dict) -> List[float]:
    """
    Build the full feature vector for XGBoost inference from simplified user inputs.
    Categorical features: mapped via encodings dict or -1 for unknown.
    Numeric features: use user value if supplied, otherwise stored training median.
    Per-over granular features (inn{n}_ov{k}_*): always filled from median.
    """
    checkpoint = inputs.get("model", "innings1-only")
    inn2_over = _CHECKPOINT_OVER.get(checkpoint)

    # ── User-supplied aggregate inputs ────────────────────────────────────────
    venue      = str(inputs.get("venue", "") or "").strip() or "unknown"
    bat_team   = str(inputs.get("bat_team", "") or "").strip() or "unknown"
    bowl_team  = str(inputs.get("bowl_team", "") or "").strip() or "unknown"
    is_womens  = float(inputs.get("is_womens", 0) or 0)
    is_weekend = float(inputs.get("is_weekend", 0) or 0)

    total_score = float(inputs.get("inn1_total_score") or 0)
    total_wkts  = float(inputs.get("inn1_total_wickets") or 0)
    pp_score    = float(inputs.get("inn1_pp_score") or 0)
    pp_wkts     = float(inputs.get("inn1_pp_wickets") or 0)
    mid_score   = float(inputs.get("inn1_mid_score") or 0)
    mid_wkts    = float(inputs.get("inn1_mid_wickets") or 0)

    ov1_odds  = inputs.get("inn1_ov1_bat_odds")
    pp_odds   = inputs.get("inn1_pp_bat_odds")
    last_odds = inputs.get("inn1_last_bat_odds")

    # ── Innings 1 composite features ─────────────────────────────────────────
    inn1_pp_rp_wkt       = _safe_div(pp_score, 10 - pp_wkts)
    inn1_mid_rp_wkt      = _safe_div(mid_score - pp_score, 10 - mid_wkts)
    inn1_mid_wkts_only   = max(0.0, mid_wkts - pp_wkts)
    inn1_death_runs      = max(0.0, total_score - mid_score)
    inn1_death_wkts      = max(0.0, total_wkts - mid_wkts)
    inn1_pressure        = _safe_div(total_score, 10 - total_wkts)
    inn1_odds_swing_full = (float(last_odds) - float(ov1_odds)) if (last_odds and ov1_odds) else None
    inn1_odds_swing_pp   = (float(pp_odds) - float(ov1_odds))   if (pp_odds and ov1_odds)   else None

    # ── Innings 2 inputs (optional for inn2 checkpoints) ─────────────────────
    inn2_score    = float(inputs.get("inn2_score") or 0)
    inn2_wkts     = float(inputs.get("inn2_wickets") or 0)
    inn2_bat_odds = inputs.get("inn2_bat_odds")
    runs_needed   = max(0.0, (total_score + 1) - inn2_score)

    if inn2_over and inn2_over > 0:
        balls_done  = inn2_over * 6
        balls_left  = 120 - balls_done
        crr         = round(inn2_score / (balls_done / 6), 4) if balls_done > 0 else 0.0
        rrr_val     = round(runs_needed / (balls_left / 6), 4) if balls_left > 0 else 99.0
        rr_diff     = round(crr - rrr_val, 4)
    else:
        crr = rrr_val = rr_diff = 0.0

    # Composites for the specific inn2 checkpoint
    if inn2_over:
        n = str(inn2_over)
        inn2_rp_wkt = _safe_div(inn2_score, 10 - inn2_wkts)
        inn2_odds_swing = (float(inn2_bat_odds) - float(last_odds)) if (inn2_bat_odds and last_odds) else None
        inn2_rnpw = _safe_div(runs_needed, 10 - inn2_wkts)
        inn2_chase_diff = round(rrr_val * (inn2_wkts + 1), 3)
    else:
        n = ""
        inn2_rp_wkt = inn2_odds_swing = inn2_rnpw = inn2_chase_diff = None

    # ── Full feature value dict ───────────────────────────────────────────────
    explicit_vals: Dict[str, Any] = {
        "venue":             venue,
        "inn1_bat_team":     bat_team,
        "inn1_bowl_team":    bowl_team,
        "is_womens_match":   is_womens,
        "is_weekend_match":  is_weekend,
        "inn1_total_score":  total_score,
        "inn1_total_wickets": total_wkts,
        "inn1_pp_rp_wkt":        inn1_pp_rp_wkt,
        "inn1_pp_wickets":       pp_wkts,
        "inn1_mid_rp_wkt":       inn1_mid_rp_wkt,
        "inn1_mid_wickets_only": inn1_mid_wkts_only,
        "inn1_death_runs":       inn1_death_runs,
        "inn1_death_wickets":    inn1_death_wkts,
        "inn1_pressure":         inn1_pressure,
        "inn1_odds_swing_full":  inn1_odds_swing_full,
        "inn1_odds_swing_pp":    inn1_odds_swing_pp,
    }

    if inn2_over:
        k = inn2_over
        explicit_vals.update({
            f"inn2_ov{k}_score":      inn2_score,
            f"inn2_ov{k}_wickets":    inn2_wkts,
            f"inn2_ov{k}_crr":        crr,
            f"inn2_ov{k}_rrr":        rrr_val,
            f"inn2_ov{k}_rr_diff":    rr_diff,
            f"inn2_ov{k}_runs_needed": runs_needed,
            f"inn2_ov{k}_rp_wkt":           inn2_rp_wkt,
            f"inn2_ov{k}_odds_swing":        inn2_odds_swing,
            f"inn2_ov{k}_runs_needed_per_wkt": inn2_rnpw,
            f"inn2_ov{k}_chase_difficulty":  inn2_chase_diff,
        })
        if inn2_bat_odds:
            explicit_vals[f"inn2_ov{k}_bat_odds"] = float(inn2_bat_odds)

    # ── Build vector using features list + encodings for NA-fill ─────────────
    X = []
    feature_details = []
    for feat in features:
        enc = encodings.get(feat)
        raw = explicit_vals.get(feat)

        if isinstance(enc, dict):
            # Categorical feature
            if raw is not None and raw in enc:
                val = float(enc[raw])
                src = "user"
            else:
                val = -1.0
                src = "unknown" if raw is not None else "median"
        else:
            # Numeric feature
            if raw is not None:
                try:
                    val = float(raw)
                    src = "user" if feat in explicit_vals else "computed"
                except (TypeError, ValueError):
                    val = float(enc) if enc is not None else 0.0
                    src = "median"
            else:
                # Per-over granular feature — fill with training median
                val = float(enc) if enc is not None else 0.0
                src = "median"

        X.append(val)
        feature_details.append({"name": feat, "value": round(val, 4), "source": src})

    return X, feature_details


# ── POST: inference endpoint ─────────────────────────────────────────────────

def view_win_predictor_whatif_post(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(
            json.dumps({"ok": False, "error": "Invalid JSON body"}),
            mimetype="application/json", status_code=400,
        )

    checkpoint = body.get("model", "innings1-only")
    if checkpoint not in _VALID_CHECKPOINTS:
        return func.HttpResponse(
            json.dumps({"ok": False, "error": f"Unknown model: {checkpoint}"}),
            mimetype="application/json", status_code=400,
        )

    gold = get_named_container_client("gold")
    model_obj = _load_model_obj(gold, checkpoint)
    if model_obj is None:
        return func.HttpResponse(
            json.dumps({"ok": False, "error": f"Model '{checkpoint}' not found — run pl_ml_and_hypothesis first"}),
            mimetype="application/json", status_code=404,
        )

    features  = model_obj["features"]
    encodings = model_obj.get("encodings") or {}

    try:
        if "raw_features" in body:
            # Direct path: caller supplies the actual feature values (pre-encoding),
            # as stored in prediction records' feature_values dict.  Missing features
            # are filled with training-set medians from the encodings dict.
            raw = body["raw_features"]
            X: List[float] = []
            feature_details = []
            for feat in features:
                enc = encodings.get(feat)
                val_raw = raw.get(feat)
                if isinstance(enc, dict):
                    key = str(val_raw) if val_raw is not None else None
                    if key is not None and key in enc:
                        val, src = float(enc[key]), "user"
                    elif val_raw is not None and val_raw in enc:
                        val, src = float(enc[val_raw]), "user"
                    else:
                        val, src = -1.0, "unknown_cat" if val_raw is not None else "median"
                else:
                    if val_raw is not None:
                        val, src = float(val_raw), "user"
                    else:
                        val, src = float(enc) if enc is not None else 0.0, "median"
                X.append(val)
                feature_details.append({"name": feat, "value": round(val, 4), "source": src})
        else:
            X, feature_details = _build_feature_vector(body, features, encodings)

        import numpy as np
        X_arr = np.array(X, dtype=float).reshape(1, -1)
        model = model_obj["model"]
        proba = model.predict_proba(X_arr)[0]
        prob_chase = round(float(proba[1]), 4)
        prob_defend = round(float(proba[0]), 4)
    except Exception as e:
        logging.exception("What-if inference failed")
        return func.HttpResponse(
            json.dumps({"ok": False, "error": str(e)}),
            mimetype="application/json", status_code=500,
        )

    return func.HttpResponse(
        json.dumps({
            "ok": True,
            "model": checkpoint,
            "prob_chase_wins": prob_chase,
            "prob_defends": prob_defend,
            "feature_count": len(features),
            "features_used": feature_details[:30],  # cap for response size
        }),
        mimetype="application/json",
    )


# ── GET: interactive form page ───────────────────────────────────────────────

def view_win_predictor_whatif_html(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    cat_encodings = _load_cat_encodings(gold)

    venues    = sorted(cat_encodings.get("venue", {}).keys())
    bat_teams = sorted(cat_encodings.get("inn1_bat_team", {}).keys())

    def _opts(choices):
        return "".join(f'<option value="{escape(c)}">{escape(c)}</option>' for c in choices)

    venues_opts = _opts(venues)
    teams_opts  = _opts(bat_teams)

    models_loaded = []
    for cp in _VALID_CHECKPOINTS:
        try:
            gold.get_blob_client(f"{_MODEL_PREFIX}/{cp}.pkl").get_blob_properties()
            models_loaded.append(cp)
        except Exception:
            pass

    model_select_opts = "".join(
        f'<option value="{cp}">{cp}</option>' for cp in models_loaded
    ) or "".join(f'<option value="{cp}">{cp}</option>' for cp in _VALID_CHECKPOINTS)

    no_model_msg = ""
    if not models_loaded:
        no_model_msg = '<div class="warn">⚠ No models found — run pl_ml_and_hypothesis first.</div>'

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Win Predictor — What-If</title>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, sans-serif; background:#f5f5f5; margin:0; padding:30px; max-width:900px; }}
    h1 {{ margin-bottom:4px; }}
    h2 {{ margin-top:28px; margin-bottom:10px; font-size:16px; color:#333;
          border-bottom:1px solid #ddd; padding-bottom:4px; }}
    nav a {{ color:#0066cc; text-decoration:none; margin-right:16px; font-size:14px; }}
    .badge {{ font-size:11px; color:#888; margin:6px 0 18px; }}
    .warn {{ background:#fff3cd; border-left:4px solid #f0a000; padding:12px 16px;
             border-radius:4px; margin:12px 0; font-size:14px; }}
    .form-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
    .form-row {{ display:flex; flex-direction:column; gap:4px; }}
    .form-row label {{ font-size:13px; color:#555; font-weight:600; }}
    .form-row input, .form-row select {{
        padding:7px 10px; border:1px solid #ccc; border-radius:4px;
        font-size:14px; font-family:monospace; }}
    .form-row.full {{ grid-column:1/-1; }}
    .hint {{ font-size:11px; color:#999; margin-top:2px; }}
    .section-box {{ background:white; border-radius:8px; padding:20px;
                    box-shadow:0 1px 4px #ccc; margin-bottom:16px; }}
    .inn2-section {{ display:none; }}
    .btn {{ background:#0066cc; color:white; border:none; border-radius:5px;
            padding:10px 28px; font-size:15px; cursor:pointer; margin-top:12px; }}
    .btn:hover {{ background:#0052a3; }}
    .result-box {{ display:none; background:white; border-radius:8px; padding:20px;
                   box-shadow:0 1px 4px #ccc; margin-top:20px; }}
    .prob-bar {{ display:flex; height:36px; border-radius:6px; overflow:hidden; margin:12px 0; }}
    .prob-chase {{ background:#2d7a2d; display:flex; align-items:center;
                   justify-content:center; color:white; font-weight:bold; font-size:14px; }}
    .prob-defend {{ background:#cc2200; display:flex; align-items:center;
                    justify-content:center; color:white; font-weight:bold; font-size:14px; }}
    .feat-table {{ width:100%; border-collapse:collapse; font-size:12px; margin-top:10px; }}
    .feat-table th {{ background:#f0f0f0; padding:5px 8px; text-align:left; }}
    .feat-table td {{ padding:4px 8px; border-bottom:1px solid #f3f3f3; font-family:monospace; }}
    .src-user {{ color:#2d7a2d; font-size:11px; }}
    .src-median {{ color:#999; font-size:11px; }}
    .src-computed {{ color:#0066cc; font-size:11px; }}
    .src-unknown {{ color:#cc7700; font-size:11px; }}
    .error-box {{ background:#f8d7da; color:#721c24; border-radius:6px;
                  padding:12px 16px; margin-top:16px; display:none; }}
    .spinner {{ display:none; color:#888; margin-top:8px; font-size:13px; }}
  </style>
</head>
<body>
  <nav>
    <a href="/api/home">Home</a>
    <a href="/api/live/view" style="color:#c00;font-weight:bold;">🔴 Live</a>
    <a href="/api/ml/win-predictor">Win Predictor</a>
    <a href="/api/ml/glossary">Glossary</a>
  </nav>
  <h1>Win Predictor — What-If</h1>
  {adf_activity_badge("MlWinPredictor")}
  <p style="color:#666;font-size:14px">Adjust match inputs below and see how the model's prediction changes.
  Per-over granular features are filled with training-set medians.</p>
  {no_model_msg}

  <div class="section-box">
    <h2>Model checkpoint</h2>
    <div class="form-row">
      <label>Select model</label>
      <select id="model" onchange="onModelChange(this.value)">
        {model_select_opts}
      </select>
      <div class="hint">innings1-only = after 1st innings; innings2-Nover = N overs into the chase</div>
    </div>
  </div>

  <div class="section-box">
    <h2>Match context</h2>
    <div class="form-grid">
      <div class="form-row">
        <label>Batting team (innings 1)</label>
        <input type="text" id="bat_team" list="team-list" placeholder="e.g. Mumbai Indians">
        <datalist id="team-list">{teams_opts}</datalist>
      </div>
      <div class="form-row">
        <label>Bowling team (innings 1)</label>
        <input type="text" id="bowl_team" list="team-list2" placeholder="e.g. Chennai Super Kings">
        <datalist id="team-list2">{teams_opts}</datalist>
      </div>
      <div class="form-row">
        <label>Venue</label>
        <input type="text" id="venue" list="venue-list" placeholder="e.g. Wankhede Stadium">
        <datalist id="venue-list">{venues_opts}</datalist>
      </div>
      <div class="form-row">
        <label>Match type</label>
        <div style="display:flex;gap:20px;padding-top:4px;">
          <label><input type="checkbox" id="is_womens"> Women's match</label>
          <label><input type="checkbox" id="is_weekend"> Weekend</label>
        </div>
      </div>
    </div>
  </div>

  <div class="section-box">
    <h2>Innings 1</h2>
    <div class="form-grid">
      <div class="form-row">
        <label>Final score</label>
        <input type="number" id="inn1_total_score" placeholder="e.g. 180" min="0" max="300">
      </div>
      <div class="form-row">
        <label>Final wickets</label>
        <input type="number" id="inn1_total_wickets" placeholder="e.g. 6" min="0" max="10">
      </div>
      <div class="form-row">
        <label>Powerplay score (end of over 6)</label>
        <input type="number" id="inn1_pp_score" placeholder="e.g. 55" min="0" max="120">
      </div>
      <div class="form-row">
        <label>Powerplay wickets lost</label>
        <input type="number" id="inn1_pp_wickets" placeholder="e.g. 2" min="0" max="10">
      </div>
      <div class="form-row">
        <label>Middle-overs score (end of over 15)</label>
        <input type="number" id="inn1_mid_score" placeholder="e.g. 120" min="0" max="250">
      </div>
      <div class="form-row">
        <label>Middle-overs wickets (cumulative at ov15)</label>
        <input type="number" id="inn1_mid_wickets" placeholder="e.g. 5" min="0" max="10">
      </div>
      <div class="form-row">
        <label>Batting odds after over 1 <span class="hint">(optional)</span></label>
        <input type="number" id="inn1_ov1_bat_odds" placeholder="e.g. 1.85" step="0.01" min="1.0">
      </div>
      <div class="form-row">
        <label>Batting odds at powerplay end <span class="hint">(optional)</span></label>
        <input type="number" id="inn1_pp_bat_odds" placeholder="e.g. 1.75" step="0.01" min="1.0">
      </div>
      <div class="form-row">
        <label>Batting odds at end of innings 1 <span class="hint">(optional)</span></label>
        <input type="number" id="inn1_last_bat_odds" placeholder="e.g. 1.60" step="0.01" min="1.0">
      </div>
    </div>
  </div>

  <div class="section-box inn2-section" id="inn2-section">
    <h2>Innings 2 chase state</h2>
    <p class="hint">Enter the state at the point where the selected checkpoint model predicts.</p>
    <div class="form-grid">
      <div class="form-row">
        <label>Chase score at checkpoint over</label>
        <input type="number" id="inn2_score" placeholder="e.g. 65" min="0" max="300">
      </div>
      <div class="form-row">
        <label>Wickets fallen at checkpoint</label>
        <input type="number" id="inn2_wickets" placeholder="e.g. 2" min="0" max="10">
      </div>
      <div class="form-row">
        <label>Chasing batting odds at checkpoint <span class="hint">(optional)</span></label>
        <input type="number" id="inn2_bat_odds" placeholder="e.g. 1.90" step="0.01" min="1.0">
      </div>
    </div>
  </div>

  <button class="btn" onclick="runWhatIf()">Predict</button>
  <div class="spinner" id="spinner">Computing…</div>

  <div class="result-box" id="result-box">
    <h2>Prediction result</h2>
    <div id="result-model" style="font-size:13px;color:#888;margin-bottom:8px;"></div>
    <div class="prob-bar">
      <div class="prob-chase" id="bar-chase" style="width:50%">Chase wins: 50%</div>
      <div class="prob-defend" id="bar-defend" style="width:50%">Defends: 50%</div>
    </div>
    <h3 style="margin-top:20px;font-size:14px;">Feature values sent to model</h3>
    <p class="hint">
      <span class="src-user">■ User input</span> &nbsp;
      <span class="src-computed">■ Computed from inputs</span> &nbsp;
      <span class="src-median">■ Training median (per-over detail)</span> &nbsp;
      <span class="src-unknown">■ Unknown category (−1)</span>
    </p>
    <table class="feat-table">
      <thead><tr><th>#</th><th>Feature</th><th>Value</th><th>Source</th></tr></thead>
      <tbody id="feat-rows"></tbody>
    </table>
  </div>
  <div class="error-box" id="error-box"></div>

<script>
function onModelChange(v) {{
  const inn2 = document.getElementById('inn2-section');
  inn2.style.display = v.startsWith('innings2') ? 'block' : 'none';
}}

function numOrNull(id) {{
  const v = document.getElementById(id).value.trim();
  return v === '' ? null : parseFloat(v);
}}

function strOrNull(id) {{
  const v = document.getElementById(id).value.trim();
  return v === '' ? null : v;
}}

function runWhatIf() {{
  const body = {{
    model: document.getElementById('model').value,
    venue: strOrNull('venue'),
    bat_team: strOrNull('bat_team'),
    bowl_team: strOrNull('bowl_team'),
    is_womens:  document.getElementById('is_womens').checked ? 1 : 0,
    is_weekend: document.getElementById('is_weekend').checked ? 1 : 0,
    inn1_total_score:    numOrNull('inn1_total_score'),
    inn1_total_wickets:  numOrNull('inn1_total_wickets'),
    inn1_pp_score:       numOrNull('inn1_pp_score'),
    inn1_pp_wickets:     numOrNull('inn1_pp_wickets'),
    inn1_mid_score:      numOrNull('inn1_mid_score'),
    inn1_mid_wickets:    numOrNull('inn1_mid_wickets'),
    inn1_ov1_bat_odds:   numOrNull('inn1_ov1_bat_odds'),
    inn1_pp_bat_odds:    numOrNull('inn1_pp_bat_odds'),
    inn1_last_bat_odds:  numOrNull('inn1_last_bat_odds'),
    inn2_score:    numOrNull('inn2_score'),
    inn2_wickets:  numOrNull('inn2_wickets'),
    inn2_bat_odds: numOrNull('inn2_bat_odds'),
  }};

  document.getElementById('spinner').style.display = 'block';
  document.getElementById('result-box').style.display = 'none';
  document.getElementById('error-box').style.display = 'none';

  fetch('/api/ml/win-predictor/whatif', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify(body),
  }})
  .then(r => r.json())
  .then(d => {{
    document.getElementById('spinner').style.display = 'none';
    if (!d.ok) {{
      const eb = document.getElementById('error-box');
      eb.textContent = 'Error: ' + (d.error || 'Unknown');
      eb.style.display = 'block';
      return;
    }}
    const pChase  = Math.round(d.prob_chase_wins * 100);
    const pDefend = Math.round(d.prob_defends * 100);
    document.getElementById('bar-chase').style.width  = pChase  + '%';
    document.getElementById('bar-defend').style.width = pDefend + '%';
    document.getElementById('bar-chase').textContent  = 'Chase wins: '+ pChase  + '%';
    document.getElementById('bar-defend').textContent = 'Defends: '   + pDefend + '%';
    document.getElementById('result-model').textContent = 'Model: ' + d.model + ' · ' + d.feature_count + ' features';
    const rows = (d.features_used || []).map((f, i) =>
      `<tr>
         <td style="color:#bbb">${{i+1}}</td>
         <td>${{f.name}}</td>
         <td>${{f.value}}</td>
         <td class="src-${{f.source}}">${{f.source}}</td>
       </tr>`
    ).join('');
    document.getElementById('feat-rows').innerHTML = rows;
    document.getElementById('result-box').style.display = 'block';
  }})
  .catch(e => {{
    document.getElementById('spinner').style.display = 'none';
    const eb = document.getElementById('error-box');
    eb.textContent = 'Network error: ' + e;
    eb.style.display = 'block';
  }});
}}
</script>
</body>
</html>"""

    return func.HttpResponse(html, mimetype="text/html")
