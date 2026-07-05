from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    download_json, get_named_container_client, adf_activity_badge,
)

_CONFIG_BLOB   = "ml/train_config.json"
_METADATA_BLOB = "ml/over_under_model_metadata.json"
_PREDS_BLOB    = "ml/over_under_match_predictions.json"

# ── Market definitions ────────────────────────────────────────────────────────
_MARKETS = [
    {
        "slug":        "inn1-first6",
        "title":       "1st Innings First 6 Overs Runs",
        "subtitle":    "Predict whether inn1 will score OVER or UNDER the live betting line at 6 overs",
        "market_key":  "first_6_overs",
        "innings":     1,
        "target_over": 6,
        "live":        True,
    },
    {
        "slug":        "inn1-total",
        "title":       "1st Innings Total Runs",
        "subtitle":    "Predict whether inn1 full total will go OVER or UNDER the innings total market line",
        "market_key":  "innings_total",
        "innings":     1,
        "target_over": None,
        "live":        True,
    },
    {
        "slug":        "inn1-first12",
        "title":       "1st Innings First 12 Overs Runs",
        "subtitle":    "Predict whether inn1 will score OVER or UNDER the live betting line at 12 overs",
        "market_key":  "first_12_overs",
        "innings":     1,
        "target_over": 12,
        "live":        True,
    },
    {
        "slug":        "inn2-first6",
        "title":       "2nd Innings First 6 Overs Runs",
        "subtitle":    "Predict whether inn2 will score OVER or UNDER the live betting line at 6 overs",
        "market_key":  "inn2_first_6_overs",
        "innings":     2,
        "target_over": 6,
        "live":        False,
    },
    {
        "slug":        "inn2-first12",
        "title":       "2nd Innings First 12 Overs Runs",
        "subtitle":    "Predict whether inn2 will score OVER or UNDER the live betting line at 12 overs",
        "market_key":  "inn2_first_12_overs",
        "innings":     2,
        "target_over": 12,
        "live":        False,
    },
]

_SLUG_TO_MARKET = {m["slug"]: m for m in _MARKETS}

# ── Shared helpers ────────────────────────────────────────────────────────────

_NAV = """<nav>
  <a href="/api/home">Home</a>
  <a href="/api/live/view" style="color:#c00;font-weight:bold;">🔴 Live</a>
  <a href="/api/ml/over-under">T20 Over/Under Runs</a>
  <a href="/api/ml/win-predictor">ML Win Predictor</a>
  <a href="/api/ml/glossary">Glossary</a>
</nav>"""

_NAV_STYLE = """
nav { margin-bottom:24px; font-size:14px; }
nav a { color:#0066cc; text-decoration:none; margin-right:16px; }
nav a:hover { text-decoration:underline; }
"""

_CUTOFF_SCRIPT = """
function saveCutoff() {
    var d = document.getElementById('cutoffInput').value;
    var s = document.getElementById('cutoffStatus');
    if (!d) { s.style.color='#c00'; s.textContent='Pick a date first.'; return; }
    s.style.color='#888'; s.textContent='Saving…';
    fetch('/api/ml/over-under/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({train_cutoff_date: d})
    })
    .then(r => r.json())
    .then(j => {
        if (j.ok) { s.style.color='#2d7a2d'; s.textContent='Saved ✓ — re-run pl_ml_and_hypothesis in ADF to apply.'; }
        else       { s.style.color='#c00';    s.textContent='Error: ' + (j.error || 'unknown'); }
    })
    .catch(e => { s.style.color='#c00'; s.textContent='Network error: ' + e; });
}
"""


def _load_config(gold) -> dict:
    try:
        return json.loads(gold.get_blob_client(_CONFIG_BLOB).download_blob().readall())
    except Exception:
        return {}


def _result_color(result):
    return "#2d7a2d" if result == "over" else "#cc2200"


def _tick(correct):
    return ('<span style="color:#2d7a2d;font-weight:bold;font-size:16px">✓</span>'
            if correct else
            '<span style="color:#cc2200;font-weight:bold;font-size:16px">✗</span>')


def _prob_bar(prob_over):
    w_over = int(prob_over * 80)
    w_under = 80 - w_over
    return (f'<div style="display:inline-flex;width:80px;height:10px;border-radius:3px;overflow:hidden;vertical-align:middle">'
            f'<div style="width:{w_over}px;background:#2d7a2d"></div>'
            f'<div style="width:{w_under}px;background:#cc2200"></div>'
            f'</div> <span style="font-size:11px;color:#555">{prob_over:.0%}</span>')


def _pred_table(rows, target_over):
    """Render a per-match prediction table for one checkpoint."""
    if not rows:
        return "<p style='color:#999;margin:8px 0'>No data for this checkpoint.</p>"

    target_label = f"Actual @ Ov {target_over}" if target_over else "Actual Total"

    html = f"""<div style="overflow-x:auto">
<table class="pred-table">
<thead><tr>
  <th>Event ID</th>
  <th>Match</th>
  <th>Date</th>
  <th>League</th>
  <th>G</th>
  <th>Inn1 Batting</th>
  <th>Score @ CP</th>
  <th>Wkts</th>
  <th>Market Line</th>
  <th>{escape(target_label)}</th>
  <th>Actual</th>
  <th>Predicted</th>
  <th>Prob Over</th>
  <th></th>
</tr></thead>
<tbody>"""

    for p in sorted(rows, key=lambda r: r.get("match_date") or "", reverse=True):
        eid       = escape(str(p.get("event_id") or ""))
        match     = escape(str(p.get("match_name") or ""))
        date      = escape(str(p.get("match_date") or "")[:10])
        league    = escape(str(p.get("league_name") or ""))
        gender    = escape(str(p.get("gender") or "M"))
        batting   = escape(str(p.get("batting_team") or ""))
        score_cp  = p.get("score_at_cp") or "—"
        wkts      = p.get("wickets_at_cp") or "—"
        line      = p.get("betting_line")
        actual_v  = p.get("actual_value")
        actual_r  = str(p.get("actual_result") or "")
        predicted = str(p.get("predicted") or "")
        prob      = _to_float_safe(p.get("prob_over"))
        correct   = p.get("correct", False)

        line_str   = f"{float(line):.1f}" if line else "—"
        actual_str = str(int(float(actual_v))) if actual_v else "—"
        a_col      = _result_color(actual_r)
        p_col      = _result_color(predicted)

        gender_badge = (
            '<span style="background:#dbeafe;color:#1e40af;font-size:10px;padding:1px 5px;border-radius:3px">W</span>'
            if gender == "W" else
            '<span style="background:#dcfce7;color:#15803d;font-size:10px;padding:1px 5px;border-radius:3px">M</span>'
        )

        # data-league and data-gender enable JS filtering
        html += f"""<tr data-league="{league}" data-gender="{gender}">
  <td style="font-family:monospace;font-size:12px;color:#888">{eid}</td>
  <td>{match}</td>
  <td style="color:#666;white-space:nowrap">{date}</td>
  <td style="font-size:12px;color:#555">{league}</td>
  <td style="text-align:center">{gender_badge}</td>
  <td style="font-size:13px">{batting}</td>
  <td style="font-family:monospace;text-align:center">{score_cp}</td>
  <td style="text-align:center">{wkts}</td>
  <td style="font-family:monospace;font-weight:bold;text-align:center">{line_str}</td>
  <td style="font-family:monospace;text-align:center">{actual_str}</td>
  <td style="color:{a_col};font-weight:bold;text-align:center">{escape(actual_r.upper())}</td>
  <td style="color:{p_col};font-weight:bold;text-align:center">{escape(predicted.upper())}</td>
  <td>{_prob_bar(prob) if prob is not None else '—'}</td>
  <td style="text-align:center">{_tick(correct)}</td>
</tr>"""

    html += "</tbody></table></div>"
    return html


def _to_float_safe(v):
    try:
        return float(v)
    except Exception:
        return None


def _accuracy(rows):
    if not rows:
        return None
    n_correct = sum(1 for r in rows if r.get("correct"))
    return n_correct / len(rows)


def _acc_color(v):
    if v is None:
        return "#999"
    if v >= 0.70:
        return "#2d7a2d"
    if v >= 0.60:
        return "#cc7700"
    return "#cc2200"


_FEATURE_GLOSSARY = {
    "score":                  ("Current score at this checkpoint", "e.g. 112 runs at over 12"),
    "wickets_in_hand":        ("Wickets still available (10 − fallen)", "e.g. 7 if 3 wickets lost"),
    "betting_line":           ("Live market total — the number to beat", "e.g. 168.5 means market predicts ~169 total"),
    "score_vs_line_pace":     ("How far ahead/behind expected pace: score − (line × balls_done / total_balls)", "e.g. +12 = 12 runs ahead of implied pace right now"),
    "run_rate":               ("Runs per over so far (score × 6 / balls)", "e.g. 8.2 rpo after 12 overs"),
    "rr_required":            ("Run rate needed from here to exactly hit the betting line", "e.g. 9.4 rpo needed in last 8 overs"),
    "implied_prob_over":      ("Market's implied probability of OVER (1 / over_odds)", "e.g. 0.48 from odds of 2.08"),
    "batting_team_win_odds":  ("Live decimal win odds for the batting team — captures match context beyond runs", "e.g. 1.45 = strong favourite; 3.2 = underdog"),
    "is_weekend_match":       ("Whether the match was played on a Saturday or Sunday (1) vs a weekday (0)", "e.g. 1 = Saturday fixture — different crowd/intent dynamics than a weekday game"),
    "line_ov1":               ("Betting line at the very first over — the pre-innings market view", "e.g. 163.5 set before any balls bowled"),
    "line_drift_total":       ("Total change in betting line from over 1 to this checkpoint", "e.g. +18.0 = market moved the line up 18 runs across the innings"),
    "line_trend_slope":       ("Linear slope of betting line across all overs — how fast it is rising or falling (runs per over)", "e.g. +2.1 = line rising ~2 runs every over on average"),
    "pct_overs_line_up":      ("Fraction of overs where the market's line went UP (0–1)", "e.g. 0.83 = line rose in 5 of 6 overs — consistent market upward pressure"),
    "max_line_jump":          ("Biggest single-over rise in the betting line", "e.g. 8.0 = one over caused the market to jump 8 runs at once"),
    "line_accel":             ("Is the line rising faster in the second half than the first? (2nd-half drift − 1st-half drift)", "e.g. +5.0 = accelerating; −3.0 = early burst then flat"),
    "score_vs_pace_at_ov2":   ("How far ahead/behind the line's implied pace the team was after just 2 overs", "e.g. +4.5 = already 4.5 runs ahead of implied pace at over 2"),
    "score_vs_pace_trend":    ("Linear slope of score_vs_pace across all overs — consistently pulling ahead or falling behind the line?", "e.g. +0.8 = gaining ~0.8 runs/over advantage vs the line every over"),
    "recent_rr_2":            ("Average runs per over in the last 2 overs — current scoring momentum", "e.g. 11.5 = last 2 overs went for 23 runs total"),
    "recent_rr_4":            ("Average runs per over in the last 4 overs — medium-term momentum", "e.g. 9.25 = last 4 overs averaged 9.25 rpo"),
    "rr_trend":               ("Recent momentum vs first-half average (recent_rr_2 − first-half avg rpo)", "e.g. +3.2 = scoring 3.2 more rpo recently than earlier in innings"),
    "max_over_runs":          ("Most runs scored in any single over up to this point", "e.g. 18 = one over produced 18 runs"),
    "min_over_runs":          ("Fewest runs in any single over — captures quietest/dot-ball phase", "e.g. 2 = one over went for only 2 runs"),
    "first_wkt_over":         ("Over number when the first wicket fell (= checkpoint + 1 if none yet)", "e.g. 14 = team batted 13+ overs without losing a wicket"),
    "pp_score":               ("Score at end of powerplay (over 6) — always available for checkpoints ≥ 6", "e.g. 58 at end of over 6"),
    "pp_wickets":             ("Wickets lost in powerplay overs 1–6 — always available for checkpoints ≥ 6", "e.g. 1 = strong start with only 1 powerplay wicket"),
    "checkpoint_over":        ("Which over this row represents — pooled model position feature", "e.g. 8, 12, 16"),
    "balls_remaining":        ("Balls left to bowl — pooled model position feature", "e.g. 24 = 4 overs left"),
    "balls_completed":        ("Balls bowled so far — pooled model position feature", "e.g. 96 = 16 overs done"),
    "venue_enc":              ("Historical OVER-rate for this venue (smoothed toward the global average for venues seen only a few times)", "e.g. 0.65 = at this ground, 65% of historical innings went OVER the line — a high-scoring ground"),
    "league_enc":             ("Historical OVER-rate for this league/tournament", "e.g. 0.40 = in this league, only 40% of innings go OVER — generally lower-scoring or tighter lines"),
    "gender_enc":             ("Historical OVER-rate for this gender category (men's vs women's cricket)", "e.g. 0.45 = women's matches in the data go OVER slightly less often than men's"),
    "batting_team_enc":       ("Historical OVER-rate for this specific batting team across past innings", "e.g. 0.70 = this team has gone OVER the line in 70% of their past innings — aggressive/high-scoring side"),
    "bowling_team_enc":       ("Historical OVER-rate for innings bowled against this specific team", "e.g. 0.30 = teams batting against this bowling side go OVER only 30% of the time — strong bowling attack"),
}

_PER_OVER_GLOSSARY = {
    "runs":    ("Runs scored in over {k} only (incremental, not cumulative)", "e.g. ov8_runs=14 means over 8 went for 14 runs"),
    "cumwkts": ("Cumulative wickets lost up to end of over {k}", "e.g. ov8_cumwkts=2 means 2 wickets by end of over 8"),
    "line":    ("Betting line at the end of over {k} — snapshot of the market at that moment", "e.g. ov8_line=172.5 means market had set 172.5 at over 8"),
    "vs_pace": ("score_vs_line_pace at over {k}: how far ahead/behind at that point in history", "e.g. ov8_vs_pace=+9.2 means team was 9 runs ahead of implied pace at over 8"),
}


def _feature_desc(name: str):
    """Return (description, example) for a feature name, or ('', '')."""
    import re
    if name in _FEATURE_GLOSSARY:
        return _FEATURE_GLOSSARY[name]
    m = re.fullmatch(r"ov(\d+)_(runs|cumwkts|line|vs_pace)", name)
    if m:
        k, suf = m.group(1), m.group(2)
        td, te = _PER_OVER_GLOSSARY[suf]
        return td.replace("{k}", k), te
    return "", ""


def _feature_available_at_cp(feat: str, cp: int) -> bool:
    """
    True if `feat` could actually be known at checkpoint `cp` overs into the innings.
    The pooled model is trained across all checkpoints combined, so its importance
    dict includes later-over features (e.g. ov14_runs) that are NaN/unknowable for
    an early checkpoint like Over 10 — those must be filtered out before display.
    """
    import re
    m = re.fullmatch(r"ov(\d+)_(runs|cumwkts|line|vs_pace)", feat)
    if m:
        return int(m.group(1)) <= cp
    if feat == "recent_rr_4":
        return cp >= 4
    if feat in ("pp_score", "pp_wickets", "rr_trend"):
        return cp >= 6
    return True  # always-available: score, betting_line, line_drift_total, etc.


def _feature_importance_for_model(meta_doc: Optional[dict], mkt_key: str, model_used: str, cp: int):
    """
    Find the metadata entry for the specific model actually used at this
    checkpoint (per-checkpoint model, or pooled fallback if CV-AUC was too low).
    Returns (feature_importance_dict, source_label) or (None, "").
    """
    if not meta_doc:
        return None, ""
    models = meta_doc.get("models") or []
    mkt_models = [m for m in models if m.get("market") == mkt_key]
    if not mkt_models:
        return None, ""

    if model_used.endswith("_pooled"):
        match = next((m for m in mkt_models if str(m.get("checkpoint_over")) == "pooled"), None)
        label = (f"Pooled model — this checkpoint's own CV-AUC was below the 0.60 threshold, so predictions "
                  "fall back to the all-checkpoints pooled model (filtered to features known by this over)")
    else:
        try:
            cp_num = model_used.rsplit("_cp", 1)[1]
        except Exception:
            cp_num = None
        match = next((m for m in mkt_models if str(m.get("checkpoint_over")) == str(cp_num)), None)
        label = f"Model trained specifically on Over {cp_num} data — features reflect everything known up to that point"

    if not match:
        return None, ""

    imp_raw = match.get("feature_importance") or {}
    if model_used.endswith("_pooled"):
        imp_raw = {f: v for f, v in imp_raw.items() if _feature_available_at_cp(f, cp)}
    return imp_raw, label


def _feature_importance_html(imp_raw: Optional[dict], source_label: str) -> str:
    """Render a feature importance table with plain-English descriptions."""
    if not imp_raw:
        return "<p style='color:#999;font-size:13px'>No feature importance data available for this checkpoint's model.</p>"

    total  = sum(imp_raw.values()) or 1
    ranked = sorted(imp_raw.items(), key=lambda x: -x[1])

    rows_html = ""
    for rank, (feat, val) in enumerate(ranked, 1):
        pct   = val / total * 100
        bar_w = min(int(pct * 2.0), 200)
        desc, example = _feature_desc(feat)
        sub_html = ""
        if desc:
            sub_html += f'<div style="font-size:11px;color:#555;margin-top:2px">{escape(desc)}</div>'
        if example:
            sub_html += f'<div style="font-size:11px;color:#2563eb;margin-top:1px;font-style:italic">{escape(example)}</div>'

        rows_html += f"""<tr style="border-bottom:1px solid #f3f4f6">
  <td style="text-align:center;color:#aaa;width:36px;padding:8px 4px;font-size:12px">{rank}</td>
  <td style="padding:8px 10px">
    <div style="font-family:monospace;font-size:13px;font-weight:600;color:#1e3a5f">{escape(feat)}</div>
    {sub_html}
  </td>
  <td style="text-align:right;font-family:monospace;width:52px;padding:8px 8px;font-weight:bold;color:#374151">{pct:.1f}%</td>
  <td style="width:210px;padding:8px 10px">
    <div style="background:#e5e7eb;border-radius:3px;height:10px;overflow:hidden">
      <div style="width:{bar_w}px;height:10px;background:#1e40af;border-radius:3px"></div>
    </div>
  </td>
</tr>"""

    return f"""<div style="background:white;border:1px solid #e5e7eb;border-radius:8px;padding:20px;margin-bottom:24px;box-shadow:0 1px 4px #ccc">
  <h3 style="margin:0 0 4px;font-size:15px;color:#111">Feature Importance</h3>
  <p style="margin:0 0 16px;font-size:12px;color:#777">{escape(source_label)} · {len(ranked)} features with non-zero importance</p>
  <div style="overflow-y:auto;max-height:520px">
  <table style="border-collapse:collapse;width:100%">
  <thead>
    <tr style="border-bottom:2px solid #e5e7eb;background:#f9fafb">
      <th style="text-align:center;padding:8px 4px;color:#6b7280;font-weight:600;font-size:12px">#</th>
      <th style="text-align:left;padding:8px 10px;color:#6b7280;font-weight:600;font-size:12px">Feature &amp; meaning</th>
      <th style="text-align:right;padding:8px 8px;color:#6b7280;font-weight:600;font-size:12px">% share</th>
      <th style="padding:8px 10px;color:#6b7280;font-weight:600;font-size:12px">Importance</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
  </table>
  </div>
</div>"""


# ── CONFIG GET ────────────────────────────────────────────────────────────────

def view_ml_over_under_config_get(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    cfg = _load_config(gold)
    return func.HttpResponse(
        json.dumps({"train_cutoff_date": cfg.get("train_cutoff_date", "")}),
        mimetype="application/json",
    )


# ── CONFIG POST ───────────────────────────────────────────────────────────────

def view_ml_over_under_config_post(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    try:
        body   = req.get_json()
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


# ── PARENT PAGE ───────────────────────────────────────────────────────────────

def view_ml_over_under_html(req: func.HttpRequest) -> func.HttpResponse:
    gold         = get_named_container_client("gold")
    config       = _load_config(gold)
    saved_cutoff = config.get("train_cutoff_date", "")

    try:
        meta = json.loads(gold.get_blob_client(_METADATA_BLOB).download_blob().readall())
    except Exception:
        meta = None

    try:
        preds = json.loads(gold.get_blob_client(_PREDS_BLOB).download_blob().readall())
    except Exception:
        preds = None

    trained_at = (meta or {}).get("trained_at_utc", "")[:19].replace("T", " ") if meta else "—"
    n_train    = (preds or {}).get("n_train_rows", "—") if preds else "—"
    n_test     = (preds or {}).get("n_test_rows",  "—") if preds else "—"

    cards = ""
    for m in _MARKETS:
        live_badge = "" if m["live"] else '<span style="background:#e5e7eb;color:#6b7280;font-size:11px;padding:2px 8px;border-radius:999px;margin-left:8px">Coming soon</span>'
        # Count test predictions for live markets
        test_n = ""
        if m["live"] and preds:
            mkt_data = (preds.get("markets") or {}).get(m["market_key"], {})
            total_test = sum(
                cp_data.get("n_test", 0)
                for cp_data in (mkt_data.get("checkpoints") or {}).values()
            )
            if total_test:
                test_n = f'<div style="font-size:12px;color:#2d7a2d;margin-top:4px">{total_test} test predictions</div>'

        link = f"/api/ml/over-under/{m['slug']}"
        if m["live"]:
            cards += f"""<a href="{link}" class="market-card live">
                <div class="mc-title">{escape(m['title'])}{live_badge}</div>
                <div class="mc-sub">{escape(m['subtitle'])}</div>
                {test_n}
            </a>"""
        else:
            cards += f"""<div class="market-card">
                <div class="mc-title">{escape(m['title'])}{live_badge}</div>
                <div class="mc-sub">{escape(m['subtitle'])}</div>
            </div>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>T20 Over/Under Runs Model</title>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, sans-serif; background:#f5f5f5; margin:0; padding:30px; }}
    h1 {{ margin-bottom:4px; }}
    h2 {{ margin-top:32px; margin-bottom:12px; border-bottom:2px solid #ddd; padding-bottom:6px; }}
    {_NAV_STYLE}
    .meta-grid {{ display:flex; gap:16px; flex-wrap:wrap; margin:20px 0; }}
    .meta-box {{ background:white; padding:16px 20px; border-radius:8px; box-shadow:0 1px 4px #ccc; min-width:150px; }}
    .meta-label {{ font-size:12px; color:#888; margin-bottom:4px; }}
    .meta-val {{ font-size:16px; font-weight:bold; color:#222; }}
    .market-cards {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:16px; margin-top:16px; }}
    .market-card {{ background:white; border:1px solid #e0e0e0; border-radius:10px; padding:20px; box-shadow:0 1px 4px #ccc; }}
    a.market-card {{ text-decoration:none; color:inherit; display:block; transition:box-shadow 0.15s; }}
    a.market-card:hover {{ box-shadow:0 4px 16px #bbb; border-color:#2563eb; }}
    a.market-card.live {{ border-left:4px solid #2563eb; }}
    .mc-title {{ font-size:15px; font-weight:bold; color:#1e40af; margin-bottom:6px; }}
    .mc-sub {{ font-size:13px; color:#555; line-height:1.5; }}
    .cutoff-card {{ background:white; border:1px solid #e0e0e0; border-radius:8px; padding:16px 20px;
                    margin-bottom:24px; box-shadow:0 1px 4px #ccc; display:flex;
                    align-items:center; gap:16px; flex-wrap:wrap; }}
    .cutoff-label {{ font-size:13px; color:#555; }}
    .cutoff-input {{ font-size:14px; padding:6px 10px; border:1px solid #ccc; border-radius:4px; font-family:monospace; }}
    .cutoff-btn {{ background:#0066cc; color:white; border:none; border-radius:4px; padding:7px 18px; font-size:14px; cursor:pointer; }}
    .cutoff-btn:hover {{ background:#0052a3; }}
    .cutoff-status {{ font-size:13px; }}
  </style>
</head>
<body>
  {_NAV}
  <h1>T20 Over/Under Runs Model</h1>
  {adf_activity_badge("MlOverUnderFeatureExtraction", "MlOverUnderModelTraining")}
  <p style="color:#666">LightGBM classifiers predicting Over/Under outcomes for 5 cricket betting markets. Click a market to see per-match predictions.</p>

  <div class="meta-grid">
    <div class="meta-box"><div class="meta-label">Last trained</div><div class="meta-val">{escape(trained_at)}</div></div>
    <div class="meta-box"><div class="meta-label">Training rows</div><div class="meta-val">{n_train}</div></div>
    <div class="meta-box"><div class="meta-label">Test rows</div><div class="meta-val">{n_test}</div></div>
  </div>

  <div class="cutoff-card">
    <div class="cutoff-label">
      <strong>Train / test cutoff date</strong><br>
      Matches on or before this date → training. After → held-out test.<br>
      Save here, then run <code>pl_ml_and_hypothesis</code> in ADF.
    </div>
    <input type="date" id="cutoffInput" class="cutoff-input" value="{escape(saved_cutoff)}">
    <button class="cutoff-btn" onclick="saveCutoff()">Save</button>
    <span id="cutoffStatus" class="cutoff-status"></span>
  </div>

  <h2>Markets</h2>
  <div class="market-cards">{cards}</div>

  <script>{_CUTOFF_SCRIPT}</script>
</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")


# ── MARKET SUB-PAGES ──────────────────────────────────────────────────────────

def _common_page_style():
    return f"""
    body {{ font-family:Arial,sans-serif; background:#f5f5f5; margin:0; padding:30px; }}
    h1 {{ margin-bottom:4px; }}
    h2 {{ margin-top:32px; margin-bottom:12px; border-bottom:2px solid #ddd; padding-bottom:6px; }}
    h3 {{ margin-top:0; margin-bottom:8px; color:#333; font-size:15px; }}
    {_NAV_STYLE}
    .meta-grid {{ display:flex; gap:16px; flex-wrap:wrap; margin:20px 0; }}
    .meta-box {{ background:white; padding:14px 18px; border-radius:8px; box-shadow:0 1px 4px #ccc; min-width:130px; }}
    .meta-label {{ font-size:12px; color:#888; margin-bottom:3px; }}
    .meta-val {{ font-size:15px; font-weight:bold; color:#222; }}
    .filter-bar {{ background:white; border:1px solid #e5e7eb; border-radius:8px; padding:14px 18px;
                   margin-bottom:20px; box-shadow:0 1px 4px #ccc; display:flex; gap:20px;
                   align-items:center; flex-wrap:wrap; }}
    .filter-label {{ font-size:12px; color:#777; font-weight:600; text-transform:uppercase;
                     letter-spacing:0.04em; white-space:nowrap; }}
    .filter-select {{ font-size:13px; padding:5px 10px; border:1px solid #d1d5db;
                      border-radius:5px; background:white; cursor:pointer; }}
    .gender-btn {{ padding:5px 14px; border-radius:5px; border:1px solid #d1d5db; cursor:pointer;
                   font-size:13px; background:white; font-weight:bold; transition:all 0.1s; }}
    .gender-btn.active-M {{ background:#dcfce7; color:#15803d; border-color:#15803d; }}
    .gender-btn.active-W {{ background:#dbeafe; color:#1e40af; border-color:#1e40af; }}
    .gender-btn.active-All {{ background:#374151; color:white; border-color:#374151; }}
    .cp-tabs {{ display:flex; gap:8px; flex-wrap:wrap; margin-bottom:16px; }}
    .cp-tab {{ padding:6px 16px; border-radius:6px; border:1px solid #d1d5db; cursor:pointer;
               font-size:13px; background:white; }}
    .cp-tab.active {{ background:#1e40af; color:white; border-color:#1e40af; }}
    .cp-section {{ display:none; }}
    .cp-section.active {{ display:block; }}
    .split-tabs {{ display:flex; gap:6px; margin-bottom:12px; }}
    .split-tab {{ padding:4px 14px; border-radius:5px; border:1px solid #d1d5db; cursor:pointer;
                  font-size:12px; background:white; }}
    .split-tab.active {{ background:#374151; color:white; border-color:#374151; }}
    .split-section {{ display:none; }}
    .split-section.active {{ display:block; }}
    .pred-table {{ border-collapse:collapse; width:100%; background:white; border-radius:8px;
                   overflow:hidden; box-shadow:0 1px 4px #ccc; margin-bottom:12px; font-size:13px; }}
    .pred-table th {{ background:#333; color:white; padding:9px 12px; text-align:left; white-space:nowrap; }}
    .pred-table td {{ padding:9px 12px; border-bottom:1px solid #eee; }}
    .pred-table tr:last-child td {{ border-bottom:none; }}
    .pred-table tr:hover td {{ background:#f9f9f9; }}
    .pred-table tr.filtered-out {{ display:none; }}
    .acc-badge {{ display:inline-block; padding:3px 10px; border-radius:999px; font-size:12px;
                  font-weight:bold; color:white; margin-left:8px; vertical-align:middle; }}
    .hint {{ color:#777; font-size:12px; margin-top:6px; }}
    .no-results-msg {{ display:none; color:#999; font-size:13px; padding:12px 0; }}
    """


def _collect_filter_options(preds_doc: dict, mkt_key: str):
    """Return (sorted leagues list, genders set) across all predictions for this market."""
    leagues = set()
    genders = set()
    mkt_data = (preds_doc.get("markets") or {}).get(mkt_key) or {}
    for cp_data in (mkt_data.get("checkpoints") or {}).values():
        for split in ("train", "test"):
            for row in (cp_data.get(split) or []):
                lg = str(row.get("league_name") or "").strip()
                gd = str(row.get("gender") or "M").strip()
                if lg:
                    leagues.add(lg)
                genders.add(gd)
    return sorted(leagues), genders


def _filter_bar_html(leagues: list, genders: set) -> str:
    league_options = '<option value="">All Leagues</option>'
    for lg in leagues:
        league_options += f'<option value="{escape(lg)}">{escape(lg)}</option>'

    has_women = "W" in genders
    has_men   = "M" in genders

    gender_btns = '<button class="gender-btn active-All" id="gbtn-All" onclick="setGender(\'All\')">All</button>'
    if has_men:
        gender_btns += '<button class="gender-btn" id="gbtn-M" onclick="setGender(\'M\')">M</button>'
    if has_women:
        gender_btns += '<button class="gender-btn" id="gbtn-W" onclick="setGender(\'W\')">W</button>'

    return f"""<div class="filter-bar">
  <div>
    <span class="filter-label">League</span><br style="margin-bottom:4px">
    <select class="filter-select" id="leagueFilter" onchange="applyFilters()">
      {league_options}
    </select>
  </div>
  <div>
    <span class="filter-label">Gender</span><br style="margin-bottom:4px">
    <div style="display:flex;gap:6px">{gender_btns}</div>
  </div>
  <div style="font-size:12px;color:#999;margin-left:auto" id="filterStatus"></div>
</div>"""


_FILTER_SCRIPT = """
var _activeGender = 'All';

function setGender(g) {
    _activeGender = g;
    document.querySelectorAll('.gender-btn').forEach(function(b) {
        b.className = 'gender-btn';
    });
    var btn = document.getElementById('gbtn-' + g);
    if (btn) btn.classList.add('active-' + g);
    applyFilters();
}

function applyFilters() {
    var league = document.getElementById('leagueFilter').value;
    var gender = _activeGender;
    var total = 0, visible = 0;
    document.querySelectorAll('.pred-table tbody tr').forEach(function(row) {
        var rowLeague = row.getAttribute('data-league') || '';
        var rowGender = row.getAttribute('data-gender') || 'M';
        var leagueOk = !league || rowLeague === league;
        var genderOk = gender === 'All' || rowGender === gender;
        total++;
        if (leagueOk && genderOk) {
            row.classList.remove('filtered-out');
            visible++;
        } else {
            row.classList.add('filtered-out');
        }
    });
    var status = document.getElementById('filterStatus');
    if (status) {
        if (total > 0 && visible < total) {
            status.textContent = 'Showing ' + visible + ' of ' + total + ' rows';
        } else {
            status.textContent = '';
        }
    }
}
"""


def _render_market_page(market_def: dict, preds_doc: Optional[dict], meta_doc: Optional[dict] = None) -> str:
    title    = market_def["title"]
    subtitle = market_def["subtitle"]
    mkt_key  = market_def["market_key"]
    target_over = market_def.get("target_over")

    if preds_doc is None:
        body = """<div style="background:#fff3cd;border-left:4px solid #f0a000;padding:16px 20px;
                  border-radius:4px;margin:20px 0;font-size:14px;color:#555;">
            No model results found.<br>Run <strong>pl_ml_and_hypothesis</strong> in ADF to train models.
        </div>"""
        return body

    mkt_data    = (preds_doc.get("markets") or {}).get(mkt_key) or {}
    checkpoints = mkt_data.get("checkpoints") or {}

    if not checkpoints:
        return f"<p style='color:#c00'>No predictions found for market <code>{escape(mkt_key)}</code>. Check that the model was trained.</p>"

    # Collect filter options from all rows
    leagues, genders = _collect_filter_options(preds_doc, mkt_key)
    filter_bar = _filter_bar_html(leagues, genders)

    # Sort checkpoints numerically
    sorted_cps = sorted(checkpoints.keys(), key=lambda x: int(x))

    # Build tabs + sections
    tabs_html     = ""
    sections_html = ""

    for i, cp in enumerate(sorted_cps):
        cp_data     = checkpoints[cp]
        model_used  = cp_data.get("model_used", "?")
        cv_auc      = cp_data.get("cv_auc", 0)
        n_train     = cp_data.get("n_train", 0)
        n_test      = cp_data.get("n_test",  0)
        train_rows  = cp_data.get("train", [])
        test_rows_d = cp_data.get("test",  [])

        train_acc = _accuracy(train_rows)
        test_acc  = _accuracy(test_rows_d)

        active_cls = "active" if i == 0 else ""
        section_id = f"cp_{cp}"
        tab_id     = f"tab_{cp}"

        tabs_html += f'<button class="cp-tab {active_cls}" id="{tab_id}" onclick="showCp(\'{cp}\')"">Over {cp}</button>'

        cp_imp_raw, cp_imp_label = _feature_importance_for_model(meta_doc, mkt_key, model_used, int(cp))

        sections_html += f"""<div class="cp-section {active_cls}" id="{section_id}">
  <div style="background:white;border:1px solid #e5e7eb;border-radius:8px;padding:16px 20px;margin-bottom:16px;">
    <div style="display:flex;gap:24px;flex-wrap:wrap;font-size:13px;color:#555">
      <div><strong>Checkpoint:</strong> After over {cp}</div>
      <div><strong>Model:</strong> <code>{escape(model_used)}</code></div>
      <div><strong>CV-AUC:</strong> <span style="color:{_acc_color(cv_auc) if cv_auc else '#999'};font-weight:bold">{cv_auc:.3f}</span></div>
      {'<div><strong>Train accuracy:</strong> ' + (f'<span style="color:{_acc_color(train_acc)};font-weight:bold">{train_acc:.0%}</span>' if train_acc is not None else '—') + '</div>' if train_rows else ''}
      {'<div><strong>Test accuracy:</strong> ' + (f'<span style="color:{_acc_color(test_acc)};font-weight:bold">{test_acc:.0%}</span>' if test_acc is not None else 'n/a') + '</div>'}
    </div>
  </div>

  <div class="split-tabs">
    <button class="split-tab active" onclick="showSplit('{cp}','test')">Test matches ({n_test})</button>
    <button class="split-tab" onclick="showSplit('{cp}','train')">Training matches ({n_train})</button>
  </div>
  <div class="split-section active" id="split_{cp}_test">
    <p class="hint" style="margin-bottom:8px">Matches <strong>after</strong> the cutoff — model never saw these during training.</p>
    {_pred_table(test_rows_d, target_over)}
  </div>
  <div class="split-section" id="split_{cp}_train">
    <p class="hint" style="margin-bottom:8px">Matches the model was <strong>trained on</strong> — high accuracy here is expected.</p>
    {_pred_table(train_rows, target_over)}
  </div>

  <h3 style="margin-top:24px">Feature Importance — Over {cp}</h3>
  {_feature_importance_html(cp_imp_raw, cp_imp_label)}
</div>"""

    body = f"""
<div class="meta-grid">
  <div class="meta-box"><div class="meta-label">Checkpoints</div><div class="meta-val">{len(sorted_cps)}</div></div>
  <div class="meta-box"><div class="meta-label">Train matches</div><div class="meta-val">{preds_doc.get('n_train_rows','—')}</div></div>
  <div class="meta-box"><div class="meta-label">Test matches</div><div class="meta-val">{preds_doc.get('n_test_rows','—')}</div></div>
</div>

<p style="color:#555;font-size:14px;margin-bottom:20px">{escape(subtitle)}</p>

{filter_bar}

<div class="cp-tabs">{tabs_html}</div>
{sections_html}

<script>
function showCp(cp) {{
  document.querySelectorAll('.cp-section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.cp-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('cp_' + cp).classList.add('active');
  document.getElementById('tab_' + cp).classList.add('active');
}}
function showSplit(cp, split) {{
  var sec = document.getElementById('cp_' + cp);
  sec.querySelectorAll('.split-section').forEach(s => s.classList.remove('active'));
  sec.querySelectorAll('.split-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('split_' + cp + '_' + split).classList.add('active');
  event.target.classList.add('active');
}}
{_FILTER_SCRIPT}
</script>"""
    return body


def _coming_soon_body(market_def):
    return f"""
<div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;padding:32px 24px;margin:24px 0;text-align:center">
  <div style="font-size:32px;margin-bottom:12px">🚧</div>
  <h2 style="border:none;margin:0 0 8px">Coming soon</h2>
  <p style="color:#555;max-width:480px;margin:0 auto">
    The <strong>{escape(market_def['title'])}</strong> model requires extracting
    betting lines from the <code>{escape(market_def['market_key'])}</code> market in silver active_markets data.
    This market data collection and model training will be added in a future pipeline run.
  </p>
</div>"""


def view_ml_over_under_market_html(req: func.HttpRequest) -> func.HttpResponse:
    slug = req.route_params.get("slug", "")
    market_def = _SLUG_TO_MARKET.get(slug)
    if not market_def:
        return func.HttpResponse(f"Unknown market slug: {escape(slug)}", status_code=404)

    gold = get_named_container_client("gold")

    if not market_def["live"]:
        body = _coming_soon_body(market_def)
    else:
        try:
            preds_doc = json.loads(gold.get_blob_client(_PREDS_BLOB).download_blob().readall())
        except Exception:
            preds_doc = None

        try:
            meta_doc = json.loads(gold.get_blob_client(_METADATA_BLOB).download_blob().readall())
        except Exception:
            meta_doc = None

        body = _render_market_page(market_def, preds_doc, meta_doc)

    # Breadcrumb nav links for sibling markets
    sibling_links = " &nbsp;|&nbsp; ".join(
        f'<a href="/api/ml/over-under/{m["slug"]}" style="{"font-weight:bold" if m["slug"]==slug else ""}">{escape(m["title"])}</a>'
        for m in _MARKETS
    )

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>{escape(market_def['title'])} — T20 Over/Under</title>
  <meta charset="utf-8">
  <style>{_common_page_style()}</style>
</head>
<body>
  {_NAV}
  <div style="font-size:12px;color:#888;margin-bottom:12px">{sibling_links}</div>
  <h1>{escape(market_def['title'])}</h1>
  {adf_activity_badge("MlOverUnderFeatureExtraction", "MlOverUnderModelTraining")}
  {body}
</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")
