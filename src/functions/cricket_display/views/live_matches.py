"""
Live Matches — /api/live/view

Shows every currently-live match with real-time ML predictions.
Data source: gold/cricket/inplay/live_index.json written by func-ramanuj-live-ml
every 60 seconds.

Auto-refreshes every 60 seconds so the page stays current without manual reload.
"""
from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    get_named_container_client,
    download_json,
)

_LIVE_INDEX_BLOB = "cricket/inplay/live_index.json"


def _prob_bar(pct_left: int, label_left: str, label_right: str,
              col_left: str, col_right: str) -> str:
    pct_right = 100 - pct_left
    def _half(pct, label, col):
        if pct < 10:
            return f'<div style="width:{pct}%;background:{col};"></div>'
        return (
            f'<div style="width:{pct}%;background:{col};color:white;font-size:11px;'
            f'font-weight:bold;display:flex;align-items:center;justify-content:center;">'
            f'{label} {pct}%</div>'
        )
    return (
        f'<div style="display:flex;height:26px;border-radius:5px;overflow:hidden;'
        f'min-width:160px;box-shadow:0 1px 3px #0005;">'
        + _half(pct_left, label_left, col_left)
        + _half(pct_right, label_right, col_right)
        + '</div>'
    )


def _ou_bar(prob_over: float) -> str:
    return _prob_bar(round(prob_over * 100), "OVER", "UNDER", "#1a6b3a", "#8b1a1a")


def _win_bar(prob_chase: float) -> str:
    return _prob_bar(round(prob_chase * 100), "Chase", "Defend", "#0052a3", "#5a2d00")


def _innings_badge(innings: Optional[int], over: Optional[float]) -> str:
    if innings is None:
        return '<span class="badge">—</span>'
    label = f"Inn {innings} · Ov {over}" if over is not None else f"Inn {innings}"
    col   = "#1a6b3a" if innings == 1 else "#0052a3"
    return f'<span class="badge" style="background:{col}">{escape(str(label))}</span>'


def _ou_summary_html(ou: Optional[Dict]) -> str:
    if not ou:
        return '<span style="color:#555;font-size:12px;">No O/U prediction yet</span>'
    market   = ou.get("market", "innings_total").replace("_", " ").title()
    cp       = ou.get("checkpoint_over", "?")
    line     = ou.get("betting_line")
    po       = ou.get("prob_over", 0.5)
    score    = ou.get("score")
    wkts     = ou.get("wickets", 0)
    score_str = f"{score}/{wkts}" if score is not None else "?"
    line_str  = f"Line {line}" if line is not None else "no line"
    bar       = _ou_bar(po)
    return (
        f'<div style="font-size:11px;color:#888;margin-bottom:4px;">'
        f'{escape(market)} · Over {cp} · Score {escape(score_str)} · {escape(line_str)}</div>'
        + bar
    )


def _win_summary_html(win: Optional[Dict]) -> str:
    if not win:
        return '<span style="color:#555;font-size:12px;">No win prediction yet</span>'
    cp        = win.get("checkpoint", "?")
    prob_ch   = win.get("prob_chase_wins", 0.5)
    bat       = escape(str(win.get("bat_team") or ""))
    bowl      = escape(str(win.get("bowl_team") or ""))
    inn1_sc   = win.get("inn1_total_score")
    inn2_sc   = win.get("inn2_score_at_cp")
    inn2_wk   = win.get("inn2_wickets_at_cp")
    ctx       = f"Inn2 {inn2_sc}/{inn2_wk}" if inn2_sc is not None else f"Inn1 {inn1_sc} runs"
    bar       = _win_bar(prob_ch)
    return (
        f'<div style="font-size:11px;color:#888;margin-bottom:4px;">'
        f'{bat} bat · {escape(cp)} · {escape(ctx)}</div>'
        + bar
    )


def view_live_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        return _view_live_matches_html_inner(req)
    except Exception as _ex:
        import traceback
        _tb = traceback.format_exc()
        logging.exception("live_matches page error")
        return func.HttpResponse(
            f"<pre style='color:red;padding:20px'>Live Matches error:\n{escape(str(_ex))}\n\n{escape(_tb)}</pre>",
            mimetype="text/html", status_code=200,
        )


def _view_live_matches_html_inner(req: func.HttpRequest) -> func.HttpResponse:
    gold  = get_named_container_client("gold")
    index = download_json(gold, _LIVE_INDEX_BLOB) or {}

    gen_time    = str(index.get("generated_at_utc", ""))[:16].replace("T", " ")
    active_count = index.get("active_count", 0)
    matches      = index.get("matches") or []

    if not matches:
        body = """
        <div class="empty-state">
            <div class="empty-icon">🏏</div>
            <div class="empty-msg">No live matches right now</div>
            <div class="empty-sub">
                Live predictions appear here automatically when a match is in progress
                and <code>func-ramanuj-live-ml</code> has had at least one tick to run.
            </div>
        </div>"""
    else:
        cards = ""
        for m in matches:
            eid        = str(m.get("event_id") or "")
            home       = escape(str(m.get("home_team_name") or "Home"))
            away       = escape(str(m.get("away_team_name") or "Away"))
            match_name = escape(str(m.get("match_name") or f"{home} v {away}"))
            innings    = m.get("current_innings")
            over       = m.get("current_over")
            score      = m.get("current_score")
            wickets    = m.get("current_wickets")
            bat_team   = escape(str(m.get("batting_team") or ""))
            bat_odds   = m.get("batting_team_odds")
            bowl_odds  = m.get("bowling_team_odds")
            updated    = str(m.get("updated_utc") or "")[:16].replace("T", " ")

            inn_badge = _innings_badge(innings, over)

            # Current score row
            score_str = f"{score}/{wickets} ({over} ov)" if score is not None else "—"
            odds_str  = ""
            if bat_odds or bowl_odds:
                bowl_team = escape(str(m.get("away_team_name") if bat_team == home else m.get("home_team_name") or ""))
                odds_str = (
                    f'<span style="color:#aaa;font-size:12px;">'
                    f'Odds: {bat_team} {bat_odds or "—"} · {bowl_team} {bowl_odds or "—"}'
                    f'</span>'
                )

            win_html = _win_summary_html(m.get("latest_win"))
            ou_html  = _ou_summary_html(m.get("latest_ou"))

            cards += f"""
            <div class="match-card">
              <div class="card-header">
                <div class="match-title">
                  <span class="match-link">{match_name}</span>
                  {inn_badge}
                </div>
                <div class="score-row">
                  <span style="color:#fff;font-size:15px;font-weight:bold;">{bat_team} {escape(score_str)}</span>
                  &nbsp; {odds_str}
                </div>
                <div class="card-meta" style="margin-top:4px;">
                  <span class="live-dot">●</span> LIVE
                  &nbsp;·&nbsp; updated {escape(updated)} UTC
                </div>
              </div>

              <div class="pred-section">
                <div class="pred-label">Win Predictor</div>
                {win_html}
              </div>

              <div class="pred-section" style="margin-top:12px;">
                <div class="pred-label">Over / Under</div>
                {ou_html}
              </div>
            </div>"""

        body = cards

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Live Matches — ML Predictions</title>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="60">
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      font-family: Arial, sans-serif;
      background: #0a0f1a;
      color: #e0e0e0;
      margin: 0;
      padding: 24px 32px;
    }}
    nav {{ margin-bottom: 20px; font-size: 14px; }}
    nav a {{ color: #00a8ff; text-decoration: none; margin-right: 16px; }}
    nav a:hover {{ text-decoration: underline; }}
    .page-header {{
      display: flex; align-items: baseline; gap: 16px; margin-bottom: 6px;
    }}
    h1 {{ margin: 0; font-size: 22px; color: #fff; }}
    .live-badge {{
      background: #c00; color: white; font-size: 11px; font-weight: bold;
      padding: 2px 8px; border-radius: 3px; letter-spacing: 1px;
    }}
    .sub-meta {{
      font-size: 13px; color: #555; margin-bottom: 24px;
    }}
    .auto-refresh-note {{
      font-size: 11px; color: #444; margin-left: 8px;
    }}

    /* Match cards */
    .match-card {{
      background: #111827;
      border: 1px solid #1e293b;
      border-radius: 10px;
      padding: 20px 24px;
      margin-bottom: 18px;
      box-shadow: 0 2px 8px #0004;
    }}
    .card-header {{
      margin-bottom: 16px;
    }}
    .match-title {{
      display: flex; align-items: center; gap: 10px; margin-bottom: 4px;
    }}
    .match-link {{
      color: #fff; text-decoration: none; font-size: 17px; font-weight: bold;
    }}
    .match-link:hover {{ color: #00e5ff; }}
    .badge {{
      display: inline-block; background: #1a6b3a; color: white;
      font-size: 11px; font-weight: bold; padding: 2px 9px;
      border-radius: 99px; letter-spacing: 0.5px;
    }}
    .card-meta {{
      font-size: 12px; color: #555;
    }}
    .live-dot {{
      color: #f00;
      animation: blink 1s step-start infinite;
    }}
    @keyframes blink {{ 50% {{ opacity: 0; }} }}

    .score-row {{
      margin: 6px 0 2px;
    }}
    .pred-section {{
      margin-top: 14px;
    }}
    .pred-label {{
      font-size: 11px; font-weight: bold; color: #667; text-transform: uppercase;
      letter-spacing: 1px; margin-bottom: 8px;
    }}

    /* Empty state */
    .empty-state {{
      text-align: center; padding: 80px 20px;
    }}
    .empty-icon {{ font-size: 56px; margin-bottom: 16px; }}
    .empty-msg {{
      font-size: 20px; color: #ccc; font-weight: bold; margin-bottom: 10px;
    }}
    .empty-sub {{
      font-size: 14px; color: #555; max-width: 480px; margin: 0 auto;
      line-height: 1.6;
    }}
    .empty-sub code {{ color: #888; background: #1a1a2e; padding: 2px 6px; border-radius: 3px; }}
  </style>
</head>
<body>
  <nav>
    <a href="/api/home">Home</a>
    <a href="/api/ended/view">Ended Matches</a>
    <a href="/api/ml/win-predictor">Win Predictor</a>
    <a href="/api/ml/over-under">Over / Under</a>
    <a href="/api/analysis/odds-movement">Odds Movement</a>
    <a href="/api/notification/settings" style="color:#f0a500;">⚙ Notifications</a>
  </nav>

  <div class="page-header">
    <h1>Live Matches</h1>
    <span class="live-badge">LIVE</span>
  </div>
  <div class="sub-meta">
    {f"{active_count} match{'es' if active_count != 1 else ''} in progress" if matches else "No live matches"}
    {('&nbsp;·&nbsp; ML updated ' + escape(gen_time) + ' UTC') if gen_time else ''}
    <span class="auto-refresh-note">(page auto-refreshes every 60 s)</span>
  </div>

  {body}
</body>
</html>"""

    return func.HttpResponse(html, mimetype="text/html")
