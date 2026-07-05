"""Odds Movement Analysis page — per-match swing ranking, league/team aggregates,
and opening-odds-gap vs in-play-swing scatter."""
from .common import json, logging, escape, func, download_json, get_named_container_client, adf_activity_badge

_NAV = """<nav style="font-size:14px;margin-bottom:24px;">
  <a href="/api/home">Home</a>
  <a href="/api/live/view" style="color:#c00;font-weight:bold;">🔴 Live</a>
  <a href="/api/analysis/odds-movement">Odds Movement</a>
  <a href="/api/ml/win-predictor">Win Predictor</a>
  <a href="/api/ml/over-under">Over/Under</a>
</nav>"""

_STYLE = """
body{font-family:Arial,sans-serif;background:#f5f5f5;margin:0;padding:30px;}
h1{margin-bottom:4px;} h2{margin-top:32px;margin-bottom:12px;border-bottom:2px solid #ddd;padding-bottom:6px;}
.meta-grid{display:flex;gap:16px;flex-wrap:wrap;margin:20px 0;}
.meta-box{background:white;padding:16px 20px;border-radius:8px;box-shadow:0 1px 4px #ccc;min-width:140px;}
.meta-label{font-size:12px;color:#888;margin-bottom:4px;}
.meta-val{font-size:18px;font-weight:bold;color:#222;}
.tabs{display:flex;gap:4px;margin-bottom:24px;border-bottom:2px solid #ddd;}
.tab-btn{padding:10px 20px;background:#f0f0f0;border:none;border-radius:6px 6px 0 0;cursor:pointer;font-size:14px;color:#555;}
.tab-btn.active{background:white;color:#1a3a6b;font-weight:bold;border-bottom:2px solid white;margin-bottom:-2px;}
.tab-panel{display:none;} .tab-panel.active{display:block;}
table{width:100%;border-collapse:collapse;background:white;box-shadow:0 1px 4px #ccc;border-radius:8px;overflow:hidden;margin-bottom:16px;}
th{background:#333;color:white;padding:10px 14px;text-align:left;font-size:13px;}
td{padding:9px 14px;border-bottom:1px solid #eee;font-size:13px;}
tr:last-child td{border-bottom:none;} tr:hover td{background:#f9f9f9;}
.badge-yes{background:#d4edda;color:#155724;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:bold;}
.badge-no{background:#f8f8f8;color:#999;padding:2px 8px;border-radius:999px;font-size:11px;}
.hint{color:#777;font-size:13px;margin-top:6px;}
nav a{color:#0066cc;text-decoration:none;margin-right:16px;}
nav a:hover{text-decoration:underline;}
#scatter-canvas{background:white;border-radius:8px;box-shadow:0 1px 4px #ccc;display:block;}
"""


def _swing_color(v):
    if v is None: return "#999"
    if v >= 0.8:  return "#cc2200"
    if v >= 0.4:  return "#cc7700"
    return "#2d7a2d"


def view_odds_movement_html(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    data = download_json(gold, "cricket/analysis/odds_movement_summary.json")

    if not data:
        body = f"""<p style='color:#c00'>No data yet. Run <b>pl_ml_and_hypothesis</b> in ADF first
               ({adf_activity_badge("OddsMovementAnalysis")})</p>"""
        return func.HttpResponse(f"<!DOCTYPE html><html><head><title>Odds Movement</title><style>{_STYLE}</style></head><body>{_NAV}<h1>Odds Movement Analysis</h1>{body}</body></html>", mimetype="text/html")

    generated  = (data.get("generated_at_utc") or "")[:19].replace("T", " ")
    total      = data.get("total_matches", 0)
    dbl_count  = data.get("double_opportunity_count", 0)
    dbl_pct    = data.get("pct_double_opportunity", 0)
    avg_swing  = data.get("avg_swing", 0)
    max_swing  = data.get("max_swing_ever", 0)
    matches    = data.get("matches") or []
    by_league  = data.get("by_league") or []
    by_team    = data.get("by_team") or []

    # ── Tab 1: Match ranking ─────────────────────────────────────────────────
    def _state_label(at: dict) -> str:
        """Compact label: 'Inn1 Ov6.3 · 72/2'"""
        if not at or at.get("innings") is None:
            return "—"
        inn  = at.get("innings", "?")
        ov   = at.get("over", "?")
        sc   = at.get("score")
        wk   = at.get("wickets")
        score_part = f" · {sc}/{wk}" if sc is not None else ""
        return f"Inn{inn} Ov{ov}{score_part}"

    def _final_score(m: dict) -> str:
        s1 = m.get("final_inn1_score")
        w1 = m.get("final_inn1_wickets")
        s2 = m.get("final_inn2_score")
        w2 = m.get("final_inn2_wickets")
        if s1 is None:
            return "—"
        inn1 = f"{s1}/{w1}" if w1 is not None else str(s1)
        if s2 is None:
            return inn1
        inn2 = f"{s2}/{w2}" if w2 is not None else str(s2)
        return f"{inn1} — {inn2}"

    rows1 = ""
    for m in matches[:200]:
        dbl    = m.get("double_opportunity")
        profit = m.get("net_profit_if_both_backed")
        profit_s = f"+{profit:.3f}" if profit and profit > 0 else ("—" if profit is None else f"{profit:.3f}")
        profit_c = "#2d7a2d" if profit and profit > 0 else "#999"
        sw     = m.get("max_swing", 0)
        winner = m.get("winner") or "—"
        home_at = _state_label(m.get("peak_home_at") or {})
        away_at = _state_label(m.get("peak_away_at") or {})
        final   = _final_score(m)
        rows1 += f"""<tr>
            <td style="white-space:nowrap">{escape(m.get("match_date_utc","")[:10])}</td>
            <td>{escape(m.get("match_name",""))}<br>
                <small style="color:#888">{escape(m.get("league_name",""))}</small></td>
            <td style="font-family:monospace;font-weight:bold">{final}</td>
            <td style="color:#555;font-size:12px">{escape(winner)}</td>
            <td>
                <span style="font-family:monospace;font-weight:bold">{m.get("peak_home_odds","—")}</span><br>
                <small style="color:#888;font-size:11px">{escape(home_at)}</small>
            </td>
            <td>
                <span style="font-family:monospace;font-weight:bold">{m.get("peak_away_odds","—")}</span><br>
                <small style="color:#888;font-size:11px">{escape(away_at)}</small>
            </td>
            <td style="font-weight:bold;color:{_swing_color(sw)}">{sw:.3f}</td>
            <td>{"<span class='badge-yes'>✓ Yes</span>" if dbl else "<span class='badge-no'>No</span>"}</td>
            <td style="color:{profit_c};font-weight:bold;font-family:monospace">{profit_s}</td>
        </tr>"""

    tab1 = f"""<table>
        <thead><tr>
            <th>Date</th><th>Match</th>
            <th>Final Score</th><th>Winner</th>
            <th>Peak Home Odds<br><small style="font-weight:normal;color:#aaa">Inn · Over · Score</small></th>
            <th>Peak Away Odds<br><small style="font-weight:normal;color:#aaa">Inn · Over · Score</small></th>
            <th>Swing ↑</th><th>Double Opp?</th>
            <th>Net Profit<br><small>(if both backed)</small></th>
        </tr></thead>
        <tbody>{rows1}</tbody>
    </table>
    <p class="hint">
        <b>Final Score</b> = Inn1 score — Inn2 score &nbsp;|&nbsp;
        <b>Inn · Over · Score</b> = match state when that team's odds peaked &nbsp;|&nbsp;
        <b>Swing</b> = peak odds − 2.0 &nbsp;|&nbsp;
        Net profit assumes stake=1 on each team at their peak; positive = profitable if winning team was backed.
    </p>"""

    # ── Tab 2: By league ─────────────────────────────────────────────────────
    rows2 = ""
    for lg in by_league:
        sw = lg.get("avg_swing", 0)
        rows2 += f"""<tr>
            <td>{escape(lg.get("league_name",""))}</td>
            <td style="text-align:right">{lg.get("match_count",0)}</td>
            <td style="font-weight:bold;color:{_swing_color(sw)};text-align:right">{sw:.3f}</td>
            <td style="text-align:right">{lg.get("max_swing",0):.3f}</td>
            <td style="text-align:right">{lg.get("double_opp_count",0)}</td>
            <td style="text-align:right">{lg.get("pct_double_opportunity",0):.1f}%</td>
        </tr>"""

    tab2 = f"""<table>
        <thead><tr>
            <th>League</th><th style="text-align:right">Matches</th>
            <th style="text-align:right">Avg Swing ↑</th><th style="text-align:right">Max Swing</th>
            <th style="text-align:right">Double-Opp Count</th><th style="text-align:right">Double-Opp %</th>
        </tr></thead>
        <tbody>{rows2}</tbody>
    </table>"""

    # ── Tab 3: By team ────────────────────────────────────────────────────────
    rows3 = ""
    for tm in by_team[:100]:
        sw = tm.get("avg_swing", 0)
        rows3 += f"""<tr>
            <td>{escape(tm.get("team_name",""))}</td>
            <td style="text-align:right">{tm.get("match_count",0)}</td>
            <td style="font-weight:bold;color:{_swing_color(sw)};text-align:right">{sw:.3f}</td>
            <td style="text-align:right">{tm.get("double_opp_count",0)}</td>
            <td style="text-align:right">{tm.get("pct_double_opp",0):.1f}%</td>
            <td style="font-family:monospace;text-align:right">{tm.get("avg_peak_odds_as_home") or "—"}</td>
            <td style="font-family:monospace;text-align:right">{tm.get("avg_peak_odds_as_away") or "—"}</td>
        </tr>"""

    tab3 = f"""<table>
        <thead><tr>
            <th>Team</th><th style="text-align:right">Matches</th>
            <th style="text-align:right">Avg Swing ↑</th>
            <th style="text-align:right">Double-Opp</th><th style="text-align:right">Double-Opp %</th>
            <th style="text-align:right">Avg Peak Odds (home)</th>
            <th style="text-align:right">Avg Peak Odds (away)</th>
        </tr></thead>
        <tbody>{rows3}</tbody>
    </table>
    <p class="hint">Only top 100 teams shown. Avg Peak Odds = average of the peak odds reached by this team across all matches played in that role.</p>"""

    # ── Tab 4: Opening gap vs swing scatter ───────────────────────────────────
    # Build a JS-driven SVG scatter. Filter to matches with opening_odds_gap data.
    scatter_data = json.dumps([
        {"x": m["opening_odds_gap"], "y": m["max_swing"],
         "label": m.get("match_name","")[:40], "dbl": m.get("double_opportunity", False)}
        for m in matches if m.get("opening_odds_gap") is not None and m.get("max_swing") is not None
    ])

    tab4 = f"""<p style="color:#555;margin-bottom:12px">
        Each point is one match. X-axis = opening odds gap (|home odds − away odds| at the first snapshot).
        Y-axis = in-play swing (max peak above 2.0). Hypothesis: smaller gap → bigger potential swing.
        <span style="color:#1a7a1a">●</span> = double-opportunity match.
    </p>
    <canvas id="scatter-canvas" width="860" height="440"></canvas>
    <script>
    (function(){{
        var data = {scatter_data};
        var canvas = document.getElementById("scatter-canvas");
        var ctx = canvas.getContext("2d");
        var W = canvas.width, H = canvas.height;
        var PAD = {{l:52, r:20, t:20, b:50}};
        var maxX = Math.max.apply(null, data.map(d=>d.x)) || 2;
        var maxY = Math.max.apply(null, data.map(d=>d.y)) || 1;
        function sx(x){{ return PAD.l + (x / maxX) * (W - PAD.l - PAD.r); }}
        function sy(y){{ return H - PAD.b - (y / maxY) * (H - PAD.t - PAD.b); }}

        // Grid
        ctx.strokeStyle="#eee"; ctx.lineWidth=1;
        for(var i=0;i<=5;i++){{
            var yv=i*maxY/5;
            ctx.beginPath(); ctx.moveTo(PAD.l,sy(yv)); ctx.lineTo(W-PAD.r,sy(yv)); ctx.stroke();
            ctx.fillStyle="#888"; ctx.font="11px Arial"; ctx.textAlign="right";
            ctx.fillText(yv.toFixed(2),PAD.l-4,sy(yv)+4);
        }}
        for(var j=0;j<=5;j++){{
            var xv=j*maxX/5;
            ctx.beginPath(); ctx.moveTo(sx(xv),PAD.t); ctx.lineTo(sx(xv),H-PAD.b); ctx.stroke();
            ctx.fillStyle="#888"; ctx.font="11px Arial"; ctx.textAlign="center";
            ctx.fillText(xv.toFixed(2),sx(xv),H-PAD.b+16);
        }}

        // Axes labels
        ctx.fillStyle="#444"; ctx.font="13px Arial"; ctx.textAlign="center";
        ctx.fillText("Opening Odds Gap (|home − away| at first snapshot)",W/2,H-4);
        ctx.save(); ctx.translate(14,H/2); ctx.rotate(-Math.PI/2);
        ctx.fillText("In-Play Swing (peak above 2.0)",0,0); ctx.restore();

        // Points
        data.forEach(function(d){{
            ctx.beginPath();
            ctx.arc(sx(d.x),sy(d.y),d.dbl?5:3,0,2*Math.PI);
            ctx.fillStyle=d.dbl?"#1a7a1a":"#0066cc";
            ctx.globalAlpha=0.6;
            ctx.fill();
            ctx.globalAlpha=1.0;
        }});

        // Tooltip on hover
        canvas.addEventListener("mousemove",function(e){{
            var rect=canvas.getBoundingClientRect();
            var mx=e.clientX-rect.left, my=e.clientY-rect.top;
            ctx.clearRect(0,0,W,H);
            // redraw (simplified: just redraw points)
            ctx.strokeStyle="#eee"; ctx.lineWidth=1;
            for(var i=0;i<=5;i++){{var yv=i*maxY/5;ctx.beginPath();ctx.moveTo(PAD.l,sy(yv));ctx.lineTo(W-PAD.r,sy(yv));ctx.stroke();}}
            for(var j=0;j<=5;j++){{var xv=j*maxX/5;ctx.beginPath();ctx.moveTo(sx(xv),PAD.t);ctx.lineTo(sx(xv),H-PAD.b);ctx.stroke();}}
            var hit=null;
            data.forEach(function(d){{
                var dx=sx(d.x)-mx, dy=sy(d.y)-my;
                ctx.beginPath(); ctx.arc(sx(d.x),sy(d.y),d.dbl?5:3,0,2*Math.PI);
                ctx.fillStyle=d.dbl?"#1a7a1a":"#0066cc"; ctx.globalAlpha=0.6; ctx.fill(); ctx.globalAlpha=1.0;
                if(Math.sqrt(dx*dx+dy*dy)<8) hit=d;
            }});
            if(hit){{
                ctx.fillStyle="rgba(0,0,0,0.75)"; ctx.fillRect(mx+10,my-22,340,22);
                ctx.fillStyle="white"; ctx.font="11px Arial"; ctx.textAlign="left";
                ctx.fillText(hit.label+" | gap="+hit.x.toFixed(3)+" swing="+hit.y.toFixed(3),mx+14,my-6);
            }}
        }});
    }})();
    </script>"""

    # ── Page assembly ─────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Odds Movement Analysis</title>
  <meta charset="utf-8">
  <style>{_STYLE}</style>
</head>
<body>
  {_NAV}
  <h1>Odds Movement Analysis</h1>
  {adf_activity_badge("OddsMovementAnalysis")}
  <p style="color:#666">In-play odds swing analysis across all T20 matches — how far did each team's win odds swing, and how often did both teams exceed even-money at different points in the same match?</p>

  <div class="meta-grid">
    <div class="meta-box"><div class="meta-label">Matches analysed</div><div class="meta-val">{total}</div></div>
    <div class="meta-box"><div class="meta-label">Double-opportunity matches</div><div class="meta-val">{dbl_count} <small style="font-size:13px;color:#888">({dbl_pct}%)</small></div></div>
    <div class="meta-box"><div class="meta-label">Avg swing (all matches)</div><div class="meta-val">{avg_swing}</div></div>
    <div class="meta-box"><div class="meta-label">Largest single-match swing</div><div class="meta-val">{max_swing}</div></div>
    <div class="meta-box"><div class="meta-label">Generated</div><div class="meta-val" style="font-size:13px">{escape(generated)} UTC</div></div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" onclick="switchTab(0)">Match Ranking</button>
    <button class="tab-btn"        onclick="switchTab(1)">By League</button>
    <button class="tab-btn"        onclick="switchTab(2)">By Team</button>
    <button class="tab-btn"        onclick="switchTab(3)">Gap vs Swing</button>
  </div>

  <div class="tab-panel active" id="tab0">{tab1}</div>
  <div class="tab-panel"        id="tab1">{tab2}</div>
  <div class="tab-panel"        id="tab2">{tab3}</div>
  <div class="tab-panel"        id="tab3">{tab4}</div>

  <script>
  function switchTab(i){{
    document.querySelectorAll(".tab-btn").forEach(function(b,j){{b.classList.toggle("active",i===j);}});
    document.querySelectorAll(".tab-panel").forEach(function(p,j){{p.classList.toggle("active",i===j);}});
  }}
  </script>
</body>
</html>"""
    return func.HttpResponse(html, mimetype="text/html")
