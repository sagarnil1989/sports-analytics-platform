from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    download_json, get_named_container_client,
)


def view_market_heatmap_html(req: func.HttpRequest) -> func.HttpResponse:
    """Post-match betting market heatmap. Reads pre-computed gold/event_id={eid}/heatmap.json."""
    import re as _re

    try:
        event_id = req.route_params.get("event_id", "")
        gold_c   = get_named_container_client("gold")

        # ── load gold tracker for match meta ──────────────────────────────────
        tracker = download_json(gold_c, f"event_id={event_id}/innings_tracker.json") or {}
        match_name = escape(tracker.get("match_name") or f"Event {event_id}")
        home_team  = tracker.get("home_team_name") or ""
        away_team  = tracker.get("away_team_name") or ""
        final_ss   = tracker.get("score_summary_events") or ""
        stadium    = tracker.get("stadium_data") or {}
        venue_str  = stadium.get("name") or tracker.get("venue") or ""

        # ── load pre-computed heatmap data from gold ──────────────────────────
        heatmap_doc = download_json(gold_c, f"event_id={event_id}/heatmap.json")
        if not heatmap_doc or not heatmap_doc.get("balls"):
            return func.HttpResponse(
                f"<h2>Heatmap not available</h2>"
                f"<p>Event {escape(event_id)}: heatmap data has not been built yet. "
                f"Run the silver_to_gold pipeline to generate it.</p>",
                status_code=404, mimetype="text/html",
            )

        # ── unpack heatmap data ───────────────────────────────────────────────
        # categories: {name: {type, color}}  →  all_cats: {name: (type, color)}
        cats_meta = heatmap_doc.get("categories") or {}
        all_cats: Dict[str, tuple] = {k: (v["type"], v["color"]) for k, v in cats_meta.items()}

        # balls: list of {over, over_num, ball, innings, score, cats: [str]}
        raw_balls = heatmap_doc.get("balls") or []
        balls = []
        for b in raw_balls:
            balls.append({
                "over":     b.get("over", "0"),
                "over_num": b.get("over_num", 0),
                "ball":     b.get("ball", 0),
                "innings":  b.get("innings", 1),
                "score":    b.get("score", "0/0"),
                "cats":     set(b.get("cats") or []),
            })

        if not balls:
            return func.HttpResponse("No market data found in heatmap.", status_code=404)

        # ── order market rows: match → over → ball → player ──────────────────
        _TYPE_ORDER = {"match": 0, "over": 1, "ball": 2, "player": 3}
        sorted_cats = sorted(all_cats.keys(), key=lambda c: (_TYPE_ORDER.get(all_cats[c][0], 9), c))

        # ── compute per-market open-rate (for colour intensity) ───────────────
        n_balls = len(balls)
        cat_rate: Dict[str, float] = {
            c: sum(1 for b in balls if c in b["cats"]) / n_balls
            for c in sorted_cats
        }

        def _cell_color(cat, ball):
            if cat not in ball["cats"]:
                return "#e5e7eb"  # grey = unavailable
            rate = cat_rate[cat]
            if rate >= 0.75:
                return "#0d7377"   # dark teal = always open
            if rate >= 0.40:
                return "#14a085"   # mid teal
            return "#3bc9a1"        # light teal = occasional

        # ── build HTML ────────────────────────────────────────────────────────
        # Serialise ball data for JS detail panel
        balls_json = json.dumps([{
            "idx":     i,
            "over":    b["over"],
            "innings": b["innings"],
            "score":   b["score"],
            "open":    sorted(b["cats"]),
            "closed":  [c for c in sorted_cats if c not in b["cats"]],
        } for i, b in enumerate(balls)], ensure_ascii=False)

        cats_json = json.dumps({c: {"type": all_cats[c][0], "color": all_cats[c][1]}
                                for c in sorted_cats}, ensure_ascii=False)

        # Over header row
        over_headers = ""
        prev_ov = -1
        for i, b in enumerate(balls):
            ov = b["over_num"]
            inn = b["innings"]
            bg  = "#2563eb" if inn == 1 else "#dc2626"
            if ov != prev_ov:
                span = sum(1 for bb in balls[i:] if bb["over_num"] == ov and bb["innings"] == inn)
                over_headers += f'<th colspan="{span}" style="background:{bg};color:white;font-size:11px;text-align:center;padding:3px;">Ov {ov}</th>'
                prev_ov = ov

        # Ball number row
        ball_cells = "".join(
            f'<th data-ball="{i}" style="font-size:10px;color:#666;padding:2px 3px;min-width:18px;">{b["ball"]}</th>'
            for i, b in enumerate(balls)
        )

        # Market rows
        market_rows_html = ""
        type_colors = {"match": "#2563eb", "over": "#d97706", "ball": "#16a34a", "player": "#9333ea"}
        for cat in sorted_cats:
            mtype, _ = all_cats[cat]
            tag_color = type_colors.get(mtype, "#6b7280")
            tag_html  = f'<span style="background:{tag_color};color:white;font-size:9px;padding:1px 5px;border-radius:3px;margin-left:4px;">{mtype}</span>'
            cell_parts = []
            for i, b in enumerate(balls):
                title = f'{escape(cat)} \xb7 over {b["over"]}'
                color = _cell_color(cat, b)
                cell_parts.append(
                    f'<td onclick="selectBall({i})" data-ball="{i}" title="{title}" '
                    f'style="background:{color};cursor:pointer;padding:0;height:16px;min-width:18px;"></td>'
                )
            cells = "".join(cell_parts)
            market_rows_html += (
                f'<tr><td style="font-size:12px;white-space:nowrap;padding:3px 8px;position:sticky;left:0;'
                f'background:white;z-index:1;border-right:1px solid #ddd;">'
                f'{escape(cat)}{tag_html}</td>{cells}</tr>'
            )

        # Stats bar
        n_markets  = len(sorted_cats)
        avg_open   = sum(len(b["cats"]) for b in balls) / n_balls if n_balls else 0
        n_wickets  = sum(1 for b in balls if b.get("wkts") and b["wkts"] > 0)

        # Format final score
        final_html = ""
        if final_ss:
            parts = final_ss.split("-")
            if len(parts) == 2:
                def _fmt(s, team):
                    mm = _re.match(r'(\d+/\d+)\(?([.\d]+)?\)?', s.strip())
                    if mm:
                        return f"<b>{escape(team)}</b> {mm.group(1)}" + (f" ({mm.group(2)} ov)" if mm.group(2) else "")
                    return f"<b>{escape(team)}</b> {escape(s.strip())}"
                final_html = f'<span style="margin-left:16px;font-size:14px;">{_fmt(parts[0],home_team)} &nbsp;vs&nbsp; {_fmt(parts[1],away_team)}</span>'

        html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Market Heatmap — {match_name}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f3f4f6; }}

  /* ── top nav ── */
  #topnav {{ background: #111827; color: white; padding: 12px 20px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; position: sticky; top: 0; z-index: 100; }}
  #topnav h1 {{ font-size: 16px; font-weight: 600; }}
  .badge-ended {{ background: #16a34a; color: white; font-size: 11px; padding: 2px 8px; border-radius: 999px; }}
  #topnav a {{ color: #93c5fd; font-size: 13px; text-decoration: none; }}

  /* ── layout ── */
  #main {{ display: flex; gap: 0; height: calc(100vh - 50px); }}
  #left  {{ flex: 1; overflow: auto; padding: 16px; }}
  #right {{ width: 340px; min-width: 280px; background: white; border-left: 1px solid #e5e7eb; overflow-y: auto; padding: 16px; display: none; }}
  #right.open {{ display: block; }}

  /* ── tabs ── */
  .tabs {{ display: flex; gap: 6px; margin-bottom: 12px; }}
  .tab {{ padding: 5px 14px; border-radius: 6px; border: 1px solid #d1d5db; cursor: pointer; font-size: 13px; background: white; }}
  .tab.active {{ background: #1e40af; color: white; border-color: #1e40af; }}

  /* ── legend ── */
  .legend {{ display: flex; gap: 16px; margin-bottom: 10px; flex-wrap: wrap; }}
  .leg {{ display: flex; align-items: center; gap: 5px; font-size: 12px; }}
  .leg-sq {{ width: 14px; height: 14px; border-radius: 2px; }}

  /* ── heatmap ── */
  #heatmap-wrap {{ overflow-x: auto; }}
  #heatmap {{ border-collapse: collapse; }}
  #heatmap th, #heatmap td {{ border: none; }}
  .selected-col {{ outline: 2px solid #2563eb; }}

  /* ── stats bar ── */
  #statsbar {{ background: #1e293b; color: #e2e8f0; padding: 8px 20px; font-size: 13px; display: flex; gap: 24px; }}
  .stat {{ display: flex; flex-direction: column; }}
  .stat-val {{ font-size: 18px; font-weight: 700; color: white; }}

  /* ── detail panel ── */
  #detail-header {{ margin-bottom: 12px; }}
  #detail-header h2 {{ font-size: 15px; font-weight: 600; }}
  .mkt-card {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; margin-bottom: 8px; }}
  .mkt-card h4 {{ font-size: 13px; font-weight: 600; margin-bottom: 4px; display: flex; align-items: center; gap: 6px; }}
  .mkt-tag {{ font-size: 10px; padding: 1px 6px; border-radius: 3px; color: white; }}
  .unavail-list {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 6px; }}
  .unavail-pill {{ background: #e5e7eb; color: #6b7280; font-size: 11px; padding: 2px 8px; border-radius: 999px; }}
</style>
</head>
<body>

<div id="topnav">
  <div>
    <h1>{match_name}</h1>
    {f'<div style="font-size:12px;color:#9ca3af;">{escape(venue_str)}</div>' if venue_str else ''}
  </div>
  <span class="badge-ended">Match ended</span>
  {final_html}
  <a href="/api/matches/{escape(str(event_id))}/innings-tracker/view" style="margin-left:auto;">Innings Tracker →</a>
  <a href="/api/matches/{escape(str(event_id))}/detailed-analysis">Detailed Analysis →</a>
  <a href="/api/matches/{escape(str(event_id))}/view">Match Page →</a>
</div>

<div id="main">
  <div id="left">
    <div class="tabs">
      <button class="tab active" onclick="filterInnings('all')">Full match</button>
      <button class="tab" onclick="filterInnings(1)">1st innings</button>
      <button class="tab" onclick="filterInnings(2)">2nd innings</button>
    </div>
    <div class="legend">
      <div class="leg"><div class="leg-sq" style="background:#0d7377;"></div> Always open</div>
      <div class="leg"><div class="leg-sq" style="background:#14a085;"></div> Mostly open</div>
      <div class="leg"><div class="leg-sq" style="background:#3bc9a1;"></div> Occasional</div>
      <div class="leg"><div class="leg-sq" style="background:#e5e7eb;border:1px solid #d1d5db;"></div> Unavailable</div>
    </div>
    <div id="heatmap-wrap">
      <table id="heatmap">
        <thead>
          <tr><th style="position:sticky;left:0;background:white;z-index:2;min-width:180px;"></th>{over_headers}</tr>
          <tr><th style="position:sticky;left:0;background:white;z-index:2;"></th>{ball_cells}</tr>
        </thead>
        <tbody>
          {market_rows_html}
        </tbody>
      </table>
    </div>
  </div>

  <div id="right">
    <div id="detail-header">
      <h2 id="detail-title">Click a cell to see market detail</h2>
      <div id="detail-meta" style="font-size:13px;color:#6b7280;margin-top:4px;"></div>
    </div>
    <div id="open-section"></div>
    <div id="unavail-section" style="margin-top:12px;"></div>
  </div>
</div>

<div id="statsbar">
  <div class="stat"><span class="stat-val">{n_balls}</span><span>Total states</span></div>
  <div class="stat"><span class="stat-val">{n_markets}</span><span>Market categories</span></div>
  <div class="stat"><span class="stat-val">{avg_open:.1f}</span><span>Avg open/state</span></div>
</div>

<script>
const BALLS = {balls_json};
const CATS  = {cats_json};

const TYPE_COLORS = {{match:'#2563eb', over:'#d97706', ball:'#16a34a', player:'#9333ea'}};

let selectedCol = -1;
let currentFilter = 'all';

function filterInnings(inn) {{
  currentFilter = inn;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  event.target.classList.add('active');
  const rows = document.querySelectorAll('#heatmap tbody tr');
  rows.forEach(row => {{
    const cells = row.querySelectorAll('td[data-ball]');
    cells.forEach(td => {{
      const idx = parseInt(td.dataset.ball);
      const b = BALLS[idx];
      td.style.display = (inn === 'all' || b.innings === inn) ? '' : 'none';
    }});
  }});
  const hcells = document.querySelectorAll('th[data-ball]');
  hcells.forEach(th => {{
    const idx = parseInt(th.dataset.ball);
    const b = BALLS[idx];
    th.style.display = (inn === 'all' || b.innings === inn) ? '' : 'none';
  }});
}}

function selectBall(idx) {{
  selectedCol = idx;
  const b = BALLS[idx];
  const panel = document.getElementById('right');
  panel.classList.add('open');

  document.getElementById('detail-title').textContent =
    'Over ' + b.over + ' · Innings ' + b.innings;
  document.getElementById('detail-meta').textContent =
    'Score: ' + b.score + ' · ' + b.open.length + ' / ' + Object.keys(CATS).length + ' markets open';

  let openHtml = '<h3 style="font-size:13px;font-weight:600;margin-bottom:8px;">Open markets (' + b.open.length + ')</h3>';
  b.open.forEach(cat => {{
    const c = CATS[cat] || {{}};
    const tc = TYPE_COLORS[c.type] || '#6b7280';
    openHtml += '<div class="mkt-card"><h4>' + cat +
      '<span class="mkt-tag" style="background:' + tc + '">' + (c.type||'') + '</span></h4></div>';
  }});
  document.getElementById('open-section').innerHTML = openHtml;

  if (b.closed.length) {{
    let cl = '<h3 style="font-size:13px;font-weight:600;margin-bottom:6px;">Unavailable (' + b.closed.length + ')</h3><div class="unavail-list">';
    b.closed.forEach(cat => {{ cl += '<span class="unavail-pill">' + cat + '</span>'; }});
    cl += '</div>';
    document.getElementById('unavail-section').innerHTML = cl;
  }} else {{
    document.getElementById('unavail-section').innerHTML = '';
  }}

  // Highlight column
  document.querySelectorAll('td[data-ball], th[data-ball]').forEach(el => el.classList.remove('selected-col'));
  document.querySelectorAll('[data-ball="' + idx + '"]').forEach(el => el.classList.add('selected-col'));
}}
</script>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render market heatmap")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ---------------------------------------------------------------------------
# Detailed Analysis page
# ---------------------------------------------------------------------------
