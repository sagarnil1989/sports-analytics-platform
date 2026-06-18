import re as _re
from concurrent.futures import ThreadPoolExecutor, as_completed

from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    download_json, get_named_container_client,
)
from league_config import load_blocked_event_ids

_MATCH_OVERRIDES_PATH = "overrides/match_overrides.json"


def _load_match_overrides(gold) -> Dict[str, Dict]:
    raw = download_json(gold, _MATCH_OVERRIDES_PATH) or {}
    return {k: v for k, v in raw.items() if not k.startswith("_") and isinstance(v, dict)}


def _detect_format(match_name="", league_name="", score_ss=""):
    combined = f"{match_name} {league_name}".lower()
    if "t20" in combined or "twenty20" in combined:
        return "T20"
    if "odi" in combined or "one day" in combined:
        return "ODI"
    if score_ss:
        overs = [float(m) for m in _re.findall(r'\((\d+(?:\.\d+)?)\)', score_ss)]
        if overs:
            return "T20" if max(overs) <= 20 else "ODI"
    return ""


def _load_one(eid, gold, blocked, live_eids, overrides: Dict[str, Dict]):
    if eid in blocked or eid in live_eids:
        return None

    tracker = download_json(gold, f"event_id={eid}/innings_tracker.json")
    if not tracker:
        return None

    home_name  = str(tracker.get("home_team_name") or "")
    away_name  = str(tracker.get("away_team_name") or "")
    match_name = tracker.get("match_name") or f"{home_name} vs {away_name}"
    if not match_name:
        return None

    fi    = str(tracker.get("fi") or "")
    score = tracker.get("score_summary_events") or tracker.get("score_summary") or ""
    score = score.replace("-", ",") if score else score

    # Order by 1st-innings batting team
    rows = tracker.get("rows") or []
    inn1_bat = next(
        (str(r["batting_team"]).strip() for r in rows
         if r.get("innings") == 1 and r.get("batting_team")),
        None,
    )
    if inn1_bat and away_name and inn1_bat == away_name.strip():
        if score and "," in score:
            p = score.split(",", 1)
            score = f"{p[1].strip()},{p[0].strip()}"
        swapped    = match_name.replace(f"{home_name} vs {away_name}", f"{away_name} vs {home_name}", 1)
        match_name = swapped if swapped != match_name else f"{away_name} vs {home_name}"

    fmt    = _detect_format(match_name, str(tracker.get("league_name") or ""), score or "")
    gender = tracker.get("gender") or "M"
    stadium = (tracker.get("stadium_data") or {}).get("name") or None

    ov = overrides.get(eid) or {}
    return {
        "event_id":       eid,
        "fi":             fi,
        "league_id":      str(tracker.get("league_id") or ""),
        "league_name":    ov.get("league_name") or tracker.get("league_name"),
        "home_team_name": home_name,
        "away_team_name": away_name,
        "match_name":     ov.get("match_name") or match_name,
        "score":          ov.get("score") or score,
        "format":         ov.get("format") or fmt,
        "gender":         ov.get("gender") or gender,
        "stadium":        ov.get("stadium") or stadium,
        "event_time_utc": tracker.get("match_date_utc"),
        "time_status":    "3",
        "has_override":   bool(ov),
    }


def _build_matches():
    gold = get_named_container_client("gold")

    blocked   = load_blocked_event_ids()
    overrides = _load_match_overrides(gold)
    live_eids = set()
    live_idx  = download_json(gold, "cricket/matches/latest/index.json") or {}
    for m in (live_idx.get("matches") or []):
        eid = str(m.get("event_id") or "")
        if eid:
            live_eids.add(eid)

    event_ids = []
    for blob in gold.list_blobs(name_starts_with="event_id="):
        if not blob.name.endswith("/innings_tracker.json"):
            continue
        parts    = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if eid_part:
            event_ids.append(eid_part[9:])

    matches = []
    with ThreadPoolExecutor(max_workers=32) as ex:
        futs = {ex.submit(_load_one, eid, gold, blocked, live_eids, overrides): eid
                for eid in event_ids}
        for fut in as_completed(futs):
            result = fut.result()
            if result:
                matches.append(result)

    matches.sort(key=lambda m: str(m.get("event_time_utc") or ""), reverse=True)
    return matches



def view_ended_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        matches = _build_matches()
        data = {"ended_match_count": len(matches), "matches": matches}
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False),
                                 status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return ended index")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def _fmt_score(raw: str) -> str:
    """'163/9(20),167/6(19.5)' → '163/9 (20 ov), 167/6 (19.5 ov)'"""
    if not raw:
        return raw
    def _part(s):
        m = _re.match(r'^(\d+(?:/\d+)?)\s*\((\d+\.?\d*)\)', s.strip())
        return f"{m.group(1)} ({m.group(2)} ov)" if m else s.strip()
    return ", ".join(_part(p) for p in raw.split(",", 1))


def view_ended_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        matches = _build_matches()

        t20_matches = [m for m in matches if m.get("format") == "T20"]
        odi_matches = [m for m in matches if m.get("format") == "ODI"]
        oth_matches = [m for m in matches if m.get("format") not in ("T20", "ODI")]

        def _sort(lst):
            return sorted(lst, key=lambda m: m.get("event_time_utc") or "", reverse=True)

        league_names   = sorted({str(m.get("league_name") or "Unknown") for m in matches})
        league_options = '<option value="ALL">All Leagues</option>'
        for ln in league_names:
            league_options += f'<option value="{escape(ln)}">{escape(ln)}</option>'

        def _editable(eid, field, display_val, raw_val=None):
            # display_val and raw_val are already escaped by callers
            rv = str(raw_val) if raw_val is not None else str(display_val)
            dv = str(display_val)
            return (f'<td class="ec" data-eid="{eid}" data-field="{field}" '
                    f'data-raw="{rv}" title="Click to edit">{dv}</td>')

        def _rows_html(match_list, fmt):
            html = ""
            for m in match_list:
                league_name = str(m.get("league_name") or "Unknown")
                event_id    = escape(str(m.get("event_id") or "-"))
                league_esc  = escape(league_name)
                actual_fmt  = str(m.get("format") or fmt or "")
                fmt_esc     = escape(actual_fmt)
                badge_cls   = "badge-t20" if actual_fmt == "T20" else ("badge-odi" if actual_fmt == "ODI" else "badge-other")
                gender      = escape(str(m.get("gender") or "M"))
                score_raw   = str(m.get("score") or "")
                score_disp  = escape(_fmt_score(score_raw) or "-")
                stadium     = escape(str(m.get("stadium") or ""))
                has_ov      = m.get("has_override", False)
                row_style   = ' style="background:#fffde7"' if has_ov else ""
                html += f"""
                <tr data-league="{league_esc}" data-format="{fmt_esc}"{row_style}>
                    <td>{event_id}</td>
                    <td>{escape(str(m.get("fi") or "-"))}</td>
                    {_editable(event_id, "match_name", escape(str(m.get("match_name") or "-")))}
                    {_editable(event_id, "league_name", league_esc)}
                    <td class="ec fmt-cell" data-eid="{event_id}" data-field="format" data-raw="{fmt_esc}" title="Click to edit"><span class="{badge_cls}">{fmt_esc}</span></td>
                    <td class="ec" data-eid="{event_id}" data-field="gender" data-raw="{gender}" title="Click to edit">{gender}</td>
                    {_editable(event_id, "score", score_disp, score_raw)}
                    {_editable(event_id, "stadium", stadium or "-", stadium)}
                    <td>{escape(str(m.get("event_time_utc") or "-"))}</td>
                    <td><a href="/api/matches/{event_id}/heatmap">Heatmap</a></td>
                    <td><a href="/api/prematch/{event_id}/view">Prematch</a></td>
                    <td><a href="/api/matches/{event_id}/innings-tracker/view">Tracker</a></td>
                    <td><a href="/api/matches/{event_id}/detailed-analysis">Analysis</a></td>
                </tr>
                """
            return html

        t20_count = len(t20_matches)
        odi_count = len(odi_matches)
        oth_count = len(oth_matches)
        total     = len(matches)

        odi_tab_btn = (f'<button class="tab" onclick="filterFormat(\'ODI\', this)">ODI ({odi_count})</button>'
                       if odi_count else "")
        oth_tab_btn = (f'<button class="tab" onclick="filterFormat(\'Other\', this)">Other ({oth_count})</button>'
                       if oth_count else "")

        rows_html = ""
        if t20_matches:
            rows_html += f'<tr class="format-header" data-format-header="T20"><td colspan="13">T20 — {t20_count} matches</td></tr>'
            rows_html += _rows_html(_sort(t20_matches), "T20")
        if odi_matches:
            rows_html += f'<tr class="format-header" data-format-header="ODI"><td colspan="13">ODI — {odi_count} matches</td></tr>'
            rows_html += _rows_html(_sort(odi_matches), "ODI")
        if oth_matches:
            rows_html += f'<tr class="format-header" data-format-header="Other"><td colspan="13">Other — {oth_count} matches</td></tr>'
            rows_html += _rows_html(_sort(oth_matches), "")

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Ended Cricket Matches</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        .hint {{ color: #666; margin-bottom: 16px; font-size: 13px; }}
        .filter-bar {{ margin-bottom: 16px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }}
        select {{ padding: 8px 12px; font-size: 14px; border: 1px solid #ccc; border-radius: 6px; }}
        .tab-bar {{ display: flex; gap: 8px; margin-bottom: 16px; }}
        .tab {{ padding: 7px 18px; border-radius: 20px; border: 1px solid #ccc; background: #fff;
                cursor: pointer; font-size: 14px; font-weight: 600; color: #444; }}
        .tab.active {{ background: #1e293b; color: #fff; border-color: #1e293b; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
        th, td {{ padding: 9px 10px; border-bottom: 1px solid #eee; text-align: left; font-size: 13px; }}
        th {{ background: #1e293b; color: white; position: sticky; top: 0; font-size: 12px; }}
        tr:hover td {{ background: #f8faff; }}
        .format-header td {{ background: #f0f4ff; color: #1e3a8a; font-weight: 700;
                             font-size: 13px; padding: 8px 10px; border-top: 2px solid #c7d7f5; }}
        .badge-t20   {{ background: #dbeafe; color: #1d4ed8; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }}
        .badge-odi   {{ background: #dcfce7; color: #15803d; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }}
        .badge-other {{ background: #fef9c3; color: #854d0e; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }}
        a {{ color: #2563eb; font-weight: 600; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        /* Inline editing */
        td.ec {{ cursor: pointer; }}
        td.ec:hover {{ background: #fffbea !important; outline: 1px dashed #f59e0b; }}
        td.ec.editing {{ padding: 2px 4px; }}
        td.ec input, td.ec select {{ width: 100%; font-size: 13px; padding: 4px 6px;
            border: 2px solid #2563eb; border-radius: 4px; box-sizing: border-box; }}
        td.ec.saving {{ opacity: 0.5; }}
        td.ec.saved {{ background: #d1fae5 !important; transition: background 1s; }}
        td.ec.error  {{ background: #fee2e2 !important; }}
        #edit-toast {{ position: fixed; bottom: 24px; right: 24px; background: #1e293b; color: white;
            padding: 10px 18px; border-radius: 8px; font-size: 14px; display: none; z-index: 999; }}
    </style>
</head>
<body>
    <h1>Ended Cricket Matches</h1>
    <p class="hint">{total} total &nbsp;·&nbsp; <b>{t20_count} T20</b> &nbsp;·&nbsp; <b>{odi_count} ODI</b></p>

    <div class="tab-bar">
        <button class="tab active" onclick="filterFormat('ALL', this)">All ({total})</button>
        <button class="tab" onclick="filterFormat('T20', this)">T20 ({t20_count})</button>
        {odi_tab_btn}
        {oth_tab_btn}
    </div>

    <div class="filter-bar">
        <label><b>League:</b></label>
        <select id="leagueFilter" onchange="applyFilters()">{league_options}</select>
    </div>

    <p class="hint" style="margin-top:0;font-size:12px;">💡 Click any <b>Match, League, Format, Gender, Score</b> or <b>Stadium</b> cell to edit it inline. Changes are saved immediately. Rows with overrides are highlighted in yellow.</p>

    <table>
        <thead><tr>
            <th>Event ID</th><th>Bet365 FI</th><th>Match ✏</th><th>League ✏</th>
            <th>Format ✏</th><th>Gender ✏</th><th>Final Score ✏</th><th>Stadium ✏</th><th>Date</th>
            <th>Heatmap</th><th>Prematch</th><th>Tracker</th><th>Analysis</th>
        </tr></thead>
        <tbody id="matchTable">{rows_html}</tbody>
    </table>
    <div id="edit-toast"></div>

    <script>
        var activeFormat = 'ALL';
        function filterFormat(fmt, btn) {{
            activeFormat = fmt;
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            btn.classList.add('active');
            applyFilters();
        }}
        function applyFilters() {{
            var league = document.getElementById('leagueFilter').value;
            document.querySelectorAll('tr[data-format]').forEach(function(r) {{
                var fmtOk    = activeFormat === 'ALL' || r.dataset.format === activeFormat;
                var leagueOk = league === 'ALL' || r.dataset.league === league;
                r.style.display = (fmtOk && leagueOk) ? '' : 'none';
            }});
            document.querySelectorAll('tr[data-format-header]').forEach(function(hdr) {{
                var fmt = hdr.dataset.formatHeader;
                var hasSibling = Array.from(document.querySelectorAll('tr[data-format="' + fmt + '"]'))
                                      .some(r => r.style.display !== 'none');
                hdr.style.display = hasSibling ? '' : 'none';
            }});
        }}

        // ---- Inline editing ----
        var _toastTimer = null;
        function toast(msg, isErr) {{
            var el = document.getElementById('edit-toast');
            el.textContent = msg;
            el.style.background = isErr ? '#dc2626' : '#1e293b';
            el.style.display = 'block';
            clearTimeout(_toastTimer);
            _toastTimer = setTimeout(() => el.style.display = 'none', 3000);
        }}

        var BADGE_MAP = {{
            T20: 'badge-t20', ODI: 'badge-odi', Other: 'badge-other', '': 'badge-other'
        }};

        document.querySelectorAll('td.ec').forEach(function(td) {{
            td.addEventListener('click', function() {{ startEdit(td); }});
        }});

        function startEdit(td) {{
            if (td.classList.contains('editing')) return;
            td.classList.add('editing');
            var field = td.dataset.field;
            var raw   = td.dataset.raw || '';
            var inp;
            if (field === 'format') {{
                inp = document.createElement('select');
                ['T20','ODI','Other'].forEach(function(v) {{
                    var o = document.createElement('option');
                    o.value = v; o.textContent = v;
                    if (v === raw) o.selected = true;
                    inp.appendChild(o);
                }});
            }} else if (field === 'gender') {{
                inp = document.createElement('select');
                [['M','Male (M)'],['F','Female (F)']].forEach(function(pair) {{
                    var o = document.createElement('option');
                    o.value = pair[0]; o.textContent = pair[1];
                    if (pair[0] === raw) o.selected = true;
                    inp.appendChild(o);
                }});
            }} else {{
                inp = document.createElement('input');
                inp.type = 'text';
                inp.value = raw;
            }}
            td._prevHTML = td.innerHTML;
            td.innerHTML = '';
            td.appendChild(inp);
            inp.focus();
            if (inp.select) inp.select();

            function commit() {{
                var newVal = inp.value.trim();
                if (newVal === raw) {{ cancelEdit(td); return; }}
                saveEdit(td, newVal);
            }}
            inp.addEventListener('blur', commit);
            inp.addEventListener('keydown', function(e) {{
                if (e.key === 'Enter')  {{ inp.blur(); }}
                if (e.key === 'Escape') {{ cancelEdit(td); }}
            }});
        }}

        function cancelEdit(td) {{
            td.classList.remove('editing');
            td.innerHTML = td._prevHTML || '';
        }}

        async function saveEdit(td, newVal) {{
            td.classList.add('saving');
            var eid   = td.dataset.eid;
            var field = td.dataset.field;
            try {{
                var resp = await fetch('/api/mgmt/match-override', {{
                    method: 'POST',
                    headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{event_id: eid, field: field, value: newVal}})
                }});
                var data = await resp.json();
                if (!data.ok) throw new Error(data.error || 'Unknown error');

                td.classList.remove('saving', 'editing');
                td.dataset.raw = newVal;

                // Re-render cell content
                if (field === 'format') {{
                    var cls = BADGE_MAP[newVal] || 'badge-other';
                    td.innerHTML = '<span class="' + cls + '">' + newVal + '</span>';
                    td.closest('tr').dataset.format = newVal;
                }} else {{
                    td.textContent = newVal || '-';
                }}

                // Highlight row as overridden
                td.closest('tr').style.background = '#fffde7';
                td.classList.add('saved');
                setTimeout(() => td.classList.remove('saved'), 1200);
                toast('Saved ✓', false);
            }} catch(e) {{
                td.classList.remove('saving', 'editing');
                td.innerHTML = td._prevHTML || '';
                td.classList.add('error');
                setTimeout(() => td.classList.remove('error'), 1500);
                toast('Error: ' + e.message, true);
            }}
        }}
    </script>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render ended matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ------------------------------------------------------------------
# Live matches routes
# ------------------------------------------------------------------
