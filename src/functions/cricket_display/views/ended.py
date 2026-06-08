from ._common import (
    json, logging, os, escape, Any, Dict, List, Optional,
    func, ResourceNotFoundError,
    call_betsapi, download_json, download_required_json, format_unix_ts,
    get_bronze_container_client, get_named_container_client, safe_float, upload_json, utc_now,
    extract_bet365_current_markets,
    collect_known_leagues, load_allowed_league_ids, save_league_preferences,
    extract_innings_snapshot,
    build_simple_table_page,
)


def view_ended_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        bronze = get_bronze_container_client()
        data = download_required_json(bronze, "cricket/ended/latest/index.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return ended index")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def _fmt_score(raw: str) -> str:
    """Format score_summary_events for display: '163/9(20),167/6(19.5)' → '163/9 (20 ov), 167/6 (19.5 ov)'."""
    import re
    if not raw:
        return raw
    def _part(s):
        m = re.match(r'^(\d+(?:/\d+)?)\s*\((\d+\.?\d*)\)', s.strip())
        return f"{m.group(1)} ({m.group(2)} ov)" if m else s.strip()
    return ", ".join(_part(p) for p in raw.split(",", 1))


def view_ended_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        bronze = get_bronze_container_client()
        index = download_required_json(bronze, "cricket/ended/latest/index.json")
        matches = index.get("matches", []) if isinstance(index, dict) else []

        # Attach format — index may not have it yet for older entries; fall back to T20
        for m in matches:
            if not m.get("format"):
                m["format"] = "T20"  # all captured so far are IPL T20; update after next pipeline run

        t20_matches  = [m for m in matches if m.get("format") == "T20"]
        odi_matches  = [m for m in matches if m.get("format") != "T20"]

        def _sort(lst):
            return sorted(lst, key=lambda m: m.get("event_time_utc") or "", reverse=True)

        league_names = sorted({str(m.get("league_name") or "Unknown") for m in matches})
        league_options = '<option value="ALL">All Leagues</option>'
        for ln in league_names:
            league_options += f'<option value="{escape(ln)}">{escape(ln)}</option>'

        def _rows_html(match_list, fmt):
            html = ""
            for m in match_list:
                league_name = str(m.get("league_name") or "Unknown")
                event_id    = escape(str(m.get("event_id") or "-"))
                league_esc  = escape(league_name)
                fmt_esc     = escape(fmt)
                badge_cls   = "badge-t20" if fmt == "T20" else "badge-odi"
                html += f"""
                <tr data-league="{league_esc}" data-format="{fmt_esc}">
                    <td>{event_id}</td>
                    <td>{escape(str(m.get("fi") or "-"))}</td>
                    <td>{escape(str(m.get("match_name") or "-"))}</td>
                    <td>{league_esc}</td>
                    <td><span class="{badge_cls}">{fmt_esc}</span></td>
                    <td>{escape(_fmt_score(str(m.get("score") or ""))) or "-"}</td>
                    <td>{escape(str(m.get("event_time_utc") or "-"))}</td>
                    <td><a href="/api/matches/{event_id}/heatmap">Heatmap</a></td>
                    <td><a href="/api/prematch/{event_id}/view">Open</a></td>
                    <td><a href="/api/matches/{event_id}/innings-tracker/view">Tracker</a></td>
                    <td><a href="/api/matches/{event_id}/detailed-analysis">Analysis</a></td>
                </tr>
                """
            return html

        t20_count = len(t20_matches)
        odi_count = len(odi_matches)
        total     = len(matches)
        odi_tab_btn = (f'<button class="tab" onclick="filterFormat(\'ODI\', this)">ODI ({odi_count})</button>'
                       if odi_count else "")

        rows_html = ""
        if t20_matches:
            rows_html += f'<tr class="format-header" data-format-header="T20"><td colspan="11">T20 — {t20_count} matches</td></tr>'
            rows_html += _rows_html(_sort(t20_matches), "T20")
        if odi_matches:
            rows_html += f'<tr class="format-header" data-format-header="ODI"><td colspan="11">ODI — {odi_count} matches</td></tr>'
            rows_html += _rows_html(_sort(odi_matches), "ODI")

        html = f"""
        <!DOCTYPE html>
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
                .badge-t20 {{ background: #dbeafe; color: #1d4ed8; padding: 2px 8px;
                              border-radius: 10px; font-size: 11px; font-weight: 700; }}
                .badge-odi {{ background: #dcfce7; color: #15803d; padding: 2px 8px;
                              border-radius: 10px; font-size: 11px; font-weight: 700; }}
                a {{ color: #2563eb; font-weight: 600; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            <h1>Ended Cricket Matches</h1>
            <p class="hint">
                {total} total &nbsp;·&nbsp;
                <b>{t20_count} T20</b> &nbsp;·&nbsp;
                <b>{odi_count} ODI</b>
            </p>

            <div class="tab-bar">
                <button class="tab active" onclick="filterFormat('ALL', this)">All ({total})</button>
                <button class="tab" onclick="filterFormat('T20', this)">T20 ({t20_count})</button>
                {odi_tab_btn}
            </div>

            <div class="filter-bar">
                <label><b>League:</b></label>
                <select id="leagueFilter" onchange="applyFilters()">{league_options}</select>
            </div>

            <table>
                <thead><tr>
                    <th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>League</th>
                    <th>Format</th><th>Final Score</th><th>Date</th>
                    <th>Heatmap</th><th>Prematch</th><th>Tracker</th><th>Analysis</th>
                </tr></thead>
                <tbody id="matchTable">{rows_html}</tbody>
            </table>

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
                    // Show/hide format-header rows based on whether any visible rows follow
                    document.querySelectorAll('tr[data-format-header]').forEach(function(hdr) {{
                        var fmt = hdr.dataset.formatHeader;
                        var hasSibling = Array.from(document.querySelectorAll('tr[data-format="' + fmt + '"]'))
                                              .some(r => r.style.display !== 'none');
                        hdr.style.display = hasSibling ? '' : 'none';
                    }});
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render ended matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ------------------------------------------------------------------
# Live matches routes
# ------------------------------------------------------------------
