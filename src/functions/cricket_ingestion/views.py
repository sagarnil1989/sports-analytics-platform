"""HTTP route handler bodies — all @app.route logic lives here.

function_app.py contains only thin @app.route wrappers that call these functions.
"""
import json
import logging
from html import escape
from typing import Any, Dict, List, Optional

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError

from storage import (
    call_betsapi,
    download_json,
    download_required_json,
    format_unix_ts,
    get_named_container_client,
    safe_float,
    upload_json,
    utc_now,
)
from silver import extract_bet365_current_markets
from leagues import collect_known_leagues, load_excluded_league_ids, save_league_preferences
from innings_tracker import extract_innings_snapshot


# ------------------------------------------------------------------
# Shared HTML helpers
# ------------------------------------------------------------------

def build_simple_table_page(title: str, headers: List[str], rows_html: str, back_link: Optional[str] = None) -> str:
    back_html = f'<p><a href="{escape(back_link)}">← Back</a></p>' if back_link else ""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{escape(title)}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
            h1 {{ margin-bottom: 8px; }}
            .hint {{ color: #666; margin-bottom: 20px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
            th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; vertical-align: top; }}
            th {{ background: #222; color: white; position: sticky; top: 0; }}
            a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
            .pill {{ display: inline-block; padding: 3px 8px; background: #eee; border-radius: 999px; }}
        </style>
    </head>
    <body>
        {back_html}
        <h1>{escape(title)}</h1>
        <div class="hint">This page reads a small pre-built gold index file, so it should load quickly.</div>
        <table>
            <thead><tr>{''.join(f'<th>{escape(h)}</th>' for h in headers)}</tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </body>
    </html>
    """


# ------------------------------------------------------------------
# Prematch routes
# ------------------------------------------------------------------

def view_prematch_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/prematch/latest/index.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return prematch index")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_prematch_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        index = download_required_json(gold, "cricket/prematch/latest/index.json")
        matches = index.get("matches", []) if isinstance(index, dict) else []
        matches_sorted = sorted(matches, key=lambda m: (m.get("league_name") or "", m.get("match_name") or ""))
        league_names = sorted({str(m.get("league_name") or "Unknown") for m in matches})
        league_options = '<option value="ALL">All Leagues</option>'
        for ln in league_names:
            league_options += f'<option value="{escape(ln)}">{escape(ln)}</option>'
        rows = ""
        current_league = None
        for m in matches_sorted:
            league_name = str(m.get("league_name") or "Unknown")
            event_id = escape(str(m.get("event_id") or "-"))
            league_esc = escape(league_name)
            if league_name != current_league:
                current_league = league_name
                rows += f'<tr class="league-header" data-league="{league_esc}"><td colspan="8">{league_esc}</td></tr>'
            rows += f"""
            <tr data-league="{league_esc}">
                <td>{event_id}</td>
                <td>{escape(str(m.get("fi") or "-"))}</td>
                <td>{escape(str(m.get("match_name") or "-"))}</td>
                <td>{league_esc}</td>
                <td>{escape(str(m.get("event_time_utc") or "-"))}</td>
                <td>{escape(str(m.get("prematch_market_count") or 0))}</td>
                <td>{escape(str(m.get("prematch_selection_count") or 0))}</td>
                <td><a href="/api/prematch/{event_id}/view">Open</a></td>
            </tr>
            """
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Upcoming Prematch Matches</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                .hint {{ color: #666; margin-bottom: 16px; }}
                .filter-bar {{ margin-bottom: 16px; display: flex; align-items: center; gap: 12px; }}
                select {{ padding: 8px 12px; font-size: 14px; border: 1px solid #ccc; border-radius: 6px; }}
                table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; font-size: 14px; }}
                th {{ background: #222; color: white; position: sticky; top: 0; }}
                tr.league-header td {{ background: #e8f0fe; color: #1a3a6b; font-weight: bold; font-size: 13px; padding: 7px 10px; }}
                a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
            </style>
        </head>
        <body>
            <h1>Upcoming Prematch Matches ({index.get('match_count', len(matches))})</h1>
            <p class="hint">This page reads a small pre-built gold index file, so it should load quickly.</p>
            <div class="filter-bar">
                <label><b>League:</b></label>
                <select id="leagueFilter" onchange="filterLeague()">{league_options}</select>
            </div>
            <table>
                <thead><tr><th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>League</th><th>Start Time</th><th>Markets</th><th>Selections</th><th>Page</th></tr></thead>
                <tbody>{rows}</tbody>
            </table>
            <script>
                function filterLeague() {{
                    const sel = document.getElementById("leagueFilter").value;
                    document.querySelectorAll("tr[data-league]").forEach(r => {{
                        r.style.display = sel === "ALL" || r.dataset.league === sel ? "" : "none";
                    }});
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render prematch matches page")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_prematch_match_json(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        page = (
            download_json(gold, f"cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json")
            or download_required_json(gold, f"cricket/prematch/latest/event_id={event_id}/prematch_page.json")
        )
        return func.HttpResponse(json.dumps(page, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return prematch match page JSON")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_save_prematch_results(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        body = req.get_json()
        new_results = body.get("results", {})
        existing = download_json(gold, f"cricket/prematch/results/event_id={event_id}/results.json") or {}
        existing.update(new_results)
        upload_json(gold, f"cricket/prematch/results/event_id={event_id}/results.json", existing, overwrite=True)
        return func.HttpResponse(json.dumps({"ok": True, "saved": len(new_results)}), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to save prematch results")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(ex)}), status_code=500, mimetype="application/json")


def view_prematch_match_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        market_filter = req.params.get("market")
        gold = get_named_container_client("gold")
        page = (
            download_json(gold, f"cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json")
            or download_required_json(gold, f"cricket/prematch/latest/event_id={event_id}/prematch_page.json")
        )
        saved_results = download_json(gold, f"cricket/prematch/results/event_id={event_id}/results.json") or {}
        header = page.get("match_header", {}) or {}
        markets = (page.get("prematch_markets", {}) or {}).get("records", [])

        home_team_name = ((header.get("home_team") or {}).get("name")) or "Home Team"
        away_team_name = ((header.get("away_team") or {}).get("name")) or "Away Team"

        def team_label(value: Any) -> str:
            raw = str(value or "").strip()
            if raw == "1":
                return home_team_name
            if raw == "2":
                return away_team_name
            return raw or "-"

        def sel_key(mn: str, cat: str, sn: str, hcap: str, suffix: str = "") -> str:
            base = f"{mn}||{cat}||{sn}||{hcap or '-'}"
            return f"{base}||{suffix}" if suffix else base

        def single_opts(sv: str) -> str:
            def o(val: str, lbl: str) -> str:
                return f'<option value="{val}"{" selected" if sv == val else ""}>{lbl}</option>'
            return o("pending", "Pending") + o("pass", "Pass") + o("fail", "Fail")

        def pair_opts(combined: str) -> str:
            def o(val: str, lbl: str) -> str:
                return f'<option value="{val}"{" selected" if combined == val else ""}>{lbl}</option>'
            return o("pending", "Pending") + o("over_wins", "Over Wins ↑") + o("under_wins", "Under Wins ↓")

        if market_filter:
            markets = [
                m for m in markets
                if str(m.get("market_key") or "").lower() == market_filter.lower()
                or str(m.get("market_name") or "").lower() == market_filter.lower()
            ]

        market_names = sorted({str(m.get("market_name") or m.get("market_key") or "-") for m in markets})
        grouped_markets: Dict[str, List[Dict[str, Any]]] = {}
        for m in markets:
            mn = str(m.get("market_name") or m.get("market_key") or "Unknown Market")
            grouped_markets.setdefault(mn, []).append(m)

        sections_html = ""
        for market_name in sorted(grouped_markets.keys()):
            selections = grouped_markets[market_name]
            pair_groups: Dict[tuple, Dict[str, Any]] = {}
            for idx, m in enumerate(selections):
                sh = str(m.get("selection_header") or "").strip()
                sn = str(m.get("selection_name") or "").strip()
                if sh in ("Over", "Under") and sn:
                    cat = str(m.get("category_key") or "-")
                    hcap = str(m.get("handicap") or "-")
                    pk = (cat, sn, hcap)
                    pair_groups.setdefault(pk, {})
                    pair_groups[pk][sh] = (idx, m)
            complete_pairs = {pk: g for pk, g in pair_groups.items() if "Over" in g and "Under" in g}
            paired_indices: set = set()
            for pk, g in complete_pairs.items():
                paired_indices.add(g["Over"][0])
                paired_indices.add(g["Under"][0])
            rows_html = ""
            for idx, m in enumerate(selections[:1000]):
                cat = str(m.get("category_key") or "-")
                sn_raw = str(m.get("selection_name") or "").strip()
                sh_raw = str(m.get("selection_header") or "").strip()
                hcap = str(m.get("handicap") or "-")
                if idx in paired_indices:
                    if sh_raw == "Under":
                        continue
                    pk = (cat, sn_raw, hcap)
                    over_m = complete_pairs[pk]["Over"][1]
                    under_m = complete_pairs[pk]["Under"][1]
                    option_display = team_label(sn_raw)
                    over_key = sel_key(market_name, cat, sn_raw, hcap, "Over")
                    under_key = sel_key(market_name, cat, sn_raw, hcap, "Under")
                    over_val = saved_results.get(over_key, "pending")
                    under_val = saved_results.get(under_key, "pending")
                    if over_val == "pass" and under_val == "fail":
                        combined = "over_wins"
                    elif under_val == "pass" and over_val == "fail":
                        combined = "under_wins"
                    else:
                        combined = "pending"
                    over_odds = escape(str(over_m.get("odds") or "-"))
                    under_odds = escape(str(under_m.get("odds") or "-"))
                    over_key_esc = escape(over_key)
                    under_key_esc = escape(under_key)
                    rows_html += f"""
                    <tr class="pair-row">
                        <td>{escape(cat)}</td>
                        <td>{escape(option_display)}</td>
                        <td>Over / Under</td>
                        <td>{escape(hcap) if hcap != "-" else "-"}</td>
                        <td>&#8593;{over_odds} / &#8595;{under_odds}</td>
                        <td>{escape(str(over_m.get("category_updated_at_utc") or "-"))}</td>
                        <td><select class="result-sel result-sel-{combined}"
                                data-over-key="{over_key_esc}" data-under-key="{under_key_esc}"
                                data-type="pair" onchange="onPairChange(this)">{pair_opts(combined)}</select></td>
                    </tr>
                    """
                else:
                    option_raw = sn_raw or sh_raw or "-"
                    option_display = team_label(option_raw)
                    team_player = team_label(sh_raw)
                    if team_player == option_display or team_player == option_raw:
                        team_player = "-"
                    key = sel_key(market_name, cat, option_raw, hcap)
                    saved_val = saved_results.get(key, "pending")
                    key_esc = escape(key)
                    rows_html += f"""
                    <tr>
                        <td>{escape(cat)}</td>
                        <td>{escape(option_display)}</td>
                        <td>{escape(team_player)}</td>
                        <td>{escape(hcap)}</td>
                        <td>{escape(str(m.get("odds") or "-"))}</td>
                        <td>{escape(str(m.get("category_updated_at_utc") or "-"))}</td>
                        <td><select class="result-sel result-sel-{saved_val}" data-key="{key_esc}"
                                onchange="onSelChange(this)">{single_opts(saved_val)}</select></td>
                    </tr>
                    """
            sections_html += f"""
            <div class="market-section">
                <h2>{escape(market_name)} <span>{len(selections)} selections</span></h2>
                <table>
                    <thead><tr>
                        <th>Category</th><th>Option</th><th>Team / Player</th>
                        <th>Line</th><th>Odds</th><th>Updated</th><th>Result</th>
                    </tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>
            """

        title = header.get("match_name") or f"Prematch {event_id}"
        market_summary = ", ".join(market_names[:20])
        if len(market_names) > 20:
            market_summary += f" ... +{len(market_names) - 20} more"

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{escape(title)}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                .card {{ background: white; padding: 18px; border-radius: 10px; box-shadow: 0 2px 8px #ddd; margin-bottom: 20px; }}
                .save-bar {{ position: sticky; top: 0; z-index: 100; background: #222; color: white; padding: 10px 20px; display: flex; align-items: center; gap: 16px; border-radius: 8px; margin-bottom: 20px; }}
                .save-bar button {{ padding: 8px 22px; font-size: 15px; font-weight: bold; border: none; border-radius: 6px; cursor: pointer; background: #4caf50; color: white; }}
                .save-bar button:hover {{ background: #388e3c; }}
                #save-status {{ font-size: 14px; }}
                .market-section {{ background: white; padding: 18px; border-radius: 10px; box-shadow: 0 2px 8px #ddd; margin: 24px 0; }}
                .market-section h2 {{ margin-top: 0; border-bottom: 2px solid #222; padding-bottom: 8px; }}
                .market-section h2 span {{ color: #666; font-size: 16px; font-weight: normal; }}
                table {{ width: 100%; border-collapse: collapse; background: white; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; font-size: 14px; }}
                th {{ background: #222; color: white; position: sticky; top: 48px; }}
                tr.pair-row td {{ background: #f0f4ff; }}
                a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
                select.result-sel {{ padding: 4px 8px; border-radius: 6px; font-weight: bold; font-size: 13px; border: 2px solid #ccc; cursor: pointer; }}
                select.result-sel-pending   {{ background: #eee;    color: #555;    border-color: #ccc;    }}
                select.result-sel-pass      {{ background: #d4edda; color: #155724; border-color: #28a745; }}
                select.result-sel-fail      {{ background: #f8d7da; color: #721c24; border-color: #dc3545; }}
                select.result-sel-over_wins  {{ background: #d4edda; color: #155724; border-color: #28a745; }}
                select.result-sel-under_wins {{ background: #fff3cd; color: #856404; border-color: #ffc107; }}
            </style>
        </head>
        <body>
            <p><a href="/api/prematch/view">&#8592; Back to prematch matches</a></p>
            <div class="save-bar">
                <button onclick="saveResults()">Save Results</button>
                <span id="save-status"></span>
            </div>
            <div class="card">
                <h1>{escape(str(title))}</h1>
                <p><b>League:</b> {escape(str(header.get("league_name") or "-"))}</p>
                <p><b>Teams:</b> {escape(str(home_team_name))} vs {escape(str(away_team_name))}</p>
                <p><b>Event ID:</b> {escape(str(event_id))} &nbsp; <b>Bet365 FI:</b> {escape(str((page.get("snapshot") or {{}}).get("fi") or "-"))}</p>
                <p><b>Start time:</b> {escape(str(header.get("event_time_utc") or "-"))}</p>
                <p><b>Markets:</b> {escape(str((page.get("prematch_markets") or {{}}).get("market_count") or 0))}
                   &nbsp; <b>Selections:</b> {escape(str((page.get("prematch_markets") or {{}}).get("selection_count") or 0))}</p>
                <p><b>Market names:</b> {escape(market_summary or "-")}</p>
            </div>
            {sections_html}
            <script>
                function onSelChange(el) {{ el.className = 'result-sel result-sel-' + el.value; }}
                function onPairChange(el) {{ el.className = 'result-sel result-sel-' + el.value; }}
                async function saveResults() {{
                    const status = document.getElementById('save-status');
                    status.textContent = 'Saving...';
                    status.style.color = '#aaa';
                    const results = {{}};
                    document.querySelectorAll('.result-sel[data-key]').forEach(s => {{ results[s.dataset.key] = s.value; }});
                    document.querySelectorAll('.result-sel[data-type="pair"]').forEach(s => {{
                        const v = s.value;
                        if (v === 'over_wins') {{ results[s.dataset.overKey] = 'pass'; results[s.dataset.underKey] = 'fail'; }}
                        else if (v === 'under_wins') {{ results[s.dataset.overKey] = 'fail'; results[s.dataset.underKey] = 'pass'; }}
                        else {{ results[s.dataset.overKey] = 'pending'; results[s.dataset.underKey] = 'pending'; }}
                    }});
                    try {{
                        const resp = await fetch('/api/prematch/{event_id}/results', {{
                            method: 'POST', headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{results}})
                        }});
                        const data = await resp.json();
                        if (data.ok) {{ status.textContent = 'Saved ' + data.saved + ' results ✓'; status.style.color = '#4caf50'; }}
                        else {{ status.textContent = 'Save failed: ' + data.error; status.style.color = '#f44336'; }}
                    }} catch (e) {{ status.textContent = 'Error: ' + e.message; status.style.color = '#f44336'; }}
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render prematch match page")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_prematch_leagues(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/prematch/leagues/index.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return prematch leagues")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_prematch_leagues_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        index = download_required_json(gold, "cricket/prematch/leagues/index.json")
        rows = ""
        for league in index.get("leagues", []):
            league_id = escape(str(league.get("league_id") or "unknown"))
            rows += f"""
            <tr>
                <td>{league_id}</td>
                <td>{escape(str(league.get("league_name") or "-"))}</td>
                <td>{escape(str(league.get("match_count") or 0))}</td>
                <td><a href="/api/prematch/leagues/{league_id}/matches/view">Open</a></td>
            </tr>
            """
        html = build_simple_table_page(
            f"Prematch Leagues ({index.get('league_count', 0)})",
            ["League ID", "League", "Upcoming Matches", "Page"],
            rows,
            back_link="/api/prematch/view",
        )
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render prematch leagues")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_prematch_league_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        league_id = req.route_params.get("league_id")
        gold = get_named_container_client("gold")
        data = download_required_json(gold, f"cricket/prematch/leagues/{league_id}/matches.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return prematch league matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_prematch_league_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        league_id = req.route_params.get("league_id")
        gold = get_named_container_client("gold")
        data = download_required_json(gold, f"cricket/prematch/leagues/{league_id}/matches.json")
        rows = ""
        for m in data.get("matches", []):
            event_id = escape(str(m.get("event_id") or "-"))
            rows += f"""
            <tr>
                <td>{event_id}</td>
                <td>{escape(str(m.get("fi") or "-"))}</td>
                <td>{escape(str(m.get("match_name") or "-"))}</td>
                <td>{escape(str(m.get("event_time_utc") or "-"))}</td>
                <td>{escape(str(m.get("prematch_market_count") or 0))}</td>
                <td>{escape(str(m.get("prematch_selection_count") or 0))}</td>
                <td><a href="/api/prematch/{event_id}/view">Open</a></td>
            </tr>
            """
        html = build_simple_table_page(
            f"{data.get('league_name', 'Prematch League')} ({data.get('match_count', 0)})",
            ["Event ID", "Bet365 FI", "Match", "Start Time", "Markets", "Selections", "Page"],
            rows,
            back_link="/api/prematch/leagues/view",
        )
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render prematch league matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ------------------------------------------------------------------
# Ended matches routes
# ------------------------------------------------------------------

def view_ended_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/ended/latest/index.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return ended index")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_ended_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        index = download_required_json(gold, "cricket/ended/latest/index.json")
        matches = index.get("matches", []) if isinstance(index, dict) else []
        matches_sorted = sorted(matches, key=lambda m: m.get("event_time_utc") or m.get("event_time_unix") or "", reverse=True)
        league_names = sorted({str(m.get("league_name") or "Unknown") for m in matches})
        league_options = '<option value="ALL">All Leagues</option>'
        for ln in league_names:
            league_options += f'<option value="{escape(ln)}">{escape(ln)}</option>'
        rows = ""
        for m in matches_sorted:
            league_name = str(m.get("league_name") or "Unknown")
            event_id = escape(str(m.get("event_id") or "-"))
            league_esc = escape(league_name)
            rows += f"""
            <tr data-league="{league_esc}">
                <td>{event_id}</td>
                <td>{escape(str(m.get("fi") or "-"))}</td>
                <td>{escape(str(m.get("match_name") or "-"))}</td>
                <td>{league_esc}</td>
                <td>{escape(str(m.get("score") or "-"))}</td>
                <td>{escape(str(m.get("event_time_utc") or "-"))}</td>
                <td><a href="/api/matches/{event_id}/heatmap">Heatmap</a></td>
                <td><a href="/api/prematch/{event_id}/view">Open</a></td>
                <td><a href="/api/matches/{event_id}/innings-tracker/view">Tracker</a></td>
            </tr>
            """
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Ended Cricket Matches</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                .hint {{ color: #666; margin-bottom: 16px; }}
                .filter-bar {{ margin-bottom: 16px; display: flex; align-items: center; gap: 12px; }}
                select {{ padding: 8px 12px; font-size: 14px; border: 1px solid #ccc; border-radius: 6px; }}
                table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; font-size: 14px; }}
                th {{ background: #222; color: white; position: sticky; top: 0; }}
                a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
            </style>
        </head>
        <body>
            <h1>Ended Cricket Matches ({index.get('ended_match_count', len(matches))})</h1>
            <p class="hint">This page reads a small pre-built gold index file, so it should load quickly.</p>
            <div class="filter-bar">
                <label><b>League:</b></label>
                <select id="leagueFilter" onchange="filterLeague()">{league_options}</select>
            </div>
            <table>
                <thead><tr><th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>League</th><th>Final Score</th><th>Start Time</th><th>Heatmap</th><th>Prematch Odds</th><th>Innings Tracker</th></tr></thead>
                <tbody>{rows}</tbody>
            </table>
            <script>
                function filterLeague() {{
                    const sel = document.getElementById("leagueFilter").value;
                    document.querySelectorAll("tr[data-league]").forEach(r => {{
                        r.style.display = sel === "ALL" || r.dataset.league === sel ? "" : "none";
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

def view_latest_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/matches/latest/index.json")
        return func.HttpResponse(json.dumps(data), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get latest matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_match_page(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        data = (
            download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
            or download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
        )
        return func.HttpResponse(json.dumps(data), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get match page")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_matches_list_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        index = download_required_json(gold, "cricket/matches/latest/index.json")
        matches = index.get("matches", []) if isinstance(index, dict) else []
        matches_sorted = sorted(matches, key=lambda m: m.get("snapshot_time_utc") or "", reverse=True)
        league_names = sorted({str(m.get("league_name") or "Unknown") for m in matches})
        league_options = '<option value="ALL">All Leagues</option>'
        for ln in league_names:
            league_options += f'<option value="{escape(ln)}">{escape(ln)}</option>'
        rows = ""
        for m in matches_sorted:
            league_name = str(m.get("league_name") or "Unknown")
            event_id = escape(str(m.get("event_id") or "-"))
            fi = escape(str(m.get("fi") or "-"))
            match_name = escape(str(m.get("match_name") or "-"))
            league_id = escape(str(m.get("league_id") or "unknown"))
            league_esc = escape(league_name)
            score = escape(str(m.get("score_summary") or "-"))
            markets = escape(str(m.get("current_market_count") or 0))
            selections = escape(str(m.get("current_market_selection_count") or 0))
            snapshot_time = escape(str(m.get("snapshot_time_utc") or "-"))
            rows += f"""
            <tr data-league="{league_esc}">
                <td>{event_id}</td><td>{fi}</td><td>{match_name}</td>
                <td><a href="/api/leagues/{league_id}/matches/view">{league_esc}</a></td>
                <td>{score}</td><td>{markets}</td><td>{selections}</td><td>{snapshot_time}</td>
                <td><a href="/api/matches/{event_id}/view">Open</a></td>
                <td><a href="/api/matches/{event_id}/heatmap">Heatmap</a></td>
                <td><a href="/api/matches/{event_id}/innings-tracker/view">Tracker</a></td>
            </tr>
            """
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Live Cricket Matches</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                .nav {{ margin-bottom: 16px; }}
                .filter-bar {{ margin-bottom: 16px; display: flex; align-items: center; gap: 12px; }}
                select {{ padding: 8px 12px; font-size: 14px; border: 1px solid #ccc; border-radius: 6px; }}
                table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; font-size: 14px; }}
                th {{ background: #222; color: white; }}
                a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
                .live-badge {{ display: inline-block; background: #d00; color: white; font-size: 13px; font-weight: bold; padding: 3px 10px; border-radius: 999px; vertical-align: middle; margin-left: 10px; letter-spacing: 0.5px; }}
            </style>
        </head>
        <body>
            <h1>Live Cricket Matches ({len(matches)}) <span class="live-badge">LIVE</span></h1>
            <div class="nav"><a href="/api/leagues/view">View leagues</a> | <a href="/api/matches">JSON</a></div>
            <div class="filter-bar">
                <label><b>League:</b></label>
                <select id="leagueFilter" onchange="filterLeague()">{league_options}</select>
            </div>
            <table>
                <thead>
                    <tr><th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>League</th><th>Score</th>
                    <th>Markets</th><th>Selections</th><th>Last Snapshot</th><th>Page</th><th>Heatmap</th><th>Innings Tracker</th></tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            <script>
                function filterLeague() {{
                    const sel = document.getElementById("leagueFilter").value;
                    document.querySelectorAll("tr[data-league]").forEach(r => {{
                        r.style.display = sel === "ALL" || r.dataset.league === sel ? "" : "none";
                    }});
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except ResourceNotFoundError:
        return func.HttpResponse("Index not created yet. Wait for gold_build_match_pages to run.", status_code=404)
    except Exception as ex:
        logging.exception("Failed to render matches list")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ------------------------------------------------------------------
# Leagues routes
# ------------------------------------------------------------------

def view_leagues(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/leagues/index.json")
        return func.HttpResponse(json.dumps(data, ensure_ascii=False, indent=2), status_code=200, mimetype="application/json")
    except ResourceNotFoundError:
        return func.HttpResponse(json.dumps({"league_count": 0, "leagues": []}), status_code=404, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get leagues")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_leagues_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        index = download_required_json(gold, "cricket/leagues/index.json")
        leagues = index.get("leagues", []) if isinstance(index, dict) else []
        rows = ""
        for l in leagues:
            league_id = escape(str(l.get("league_id") or "unknown"))
            league_name = escape(str(l.get("league_name") or "Unknown League"))
            match_count = escape(str(l.get("match_count") or 0))
            latest = escape(str(l.get("latest_snapshot_time_utc") or "-"))
            rows += f"""
            <tr><td>{league_id}</td><td>{league_name}</td><td>{match_count}</td><td>{latest}</td>
            <td><a href="/api/leagues/{league_id}/matches/view">Open matches</a></td></tr>
            """
        html = f"""
        <!DOCTYPE html><html><head><title>Cricket Leagues</title>
        <style>body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
        th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }}
        th {{ background: #222; color: white; }} a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}</style>
        </head><body><h1>Cricket Leagues ({len(leagues)})</h1><p><a href="/api/matches/view">All matches</a></p>
        <table><thead><tr><th>League ID</th><th>League</th><th>Matches</th><th>Latest Snapshot</th><th>Page</th></tr></thead>
        <tbody>{rows}</tbody></table></body></html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except ResourceNotFoundError:
        return func.HttpResponse("League index not created yet. Wait for gold_build_match_pages to run.", status_code=404)
    except Exception as ex:
        logging.exception("Failed to render leagues")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_league_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        league_id = req.route_params.get("league_id") or "unknown"
        gold = get_named_container_client("gold")
        data = download_required_json(gold, f"cricket/leagues/{league_id}/matches.json")
        return func.HttpResponse(json.dumps(data, ensure_ascii=False, indent=2), status_code=200, mimetype="application/json")
    except ResourceNotFoundError:
        return func.HttpResponse(json.dumps({"match_count": 0, "matches": []}), status_code=404, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get league matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_league_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        league_id = req.route_params.get("league_id") or "unknown"
        gold = get_named_container_client("gold")
        data = download_required_json(gold, f"cricket/leagues/{league_id}/matches.json")
        matches = data.get("matches", []) if isinstance(data, dict) else []
        league_name = escape(str(data.get("league_name") or league_id)) if isinstance(data, dict) else escape(str(league_id))
        rows = ""
        for m in matches:
            event_id = escape(str(m.get("event_id") or "-"))
            fi = escape(str(m.get("fi") or "-"))
            match_name = escape(str(m.get("match_name") or "-"))
            score = escape(str(m.get("score_summary") or "-"))
            markets = escape(str(m.get("current_market_count") or 0))
            selections = escape(str(m.get("current_market_selection_count") or 0))
            snapshot_time = escape(str(m.get("snapshot_time_utc") or "-"))
            rows += f"""
            <tr><td>{event_id}</td><td>{fi}</td><td>{match_name}</td><td>{score}</td>
            <td>{markets}</td><td>{selections}</td><td>{snapshot_time}</td>
            <td><a href="/api/matches/{event_id}/view">Open</a></td></tr>
            """
        html = f"""
        <!DOCTYPE html><html><head><title>{league_name} Matches</title>
        <style>body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
        th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }}
        th {{ background: #222; color: white; }} a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}</style>
        </head><body><h1>{league_name} ({len(matches)} matches)</h1>
        <p><a href="/api/leagues/view">Back to leagues</a> | <a href="/api/matches/view">All matches</a></p>
        <table><thead><tr><th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>Score</th>
        <th>Markets</th><th>Selections</th><th>Last Snapshot</th><th>Page</th></tr></thead>
        <tbody>{rows}</tbody></table></body></html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except ResourceNotFoundError:
        return func.HttpResponse("League matches index not found yet.", status_code=404)
    except Exception as ex:
        logging.exception("Failed to render league matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ------------------------------------------------------------------
# Match detail routes
# ------------------------------------------------------------------

def view_match_page_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        data = (
            download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
            or download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
        )
        if data is None:
            eid_esc = escape(str(event_id))
            return func.HttpResponse(
                f"""<!DOCTYPE html><html><head><title>Match Not Found</title>
<style>body{{font-family:Arial,sans-serif;margin:40px;background:#f7f7f7}}
.box{{background:white;padding:30px;border-radius:10px;box-shadow:0 2px 8px #ddd;max-width:600px}}
a{{color:#0066cc}}</style></head>
<body><div class="box">
<h2>Live match data not available for event {eid_esc}</h2>
<p>This match may have ended before the pipeline captured a full live snapshot.</p>
<ul>
  <li><a href="/api/prematch/{eid_esc}/view">View prematch odds for this match</a></li>
  <li><a href="/api/ended/view">Ended matches</a></li>
  <li><a href="/api/matches/view">Current live matches</a></li>
</ul>
</div></body></html>""",
                status_code=404, mimetype="text/html",
            )

        header = data.get("match_header", {})
        score = data.get("score", {})
        odds = data.get("odds", {}).get("records", [])
        snapshot = data.get("snapshot") or {}
        fi = snapshot.get("fi") or (data.get("current_markets") or {}).get("fi") or header.get("fi")

        current_market_rows: List[Dict[str, Any]] = []
        markets_source = "cached"
        if fi:
            try:
                live_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi})
                if live_payload["response"]["success"]:
                    now = utc_now().isoformat()
                    current_market_rows = extract_bet365_current_markets(live_payload, "live", now, event_id, fi, {})
                    markets_source = "live"
            except Exception:
                pass

        if not current_market_rows:
            cached = data.get("current_markets", {})
            current_market_rows = cached.get("records", []) if isinstance(cached, dict) else []
            markets_source = "cached"

        match_name = header.get("match_name") or (
            f"{header.get('home_team', {}).get('name')} vs {header.get('away_team', {}).get('name')}"
            if header.get("home_team", {}).get("name") and header.get("away_team", {}).get("name") else "Match"
        )
        score_text = score.get("summary_from_events") or score.get("summary_from_bet365") or "-"
        venue_text = escape(str(header.get("venue") or ""))
        match_date_text = escape(str(header.get("event_time_utc") or "")[:10])

        current_market_names = sorted({m.get("market_group_name") or "Unknown Market" for m in current_market_rows})
        market_options = '<option value="ALL">All current markets</option>'
        for market_name in current_market_names:
            safe_value = str(market_name).replace('"', '&quot;')
            market_options += f'<option value="{safe_value}">{market_name}</option>'

        current_market_table_rows = ""
        for m in current_market_rows[:1500]:
            market_group_name = m.get("market_group_name") or "Unknown Market"
            market_name = m.get("market_name") or market_group_name
            odds_display = (
                f"{m.get('odds_decimal')} <span style='color:#777;'>({m.get('odds_fractional') or m.get('odds')})</span>"
                if m.get("odds_decimal") is not None
                else (m.get("odds_fractional") or m.get("odds") or "-")
            )
            current_market_table_rows += f"""
            <tr data-current-market="{str(market_group_name).replace('"', '&quot;')}">
                <td>{market_group_name}</td>
                <td>{market_name}</td>
                <td>{m.get('display_selection_name') or m.get('selection_name') or '-'}</td>
                <td>{odds_display}</td>
                <td>{m.get('handicap') or '-'}</td>
                <td>{'Yes' if m.get('suspended') else 'No'}</td>
            </tr>
            """

        home_name = escape(str((header.get("home_team") or {}).get("name") or "Home"))
        away_name = escape(str((header.get("away_team") or {}).get("name") or "Away"))

        tracker_data = download_json(gold, f"cricket/innings_tracker/event_id={event_id}/innings_1.json") or {}
        tracker_rows = tracker_data.get("rows", [])

        odds_rows = ""
        for r in tracker_rows:
            score_val = r.get("score")
            wickets_val = r.get("wickets")
            score_display = (
                f"{score_val}/{wickets_val}" if score_val is not None and wickets_val is not None
                else str(score_val) if score_val is not None else "-"
            )
            odds_rows += f"""
            <tr>
                <td>{escape(str(r.get('over') or '-'))}</td>
                <td>{escape(score_display)}</td>
                <td>{escape(str(r.get('home_team_odds') or '-'))}</td>
                <td>{escape(str(r.get('away_team_odds') or '-'))}</td>
                <td>{escape(str((r.get('snapshot_time_utc') or '')[:19].replace('T', ' ')))}</td>
            </tr>
            """
        if not odds_rows:
            odds_rows = '<tr><td colspan="5" style="color:#999;">No Match Winner timeline data captured yet.</td></tr>'

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{match_name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                h1 {{ margin-bottom: 5px; }}
                .score {{ font-size: 22px; margin-bottom: 8px; }}
                .meta {{ color: #555; font-size: 14px; margin-bottom: 4px; }}
                .cards {{ display: flex; gap: 15px; margin-bottom: 25px; flex-wrap: wrap; }}
                .card {{ background: white; padding: 18px; border-radius: 10px; box-shadow: 0 2px 8px #ddd; min-width: 160px; }}
                .label {{ color: #666; font-size: 13px; }}
                .value {{ font-size: 24px; font-weight: bold; }}
                .section {{ margin-top: 35px; }}
                select {{ padding: 10px; font-size: 16px; margin-bottom: 15px; }}
                table {{ width: 100%; border-collapse: collapse; background: white; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }}
                th {{ background: #222; color: white; position: sticky; top: 0; }}
            </style>
        </head>
        <body>
            <h1>{match_name}</h1>
            <div class="score">Score: {score_text}</div>
            {f'<div class="meta">📍 {venue_text}</div>' if venue_text else ""}
            {f'<div class="meta">📅 {match_date_text}</div>' if match_date_text else ""}
            <p>
                <a href="/api/matches/{event_id}/lineage/view">View data lineage</a> |
                <a href="/api/matches/{event_id}/innings-tracker/view">Innings Tracker</a> |
                <a href="/api/matches/{event_id}/heatmap">Market Heatmap</a>
            </p>
            <div class="cards">
                <div class="card"><div class="label">Current Markets</div><div class="value">{len({m.get('market_group_id') or m.get('market_group_name') for m in current_market_rows})}</div></div>
                <div class="card"><div class="label">Current Selections</div><div class="value">{len(current_market_rows)}</div></div>
                <div class="card"><div class="label">Odds History Rows</div><div class="value">{len(odds)}</div></div>
                <div class="card"><div class="label">Event ID</div><div class="value" style="font-size:16px;">{event_id}</div></div>
            </div>
            <div class="section">
                <h2>Current Bet365 Markets <span style="font-size:13px;font-weight:normal;color:#888;">({markets_source})</span></h2>
                <label><b>Filter market:</b></label><br>
                <select id="currentMarketFilter" onchange="filterCurrentMarket()">{market_options}</select>
                <table>
                    <thead><tr><th>Group</th><th>Market</th><th>Selection</th><th>Odds Decimal (Fractional)</th><th>Line / Handicap</th><th>Suspended</th></tr></thead>
                    <tbody>{current_market_table_rows}</tbody>
                </table>
            </div>
            <div class="section">
                <h2>Match Winner 2-Way Timeline <span style="font-size:13px;font-weight:normal;color:#888;">({len(tracker_rows)} snapshots from innings tracker)</span></h2>
                <table>
                    <thead><tr><th>Over</th><th>Score/Wkts</th><th>{home_name}</th><th>{away_name}</th><th>Time (UTC)</th></tr></thead>
                    <tbody>{odds_rows}</tbody>
                </table>
            </div>
            <script>
                function filterCurrentMarket() {{
                    const selected = document.getElementById("currentMarketFilter").value;
                    document.querySelectorAll("tr[data-current-market]").forEach(row => {{
                        const market = row.getAttribute("data-current-market");
                        row.style.display = selected === "ALL" || market === selected ? "" : "none";
                    }});
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render match HTML")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_match_lineage_json(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        lineage = download_json(gold, f"cricket/matches/latest/event_id={event_id}/lineage.json")
        if not lineage:
            page = (
                download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
                or download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
            )
            lineage = page.get("data_lineage") or {}
        return func.HttpResponse(json.dumps(lineage, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return match lineage")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_match_lineage_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        lineage = download_json(gold, f"cricket/matches/latest/event_id={event_id}/lineage.json")
        if not lineage:
            page = (
                download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
                or download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
            )
            lineage = page.get("data_lineage") or {}

        id_mapping = lineage.get("id_mapping", {}) if isinstance(lineage, dict) else {}
        rows = ""
        for call in lineage.get("api_calls", []) if isinstance(lineage, dict) else []:
            rows += f"""
            <tr>
                <td>{escape(str(call.get('api_name') or '-'))}</td>
                <td>{escape(str(call.get('purpose') or '-'))}</td>
                <td><pre>{escape(json.dumps(call.get('id_used') or {}, ensure_ascii=False))}</pre></td>
                <td>{escape(str(call.get('success')))}</td>
                <td>{escape(str(call.get('http_status_code') or '-'))}</td>
                <td>{escape(str(call.get('result_count') if call.get('result_count') is not None else '-'))}</td>
                <td>{escape(str(call.get('elapsed_ms') or '-'))}</td>
                <td>{escape(str(call.get('bronze_path') or '-'))}</td>
            </tr>
            """
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Lineage - {escape(str(event_id))}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                .card {{ background: white; padding: 18px; border-radius: 10px; box-shadow: 0 2px 8px #ddd; margin-bottom: 20px; }}
                table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; vertical-align: top; font-size: 14px; }}
                th {{ background: #222; color: white; position: sticky; top: 0; }}
                pre {{ white-space: pre-wrap; margin: 0; }}
                a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
            </style>
        </head>
        <body>
            <p><a href="/api/matches/{escape(str(event_id))}/view">← Back to match</a> | <a href="/api/matches/{escape(str(event_id))}/lineage">JSON</a></p>
            <div class="card">
                <h1>Data Lineage</h1>
                <p><b>Event ID:</b> {escape(str(lineage.get('event_id') or event_id))}</p>
                <p><b>Bet365 FI:</b> {escape(str(lineage.get('fi') or '-'))}</p>
                <p><b>Bronze base path:</b> {escape(str(lineage.get('bronze_base_path') or '-'))}</p>
                <p><b>ID mapping:</b></p>
                <pre>{escape(json.dumps(id_mapping, indent=2, ensure_ascii=False))}</pre>
            </div>
            <table>
                <thead>
                    <tr><th>API</th><th>Purpose</th><th>ID Used</th><th>Success</th><th>HTTP</th>
                    <th>Result Count</th><th>Elapsed ms</th><th>Bronze Raw File</th></tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render match lineage")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ------------------------------------------------------------------
# Innings tracker routes
# ------------------------------------------------------------------

def view_innings_tracker_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        tracker = download_json(gold, f"cricket/innings_tracker/event_id={event_id}/innings_1.json") or {}

        match_name = escape(str(tracker.get("match_name") or f"Match {event_id}"))
        venue = escape(str(tracker.get("venue") or ""))
        match_date = escape(str(tracker.get("match_date_utc") or "")[:10])
        league = escape(str(tracker.get("league_name") or ""))
        home_team = str(tracker.get("home_team_name") or "Home")
        away_team = str(tracker.get("away_team_name") or "Away")
        rows_data: List[Dict[str, Any]] = tracker.get("rows", [])
        outcome = tracker.get("outcome")
        actual_total = tracker.get("actual_total")

        timeline_rows = sorted(
            [r for r in rows_data if r.get("predicted_total") is not None],
            key=lambda r: str(r.get("snapshot_time_utc") or ""),
        )
        innings_market = (
            next((r.get("innings_market_name") for r in reversed(rows_data) if r.get("innings_market_name")), None)
        )
        is_live = (actual_total is None and outcome is None)

        over_count = sum(1 for r in timeline_rows if actual_total is not None and actual_total > r["predicted_total"])
        under_count = sum(1 for r in timeline_rows if actual_total is not None and actual_total < r["predicted_total"])
        total_decided = over_count + under_count
        over_pct = round(100 * over_count / total_decided, 1) if total_decided else None
        under_pct = round(100 * under_count / total_decided, 1) if total_decided else None

        summary_html = ""
        if is_live and timeline_rows:
            latest = timeline_rows[-1]
            summary_html = f"""
            <div class="live-banner">
                <span class="live-dot">●</span> LIVE &nbsp;|&nbsp;
                Over {escape(str(latest['over']) if latest.get('over') is not None else '?')}, Score {escape(str(latest['score']) if latest.get('score') is not None else '?')} &nbsp;|&nbsp;
                <b>Current prediction: {latest['predicted_total']} runs</b>
            </div>"""
        elif total_decided:
            bias = ""
            if over_pct and over_pct >= 70:
                bias = f' <span class="bias-tag">⚠ OVER biased</span>'
            elif under_pct and under_pct >= 70:
                bias = f' <span class="bias-tag under-bias">⚠ UNDER biased</span>'
            summary_html = f"""
            <div class="summary-cards">
                <div class="scard over">Over: {over_count} ({over_pct}%){bias}</div>
                <div class="scard under">Under: {under_count} ({under_pct}%)</div>
                <div class="scard">Actual total: <b>{actual_total}</b></div>
                <div class="scard outcome-{outcome or 'pending'}">Outcome: {(outcome or 'Pending').upper()}</div>
            </div>"""

        table_rows = ""
        for r in timeline_rows:
            pred = r["predicted_total"]
            row_class = ""
            outcome_cell = "-"
            if actual_total is not None:
                if actual_total > pred:
                    row_class = ' class="over-row"'
                    outcome_cell = "▲ Over"
                elif actual_total < pred:
                    row_class = ' class="under-row"'
                    outcome_cell = "▼ Under"
                else:
                    outcome_cell = "= Push"
            snap_time = escape(str(r.get("snapshot_time_utc") or "")[:16].replace("T", " "))
            home_odd = r.get("home_team_odds")
            away_odd = r.get("away_team_odds")
            home_odd_cell = escape(str(home_odd)) if home_odd is not None else "-"
            away_odd_cell = escape(str(away_odd)) if away_odd is not None else "-"
            table_rows += f"""
            <tr{row_class}>
                <td>{snap_time}</td>
                <td>{escape(str(r['over']) if r.get('over') is not None else '-')}</td>
                <td>{escape(str(r['score']) if r.get('score') is not None else '-')}/{escape(str(r['wickets']) if r.get('wickets') is not None else '0')}</td>
                <td><b>{pred}</b></td>
                <td style="color:#666;">{escape(str(r.get('over_odds_at_line') or '-'))} / {escape(str(r.get('under_odds_at_line') or '-'))}</td>
                <td>{home_odd_cell}</td>
                <td>{away_odd_cell}</td>
                <td>{outcome_cell}</td>
            </tr>"""

        if outcome and actual_total is not None:
            colour = "#d4edda" if outcome == "over" else "#f8d7da" if outcome == "under" else "#fff3cd"
            table_rows += f'<tr style="background:{colour};font-weight:bold;"><td colspan="8">FINAL: {actual_total} runs → {outcome.upper()}</td></tr>'

        no_data_msg = ""
        if not timeline_rows:
            if rows_data:
                no_data_msg = '<p style="color:#999;">Innings market data not available in captured snapshots — market may have been suspended or not yet offered.</p>'
            else:
                no_data_msg = '<p style="color:#999;">No tracker data yet — builds during live play.</p>'

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Innings Tracker — {match_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        h1 {{ margin-bottom: 4px; }}
        .meta {{ color: #555; font-size: 14px; margin-bottom: 3px; }}
        .summary-cards {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 18px 0; }}
        .scard {{ padding: 12px 20px; border-radius: 8px; background: white; box-shadow: 0 2px 6px #ddd; font-weight: bold; }}
        .scard.over {{ background: #d4edda; color: #155724; }}
        .scard.under {{ background: #f8d7da; color: #721c24; }}
        .scard.outcome-over {{ background: #d4edda; color: #155724; }}
        .scard.outcome-under {{ background: #f8d7da; color: #721c24; }}
        .scard.outcome-push {{ background: #fff3cd; color: #856404; }}
        .scard.outcome-pending {{ background: #eee; color: #555; }}
        .live-banner {{ background: #1a1a2e; color: #00e5ff; padding: 14px 20px; border-radius: 8px; font-size: 15px; font-weight: bold; margin: 14px 0; }}
        .live-dot {{ color: #f00; animation: blink 1s step-start infinite; }}
        @keyframes blink {{ 50% {{ opacity: 0; }} }}
        .bias-tag {{ background: #ff9800; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        .bias-tag.under-bias {{ background: #9c27b0; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; margin-top: 10px; }}
        th, td {{ padding: 9px 12px; border-bottom: 1px solid #ddd; text-align: left; font-size: 13px; }}
        th {{ background: #222; color: white; position: sticky; top: 0; }}
        tr.over-row {{ background: #eafbea; }}
        tr.under-row {{ background: #fdf0f0; }}
        a {{ color: #0066cc; text-decoration: none; }}
    </style>
</head>
<body>
    <p><a href="/api/matches/{escape(str(event_id))}/view">← Back to match</a> | <a href="/api/innings-tracker">All matches analytics</a></p>
    <h1>Innings Tracker — {match_name}</h1>
    {f'<div class="meta">📍 {venue}</div>' if venue else ""}
    {f'<div class="meta">📅 {match_date} &nbsp;|&nbsp; {league}</div>' if match_date else ""}
    <div class="meta">{escape(home_team)} vs {escape(away_team)}</div>
    {f'<div class="meta" style="color:#888;font-size:12px;">📊 Market: {escape(innings_market)}</div>' if innings_market else ""}
    {summary_html}
    {no_data_msg}
    <table>
        <thead>
            <tr><th>Time (UTC)</th><th>Over</th><th>Score/Wkts</th><th>Last 6 Balls (oldest→newest)</th><th>Predicted Total</th><th>O/U Odds</th><th>{escape(home_team)} Win</th><th>{escape(away_team)} Win</th><th>Outcome</th><th style="font-size:11px;color:#aaa;">Silver File</th></tr>
        </thead>
        <tbody>{table_rows}</tbody>
    </table>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render innings tracker")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_silver_innings_tracker_html(req: func.HttpRequest) -> func.HttpResponse:
    """Innings tracker built from per-delivery silver state files.

    Reads gold/cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json
    which is pre-built by the batch silver reprocessing script.
    Shows one row per unique match state with ball window, score, predicted total,
    and Match Winner 2-Way odds per delivery.
    """
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        tracker = download_json(gold, f"cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json") or {}

        if not tracker:
            return func.HttpResponse(
                f"<h2>No silver tracker data for event {event_id}.</h2>"
                "<p>Run the batch silver reprocess job first to build this file.</p>",
                status_code=404, mimetype="text/html",
            )

        home_team  = str(tracker.get("home_team_name") or "Home")
        away_team  = str(tracker.get("away_team_name") or "Away")
        match_name = escape(str(tracker.get("match_name") or f"Match {event_id}"))
        league     = escape(str(tracker.get("league_name") or ""))
        match_date = escape(str(tracker.get("match_date_utc") or "")[:10])
        stadium    = tracker.get("stadium_data") or {}
        venue_str  = stadium.get("name") or tracker.get("venue") or ""
        city_str   = stadium.get("city") or ""
        country_str= stadium.get("country") or ""
        venue_parts= [p for p in [venue_str, city_str, country_str] if p]
        venue      = escape(", ".join(venue_parts))

        # Parse final score: ss format "158/6(15.5)-155/10(19.2)" → home - away
        final_ss   = tracker.get("final_score_ss") or ""
        final_score_html = ""
        if final_ss:
            import re as _re
            parts = final_ss.split("-")
            if len(parts) == 2:
                def fmt_score(s, team):
                    m = _re.match(r'(\d+/\d+)\(?([\d.]+)?\)?', s.strip())
                    if m:
                        sc = m.group(1)
                        ov = f" ({m.group(2)} ov)" if m.group(2) else ""
                        return f"<b>{escape(team)}</b>: {sc}{ov}"
                    return f"<b>{escape(team)}</b>: {escape(s.strip())}"
                home_sc = fmt_score(parts[0], home_team)
                away_sc = fmt_score(parts[1], away_team)
                final_score_html = f'<div class="meta" style="font-size:15px;margin-top:6px;">🏏 Final Score &nbsp;|&nbsp; {home_sc} &nbsp;vs&nbsp; {away_sc}</div>'
        rows_data: List[Dict[str, Any]] = tracker.get("rows", [])
        actual_total = tracker.get("actual_total")
        outcome      = tracker.get("outcome")

        def ball_pill(b: str) -> str:
            b = str(b).strip()
            if b == "W":
                return '<span class="ball w">W</span>'
            if b in ("0",):
                return '<span class="ball dot">•</span>'
            if b == "6":
                return '<span class="ball six">6</span>'
            if b == "4":
                return '<span class="ball four">4</span>'
            if "wd" in b or "nb" in b or "lb" in b:
                return f'<span class="ball extra">{escape(b)}</span>'
            return f'<span class="ball run">{escape(b)}</span>'

        # Summary cards
        rows_with_pred = [r for r in rows_data if r.get("predicted_total") is not None]
        over_count  = sum(1 for r in rows_with_pred if actual_total is not None and actual_total > r["predicted_total"])
        under_count = sum(1 for r in rows_with_pred if actual_total is not None and actual_total < r["predicted_total"])
        total_dec   = over_count + under_count
        over_pct    = round(100 * over_count / total_dec, 1) if total_dec else None
        under_pct   = round(100 * under_count / total_dec, 1) if total_dec else None

        if actual_total and outcome:
            summary_html = f"""
            <div class="summary-cards">
                <div class="scard over">Over: {over_count} ({over_pct}%)</div>
                <div class="scard under">Under: {under_count} ({under_pct}%)</div>
                <div class="scard">1st innings: <b>{actual_total} runs</b></div>
                <div class="scard outcome-{outcome}">Outcome: {outcome.upper()}</div>
            </div>"""
        else:
            summary_html = ""

        # Pre-compute true batting team per innings (first row with actual runs, not sentinel)
        innings_batting: Dict[int, str] = {}
        innings_bowling: Dict[int, str] = {}
        for r in rows_data:
            inn = r.get("innings", 1)
            if inn not in innings_batting:
                if r.get("runs") is not None and r.get("runs", 0) > 0:
                    innings_batting[inn] = str(r.get("batting_team") or home_team)
                    innings_bowling[inn] = str(r.get("bowling_team") or away_team)
        # Fallback: if no row has runs>0 (e.g. all ducks), use first row's batting team
        for r in rows_data:
            inn = r.get("innings", 1)
            if inn not in innings_batting:
                innings_batting[inn] = str(r.get("batting_team") or home_team)
                innings_bowling[inn] = str(r.get("bowling_team") or away_team)

        # Table rows — one per state
        prev_innings = None
        table_rows = ""
        for r in rows_data:
            innings  = r.get("innings", 1)
            if innings != 1:
                continue
            pred     = r.get("predicted_total")
            runs     = r.get("runs")
            wickets  = r.get("wickets", 0)
            target   = r.get("target")
            bat_team = str(r.get("batting_team") or home_team)
            bowl_team= str(r.get("bowling_team") or away_team)
            balls    = r.get("ball_window", [])
            home_odd = r.get("home_team_odds")
            away_odd = r.get("away_team_odds")
            ov_odds  = r.get("over_odds_at_line")
            un_odds  = r.get("under_odds_at_line")

            # Innings divider
            if innings != prev_innings:
                prev_innings = innings
                inn_label = "1st Innings" if innings == 1 else f"2nd Innings (chasing {target})"
                bat_label = escape(innings_batting.get(innings, bat_team))
                bowl_label= escape(innings_bowling.get(innings, bowl_team))
                table_rows += f'<tr class="inn-divider"><td colspan="10">📌 {inn_label} — {bat_label} batting vs {bowl_label}</td></tr>'

            row_class = ""
            outcome_cell = "-"
            if actual_total is not None and pred is not None and innings == 1:
                if actual_total > pred:
                    row_class = ' class="over-row"'
                    outcome_cell = "▲ Over"
                elif actual_total < pred:
                    row_class = ' class="under-row"'
                    outcome_cell = "▼ Under"
                else:
                    outcome_cell = "= Push"

            balls_html = "".join(ball_pill(b) for b in reversed(balls)) if balls else "-"
            pred_cell  = f"<b>{pred}</b>" if pred is not None else '<span style="color:#aaa">—</span>'
            odds_cell  = f"{ov_odds} / {un_odds}" if ov_odds and un_odds else '<span style="color:#aaa">—</span>'
            home_cell  = escape(str(home_odd)) if home_odd is not None else "-"
            away_cell  = escape(str(away_odd)) if away_odd is not None else "-"

            # Colour the win cell if very low (favourite)
            if home_odd is not None and home_odd < 1.2:
                home_cell = f'<b style="color:#155724">{home_cell}</b>'
            if away_odd is not None and away_odd < 1.2:
                away_cell = f'<b style="color:#155724">{away_cell}</b>'

            snap_time = escape(str(r.get("snapshot_time_utc") or "")[:16].replace("T", " "))
            score_cell = f"{runs}/{wickets}" if runs is not None else "-"

            state_file = escape(str(r.get("state_file") or ""))
            table_rows += f"""
            <tr{row_class}>
                <td style="font-size:11px;color:#888;">{snap_time}</td>
                <td>{escape(str(r.get('over') or '-'))}</td>
                <td><b>{escape(score_cell)}</b></td>
                <td class="balls">{balls_html}</td>
                <td>{pred_cell}</td>
                <td style="color:#666;font-size:12px;">{odds_cell}</td>
                <td>{home_cell}</td>
                <td>{away_cell}</td>
                <td>{outcome_cell}</td>
                <td style="font-size:11px;color:#999;font-family:monospace;">{state_file}</td>
            </tr>"""

        if outcome and actual_total is not None:
            colour = "#d4edda" if outcome == "over" else "#f8d7da" if outcome == "under" else "#fff3cd"
            table_rows += f'<tr style="background:{colour};font-weight:bold;"><td colspan="10">FINAL 1st innings: {actual_total} runs → 2nd innings outcome: {outcome.upper()}</td></tr>'

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Innings Tracker — {match_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        h1 {{ margin-bottom: 4px; }}
        .meta {{ color: #555; font-size: 14px; margin-bottom: 3px; }}
        .summary-cards {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 18px 0; }}
        .scard {{ padding: 12px 20px; border-radius: 8px; background: white; box-shadow: 0 2px 6px #ddd; font-weight: bold; }}
        .scard.over {{ background: #d4edda; color: #155724; }}
        .scard.under {{ background: #f8d7da; color: #721c24; }}
        .scard.outcome-over {{ background: #d4edda; color: #155724; }}
        .scard.outcome-under {{ background: #f8d7da; color: #721c24; }}
        .scard.outcome-push {{ background: #fff3cd; color: #856404; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; margin-top: 16px; }}
        th, td {{ padding: 8px 10px; border-bottom: 1px solid #eee; text-align: left; font-size: 13px; }}
        th {{ background: #1a1a2e; color: white; position: sticky; top: 0; font-size: 12px; }}
        tr.over-row {{ background: #eafbea; }}
        tr.under-row {{ background: #fdf0f0; }}
        tr.inn-divider td {{ background: #2c3e50; color: #ecf0f1; font-weight: bold; font-size: 13px; padding: 10px; }}
        .balls {{ white-space: nowrap; }}
        .ball {{ display: inline-block; width: 22px; height: 22px; line-height: 22px; text-align: center;
                  border-radius: 50%; font-size: 11px; font-weight: bold; margin: 1px; }}
        .ball.w    {{ background: #e74c3c; color: white; }}
        .ball.dot  {{ background: #bdc3c7; color: #555; font-size: 16px; }}
        .ball.six  {{ background: #2980b9; color: white; }}
        .ball.four {{ background: #27ae60; color: white; }}
        .ball.extra{{ background: #f39c12; color: white; font-size: 9px; width: 28px; border-radius: 4px; }}
        .ball.run  {{ background: #ecf0f1; color: #333; border: 1px solid #ccc; }}
        a {{ color: #0066cc; text-decoration: none; }}
        .source-tag {{ display: inline-block; background: #6c757d; color: white; font-size: 11px;
                        padding: 2px 8px; border-radius: 4px; margin-left: 8px; vertical-align: middle; }}
    </style>
</head>
<body>
    <p><a href="/api/matches/{escape(str(event_id))}/view">← Back to match</a> | <a href="/api/innings-tracker">All matches analytics</a></p>
    <h1>Innings Tracker — {match_name}</h1>
    {f'<div class="meta">📍 {venue}</div>' if venue else ""}
    <div class="meta">📅 {match_date} &nbsp;|&nbsp; {league}</div>
    <div class="meta">{escape(home_team)} vs {escape(away_team)} &nbsp;|&nbsp; <b>{len(rows_data)} states</b></div>
    {final_score_html}
    {summary_html}
    <table>
        <thead>
            <tr>
                <th>Time (UTC)</th>
                <th>Over</th>
                <th>Score</th>
                <th>Last 6 Balls (oldest→newest)</th>
                <th>Predicted Total</th>
                <th>O/U Odds</th>
                <th>{escape(home_team)} Win</th>
                <th>{escape(away_team)} Win</th>
                <th>Outcome</th>
            </tr>
        </thead>
        <tbody>{table_rows}</tbody>
    </table>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render silver innings tracker")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_innings_tracker_analytics(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        params = req.params
        f_league = (params.get("league") or "").strip()
        f_team = (params.get("team") or "").strip().lower()
        f_venue = (params.get("venue") or "").strip().lower()
        f_min_over = safe_float(params.get("min_over") or "")
        f_max_over = safe_float(params.get("max_over") or "")
        f_min_wkts = params.get("min_wickets")
        f_max_wkts = params.get("max_wickets")
        f_min_bat_odds = safe_float(params.get("min_bat_odds") or "")
        f_max_bat_odds = safe_float(params.get("max_bat_odds") or "")
        try:
            f_min_wkts_int = int(f_min_wkts) if f_min_wkts is not None else None
        except Exception:
            f_min_wkts_int = None
        try:
            f_max_wkts_int = int(f_max_wkts) if f_max_wkts is not None else None
        except Exception:
            f_max_wkts_int = None

        idx = download_json(gold, "cricket/innings_tracker/index.json") or {}
        all_matches = idx.get("matches", [])

        result_rows: List[Dict[str, Any]] = []
        over_total = under_total = no_outcome = 0

        for meta in all_matches:
            if f_league and meta.get("league_name") != f_league:
                continue
            if f_venue and f_venue not in str(meta.get("venue") or "").lower():
                continue
            event_id_m = str(meta.get("event_id") or "")
            actual_total = meta.get("actual_total")
            tracker = download_json(gold, f"cricket/innings_tracker/event_id={event_id_m}/innings_1.json") or {}

            for r in tracker.get("rows", []):
                if f_team:
                    batting = str(r.get("batting_team") or "").lower()
                    bowling = str(r.get("bowling_team") or "").lower()
                    if f_team not in batting and f_team not in bowling:
                        continue
                over_fl = r.get("over_float")
                if f_min_over is not None and (over_fl is None or over_fl < f_min_over):
                    continue
                if f_max_over is not None and (over_fl is None or over_fl > f_max_over):
                    continue
                wkts = r.get("wickets")
                if f_min_wkts_int is not None and (wkts is None or wkts < f_min_wkts_int):
                    continue
                if f_max_wkts_int is not None and (wkts is None or wkts > f_max_wkts_int):
                    continue
                bat_od = r.get("batting_team_odds")
                if f_min_bat_odds is not None and (bat_od is None or bat_od < f_min_bat_odds):
                    continue
                if f_max_bat_odds is not None and (bat_od is None or bat_od > f_max_bat_odds):
                    continue

                pred = r.get("predicted_total")
                row_outcome = None
                if pred is not None and actual_total is not None:
                    if actual_total > pred:
                        row_outcome = "over"
                        over_total += 1
                    elif actual_total < pred:
                        row_outcome = "under"
                        under_total += 1
                    else:
                        row_outcome = "push"
                else:
                    no_outcome += 1

                result_rows.append({**r, "event_id": event_id_m, "match_name": meta.get("match_name"), "league_name": meta.get("league_name"), "venue": meta.get("venue"), "actual_total": actual_total, "row_outcome": row_outcome})

        total_decided = over_total + under_total
        over_pct = round(100 * over_total / total_decided, 1) if total_decided else None
        under_pct = round(100 * under_total / total_decided, 1) if total_decided else None

        league_options = '<option value="">All Leagues</option>'
        for m in all_matches:
            ln = escape(str(m.get("league_name") or ""))
            sel = ' selected' if ln == escape(f_league) else ''
            league_options += f'<option value="{ln}"{sel}>{ln}</option>'

        def fv(name: str) -> str:
            v = params.get(name) or ""
            return f'value="{escape(v)}"' if v else ""

        table_rows_html = ""
        for r in result_rows:
            oc = r.get("row_outcome") or "pending"
            bg = {"over": "#eafbea", "under": "#fdf0f0", "push": "#fff9e6"}.get(oc, "")
            style = f' style="background:{bg}"' if bg else ""
            table_rows_html += f"""
            <tr{style}>
                <td><a href="/api/matches/{r['event_id']}/innings-tracker/view">{escape(str(r.get('match_name') or r['event_id']))}</a></td>
                <td>{escape(str(r.get('league_name') or '-'))}</td>
                <td>{escape(str(r.get('venue') or '-'))}</td>
                <td>{escape(str(r['over']) if r.get('over') is not None else '-')}</td>
                <td>{escape(str(r['score']) if r.get('score') is not None else '-')}/{escape(str(r['wickets']) if r.get('wickets') is not None else '0')}</td>
                <td>{escape(str(r.get('batting_team') or '-'))}</td>
                <td>{escape(str(r.get('bowling_team') or '-'))}</td>
                <td>{escape(str(r.get('batting_team_odds') or '-'))}</td>
                <td>{escape(str(r.get('bowling_team_odds') or '-'))}</td>
                <td><b>{escape(str(r.get('predicted_total') or '-'))}</b></td>
                <td>{escape(str(r.get('actual_total') or 'TBD'))}</td>
                <td><b>{oc.upper()}</b></td>
            </tr>"""

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Innings Tracker Analytics</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        h1 {{ margin-bottom: 16px; }}
        .filter-form {{ background: white; padding: 18px; border-radius: 10px; box-shadow: 0 2px 8px #ddd; margin-bottom: 20px; display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; }}
        .filter-form label {{ font-size: 13px; color: #555; display: block; margin-bottom: 4px; }}
        .filter-form input, .filter-form select {{ padding: 7px 10px; font-size: 13px; border: 1px solid #ccc; border-radius: 5px; width: 130px; }}
        .filter-form button {{ padding: 8px 20px; background: #222; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 14px; }}
        .summary-cards {{ display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 20px; }}
        .scard {{ padding: 14px 22px; border-radius: 8px; background: white; box-shadow: 0 2px 6px #ddd; font-weight: bold; font-size: 15px; }}
        .scard.over {{ background: #d4edda; color: #155724; }}
        .scard.under {{ background: #f8d7da; color: #721c24; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
        th, td {{ padding: 9px 11px; border-bottom: 1px solid #ddd; text-align: left; font-size: 13px; }}
        th {{ background: #222; color: white; position: sticky; top: 0; }}
        a {{ color: #0066cc; text-decoration: none; }}
    </style>
</head>
<body>
    <h1>Innings Tracker Analytics ({len(all_matches)} matches)</h1>
    <form class="filter-form" method="get">
        <div><label>League</label><select name="league">{league_options}</select></div>
        <div><label>Team (batting or bowling)</label><input name="team" {fv('team')} placeholder="e.g. GT"></div>
        <div><label>Venue (partial)</label><input name="venue" {fv('venue')} placeholder="e.g. Wankhede"></div>
        <div><label>Min Over</label><input name="min_over" {fv('min_over')} placeholder="e.g. 3"></div>
        <div><label>Max Over</label><input name="max_over" {fv('max_over')} placeholder="e.g. 5"></div>
        <div><label>Min Wickets</label><input name="min_wickets" {fv('min_wickets')} placeholder="e.g. 0"></div>
        <div><label>Max Wickets</label><input name="max_wickets" {fv('max_wickets')} placeholder="e.g. 3"></div>
        <div><label>Batting odds ≥</label><input name="min_bat_odds" {fv('min_bat_odds')} placeholder="e.g. 1.5"></div>
        <div><label>Batting odds ≤</label><input name="max_bat_odds" {fv('max_bat_odds')} placeholder="e.g. 2.5"></div>
        <button type="submit">Apply Filters</button>
        <a href="/api/innings-tracker" style="padding:8px 14px;background:#eee;border-radius:5px;color:#333;font-size:13px;">Reset</a>
    </form>
    <div class="summary-cards">
        <div class="scard">Filtered rows: {len(result_rows)}</div>
        {'<div class="scard over">Over: ' + str(over_total) + ' (' + str(over_pct) + '%)</div>' if over_pct is not None else ""}
        {'<div class="scard under">Under: ' + str(under_total) + ' (' + str(under_pct) + '%)</div>' if under_pct is not None else ""}
        <div class="scard">No outcome yet: {no_outcome}</div>
    </div>
    {'<p style="color:#999">No rows match the current filters.</p>' if not result_rows else ""}
    <table>
        <thead>
            <tr><th>Match</th><th>League</th><th>Venue</th>
            <th>Over</th><th>Score/Wkts</th>
            <th>Batting</th><th>Bowling</th>
            <th>Bat Odds</th><th>Bowl Odds</th>
            <th>Predicted</th><th>Actual</th><th>Outcome</th></tr>
        </thead>
        <tbody>{table_rows_html}</tbody>
    </table>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render innings tracker analytics")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


# ------------------------------------------------------------------
# Live markets debug routes
# ------------------------------------------------------------------

def view_market_heatmap_html(req: func.HttpRequest) -> func.HttpResponse:
    """Post-match betting market heatmap. Reads silver state files."""
    import re as _re

    # ── market categorisation ─────────────────────────────────────────────────
    _OVER_ORD   = _re.compile(r'^\d+(?:st|nd|rd|th)?\s+over', _re.I)
    _PLAYER_RUN = _re.compile(r'.+\s+innings\s+runs?$', _re.I)
    _PLAYER_MIL = _re.compile(r'.+\s+milestones?$', _re.I)
    _TOP_BATTER = _re.compile(r'.+\s+top\s+batter$', _re.I)
    _TOP_BOWL   = _re.compile(r'.+\s+top\s+bowl', _re.I)
    _INN_TOTAL  = _re.compile(r'.+\s+\d+\s+overs?\s+runs?$', _re.I)
    _BALL_DEL   = _re.compile(r'runs?\s+off\s+\d+\w*\s+delivery', _re.I)
    _FALL_WKT   = _re.compile(r'runs?\s+at\s+fall\s+of', _re.I)

    def _categorise(name: str):
        g = name.strip()
        gl = g.lower()
        if 'match winner' in gl:
            return 'Match Winner', 'match', '#2563eb'
        if _INN_TOTAL.match(g):
            return 'Innings Total (O/U)', 'match', '#2563eb'
        if _OVER_ORD.match(g):
            if 'odd' in gl or 'even' in gl:
                return 'Over Runs – Odd/Even', 'over', '#d97706'
            if 'wicket' in gl:
                return 'Over – Wicket', 'over', '#d97706'
            return 'Over Total Runs', 'over', '#d97706'
        if 'dismissal method' in gl or 'method of dismissal' in gl:
            return 'Dismissal Method', 'ball', '#16a34a'
        if _BALL_DEL.search(gl):
            return 'Ball Delivery Runs', 'ball', '#16a34a'
        if 'next batter out' in gl:
            return 'Next Batter Out', 'player', '#9333ea'
        if _PLAYER_RUN.match(g):
            return 'Batter Innings Runs', 'player', '#9333ea'
        if _PLAYER_MIL.match(g) or 'batter milestones' in gl:
            return 'Batter Milestones', 'player', '#9333ea'
        if _TOP_BATTER.match(g):
            return 'Top Batter', 'match', '#2563eb'
        if _TOP_BOWL.match(g):
            return 'Top Bowler', 'match', '#2563eb'
        if 'to score most runs' in gl:
            return 'Top Scorer', 'match', '#2563eb'
        if "6's" in gl or 'sixes' in gl:
            return 'Team Sixes (O/U)', 'match', '#2563eb'
        if 'highest' in gl and 'partnership' in gl:
            return 'Opening Partnership', 'player', '#9333ea'
        if _FALL_WKT.search(gl):
            return 'Fall of Wicket Runs', 'player', '#9333ea'
        if 'session runs' in gl:
            return 'Session Runs', 'over', '#d97706'
        if 'runs in first' in gl:
            return 'Powerplay Runs', 'over', '#d97706'
        return None, None, None   # skip unknown

    try:
        event_id = req.route_params.get("event_id", "")
        silver_c = get_named_container_client("silver")
        gold_c   = get_named_container_client("gold")

        # ── load gold tracker for match meta + final score ────────────────────
        tracker = (
            download_json(gold_c, f"cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json")
            or {}
        )
        match_name  = escape(tracker.get("match_name") or f"Event {event_id}")
        home_team   = tracker.get("home_team_name") or ""
        away_team   = tracker.get("away_team_name") or ""
        final_ss    = tracker.get("final_score_ss") or ""
        stadium     = tracker.get("stadium_data") or {}
        venue_str   = stadium.get("name") or tracker.get("venue") or ""

        # ── load & sort silver state files ────────────────────────────────────
        def _sort_key(bname):
            m = _re.match(r'state_(\d+)_(\d+)_(\d+)_(\d+)\.json', bname.rsplit("/", 1)[-1])
            return (int(m.group(1)), int(m.group(2)), int(m.group(4)), int(m.group(3))) if m else (99,999,99,99)

        prefix = f"cricket/inplay/state/event_id={event_id}/"
        blobs  = sorted(
            [b.name for b in silver_c.list_blobs(name_starts_with=prefix) if b.name.endswith(".json")],
            key=_sort_key
        )

        if not blobs:
            return func.HttpResponse(
                f"No silver state files found for event {event_id}. Run the silver reprocessor first.",
                status_code=404
            )

        # ── build ball × market matrix ────────────────────────────────────────
        # balls: list of {over, innings, score, wickets, market_cats: set}
        balls = []
        all_cats: Dict[str, tuple] = {}  # canonical_name -> (type, color)

        for bname in blobs:
            try:
                state = download_json(silver_c, bname)
            except Exception:
                continue
            if not state:
                continue

            over    = str(state.get("over") or "0.0")
            innings = int(state.get("innings") or 1)
            score   = str(state.get("score") or "0/0")
            runs    = state.get("runs")
            wkts    = state.get("wickets")

            # parse over to float
            try:
                ov_f = float(over)
            except Exception:
                ov_f = 0.0
            over_num    = int(ov_f)
            ball_in_ov  = round((ov_f - over_num) * 10)

            open_cats: set = set()
            for mkt in state.get("markets", []):
                grp = str(mkt.get("market_group_name") or "")
                if not grp:
                    continue
                canon, mtype, mcolor = _categorise(grp)
                if canon is None:
                    continue
                if canon not in all_cats:
                    all_cats[canon] = (mtype, mcolor)
                open_cats.add(canon)

            balls.append({
                "over": over, "over_num": over_num, "ball": ball_in_ov,
                "innings": innings, "score": score, "runs": runs, "wkts": wkts,
                "cats": open_cats,
            })

        if not balls:
            return func.HttpResponse("No market data in silver state files.", status_code=404)

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


def view_match_live_markets(req: func.HttpRequest) -> func.HttpResponse:
    """Redirects to market heatmap (live markets page removed)."""
    event_id = req.route_params.get("event_id", "")
    return func.HttpResponse(
        status_code=302,
        headers={"Location": f"/api/matches/{event_id}/heatmap"},
    )


def view_match_markets_raw(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        page = (
            download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
            or download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
        )
        fi = (page.get("snapshot") or {}).get("fi")
        if not fi:
            return func.HttpResponse(json.dumps({"error": "FI not found in gold data for this event"}), status_code=404, mimetype="application/json")

        payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi})
        body = (payload.get("response") or {}).get("body") or {}
        results = body.get("results", [])
        flat_records = results[0] if results and isinstance(results[0], list) else (results if isinstance(results, list) else [])
        type_counts: Dict[str, int] = {}
        for r in flat_records:
            if isinstance(r, dict):
                t = r.get("type", "UNKNOWN")
                type_counts[t] = type_counts.get(t, 0) + 1
        debug = {
            "event_id": event_id,
            "fi": fi,
            "api_success": payload["response"]["success"],
            "http_status": payload["response"]["http_status_code"],
            "total_records": len(flat_records),
            "record_type_counts": type_counts,
            "raw_body": body,
        }
        return func.HttpResponse(json.dumps(debug, indent=2, default=str), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to fetch raw markets")
        return func.HttpResponse(json.dumps({"error": str(ex)}), status_code=500, mimetype="application/json")


# ------------------------------------------------------------------
# Admin routes
# ------------------------------------------------------------------

def view_admin_rebuild_innings(req: func.HttpRequest) -> func.HttpResponse:
    event_id = req.route_params.get("event_id", "").strip()
    if not event_id:
        return func.HttpResponse("event_id required", status_code=400)

    try:
        silver = get_named_container_client("silver")
        gold = get_named_container_client("gold")

        needle = f"/event_id={event_id}/"
        snapshot_paths: List[str] = []
        for blob in silver.list_blobs(name_starts_with="cricket/inplay/"):
            if needle in blob.name and blob.name.endswith("/match_state.json"):
                snapshot_paths.append(blob.name)

        if not snapshot_paths:
            return func.HttpResponse(
                json.dumps({"event_id": event_id, "message": "no silver snapshots found", "rows_written": 0}),
                mimetype="application/json", status_code=200,
            )

        snapshot_paths.sort()
        raw_points: List[Dict[str, Any]] = []
        errors = 0
        for ms_path in snapshot_paths:
            base = ms_path.removesuffix("/match_state.json")
            try:
                match_state = download_json(silver, ms_path)
                team_scores_doc = download_json(silver, f"{base}/team_scores.json") or {}
                active_markets_doc = download_json(silver, f"{base}/active_markets.json") or {}
                if not match_state:
                    continue
                point = extract_innings_snapshot(match_state, team_scores_doc.get("rows", []), active_markets_doc.get("rows", []))
                if point is not None:
                    raw_points.append(point)
            except Exception:
                errors += 1

        raw_points.sort(key=lambda p: str(p.get("snapshot_time_utc") or ""))
        seen: Dict[tuple, Dict[str, Any]] = {}
        for p in raw_points:
            key = (p.get("over"), p.get("score"))
            seen[key] = p
        deduped = sorted(seen.values(), key=lambda p: str(p.get("snapshot_time_utc") or ""))

        acc_path = f"cricket/inplay/control/event_id={event_id}/innings_accumulator.json"
        old_acc = download_json(silver, acc_path) or {}
        new_acc = {**old_acc, "event_id": event_id, "rows": deduped, "last_updated_utc": utc_now().isoformat(), "rebuilt_from_snapshot_count": len(snapshot_paths)}
        if deduped and not new_acc.get("home_team_name"):
            p0 = deduped[0]
            new_acc["home_team_name"] = new_acc.get("home_team_name") or p0.get("batting_team")
            new_acc["away_team_name"] = new_acc.get("away_team_name") or p0.get("bowling_team")
        upload_json(silver, acc_path, new_acc, overwrite=True)

        gold_tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
        existing_gold = download_json(gold, gold_tracker_path) or {}
        tracker = {**existing_gold, "rows": deduped, "last_updated_utc": utc_now().isoformat()}
        upload_json(gold, gold_tracker_path, tracker, overwrite=True)

        return func.HttpResponse(
            json.dumps({"event_id": event_id, "silver_snapshots_scanned": len(snapshot_paths), "raw_points_extracted": len(raw_points), "rows_written": len(deduped), "errors": errors, "message": "accumulator rebuilt — reload the innings tracker page"}, indent=2),
            mimetype="application/json", status_code=200,
        )
    except Exception as ex:
        logging.exception("admin_rebuild_innings_accumulator failed")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_admin_leagues(req: func.HttpRequest) -> func.HttpResponse:
    try:
        leagues = collect_known_leagues()
        excluded = load_excluded_league_ids()
        rows_html = ""
        for lg in leagues:
            lid = escape(str(lg.get("league_id") or ""))
            lname = escape(str(lg.get("league_name") or lid))
            sources = ", ".join(lg.get("sources", []))
            is_excluded = str(lg.get("league_id") or "") in excluded
            checked = "" if is_excluded else "checked"
            bg = "" if is_excluded else ' style="background:#f0fff0"'
            rows_html += f"""
            <tr{bg}>
                <td>{lid}</td>
                <td><b>{lname}</b></td>
                <td style="color:#666;font-size:12px;">{escape(sources)}</td>
                <td>
                    <label class="toggle">
                        <input type="checkbox" {checked}
                               onchange="toggle(this, '{lid}')"
                               data-league-id="{lid}" data-league-name="{lname}">
                        <span class="slider"></span>
                    </label>
                </td>
                <td id="status-{lid}" style="font-size:12px;color:#888;">
                    {'Excluded — not captured' if is_excluded else 'Included'}
                </td>
            </tr>"""
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>League Filter</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        h1 {{ margin-bottom: 6px; }}
        .hint {{ color: #666; margin-bottom: 20px; font-size: 14px; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
        th, td {{ padding: 10px 13px; border-bottom: 1px solid #ddd; text-align: left; font-size: 14px; }}
        th {{ background: #222; color: white; }}
        a {{ color: #0066cc; text-decoration: none; }}
        .toggle {{ position: relative; display: inline-block; width: 48px; height: 26px; }}
        .toggle input {{ opacity: 0; width: 0; height: 0; }}
        .slider {{ position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background: #ccc; border-radius: 26px; transition: .3s; }}
        .slider:before {{ position: absolute; content: ""; height: 20px; width: 20px; left: 3px; bottom: 3px; background: white; border-radius: 50%; transition: .3s; }}
        input:checked + .slider {{ background: #28a745; }}
        input:checked + .slider:before {{ transform: translateX(22px); }}
    </style>
</head>
<body>
    <p><a href="/api/home">← Home</a></p>
    <h1>League Filter</h1>
    <p class="hint">Toggle ON = captured in bronze/silver/gold. Toggle OFF = skipped entirely.<br>
    Changes apply from the next capture cycle (within 5 seconds for live, 1 minute for prematch).</p>
    <p id="save-status" style="color:#28a745;font-weight:bold;display:none;">Saved.</p>
    <table>
        <thead><tr><th>League ID</th><th>League Name</th><th>Seen In</th><th>Capture</th><th>Status</th></tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    <script>
        async function toggle(checkbox, leagueId) {{
            const include = checkbox.checked;
            const statusEl = document.getElementById('status-' + leagueId);
            statusEl.textContent = 'Saving...';
            statusEl.style.color = '#888';
            try {{
                const resp = await fetch('/api/mgmt/leagues/toggle', {{
                    method: 'POST', headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{league_id: leagueId, include: include}})
                }});
                const data = await resp.json();
                if (data.ok) {{
                    statusEl.textContent = include ? 'Included' : 'Excluded — not captured';
                    statusEl.style.color = include ? '#28a745' : '#dc3545';
                    document.getElementById('save-status').style.display = 'block';
                    const row = checkbox.closest('tr');
                    row.style.background = include ? '#f0fff0' : '';
                }} else {{
                    statusEl.textContent = 'Error: ' + (data.error || 'unknown');
                    statusEl.style.color = '#dc3545';
                    checkbox.checked = !include;
                }}
            }} catch(e) {{
                statusEl.textContent = 'Network error';
                statusEl.style.color = '#dc3545';
                checkbox.checked = !include;
            }}
        }}
    </script>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render admin leagues page")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_admin_league_toggle(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        league_id = str(body.get("league_id") or "").strip()
        include = bool(body.get("include", True))
        if not league_id:
            return func.HttpResponse(json.dumps({"ok": False, "error": "league_id required"}), status_code=400, mimetype="application/json")
        excluded = load_excluded_league_ids()
        if include:
            excluded.discard(league_id)
        else:
            excluded.add(league_id)
        save_league_preferences(excluded)
        return func.HttpResponse(json.dumps({"ok": True, "league_id": league_id, "included": include, "total_excluded": len(excluded)}), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to toggle league")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(ex)}), status_code=500, mimetype="application/json")


# ------------------------------------------------------------------
# Home page
# ------------------------------------------------------------------

def view_home(req: func.HttpRequest) -> func.HttpResponse:
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Cricket Data Platform</title>
        <style>
            body { font-family: Arial; background:#f7f7f7; padding:40px; }
            h1 { margin-bottom:10px; }
            .card { background:white; padding:20px; margin:15px 0; border-radius:10px; box-shadow:0 2px 8px #ddd; }
            a { font-size:18px; font-weight:bold; color:#0066cc; text-decoration:none; }
            p { margin:8px 0 0; color:#555; }
        </style>
    </head>
    <body>
        <h1>🏏 Cricket Analytics Platform</h1>
        <p>Browse live, upcoming and historical betting data</p>
        <div class="card"><a href="/api/matches/view">Live Matches</a><p>Real-time matches with live odds and Bet365 markets</p></div>
        <div class="card"><a href="/api/prematch/view">Upcoming Matches</a><p>Prematch odds and markets before the game starts</p></div>
        <div class="card"><a href="/api/leagues/view">Leagues</a><p>Browse matches grouped by leagues</p></div>
        <div class="card"><a href="/api/prematch/leagues/view">Prematch Leagues</a><p>Upcoming matches grouped by leagues</p></div>
        <div class="card"><a href="/api/ended/view">Ended Matches</a><p>Recently finished matches with final results</p></div>
        <div class="card"><a href="/api/innings-tracker">Innings Tracker Analytics</a><p>Over/Under prediction accuracy by over stage, team, venue and odds</p></div>
        <div class="card"><a href="/api/mgmt/leagues/view">League Filter</a><p>Select which leagues to capture — excluded leagues skip bronze, silver and gold entirely</p></div>
    </body>
    </html>
    """
    return func.HttpResponse(html, mimetype="text/html")
