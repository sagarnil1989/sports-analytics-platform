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
