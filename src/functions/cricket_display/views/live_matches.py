from .common import (
    json, logging, os, escape, Any, Dict, List, Optional,
    func, ResourceNotFoundError,
    call_betsapi, download_json, download_required_json, format_unix_ts,
    get_named_container_client, safe_float, upload_json, utc_now,
    extract_bet365_current_markets,
    collect_known_leagues, load_allowed_league_ids, save_league_preferences,
    extract_innings_snapshot,
    build_simple_table_page,
)


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
                <td><a href="/api/matches/{event_id}/detailed-analysis">Analysis</a></td>
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

        tracker_data = download_json(gold, f"event_id={event_id}/innings_tracker.json") or {}
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
                <a href="/api/matches/{event_id}/heatmap">Market Heatmap</a> |
                <a href="/api/matches/{event_id}/detailed-analysis">Detailed Analysis</a>
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
