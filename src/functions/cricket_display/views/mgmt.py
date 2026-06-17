from .common import (
    json, logging, os, escape, Any, Dict, List, Optional,
    func, ResourceNotFoundError,
    download_json, get_named_container_client, upload_json,
    collect_known_leagues, load_disabled_league_ids, save_league_preferences,
    build_simple_table_page,
)


def gold_rebuild_ended_matches(event_id: Optional[str] = None) -> None:
    """Rebuild is handled by the Databricks nightly pipeline — not available in display function."""
    logging.warning(json.dumps({"event": "gold_rebuild_ended_matches_not_available_in_display"}))


def _stub_not_available() -> None:
    pass  # placeholder so existing call sites compile


def _rebuild_innings_core_stub(event_id: Optional[str] = None) -> None:
    pass


def auto_rebuild_ended_innings() -> None:
    pass


def view_admin_rebuild_innings(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"error": "Rebuild is handled by the Databricks nightly pipeline. Not available in display function."}),
        status_code=503, mimetype="application/json",
    )


def view_admin_reprocess_silver(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"error": "Silver reprocessing is not available in the display function. Trigger the ingestion pipeline instead."}),
        status_code=503, mimetype="application/json",
    )


def view_admin_leagues(req: func.HttpRequest) -> func.HttpResponse:
    try:
        disabled = load_disabled_league_ids()
        leagues = collect_known_leagues()
        # Two-pass stable sort: first by date desc, then by group asc.
        # Groups: 0 = capturing (not disabled), 1 = disabled
        leagues.sort(key=lambda x: x.get("last_match_date") or "", reverse=True)
        leagues.sort(key=lambda x: (
            1 if str(x.get("league_id") or "") in disabled else 0
        ))
        rows_html = ""
        for lg in leagues:
            lid = escape(str(lg.get("league_id") or ""))
            lname = escape(str(lg.get("league_name") or lid))
            sources_list = lg.get("sources", [])
            sources = ", ".join(sources_list)
            first_date = escape((str(lg.get("first_seen_utc") or lg.get("first_match_date") or "-"))[:10])
            last_date  = escape((str(lg.get("last_seen_utc")  or lg.get("last_match_date")  or "-"))[:10])
            is_disabled = str(lg.get("league_id") or "") in disabled
            checked = "" if is_disabled else "checked"
            if is_disabled:
                bg = ' style="background:#fff5f5"'
                status = "Disabled — toggle to capture"
            else:
                bg = ' style="background:#f0fff0"'
                status = "Capturing"
            rows_html += f"""
            <tr{bg}>
                <td>{lid}</td>
                <td><b>{lname}</b></td>
                <td style="color:#666;font-size:12px;">{escape(sources)}</td>
                <td style="font-size:12px;">{first_date}</td>
                <td style="font-size:12px;">{last_date}</td>
                <td>
                    <label class="toggle">
                        <input type="checkbox" {checked}
                               onchange="toggle(this, '{lid}')"
                               data-league-id="{lid}" data-league-name="{lname}">
                        <span class="slider"></span>
                    </label>
                </td>
                <td id="status-{lid}" style="font-size:12px;color:#888;">
                    {status}
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
    <p class="hint">All leagues are <b>captured by default</b> — toggle OFF to stop capturing a league.<br>
    Changes apply from the next capture cycle (within 5 seconds for live, 1 minute for prematch).</p>
    <p id="save-status" style="color:#28a745;font-weight:bold;display:none;">Saved.</p>
    <table>
        <thead><tr><th>League ID</th><th>League Name</th><th>Seen In</th><th>First Match</th><th>Last Match</th><th>Capture</th><th>Status</th></tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    <script>
        async function toggle(checkbox, leagueId) {{
            const enabled = checkbox.checked;
            const statusEl = document.getElementById('status-' + leagueId);
            statusEl.textContent = 'Saving...';
            statusEl.style.color = '#888';
            try {{
                const resp = await fetch('/api/mgmt/leagues/toggle', {{
                    method: 'POST', headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{league_id: leagueId, include: enabled}})
                }});
                const data = await resp.json();
                if (data.ok) {{
                    statusEl.textContent = enabled ? 'Capturing' : 'Disabled — toggle to capture';
                    statusEl.style.color = enabled ? '#28a745' : '#dc3545';
                    document.getElementById('save-status').style.display = 'block';
                    const row = checkbox.closest('tr');
                    row.style.background = enabled ? '#f0fff0' : '#fff5f5';
                }} else {{
                    statusEl.textContent = 'Error: ' + (data.error || 'unknown');
                    statusEl.style.color = '#dc3545';
                    checkbox.checked = !enabled;
                }}
            }} catch(e) {{
                statusEl.textContent = 'Network error';
                statusEl.style.color = '#dc3545';
                checkbox.checked = !enabled;
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
        disabled = load_disabled_league_ids()
        if include:
            disabled.discard(league_id)   # toggled ON → remove from disabled set
        else:
            disabled.add(league_id)       # toggled OFF → add to disabled set
        save_league_preferences(disabled)
        return func.HttpResponse(json.dumps({"ok": True, "league_id": league_id, "included": include, "total_disabled": len(disabled)}), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to toggle league")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(ex)}), status_code=500, mimetype="application/json")


# ------------------------------------------------------------------
# Home page
# ------------------------------------------------------------------
