from .common import (
    json, logging, os, escape, Any, Dict, List, Optional,
    func, ResourceNotFoundError,
    download_json, get_named_container_client, upload_json,
    collect_known_leagues, load_allowed_league_ids, save_league_preferences,
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
        allowed = load_allowed_league_ids()
        leagues = collect_known_leagues()
        # Two-pass stable sort: first by date desc, then by group asc — preserves date order within each group.
        # Groups: 0 = allowed (capturing), 1 = new/upcoming-only, 2 = disabled
        leagues.sort(key=lambda x: x.get("last_match_date") or "", reverse=True)
        leagues.sort(key=lambda x: (
            0 if str(x.get("league_id") or "") in allowed else (1 if x.get("sources") == ["upcoming"] else 2)
        ))
        rows_html = ""
        for lg in leagues:
            lid = escape(str(lg.get("league_id") or ""))
            lname = escape(str(lg.get("league_name") or lid))
            sources_list = lg.get("sources", [])
            sources = ", ".join(sources_list)
            first_date = escape(str(lg.get("first_match_date") or "-"))
            last_date  = escape(str(lg.get("last_match_date") or "-"))
            is_allowed = str(lg.get("league_id") or "") in allowed
            is_new = sources_list == ["upcoming"]  # seen only in upcoming, never captured
            checked = "checked" if is_allowed else ""
            if is_allowed:
                bg = ' style="background:#f0fff0"'
                status = "Capturing"
            elif is_new:
                bg = ' style="background:#fffbe6"'
                status = "⚠️ New — not yet enabled"
            else:
                bg = ""
                status = "Disabled — toggle to enable"
            rows_html += f"""
            <tr{bg}>
                <td>{lid}</td>
                <td><b>{lname}</b>{'&nbsp;<span style="font-size:11px;background:#f5a623;color:white;padding:1px 5px;border-radius:3px;">NEW</span>' if is_new else ''}</td>
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
    <p class="hint">New leagues are <b>disabled by default</b> — toggle ON to start capturing.<br>
    Changes apply from the next capture cycle (within 5 seconds for live, 1 minute for prematch).</p>
    <p id="save-status" style="color:#28a745;font-weight:bold;display:none;">Saved.</p>
    <table>
        <thead><tr><th>League ID</th><th>League Name</th><th>Seen In</th><th>First Match</th><th>Last Match</th><th>Capture</th><th>Status</th></tr></thead>
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
        allowed = load_allowed_league_ids()
        if include:
            allowed.add(league_id)
        else:
            allowed.discard(league_id)
        save_league_preferences(allowed)
        return func.HttpResponse(json.dumps({"ok": True, "league_id": league_id, "included": include, "total_allowed": len(allowed)}), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to toggle league")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(ex)}), status_code=500, mimetype="application/json")


# ------------------------------------------------------------------
# Home page
# ------------------------------------------------------------------
