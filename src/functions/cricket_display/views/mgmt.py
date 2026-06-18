from .common import (
    json, logging, os, escape, Any, Dict, List, Optional,
    func, ResourceNotFoundError,
    download_json, get_named_container_client, upload_json, utc_now,
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
# Stadium override admin
# ------------------------------------------------------------------

_MATCH_OVERRIDES_PATH = "overrides/match_overrides.json"


def view_admin_stadium_override_get(req: func.HttpRequest) -> func.HttpResponse:
    """Show current stadium overrides and a form to add/update one."""
    try:
        gold = get_named_container_client("gold")
        raw  = download_json(gold, _MATCH_OVERRIDES_PATH) or {}
        overrides: Dict[str, str] = {
            k: v.get("stadium", "") for k, v in raw.items()
            if not k.startswith("_") and isinstance(v, dict) and v.get("stadium")
        }

        # Pre-fill event_id from query string so the innings tracker can link directly
        prefill_event_id = escape(req.params.get("event_id", ""))
        prefill_stadium  = ""
        if prefill_event_id and prefill_event_id in overrides:
            prefill_stadium = escape(overrides[prefill_event_id])

        rows_html = ""
        for eid, sname in sorted((k, v) for k, v in overrides.items() if not str(k).startswith("_")):
            eid_e   = escape(str(eid))
            sname_e = escape(str(sname))
            rows_html += f"""
            <tr>
                <td>{eid_e}</td>
                <td>{sname_e}</td>
                <td>
                    <a href="/api/matches/{eid_e}/innings-tracker/view">View match</a>
                </td>
                <td>
                    <button onclick="deleteOverride('{eid_e}')" style="color:#dc3545;cursor:pointer;background:none;border:none;font-size:13px;">Remove</button>
                </td>
            </tr>"""

        if not rows_html:
            rows_html = '<tr><td colspan="4" style="color:#888;font-style:italic;">No overrides set.</td></tr>'

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Stadium Overrides</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
        h1 {{ margin-bottom: 6px; }}
        .hint {{ color: #666; margin-bottom: 20px; font-size: 14px; }}
        .form-box {{ background: white; padding: 20px; border-radius: 6px; box-shadow: 0 2px 8px #ddd; margin-bottom: 28px; max-width: 520px; }}
        .form-box label {{ display: block; margin-bottom: 4px; font-size: 13px; font-weight: bold; }}
        .form-box input {{ width: 100%; padding: 8px; margin-bottom: 12px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; box-sizing: border-box; }}
        .form-box button {{ background: #0066cc; color: white; padding: 9px 20px; border: none; border-radius: 4px; cursor: pointer; font-size: 14px; }}
        .form-box button:hover {{ background: #0052a3; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
        th, td {{ padding: 10px 13px; border-bottom: 1px solid #ddd; text-align: left; font-size: 14px; }}
        th {{ background: #222; color: white; }}
        a {{ color: #0066cc; text-decoration: none; }}
        #msg {{ margin-bottom: 14px; font-weight: bold; display: none; }}
    </style>
</head>
<body>
    <p><a href="/api/home">← Home</a></p>
    <h1>Stadium Overrides</h1>
    <p class="hint">Set a stadium name for a specific match when the data is missing or wrong. The name is applied immediately on the next innings tracker load.</p>
    <div id="msg"></div>
    <div class="form-box">
        <label>Event ID</label>
        <input id="inp-event-id" type="text" placeholder="e.g. 11805927" value="{prefill_event_id}">
        <label>Stadium Name</label>
        <input id="inp-stadium" type="text" placeholder="e.g. Wankhede Stadium, Mumbai" value="{prefill_stadium}">
        <button onclick="saveOverride()">Save Override</button>
    </div>
    <h2 style="margin-bottom:12px;">Current Overrides</h2>
    <table id="overridesTable">
        <thead><tr><th>Event ID</th><th>Stadium Name</th><th>Match</th><th></th></tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    <script>
        function showMsg(text, color) {{
            const el = document.getElementById('msg');
            el.textContent = text;
            el.style.color = color;
            el.style.display = 'block';
            setTimeout(() => el.style.display = 'none', 3000);
        }}
        async function saveOverride() {{
            const event_id   = document.getElementById('inp-event-id').value.trim();
            const stadium    = document.getElementById('inp-stadium').value.trim();
            if (!event_id || !stadium) {{ showMsg('Both fields are required.', '#dc3545'); return; }}
            try {{
                const resp = await fetch('/api/mgmt/stadium-override', {{
                    method: 'POST', headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{event_id, stadium_name: stadium}})
                }});
                const data = await resp.json();
                if (data.ok) {{ showMsg('Saved. Reload to see updated list.', '#28a745'); }}
                else {{ showMsg('Error: ' + (data.error || 'unknown'), '#dc3545'); }}
            }} catch(e) {{ showMsg('Network error', '#dc3545'); }}
        }}
        async function deleteOverride(event_id) {{
            if (!confirm('Remove override for ' + event_id + '?')) return;
            try {{
                const resp = await fetch('/api/mgmt/stadium-override', {{
                    method: 'POST', headers: {{'Content-Type': 'application/json'}},
                    body: JSON.stringify({{event_id, stadium_name: ''}})
                }});
                const data = await resp.json();
                if (data.ok) {{ showMsg('Removed. Reload to see updated list.', '#28a745'); }}
                else {{ showMsg('Error: ' + (data.error || 'unknown'), '#dc3545'); }}
            }} catch(e) {{ showMsg('Network error', '#dc3545'); }}
        }}
    </script>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render stadium override page")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_admin_stadium_override_save(req: func.HttpRequest) -> func.HttpResponse:
    """POST {event_id, stadium_name} — upsert or delete stadium override in match_overrides.json."""
    try:
        body     = req.get_json()
        event_id = str(body.get("event_id") or "").strip()
        stadium  = str(body.get("stadium_name") or "").strip()
        if not event_id:
            return func.HttpResponse(
                json.dumps({"ok": False, "error": "event_id required"}),
                status_code=400, mimetype="application/json",
            )
        gold  = get_named_container_client("gold")
        raw   = download_json(gold, _MATCH_OVERRIDES_PATH) or {}
        ovs   = {k: v for k, v in raw.items() if not k.startswith("_") and isinstance(v, dict)}
        entry = ovs.setdefault(event_id, {})
        if stadium:
            entry["stadium"] = stadium
        else:
            entry.pop("stadium", None)
        if not entry:
            ovs.pop(event_id, None)
        upload_json(gold, _MATCH_OVERRIDES_PATH,
                    {**ovs, "_updated_at_utc": utc_now().isoformat()}, overwrite=True)
        return func.HttpResponse(
            json.dumps({"ok": True, "event_id": event_id, "stadium_name": stadium or None}),
            status_code=200, mimetype="application/json",
        )
    except Exception as ex:
        logging.exception("Failed to save stadium override")
        return func.HttpResponse(
            json.dumps({"ok": False, "error": str(ex)}),
            status_code=500, mimetype="application/json",
        )


def view_admin_match_override_save(req: func.HttpRequest) -> func.HttpResponse:
    """POST {event_id, field, value} — save a single-field override for an ended match."""
    _ALLOWED_FIELDS = {"match_name", "league_name", "format", "gender", "score", "stadium"}
    try:
        body     = req.get_json()
        event_id = str(body.get("event_id") or "").strip()
        field    = str(body.get("field") or "").strip()
        value    = str(body.get("value") or "").strip()
        if not event_id:
            return func.HttpResponse(json.dumps({"ok": False, "error": "event_id required"}),
                                     status_code=400, mimetype="application/json")
        if field not in _ALLOWED_FIELDS:
            return func.HttpResponse(json.dumps({"ok": False, "error": f"field '{field}' not allowed"}),
                                     status_code=400, mimetype="application/json")
        gold  = get_named_container_client("gold")
        raw   = download_json(gold, _MATCH_OVERRIDES_PATH) or {}
        ovs   = {k: v for k, v in raw.items() if not k.startswith("_") and isinstance(v, dict)}
        entry = ovs.setdefault(event_id, {})
        if value:
            entry[field] = value
        else:
            entry.pop(field, None)
        if not entry:
            ovs.pop(event_id, None)
        upload_json(gold, _MATCH_OVERRIDES_PATH,
                    {**ovs, "_updated_at_utc": utc_now().isoformat()}, overwrite=True)
        return func.HttpResponse(
            json.dumps({"ok": True, "event_id": event_id, "field": field, "value": value}),
            status_code=200, mimetype="application/json",
        )
    except Exception as ex:
        logging.exception("Failed to save match field override")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(ex)}),
                                 status_code=500, mimetype="application/json")


# ------------------------------------------------------------------
# Home page
# ------------------------------------------------------------------
