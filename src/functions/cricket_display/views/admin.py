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


def _silver_snapshot_paths_for_event(silver_container, event_id: str) -> List[str]:
    """List all silver match_state.json paths for one event, sorted oldest-first. No bronze access."""
    paths = []
    seen_sids: set = set()
    for blob in silver_container.list_blobs(name_starts_with="cricket/inplay/year="):
        if not blob.name.endswith("/match_state.json"):
            continue
        if f"/event_id={event_id}/" not in blob.name:
            continue
        parts = blob.name.split("/")
        sid = next((p for p in parts if p.startswith("snapshot_id=")), None)
        if not sid or sid in seen_sids:
            continue
        seen_sids.add(sid)
        paths.append(blob.name)
    paths.sort()
    return paths


def gold_rebuild_ended_matches(event_id: Optional[str] = None) -> None:
    """Build/rebuild innings_1_from_silver.json from silver data only. No bronze access.

    Finds event_ids that have silver-processed snapshots but are missing or have a stale
    innings_1_from_silver.json, then rebuilds each one from silver snapshot data.

    Args:
        event_id: If provided, process only this event. Otherwise process all stale events.
    """
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")

    # Find newest silver marker timestamp per event_id.
    # Marker filename: {snapshot_id}_{event_id}_{fi}.json — rsplit("_", 2) isolates event_id reliably.
    marker_prefix = "cricket/control/processed_snapshots/"
    silver_newest: Dict[str, Any] = {}
    for blob in silver.list_blobs(name_starts_with=marker_prefix):
        fname = blob.name.rsplit("/", 1)[-1]
        if not fname.endswith(".json"):
            continue
        parts = fname[:-5].rsplit("_", 2)
        if len(parts) != 3:
            continue
        eid = parts[1]
        lm = blob.last_modified
        if lm and (eid not in silver_newest or lm > silver_newest[eid]):
            silver_newest[eid] = lm

    if event_id:
        if event_id not in silver_newest:
            logging.warning(json.dumps({
                "event": "gold_rebuild_ended_matches_skipped",
                "reason": "no silver processed snapshots found",
                "event_id": event_id,
            }))
            return
        to_rebuild = [event_id]
    else:
        # Only rebuild events where silver has data newer than the existing gold file.
        gold_ts: Dict[str, Any] = {}
        for blob in gold.list_blobs(name_starts_with="cricket/innings_tracker/event_id="):
            if blob.name.endswith("innings_1_from_silver.json"):
                bparts = blob.name.split("/")
                ep = next((p for p in bparts if p.startswith("event_id=")), None)
                if ep:
                    gold_ts[ep.replace("event_id=", "")] = blob.last_modified

        to_rebuild = []
        for eid, silver_lm in silver_newest.items():
            gold_lm = gold_ts.get(eid)
            if gold_lm is None or (silver_lm and silver_lm > gold_lm):
                to_rebuild.append(eid)

    logging.info(json.dumps({
        "event": "gold_rebuild_ended_matches_started",
        "total_silver_events": len(silver_newest),
        "to_rebuild": len(to_rebuild),
    }))

    rebuilt = failed = 0
    for eid in to_rebuild:
        paths = _silver_snapshot_paths_for_event(silver, eid)
        try:
            _rebuild_innings_core(eid, snapshot_paths=paths)
            rebuilt += 1
        except Exception:
            failed += 1
            logging.exception(json.dumps({"event": "gold_rebuild_failed", "event_id": eid}))

    logging.warning(json.dumps({
        "event": "gold_rebuild_ended_matches_done",
        "rebuilt": rebuilt,
        "failed": failed,
    }))


def _rebuild_innings_core(event_id: str, snapshot_paths: Optional[List[str]] = None) -> Dict[str, Any]:
    """Rebuild innings_1.json and innings_1_from_silver.json for one event.

    Returns a result dict with keys: silver_snapshots_scanned, raw_points_extracted,
    rows_written, errors, message. Raises on fatal errors.

    Args:
        event_id: The match event ID.
        snapshot_paths: Pre-computed list of silver match_state.json paths, sorted oldest-first.
            If None, paths are derived from bronze manifest listings (used by the HTTP admin endpoint).
    """
    import re as _re
    from datetime import datetime as _dt

    silver = get_named_container_client("silver")
    gold   = get_named_container_client("gold")

    if snapshot_paths is None:
        # Derive silver paths from bronze manifest listing (original behaviour for HTTP admin endpoint).
        bronze = get_named_container_client("bronze")
        bronze_prefix = f"betsapi/inplay_snapshot/sport_id=3/event_id={event_id}/"
        seen_sids: set = set()
        snapshot_paths = []
        for blob in bronze.list_blobs(name_starts_with=bronze_prefix):
            if not blob.name.endswith("/manifest.json"):
                continue
            m = _re.search(r"/snapshot_id=([^/]+)/manifest\.json$", blob.name)
            if not m:
                continue
            sid = m.group(1)
            if sid in seen_sids:
                continue
            seen_sids.add(sid)
            try:
                ts = _dt.strptime(sid[:15], "%Y%m%dT%H%M%S")
                base = (
                    f"cricket/inplay/year={ts.year}/month={ts.month:02d}"
                    f"/day={ts.day:02d}/hour={ts.hour:02d}"
                    f"/event_id={event_id}/snapshot_id={sid}"
                )
                snapshot_paths.append(f"{base}/match_state.json")
            except Exception:
                continue

    if not snapshot_paths:
        return {"event_id": event_id, "message": "no silver snapshots found", "rows_written": 0,
                "silver_snapshots_scanned": 0, "raw_points_extracted": 0, "errors": 0}

    snapshot_paths.sort()

    gold_tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
    existing_gold = download_json(gold, gold_tracker_path) or {}

    # Scan last 50 silver snapshots for final score, venue, and match metadata.
    silver_score = None
    stadium_data = None
    silver_match_name = None
    silver_league_id  = None
    silver_league_name = None
    silver_home_team  = None
    silver_away_team  = None
    silver_match_date = None
    for ms_path in reversed(snapshot_paths[-50:]):
        ms = download_json(silver, ms_path)
        if ms:
            if not silver_score and ms.get("score_summary_events"):
                silver_score = ms["score_summary_events"]
            if not stadium_data and ms.get("stadium_data"):
                stadium_data = ms["stadium_data"]
            if not silver_match_name and ms.get("match_name"):
                silver_match_name = ms["match_name"]
            if not silver_league_id and ms.get("league_id"):
                silver_league_id = str(ms["league_id"])
            if not silver_league_name and ms.get("league_name"):
                silver_league_name = ms["league_name"]
            if not silver_home_team and ms.get("home_team_name"):
                silver_home_team = ms["home_team_name"]
            if not silver_away_team and ms.get("away_team_name"):
                silver_away_team = ms["away_team_name"]
            if not silver_match_date and ms.get("event_time_utc"):
                silver_match_date = ms["event_time_utc"]
        if silver_score and stadium_data and silver_match_name and silver_league_id and silver_home_team:
            break
    if not stadium_data:
        stadium_data = existing_gold.get("stadium_data") or None

    def _norm_score(s):
        return s.replace("-", ",") if s else s
    silver_score = _norm_score(silver_score)
    gold_score   = _norm_score(existing_gold.get("score_summary_events") or None)

    def _2nd_runs(ss):
        if not ss:
            return -1
        parts = ss.split(",", 1)
        if len(parts) == 2:
            m = _re.match(r'(\d+)', parts[1].strip())
            return int(m.group(1)) if m else 0
        return 0

    final_score = silver_score or gold_score if _2nd_runs(silver_score) >= _2nd_runs(gold_score) else gold_score

    raw_points: List[Dict[str, Any]] = []
    errors = 0
    for ms_path in snapshot_paths:
        base = ms_path.removesuffix("/match_state.json")
        try:
            # Always re-derive from raw silver data so that batting_team is resolved
            # from the innings market name (most reliable) rather than from cached
            # innings_snapshot.json which may have wrong batting_team from the PI bug.
            match_state = download_json(silver, ms_path)
            if not match_state:
                continue
            team_scores_doc    = download_json(silver, f"{base}/team_scores.json") or {}
            active_markets_doc = download_json(silver, f"{base}/active_markets.json") or {}
            point = extract_innings_snapshot(
                match_state,
                team_scores_doc.get("rows", []),
                active_markets_doc.get("rows", []),
            )
            if point is not None and (point.get("home_team_odds") is None or point.get("away_team_odds") is None):
                mkt_odds_doc = download_json(silver, f"{base}/market_odds.json") or {}
                for mr in mkt_odds_doc.get("rows", []):
                    if mr.get("market_key") == "3_1":
                        h = mr.get("home_odds_decimal")
                        a = mr.get("away_odds_decimal")
                        if h and a:
                            point["home_team_odds"] = h
                            point["away_team_odds"] = a
                            break
            if point is not None:
                raw_points.append(point)
        except Exception:
            errors += 1

    raw_points.sort(key=lambda p: str(p.get("snapshot_time_utc") or ""))
    seen: Dict[tuple, Dict[str, Any]] = {}
    for p in raw_points:
        key = (p.get("innings", 1), p.get("over"), p.get("score"))
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

    outcome = None
    actual_total = None
    import re as _re2
    m_score = _re2.match(r'^(\d+)', str(final_score or ""))
    if m_score:
        actual_total = int(m_score.group(1))
        last_pred = next(
            (r["predicted_total"] for r in reversed(deduped)
             if r.get("innings", 1) == 1 and r.get("predicted_total") is not None),
            None
        )
        if last_pred is not None:
            outcome = "over" if actual_total > last_pred else ("under" if actual_total < last_pred else "push")

    tracker = {**existing_gold, "rows": deduped, "last_updated_utc": utc_now().isoformat()}
    if final_score:
        tracker["score_summary_events"] = final_score
    if outcome is not None:
        tracker["outcome"] = outcome
    if actual_total is not None:
        tracker["actual_total"] = actual_total
    if stadium_data:
        tracker["stadium_data"] = stadium_data
    # Backfill header metadata from silver when existing_gold is missing them.
    # This handles events where innings_1.json was never built or had null metadata.
    if not tracker.get("match_name") and silver_match_name:
        tracker["match_name"] = silver_match_name
    if not tracker.get("league_id") and silver_league_id:
        tracker["league_id"] = silver_league_id
    if not tracker.get("league_name") and silver_league_name:
        tracker["league_name"] = silver_league_name
    if not tracker.get("home_team_name") and silver_home_team:
        tracker["home_team_name"] = silver_home_team
    if not tracker.get("away_team_name") and silver_away_team:
        tracker["away_team_name"] = silver_away_team
    if not tracker.get("match_date_utc") and silver_match_date:
        tracker["match_date_utc"] = silver_match_date
    upload_json(gold, gold_tracker_path, tracker, overwrite=True)

    if deduped:
        silver_tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json"
        upload_json(gold, silver_tracker_path, tracker, overwrite=True)

    return {
        "event_id": event_id,
        "silver_snapshots_scanned": len(snapshot_paths),
        "raw_points_extracted": len(raw_points),
        "rows_written": len(deduped),
        "errors": errors,
        "message": "accumulator rebuilt — reload the innings tracker page",
    }


def auto_rebuild_ended_innings() -> None:
    """Timer trigger body: detect matches that ended and auto-rebuild their innings tracker.

    A match is a candidate when:
      1. gold/innings_tracker/event_id={eid}/innings_1.json exists (or silver accumulator)
      2. innings_1.json has not been modified for > 1 hour (match is quiet)
      3. The match is NOT in the live index
      4. innings_1_from_silver.json does not exist, OR the silver accumulator has been
         updated after the file was last written (stale rebuild needed)
      5. The match's league_id is in the allowed leagues list

    Processes up to 2 matches per run to stay within the 10-minute function timeout.
    """
    from datetime import timedelta
    from leagues import load_allowed_league_ids

    now = utc_now()
    one_hour_ago = now - timedelta(hours=1)
    gold = get_named_container_client("gold")
    allowed_leagues = load_allowed_league_ids()

    # Live event IDs to exclude
    live_eids: set = set()
    try:
        live_idx = download_json(gold, "cricket/matches/latest/index.json") or {}
        for m in (live_idx.get("matches") or []):
            eid = str(m.get("event_id") or "")
            if eid:
                live_eids.add(eid)
    except Exception:
        pass

    # Map event_id → last_modified of innings_1_from_silver.json (None if not yet built)
    silver_file_written_at: Dict[str, Any] = {}
    tracker_prefix = "cricket/innings_tracker/event_id="
    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if blob.name.endswith("innings_1_from_silver.json"):
            parts = blob.name.split("/")
            ep = next((p for p in parts if p.startswith("event_id=")), None)
            if ep:
                silver_file_written_at[ep.replace("event_id=", "")] = blob.last_modified

    # Find candidates from two sources:
    # Source A — innings_1.json quiet for > 1 hour (match was captured live in gold)
    # Source B — silver accumulator exists but no innings_1.json (gold missed the match)
    # Both sources also include events where the accumulator was updated AFTER
    # innings_1_from_silver.json was last written (stale rebuild).
    candidates: List[str] = []
    seen: set = set()

    silver = get_named_container_client("silver")
    acc_prefix = "cricket/inplay/control/event_id="
    acc_last_mod: Dict[str, Any] = {}
    for blob in silver.list_blobs(name_starts_with=acc_prefix):
        if not blob.name.endswith("innings_accumulator.json"):
            continue
        parts = blob.name.split("/")
        ep = next((p for p in parts if p.startswith("event_id=")), None)
        if ep:
            acc_last_mod[ep.replace("event_id=", "")] = blob.last_modified

    for blob in gold.list_blobs(name_starts_with=tracker_prefix):
        if not blob.name.endswith("innings_1.json"):
            continue
        parts = blob.name.split("/")
        ep = next((p for p in parts if p.startswith("event_id=")), None)
        if not ep:
            continue
        eid = ep.replace("event_id=", "")
        if eid in live_eids or eid in seen:
            continue
        last_mod = blob.last_modified
        if last_mod and last_mod > one_hour_ago:
            continue
        # Skip if innings_1_from_silver.json exists AND the accumulator hasn't grown since
        silver_ts = silver_file_written_at.get(eid)
        if silver_ts is not None:
            acc_ts = acc_last_mod.get(eid)
            if acc_ts is None or acc_ts <= silver_ts:
                continue  # accumulator not updated since last rebuild — nothing new to do
        candidates.append(eid)
        seen.add(eid)

    # Source B: silver accumulators with no innings_1.json (or accumulator newer than existing file)
    for eid, acc_ts in acc_last_mod.items():
        if eid in live_eids or eid in seen:
            continue
        if acc_ts and acc_ts > one_hour_ago:
            continue
        silver_ts = silver_file_written_at.get(eid)
        if silver_ts is not None and acc_ts is not None and acc_ts <= silver_ts:
            continue  # file is up to date
        candidates.append(eid)
        seen.add(eid)

    if not candidates:
        logging.info(json.dumps({"event": "auto_rebuild_ended_innings_no_candidates"}))
        return

    # Filter by allowed league and rebuild up to 2 per run
    rebuilt = 0
    for eid in candidates:
        if rebuilt >= 2:
            break
        # Try gold innings_1.json first, fall back to silver accumulator for league_id
        tracker = (
            download_json(gold, f"cricket/innings_tracker/event_id={eid}/innings_1.json")
            or download_json(silver, f"cricket/inplay/control/event_id={eid}/innings_accumulator.json")
        )
        if not tracker:
            continue
        league_id = str(tracker.get("league_id") or "")
        if league_id not in allowed_leagues:
            logging.info(json.dumps({"event": "auto_rebuild_skip_league", "event_id": eid, "league_id": league_id}))
            continue
        try:
            result = _rebuild_innings_core(eid)
            logging.warning(json.dumps({"event": "auto_rebuild_ended_innings_done", **result}))
            rebuilt += 1
        except Exception:
            logging.exception(json.dumps({"event": "auto_rebuild_ended_innings_failed", "event_id": eid}))

    logging.warning(json.dumps({
        "event": "auto_rebuild_ended_innings_completed",
        "candidates_found": len(candidates),
        "rebuilt_this_run": rebuilt,
    }))


def view_admin_rebuild_innings(req: func.HttpRequest) -> func.HttpResponse:
    event_id = req.route_params.get("event_id", "").strip()
    if not event_id:
        return func.HttpResponse("event_id required", status_code=400)
    try:
        result = _rebuild_innings_core(event_id)
        return func.HttpResponse(json.dumps(result, indent=2), mimetype="application/json", status_code=200)
    except Exception as ex:
        logging.exception("admin_rebuild_innings_accumulator failed")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


def view_admin_reprocess_silver(req: func.HttpRequest) -> func.HttpResponse:
    """POST mgmt/reprocess-silver — called by ADF batch_silver_reprocess pipeline.

    Body: {"event_id": "12345678"}
    Rebuilds the silver innings accumulator and gold tracker for a finished match.
    """
    try:
        body = req.get_json()
        event_id = str(body.get("event_id") or "").strip()
    except Exception:
        event_id = ""
    if not event_id:
        return func.HttpResponse(
            json.dumps({"error": "event_id required in JSON body"}),
            mimetype="application/json", status_code=400,
        )
    # Reuse the same route params structure that view_admin_rebuild_innings expects
    # by constructing a fake request-like call — simpler to just inline the logic
    class _FakeReq:
        route_params = {"event_id": event_id}
    return view_admin_rebuild_innings(_FakeReq())


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
