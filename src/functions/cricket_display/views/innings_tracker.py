from .common import (
    json, logging, os, escape, Any, Dict, List, Optional,
    func, ResourceNotFoundError,
    call_betsapi, download_json, download_required_json, format_unix_ts,
    get_named_container_client, parse_ss_final_scores,
    safe_float, upload_json, utc_now,
    extract_bet365_current_markets,
    collect_known_leagues, load_disabled_league_ids, save_league_preferences,
    extract_innings_snapshot,
    build_simple_table_page,
)


def view_innings_tracker_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        tracker = download_json(gold, f"event_id={event_id}/innings_tracker.json") or {}

        match_name = escape(str(tracker.get("match_name") or f"Match {event_id}"))
        venue = escape(str(tracker.get("venue") or ""))
        match_date = escape(str(tracker.get("match_date_utc") or "")[:10])
        league = escape(str(tracker.get("league_name") or ""))
        home_team = str(tracker.get("home_team_name") or "Home")
        away_team = str(tracker.get("away_team_name") or "Away")
        rows_data: List[Dict[str, Any]] = tracker.get("rows", [])
        outcome = tracker.get("outcome")
        actual_total = tracker.get("actual_total")
        if actual_total is None:
            _es = parse_ss_final_scores(tracker.get("score_summary_events") or "")
            actual_total = _es.get("inn1_runs")

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
    <p><a href="/api/live/view" style="color:#c00;font-weight:bold;">🔴 Live</a> | <a href="/api/matches/{escape(str(event_id))}/view">← Back to match</a> | <a href="/api/matches/{escape(str(event_id))}/heatmap">Heatmap</a> | <a href="/api/matches/{escape(str(event_id))}/detailed-analysis">Detailed Analysis</a> | <a href="/api/innings-tracker">All matches analytics</a></p>
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


def _prob_bar_html(pct_left: int, label_left: str, label_right: str,
                   color_left: str = "#2d7a2d", color_right: str = "#cc2200") -> str:
    pct_right = 100 - pct_left
    return (
        f'<div style="display:flex;height:22px;border-radius:4px;overflow:hidden;min-width:120px">'
        f'<div style="width:{pct_left}%;background:{color_left};color:white;font-size:11px;'
        f'font-weight:bold;display:flex;align-items:center;justify-content:center">'
        f'{label_left} {pct_left}%</div>'
        f'<div style="width:{pct_right}%;background:{color_right};color:white;font-size:11px;'
        f'font-weight:bold;display:flex;align-items:center;justify-content:center">'
        f'{label_right} {pct_right}%</div>'
        f'</div>'
    )


def _build_live_pred_html(live_preds: Dict) -> str:
    """
    Build the live ML predictions panel from live_predictions.json.
    Shows Over/Under predictions (Phase 5) and Win Predictor predictions (Phase 6).
    Returns empty string if neither exists.
    """
    ou_preds  = live_preds.get("ou_predictions") or []
    win_preds = live_preds.get("win_predictions") or []
    if not ou_preds and not win_preds:
        return ""

    gen_time     = str(live_preds.get("generated_at_utc", ""))[:16].replace("T", " ")
    current_over = live_preds.get("current_over", "?")

    _market_labels = {
        "innings_total":  "Innings Total",
        "first_12_overs": "First 12 Overs",
        "first_6_overs":  "First 6 Overs",
        "inn2_first_6":   "2nd Inn — First 6 Overs",
        "inn2_first_12":  "2nd Inn — First 12 Overs",
    }

    # ── O/U table ─────────────────────────────────────────────────────────────
    ou_section = ""
    if ou_preds:
        ou_rows = ""
        for p in sorted(ou_preds, key=lambda x: (x.get("innings", 1), x.get("checkpoint_over", 0))):
            market    = _market_labels.get(p.get("market", ""), p.get("market", ""))
            inn       = p.get("innings", 1)
            cp        = p.get("checkpoint_over", "?")
            po        = p.get("prob_over", 0.5)
            line      = p.get("betting_line")
            score     = p.get("score")
            wkts      = p.get("wickets", 0)
            score_str = f"{score}/{wkts}" if score is not None else "—"
            line_str  = str(line) if line is not None else "—"
            bar       = _prob_bar_html(round(po * 100), "O", "U")
            ou_rows  += (
                f'<tr>'
                f'<td>{escape(market)}</td>'
                f'<td style="text-align:center">Inn {inn}</td>'
                f'<td style="text-align:center"><b>{cp}</b></td>'
                f'<td style="text-align:center">{score_str}</td>'
                f'<td style="text-align:center"><b>{escape(line_str)}</b></td>'
                f'<td>{bar}</td>'
                f'</tr>'
            )
        ou_section = f"""
  <h4 style="margin:0 0 8px;color:#00e5ff;font-size:13px;font-weight:600">
    Over / Under Markets
  </h4>
  <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:16px;">
    <thead>
      <tr style="background:#1a2a3a;color:#aaa;">
        <th style="padding:6px 10px;text-align:left">Market</th>
        <th style="padding:6px 10px;text-align:center">Inn</th>
        <th style="padding:6px 10px;text-align:center">At Over</th>
        <th style="padding:6px 10px;text-align:center">Score</th>
        <th style="padding:6px 10px;text-align:center">Line</th>
        <th style="padding:6px 10px;text-align:left">P(Over) vs P(Under)</th>
      </tr>
    </thead>
    <tbody style="color:#ccc;">{ou_rows}</tbody>
  </table>"""

    # ── Win predictor table ────────────────────────────────────────────────────
    win_section = ""
    if win_preds:
        win_gen = str(live_preds.get("win_pred_generated_utc", ""))[:16].replace("T", " ")
        win_rows = ""
        for p in win_preds:
            cp       = escape(p.get("checkpoint", "?"))
            prob_ch  = p.get("prob_chase_wins", 0.5)
            inn1_sc  = p.get("inn1_total_score", "?")
            inn2_sc  = p.get("inn2_score_at_cp")
            inn2_wk  = p.get("inn2_wickets_at_cp")
            bat_team = escape(str(p.get("bat_team", "") or ""))
            bowl_team= escape(str(p.get("bowl_team", "") or ""))
            ctx_str  = f"Inn2 {inn2_sc}/{inn2_wk}" if inn2_sc is not None else f"Inn1 total {inn1_sc}"
            bar      = _prob_bar_html(round(prob_ch * 100), "Chase", "Defend",
                                      "#0066cc", "#7b2d00")
            win_rows += (
                f'<tr>'
                f'<td><b>{cp}</b></td>'
                f'<td style="font-size:12px;color:#aaa">{bat_team} vs {bowl_team}</td>'
                f'<td style="text-align:center;font-size:12px">{escape(ctx_str)}</td>'
                f'<td>{bar}</td>'
                f'</tr>'
            )
        win_section = f"""
  <h4 style="margin:0 0 8px;color:#00e5ff;font-size:13px;font-weight:600">
    Win Predictor &nbsp;<span style="font-weight:normal;color:#666;font-size:11px">updated {escape(win_gen)} UTC</span>
  </h4>
  <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead>
      <tr style="background:#1a2a3a;color:#aaa;">
        <th style="padding:6px 10px;text-align:left">Checkpoint</th>
        <th style="padding:6px 10px;text-align:left">Teams</th>
        <th style="padding:6px 10px;text-align:center">State at Checkpoint</th>
        <th style="padding:6px 10px;text-align:left">P(Chase wins) vs P(Defends)</th>
      </tr>
    </thead>
    <tbody style="color:#ccc;">{win_rows}</tbody>
  </table>"""

    return f"""
<div style="background:#0d1b2a;border-radius:8px;padding:18px 20px;margin:16px 0;color:#e0e0e0;">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">
    <span style="color:#f00;animation:blink 1s step-start infinite">●</span>
    <strong style="font-size:15px;color:#00e5ff">Live ML Predictions</strong>
    <span style="font-size:12px;color:#888">
      Over {escape(str(current_over))} &nbsp;·&nbsp; O/U updated {escape(gen_time)} UTC
    </span>
  </div>
  {ou_section}
  {win_section}
  <div style="font-size:11px;color:#555;margin-top:8px;">
    Predictions by <code>func-ramanuj-live-ml</code> ·
    O/U: <code>gold/ml/live_models/over_under/</code> ·
    Win: <code>gold/cricket/ml_features/t20/live_models/win_predictor/</code>
  </div>
</div>"""


def _build_ou_section_html(ou_preds: Dict, home_team: str, away_team: str) -> str:
    """
    Build the Over/Under predictions HTML section from over_under_predictions.json.
    Returns empty string if no predictions available.
    """
    it_rows  = ou_preds.get("innings_total", [])
    f12_rows = ou_preds.get("first_12_overs", [])
    if not it_rows and not f12_rows:
        return ""

    def _prob_bar(prob_over: float) -> str:
        pct_o = round(prob_over * 100)
        pct_u = 100 - pct_o
        return (
            f'<div class="ou-prob-bar">'
            f'<div class="ou-prob-over" style="width:{pct_o}%">{pct_o}%</div>'
            f'<div class="ou-prob-under" style="width:{pct_u}%">{pct_u}%</div>'
            f'</div>'
        )

    def _outcome_cell(p: Dict) -> str:
        outcome = p.get("outcome")
        if not outcome:
            return '<span class="ou-outcome-pending">Pending</span>'
        prob_over = p.get("prob_over", 0.5)
        predicted = "over" if prob_over >= 0.5 else "under"
        correct   = (predicted == outcome)
        label     = f"{'▲' if outcome == 'over' else '▼'} {outcome.upper()}"
        cls       = "ou-outcome-correct" if correct else "ou-outcome-wrong"
        tick      = " ✓" if correct else " ✗"
        return f'<span class="{cls}">{label}{tick}</span>'

    def _conf_badge(auc: float) -> str:
        if auc >= 0.65:
            return '<span style="background:#27ae60;color:white;font-size:10px;padding:1px 5px;border-radius:3px;">High</span>'
        if auc >= 0.55:
            return '<span style="background:#f39c12;color:white;font-size:10px;padding:1px 5px;border-radius:3px;">Med</span>'
        return '<span class="ou-conf-low">Low</span>'

    def _render_market_table(rows: List[Dict], title: str, subtitle: str) -> str:
        if not rows:
            return ""
        header = (
            f'<h3>{title}</h3>'
            f'<div class="ou-sub">{subtitle}</div>'
            '<table class="ou-table">'
            '<thead><tr>'
            '<th>Over</th><th>Score</th><th>Betting Line</th>'
            '<th>Odds (O/U)</th><th>P(OVER) vs P(UNDER)</th>'
            '<th>Confidence</th><th>Outcome</th>'
            '</tr></thead><tbody>'
        )
        body = ""
        for p in rows:
            cp       = p.get("checkpoint_over", "?")
            score    = p.get("score", "?")
            wkts     = 10 - p.get("wickets_in_hand", 10)
            line     = p.get("betting_line", "?")
            ov_odds  = p.get("over_odds")
            un_odds  = p.get("under_odds")
            prob_o   = p.get("prob_over", 0.5)
            auc      = p.get("model_auc", 0)
            odds_str = f"{ov_odds} / {un_odds}" if ov_odds and un_odds else "—"
            body += (
                f'<tr>'
                f'<td><b>{cp}</b></td>'
                f'<td>{score}/{wkts}</td>'
                f'<td><b>{line}</b></td>'
                f'<td style="color:#666;font-size:12px;">{escape(odds_str)}</td>'
                f'<td>{_prob_bar(prob_o)}</td>'
                f'<td>{_conf_badge(auc)}</td>'
                f'<td>{_outcome_cell(p)}</td>'
                f'</tr>'
            )
        return header + body + "</tbody></table>"

    it_html  = _render_market_table(
        it_rows,
        "Innings Total — Over/Under Predictions",
        "Does the 1st innings total beat the Bet365 line? Checkpoint = over at which prediction is made.",
    )
    f12_html = _render_market_table(
        f12_rows,
        "First 12 Overs — Over/Under Predictions",
        "Does the batting team's 12-over total beat the Bet365 First 12 Overs line?",
    )

    gen_time = escape(str(ou_preds.get("generated_at_utc", ""))[:16].replace("T", " "))
    footer   = f'<div style="font-size:11px;color:#aaa;margin-top:4px;">Predictions generated {gen_time} UTC · Confidence = model CV-AUC</div>'

    return f'<div class="ou-section">{it_html}{f12_html}{footer}</div>'


def view_silver_innings_tracker_html(req: func.HttpRequest) -> func.HttpResponse:
    """Innings tracker built from silver data.

    Reads gold/event_id={event_id}/innings_tracker.json which is pre-built by the
    batch silver reprocessing job (bronze_to_silver + silver_to_gold notebooks).
    Shows one row per unique match state with ball window, score, predicted total,
    and Match Winner 2-Way odds per delivery.
    """
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        tracker = download_json(gold, f"event_id={event_id}/innings_tracker.json") or {}

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
        match_date = escape(str(tracker.get("match_date_utc") or "")[:16].replace("T", " "))
        stadium    = tracker.get("stadium_data") or {}
        venue_str  = stadium.get("name") or tracker.get("venue") or ""
        city_str   = stadium.get("city") or ""
        country_str= stadium.get("country") or ""
        venue_parts= [p for p in [venue_str, city_str, country_str] if p]
        venue      = escape(", ".join(venue_parts))

        import re as _re
        final_ss = (
            tracker.get("score_summary_events")
            or tracker.get("final_score_ss")
            or tracker.get("score_summary_bet365")
            or ""
        )
        # Normalise separator (may be "-" or "," depending on source)
        final_ss = final_ss.replace("-", ",")

        rows_data: List[Dict[str, Any]] = tracker.get("rows", [])
        inn1_rows = [r for r in rows_data if r.get("innings") in (1, "1")]
        inn2_rows = [r for r in rows_data if r.get("innings") in (2, "2")]
        inn3_rows = [r for r in rows_data if r.get("innings") in (3, "3")]
        inn4_rows = [r for r in rows_data if r.get("innings") in (4, "4")]
        is_test   = bool(inn3_rows or inn4_rows or
                         "test" in match_name.lower() or
                         "test" in league.lower())

        def _last_over(rows):
            for r in reversed(rows):
                ov = r.get("over")
                if ov is not None:
                    return ov
            return None

        inn1_last_over = _last_over(inn1_rows)
        inn2_last_over = _last_over(inn2_rows)

        # Detect whether away team batted first
        inn1_bat_team = None
        for r in rows_data:
            if r.get("innings") in (1, "1") and r.get("batting_team"):
                inn1_bat_team = str(r["batting_team"]).strip()
                break
        away_batted_first = bool(inn1_bat_team and away_team and inn1_bat_team == away_team.strip())

        # Build display name in batting-first order for header
        if away_batted_first:
            swapped = match_name.replace(f"{home_team} vs {away_team}", f"{away_team} vs {home_team}", 1)
            display_match_name = escape(swapped if swapped != match_name else f"{away_team} vs {home_team}")
        else:
            display_match_name = match_name

        scoreboard_html = ""
        if final_ss and "," in final_ss:
            sc_parts = [p.strip() for p in final_ss.split(",")]
            def _parse_score(s):
                m = _re.match(r'^(\d+)(?:/(\d+))?', s)
                if m:
                    return int(m.group(1)), int(m.group(2)) if m.group(2) else 0, s
                return None, 0, s
            home_runs, home_wkts, home_raw = _parse_score(sc_parts[0])
            away_runs, away_wkts, away_raw = _parse_score(sc_parts[1] if len(sc_parts) > 1 else "")

            # Assign 1st/2nd innings display based on batting order
            if away_batted_first:
                inn1_team, inn1_raw, inn1_wkts, inn1_ov = away_team, away_raw, away_wkts, inn1_last_over
                inn2_team, inn2_raw, inn2_wkts, inn2_ov = home_team, home_raw, home_wkts, inn2_last_over
                # Away defended; home chased
                if home_runs is not None and away_runs is not None:
                    if home_runs > away_runs:
                        result_text = f"{escape(home_team)} won by {10 - home_wkts} wicket{'s' if (10 - home_wkts) != 1 else ''}"
                    elif away_runs > home_runs:
                        result_text = f"{escape(away_team)} won by {away_runs - home_runs} run{'s' if (away_runs - home_runs) != 1 else ''}"
                    else:
                        result_text = "Match tied"
                else:
                    result_text = ""
            else:
                inn1_team, inn1_raw, inn1_wkts, inn1_ov = home_team, home_raw, home_wkts, inn1_last_over
                inn2_team, inn2_raw, inn2_wkts, inn2_ov = away_team, away_raw, away_wkts, inn2_last_over
                # Home defended; away chased
                if home_runs is not None and away_runs is not None:
                    if away_runs > home_runs:
                        result_text = f"{escape(away_team)} won by {10 - away_wkts} wicket{'s' if (10 - away_wkts) != 1 else ''}"
                    elif home_runs > away_runs:
                        result_text = f"{escape(home_team)} won by {home_runs - away_runs} run{'s' if (home_runs - away_runs) != 1 else ''}"
                    else:
                        result_text = "Match tied"
                else:
                    result_text = ""

            def _ov_str(ov, wkts):
                if wkts == 10:
                    return " (All Out)"
                return ""

            venue_line = ""
            if venue:
                venue_line = f'<div class="sb-venue">📍 {venue} — <a href="/api/mgmt/stadium-override?event_id={escape(str(event_id))}">Edit</a></div>'
            else:
                venue_line = f'<div class="sb-venue" style="color:#aaa;">📍 Stadium unknown — <a href="/api/mgmt/stadium-override?event_id={escape(str(event_id))}">Add override</a></div>'

            if is_test and len(sc_parts) >= 3:
                # Test match scoreboard: show each innings separately
                # sc_parts order: [team_a_inn1, team_b_inn1, team_a_inn2?, team_b_inn2?]
                # Batting order: team_a batted 1st (inn1+inn3), team_b batted 2nd (inn2+inn4)
                team_a = escape(inn1_team if not away_batted_first else inn2_team)
                team_b = escape(inn2_team if not away_batted_first else inn1_team)
                def _inn_cell(raw):
                    return f'<span style="font-family:monospace;font-size:15px;font-weight:bold">{escape(raw)}</span>'
                inn_rows_sb = ""
                for idx, label in enumerate(["1st Inn", "2nd Inn", "3rd Inn", "4th Inn"]):
                    if idx >= len(sc_parts):
                        break
                    team = team_a if idx % 2 == 0 else team_b
                    inn_rows_sb += (
                        f'<div style="display:flex;gap:16px;align-items:baseline;margin-bottom:4px">'
                        f'<span style="font-size:11px;color:#888;min-width:55px">{label}</span>'
                        f'<span style="font-size:12px;color:#555;min-width:100px">{team}</span>'
                        f'{_inn_cell(sc_parts[idx])}'
                        f'</div>'
                    )
                scoreboard_html = f"""
                <div class="scoreboard">
                    <div style="font-size:11px;font-weight:bold;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Test Match</div>
                    {inn_rows_sb}
                    {f'<div class="sb-result">{result_text}</div>' if result_text else ""}
                    {venue_line}
                </div>"""
            else:
                scoreboard_html = f"""
                <div class="scoreboard">
                    <div class="sb-innings">
                        <div class="sb-team-score">
                            <span class="sb-teamname">{escape(inn1_team)}</span>
                            <span class="sb-runs">{escape(inn1_raw)}</span>
                            <span class="sb-overs">{_ov_str(inn1_ov, inn1_wkts)}</span>
                        </div>
                        <div class="sb-team-score">
                            <span class="sb-teamname">{escape(inn2_team)}</span>
                            <span class="sb-runs">{escape(inn2_raw)}</span>
                            <span class="sb-overs">{_ov_str(inn2_ov, inn2_wkts)}</span>
                        </div>
                    </div>
                    {f'<div class="sb-result">{result_text}</div>' if result_text else ""}
                    {venue_line}
                </div>"""
        actual_total = tracker.get("actual_total")
        if actual_total is None and "_ended2" in dir() and _ended2:
            _es2 = parse_ss_final_scores(_ended2.get("score", ""))
            actual_total = _es2.get("inn1_runs")
        outcome      = tracker.get("outcome")

        # ── Over/Under predictions section ────────────────────────────────────
        ou_preds     = download_json(gold, f"event_id={event_id}/over_under_predictions.json") or {}
        ou_section_html = _build_ou_section_html(ou_preds, home_team, away_team)

        # ── Live ML predictions (Phase 5) ──────────────────────────────────────
        live_preds     = download_json(gold, f"event_id={event_id}/live_predictions.json") or {}
        live_pred_html = _build_live_pred_html(live_preds)

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
            pred     = r.get("predicted_total")
            runs     = r.get("runs")
            wickets  = r.get("wickets", 0)
            target   = r.get("target")
            bat_team = str(r.get("batting_team") or home_team)
            bowl_team= str(r.get("bowling_team") or away_team)
            balls    = r.get("ball_window") or []
            home_odd = r.get("home_team_odds")
            away_odd = r.get("away_team_odds")
            ov_odds  = r.get("over_odds_at_line")
            un_odds  = r.get("under_odds_at_line")

            # Innings divider
            if innings != prev_innings:
                prev_innings = innings
                _inn_ord = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}
                if is_test:
                    inn_label = f'{_inn_ord.get(innings, str(innings))} Innings'
                elif innings == 1:
                    inn_label = "1st Innings"
                else:
                    inn_label = f'2nd Innings (chasing {target})'
                bat_label  = escape(innings_batting.get(innings, bat_team))
                bowl_label = escape(innings_bowling.get(innings, bowl_team))
                table_rows += f'<tr class="inn-divider"><td colspan="10">📌 {inn_label} — {bat_label} batting vs {bowl_label}</td></tr>'

            row_class = ""
            outcome_cell = "-"
            if innings == 1 and actual_total is not None and pred is not None:
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
            if runs is None:
                # Older rows store score as "102/4" string — parse it
                raw_score = str(r.get("score") or "")
                m_sc = _re.match(r'^(\d+)(?:/(\d+))?', raw_score)
                if m_sc:
                    runs = int(m_sc.group(1))
                    if m_sc.group(2) is not None:
                        wickets = int(m_sc.group(2))
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
    <title>Innings Tracker — {display_match_name}</title>
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
        .scoreboard {{ background: white; border-radius: 10px; box-shadow: 0 2px 8px #ddd;
                       padding: 18px 24px; margin: 16px 0; max-width: 540px; }}
        .sb-innings {{ display: flex; flex-direction: column; gap: 8px; margin-bottom: 12px; }}
        .sb-team-score {{ display: flex; align-items: baseline; gap: 10px; }}
        .sb-teamname {{ font-weight: bold; font-size: 15px; min-width: 220px; }}
        .sb-runs {{ font-size: 22px; font-weight: bold; color: #1a1a2e; }}
        .sb-overs {{ font-size: 13px; color: #888; }}
        .sb-result {{ font-size: 14px; font-weight: bold; color: #155724; background: #d4edda;
                      border-radius: 6px; padding: 6px 12px; display: inline-block; margin-bottom: 8px; }}
        .sb-venue {{ font-size: 13px; color: #555; margin-top: 8px; }}
        .ou-section {{ background: white; border-radius: 10px; box-shadow: 0 2px 8px #ddd; padding: 18px 24px; margin: 16px 0; }}
        .ou-section h3 {{ margin: 0 0 4px 0; font-size: 15px; color: #1a1a2e; }}
        .ou-section .ou-sub {{ font-size: 12px; color: #888; margin-bottom: 12px; }}
        .ou-table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 14px; }}
        .ou-table th {{ background: #2c3e50; color: white; padding: 7px 10px; text-align: left; font-size: 12px; }}
        .ou-table td {{ padding: 7px 10px; border-bottom: 1px solid #eee; }}
        .ou-prob-bar {{ display: flex; height: 18px; border-radius: 4px; overflow: hidden; width: 120px; }}
        .ou-prob-over {{ background: #27ae60; display: flex; align-items: center; justify-content: center; color: white; font-size: 10px; font-weight: bold; }}
        .ou-prob-under {{ background: #e74c3c; display: flex; align-items: center; justify-content: center; color: white; font-size: 10px; font-weight: bold; }}
        .ou-outcome-correct {{ color: #155724; font-weight: bold; }}
        .ou-outcome-wrong {{ color: #721c24; font-weight: bold; }}
        .ou-outcome-pending {{ color: #888; }}
        .ou-conf-low {{ font-size: 11px; color: #aaa; }}
    </style>
</head>
<body>
    <p><a href="/api/live/view" style="color:#c00;font-weight:bold;">🔴 Live</a> | <a href="/api/matches/{escape(str(event_id))}/view">← Back to match</a> | <a href="/api/matches/{escape(str(event_id))}/heatmap">Heatmap</a> | <a href="/api/matches/{escape(str(event_id))}/detailed-analysis">Detailed Analysis</a> | <a href="/api/innings-tracker">All matches analytics</a></p>
    <h1>Innings Tracker — {display_match_name}</h1>
    <div class="meta">📅 {match_date} &nbsp;|&nbsp; {league}</div>
    <div class="meta">{display_match_name} &nbsp;|&nbsp; {len(rows_data)} snapshots captured</div>
    {scoreboard_html}
    {summary_html}
    {ou_section_html}
    {live_pred_html}
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

        idx = download_json(gold, "index/innings_tracker.json") or {}
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
            tracker = download_json(gold, f"event_id={event_id_m}/innings_tracker.json") or {}

            for r in tracker.get("rows", []):
                # Analytics page is for innings 1 O/U prediction only — skip innings 2 rows
                if r.get("innings", 1) != 1:
                    continue
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
