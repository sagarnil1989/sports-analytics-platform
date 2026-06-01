from ._common import (
    json, logging, escape, func,
    download_json, get_named_container_client,
    build_simple_table_page,
)


def view_hypothesis_timeout_wicket(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    data = download_json(gold, "cricket/hypothesis/timeout_wicket.json")

    if not data:
        return func.HttpResponse(
            "<p>No data yet. Run the <code>hypothesis_timeout_wicket</code> Databricks notebook first.</p>",
            mimetype="text/html",
            status_code=404,
        )

    results = sorted(data.get("results", []), key=lambda r: r.get("match_date") or "", reverse=True)
    total = data.get("total_timeouts_detected", 0)
    eligible = data.get("eligible_timeouts", 0)
    wicket_yes = data.get("wicket_in_resumed_over_count", 0)
    wicket_no = data.get("no_wicket_count", 0)
    unknown = data.get("unknown_count", 0)
    win_pct = data.get("wicket_pct")
    threshold = data.get("timeout_threshold_seconds", 120)
    generated = data.get("generated_at_utc", "")[:19].replace("T", " ")

    if eligible > 0:
        verdict_color = "#1a7a1a" if (win_pct or 0) >= 60 else ("#cc6600" if (win_pct or 0) >= 40 else "#aa1111")
        verdict_text = f"{win_pct}% ({wicket_yes}/{eligible} eligible timeouts)"
    else:
        verdict_color = "#555"
        verdict_text = "Not enough data yet"

    summary_html = f"""
    <div style="background:white;padding:20px;border-radius:10px;box-shadow:0 2px 8px #ddd;margin-bottom:24px;">
        <h2 style="margin:0 0 10px;">Hypothesis: Wicket After Strategic Timeout</h2>
        <p style="color:#555;margin:0 0 6px;">After a strategic timeout (game state unchanged for &gt;{threshold}s), a wicket falls in the over that immediately resumes.</p>
        <p style="color:#888;font-size:12px;margin:0 0 14px;">Timeout detection: game paused if over and wickets are unchanged for &gt;{threshold}s. Normal delivery ≈30–60s; wicket fall ≈2–2.5 min; strategic timeout ≈3–5 min. Innings-break pauses (over 0.0 → 0.1) are excluded.</p>
        <table style="border:none;box-shadow:none;background:transparent;width:auto;">
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Total timeouts detected</td><td style="border:none;padding:4px 0;font-weight:bold;">{total}</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Eligible (over data available)</td><td style="border:none;padding:4px 0;font-weight:bold;">{eligible}</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Unknown (match ended in that over)</td><td style="border:none;padding:4px 0;font-weight:bold;">{unknown}</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Wicket in resumed over</td><td style="border:none;padding:4px 0;font-weight:bold;color:{verdict_color};">{verdict_text}</td></tr>
        </table>
        <p style="color:#999;font-size:12px;margin:12px 0 0;">Generated {generated} UTC</p>
    </div>
    """

    rows_html = ""
    for r in results:
        match = escape(r.get("match_name") or r.get("event_id", ""))
        innings = r.get("innings", "?")
        over_paused = r.get("over_when_paused", "?")
        duration_min = r.get("pause_duration_min", "?")
        over_resumed = r.get("over_resumed", "?")
        wickets_at = r.get("wickets_at_resume", "?")
        wickets_end = r.get("wickets_end_of_resumed_over")
        wickets_end_str = str(wickets_end) if wickets_end is not None else "?"

        w = r.get("wicket_in_resumed_over")
        if w is True:
            result_cell = '<td style="color:#1a7a1a;font-weight:bold;">✓ Yes</td>'
        elif w is False:
            result_cell = '<td style="color:#aa1111;font-weight:bold;">✗ No</td>'
        else:
            result_cell = '<td style="color:#888;">— Unknown</td>'

        rows_html += f"""<tr>
            <td>{match}</td>
            <td>Innings {innings}</td>
            <td style="font-family:monospace;">ov {over_paused}</td>
            <td style="font-family:monospace;">{duration_min} min</td>
            <td style="font-family:monospace;">ov {over_resumed}</td>
            <td style="font-family:monospace;">{wickets_at} → {wickets_end_str}</td>
            {result_cell}
        </tr>"""

    headers = [
        "Match", "Innings", "Paused At Over", "Pause Duration",
        "Resumed At Over", "Wickets (start → end of over)", "Wicket?"
    ]

    table_page = build_simple_table_page(
        title="Hypothesis: Wicket After Strategic Timeout",
        headers=headers,
        rows_html=rows_html,
        back_link="/api/home",
    )

    html = table_page.replace(
        '<div class="hint">This page reads a small pre-built gold index file, so it should load quickly.</div>',
        summary_html,
    )

    return func.HttpResponse(html, mimetype="text/html")


def view_hypothesis_inn2_over6(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    data = download_json(gold, "cricket/hypothesis/inn2_over6_favorite.json")

    if not data:
        return func.HttpResponse(
            "<p>No data yet. Run the <code>hypothesis_inn2_over6</code> Databricks notebook first.</p>",
            mimetype="text/html",
            status_code=404,
        )

    results = sorted(data.get("results", []), key=lambda r: r.get("match_date") or "", reverse=True)
    total = data.get("total_matches", 0)
    eligible = data.get("eligible_matches", 0)
    fav_won = data.get("favorite_won_count", 0)
    fav_lost = data.get("favorite_lost_count", 0)
    no_odds = data.get("no_odds_data_count", 0)
    win_pct = data.get("favorite_win_pct")
    generated = data.get("generated_at_utc", "")[:19].replace("T", " ")

    # Summary banner
    if eligible > 0:
        verdict_color = "#1a7a1a" if (win_pct or 0) >= 60 else ("#cc6600" if (win_pct or 0) >= 50 else "#aa1111")
        verdict_text = f"{win_pct}% ({fav_won}/{eligible} eligible matches)"
    else:
        verdict_color = "#555"
        verdict_text = "Not enough data yet"

    summary_html = f"""
    <div style="background:white;padding:20px;border-radius:10px;box-shadow:0 2px 8px #ddd;margin-bottom:24px;">
        <h2 style="margin:0 0 10px;">Hypothesis: Inn2 Over-6 Favourite Wins</h2>
        <p style="color:#555;margin:0 0 14px;">During a chase, the team with lower match-winner odds <strong>after 6 overs of the 2nd innings</strong> wins the match.</p>
        <table style="border:none;box-shadow:none;background:transparent;width:auto;">
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Total matches</td><td style="border:none;padding:4px 0;font-weight:bold;">{total}</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">With odds data at over 6</td><td style="border:none;padding:4px 0;font-weight:bold;">{eligible}</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">No odds data</td><td style="border:none;padding:4px 0;font-weight:bold;">{no_odds}</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Favourite won</td><td style="border:none;padding:4px 0;font-weight:bold;color:{verdict_color};">{verdict_text}</td></tr>
        </table>
        <p style="color:#999;font-size:12px;margin:12px 0 0;">Generated {generated} UTC</p>
    </div>
    """

    rows_html = ""
    for r in results:
        event_id = escape(r.get("event_id") or "")
        match_date = escape(r.get("match_date") or "")
        match = escape(r.get("match_name") or r.get("event_id", ""))
        chasing = escape(r.get("chasing_team") or "")
        batting_first = escape(r.get("batting_first_team") or "")
        target = r.get("target") or "?"
        over_reached = r.get("over_reached") or "?"

        inn1_final = r.get("inn1_final_score")
        inn1_final_str = str(inn1_final) if inn1_final is not None else "?"
        inn1_score = r.get("inn1_score_at_over6")
        inn1_wkts = r.get("inn1_wickets_at_over6")
        inn1_str = f"{inn1_score}/{inn1_wkts}" if inn1_score is not None and inn1_wkts is not None else ("?" if inn1_score is None else str(inn1_score))

        inn2_score = r.get("score_at_over6")
        inn2_wkts = r.get("wickets_at_over6")
        inn2_str = f"{inn2_score}/{inn2_wkts}" if inn2_score is not None and inn2_wkts is not None else ("?" if inn2_score is None else str(inn2_score))

        chasing_odds = r.get("chasing_team_odds_at_over6")
        batting_first_odds = r.get("batting_first_team_odds_at_over6")
        chasing_odds_str = f"{chasing_odds:.2f}" if chasing_odds else "—"
        batting_first_odds_str = f"{batting_first_odds:.2f}" if batting_first_odds else "—"

        fav = escape(r.get("favorite_at_over6") or "—")
        winner = escape(r.get("actual_winner") or "?")
        fav_won_val = r.get("favorite_won")
        final_score = r.get("final_innings2_score")
        final_wkts = r.get("final_innings2_wickets")
        final_str = (f"{final_score}/{final_wkts}" if final_wkts is not None else str(final_score)) if final_score is not None else "?"

        if fav_won_val is True:
            result_cell = '<td style="color:#1a7a1a;font-weight:bold;">✓ Yes</td>'
        elif fav_won_val is False:
            result_cell = '<td style="color:#aa1111;font-weight:bold;">✗ No</td>'
        else:
            result_cell = '<td style="color:#888;">— No odds</td>'

        rows_html += f"""<tr>
            <td>{match_date}<br><small style="color:#888;font-family:monospace;">{event_id}</small></td>
            <td>{match}</td>
            <td>{batting_first}<br><small style="color:#888;">vs {chasing} (chasing {target})</small></td>
            <td style="font-family:monospace;font-weight:bold;">{inn1_final_str}</td>
            <td style="font-family:monospace;">{inn1_str} @ ov {over_reached}</td>
            <td style="font-family:monospace;">{inn2_str} @ ov {over_reached}</td>
            <td style="font-family:monospace;">{batting_first_odds_str}<br><small style="color:#888;">{escape(batting_first)}</small></td>
            <td style="font-family:monospace;">{chasing_odds_str}<br><small style="color:#888;">{chasing}</small></td>
            <td><strong>{fav}</strong></td>
            <td>{winner}<br><small style="color:#888;">final: {final_str}</small></td>
            {result_cell}
        </tr>"""

    headers = [
        "Date / Event ID",
        "Match",
        "Teams",
        "Inn1 Final Score",
        "Inn1 Score @ Over 6",
        "Inn2 Score @ Over 6",
        "Batting-First Odds",
        "Chasing Odds",
        "Favourite at Over 6",
        "Actual Winner",
        "Favourite Won?",
    ]

    table_page = build_simple_table_page(
        title="Hypothesis: Inn2 Over-6 Favourite Wins",
        headers=headers,
        rows_html=rows_html,
        back_link="/api/home",
    )

    # Inject summary before the table
    html = table_page.replace(
        '<div class="hint">This page reads a small pre-built gold index file, so it should load quickly.</div>',
        summary_html,
    )

    return func.HttpResponse(html, mimetype="text/html")
