from .common import (
    json, logging, escape, func,
    download_json, get_named_container_client,
    build_simple_table_page, adf_activity_badge,
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
        {adf_activity_badge("HypothesisTimeoutWicket")}
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
    generated = data.get("generated_at_utc", "")[:19].replace("T", " ")

    # Build sorted league list for the filter dropdown
    leagues = sorted({r.get("league_name") or "Unknown" for r in results})

    rows_html = ""
    for r in results:
        event_id    = escape(r.get("event_id") or "")
        league_name = escape(r.get("league_name") or "Unknown")
        match_date  = escape(r.get("match_date") or "")
        match       = escape(r.get("match_name") or r.get("event_id", ""))
        chasing     = escape(r.get("chasing_team") or "")
        batting_first = escape(r.get("batting_first_team") or "")
        target      = r.get("target") or "?"
        over_reached = r.get("over_reached") or "?"

        inn1_final     = r.get("inn1_final_score")
        inn1_final_str = r.get("inn1_final_display") or (str(inn1_final) if inn1_final is not None else "?")
        inn1_score = r.get("inn1_score_at_over6")
        inn1_wkts  = r.get("inn1_wickets_at_over6")
        inn1_str   = f"{inn1_score}/{inn1_wkts}" if inn1_score is not None and inn1_wkts is not None else ("?" if inn1_score is None else str(inn1_score))

        inn2_score = r.get("score_at_over6")
        inn2_wkts  = r.get("wickets_at_over6")
        inn2_str   = f"{inn2_score}/{inn2_wkts}" if inn2_score is not None and inn2_wkts is not None else ("?" if inn2_score is None else str(inn2_score))

        chasing_odds      = r.get("chasing_team_odds_at_over6")
        batting_first_odds = r.get("batting_first_team_odds_at_over6")
        chasing_odds_str      = f"{chasing_odds:.2f}" if chasing_odds else "—"
        batting_first_odds_str = f"{batting_first_odds:.2f}" if batting_first_odds else "—"

        fav         = escape(r.get("favorite_at_over6") or "—")
        winner      = escape(r.get("actual_winner") or "?")
        fav_won_val = r.get("favorite_won")
        final_score = r.get("final_innings2_score")
        final_wkts  = r.get("final_innings2_wickets")
        final_str   = (f"{final_score}/{final_wkts}" if final_wkts is not None else str(final_score)) if final_score is not None else "?"

        # encode favorite_won as data attribute for JS summary recalculation
        fav_won_attr = "true" if fav_won_val is True else ("false" if fav_won_val is False else "null")

        if fav_won_val is True:
            result_cell = '<td style="color:#1a7a1a;font-weight:bold;">✓ Yes</td>'
        elif fav_won_val is False:
            result_cell = '<td style="color:#aa1111;font-weight:bold;">✗ No</td>'
        else:
            result_cell = '<td style="color:#888;">— No odds</td>'

        rows_html += f"""<tr data-league="{league_name}" data-favwon="{fav_won_attr}">
            <td>{match_date}<br><small style="color:#888;font-family:monospace;">{event_id}</small></td>
            <td>{match}<br><small style="color:#888;">{league_name}</small></td>
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

    league_options = '<option value="ALL">All Leagues</option>' + "".join(
        f'<option value="{escape(ln)}">{escape(ln)}</option>' for ln in leagues
    )

    summary_and_filter_html = f"""
    <div id="summaryBanner" style="background:white;padding:20px;border-radius:10px;box-shadow:0 2px 8px #ddd;margin-bottom:16px;">
        <h2 style="margin:0 0 10px;">Hypothesis: Inn2 Over-6 Favourite Wins</h2>
        {adf_activity_badge("HypothesisInn2Over6")}
        <p style="color:#555;margin:0 0 14px;">During a chase, the team with lower match-winner odds <strong>after 6 overs of the 2nd innings</strong> wins the match.</p>
        <table style="border:none;box-shadow:none;background:transparent;width:auto;">
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Total matches</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statTotal">—</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">With odds data at over 6</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statEligible">—</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">No odds data</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statNoOdds">—</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Favourite won</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statVerdict">—</td></tr>
        </table>
        <p style="color:#999;font-size:12px;margin:12px 0 0;">Generated {generated} UTC</p>
    </div>
    <div style="margin-bottom:16px;">
        <label style="font-weight:600;margin-right:8px;">League:</label>
        <select id="leagueFilter" onchange="applyFilter()"
                style="padding:8px 12px;font-size:14px;border:1px solid #ccc;border-radius:6px;">
            {league_options}
        </select>
    </div>
    <script>
    function applyFilter() {{
        var league = document.getElementById('leagueFilter').value;
        var rows = document.querySelectorAll('#matchTable tr[data-league]');
        var total = 0, eligible = 0, favWon = 0, favLost = 0;
        rows.forEach(function(r) {{
            var show = league === 'ALL' || r.dataset.league === league;
            r.style.display = show ? '' : 'none';
            if (show) {{
                total++;
                var fw = r.dataset.favwon;
                if (fw === 'true')  {{ eligible++; favWon++; }}
                else if (fw === 'false') {{ eligible++; favLost++; }}
            }}
        }});
        var noOdds = total - eligible;
        var pct = eligible > 0 ? (100 * favWon / eligible).toFixed(1) : null;
        var verdictColor = !pct ? '#555' : (pct >= 60 ? '#1a7a1a' : (pct >= 50 ? '#cc6600' : '#aa1111'));
        var verdictText  = pct ? pct + '% (' + favWon + '/' + eligible + ' eligible matches)' : 'Not enough data yet';
        document.getElementById('statTotal').textContent    = total;
        document.getElementById('statEligible').textContent = eligible;
        document.getElementById('statNoOdds').textContent   = noOdds;
        var sv = document.getElementById('statVerdict');
        sv.textContent = verdictText;
        sv.style.color = verdictColor;
    }}
    document.addEventListener('DOMContentLoaded', applyFilter);
    </script>
    """

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

    html = table_page.replace(
        '<div class="hint">This page reads a small pre-built gold index file, so it should load quickly.</div>',
        summary_and_filter_html,
    )

    return func.HttpResponse(html, mimetype="text/html")


def view_hypothesis_inn1_prematch(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")
    data = download_json(gold, "cricket/hypothesis/inn1_prematch_over.json")

    if not data:
        return func.HttpResponse(
            "<p>No data yet. Run the <code>hypothesis_inn1_prematch</code> Databricks notebook first.</p>",
            mimetype="text/html",
            status_code=404,
        )

    results = sorted(data.get("results", []), key=lambda r: r.get("match_date") or "", reverse=True)
    generated = data.get("generated_at_utc", "")[:19].replace("T", " ")

    leagues = sorted({r.get("league_name") or "Unknown" for r in results})
    league_options = '<option value="ALL">All Leagues</option>' + "".join(
        f'<option value="{escape(ln)}">{escape(ln)}</option>' for ln in leagues
    )

    rows_html = ""
    for r in results:
        event_id    = escape(r.get("event_id") or "")
        league_name = escape(r.get("league_name") or "Unknown")
        match_date  = escape(r.get("match_date") or "")
        match       = escape(r.get("match_name") or r.get("event_id", ""))
        batting     = escape(r.get("batting_team_inn1") or "")
        line        = r.get("prematch_line")
        line_str    = f"{line:.1f}" if line is not None else "—"
        over_odds   = r.get("prematch_over_odds")
        under_odds  = r.get("prematch_under_odds")
        over_odds_str  = f"{over_odds:.2f}" if over_odds else "—"
        under_odds_str = f"{under_odds:.2f}" if under_odds else "—"
        actual      = r.get("actual_inn1_total")
        margin      = r.get("margin")
        margin_str  = f"{margin:+.1f}" if margin is not None else "—"
        result      = r.get("result") or ""

        if result == "OVER":
            result_cell = '<td style="color:#1a7a1a;font-weight:bold;">OVER</td>'
        elif result == "UNDER":
            result_cell = '<td style="color:#aa1111;font-weight:bold;">UNDER</td>'
        elif result == "PUSH":
            result_cell = '<td style="color:#888;">PUSH</td>'
        else:
            result_cell = '<td style="color:#888;">—</td>'

        rows_html += f"""<tr data-league="{league_name}" data-result="{escape(result)}">
            <td>{match_date}<br><small style="color:#888;font-family:monospace;">{event_id}</small></td>
            <td>{match}<br><small style="color:#888;">{league_name}</small></td>
            <td>{batting}</td>
            <td style="font-family:monospace;font-weight:bold;">{line_str}</td>
            <td style="font-family:monospace;">{over_odds_str} / {under_odds_str}</td>
            <td style="font-family:monospace;font-weight:bold;">{actual}</td>
            <td style="font-family:monospace;">{margin_str}</td>
            {result_cell}
        </tr>"""

    summary_and_filter_html = f"""
    <div id="summaryBanner" style="background:white;padding:20px;border-radius:10px;box-shadow:0 2px 8px #ddd;margin-bottom:16px;">
        <h2 style="margin:0 0 10px;">Hypothesis: Inn1 Pre-Match Score Over/Under</h2>
        {adf_activity_badge("HypothesisInn1Prematch")}
        <p style="color:#555;margin:0 0 6px;">The bet365 pre-match "1st Innings Score" Over/Under line is set before a ball is bowled, based purely on team strength, venue and conditions. Does the actual innings-1 total land OVER that line more often than UNDER?</p>
        <p style="color:#888;font-size:12px;margin:0 0 14px;">Only matches with a captured pre-match snapshot AND that specific market are eligible — pre-match capture started partway through the data history, so older matches are excluded.</p>
        <table style="border:none;box-shadow:none;background:transparent;width:auto;">
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">T20 matches scanned</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statTotal">—</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">Eligible (line available)</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statEligible">—</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">No pre-match market</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statNoMarket">—</td></tr>
            <tr><td style="border:none;padding:4px 16px 4px 0;color:#555;">OVER rate</td><td style="border:none;padding:4px 0;font-weight:bold;" id="statVerdict">—</td></tr>
        </table>
        <p style="color:#999;font-size:12px;margin:12px 0 0;">Generated {generated} UTC</p>
    </div>
    <div style="margin-bottom:16px;">
        <label style="font-weight:600;margin-right:8px;">League:</label>
        <select id="leagueFilter" onchange="applyFilter()"
                style="padding:8px 12px;font-size:14px;border:1px solid #ccc;border-radius:6px;">
            {league_options}
        </select>
    </div>
    <script>
    function applyFilter() {{
        var league = document.getElementById('leagueFilter').value;
        var rows = document.querySelectorAll('#matchTable tr[data-league]');
        var total = 0, over = 0, under = 0, push = 0;
        rows.forEach(function(r) {{
            var show = league === 'ALL' || r.dataset.league === league;
            r.style.display = show ? '' : 'none';
            if (show) {{
                total++;
                var res = r.dataset.result;
                if (res === 'OVER') over++;
                else if (res === 'UNDER') under++;
                else if (res === 'PUSH') push++;
            }}
        }});
        var eligible = over + under;
        var noMarket = total - eligible - push;
        var pct = eligible > 0 ? (100 * over / eligible).toFixed(1) : null;
        var verdictColor = !pct ? '#555' : (pct >= 55 ? '#1a7a1a' : (pct >= 45 ? '#cc6600' : '#aa1111'));
        var verdictText  = pct ? pct + '% OVER (' + over + '/' + eligible + ' eligible matches)' : 'Not enough data yet';
        document.getElementById('statTotal').textContent    = total;
        document.getElementById('statEligible').textContent = eligible;
        document.getElementById('statNoMarket').textContent = noMarket;
        var sv = document.getElementById('statVerdict');
        sv.textContent = verdictText;
        sv.style.color = verdictColor;
    }}
    document.addEventListener('DOMContentLoaded', applyFilter);
    </script>
    """

    headers = [
        "Date / Event ID",
        "Match",
        "Batting (Inn1)",
        "Pre-Match Line",
        "Over / Under Odds",
        "Actual Inn1 Total",
        "Margin (Actual − Line)",
        "Result",
    ]

    table_page = build_simple_table_page(
        title="Hypothesis: Inn1 Pre-Match Score Over/Under",
        headers=headers,
        rows_html=rows_html,
        back_link="/api/home",
    )

    html = table_page.replace(
        '<div class="hint">This page reads a small pre-built gold index file, so it should load quickly.</div>',
        summary_and_filter_html,
    )

    return func.HttpResponse(html, mimetype="text/html")
