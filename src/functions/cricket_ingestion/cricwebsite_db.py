import logging
import os
import re
from typing import Any, Dict, List, Optional

from storage import _is_innings_market, utc_now
from leagues import load_excluded_league_ids

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False


_cricwebsite_conn = None
_market_id_cache: Dict[str, Optional[int]] = {}


def _get_cricwebsite_conn():
    global _cricwebsite_conn
    host = os.environ.get("CRICWEBSITE_DB_HOST", "")
    if not host:
        return None
    try:
        if _cricwebsite_conn is None or _cricwebsite_conn.closed:
            _cricwebsite_conn = psycopg2.connect(
                host=host,
                port=int(os.environ.get("CRICWEBSITE_DB_PORT", "5432")),
                dbname=os.environ.get("CRICWEBSITE_DB_NAME", "oddsdb"),
                user=os.environ.get("CRICWEBSITE_DB_USER", ""),
                password=os.environ.get("CRICWEBSITE_DB_PASSWORD", ""),
                sslmode="require",
                connect_timeout=10,
            )
            logging.info("CricWebsite DB connection established")
        return _cricwebsite_conn
    except Exception as e:
        logging.warning(f"CricWebsite DB connect failed: {e}")
        _cricwebsite_conn = None
        return None


def _get_market_id(conn, name: str) -> Optional[int]:
    if name in _market_id_cache:
        return _market_id_cache[name]
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT market_id FROM tbl_markets WHERE name = %s LIMIT 1", (name,))
            row = cur.fetchone()
            _market_id_cache[name] = row[0] if row else None
            return _market_id_cache[name]
    except Exception as e:
        logging.warning(f"_get_market_id({name}) failed: {e}")
        return None


def write_to_cricwebsite_db(
    match: Dict[str, Any],
    team_scores: List[Dict[str, Any]],
    current_market_rows: List[Dict[str, Any]],
) -> None:
    """Write match, per-over score, and key market odds to CricWebsite oddsdb.

    Fails silently — never interrupts the main bronze→silver pipeline.
    """
    if not _PSYCOPG2_AVAILABLE or not os.environ.get("CRICWEBSITE_DB_HOST"):
        return

    league_id = str(match.get("league_id") or "")
    if league_id and league_id in load_excluded_league_ids():
        logging.info(f"write_to_cricwebsite_db skipped excluded league_id={league_id}")
        return

    try:
        conn = _get_cricwebsite_conn()
        if conn is None:
            return

        event_id = match.get("event_id")
        if not event_id:
            return
        try:
            event_id = int(event_id)
        except (TypeError, ValueError):
            return

        ts = str(match.get("time_status") or "")
        status = {"1": "inprogress", "3": "finished"}.get(ts, "notstarted")

        league = str(match.get("league_name") or "")
        if any(k in league for k in ("ODI", "One Day")):
            formate = "ODI"
        elif any(k in league for k in ("T20", "Twenty20")):
            formate = "T20"
        else:
            formate = "default"

        ss = str(match.get("score_summary_events") or match.get("score_summary_bet365") or "")
        ss_parts = [p.strip().replace("/", "-") for p in ss.split(",")]
        home_score = ss_parts[0] if ss_parts else None
        away_score = ss_parts[1] if len(ss_parts) > 1 else None
        inning_no = 2 if "," in ss else 1

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tbl_matches
                    (event_id, home_team_name, away_team_name, season_name,
                     status, venue, formate, start_timestamp, home_score, away_score, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (event_id) DO UPDATE SET
                    status      = EXCLUDED.status,
                    home_score  = EXCLUDED.home_score,
                    away_score  = EXCLUDED.away_score,
                    venue       = COALESCE(EXCLUDED.venue,       tbl_matches.venue),
                    season_name = COALESCE(EXCLUDED.season_name, tbl_matches.season_name),
                    formate     = COALESCE(EXCLUDED.formate,     tbl_matches.formate)
                RETURNING match_id
            """, (
                event_id,
                match.get("home_team_name"),
                match.get("away_team_name"),
                match.get("league_name"),
                status,
                match.get("venue"),
                formate,
                match.get("event_time_utc"),
                home_score,
                away_score,
            ))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return
            match_id = row[0]

            stats_id = None
            for ts_row in team_scores:
                over_str = str(ts_row.get("pg_over") or "").strip() or None
                if over_str is None:
                    continue

                runs = ts_row.get("runs")
                wickets = ts_row.get("wickets")
                cur.execute("""
                    INSERT INTO tbl_match_stats
                        (match_id, inning_no, over, score, wicket, created_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (match_id, inning_no, over) DO NOTHING
                    RETURNING match_stats_id
                """, (match_id, inning_no, over_str, runs, wickets))
                r2 = cur.fetchone()
                if r2:
                    stats_id = r2[0]
                break

            # Fallback: match is live but no over data yet (pre-play, toss done).
            # Write a sentinel stats row at over=0.0 so market odds can still be stored
            # and the match appears in the CricWebsite listing query.
            if stats_id is None:
                cur.execute("""
                    INSERT INTO tbl_match_stats
                        (match_id, inning_no, over, score, wicket, created_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (match_id, inning_no, over) DO NOTHING
                    RETURNING match_stats_id
                """, (match_id, inning_no, "0.0", None, None))
                r2 = cur.fetchone()
                if r2:
                    stats_id = r2[0]

            if stats_id:
                full_time_id = _get_market_id(conn, "Full time")
                innings_id = 2

                _SIMPLE_OV = re.compile(r'^[Oo]ver\s+([\d.]+)$')
                _SIMPLE_UN = re.compile(r'^[Uu]nder\s+([\d.]+)$')

                if full_time_id:
                    winner_rows = [
                        r for r in current_market_rows
                        if r.get("market_group_name") and any(
                            kw in r["market_group_name"].lower()
                            for kw in ("full time", "match winner", "match betting", "to win")
                        ) and r.get("odds_decimal") and not r.get("suspended")
                    ]
                    home = str(match.get("home_team_name") or "").lower()
                    for r in winner_rows:
                        sel = str(r.get("selection_name") or "").lower()
                        choice = "1" if home and home in sel else "2"
                        cur.execute("""
                            INSERT INTO tbl_market_odds
                                (match_stats_id, market_id, option, choice, odd, is_suspended, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,NOW())
                        """, (stats_id, full_time_id,
                              r.get("market_group_name"), choice,
                              r.get("odds_decimal"), bool(r.get("suspended"))))

                batting_team = next(
                    (str(ts.get("name") or "") for ts in team_scores if ts.get("pg_over")),
                    None,
                )
                total_overs = int(str(match.get("total_overs") or 20))
                innings_rows = [
                    r for r in current_market_rows
                    if r.get("market_group_name") and _is_innings_market(
                        r["market_group_name"], batting_team=batting_team, total_overs=total_overs
                    ) and r.get("odds_decimal")
                ]
                for r in innings_rows:
                    sel = str(r.get("display_selection_name") or r.get("selection_name") or "").strip()
                    if _SIMPLE_OV.match(sel):
                        choice = "over"
                    elif _SIMPLE_UN.match(sel):
                        choice = "under"
                    else:
                        continue
                    cur.execute("""
                        INSERT INTO tbl_market_odds
                            (match_stats_id, market_id, option, choice, odd,
                             detail, is_suspended, created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                    """, (stats_id, innings_id,
                          r.get("market_group_name"), choice,
                          r.get("odds_decimal"),
                          str(r.get("line") or r.get("handicap") or ""),
                          bool(r.get("suspended"))))

                # Fallback: no innings market available — write match winner odds with
                # market_id=2 so the match still appears in the CricWebsite listing query
                # (GET_MATCH_ODDS_FOR_ALL_MATCH requires an inner join on market_id=2).
                if not innings_rows:
                    winner_rows_fallback = [
                        r for r in current_market_rows
                        if r.get("market_group_name") and any(
                            kw in r["market_group_name"].lower()
                            for kw in ("full time", "match winner", "match betting", "to win")
                        ) and r.get("odds_decimal") and not r.get("suspended")
                    ]
                    home = str(match.get("home_team_name") or "").lower()
                    for r in winner_rows_fallback:
                        sel = str(r.get("selection_name") or "").lower()
                        choice = "1" if home and home in sel else "2"
                        cur.execute("""
                            INSERT INTO tbl_market_odds
                                (match_stats_id, market_id, option, choice, odd, is_suspended, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,NOW())
                        """, (stats_id, innings_id,
                              r.get("market_group_name"), choice,
                              r.get("odds_decimal"), bool(r.get("suspended"))))

        conn.commit()
        logging.debug(f"CricWebsite DB write OK event_id={event_id} match_id={match_id}")

    except Exception as e:
        logging.warning(f"write_to_cricwebsite_db failed for event {match.get('event_id')}: {e}")
        try:
            if _cricwebsite_conn and not _cricwebsite_conn.closed:
                _cricwebsite_conn.rollback()
        except Exception:
            pass
