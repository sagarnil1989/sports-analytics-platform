import json
import logging
import os
import re
from html import escape
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False

app = func.FunctionApp()

# ------------------------------------------------------------------
# Module-level innings market detection (shared by extraction + DB writer)
# ------------------------------------------------------------------
_INNINGS_MARKET_RE = re.compile(
    r'\b\d+\s+overs?\s+runs?\b'      # "50 Overs Runs", "20 Overs Run"
    r'|'
    r'\bruns?\s+in\s+\d+\s+overs?\b', # "Runs in 20 Overs"
    re.IGNORECASE,
)
_INNINGS_KEYWORDS = ["current inn", "innings run", "1st inn", "inning run"]

def _is_innings_market(name: str) -> bool:
    n = name.lower()
    if any(kw in n for kw in _INNINGS_KEYWORDS):
        return True
    return bool(_INNINGS_MARKET_RE.search(name))

# ------------------------------------------------------------------
# CricWebsite PostgreSQL writer
# ------------------------------------------------------------------
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
    Writes two markets only: 'Full time' (match winner) and 'Current innings runs'.
    """
    if not _PSYCOPG2_AVAILABLE or not os.environ.get("CRICWEBSITE_DB_HOST"):
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

        # status
        ts = str(match.get("time_status") or "")
        status = {"1": "inprogress", "3": "finished"}.get(ts, "notstarted")

        # format (T20 / ODI / default)
        league = str(match.get("league_name") or "")
        if any(k in league for k in ("ODI", "One Day")):
            formate = "ODI"
        elif any(k in league for k in ("T20", "Twenty20")):
            formate = "T20"
        else:
            formate = "default"

        # score strings for tbl_matches ("127-1" style, home vs away)
        ss = str(match.get("score_summary_events") or match.get("score_summary_bet365") or "")
        ss_parts = [p.strip().replace("/", "-") for p in ss.split(",")]
        home_score = ss_parts[0] if ss_parts else None
        away_score = ss_parts[1] if len(ss_parts) > 1 else None

        # inning_no heuristic: no comma in SS = inning 1 still in progress
        inning_no = 2 if "," in ss else 1

        with conn.cursor() as cur:
            # 1 — upsert tbl_matches (unique on event_id)
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

            # 2 — upsert tbl_match_stats for the currently batting team's over/score
            # Find the team row that has over data (the batting team)
            stats_id = None
            for ts_row in team_scores:
                over_str = None

                # Priority 1: PG field from stats=1 call
                pg_over = ts_row.get("pg_over")

                if pg_over is not None:
                    try:
                        pg_raw = str(pg_over).strip()

                        # Case 1: raw PG format, e.g. "4:6:0:1wd:1:4#16:2:1"
                        if "#" in pg_raw:
                            stats_part = pg_raw.split("#", 1)[1]
                            stats = stats_part.split(":")

                            full_overs = int(stats[0]) - 1
                            balls = int(stats[1]) - 1

                            if full_overs >= 0 and balls >= 0:
                                over_str = f"{full_overs}.{balls}"

                        # Case 2: already parsed cricket over format, e.g. "15.2"
                        else:
                            over_str = pg_raw

                    except (ValueError, IndexError):
                        pass


                # Priority 2: S3 field
                if over_str is None:
                    s3 = str(ts_row.get("s3") or "").strip()
                    m_s3 = re.match(r'^(\d+)\.?(\d?)$', s3)
                    if m_s3:
                        full_overs = int(m_s3.group(1))
                        balls = int(m_s3.group(2)) if m_s3.group(2) else 0
                        over_str = f"{full_overs}.{balls}"
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
                break  # only the batting team row matters

            # 3 — insert market odds for the two key markets
            # market_id=16 = Full time (match winner), market_id=2 = Innings total runs
            # Hardcoded because the innings market name is now team-specific
            # e.g. "Mumbai Indians 20 Overs Runs" — stored in option column, not market name.
            if stats_id:
                full_time_id = _get_market_id(conn, "Full time")
                innings_id   = 2  # always market_id=2 regardless of team-specific market name

                # Full time: match winner odds
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

                # Current innings runs: over/under odds
                if innings_id:
                    _SIMPLE_OV = re.compile(r'^[Oo]ver\s+([\d.]+)$')
                    _SIMPLE_UN = re.compile(r'^[Uu]nder\s+([\d.]+)$')
                    innings_rows = [
                        r for r in current_market_rows
                        if r.get("market_group_name") and _is_innings_market(r["market_group_name"])
                        and r.get("odds_decimal")
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

        conn.commit()
        logging.debug(f"CricWebsite DB write OK event_id={event_id} match_id={match_id}")

    except Exception as e:
        logging.warning(f"write_to_cricwebsite_db failed for event {match.get('event_id')}: {e}")
        try:
            if _cricwebsite_conn and not _cricwebsite_conn.closed:
                _cricwebsite_conn.rollback()
        except Exception:
            pass


# -----------------------------
# Common helpers
# -----------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def format_unix_ts(ts: Optional[Any]) -> Optional[str]:
    """Convert unix timestamp (seconds) to readable UTC string."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return None
    
def ts_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def get_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def get_blob_service_client() -> BlobServiceClient:
    storage_conn = get_env("DATA_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(storage_conn)


def get_named_container_client(container_name: str):
    blob_service = get_blob_service_client()
    container = blob_service.get_container_client(container_name)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    except Exception:
        pass
    return container


def get_bronze_container_client():
    return get_named_container_client("bronze")


def upload_json(container_client, blob_path: str, payload: Dict[str, Any], overwrite: bool = False) -> None:
    container_client.upload_blob(
        name=blob_path,
        data=json.dumps(payload, indent=2, ensure_ascii=False),
        overwrite=overwrite,
        content_settings=ContentSettings(content_type="application/json"),
    )


def download_json(container_client, blob_path: str) -> Optional[Dict[str, Any]]:
    try:
        data = container_client.download_blob(blob_path).readall()
        return json.loads(data)
    except ResourceNotFoundError:
        return None


def download_required_json(container_client, blob_path: str) -> Dict[str, Any]:
    data = container_client.download_blob(blob_path).readall()
    return json.loads(data)


def blob_exists(container_client, blob_path: str) -> bool:
    try:
        container_client.get_blob_client(blob_path).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False


def call_betsapi(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    base_url = get_env("BETS_API_BASE_URL", "https://api.b365api.com").rstrip("/")
    token = get_env("BETS_API_TOKEN")

    url = f"{base_url}{path}"
    query = dict(params)
    query["token"] = token
    started = utc_now()

    try:
        response = requests.get(url, params=query, timeout=20)
        elapsed_ms = int((utc_now() - started).total_seconds() * 1000)
        try:
            body = response.json()
        except ValueError:
            body = {"raw_text": response.text}
        success = response.status_code == 200 and isinstance(body, dict) and body.get("success") in [1, "1", True]
        return {
            "request": {
                "url": url,
                "params_without_token": {k: v for k, v in query.items() if k != "token"},
                "called_at_utc": started.isoformat(),
            },
            "response": {
                "http_status_code": response.status_code,
                "elapsed_ms": elapsed_ms,
                "success": success,
                "error": body.get("error") if isinstance(body, dict) else None,
                "error_detail": body.get("error_detail") if isinstance(body, dict) else None,
                "body": body,
            },
        }
    except requests.RequestException as ex:
        elapsed_ms = int((utc_now() - started).total_seconds() * 1000)
        logging.exception("BetsAPI request failed")
        return {
            "request": {
                "url": url,
                "params_without_token": {k: v for k, v in query.items() if k != "token"},
                "called_at_utc": started.isoformat(),
            },
            "response": {
                "http_status_code": None,
                "elapsed_ms": elapsed_ms,
                "success": False,
                "error": "REQUEST_EXCEPTION",
                "error_detail": str(ex),
                "body": None,
            },
        }


def extract_results(api_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = api_payload.get("response", {}).get("body")
    if not isinstance(body, dict):
        return []
    results = body.get("results")
    if isinstance(results, list):
        return results
    return []


def get_event_id_from_inplay_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ["our_event_id", "event_id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    if item.get("bet365_id") is not None:
        value = item.get("id")
        if value is not None and str(value).strip():
            return str(value)
    value = item.get("id")
    if value is not None and str(value).strip():
        return str(value)
    return None


def get_fi_from_inplay_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ["bet365_id", "FI", "fi", "id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def summarize_inplay_items(items: List[Dict[str, Any]], max_live_matches: int) -> List[Dict[str, Any]]:
    live = []
    for item in items:
        fi = get_fi_from_inplay_item(item)
        event_id = get_event_id_from_inplay_item(item)
        if not fi:
            continue
        # time_status=1 means in-play; skip ended (3) and not-started (0) matches
        if str(item.get("time_status", "1")) != "1":
            continue
        _league = item.get("league") or {}
        live.append({
            "fi": fi,
            "event_id": event_id or fi,
            "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
            "league_id": str(_league.get("id")) if _league.get("id") is not None else None,
            "league_name": _league.get("name"),
            "league": _league,
            "home": item.get("home"),
            "away": item.get("away"),
            "time_status": item.get("time_status"),
            "raw_item": item,
        })
    if max_live_matches <= 0:
        return live
    return live[:max_live_matches]


def get_api_result_count(api_payload: Optional[Dict[str, Any]]) -> Optional[int]:
    """Return a simple count of results in a stored BetsAPI wrapper."""
    if not api_payload:
        return None
    body = api_payload.get("response", {}).get("body")
    if not isinstance(body, dict):
        return None
    results = body.get("results")
    if isinstance(results, list):
        return len(results)
    if isinstance(results, dict):
        return len(results)
    return None


def build_api_call_lineage(
    api_name: str,
    api_payload: Optional[Dict[str, Any]],
    id_used: Dict[str, Any],
    bronze_path: Optional[str],
    purpose: str,
) -> Dict[str, Any]:
    """Small lineage row so we can see which API used which id and where the raw data landed."""
    response = (api_payload or {}).get("response", {})
    request = (api_payload or {}).get("request", {})
    return {
        "api_name": api_name,
        "purpose": purpose,
        "id_used": id_used,
        "path": request.get("url"),
        "params_without_token": request.get("params_without_token"),
        "called_at_utc": request.get("called_at_utc"),
        "http_status_code": response.get("http_status_code"),
        "success": response.get("success"),
        "elapsed_ms": response.get("elapsed_ms"),
        "error": response.get("error"),
        "error_detail": response.get("error_detail"),
        "result_count": get_api_result_count(api_payload),
        "bronze_path": bronze_path,
    }


def build_live_snapshot_lineage(
    sport_id: str,
    event_id: str,
    fi: str,
    base_path: str,
    payloads: Dict[str, Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    """Lineage for one live snapshot.

    Important rule:
    - event_id is used for BetsAPI event endpoints.
    - fi / bet365_id is used for Bet365 event endpoint.
    """
    bronze_base = f"bronze/{base_path}"
    calls = [
        build_api_call_lineage(
            "events_inplay",
            payloads.get("events_inplay"),
            {"sport_id": sport_id},
            f"{bronze_base}/api_inplay_event_list.json",
            "Find live cricket matches and current score/status from /v3/events/inplay.",
        ),
        build_api_call_lineage(
            "event_view",
            payloads.get("event_view"),
            {"event_id": event_id},
            f"{bronze_base}/api_event_view.json",
            "Get event scoreboard/details from /v1/event/view using event_id.",
        ),
        build_api_call_lineage(
            "event_odds_summary",
            payloads.get("event_odds_summary"),
            {"event_id": event_id},
            f"{bronze_base}/api_event_odds_summary.json",
            "Get compact odds summary from /v2/event/odds/summary using event_id.",
        ),
        build_api_call_lineage(
            "event_odds",
            payloads.get("event_odds"),
            {"event_id": event_id},
            f"{bronze_base}/api_event_odds.json",
            "Get event odds history from /v2/event/odds using event_id.",
        ),
        build_api_call_lineage(
            "bet365_event",
            payloads.get("bet365_event"),
            {"FI": fi},
            f"{bronze_base}/api_live_market_odds.json",
            "Get live Bet365 market stream from /v1/bet365/event using FI/bet365_id.",
        ),
    ]
    return {
        "generated_at_utc": utc_now().isoformat(),
        "sport_id": sport_id,
        "event_id": event_id,
        "fi": fi,
        "id_mapping": {
            "event_id": {
                "value": event_id,
                "used_for": ["/v1/event/view", "/v2/event/odds/summary", "/v2/event/odds"],
            },
            "fi": {
                "value": fi,
                "also_called": "bet365_id",
                "used_for": ["/v1/bet365/event"],
            },
        },
        "bronze_base_path": f"bronze/{base_path}",
        "api_calls": calls,
    }


# -----------------------------
# League preferences
# -----------------------------

_LEAGUE_PREFS_PATH = "cricket/config/league_preferences.json"


def load_excluded_league_ids() -> set:
    """Return a set of league_id strings that are excluded from capture."""
    gold = get_named_container_client("gold")
    prefs = download_json(gold, _LEAGUE_PREFS_PATH) or {}
    return set(str(lid) for lid in prefs.get("excluded_league_ids", []))


def save_league_preferences(excluded_ids: set) -> None:
    gold = get_named_container_client("gold")
    upload_json(
        gold,
        _LEAGUE_PREFS_PATH,
        {"excluded_league_ids": sorted(excluded_ids), "updated_at_utc": utc_now().isoformat()},
        overwrite=True,
    )


def collect_known_leagues() -> List[Dict[str, Any]]:
    """Gather all leagues seen across live, prematch, ended, league, and innings-tracker indexes."""
    gold = get_named_container_client("gold")
    leagues: Dict[str, Dict[str, Any]] = {}

    def merge(lid: Optional[str], lname: Optional[str], source: str) -> None:
        lid = str(lid or "").strip()
        if not lid:
            return
        if lid not in leagues:
            leagues[lid] = {"league_id": lid, "league_name": lname or lid, "sources": []}
        if source not in leagues[lid]["sources"]:
            leagues[lid]["sources"].append(source)
        if lname and lname != lid and not leagues[lid].get("league_name"):
            leagues[lid]["league_name"] = lname

    # Match-level indexes: live, prematch, ended
    for idx_path, source in [
        ("cricket/matches/latest/index.json", "live"),
        ("cricket/prematch/latest/index.json", "prematch"),
        ("cricket/ended/latest/index.json", "ended"),
    ]:
        try:
            idx = download_json(gold, idx_path) or {}
            for m in (idx.get("matches") or []):
                merge(m.get("league_id"), m.get("league_name"), source)
        except Exception:
            pass

    # Prebuilt league-level indexes (more stable than match-level)
    for idx_path, source in [
        ("cricket/leagues/index.json", "live_leagues"),
        ("cricket/prematch/leagues/index.json", "prematch_leagues"),
    ]:
        try:
            idx = download_json(gold, idx_path) or {}
            for lg in (idx.get("leagues") or []):
                merge(lg.get("league_id"), lg.get("league_name"), source)
        except Exception:
            pass

    # Innings tracker index — captures historical leagues even if gold indexes are stale
    try:
        tracker_idx = download_json(gold, "cricket/innings_tracker/index.json") or {}
        for m in (tracker_idx.get("matches") or []):
            merge(m.get("league_id"), m.get("league_name"), "innings_tracker")
    except Exception:
        pass

    return sorted(leagues.values(), key=lambda x: x.get("league_name") or "")


# -----------------------------
# Bronze ingestion
# -----------------------------

@app.timer_trigger(schedule="*/5 * * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def discover_cricket_inplay(timer: func.TimerRequest) -> None:
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_live_matches = get_int_env("MAX_LIVE_MATCHES", 50)
    container = get_bronze_container_client()

    api_payload = call_betsapi(path="/v3/events/inplay", params={"sport_id": sport_id})

    raw_path = (
        f"betsapi/inplay_filter/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"events_inplay_{ts_compact(now)}.json"
    )
    upload_json(container, raw_path, api_payload)

    active_matches = summarize_inplay_items(extract_results(api_payload), max_live_matches)
    control_payload = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "max_live_matches": max_live_matches,
        "active_match_count": len(active_matches),
        "active_matches": active_matches,
        "source_raw_path": f"bronze/{raw_path}",
    }
    upload_json(container, "betsapi/control/active_inplay_fi/latest.json", control_payload, overwrite=True)

    logging.info(json.dumps({
        "event": "discover_cricket_inplay_completed",
        "success": api_payload["response"]["success"],
        "active_match_count": len(active_matches),
        "raw_path": f"bronze/{raw_path}",
    }))


@app.timer_trigger(schedule="*/5 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_inplay_snapshot(timer: func.TimerRequest) -> None:
    """Capture one full live snapshot per active match.

    Bronze is intentionally complete and verbose. We store every important live
    API response separately so future silver/gold logic can be changed without
    losing raw data.

    APIs captured:
    1. /v3/events/inplay                 -> list/status/score using sport_id
    2. /v1/event/view                    -> event details using event_id
    3. /v2/event/odds/summary            -> odds summary using event_id
    4. /v2/event/odds                    -> odds history using event_id
    5. /v1/bet365/event                  -> full Bet365 live markets using FI
    """
    sport_id = get_env("SPORT_ID", "3")
    container = get_bronze_container_client()

    control = download_json(container, "betsapi/control/active_inplay_fi/latest.json")
    if not control or not control.get("active_matches"):
        logging.info(json.dumps({"event": "capture_cricket_inplay_snapshot_skipped", "reason": "no_active_matches"}))
        return

    active_matches = control["active_matches"]
    excluded_leagues = load_excluded_league_ids()

    # Capture the full inplay response once per run and copy it into every
    # snapshot folder for lineage/debugging.
    events_inplay_payload = call_betsapi(path="/v3/events/inplay", params={"sport_id": sport_id})

    for match in active_matches:
        if excluded_leagues and str(match.get("league_id") or "") in excluded_leagues:
            logging.info(json.dumps({"event": "inplay_snapshot_skipped_league", "league_id": match.get("league_id"), "league_name": match.get("league_name")}))
            continue
        snapshot_time = utc_now()
        snapshot_id = ts_compact(snapshot_time)
        fi = str(match["fi"])
        event_id = str(match.get("event_id") or fi)

        # Use event_id for event APIs.
        event_view_payload = call_betsapi(path="/v1/event/view", params={"event_id": event_id})
        event_odds_summary_payload = call_betsapi(path="/v2/event/odds/summary", params={"event_id": event_id})
        event_odds_payload = call_betsapi(path="/v2/event/odds", params={"event_id": event_id})

        # Use FI / bet365_id for Bet365 live markets.
        bet365_event_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi})
        # stats=1 adds PG field (over progress) to TE records but suppresses markets — call separately.
        bet365_event_stats_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi, "stats": 1})

        base_path = f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}"

        lineage = build_live_snapshot_lineage(
            sport_id=sport_id,
            event_id=event_id,
            fi=fi,
            base_path=base_path,
            payloads={
                "events_inplay": events_inplay_payload,
                "event_view": event_view_payload,
                "event_odds_summary": event_odds_summary_payload,
                "event_odds": event_odds_payload,
                "bet365_event": bet365_event_payload,
            },
        )

        manifest = {
            "snapshot_id": snapshot_id,
            "snapshot_time_utc": snapshot_time.isoformat(),
            "sport_id": sport_id,
            "event_id": event_id,
            "fi": fi,
            "match_from_filter": match,
            "files": {
                "api_inplay_event_list": f"bronze/{base_path}/api_inplay_event_list.json",
                "api_event_view": f"bronze/{base_path}/api_event_view.json",
                "api_event_odds_summary": f"bronze/{base_path}/api_event_odds_summary.json",
                "api_event_odds": f"bronze/{base_path}/api_event_odds.json",
                "api_live_market_odds": f"bronze/{base_path}/api_live_market_odds.json",
                "lineage": f"bronze/{base_path}/lineage.json",
            },
            "api_lineage": lineage,
            "status": {
                "events_inplay_success": events_inplay_payload["response"]["success"],
                "event_view_success": event_view_payload["response"]["success"],
                "event_view_error": event_view_payload["response"].get("error"),
                "event_view_error_detail": event_view_payload["response"].get("error_detail"),
                "event_odds_summary_success": event_odds_summary_payload["response"]["success"],
                "event_odds_summary_error": event_odds_summary_payload["response"].get("error"),
                "event_odds_summary_error_detail": event_odds_summary_payload["response"].get("error_detail"),
                "event_odds_success": event_odds_payload["response"]["success"],
                "event_odds_error": event_odds_payload["response"].get("error"),
                "event_odds_error_detail": event_odds_payload["response"].get("error_detail"),
                "bet365_event_success": bet365_event_payload["response"]["success"],
                "bet365_event_error": bet365_event_payload["response"].get("error"),
                "bet365_event_error_detail": bet365_event_payload["response"].get("error_detail"),
            },
        }

        upload_json(container, f"{base_path}/api_inplay_event_list.json", events_inplay_payload)
        upload_json(container, f"{base_path}/api_event_view.json", event_view_payload)
        upload_json(container, f"{base_path}/api_event_odds_summary.json", event_odds_summary_payload)
        upload_json(container, f"{base_path}/api_event_odds.json", event_odds_payload)
        upload_json(container, f"{base_path}/api_live_market_odds.json", bet365_event_payload)
        upload_json(container, f"{base_path}/api_live_market_stats.json", bet365_event_stats_payload)
        upload_json(container, f"{base_path}/lineage.json", lineage)
        upload_json(container, f"{base_path}/manifest.json", manifest)


# -----------------------------
# Silver parsing helpers
# -----------------------------

def list_manifest_paths(bronze_container, sport_id: str, limit: int) -> List[str]:
    prefix = f"betsapi/inplay_snapshot/sport_id={sport_id}/"
    manifests = []
    for blob in bronze_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/manifest.json"):
            manifests.append((getattr(blob, "last_modified", None), name))
    manifests.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in manifests[:limit]]


def parse_runs_wickets(raw: Optional[str]) -> Dict[str, Optional[int]]:
    if not raw or "#" not in raw:
        return {"runs": None, "wickets": None}
    left, right = raw.split("#", 1)
    try:
        runs = int(left.strip())
    except Exception:
        runs = None
    try:
        wickets = int(right.strip())
    except Exception:
        wickets = None
    return {"runs": runs, "wickets": wickets}


def extract_bet365_records(bet365_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Use `or {}` so an explicit None body (failed API call) is treated as empty.
    body = bet365_payload.get("response", {}).get("body") or {}
    results = body.get("results", [])
    if results and isinstance(results[0], list):
        return results[0]
    if isinstance(results, list):
        return results
    return []


def find_matching_event(events_inplay_payload: Dict[str, Any], event_id: str, fi: str) -> Optional[Dict[str, Any]]:
    results = extract_results(events_inplay_payload)
    for item in results:
        if str(item.get("id")) == str(event_id):
            return item
        if str(item.get("bet365_id")) == str(fi):
            return item
        if str(item.get("our_event_id")) == str(event_id):
            return item
    return None


def safe_float(value: Any) -> Optional[float]:
    """Convert normal decimal strings to float. For fractional odds, use odds_to_decimal."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def fractional_odds_to_decimal(value: Any) -> Optional[float]:
    """Convert Bet365 fractional odds such as 13/8 or 10/11 to decimal odds.

    Decimal odds = 1 + numerator / denominator.
    """
    if value is None or value == "":
        return None

    raw = str(value).strip()
    if "/" not in raw:
        return safe_float(raw)

    try:
        numerator, denominator = raw.split("/", 1)
        denominator_float = float(denominator)
        if denominator_float == 0:
            return None
        return round(1 + (float(numerator) / denominator_float), 3)
    except Exception:
        return None


def extract_market_template_id_from_it(it_value: Any) -> Optional[str]:
    """Extract template id from Bet365 IT values such as ...-30143-H_1_1."""
    if not it_value:
        return None
    match = re.search(r"-(\d+)-H", str(it_value))
    return match.group(1) if match else None


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def extract_current_score_context(match_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    score = match_snapshot.get("score_summary_events") or match_snapshot.get("score_summary_bet365")
    return {
        "score": score,
        "score_summary_events": match_snapshot.get("score_summary_events"),
        "score_summary_bet365": match_snapshot.get("score_summary_bet365"),
    }


def extract_event_odds_records(
    event_odds_payload: Dict[str, Any],
    snapshot_id: str,
    snapshot_time_utc: str,
    event_id: str,
    fi: str,
    score_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    body = event_odds_payload.get("response", {}).get("body", {})
    results = body.get("results", {}) if isinstance(body, dict) else {}
    stats = results.get("stats", {}) if isinstance(results, dict) else {}
    odds_root = results.get("odds", {}) if isinstance(results, dict) else {}
    rows: List[Dict[str, Any]] = []

    if not isinstance(odds_root, dict):
        return rows

    for market_key, odds_list in odds_root.items():
        if not isinstance(odds_list, list):
            continue
        for item in odds_list:
            if not isinstance(item, dict):
                continue
            rows.append({
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time_utc,
                "event_id": event_id,
                "fi": fi,
                "market_key": str(market_key),
                "odds_id": str(item.get("id")) if item.get("id") is not None else None,
                "score_from_match_snapshot": score_context.get("score"),
                "score_from_odds": item.get("ss"),
                "over_from_score": parse_overs_string(item.get("ss")),
                "home_odds": item.get("home_od"),
                "home_odds_decimal": safe_float(item.get("home_od")),
                "away_odds": item.get("away_od"),
                "away_odds_decimal": safe_float(item.get("away_od")),
                "draw_odds": item.get("draw_od"),
                "draw_odds_decimal": safe_float(item.get("draw_od")),
                "handicap": item.get("handicap"),
                "over_odds": item.get("over_od"),
                "over_odds_decimal": safe_float(item.get("over_od")),
                "under_odds": item.get("under_od"),
                "under_odds_decimal": safe_float(item.get("under_od")),
                "add_time": item.get("add_time"),
                "odds_update_stats": stats.get("odds_update"),
                "raw": item,
            })
    return rows



def extract_bet365_current_markets(
    bet365_event_payload: Dict[str, Any],
    snapshot_id: str,
    snapshot_time_utc: str,
    event_id: str,
    fi: str,
    score_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Parse all current Bet365 markets from /v1/bet365/event.

    Bet365 event data is a flat ordered stream:
    - MG = market group, e.g. "Match Winner 2-Way"
    - MA = market/column within the group, e.g. "Over" or "Under"
    - PA = selection/price row. PA rows with OD contain real odds.

    Some markets first publish handicap/line values as PA rows without OD
    (for example "0.5", "3.5", "153.5"). We remember those and attach
    them to later PA rows using the MA/template id.
    """
    records = extract_bet365_records(bet365_event_payload)
    rows: List[Dict[str, Any]] = []

    current_group: Dict[str, Any] = {}
    current_market: Dict[str, Any] = {}

    # Reset for every MG. Used for markets like:
    # PA NA=0.5 IT=...-30143-H, then PA OD with MA=30143.
    line_by_template_id: Dict[str, str] = {}
    line_by_order: Dict[str, str] = {}

    for r in records:
        if not isinstance(r, dict):
            continue

        r_type = r.get("type")

        if r_type == "MG":
            current_group = {
                "market_group_id": clean_text(r.get("ID")),
                "market_group_name": clean_text(r.get("NA")) or clean_text(r.get("IT")) or "Unknown Market",
                "market_group_order": clean_text(r.get("OR")),
                "market_group_suspended": clean_text(r.get("SU")),
                "market_group_raw": r,
            }
            current_market = {}
            line_by_template_id = {}
            line_by_order = {}
            continue

        if r_type == "MA":
            ma_name = clean_text(r.get("NA"))
            current_market = {
                "market_id": clean_text(r.get("ID")),
                "market_name": ma_name,
                "market_it": clean_text(r.get("IT")),
                "market_py": clean_text(r.get("PY")),
                "market_sy": clean_text(r.get("SY")),
                "market_order": clean_text(r.get("OR")),
                "market_raw": r,
            }
            continue

        if r_type != "PA":
            continue

        odds_fractional = clean_text(r.get("OD"))

        # PA without OD often carries line/header values, not a price.
        if not odds_fractional:
            line_value = clean_text(r.get("NA"))
            if line_value:
                template_id = clean_text(r.get("MA")) or extract_market_template_id_from_it(r.get("IT"))
                if template_id:
                    line_by_template_id[template_id] = line_value
                order = clean_text(r.get("OR"))
                if order:
                    line_by_order[order] = line_value
            continue

        selection_order = clean_text(r.get("OR"))
        template_id = clean_text(r.get("MA"))
        line_value = clean_text(r.get("HA")) or clean_text(r.get("HD"))
        if not line_value and template_id:
            line_value = line_by_template_id.get(template_id)
        if not line_value and selection_order:
            line_value = line_by_order.get(selection_order)

        market_group_name = current_group.get("market_group_name") or "Unknown Market"
        market_name = current_market.get("market_name")
        selection_name = clean_text(r.get("NA"))

        # For Over/Under markets the PA often has no NA; the MA name is the selection.
        if not selection_name:
            selection_name = market_name or market_group_name

        display_selection_name = selection_name
        if line_value and selection_name and line_value not in selection_name:
            display_selection_name = f"{selection_name} {line_value}"

        suspended_raw = clean_text(r.get("SU"))
        is_suspended = suspended_raw == "0" or current_group.get("market_group_suspended") == "0"

        rows.append({
            "snapshot_id": snapshot_id,
            "snapshot_time_utc": snapshot_time_utc,
            "event_id": event_id,
            "fi": fi,
            "score_from_match_snapshot": score_context.get("score"),

            "record_type": r_type,

            "market_group_id": current_group.get("market_group_id"),
            "market_group_name": market_group_name,
            "market_group_order": current_group.get("market_group_order"),
            "market_group_suspended": current_group.get("market_group_suspended"),

            "market_id": current_market.get("market_id"),
            "market_name": market_name,
            "market_order": current_market.get("market_order"),
            "market_template_id": template_id,

            "selection_id": clean_text(r.get("ID")),
            "selection_name": selection_name,
            "display_selection_name": display_selection_name,
            "selection_order": selection_order,

            "odds_fractional": odds_fractional,
            "odds_decimal": fractional_odds_to_decimal(odds_fractional),

            # Keep old field name for backward compatibility with the HTML page.
            "odds": odds_fractional,

            "line": line_value,
            "handicap": line_value,
            "suspended": is_suspended,
            "suspended_raw": suspended_raw,

            "raw": r,
        })

    return rows

def parse_silver_snapshot(
    manifest: Dict[str, Any],
    events_inplay_payload: Dict[str, Any],
    bet365_event_payload: Dict[str, Any],
    event_odds_payload: Dict[str, Any],
    event_view_payload: Optional[Dict[str, Any]] = None,
    event_odds_summary_payload: Optional[Dict[str, Any]] = None,
    lineage_payload: Optional[Dict[str, Any]] = None,
    bet365_event_stats_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    snapshot_id = manifest["snapshot_id"]
    snapshot_time_utc = manifest["snapshot_time_utc"]
    event_id = str(manifest["event_id"])
    fi = str(manifest["fi"])
    sport_id = str(manifest.get("sport_id", "3"))

    # Build a name→PG_over map from the stats=1 response (TE records have PG field with over info).
    pg_overs_by_name: Dict[str, str] = {}
    fallback_pg_over: Optional[str] = None

    if bet365_event_stats_payload:
        for r in extract_bet365_records(bet365_event_stats_payload):
            r_type = r.get("type")

            # RV/RG records can carry team/player-specific PG values
            if r_type in ("RV", "RG") and r.get("NA") and r.get("PG"):
                pg_over = parse_over_from_pg(r["PG"])
                if pg_over is not None:
                    pg_overs_by_name[r["NA"]] = pg_over

            # EV record can carry match-level/current PG value
            elif r_type == "EV" and r.get("PG"):
                pg_over = parse_over_from_pg(r["PG"])
                if pg_over is not None:
                    fallback_pg_over = pg_over

    records = extract_bet365_records(bet365_event_payload)
    ev = next((r for r in records if r.get("type") == "EV"), {})
    matching_event = find_matching_event(events_inplay_payload, event_id, fi) or {}

    # /v1/event/view can also contain useful match/team/league details.
    event_view_results = extract_results(event_view_payload or {})
    event_view_item = event_view_results[0] if event_view_results and isinstance(event_view_results[0], dict) else {}

    match_from_filter = manifest.get("match_from_filter", {})
    home = matching_event.get("home") or event_view_item.get("home") or match_from_filter.get("home") or {}
    away = matching_event.get("away") or event_view_item.get("away") or match_from_filter.get("away") or {}
    league = matching_event.get("league") or event_view_item.get("league") or match_from_filter.get("league") or {}

    match_snapshot = {
        "snapshot_id": snapshot_id,
        "snapshot_time_utc": snapshot_time_utc,
        "sport_id": sport_id,
        "event_id": event_id,
        "fi": fi,
        "league_id": str(league.get("id")) if league.get("id") is not None else None,
        "league_name": league.get("name") or ev.get("CT"),
        "home_team_id": str(home.get("id")) if home.get("id") is not None else None,
        "home_team_name": home.get("name"),
        "away_team_id": str(away.get("id")) if away.get("id") is not None else None,
        "away_team_name": away.get("name"),
        "match_name": ev.get("NA"),
        "time_status": matching_event.get("time_status") or match_from_filter.get("time_status"),
        "score_summary_events": matching_event.get("ss") or event_view_item.get("ss") or match_from_filter.get("raw_item", {}).get("ss"),
        "score_summary_bet365": ev.get("SS"),
        "venue": event_view_item.get("venue") or event_view_item.get("ve") or matching_event.get("venue") or None,
        "event_time_unix": matching_event.get("time") or match_from_filter.get("event_time_unix"),
        "event_time_utc": format_unix_ts(matching_event.get("time") or match_from_filter.get("event_time_unix")),
        "bet365_event_success": manifest.get("status", {}).get("bet365_event_success"),
        "events_inplay_success": manifest.get("status", {}).get("events_inplay_success"),
        "event_odds_success": manifest.get("status", {}).get("event_odds_success"),
        "event_view_success": manifest.get("status", {}).get("event_view_success"),
        "event_odds_summary_success": manifest.get("status", {}).get("event_odds_summary_success"),
        "api_lineage": lineage_payload or manifest.get("api_lineage"),
        "source_bronze_manifest_path": f"bronze/betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/manifest.json",
        "source_bronze_lineage_path": f"bronze/betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/lineage.json",
    }

    team_scores: List[Dict[str, Any]] = []
    player_entries: List[Dict[str, Any]] = []

    for r in records:
        r_type = r.get("type")
        if r_type == "TE" and r.get("NA") and (r.get("SC") or r.get("S5")):
            parsed = parse_runs_wickets(r.get("S5"))
            team_scores.append({
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time_utc,
                "event_id": event_id,
                "fi": fi,
                "team_or_player_id": str(r.get("ID")) if r.get("ID") is not None else None,
                "name": r.get("NA"),
                "raw_score_text": r.get("SC"),
                "runs_wickets_raw": r.get("S5"),
                "runs": parsed["runs"],
                "wickets": parsed["wickets"],
                "s1": r.get("S1"),
                "s2": r.get("S2"),
                "s3": r.get("S3"),
                "s4": r.get("S4"),
                "s6": r.get("S6"),
                "s7": r.get("S7"),
                "s8": r.get("S8"),
                "raw": r,
                "pg_over": pg_overs_by_name.get(r.get("NA", "")) or fallback_pg_over,
            })
        elif r_type == "TE" and r.get("NA"):
            player_entries.append({
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time_utc,
                "event_id": event_id,
                "fi": fi,
                "player_id": str(r.get("ID")) if r.get("ID") is not None else None,
                "player_name": r.get("NA"),
                "order": r.get("OR"),
                "raw": r,
            })

    score_context = extract_current_score_context(match_snapshot)
    odds_records = extract_event_odds_records(event_odds_payload, snapshot_id, snapshot_time_utc, event_id, fi, score_context)
    current_markets = extract_bet365_current_markets(
        bet365_event_payload,
        snapshot_id,
        snapshot_time_utc,
        event_id,
        fi,
        score_context,
    )

    return {
        "match_snapshot": match_snapshot,
        "team_scores": team_scores,
        "player_entries": player_entries,
        "odds_records": odds_records,
        "current_markets": current_markets,
    }


def write_silver_outputs(silver_container, parsed: Dict[str, Any]) -> None:
    match = parsed["match_snapshot"]
    dt = datetime.fromisoformat(match["snapshot_time_utc"].replace("Z", "+00:00"))
    event_id = match["event_id"]
    base = (
        f"cricket/inplay/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/"
        f"event_id={event_id}/snapshot_id={match['snapshot_id']}"
    )
    upload_json(silver_container, f"{base}/match_state.json", parsed["match_snapshot"], overwrite=True)
    upload_json(silver_container, f"{base}/team_scores.json", {"rows": parsed["team_scores"]}, overwrite=True)
    upload_json(silver_container, f"{base}/player_entries.json", {"rows": parsed["player_entries"]}, overwrite=True)
    upload_json(silver_container, f"{base}/market_odds.json", {"rows": parsed["odds_records"]}, overwrite=True)
    current_market_rows = parsed.get("current_markets", [])
    upload_json(silver_container, f"{base}/active_markets.json", {"rows": current_market_rows}, overwrite=True)

    # Keep a stable per-event file with the last non-empty market snapshot so the
    # gold page never shows 0 markets just because Bet365 suspended prices momentarily.
    if current_market_rows:
        control_path = f"cricket/inplay/control/event_id={event_id}/last_known_markets.json"
        upload_json(silver_container, control_path, {
            "rows": current_market_rows,
            "snapshot_id": match["snapshot_id"],
            "snapshot_time_utc": match["snapshot_time_utc"],
        }, overwrite=True)

    # --- Write to CricWebsite PostgreSQL (oddsdb) ---
    write_to_cricwebsite_db(match, parsed["team_scores"], current_market_rows)

    # --- Innings tracker: extract data point here (bronze→silver), not in gold ---
    # The raw ingredients (team_scores + active_markets) are already parsed above.
    # We write one innings_snapshot.json per snapshot and maintain a per-event
    # accumulator so the gold builder only needs to read from silver.
    innings_point = extract_innings_snapshot(
        match,
        parsed["team_scores"],
        current_market_rows,
    )
    if innings_point is not None:
        upload_json(silver_container, f"{base}/innings_snapshot.json", innings_point, overwrite=True)
        acc_path = f"cricket/inplay/control/event_id={event_id}/innings_accumulator.json"
        acc = download_json(silver_container, acc_path) or {
            "event_id": event_id,
            "match_name": match.get("match_name"),
            "league_id": match.get("league_id"),
            "league_name": match.get("league_name"),
            "home_team_name": match.get("home_team_name"),
            "away_team_name": match.get("away_team_name"),
            "venue": match.get("venue"),
            "match_date_utc": match.get("event_time_utc"),
            "rows": [],
        }
        rows: List[Dict[str, Any]] = acc.get("rows", [])
        last = rows[-1] if rows else None
        if last is None or last.get("over") != innings_point.get("over") or last.get("score") != innings_point.get("score"):
            rows.append(innings_point)
        elif innings_point.get("predicted_total") is not None and last.get("predicted_total") != innings_point.get("predicted_total"):
            # Same over/score but prediction changed (e.g. market detection improved) — update in place
            rows[-1] = innings_point
        acc["rows"] = rows
        acc["last_updated_utc"] = utc_now().isoformat()
        # Propagate latest match metadata in case earlier snapshots lacked it
        if match.get("match_name") and not acc.get("match_name"):
            acc["match_name"] = match["match_name"]
        if match.get("venue") and not acc.get("venue"):
            acc["venue"] = match["venue"]
        upload_json(silver_container, acc_path, acc, overwrite=True)

    if match.get("api_lineage"):
        upload_json(silver_container, f"{base}/lineage.json", match["api_lineage"], overwrite=True)

    marker_path = f"cricket/control/processed_snapshots/{match['snapshot_id']}_{event_id}_{match['fi']}.json"
    marker = {
        "processed_at_utc": utc_now().isoformat(),
        "snapshot_id": match["snapshot_id"],
        "event_id": event_id,
        "fi": match["fi"],
        "silver_base_path": f"silver/{base}",
        "team_score_count": len(parsed["team_scores"]),
        "player_entry_count": len(parsed["player_entries"]),
        "odds_record_count": len(parsed["odds_records"]),
        "current_market_selection_count": len(current_market_rows),
        "innings_snapshot_written": innings_point is not None,
        "lineage_path": f"silver/{base}/lineage.json" if match.get("api_lineage") else None,
    }
    upload_json(silver_container, marker_path, marker, overwrite=True)


@app.timer_trigger(schedule="*/10 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def parse_cricket_bronze_to_silver(timer: func.TimerRequest) -> None:
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_SILVER_SNAPSHOTS_PER_RUN", 200)
    bronze = get_named_container_client("bronze")
    silver = get_named_container_client("silver")
    manifest_paths = list_manifest_paths(bronze, sport_id, max_per_run)
    processed = skipped = failed = 0

    for manifest_path in manifest_paths:
        try:
            manifest = download_required_json(bronze, manifest_path)
            snapshot_id = manifest["snapshot_id"]
            event_id = str(manifest["event_id"])
            fi = str(manifest["fi"])
            marker_path = f"cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json"
            if blob_exists(silver, marker_path):
                skipped += 1
                continue
            base_path = manifest_path.removesuffix("/manifest.json")
            events_inplay_payload = (
                download_json(bronze, f"{base_path}/api_inplay_event_list.json")
                or download_json(bronze, f"{base_path}/events_inplay_full.json")
                or download_required_json(bronze, f"{base_path}/events_inplay.json")
            )
            bet365_event_payload = (
                download_json(bronze, f"{base_path}/api_live_market_odds.json")
                or download_json(bronze, f"{base_path}/bet365_event_by_fi.json")
                or download_required_json(bronze, f"{base_path}/bet365_event.json")
            )
            bet365_event_stats_payload = download_json(bronze, f"{base_path}/api_live_market_stats.json")
            event_odds_payload = (
                download_json(bronze, f"{base_path}/api_event_odds.json")
                or download_json(bronze, f"{base_path}/event_odds_by_event_id.json")
                or download_required_json(bronze, f"{base_path}/event_odds.json")
            )
            event_view_payload = (
                download_json(bronze, f"{base_path}/api_event_view.json")
                or download_json(bronze, f"{base_path}/event_view_by_event_id.json")
            )
            event_odds_summary_payload = (
                download_json(bronze, f"{base_path}/api_event_odds_summary.json")
                or download_json(bronze, f"{base_path}/event_odds_summary_by_event_id.json")
            )
            lineage_payload = download_json(bronze, f"{base_path}/lineage.json")
            parsed = parse_silver_snapshot(
                manifest,
                events_inplay_payload,
                bet365_event_payload,
                event_odds_payload,
                event_view_payload=event_view_payload,
                event_odds_summary_payload=event_odds_summary_payload,
                lineage_payload=lineage_payload,
                bet365_event_stats_payload=bet365_event_stats_payload,
            )
            write_silver_outputs(silver, parsed)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to parse bronze snapshot to silver")

    logging.info(json.dumps({
        "event": "parse_cricket_bronze_to_silver_completed",
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "checked": len(manifest_paths),
    }))


# -----------------------------
# Gold serving layer
# -----------------------------

def list_latest_silver_match_snapshots(silver_container, limit: int) -> List[str]:
    prefix = "cricket/inplay/"
    snapshots = []
    for blob in silver_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/match_state.json"):
            snapshots.append((getattr(blob, "last_modified", None), name))
    snapshots.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in snapshots[:limit]]


def base_path_from_match_snapshot_path(match_snapshot_path: str) -> str:
    return match_snapshot_path.removesuffix("/match_state.json")


def build_match_page(
    match_snapshot: Dict[str, Any],
    team_scores: Dict[str, Any],
    player_entries: Dict[str, Any],
    odds_records: Dict[str, Any],
    current_markets: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    teams = team_scores.get("rows", []) if isinstance(team_scores, dict) else []
    players = player_entries.get("rows", []) if isinstance(player_entries, dict) else []
    odds = odds_records.get("rows", []) if isinstance(odds_records, dict) else []
    markets = current_markets.get("rows", []) if isinstance(current_markets, dict) else []
    unique_market_keys = {
        m.get("market_group_id") or m.get("market_group_name") or m.get("market_id") or m.get("market_name")
        for m in markets
        if m.get("market_group_id") or m.get("market_group_name") or m.get("market_id") or m.get("market_name")
    }
    return {
        "generated_at_utc": utc_now().isoformat(),
        "snapshot": {
            "snapshot_id": match_snapshot.get("snapshot_id"),
            "snapshot_time_utc": match_snapshot.get("snapshot_time_utc"),
            "event_id": match_snapshot.get("event_id"),
            "fi": match_snapshot.get("fi"),
        },
        "match_header": {
            "league_id": match_snapshot.get("league_id"),
            "league_name": match_snapshot.get("league_name"),
            "match_name": match_snapshot.get("match_name"),
            "home_team": {"id": match_snapshot.get("home_team_id"), "name": match_snapshot.get("home_team_name")},
            "away_team": {"id": match_snapshot.get("away_team_id"), "name": match_snapshot.get("away_team_name")},
            "time_status": match_snapshot.get("time_status"),
            "venue": match_snapshot.get("venue"),
            "event_time_unix": match_snapshot.get("event_time_unix"),
            "event_time_utc": match_snapshot.get("event_time_utc"),
        },
        "score": {
            "summary_from_events": match_snapshot.get("score_summary_events"),
            "summary_from_bet365": match_snapshot.get("score_summary_bet365"),
            "team_scores": teams,
        },
        "players": players,
        "odds": {"count": len(odds), "records": odds},
        "current_markets": {
            "selection_count": len(markets),
            "market_count": len(unique_market_keys),
            "records": markets,
        },
        "source": {
            "silver_match_snapshot_path": match_snapshot.get("source_silver_match_snapshot_path"),
            "silver_lineage_path": match_snapshot.get("source_silver_lineage_path"),
            "bronze_manifest_path": match_snapshot.get("source_bronze_manifest_path"),
            "bronze_lineage_path": match_snapshot.get("source_bronze_lineage_path"),
        },
        "data_lineage": match_snapshot.get("api_lineage"),
    }


def write_gold_match_page(gold_container, match_page: Dict[str, Any]) -> None:
    event_id = str(match_page["snapshot"]["event_id"])
    snapshot_id = str(match_page["snapshot"]["snapshot_id"])
    snapshot_time = match_page["snapshot"].get("snapshot_time_utc")
    dt = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00")) if snapshot_time else utc_now()
    history_base = (
        f"cricket/matches/history/event_id={event_id}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/snapshot_id={snapshot_id}"
    )
    latest_base = f"cricket/matches/latest/event_id={event_id}"
    upload_json(gold_container, f"{history_base}/match_dashboard.json", match_page, overwrite=True)
    upload_json(gold_container, f"{latest_base}/match_dashboard.json", match_page, overwrite=True)
    if match_page.get("data_lineage"):
        upload_json(gold_container, f"{history_base}/lineage.json", match_page["data_lineage"], overwrite=True)
        upload_json(gold_container, f"{latest_base}/lineage.json", match_page["data_lineage"], overwrite=True)


def build_match_index_row(page: Dict[str, Any]) -> Dict[str, Any]:
    header = page.get("match_header", {}) or {}
    snapshot = page.get("snapshot", {}) or {}
    score = page.get("score", {}) or {}
    current_markets = page.get("current_markets", {}) or {}
    event_id = snapshot.get("event_id")
    home_name = (header.get("home_team") or {}).get("name")
    away_name = (header.get("away_team") or {}).get("name")
    match_name = header.get("match_name") or (f"{home_name} vs {away_name}" if home_name and away_name else None)
    return {
        "event_id": event_id,
        "fi": snapshot.get("fi"),
        "snapshot_id": snapshot.get("snapshot_id"),
        "snapshot_time_utc": snapshot.get("snapshot_time_utc"),
        "league_id": header.get("league_id"),
        "league_name": header.get("league_name"),
        "match_name": match_name,
        "home_team_name": home_name,
        "away_team_name": away_name,
        "score_summary": score.get("summary_from_events") or score.get("summary_from_bet365"),
        "odds_count": (page.get("odds") or {}).get("count"),
        "time_status": header.get("time_status"),
        "current_market_count": current_markets.get("market_count"),
        "current_market_selection_count": current_markets.get("selection_count"),
        "latest_gold_path": f"gold/cricket/matches/latest/event_id={event_id}/match_dashboard.json",
    }


def write_gold_index(gold_container, pages: List[Dict[str, Any]]) -> None:
    """Write small precomputed index files used by HTTP pages.

    This merges new pages into the existing latest index, so HTTP routes do not
    scan the full cricket/matches/history folder and should not timeout.
    Entries whose last snapshot is older than LIVE_MATCH_STALE_HOURS are pruned
    so ended matches do not linger in the live index.
    """
    stale_hours = get_int_env("LIVE_MATCH_STALE_HOURS", 4)
    cutoff = utc_now() - timedelta(hours=stale_hours)

    existing_index = download_json(gold_container, "cricket/matches/latest/index.json") or {}
    latest_by_event: Dict[str, Dict[str, Any]] = {}

    if isinstance(existing_index, dict):
        for row in existing_index.get("matches", []):
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            # Drop ended matches immediately
            if str(row.get("time_status") or "") == "3":
                continue
            snapshot_time = row.get("snapshot_time_utc")
            if snapshot_time:
                try:
                    dt = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00"))
                    if dt < cutoff:
                        continue  # prune: stale / no longer live
                except Exception:
                    pass
            latest_by_event[event_id] = row

    for page in pages:
        row = build_match_index_row(page)
        event_id = str(row.get("event_id") or "")
        if not event_id:
            continue
        # Never add ended matches to the live index
        if str(row.get("time_status") or "") == "3":
            latest_by_event.pop(event_id, None)
            continue
        # Also drop stale snapshots from new pages (same cutoff as the existing-index loop)
        snapshot_time = row.get("snapshot_time_utc")
        if snapshot_time:
            try:
                dt = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00"))
                if dt < cutoff:
                    latest_by_event.pop(event_id, None)
                    continue
            except Exception:
                pass
        current = latest_by_event.get(event_id)
        if current is None or (row.get("snapshot_time_utc") or "") >= (current.get("snapshot_time_utc") or ""):
            latest_by_event[event_id] = row

    matches = list(latest_by_event.values())
    matches.sort(key=lambda x: x.get("snapshot_time_utc") or "", reverse=True)

    upload_json(
        gold_container,
        "cricket/matches/latest/index.json",
        {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches), "matches": matches},
        overwrite=True,
    )
    write_gold_league_indexes(gold_container, matches)


def write_gold_league_indexes(gold_container, matches: List[Dict[str, Any]]) -> None:
    leagues: Dict[str, Dict[str, Any]] = {}

    for match in matches:
        league_id = str(match.get("league_id") or "unknown")
        league_name = match.get("league_name") or "Unknown League"
        if league_id not in leagues:
            leagues[league_id] = {
                "league_id": league_id,
                "league_name": league_name,
                "match_count": 0,
                "latest_snapshot_time_utc": None,
                "matches": [],
            }
        leagues[league_id]["matches"].append(match)
        leagues[league_id]["match_count"] += 1
        snapshot_time = match.get("snapshot_time_utc") or ""
        if not leagues[league_id]["latest_snapshot_time_utc"] or snapshot_time > leagues[league_id]["latest_snapshot_time_utc"]:
            leagues[league_id]["latest_snapshot_time_utc"] = snapshot_time

    league_rows = []
    for league in leagues.values():
        league["matches"].sort(key=lambda x: x.get("snapshot_time_utc") or "", reverse=True)
        upload_json(
            gold_container,
            f"cricket/leagues/{league['league_id']}/matches.json",
            {
                "generated_at_utc": utc_now().isoformat(),
                "league_id": league["league_id"],
                "league_name": league["league_name"],
                "match_count": league["match_count"],
                "matches": league["matches"],
            },
            overwrite=True,
        )
        league_rows.append({
            "league_id": league["league_id"],
            "league_name": league["league_name"],
            "match_count": league["match_count"],
            "latest_snapshot_time_utc": league["latest_snapshot_time_utc"],
            "matches_path": f"gold/cricket/leagues/{league['league_id']}/matches.json",
        })

    league_rows.sort(key=lambda x: x.get("latest_snapshot_time_utc") or "", reverse=True)
    upload_json(
        gold_container,
        "cricket/leagues/index.json",
        {"generated_at_utc": utc_now().isoformat(), "league_count": len(league_rows), "leagues": league_rows},
        overwrite=True,
    )


@app.timer_trigger(schedule="*/10 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def build_cricket_gold_match_pages(timer: func.TimerRequest) -> None:
    max_events = get_int_env("MAX_GOLD_EVENTS_PER_RUN", 100)
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")

    # Fetch a larger pool (newest-first) then deduplicate to the single newest
    # snapshot per event. Without deduplication the loop overwrites gold for
    # each snapshot of the same event and ends up writing the *oldest* one.
    all_paths = list_latest_silver_match_snapshots(silver, max_events * 20)
    seen_events: Dict[str, str] = {}
    for path in all_paths:
        m = re.search(r"/event_id=([^/]+)/", path)
        if m:
            eid = m.group(1)
            if eid not in seen_events:
                seen_events[eid] = path  # list is newest-first; first hit is newest
    match_snapshot_paths = list(seen_events.values())[:max_events]

    built_pages = []
    processed = failed = 0

    for match_snapshot_path in match_snapshot_paths:
        try:
            base_path = base_path_from_match_snapshot_path(match_snapshot_path)
            match_snapshot = download_required_json(silver, f"{base_path}/match_state.json")
            match_snapshot["source_silver_match_snapshot_path"] = f"silver/{base_path}/match_state.json"
            match_snapshot["source_silver_lineage_path"] = f"silver/{base_path}/lineage.json"
            if not match_snapshot.get("api_lineage"):
                lineage_from_silver = download_json(silver, f"{base_path}/lineage.json")
                if lineage_from_silver:
                    match_snapshot["api_lineage"] = lineage_from_silver
            team_scores = download_required_json(silver, f"{base_path}/team_scores.json")
            player_entries = download_required_json(silver, f"{base_path}/player_entries.json")
            odds_records = download_required_json(silver, f"{base_path}/market_odds.json")
            try:
                current_markets = download_required_json(silver, f"{base_path}/active_markets.json")
            except ResourceNotFoundError:
                current_markets = {"rows": []}
            # If this snapshot has no markets (Bet365 suspended/ended), use the last
            # known non-empty markets for this event so the page doesn't go blank.
            if not current_markets.get("rows"):
                event_id_for_control = match_snapshot.get("event_id")
                control_path = f"cricket/inplay/control/event_id={event_id_for_control}/last_known_markets.json"
                fallback = download_json(silver, control_path)
                if fallback and fallback.get("rows"):
                    current_markets = fallback
            match_page = build_match_page(match_snapshot, team_scores, player_entries, odds_records, current_markets)
            write_gold_match_page(gold, match_page)
            try:
                write_gold_innings_tracker_from_silver(gold, silver, match_page)
            except Exception:
                logging.exception("Failed to write gold innings tracker")
            built_pages.append(match_page)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to build gold match page")

    if built_pages:
        write_gold_index(gold, built_pages)

    logging.info(json.dumps({
        "event": "build_cricket_gold_match_pages_completed",
        "processed": processed,
        "failed": failed,
        "checked": len(match_snapshot_paths),
    }))



# -----------------------------
# Innings tracker
# -----------------------------

_OVERS_RE = re.compile(r'(?<!\d)(\d+)\.([0-5])(?!\d)')


def parse_over_from_pg(pg: str) -> Optional[str]:
    """Extract overs from Bet365 PG field (from stats=1 response).

    Format: 'deliveries_in_over#over_1indexed:balls_in_over:wickets'
    e.g. '1:1:1:1:4:6#15:5:4' → '14.5'  (15-1=14 completed overs, 5 balls)
         '1:4:6:0:1wd:1#16:1:0' → '15.1' (15 completed overs, 1 ball)
    """
    if not pg:
        return None
    try:
        parts = pg.split("#")
        if len(parts) < 2:
            return None
        stats = parts[-1].split(":")
        if len(stats) < 2:
            return None
        completed_overs = int(stats[0]) - 1
        balls = int(stats[1]) -1 
        if completed_overs < 0:
            return None
        if balls > 0:
            return f"{completed_overs}.{balls}"
        return str(completed_overs)
    except (ValueError, IndexError):
        return None


def parse_overs_string(text: Optional[str]) -> Optional[str]:
    """Return overs like '17.3' from text like '185/5 (17.3 ov)'. Returns None if not found."""
    if not text:
        return None
    m = _OVERS_RE.search(str(text))
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return None

def extract_innings_snapshot(
    match_snapshot: Dict[str, Any],
    team_score_rows: List[Dict[str, Any]],
    current_market_rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build one innings tracker data point from the current match state.

    Returns None if there is not enough data (no over or score).
    """
    home_team = str(match_snapshot.get("home_team_name") or "")
    away_team = str(match_snapshot.get("away_team_name") or "")

    # --- Extract current over, score, wickets ---
    # The batting team's TE record usually has overs in raw_score_text (SC field).
    current_over: Optional[str] = None
    current_score: Optional[int] = None
    current_wickets: Optional[int] = None
    batting_team: Optional[str] = None

    for row in team_score_rows:
        # Priority 1: PG field from stats=1 call — most accurate over source.
        over_str = row.get("pg_over")

        # Priority 2: SC field (raw score text like "185/5 (17.3 ov)")
        if not over_str:
            raw_text = str(row.get("raw_score_text") or "")
            over_str = parse_overs_string(raw_text)

        # Priority 3: S3 field which carries overs as "17.3" or plain integer "17" / "0"
        s3 = str(row.get("s3") or "").strip()
        if not over_str and s3:
            over_str = parse_overs_string(s3)
            if not over_str and re.match(r'^\d+\.[0-5]$', s3):
                over_str = s3
            # Plain integer like "0" or "17" — treat as completed overs (no balls)
            if not over_str and re.match(r'^\d+$', s3):
                over_str = f"{s3}.0"

        if over_str:
            current_over = over_str
            batting_team = str(row.get("name") or "")
            current_score = row.get("runs")
            current_wickets = row.get("wickets")
            break

    # Fallback: parse score from summary string
    if current_score is None:
        ss = str(match_snapshot.get("score_summary_events") or match_snapshot.get("score_summary_bet365") or "")
        m_ss = re.match(r'^(\d+)[/#](\d+)', ss)
        if m_ss:
            current_score = int(m_ss.group(1))
            current_wickets = int(m_ss.group(2))

    if current_score is None:
        return None

    # Fallback: try to get overs from Bet365 SS field
    if not current_over:
        ss_b365 = str(match_snapshot.get("score_summary_bet365") or "")
        current_over = parse_overs_string(ss_b365)

    bowling_team = (away_team if batting_team == home_team else home_team) if batting_team else away_team

    # --- Find predicted total from innings total runs market ---
    # _is_innings_market is defined at module level and shared with the DB writer.

    # Include suspended rows — the batting team's total market is routinely suspended
    # while in-play, but its line (e.g. 276.5) is still the valid predicted total.
    innings_rows = [
        r for r in current_market_rows
        if r.get("market_group_name") and _is_innings_market(r["market_group_name"])
    ]

    # Parse line from display_selection_name to avoid the sub-market overwrite problem.
    # "Over 276.5"  → direction=over,  line=276.5   ✓
    # "Over 290 276.5" → no match (two numbers)      ✗  (different sub-market)
    # "Under 270 276.5" → no match                   ✗
    _SIMPLE_OVER_RE = re.compile(r'^[Oo]ver\s+([\d.]+)$')
    _SIMPLE_UNDER_RE = re.compile(r'^[Uu]nder\s+([\d.]+)$')

    by_line: Dict[str, Dict[str, Optional[float]]] = {}
    for r in innings_rows:
        sel_name = str(r.get("display_selection_name") or r.get("selection_name") or "").strip()
        m_ov = _SIMPLE_OVER_RE.match(sel_name)
        m_un = _SIMPLE_UNDER_RE.match(sel_name)
        if m_ov:
            line_key = m_ov.group(1)
            by_line.setdefault(line_key, {})["over"] = r.get("odds_decimal")
        elif m_un:
            line_key = m_un.group(1)
            by_line.setdefault(line_key, {})["under"] = r.get("odds_decimal")

    predicted_total: Optional[float] = None
    over_odds_at_line: Optional[float] = None
    under_odds_at_line: Optional[float] = None
    best_diff = float("inf")
    best_line_val = -1.0
    for line_str, ln_odds in by_line.items():
        ov = ln_odds.get("over")
        un = ln_odds.get("under")
        if ov is None or un is None:
            continue
        diff = abs(ov - un)
        try:
            line_val = float(line_str)
        except Exception:
            continue
        # Prefer the line with smallest odds diff (closest to fair).
        # On a tie, prefer the HIGHER line — the full innings total is always
        # higher than any sub-segment market (e.g. "5 Overs Runs" line ~52.5
        # vs "50 Overs Runs" line ~276.5).
        if diff < best_diff or (diff == best_diff and line_val > best_line_val):
            best_diff = diff
            best_line_val = line_val
            predicted_total = line_val
            over_odds_at_line = ov
            under_odds_at_line = un

    # --- Find "Full Time" / "Match Betting" team odds ---
    winner_keywords = ["full time", "match betting", "match winner", "to win the match", "win match"]
    winner_rows = [
        r for r in current_market_rows
        if r.get("market_group_name") and any(
            kw in r["market_group_name"].lower() for kw in winner_keywords
        ) and r.get("odds_decimal") and not r.get("suspended")
    ]

    home_odds: Optional[float] = None
    away_odds: Optional[float] = None
    for r in winner_rows:
        sel = str(r.get("selection_name") or "").strip().lower()
        od = r.get("odds_decimal")
        if home_team and home_team.lower() in sel:
            home_odds = od
        elif away_team and away_team.lower() in sel:
            away_odds = od
    if home_odds is None and len(winner_rows) >= 1:
        home_odds = winner_rows[0].get("odds_decimal")
    if away_odds is None and len(winner_rows) >= 2:
        away_odds = winner_rows[1].get("odds_decimal")

    batting_odds = (home_odds if batting_team == home_team else away_odds) if batting_team else home_odds
    bowling_odds = (away_odds if batting_team == home_team else home_odds) if batting_team else away_odds

    innings_market_name = innings_rows[0].get("market_group_name") if innings_rows else None
    return {
        "over": current_over,
        "over_float": current_over,
        "score": current_score,
        "wickets": current_wickets,
        "batting_team": batting_team or home_team,
        "bowling_team": bowling_team or away_team,
        "batting_team_odds": batting_odds,
        "bowling_team_odds": bowling_odds,
        "home_team_odds": home_odds,
        "away_team_odds": away_odds,
        "predicted_total": predicted_total,
        "over_odds_at_line": over_odds_at_line,
        "under_odds_at_line": under_odds_at_line,
        "innings_market_name": innings_market_name,
        "snapshot_id": match_snapshot.get("snapshot_id"),
        "snapshot_time_utc": match_snapshot.get("snapshot_time_utc"),
    }


def write_gold_innings_tracker_from_silver(
    gold_container,
    silver_container,
    match_page: Dict[str, Any],
) -> None:
    """Build the gold innings tracker by reading from the silver accumulator.

    All extraction logic lives in write_silver_outputs (bronze→silver step).
    This function only reads the silver accumulator and promotes it to gold,
    adding outcome detection if the match has ended.
    """
    snapshot = match_page.get("snapshot") or {}
    header = match_page.get("match_header") or {}
    event_id = str(snapshot.get("event_id") or "")
    if not event_id:
        return

    acc_path = f"cricket/inplay/control/event_id={event_id}/innings_accumulator.json"
    acc = download_json(silver_container, acc_path) or {}
    rows: List[Dict[str, Any]] = acc.get("rows", [])

    # Detect outcome if the match has ended (time_status == "3")
    time_status = str(header.get("time_status") or "")
    score = match_page.get("score") or {}
    score_summary = score.get("summary_from_events") or score.get("summary_from_bet365") or ""
    outcome: Optional[str] = acc.get("outcome")
    actual_total: Optional[int] = acc.get("actual_total")
    if time_status == "3" and outcome is None and rows:
        m_ss = re.match(r'^(\d+)', str(score_summary))
        if m_ss:
            actual_total = int(m_ss.group(1))
            last_predicted = rows[-1].get("predicted_total")
            if last_predicted is not None:
                if actual_total > last_predicted:
                    outcome = "over"
                elif actual_total < last_predicted:
                    outcome = "under"
                else:
                    outcome = "push"

    match_name = acc.get("match_name") or header.get("match_name")
    venue = acc.get("venue") or header.get("venue")
    tracker = {
        "event_id": event_id,
        "match_name": match_name,
        "venue": venue,
        "match_date_utc": acc.get("match_date_utc") or header.get("event_time_utc"),
        "league_id": acc.get("league_id") or header.get("league_id"),
        "league_name": acc.get("league_name") or header.get("league_name"),
        "home_team_name": acc.get("home_team_name") or (header.get("home_team") or {}).get("name"),
        "away_team_name": acc.get("away_team_name") or (header.get("away_team") or {}).get("name"),
        "rows": rows,
        "outcome": outcome,
        "actual_total": actual_total,
        "last_updated_utc": utc_now().isoformat(),
    }

    tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
    upload_json(gold_container, tracker_path, tracker, overwrite=True)

    index_path = "cricket/innings_tracker/index.json"
    idx = download_json(gold_container, index_path) or {}
    matches_idx: Dict[str, Dict] = {str(m.get("event_id") or ""): m for m in idx.get("matches", [])}
    matches_idx[event_id] = {
        "event_id": event_id,
        "match_name": match_name,
        "venue": venue,
        "match_date_utc": tracker["match_date_utc"],
        "league_id": tracker["league_id"],
        "league_name": tracker["league_name"],
        "home_team_name": tracker["home_team_name"],
        "away_team_name": tracker["away_team_name"],
        "row_count": len(rows),
        "has_outcome": outcome is not None,
        "outcome": outcome,
        "actual_total": actual_total,
        "tracker_path": f"gold/{tracker_path}",
    }
    upload_json(
        gold_container,
        index_path,
        {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches_idx), "matches": list(matches_idx.values())},
        overwrite=True,
    )


def update_innings_tracker(
    gold_container,
    match_page: Dict[str, Any],
    team_score_rows: List[Dict[str, Any]],
    current_market_rows: List[Dict[str, Any]],
) -> None:
    """Append a tracker row when the over or score changed. Also resolve outcome when match ends."""
    snapshot = match_page.get("snapshot") or {}
    header = match_page.get("match_header") or {}
    event_id = str(snapshot.get("event_id") or "")
    if not event_id:
        return

    tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
    existing = download_json(gold_container, tracker_path) or {}
    rows: List[Dict[str, Any]] = existing.get("rows", [])

    time_status = str(header.get("time_status") or "")
    score = match_page.get("score") or {}
    score_summary = score.get("summary_from_events") or score.get("summary_from_bet365") or ""

    # Try to resolve outcome when match ends (time_status == "3").
    outcome = existing.get("outcome")
    actual_total = existing.get("actual_total")
    if time_status == "3" and outcome is None and rows:
        m_ss = re.match(r'^(\d+)', str(score_summary))
        if m_ss:
            actual_total = int(m_ss.group(1))
            last_predicted = rows[-1].get("predicted_total")
            if last_predicted is not None and actual_total is not None:
                if actual_total > last_predicted:
                    outcome = "over"
                elif actual_total < last_predicted:
                    outcome = "under"
                else:
                    outcome = "push"

    # Only append new data rows during live play (not yet ended).
    if time_status != "3":
        ms_for_tracker = {
            "snapshot_id": snapshot.get("snapshot_id"),
            "snapshot_time_utc": snapshot.get("snapshot_time_utc"),
            "event_id": event_id,
            "home_team_name": (header.get("home_team") or {}).get("name"),
            "away_team_name": (header.get("away_team") or {}).get("name"),
            "score_summary_events": score.get("summary_from_events"),
            "score_summary_bet365": score.get("summary_from_bet365"),
            "venue": header.get("venue"),
        }
        data_point = extract_innings_snapshot(ms_for_tracker, team_score_rows, current_market_rows)
        if data_point is not None:
            last = rows[-1] if rows else None
            if last is None or last.get("over") != data_point.get("over") or last.get("score") != data_point.get("score"):
                rows.append(data_point)

    tracker = {
        "event_id": event_id,
        "match_name": header.get("match_name"),
        "venue": header.get("venue"),
        "match_date_utc": header.get("event_time_utc"),
        "league_id": header.get("league_id"),
        "league_name": header.get("league_name"),
        "home_team_name": (header.get("home_team") or {}).get("name"),
        "away_team_name": (header.get("away_team") or {}).get("name"),
        "rows": rows,
        "outcome": outcome,
        "actual_total": actual_total,
        "last_updated_utc": utc_now().isoformat(),
    }
    upload_json(gold_container, tracker_path, tracker, overwrite=True)

    # Update the cross-match index.
    index_path = "cricket/innings_tracker/index.json"
    idx = download_json(gold_container, index_path) or {}
    matches_idx: Dict[str, Dict] = {str(m.get("event_id") or ""): m for m in idx.get("matches", [])}
    matches_idx[event_id] = {
        "event_id": event_id,
        "match_name": tracker["match_name"],
        "venue": tracker["venue"],
        "match_date_utc": tracker["match_date_utc"],
        "league_id": tracker["league_id"],
        "league_name": tracker["league_name"],
        "home_team_name": tracker["home_team_name"],
        "away_team_name": tracker["away_team_name"],
        "row_count": len(rows),
        "has_outcome": outcome is not None,
        "outcome": outcome,
        "actual_total": actual_total,
        "tracker_path": f"gold/{tracker_path}",
    }
    upload_json(
        gold_container,
        index_path,
        {
            "generated_at_utc": utc_now().isoformat(),
            "match_count": len(matches_idx),
            "matches": list(matches_idx.values()),
        },
        overwrite=True,
    )


# -----------------------------
# Prematch / upcoming ingestion
# -----------------------------

def summarize_event_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a BetsAPI event row into our standard small match record."""
    event_id = str(item.get("id")) if item.get("id") is not None else None
    fi = str(item.get("bet365_id")) if item.get("bet365_id") is not None else None
    league = item.get("league") or {}
    home = item.get("home") or {}
    away = item.get("away") or {}

    return {
        "event_id": event_id,
        "fi": fi,
        "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
        "event_time_unix": item.get("time"),
        "event_time_utc": format_unix_ts(item.get("time")),
        "time_status": item.get("time_status"),
        "league": league,
        "league_id": str(league.get("id")) if league.get("id") is not None else None,
        "league_name": league.get("name"),
        "home": home,
        "home_team_id": str(home.get("id")) if home.get("id") is not None else None,
        "home_team_name": home.get("name"),
        "away": away,
        "away_team_id": str(away.get("id")) if away.get("id") is not None else None,
        "away_team_name": away.get("name"),
        "match_name": f"{home.get('name', '')} vs {away.get('name', '')}".strip(" vs "),
        "score": item.get("ss"),
        "raw_item": item,
    }


def summarize_event_items(items: List[Dict[str, Any]], max_events: int, require_bet365_id: bool = False) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        match = summarize_event_item(item)
        if not match.get("event_id"):
            continue
        if require_bet365_id and not match.get("fi"):
            continue
        matches.append(match)

    if max_events <= 0:
        return matches
    return matches[:max_events]


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def discover_cricket_upcoming(timer: func.TimerRequest) -> None:
    """Find upcoming cricket matches and store a small control file.

    This calls /v3/events/upcoming. Only rows with bet365_id can be used for
    /v4/bet365/prematch odds, but we store all rows in bronze for traceability.
    """
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_upcoming = get_int_env("MAX_UPCOMING_MATCHES", 100)
    only_bet365 = get_bool_env("UPCOMING_REQUIRE_BET365_ID", True)
    container = get_bronze_container_client()

    api_payload = call_betsapi(path="/v3/events/upcoming", params={"sport_id": sport_id})

    raw_path = (
        f"betsapi/upcoming/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"events_upcoming_{ts_compact(now)}.json"
    )
    upload_json(container, raw_path, api_payload)

    upcoming_matches = summarize_event_items(extract_results(api_payload), max_upcoming, require_bet365_id=only_bet365)
    control_payload = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "max_upcoming_matches": max_upcoming,
        "require_bet365_id": only_bet365,
        "upcoming_match_count": len(upcoming_matches),
        "upcoming_matches": upcoming_matches,
        "source_raw_path": f"bronze/{raw_path}",
    }
    upload_json(container, "betsapi/control/upcoming_cricket/latest.json", control_payload, overwrite=True)

    logging.info(json.dumps({
        "event": "discover_cricket_upcoming_completed",
        "success": api_payload["response"]["success"],
        "upcoming_match_count": len(upcoming_matches),
        "raw_path": f"bronze/{raw_path}",
    }))


@app.timer_trigger(schedule="10 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_prematch_odds(timer: func.TimerRequest) -> None:
    """Capture prematch odds for upcoming matches using /v4/bet365/prematch."""
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_PREMATCH_ODDS_PER_RUN", 100)
    container = get_bronze_container_client()

    control = download_json(container, "betsapi/control/upcoming_cricket/latest.json")
    if not control or not control.get("upcoming_matches"):
        logging.info(json.dumps({"event": "capture_cricket_prematch_odds_skipped", "reason": "no_upcoming_matches"}))
        return

    excluded_leagues = load_excluded_league_ids()
    upcoming_matches = [
        m for m in control.get("upcoming_matches", [])
        if m.get("fi") and str(m.get("league_id") or "") not in excluded_leagues
    ]
    if max_per_run > 0:
        upcoming_matches = upcoming_matches[:max_per_run]

    processed = failed = skipped = 0
    for match in upcoming_matches:
        try:
            snapshot_time = utc_now()
            snapshot_id = ts_compact(snapshot_time)
            event_id = str(match.get("event_id"))
            fi = str(match.get("fi"))
            if not event_id or not fi:
                skipped += 1
                continue

            prematch_payload = call_betsapi(path="/v4/bet365/prematch", params={"FI": fi})

            base_path = f"betsapi/prematch_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}"
            manifest = {
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time.isoformat(),
                "sport_id": sport_id,
                "event_id": event_id,
                "fi": fi,
                "match_from_upcoming": match,
                "files": {
                    "api_prematch_odds": f"bronze/{base_path}/api_prematch_odds.json",
                },
                "status": {
                    "prematch_success": prematch_payload["response"]["success"],
                    "prematch_error": prematch_payload["response"].get("error"),
                    "prematch_error_detail": prematch_payload["response"].get("error_detail"),
                },
            }

            upload_json(container, f"{base_path}/api_prematch_odds.json", prematch_payload)
            upload_json(container, f"{base_path}/manifest.json", manifest)
            processed += 1

        except Exception:
            failed += 1
            logging.exception("Failed to capture prematch odds")

    logging.info(json.dumps({
        "event": "capture_cricket_prematch_odds_completed",
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "checked": len(upcoming_matches),
    }))


@app.timer_trigger(schedule="30 */5 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def discover_cricket_ended(timer: func.TimerRequest) -> None:
    """Capture recently ended cricket matches for result lookup."""
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_ended = get_int_env("MAX_ENDED_MATCHES", 100)
    bronze = get_bronze_container_client()
    gold = get_named_container_client("gold")

    api_payload = call_betsapi(path="/v3/events/ended", params={"sport_id": sport_id})

    raw_path = (
        f"betsapi/ended/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"events_ended_{ts_compact(now)}.json"
    )
    upload_json(bronze, raw_path, api_payload)

    ended_matches = summarize_event_items(extract_results(api_payload), max_ended, require_bet365_id=False)

    # Enrich FI from the live index and the previous ended index — /v3/events/ended never returns bet365_id.
    fi_lookup: Dict[str, str] = {}
    for index_path in ("cricket/matches/latest/index.json", "cricket/ended/latest/index.json"):
        idx = download_json(gold, index_path) or {}
        for row in idx.get("matches", []):
            eid = str(row.get("event_id") or "")
            fi_val = row.get("fi")
            if eid and fi_val:
                fi_lookup[eid] = str(fi_val)

    for m in ended_matches:
        if not m.get("fi"):
            eid = str(m.get("event_id") or "")
            if eid in fi_lookup:
                m["fi"] = fi_lookup[eid]

    ended_index = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "ended_match_count": len(ended_matches),
        "matches": ended_matches,
        "source_raw_path": f"bronze/{raw_path}",
    }
    upload_json(gold, "cricket/ended/latest/index.json", ended_index, overwrite=True)

    logging.info(json.dumps({
        "event": "discover_cricket_ended_completed",
        "success": api_payload["response"]["success"],
        "ended_match_count": len(ended_matches),
        "raw_path": f"bronze/{raw_path}",
    }))


# -----------------------------
# Prematch parsing and gold pages
# -----------------------------

def list_prematch_manifest_paths(bronze_container, sport_id: str, limit: int) -> List[str]:
    prefix = f"betsapi/prematch_snapshot/sport_id={sport_id}/"
    manifests = []
    for blob in bronze_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/manifest.json"):
            manifests.append((getattr(blob, "last_modified", None), name))
    manifests.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in manifests[:limit]]


def extract_prematch_result(prematch_payload: Dict[str, Any]) -> Dict[str, Any]:
    body = prematch_payload.get("response", {}).get("body", {})
    results = body.get("results", []) if isinstance(body, dict) else []
    if isinstance(results, list) and results and isinstance(results[0], dict):
        return results[0]
    if isinstance(results, dict):
        return results
    return {}


def parse_prematch_markets(
    prematch_payload: Dict[str, Any],
    snapshot_id: str,
    snapshot_time_utc: str,
    event_id: str,
    fi: str,
) -> List[Dict[str, Any]]:
    """Flatten /v4/bet365/prematch into one row per odds selection.

    Prematch response shape:
    results[0] -> category, for example "1st_over"
      category["sp"] -> market key, for example "1st_over_total_runs"
        market["odds"] -> list of odds selections
    """
    root = extract_prematch_result(prematch_payload)
    rows: List[Dict[str, Any]] = []

    for category_key, category_value in root.items():
        if not isinstance(category_value, dict):
            continue

        sp = category_value.get("sp")
        if not isinstance(sp, dict):
            continue

        category_updated_at = category_value.get("updated_at")
        for market_key, market_value in sp.items():
            if not isinstance(market_value, dict):
                continue

            odds_list = market_value.get("odds", [])
            if not isinstance(odds_list, list):
                continue

            for odds_item in odds_list:
                if not isinstance(odds_item, dict):
                    continue

                odds_value = odds_item.get("odds")
                rows.append({
                    "snapshot_id": snapshot_id,
                    "snapshot_time_utc": snapshot_time_utc,
                    "event_id": event_id,
                    "fi": fi,

                    "category_key": str(category_key),
                    "category_updated_at": category_updated_at,
                    "category_updated_at_utc": format_unix_ts(category_updated_at),

                    "market_key": str(market_key),
                    "market_id": str(market_value.get("id")) if market_value.get("id") is not None else None,
                    "market_name": market_value.get("name"),

                    "selection_id": str(odds_item.get("id")) if odds_item.get("id") is not None else None,
                    "selection_name": odds_item.get("name"),
                    "selection_header": odds_item.get("header"),
                    "handicap": odds_item.get("handicap"),

                    "odds": odds_value,
                    "odds_decimal": safe_float(odds_value),

                    "raw": odds_item,
                })

    return rows


def build_prematch_snapshot(manifest: Dict[str, Any], prematch_payload: Dict[str, Any]) -> Dict[str, Any]:
    event_id = str(manifest.get("event_id"))
    fi = str(manifest.get("fi"))
    match = manifest.get("match_from_upcoming", {}) or {}
    league = match.get("league") or {}
    home = match.get("home") or {}
    away = match.get("away") or {}
    markets = parse_prematch_markets(
        prematch_payload,
        manifest["snapshot_id"],
        manifest["snapshot_time_utc"],
        event_id,
        fi,
    )

    market_keys = {
        f"{row.get('category_key')}::{row.get('market_key')}"
        for row in markets
        if row.get("category_key") or row.get("market_key")
    }

    return {
        "generated_at_utc": utc_now().isoformat(),
        "snapshot": {
            "snapshot_id": manifest.get("snapshot_id"),
            "snapshot_time_utc": manifest.get("snapshot_time_utc"),
            "event_id": event_id,
            "fi": fi,
        },
        "match_header": {
            "league_id": str(league.get("id")) if league.get("id") is not None else match.get("league_id"),
            "league_name": league.get("name") or match.get("league_name"),
            "match_name": match.get("match_name"),
            "home_team": {
                "id": str(home.get("id")) if home.get("id") is not None else match.get("home_team_id"),
                "name": home.get("name") or match.get("home_team_name"),
            },
            "away_team": {
                "id": str(away.get("id")) if away.get("id") is not None else match.get("away_team_id"),
                "name": away.get("name") or match.get("away_team_name"),
            },
            "event_time_unix": match.get("event_time_unix"),
            "event_time_utc": match.get("event_time_utc"),
            "time_status": match.get("time_status"),
        },
        "prematch_markets": {
            "selection_count": len(markets),
            "market_count": len(market_keys),
            "records": markets,
        },
        "source": {
            "bronze_manifest_path": (
                f"bronze/betsapi/prematch_snapshot/sport_id={manifest.get('sport_id', '3')}/"
                f"event_id={event_id}/fi={fi}/snapshot_id={manifest.get('snapshot_id')}/manifest.json"
            )
        },
    }


def write_prematch_gold_indexes(gold_container, pages: List[Dict[str, Any]]) -> None:
    existing_index = download_json(gold_container, "cricket/prematch/latest/index.json") or {}
    latest_by_event: Dict[str, Dict[str, Any]] = {}

    if isinstance(existing_index, dict):
        for row in existing_index.get("matches", []):
            event_id = str(row.get("event_id") or "")
            if event_id:
                latest_by_event[event_id] = row

    for page in pages:
        snapshot = page.get("snapshot", {}) or {}
        header = page.get("match_header", {}) or {}
        markets = page.get("prematch_markets", {}) or {}
        event_id = str(snapshot.get("event_id") or "")
        if not event_id:
            continue

        home_name = (header.get("home_team") or {}).get("name")
        away_name = (header.get("away_team") or {}).get("name")
        match_name = header.get("match_name") or (f"{home_name} vs {away_name}" if home_name and away_name else None)
        row = {
            "event_id": event_id,
            "fi": snapshot.get("fi"),
            "snapshot_id": snapshot.get("snapshot_id"),
            "snapshot_time_utc": snapshot.get("snapshot_time_utc"),
            "league_id": header.get("league_id"),
            "league_name": header.get("league_name"),
            "match_name": match_name,
            "home_team_name": home_name,
            "away_team_name": away_name,
            "event_time_unix": header.get("event_time_unix"),
            "event_time_utc": header.get("event_time_utc"),
            "prematch_market_count": markets.get("market_count"),
            "prematch_selection_count": markets.get("selection_count"),
            "latest_gold_path": f"gold/cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json",
        }

        current = latest_by_event.get(event_id)
        if current is None or (row.get("snapshot_time_utc") or "") >= (current.get("snapshot_time_utc") or ""):
            latest_by_event[event_id] = row

    matches = list(latest_by_event.values())
    matches.sort(key=lambda x: x.get("event_time_unix") or "", reverse=False)

    upload_json(
        gold_container,
        "cricket/prematch/latest/index.json",
        {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches), "matches": matches},
        overwrite=True,
    )

    leagues: Dict[str, Dict[str, Any]] = {}
    for match in matches:
        league_id = str(match.get("league_id") or "unknown")
        league_name = match.get("league_name") or "Unknown League"
        if league_id not in leagues:
            leagues[league_id] = {"league_id": league_id, "league_name": league_name, "match_count": 0, "matches": []}
        leagues[league_id]["matches"].append(match)
        leagues[league_id]["match_count"] += 1

    league_rows = []
    for league in leagues.values():
        league["matches"].sort(key=lambda x: x.get("event_time_unix") or "", reverse=False)
        upload_json(
            gold_container,
            f"cricket/prematch/leagues/{league['league_id']}/matches.json",
            {
                "generated_at_utc": utc_now().isoformat(),
                "league_id": league["league_id"],
                "league_name": league["league_name"],
                "match_count": league["match_count"],
                "matches": league["matches"],
            },
            overwrite=True,
        )
        league_rows.append({
            "league_id": league["league_id"],
            "league_name": league["league_name"],
            "match_count": league["match_count"],
            "matches_path": f"gold/cricket/prematch/leagues/{league['league_id']}/matches.json",
        })

    league_rows.sort(key=lambda x: x.get("league_name") or "")
    upload_json(
        gold_container,
        "cricket/prematch/leagues/index.json",
        {"generated_at_utc": utc_now().isoformat(), "league_count": len(league_rows), "leagues": league_rows},
        overwrite=True,
    )


@app.timer_trigger(schedule="30 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def build_cricket_prematch_pages(timer: func.TimerRequest) -> None:
    """Build fast website/API pages from captured prematch odds."""
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_PREMATCH_SNAPSHOTS_PER_RUN", 200)
    bronze = get_named_container_client("bronze")
    gold = get_named_container_client("gold")

    manifest_paths = list_prematch_manifest_paths(bronze, sport_id, max_per_run)
    built_pages: List[Dict[str, Any]] = []
    processed = failed = 0

    for manifest_path in manifest_paths:
        try:
            manifest = download_required_json(bronze, manifest_path)
            base_path = manifest_path.removesuffix("/manifest.json")
            prematch_payload = (
                download_json(bronze, f"{base_path}/api_prematch_odds.json")
                or download_required_json(bronze, f"{base_path}/prematch.json")
            )
            page = build_prematch_snapshot(manifest, prematch_payload)

            event_id = page["snapshot"]["event_id"]
            snapshot_id = page["snapshot"]["snapshot_id"]
            upload_json(gold, f"cricket/prematch/history/event_id={event_id}/snapshot_id={snapshot_id}/prematch_dashboard.json", page, overwrite=True)
            upload_json(gold, f"cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json", page, overwrite=True)
            built_pages.append(page)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to build prematch page")

    if built_pages:
        write_prematch_gold_indexes(gold, built_pages)

    logging.info(json.dumps({
        "event": "build_cricket_prematch_pages_completed",
        "processed": processed,
        "failed": failed,
        "checked": len(manifest_paths),
    }))


def build_simple_table_page(title: str, headers: List[str], rows_html: str, back_link: Optional[str] = None) -> str:
    back_html = f'<p><a href="{escape(back_link)}">← Back</a></p>' if back_link else ""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{escape(title)}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
            h1 {{ margin-bottom: 8px; }}
            .hint {{ color: #666; margin-bottom: 20px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
            th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; vertical-align: top; }}
            th {{ background: #222; color: white; position: sticky; top: 0; }}
            a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
            .pill {{ display: inline-block; padding: 3px 8px; background: #eee; border-radius: 999px; }}
        </style>
    </head>
    <body>
        {back_html}
        <h1>{escape(title)}</h1>
        <div class="hint">This page reads a small pre-built gold index file, so it should load quickly.</div>
        <table>
            <thead><tr>{''.join(f'<th>{escape(h)}</th>' for h in headers)}</tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </body>
    </html>
    """


# -----------------------------
# Prematch and ended HTTP routes
# -----------------------------

@app.route(route="prematch", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/prematch/latest/index.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return prematch index")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="prematch/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_matches_html(req: func.HttpRequest) -> func.HttpResponse:
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


@app.route(route="prematch/{event_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_match_json(req: func.HttpRequest) -> func.HttpResponse:
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


@app.route(route="prematch/{event_id}/results", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def save_prematch_results(req: func.HttpRequest) -> func.HttpResponse:
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


@app.route(route="prematch/{event_id}/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_match_html(req: func.HttpRequest) -> func.HttpResponse:
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

            # Find Over/Under pairs: group by (category_key, selection_name, handicap)
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

            rows = ""
            for idx, m in enumerate(selections[:1000]):
                cat = str(m.get("category_key") or "-")
                sn_raw = str(m.get("selection_name") or "").strip()
                sh_raw = str(m.get("selection_header") or "").strip()
                hcap = str(m.get("handicap") or "-")

                if idx in paired_indices:
                    if sh_raw == "Under":
                        continue  # rendered together with its Over row
                    # sh_raw == "Over": render the combined pair row
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
                    rows += f"""
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
                    # hide team_player column if it duplicates the option display
                    if team_player == option_display or team_player == option_raw:
                        team_player = "-"
                    key = sel_key(market_name, cat, option_raw, hcap)
                    saved_val = saved_results.get(key, "pending")
                    key_esc = escape(key)
                    rows += f"""
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
                    <tbody>{rows}</tbody>
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
                function onSelChange(el) {{
                    el.className = 'result-sel result-sel-' + el.value;
                }}
                function onPairChange(el) {{
                    el.className = 'result-sel result-sel-' + el.value;
                }}
                async function saveResults() {{
                    const status = document.getElementById('save-status');
                    status.textContent = 'Saving...';
                    status.style.color = '#aaa';
                    const results = {{}};
                    document.querySelectorAll('.result-sel[data-key]').forEach(s => {{
                        results[s.dataset.key] = s.value;
                    }});
                    document.querySelectorAll('.result-sel[data-type="pair"]').forEach(s => {{
                        const v = s.value;
                        if (v === 'over_wins') {{
                            results[s.dataset.overKey] = 'pass';
                            results[s.dataset.underKey] = 'fail';
                        }} else if (v === 'under_wins') {{
                            results[s.dataset.overKey] = 'fail';
                            results[s.dataset.underKey] = 'pass';
                        }} else {{
                            results[s.dataset.overKey] = 'pending';
                            results[s.dataset.underKey] = 'pending';
                        }}
                    }});
                    try {{
                        const resp = await fetch('/api/prematch/{event_id}/results', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{results}})
                        }});
                        const data = await resp.json();
                        if (data.ok) {{
                            status.textContent = 'Saved ' + data.saved + ' results ✓';
                            status.style.color = '#4caf50';
                        }} else {{
                            status.textContent = 'Save failed: ' + data.error;
                            status.style.color = '#f44336';
                        }}
                    }} catch (e) {{
                        status.textContent = 'Error: ' + e.message;
                        status.style.color = '#f44336';
                    }}
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render prematch match page")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="prematch/leagues", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_leagues(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/prematch/leagues/index.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return prematch leagues")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="prematch/leagues/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_leagues_html(req: func.HttpRequest) -> func.HttpResponse:
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


@app.route(route="prematch/leagues/{league_id}/matches", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_league_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        league_id = req.route_params.get("league_id")
        gold = get_named_container_client("gold")
        data = download_required_json(gold, f"cricket/prematch/leagues/{league_id}/matches.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return prematch league matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="prematch/leagues/{league_id}/matches/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_prematch_league_matches_html(req: func.HttpRequest) -> func.HttpResponse:
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


@app.route(route="ended", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ended_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/ended/latest/index.json")
        return func.HttpResponse(json.dumps(data, indent=2, ensure_ascii=False), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to return ended index")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="ended/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_ended_matches_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        index = download_required_json(gold, "cricket/ended/latest/index.json")
        matches = index.get("matches", []) if isinstance(index, dict) else []
        matches_sorted = sorted(matches, key=lambda m: m.get("event_time_utc") or m.get("event_time_unix") or "", reverse=True)
        league_names = sorted({str(m.get("league_name") or "Unknown") for m in matches})
        league_options = '<option value="ALL">All Leagues</option>'
        for ln in league_names:
            league_options += f'<option value="{escape(ln)}">{escape(ln)}</option>'
        rows = ""
        for m in matches_sorted:
            league_name = str(m.get("league_name") or "Unknown")
            event_id = escape(str(m.get("event_id") or "-"))
            league_esc = escape(league_name)
            rows += f"""
            <tr data-league="{league_esc}">
                <td>{event_id}</td>
                <td>{escape(str(m.get("fi") or "-"))}</td>
                <td>{escape(str(m.get("match_name") or "-"))}</td>
                <td>{league_esc}</td>
                <td>{escape(str(m.get("score") or "-"))}</td>
                <td>{escape(str(m.get("event_time_utc") or "-"))}</td>
                <td><a href="/api/matches/{event_id}/view">Open</a></td>
                <td><a href="/api/prematch/{event_id}/view">Open</a></td>
                <td><a href="/api/matches/{event_id}/innings-tracker/view">Tracker</a></td>
            </tr>
            """
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Ended Cricket Matches</title>
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
            <h1>Ended Cricket Matches ({index.get('ended_match_count', len(matches))})</h1>
            <p class="hint">This page reads a small pre-built gold index file, so it should load quickly.</p>
            <div class="filter-bar">
                <label><b>League:</b></label>
                <select id="leagueFilter" onchange="filterLeague()">{league_options}</select>
            </div>
            <table>
                <thead><tr><th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>League</th><th>Final Score</th><th>Start Time</th><th>Live Odds</th><th>Prematch Odds</th><th>Innings Tracker</th></tr></thead>
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
        logging.exception("Failed to render ended matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="matches", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_latest_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/matches/latest/index.json")
        return func.HttpResponse(json.dumps(data), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get latest matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="matches/{event_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_page(req: func.HttpRequest) -> func.HttpResponse:
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
@app.route(route="matches/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_matches_list_html(req: func.HttpRequest) -> func.HttpResponse:
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
                <td>{event_id}</td>
                <td>{fi}</td>
                <td>{match_name}</td>
                <td><a href="/api/leagues/{league_id}/matches/view">{league_esc}</a></td>
                <td>{score}</td>
                <td>{markets}</td>
                <td>{selections}</td>
                <td>{snapshot_time}</td>
                <td><a href="/api/matches/{event_id}/view">Open</a></td>
                <td><a href="/api/matches/{event_id}/innings-tracker/view">Tracker</a></td>
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
                tr.league-header td {{ background: #e8f0fe; color: #1a3a6b; font-weight: bold; font-size: 13px; padding: 7px 10px; }}
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
                    <tr>
                        <th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>League</th><th>Score</th>
                        <th>Markets</th><th>Selections</th><th>Last Snapshot</th><th>Page</th><th>Innings Tracker</th>
                    </tr>
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
        return func.HttpResponse("Index not created yet. Wait for build_cricket_gold_match_pages to run.", status_code=404)
    except Exception as ex:
        logging.exception("Failed to render matches list")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="leagues", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_leagues(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/leagues/index.json")
        return func.HttpResponse(json.dumps(data, ensure_ascii=False, indent=2), status_code=200, mimetype="application/json")
    except ResourceNotFoundError:
        return func.HttpResponse(json.dumps({"league_count": 0, "leagues": []}), status_code=404, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get leagues")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="leagues/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_leagues_html(req: func.HttpRequest) -> func.HttpResponse:
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
            <tr><td>{league_id}</td><td>{league_name}</td><td>{match_count}</td><td>{latest}</td><td><a href="/api/leagues/{league_id}/matches/view">Open matches</a></td></tr>
            """
        html = f"""
        <!DOCTYPE html><html><head><title>Cricket Leagues</title>
        <style>body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }} table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }} th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }} th {{ background: #222; color: white; }} a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}</style>
        </head><body><h1>Cricket Leagues ({len(leagues)})</h1><p><a href="/api/matches/view">All matches</a></p>
        <table><thead><tr><th>League ID</th><th>League</th><th>Matches</th><th>Latest Snapshot</th><th>Page</th></tr></thead><tbody>{rows}</tbody></table></body></html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except ResourceNotFoundError:
        return func.HttpResponse("League index not created yet. Wait for build_cricket_gold_match_pages to run.", status_code=404)
    except Exception as ex:
        logging.exception("Failed to render leagues")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="leagues/{league_id}/matches", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_league_matches(req: func.HttpRequest) -> func.HttpResponse:
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


@app.route(route="leagues/{league_id}/matches/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_league_matches_html(req: func.HttpRequest) -> func.HttpResponse:
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
            <tr><td>{event_id}</td><td>{fi}</td><td>{match_name}</td><td>{score}</td><td>{markets}</td><td>{selections}</td><td>{snapshot_time}</td><td><a href="/api/matches/{event_id}/view">Open</a></td></tr>
            """
        html = f"""
        <!DOCTYPE html><html><head><title>{league_name} Matches</title>
        <style>body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }} table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }} th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }} th {{ background: #222; color: white; }} a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}</style>
        </head><body><h1>{league_name} ({len(matches)} matches)</h1><p><a href="/api/leagues/view">Back to leagues</a> | <a href="/api/matches/view">All matches</a></p>
        <table><thead><tr><th>Event ID</th><th>Bet365 FI</th><th>Match</th><th>Score</th><th>Markets</th><th>Selections</th><th>Last Snapshot</th><th>Page</th></tr></thead><tbody>{rows}</tbody></table></body></html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except ResourceNotFoundError:
        return func.HttpResponse("League matches index not found yet.", status_code=404)
    except Exception as ex:
        logging.exception("Failed to render league matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="matches/{event_id}/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_page_html(req: func.HttpRequest) -> func.HttpResponse:
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
<p>This match may have ended before the pipeline captured a full live snapshot,
or the data was not processed in time.</p>
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

        # Always fetch live markets directly from Bet365 so the page never shows
        # stale-zero markets from gold cache (e.g. after stats=1 was removed).
        snapshot = data.get("snapshot") or {}
        fi = snapshot.get("fi") or (data.get("current_markets") or {}).get("fi")
        if not fi:
            # Try reading fi from match_header fallback path in gold
            fi = header.get("fi")

        current_market_rows: List[Dict[str, Any]] = []
        markets_source = "cached"
        if fi:
            try:
                live_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi})
                if live_payload["response"]["success"]:
                    now = utc_now().isoformat()
                    current_market_rows = extract_bet365_current_markets(
                        live_payload, "live", now, event_id, fi, {}
                    )
                    markets_source = "live"
            except Exception:
                pass  # fall through to cached below

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

        # Match Winner 2-Way timeline: rows that have home + away odds (not over/under)
        mw_rows = sorted(
            [o for o in odds if o.get("home_odds") is not None and o.get("away_odds") is not None and o.get("over_odds") is None],
            key=lambda o: o.get("add_time") or 0,
            reverse=True,
        )
        home_name = escape(str((header.get("home_team") or {}).get("name") or "Home"))
        away_name = escape(str((header.get("away_team") or {}).get("name") or "Away"))
        odds_rows = ""
        for o in mw_rows[:500]:
            odds_rows += f"""
            <tr>
                <td>{escape(str(o.get('over_from_score') or '-'))}</td>
                <td>{escape(str(o.get('score_from_odds') or o.get('score_from_match_snapshot') or '-'))}</td>
                <td>{escape(str(o.get('home_odds') or '-'))}</td>
                <td>{escape(str(o.get('away_odds') or '-'))}</td>
                <td>{escape(str(format_unix_ts(o.get('add_time')) or '-'))}</td>
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
                <a href="/api/matches/{event_id}/innings-tracker/view">Innings Tracker</a>
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
                <h2>Match Winner 2-Way Timeline <span style="font-size:13px;font-weight:normal;color:#888;">({len(mw_rows)} data points from /v2/event/odds)</span></h2>
                <table>
                    <thead><tr><th>Over</th><th>Score</th><th>{home_name}</th><th>{away_name}</th><th>Time</th></tr></thead>
                    <tbody>{odds_rows}</tbody>
                </table>
            </div>

            <script>
                function filterCurrentMarket() {{
                    const selected = document.getElementById("currentMarketFilter").value;
                    const rows = document.querySelectorAll("tr[data-current-market]");
                    rows.forEach(row => {{
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
    
@app.route(route="matches/{event_id}/lineage", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_lineage_json(req: func.HttpRequest) -> func.HttpResponse:
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


@app.route(route="matches/{event_id}/lineage/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_lineage_html(req: func.HttpRequest) -> func.HttpResponse:
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
                    <tr>
                        <th>API</th><th>Purpose</th><th>ID Used</th><th>Success</th><th>HTTP</th>
                        <th>Result Count</th><th>Elapsed ms</th><th>Bronze Raw File</th>
                    </tr>
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


@app.route(route="matches/{event_id}/innings-tracker/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_innings_tracker_html(req: func.HttpRequest) -> func.HttpResponse:
    """Show the 1st innings progression tracker for one match."""
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        tracker = download_json(gold, f"cricket/innings_tracker/event_id={event_id}/innings_1.json") or {}

        match_name = escape(str(tracker.get("match_name") or f"Match {event_id}"))
        venue = escape(str(tracker.get("venue") or ""))
        match_date = escape(str(tracker.get("match_date_utc") or "")[:10])
        league = escape(str(tracker.get("league_name") or ""))
        home_team = str(tracker.get("home_team_name") or "Home")
        away_team = str(tracker.get("away_team_name") or "Away")
        rows_data: List[Dict[str, Any]] = tracker.get("rows", [])
        outcome = tracker.get("outcome")
        actual_total = tracker.get("actual_total")

        # Only rows where innings market had data, sorted oldest→newest
        timeline_rows = sorted(
            [r for r in rows_data if r.get("predicted_total") is not None],
            key=lambda r: str(r.get("snapshot_time_utc") or ""),
        )
        innings_market = (
            next((r.get("innings_market_name") for r in reversed(rows_data) if r.get("innings_market_name")), None)
        )
        is_live = (actual_total is None and outcome is None)

        # Summary cards
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

        # Simple timeline: one row per (over, score) where market had data
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
    <p><a href="/api/matches/{escape(str(event_id))}/view">← Back to match</a> | <a href="/api/innings-tracker">All matches analytics</a></p>
    <h1>Innings Tracker — {match_name}</h1>
    {f'<div class="meta">📍 {venue}</div>' if venue else ""}
    {f'<div class="meta">📅 {match_date} &nbsp;|&nbsp; {league}</div>' if match_date else ""}
    <div class="meta">{escape(home_team)} vs {escape(away_team)}</div>
    {f'<div class="meta" style="color:#888;font-size:12px;">📊 Market: {escape(innings_market)}</div>' if innings_market else ""}
    {summary_html}
    {no_data_msg}
    <table>
        <thead>
            <tr><th>Time (UTC)</th><th>Over</th><th>Score/Wkts</th><th>Predicted Total</th><th>Over / Under Odds</th><th>{escape(home_team)} (Win)</th><th>{escape(away_team)} (Win)</th><th>Outcome</th></tr>
        </thead>
        <tbody>{table_rows}</tbody>
    </table>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render innings tracker")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="innings-tracker", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_innings_tracker_analytics(req: func.HttpRequest) -> func.HttpResponse:
    """Cross-match innings tracker analytics with filters.

    Query params (all optional):
      league=IPL          exact league name
      team=GT             batting OR bowling team name (partial, case-insensitive)
      venue=Wankhede      venue name (partial, case-insensitive)
      min_over=0          filter rows where over_float >= min_over
      max_over=20         filter rows where over_float <= max_over
      min_wickets=0       filter rows where wickets >= min_wickets
      max_wickets=10      filter rows where wickets <= max_wickets
      min_bat_odds=1.0    filter rows where batting_team_odds >= value
      max_bat_odds=9.9    filter rows where batting_team_odds <= value
      min_over_pct=0      only show combinations with over% >= value (for bias report)
    """
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
        f_min_over_pct = safe_float(params.get("min_over_pct") or "")
        try:
            f_min_wkts_int = int(f_min_wkts) if f_min_wkts is not None else None
        except Exception:
            f_min_wkts_int = None
        try:
            f_max_wkts_int = int(f_max_wkts) if f_max_wkts is not None else None
        except Exception:
            f_max_wkts_int = None

        idx = download_json(gold, "cricket/innings_tracker/index.json") or {}
        all_matches = idx.get("matches", [])

        # Collect all qualifying rows across all matches
        result_rows: List[Dict[str, Any]] = []
        over_total = under_total = no_outcome = 0

        for meta in all_matches:
            if f_league and meta.get("league_name") != f_league:
                continue
            if f_venue and f_venue not in str(meta.get("venue") or "").lower():
                continue

            event_id_m = str(meta.get("event_id") or "")
            actual_total = meta.get("actual_total")
            tracker = download_json(gold, f"cricket/innings_tracker/event_id={event_id_m}/innings_1.json") or {}

            for r in tracker.get("rows", []):
                # Team filter
                if f_team:
                    batting = str(r.get("batting_team") or "").lower()
                    bowling = str(r.get("bowling_team") or "").lower()
                    if f_team not in batting and f_team not in bowling:
                        continue

                # Over range filter
                over_fl = r.get("over_float")
                if f_min_over is not None and (over_fl is None or over_fl < f_min_over):
                    continue
                if f_max_over is not None and (over_fl is None or over_fl > f_max_over):
                    continue

                # Wickets filter
                wkts = r.get("wickets")
                if f_min_wkts_int is not None and (wkts is None or wkts < f_min_wkts_int):
                    continue
                if f_max_wkts_int is not None and (wkts is None or wkts > f_max_wkts_int):
                    continue

                # Batting odds filter
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

                result_rows.append({
                    **r,
                    "event_id": event_id_m,
                    "match_name": meta.get("match_name"),
                    "league_name": meta.get("league_name"),
                    "venue": meta.get("venue"),
                    "actual_total": actual_total,
                    "row_outcome": row_outcome,
                })

        total_decided = over_total + under_total
        over_pct = round(100 * over_total / total_decided, 1) if total_decided else None
        under_pct = round(100 * under_total / total_decided, 1) if total_decided else None

        # Build filter form
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
            <tr>
                <th>Match</th><th>League</th><th>Venue</th>
                <th>Over</th><th>Score/Wkts</th>
                <th>Batting</th><th>Bowling</th>
                <th>Bat Odds</th><th>Bowl Odds</th>
                <th>Predicted</th><th>Actual</th><th>Outcome</th>
            </tr>
        </thead>
        <tbody>{table_rows_html}</tbody>
    </table>
</body>
</html>"""
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render innings tracker analytics")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="matches/{event_id}/markets/live", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_live_markets(req: func.HttpRequest) -> func.HttpResponse:
    """Fetch current Bet365 markets directly from the API (bypasses pipeline cache)."""
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        page = (
            download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
            or download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
        )
        fi = (page.get("snapshot") or {}).get("fi")
        if not fi:
            return func.HttpResponse("FI not found for this event in gold data.", status_code=404)

        payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi})
        if not payload["response"]["success"]:
            return func.HttpResponse(
                json.dumps({"error": "BetsAPI call failed", "detail": payload["response"]}, indent=2),
                status_code=502, mimetype="application/json",
            )

        now = utc_now().isoformat()
        markets = extract_bet365_current_markets(payload, "live", now, event_id, fi, {})

        # Debug: count raw records so we can tell if 0 markets is an API vs parse issue
        raw_body = (payload.get("response") or {}).get("body") or {}
        raw_results = raw_body.get("results", [])
        if raw_results and isinstance(raw_results[0], list):
            flat_records = raw_results[0]
        elif isinstance(raw_results, list):
            flat_records = raw_results
        else:
            flat_records = []
        type_counts: Dict[str, int] = {}
        for _r in flat_records:
            if isinstance(_r, dict):
                _t = _r.get("type", "?")
                type_counts[_t] = type_counts.get(_t, 0) + 1
        debug_summary = ", ".join(f"{k}={v}" for k, v in sorted(type_counts.items())) or "no records"

        match_name = (page.get("match_header") or {}).get("match_name") or f"Event {event_id}"
        header = page.get("match_header", {}) or {}

        market_names = sorted({m.get("market_group_name") or "Unknown" for m in markets})
        market_options = '<option value="ALL">All Markets</option>'
        for mn in market_names:
            market_options += f'<option value="{escape(mn)}">{escape(mn)}</option>'

        rows = ""
        for m in markets:
            group = escape(str(m.get("market_group_name") or "-"))
            market_nm = escape(str(m.get("market_name") or group))
            selection = escape(str(m.get("display_selection_name") or m.get("selection_name") or "-"))
            odds_dec = m.get("odds_decimal")
            odds_frac = escape(str(m.get("odds_fractional") or "-"))
            odds_display = f"{odds_dec} ({odds_frac})" if odds_dec is not None else odds_frac
            line = escape(str(m.get("line") or "-"))
            suspended = "Yes" if m.get("suspended") else "No"
            rows += f"""
            <tr data-group="{group}">
                <td>{group}</td>
                <td>{market_nm}</td>
                <td>{selection}</td>
                <td>{escape(odds_display)}</td>
                <td>{line}</td>
                <td>{suspended}</td>
            </tr>
            """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Live Markets – {escape(match_name)}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                .card {{ background: white; padding: 18px; border-radius: 10px; box-shadow: 0 2px 8px #ddd; margin-bottom: 20px; }}
                .filter-bar {{ margin-bottom: 16px; display: flex; align-items: center; gap: 12px; }}
                select {{ padding: 8px 12px; font-size: 14px; border: 1px solid #ccc; border-radius: 6px; }}
                table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; font-size: 14px; }}
                th {{ background: #222; color: white; position: sticky; top: 0; }}
                a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
                .live-badge {{ display: inline-block; background: #d00; color: white; font-size: 13px; font-weight: bold; padding: 3px 10px; border-radius: 999px; vertical-align: middle; margin-left: 10px; }}
            </style>
        </head>
        <body>
            <p><a href="/api/matches/{escape(str(event_id))}/view">← Back to match page</a></p>
            <div class="card">
                <h1>{escape(match_name)} <span class="live-badge">LIVE</span></h1>
                <p><b>League:</b> {escape(str(header.get('league_name') or '-'))}</p>
                <p><b>Event ID:</b> {escape(str(event_id))} &nbsp; <b>Bet365 FI:</b> {escape(str(fi))}</p>
                <p><b>Markets fetched at:</b> {escape(now)}</p>
                <p><b>Total selections:</b> {len(markets)} across {len(market_names)} market groups</p>
                <p><b>Raw API records:</b> {escape(debug_summary)}</p>
            </div>
            <div class="filter-bar">
                <label><b>Market group:</b></label>
                <select id="marketFilter" onchange="filterMarket()">{market_options}</select>
            </div>
            <table>
                <thead>
                    <tr><th>Group</th><th>Market</th><th>Selection</th><th>Odds Decimal (Fractional)</th><th>Line</th><th>Suspended</th></tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            <script>
                function filterMarket() {{
                    const sel = document.getElementById("marketFilter").value;
                    document.querySelectorAll("tr[data-group]").forEach(r => {{
                        r.style.display = sel === "ALL" || r.dataset.group === sel ? "" : "none";
                    }});
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render live markets")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="matches/{event_id}/markets/raw", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_markets_raw(req: func.HttpRequest) -> func.HttpResponse:
    """Return the raw BetsAPI /v1/bet365/event JSON so you can inspect the actual response structure."""
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        page = (
            download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
            or download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
        )
        fi = (page.get("snapshot") or {}).get("fi")
        if not fi:
            return func.HttpResponse(
                json.dumps({"error": "FI not found in gold data for this event"}),
                status_code=404, mimetype="application/json",
            )

        payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi})
        body = (payload.get("response") or {}).get("body") or {}
        results = body.get("results", [])

        # Summarise the record types so the caller can see what arrived
        if results and isinstance(results[0], list):
            flat_records = results[0]
        elif isinstance(results, list):
            flat_records = results
        else:
            flat_records = []

        type_counts: Dict[str, int] = {}
        for r in flat_records:
            if isinstance(r, dict):
                t = r.get("type", "UNKNOWN")
                type_counts[t] = type_counts.get(t, 0) + 1

        debug = {
            "event_id": event_id,
            "fi": fi,
            "api_success": payload["response"]["success"],
            "http_status": payload["response"]["http_status_code"],
            "total_records": len(flat_records),
            "record_type_counts": type_counts,
            "raw_body": body,
        }
        return func.HttpResponse(json.dumps(debug, indent=2, default=str), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to fetch raw markets")
        return func.HttpResponse(json.dumps({"error": str(ex)}), status_code=500, mimetype="application/json")


@app.route(route="run-prematch-now", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def run_prematch_now(req: func.HttpRequest) -> func.HttpResponse:
    try:
        discover_cricket_upcoming(None)
        capture_cricket_prematch_odds(None)
        build_cricket_prematch_pages(None)
        return func.HttpResponse("Prematch pipeline executed successfully", status_code=200)
    except Exception as ex:
        logging.exception("Failed to run prematch pipeline manually")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)

@app.route(route="admin/innings/{event_id}/rebuild", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def admin_rebuild_innings_accumulator(req: func.HttpRequest) -> func.HttpResponse:
    """Rebuild the innings accumulator for one event by replaying all silver snapshots.

    This is useful when the accumulator has stale/wrong rows written by an older
    version of extract_innings_snapshot. It scans every silver snapshot folder for
    the event, re-runs the current extraction logic on each one, deduplicates by
    (over, score), and writes a fresh silver accumulator + gold tracker file.
    """
    event_id = req.route_params.get("event_id", "").strip()
    if not event_id:
        return func.HttpResponse("event_id required", status_code=400)

    try:
        silver = get_named_container_client("silver")
        gold = get_named_container_client("gold")

        # Find all silver match_state.json blobs for this event.
        needle = f"/event_id={event_id}/"
        snapshot_paths: List[str] = []
        for blob in silver.list_blobs(name_starts_with="cricket/inplay/"):
            if needle in blob.name and blob.name.endswith("/match_state.json"):
                snapshot_paths.append(blob.name)

        if not snapshot_paths:
            return func.HttpResponse(
                json.dumps({"event_id": event_id, "message": "no silver snapshots found", "rows_written": 0}),
                mimetype="application/json",
                status_code=200,
            )

        snapshot_paths.sort()

        raw_points: List[Dict[str, Any]] = []
        errors = 0
        for ms_path in snapshot_paths:
            base = ms_path.removesuffix("/match_state.json")
            try:
                match_state = download_json(silver, ms_path)
                team_scores_doc = download_json(silver, f"{base}/team_scores.json") or {}
                active_markets_doc = download_json(silver, f"{base}/active_markets.json") or {}
                if not match_state:
                    continue
                team_score_rows = team_scores_doc.get("rows", [])
                active_market_rows = active_markets_doc.get("rows", [])
                point = extract_innings_snapshot(match_state, team_score_rows, active_market_rows)
                if point is not None:
                    raw_points.append(point)
            except Exception:
                errors += 1
                continue

        # Sort by snapshot_time_utc and dedup: keep one row per (over, score),
        # using the LAST occurrence so we get the most recent prediction.
        raw_points.sort(key=lambda p: str(p.get("snapshot_time_utc") or ""))
        seen: Dict[tuple, Dict[str, Any]] = {}
        for p in raw_points:
            key = (p.get("over"), p.get("score"))
            seen[key] = p  # overwrite → keep last value per (over, score)
        deduped = sorted(seen.values(), key=lambda p: str(p.get("snapshot_time_utc") or ""))

        # Rebuild silver accumulator.
        acc_path = f"cricket/inplay/control/event_id={event_id}/innings_accumulator.json"
        old_acc = download_json(silver, acc_path) or {}
        new_acc = {
            "event_id": event_id,
            "match_name": old_acc.get("match_name"),
            "league_id": old_acc.get("league_id"),
            "league_name": old_acc.get("league_name"),
            "home_team_name": old_acc.get("home_team_name"),
            "away_team_name": old_acc.get("away_team_name"),
            "venue": old_acc.get("venue"),
            "match_date_utc": old_acc.get("match_date_utc"),
            "outcome": old_acc.get("outcome"),
            "actual_total": old_acc.get("actual_total"),
            "rows": deduped,
            "last_updated_utc": utc_now().isoformat(),
            "rebuilt_from_snapshot_count": len(snapshot_paths),
        }
        # Fill metadata from the first data point if accumulator was empty.
        if deduped and not new_acc.get("match_name"):
            p0 = deduped[0]
            new_acc["home_team_name"] = new_acc["home_team_name"] or p0.get("batting_team")
            new_acc["away_team_name"] = new_acc["away_team_name"] or p0.get("bowling_team")

        upload_json(silver, acc_path, new_acc, overwrite=True)

        # Also refresh the gold tracker so the page reflects the rebuilt data immediately.
        gold_tracker_path = f"cricket/innings_tracker/event_id={event_id}/innings_1.json"
        existing_gold = download_json(gold, gold_tracker_path) or {}
        tracker = {**existing_gold, "rows": deduped, "last_updated_utc": utc_now().isoformat()}
        upload_json(gold, gold_tracker_path, tracker, overwrite=True)

        return func.HttpResponse(
            json.dumps({
                "event_id": event_id,
                "silver_snapshots_scanned": len(snapshot_paths),
                "raw_points_extracted": len(raw_points),
                "rows_written": len(deduped),
                "errors": errors,
                "message": "accumulator rebuilt — reload the innings tracker page",
            }, indent=2),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as ex:
        logging.exception("admin_rebuild_innings_accumulator failed")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="admin/leagues/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_admin_leagues_view(req: func.HttpRequest) -> func.HttpResponse:
    """Admin page to tick/untick leagues for pipeline inclusion."""
    try:
        leagues = collect_known_leagues()
        excluded = load_excluded_league_ids()

        rows_html = ""
        for lg in leagues:
            lid = escape(str(lg.get("league_id") or ""))
            lname = escape(str(lg.get("league_name") or lid))
            sources = ", ".join(lg.get("sources", []))
            is_excluded = str(lg.get("league_id") or "") in excluded
            checked = "" if is_excluded else "checked"
            bg = "" if is_excluded else ' style="background:#f0fff0"'
            rows_html += f"""
            <tr{bg}>
                <td>{lid}</td>
                <td><b>{lname}</b></td>
                <td style="color:#666;font-size:12px;">{escape(sources)}</td>
                <td>
                    <label class="toggle">
                        <input type="checkbox" {checked}
                               onchange="toggle(this, '{lid}')"
                               data-league-id="{lid}" data-league-name="{lname}">
                        <span class="slider"></span>
                    </label>
                </td>
                <td id="status-{lid}" style="font-size:12px;color:#888;">
                    {'Excluded — not captured' if is_excluded else 'Included'}
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
        /* Toggle switch */
        .toggle {{ position: relative; display: inline-block; width: 48px; height: 26px; }}
        .toggle input {{ opacity: 0; width: 0; height: 0; }}
        .slider {{ position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0;
                   background: #ccc; border-radius: 26px; transition: .3s; }}
        .slider:before {{ position: absolute; content: ""; height: 20px; width: 20px; left: 3px;
                          bottom: 3px; background: white; border-radius: 50%; transition: .3s; }}
        input:checked + .slider {{ background: #28a745; }}
        input:checked + .slider:before {{ transform: translateX(22px); }}
    </style>
</head>
<body>
    <p><a href="/api/home">← Home</a></p>
    <h1>League Filter</h1>
    <p class="hint">Toggle ON = captured in bronze/silver/gold. Toggle OFF = skipped entirely (saves API quota).<br>
    Changes apply from the next capture cycle (within 5 seconds for live, 1 minute for prematch).</p>
    <p id="save-status" style="color:#28a745;font-weight:bold;display:none;">Saved.</p>
    <table>
        <thead><tr><th>League ID</th><th>League Name</th><th>Seen In</th><th>Capture</th><th>Status</th></tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    <script>
        async function toggle(checkbox, leagueId) {{
            const include = checkbox.checked;
            const statusEl = document.getElementById('status-' + leagueId);
            statusEl.textContent = 'Saving...';
            statusEl.style.color = '#888';
            try {{
                const resp = await fetch('/api/admin/leagues/toggle', {{
                    method: 'POST',
                    headers: {{'Content-Type': 'application/json'}},
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


@app.route(route="admin/leagues/toggle", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def post_admin_league_toggle(req: func.HttpRequest) -> func.HttpResponse:
    """Toggle a league's inclusion in the capture pipeline."""
    try:
        body = req.get_json()
        league_id = str(body.get("league_id") or "").strip()
        include = bool(body.get("include", True))
        if not league_id:
            return func.HttpResponse(json.dumps({"ok": False, "error": "league_id required"}), status_code=400, mimetype="application/json")

        excluded = load_excluded_league_ids()
        if include:
            excluded.discard(league_id)
        else:
            excluded.add(league_id)
        save_league_preferences(excluded)
        return func.HttpResponse(json.dumps({"ok": True, "league_id": league_id, "included": include, "total_excluded": len(excluded)}), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to toggle league")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(ex)}), status_code=500, mimetype="application/json")


@app.route(route="home", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_home(req: func.HttpRequest) -> func.HttpResponse:
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Cricket Data Platform</title>
        <style>
            body { font-family: Arial; background:#f7f7f7; padding:40px; }
            h1 { margin-bottom:10px; }
            .card {
                background:white;
                padding:20px;
                margin:15px 0;
                border-radius:10px;
                box-shadow:0 2px 8px #ddd;
            }
            a {
                font-size:18px;
                font-weight:bold;
                color:#0066cc;
                text-decoration:none;
            }
            p { margin:8px 0 0; color:#555; }
        </style>
    </head>
    <body>

        <h1>🏏 Cricket Analytics Platform</h1>
        <p>Browse live, upcoming and historical betting data</p>

        <div class="card">
            <a href="/api/matches/view">Live Matches</a>
            <p>Real-time matches with live odds and Bet365 markets</p>
        </div>

        <div class="card">
            <a href="/api/prematch/view">Upcoming Matches</a>
            <p>Prematch odds and markets before the game starts</p>
        </div>

        <div class="card">
            <a href="/api/leagues/view">Leagues</a>
            <p>Browse matches grouped by leagues</p>
        </div>

        <div class="card">
            <a href="/api/prematch/leagues/view">Prematch Leagues</a>
            <p>Upcoming matches grouped by leagues</p>
        </div>

        <div class="card">
            <a href="/api/ended/view">Ended Matches</a>
            <p>Recently finished matches with final results</p>
        </div>

        <div class="card">
            <a href="/api/innings-tracker">Innings Tracker Analytics</a>
            <p>Over/Under prediction accuracy by over stage, team, venue and odds</p>
        </div>

        <div class="card">
            <a href="/api/admin/leagues/view">League Filter</a>
            <p>Select which leagues to capture — excluded leagues skip bronze, silver and gold entirely</p>
        </div>

    </body>
    </html>
    """
    return func.HttpResponse(html, mimetype="text/html")

@app.route(route="", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def root(req: func.HttpRequest) -> func.HttpResponse:
    return get_home(req)