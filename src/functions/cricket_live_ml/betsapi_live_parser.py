"""
BetsAPI Live Parser — fetches live cricket match data directly from BetsAPI
and builds accumulator dicts (identical schema to silver innings_accumulator.json)
for the win predictor ML model.

Called every 60 seconds by the live_ml_tick timer.  Accumulators are persisted
in gold between ticks, building up per-over history the same way the silver
pipeline does for ended matches.

ISOLATION: reads from BetsAPI only; writes only to
  gold/cricket/inplay/live_accumulators/event_id={eid}.json
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

_LIVE_ACCUM_BLOB = "cricket/inplay/live_accumulators/event_id={eid}.json"


# ── BetsAPI HTTP helper ────────────────────────────────────────────────────────

def _api_get(path: str, params: Dict[str, Any]) -> Optional[Dict]:
    base = os.environ.get("BETS_API_BASE_URL", "https://api.b365api.com").rstrip("/")
    token = os.environ.get("BETS_API_TOKEN", "")
    if not token:
        logging.warning("betsapi_live_parser: BETS_API_TOKEN not set")
        return None
    try:
        resp = requests.get(
            f"{base}{path}",
            params={**params, "token": token},
            timeout=8,
        )
        body = resp.json()
        if isinstance(body, dict) and body.get("success") in (1, "1", True):
            return body
        logging.debug(f"betsapi_live_parser: {path} returned non-success: {body.get('error', '')}")
    except Exception as exc:
        logging.warning(f"betsapi_live_parser: {path} failed: {exc}")
    return None


# ── Bet365 stream parsing ─────────────────────────────────────────────────────

def _extract_records(body: Dict) -> List[Dict]:
    results = body.get("results", [])
    if results and isinstance(results[0], list):
        return results[0]
    return results if isinstance(results, list) else []


def _parse_over_from_pg(pg: str) -> Optional[str]:
    """PG = 'B1:B2:B3:B4:B5:B6#N:W:B' → completed over = (N-1).B"""
    if not pg or "#" not in pg:
        return None
    try:
        suffix = pg.rsplit("#", 1)[1].split(":")
        n = int(suffix[0])
        b = int(suffix[2])
        return f"{max(0, n - 1)}.{b}"
    except Exception:
        return None


def _parse_s5(raw: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """'runs#wickets' → (runs, wickets). Returns (None, None) on failure."""
    if not raw or "#" not in raw:
        return None, None
    try:
        left, right = raw.split("#", 1)
        return int(left.strip()), int(right.strip())
    except Exception:
        return None, None


def _fractional_to_decimal(value: Any) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip()
    if "/" not in raw:
        try:
            return float(raw)
        except Exception:
            return None
    try:
        num, den = raw.split("/", 1)
        den_f = float(den)
        return round(1 + float(num) / den_f, 3) if den_f != 0 else None
    except Exception:
        return None


def _parse_bet365_stream(
    body: Dict,
    home_name: str,
    away_name: str,
) -> Optional[Dict[str, Any]]:
    """
    Parse a /v1/bet365/event?stats=1 response into match-state fields.

    Returns:
        {innings, over, score, wickets, batting_team, bowling_team,
         batting_team_odds, bowling_team_odds, match_name, target_score}
    or None if score data is unavailable.
    """
    records = _extract_records(body)

    ev = next((r for r in records if isinstance(r, dict) and r.get("type") == "EV"), {})
    pg_raw = str(ev.get("PG") or "")
    s3_raw = str(ev.get("S3") or "").strip()
    match_name = str(ev.get("NA") or "").strip()

    current_over = _parse_over_from_pg(pg_raw)
    innings_no = 2 if s3_raw and s3_raw not in ("", "0") else 1

    target_score: Optional[int] = None
    if innings_no == 2:
        try:
            target_score = int(s3_raw)
        except ValueError:
            pass

    # Extract batting team score from TE record with PI="1"
    batting_team: Optional[str] = None
    score: Optional[int] = None
    wickets: Optional[int] = None

    for r in records:
        if not isinstance(r, dict) or r.get("type") != "TE":
            continue
        if not (r.get("S5") or r.get("SC")):
            continue
        if str(r.get("PI") or "") != "1":
            continue
        runs, wkts = _parse_s5(r.get("S5"))
        if runs is None:
            continue
        batting_team = str(r.get("NA") or "").strip()
        score = runs
        wickets = wkts or 0
        break

    if score is None:
        return None

    bowling_team: Optional[str] = None
    if batting_team:
        bowling_team = (
            away_name
            if batting_team.lower() == (home_name or "").lower()
            else home_name
        )

    # Win market odds: scan MG/PA records
    home_od: Optional[float] = None
    away_od: Optional[float] = None
    in_winner_group = False
    _WINNER_KWS = ("match betting", "match winner", "to win the match", "win match", "full time")

    for r in records:
        if not isinstance(r, dict):
            continue
        rtype = r.get("type")
        if rtype == "MG":
            gname = str(r.get("NA") or r.get("IT") or "").lower()
            in_winner_group = any(kw in gname for kw in _WINNER_KWS)
        elif rtype == "PA" and in_winner_group:
            od = _fractional_to_decimal(r.get("OD"))
            if od is None:
                continue
            sel = str(r.get("NA") or "").strip().lower()
            if home_name and home_name.lower() in sel:
                home_od = od
            elif away_name and away_name.lower() in sel:
                away_od = od
            elif home_od is None:
                home_od = od
            elif away_od is None:
                away_od = od

    bat_odds: Optional[float] = None
    bowl_odds: Optional[float] = None
    if batting_team:
        if batting_team.lower() == (home_name or "").lower():
            bat_odds, bowl_odds = home_od, away_od
        else:
            bat_odds, bowl_odds = away_od, home_od

    return {
        "innings":            innings_no,
        "over":               current_over,
        "score":              score,
        "wickets":            wickets,
        "batting_team":       batting_team,
        "bowling_team":       bowling_team,
        "batting_team_odds":  bat_odds,
        "bowling_team_odds":  bowl_odds,
        "match_name":         match_name,
        "target_score":       target_score,
    }


# ── Live accumulator persistence ──────────────────────────────────────────────

def _load_live_accum(gold, event_id: str) -> Dict:
    path = _LIVE_ACCUM_BLOB.format(eid=event_id)
    try:
        return json.loads(gold.get_blob_client(path).download_blob().readall())
    except Exception:
        return {"rows": [], "event_id": event_id}


def _save_live_accum(gold, event_id: str, accum: Dict) -> None:
    path = _LIVE_ACCUM_BLOB.format(eid=event_id)
    try:
        gold.get_blob_client(path).upload_blob(
            json.dumps(accum, indent=2).encode(), overwrite=True
        )
    except Exception as exc:
        logging.warning(f"betsapi_live_parser: could not save accum {event_id}: {exc}")


def _over_float(over_str: Optional[str]) -> float:
    try:
        return float(over_str or "0")
    except Exception:
        return 0.0


def _should_append(accum: Dict, new_row: Dict) -> bool:
    """True if new_row represents a new over or new innings vs last saved row."""
    rows = accum.get("rows") or []
    if not rows:
        return True
    last = rows[-1]
    new_inn = new_row.get("innings", 1)
    new_ov = _over_float(new_row.get("over"))
    last_inn = last.get("innings", 1)
    last_ov = _over_float(last.get("over"))
    # New innings always appended; same innings only when over advances
    if new_inn > last_inn:
        return True
    if new_inn == last_inn and new_ov > last_ov:
        return True
    return False


# ── Main entry point ───────────────────────────────────────────────────────────

def fetch_live_accumulators(gold) -> Dict[str, Dict]:
    """
    Call BetsAPI, build/update live accumulators, return them keyed by event_id.

    Each call:
      1. Lists live cricket events from /v3/events/inplay.
      2. For each event, calls /v1/bet365/event?FI=XX&stats=1 to get current state.
      3. Loads existing live accumulator from gold (empty dict if first call).
      4. Appends a new row if the over has advanced since last tick.
      5. Saves updated accumulator to gold.
      6. Returns all active accumulators for the caller to run ML on.
    """
    sport_id = os.environ.get("SPORT_ID", "3")

    inplay_resp = _api_get("/v3/events/inplay", {"sport_id": sport_id})
    if not inplay_resp:
        logging.info("betsapi_live_parser: /v3/events/inplay returned nothing")
        return {}

    items = inplay_resp.get("results") or []
    if isinstance(items, dict):
        items = list(items.values())

    accumulators: Dict[str, Dict] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("time_status", "1")) != "1":
            continue  # only in-progress matches

        event_id = str(
            item.get("id") or item.get("our_event_id") or item.get("event_id") or ""
        ).strip()
        fi = str(
            item.get("bet365_id") or item.get("FI") or item.get("fi") or item.get("id") or ""
        ).strip()
        if not event_id or not fi:
            continue

        home = item.get("home") or {}
        away = item.get("away") or {}
        league = item.get("league") or {}
        home_name = str(home.get("name") or "").strip()
        away_name = str(away.get("name") or "").strip()

        bet365_resp = _api_get("/v1/bet365/event", {"FI": fi, "stats": "1"})
        if not bet365_resp:
            continue

        parsed = _parse_bet365_stream(bet365_resp, home_name, away_name)
        if not parsed or parsed.get("score") is None:
            logging.debug(f"betsapi_live_parser: no score for event {event_id}")
            continue

        accum = _load_live_accum(gold, event_id)

        # Populate metadata on first encounter
        if not accum.get("match_name"):
            accum.update({
                "event_id":       event_id,
                "fi":             fi,
                "match_name":     parsed.get("match_name") or f"{home_name} v {away_name}",
                "home_team_name": home_name,
                "away_team_name": away_name,
                "league_name":    str((league.get("name") or "")).strip(),
                "match_date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            })

        if parsed.get("target_score"):
            accum["target_score"] = parsed["target_score"]

        new_row = {
            "innings":           parsed["innings"],
            "over":              parsed["over"],
            "score":             parsed["score"],
            "wickets":           parsed["wickets"],
            "batting_team":      parsed["batting_team"],
            "bowling_team":      parsed["bowling_team"],
            "batting_team_odds": parsed["batting_team_odds"],
            "bowling_team_odds": parsed["bowling_team_odds"],
            "captured_utc":      datetime.now(timezone.utc).isoformat(),
        }

        if _should_append(accum, new_row):
            accum.setdefault("rows", []).append(new_row)
            _save_live_accum(gold, event_id, accum)
            logging.debug(
                f"betsapi_live_parser: {event_id} → inn{parsed['innings']} ov{parsed['over']} "
                f"score={parsed['score']}/{parsed['wickets']}"
            )

        accumulators[event_id] = accum

    logging.info(f"betsapi_live_parser: {len(accumulators)} live event(s)")
    return accumulators
