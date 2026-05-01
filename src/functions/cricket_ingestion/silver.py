import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from storage import (
    blob_exists,
    download_json,
    download_required_json,
    extract_results,
    get_env,
    get_int_env,
    get_named_container_client,
    safe_float,
    upload_json,
    utc_now,
)
from cricwebsite_db import write_to_cricwebsite_db
from innings_tracker import (
    extract_innings_snapshot,
    parse_over_from_pg,
    _over_key,
)


# ------------------------------------------------------------------
# Manifest listing
# ------------------------------------------------------------------

def silver_list_manifest_paths(bronze_container, sport_id: str, limit: int) -> List[str]:
    prefix = f"betsapi/inplay_snapshot/sport_id={sport_id}/"
    manifests = []
    for blob in bronze_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/manifest.json"):
            manifests.append((getattr(blob, "last_modified", None), name))
    manifests.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in manifests[:limit]]


# ------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------

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


def fractional_odds_to_decimal(value: Any) -> Optional[float]:
    """Convert Bet365 fractional odds such as 13/8 to decimal odds."""
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
                "over_from_score": None,
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
    - MG = market group, MA = market/column, PA = selection/price row.
    """
    records = extract_bet365_records(bet365_event_payload)
    rows: List[Dict[str, Any]] = []

    current_group: Dict[str, Any] = {}
    current_market: Dict[str, Any] = {}
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
            "odds": odds_fractional,
            "line": line_value,
            "handicap": line_value,
            "suspended": is_suspended,
            "suspended_raw": suspended_raw,
            "raw": r,
        })

    return rows


# ------------------------------------------------------------------
# Main silver parse function
# ------------------------------------------------------------------

def silver_parse_snapshot(
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

    pg_overs_by_name: Dict[str, str] = {}
    fallback_pg_over: Optional[str] = None
    ev_stats_pg_raw: Optional[str] = None
    ev_stats_s3_raw: Optional[str] = None
    ev_stats_ss_raw: Optional[str] = None

    # EV record is the primary source for over (PG field on the match-level event record).
    # RV/RG (team records) are used as per-team fallback only.
    if bet365_event_stats_payload:
        for r in extract_bet365_records(bet365_event_stats_payload):
            r_type = r.get("type")
            if r_type == "EV" and r.get("PG"):
                ev_stats_pg_raw = r.get("PG")
                ev_stats_s3_raw = r.get("S3")
                ev_stats_ss_raw = r.get("SS")
                pg_over = parse_over_from_pg(r["PG"])
                if pg_over is not None:
                    fallback_pg_over = pg_over
            elif r_type in ("RV", "RG") and r.get("NA") and r.get("PG"):
                pg_over = parse_over_from_pg(r["PG"])
                if pg_over is not None:
                    pg_overs_by_name[r["NA"]] = pg_over

    records = extract_bet365_records(bet365_event_payload)
    ev = next((r for r in records if r.get("type") == "EV"), {})
    matching_event = find_matching_event(events_inplay_payload, event_id, fi) or {}

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
        "event_time_utc": None,  # set below via format_unix_ts
        "ev_stats_pg_raw": ev_stats_pg_raw,
        "ev_stats_s3_raw": ev_stats_s3_raw,
        "ev_stats_ss_raw": ev_stats_ss_raw,
        "bet365_event_success": manifest.get("status", {}).get("bet365_event_success"),
        "events_inplay_success": manifest.get("status", {}).get("events_inplay_success"),
        "event_odds_success": manifest.get("status", {}).get("event_odds_success"),
        "event_view_success": manifest.get("status", {}).get("event_view_success"),
        "event_odds_summary_success": manifest.get("status", {}).get("event_odds_summary_success"),
        "api_lineage": lineage_payload or manifest.get("api_lineage"),
        "source_bronze_manifest_path": f"bronze/betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/manifest.json",
        "source_bronze_lineage_path": f"bronze/betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/lineage.json",
    }
    from storage import format_unix_ts
    match_snapshot["event_time_utc"] = format_unix_ts(match_snapshot["event_time_unix"])

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
        bet365_event_payload, snapshot_id, snapshot_time_utc, event_id, fi, score_context,
    )

    return {
        "match_snapshot": match_snapshot,
        "team_scores": team_scores,
        "player_entries": player_entries,
        "odds_records": odds_records,
        "current_markets": current_markets,
    }


def _parse_pg_state(pg_raw: str) -> Optional[Dict[str, Any]]:
    """Break a raw PG string into its state components.

    PG format: "B1:B2:B3:B4:B5:B6#N:W:B"
      N = current over (1-indexed), W = wickets in ball window, B = legal balls this over
      current_over = (N-1).B  e.g. #7:5:3 → 6.3

    Returns None if PG is missing or malformed.
    """
    if not pg_raw or "#" not in pg_raw:
        return None
    try:
        ball_part, suffix = pg_raw.rsplit("#", 1)
        parts = suffix.split(":")
        if len(parts) < 3:
            return None
        n = int(parts[0])
        w = int(parts[1])
        b = int(parts[2])
        if n - 1 < 0 or b < 0:
            return None
        ball_window = [s.strip() for s in ball_part.split(":") if s.strip()] if ball_part else []
        return {
            "pg_suffix": suffix.strip(),
            "ball_window": ball_window,
            "over": f"{n - 1}.{b}",
            "n": n,
            "w_window": w,
            "b_current": b,
        }
    except (ValueError, IndexError):
        return None


def silver_write_state_file(silver_container, parsed: Dict[str, Any]) -> None:
    """Write one file per unique match state to silver/cricket/inplay/state/.

    State key = "{innings_no}-{pg_suffix}" e.g. "1-7:5:3"
      innings_no: 1 if S3 is empty (1st innings), 2 if S3 has a target value (2nd innings)
      pg_suffix: raw N:W:B from PG field on the EV stats record

    Each state file contains: ball window, score, player entries, and all active market rows.
    Overwritten on each tick so the file always holds the latest snapshot for that state —
    this is intentional: odds settle as a state progresses, the last snapshot is cleanest.
    """
    match = parsed["match_snapshot"]
    event_id = match["event_id"]

    pg_raw = match.get("ev_stats_pg_raw")
    s3_raw = match.get("ev_stats_s3_raw")
    if not pg_raw:
        return

    pg_state = _parse_pg_state(pg_raw)
    if pg_state is None:
        return

    innings_no = 2 if s3_raw and str(s3_raw).strip() not in ("", "0") else 1
    suffix = pg_state["pg_suffix"]
    n, w, b = pg_state["n"], pg_state["w_window"], pg_state["b_current"]
    state_key = f"{innings_no}-{suffix}"

    ss = match.get("ev_stats_ss_raw") or match.get("score_summary_bet365") or ""
    runs: Optional[int] = None
    wickets: Optional[int] = None
    if ss and "/" in str(ss):
        try:
            r_str, w_str = str(ss).split("/", 1)
            runs = int(r_str.strip())
            wickets = int(w_str.strip())
        except Exception:
            pass

    target: Optional[int] = None
    if innings_no == 2 and s3_raw:
        try:
            target = int(str(s3_raw).strip())
        except Exception:
            pass

    batting_team: Optional[str] = None
    for ts in parsed["team_scores"]:
        if ts.get("pg_over"):
            batting_team = ts.get("name")
            break
    home = match.get("home_team_name") or ""
    away = match.get("away_team_name") or ""
    bowling_team = (away if batting_team == home else home) if batting_team else away

    state_file = {
        "state_key": state_key,
        "innings": innings_no,
        "pg_suffix": suffix,
        "over": pg_state["over"],
        "ball_window": pg_state["ball_window"],
        "n": n,
        "w_window": w,
        "b_current": b,
        "score": ss,
        "runs": runs,
        "wickets": wickets,
        "target": target,
        "batting_team": batting_team or home,
        "bowling_team": bowling_team,
        "home_team_name": home,
        "away_team_name": away,
        "event_id": event_id,
        "match_name": match.get("match_name"),
        "league_id": match.get("league_id"),
        "league_name": match.get("league_name"),
        "venue": match.get("venue"),
        "team_scores": parsed["team_scores"],
        "player_entries": parsed["player_entries"],
        "markets": parsed.get("current_markets", []),
        "snapshot_id": match["snapshot_id"],
        "snapshot_time_utc": match["snapshot_time_utc"],
    }

    state_path = f"cricket/inplay/state/event_id={event_id}/state_{innings_no}_{n}_{w}_{b}.json"
    upload_json(silver_container, state_path, state_file, overwrite=True)


def silver_write_outputs(silver_container, parsed: Dict[str, Any]) -> None:
    """Write all silver outputs for one parsed snapshot, including innings tracker accumulator."""
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

    if current_market_rows:
        control_path = f"cricket/inplay/control/event_id={event_id}/last_known_markets.json"
        upload_json(silver_container, control_path, {
            "rows": current_market_rows,
            "snapshot_id": match["snapshot_id"],
            "snapshot_time_utc": match["snapshot_time_utc"],
        }, overwrite=True)

    silver_write_state_file(silver_container, parsed)
    write_to_cricwebsite_db(match, parsed["team_scores"], current_market_rows)

    innings_point = extract_innings_snapshot(match, parsed["team_scores"], current_market_rows)
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
        if last is None or _over_key(last.get("over")) != _over_key(innings_point.get("over")) or last.get("score") != innings_point.get("score"):
            rows.append(innings_point)
        elif innings_point.get("predicted_total") is not None and last.get("predicted_total") != innings_point.get("predicted_total"):
            rows[-1] = innings_point
        acc["rows"] = rows
        acc["last_updated_utc"] = utc_now().isoformat()
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


def silver_parse_bronze_to_silver() -> None:
    """Timer trigger body: parse unprocessed bronze snapshots into silver."""
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_SILVER_SNAPSHOTS_PER_RUN", 200)
    bronze = get_named_container_client("bronze")
    silver = get_named_container_client("silver")
    manifest_paths = silver_list_manifest_paths(bronze, sport_id, max_per_run)
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
            parsed = silver_parse_snapshot(
                manifest,
                events_inplay_payload,
                bet365_event_payload,
                event_odds_payload,
                event_view_payload=event_view_payload,
                event_odds_summary_payload=event_odds_summary_payload,
                lineage_payload=lineage_payload,
                bet365_event_stats_payload=bet365_event_stats_payload,
            )
            silver_write_outputs(silver, parsed)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to parse bronze snapshot to silver")

    logging.info(json.dumps({
        "event": "silver_parse_bronze_to_silver_completed",
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "checked": len(manifest_paths),
    }))
