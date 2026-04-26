import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

app = func.FunctionApp()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def get_container_client():
    storage_conn = get_env("DATA_STORAGE_CONNECTION_STRING")
    blob_service = BlobServiceClient.from_connection_string(storage_conn)
    return blob_service.get_container_client("bronze")


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
    for key in ["our_event_id", "event_id", "id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def get_fi_from_inplay_item(item: Dict[str, Any]) -> Optional[str]:
    # For Bet365 inplay_filter, docs say the returned id can be used as FI.
    # Some responses also contain FI explicitly.
    for key in ["FI", "fi", "id"]:
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
        live.append({
            "fi": fi,
            "event_id": event_id or fi,
            "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
            "league": item.get("league"),
            "home": item.get("home"),
            "away": item.get("away"),
            "time_status": item.get("time_status"),
            "raw_item": item,
        })
    if max_live_matches <= 0:
        return live
    return live[:max_live_matches]


@app.timer_trigger(
    schedule="*/15 * * * * *",
    arg_name="timer",
    run_on_startup=True,
    use_monitor=False,
)
def discover_cricket_inplay(timer: func.TimerRequest) -> None:
    """Discover live cricket matches and write latest active FI list to bronze/control."""
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_live_matches = get_int_env("MAX_LIVE_MATCHES", 10)
    container = get_container_client()

    api_payload = call_betsapi(
        path="/v1/bet365/inplay_filter",
        params={"sport_id": sport_id},
    )

    raw_path = (
        f"betsapi/inplay_filter/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"inplay_filter_{ts_compact(now)}.json"
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


@app.timer_trigger(
    schedule="*/5 * * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def capture_cricket_inplay_snapshot(timer: func.TimerRequest) -> None:
    """Capture synchronized match state + Bet365 odds snapshot for active live cricket matches."""
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    container = get_container_client()

    control = download_json(container, "betsapi/control/active_inplay_fi/latest.json")
    if not control or not control.get("active_matches"):
        logging.info(json.dumps({
            "event": "capture_cricket_inplay_snapshot_skipped",
            "reason": "no_active_matches",
        }))
        return

    active_matches = control["active_matches"]
    enable_events_inplay = get_bool_env("ENABLE_EVENTS_INPLAY_MATCH_STATE", True)

    # One clean match-state call shared for all active matches in this snapshot.
    events_inplay_payload = None
    if enable_events_inplay:
        events_inplay_payload = call_betsapi(
            path="/v3/events/inplay",
            params={"sport_id": sport_id},
        )

    for match in active_matches:
        snapshot_time = utc_now()
        snapshot_id = ts_compact(snapshot_time)
        fi = str(match["fi"])
        event_id = str(match.get("event_id") or fi)

        bet365_event_payload = call_betsapi(
            path="/v1/bet365/event",
            params={"FI": fi, "stats": 1},
        )

        base_path = (
            f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/"
            f"snapshot_id={snapshot_id}"
        )

        manifest = {
            "snapshot_id": snapshot_id,
            "snapshot_time_utc": snapshot_time.isoformat(),
            "sport_id": sport_id,
            "event_id": event_id,
            "fi": fi,
            "match_from_filter": match,
            "files": {
                "events_inplay": f"bronze/{base_path}/events_inplay.json" if events_inplay_payload else None,
                "bet365_event": f"bronze/{base_path}/bet365_event.json",
            },
            "status": {
                "events_inplay_success": events_inplay_payload["response"]["success"] if events_inplay_payload else None,
                "bet365_event_success": bet365_event_payload["response"]["success"],
                "bet365_event_error": bet365_event_payload["response"].get("error"),
                "bet365_event_error_detail": bet365_event_payload["response"].get("error_detail"),
            },
        }

        if events_inplay_payload:
            upload_json(container, f"{base_path}/events_inplay.json", events_inplay_payload)

        upload_json(container, f"{base_path}/bet365_event.json", bet365_event_payload)
        upload_json(container, f"{base_path}/manifest.json", manifest)

        logging.info(json.dumps({
            "event": "capture_cricket_inplay_snapshot_completed",
            "snapshot_id": snapshot_id,
            "event_id": event_id,
            "fi": fi,
            "bet365_event_success": bet365_event_payload["response"]["success"],
            "base_path": f"bronze/{base_path}",
        }))


# -----------------------------
# Silver parsing helpers
# -----------------------------

def get_blob_service_client():
    storage_conn = get_env("DATA_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(storage_conn)


def get_named_container_client(container_name: str):
    blob_service = get_blob_service_client()
    container = blob_service.get_container_client(container_name)
    try:
        container.create_container()
    except Exception:
        pass
    return container


def list_manifest_paths(bronze_container, sport_id: str, limit: int) -> List[str]:
    prefix = f"betsapi/inplay_snapshot/sport_id={sport_id}/"
    manifests = []
    for blob in bronze_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/manifest.json"):
            manifests.append((getattr(blob, "last_modified", None), name))
    manifests.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in manifests[:limit]]


def blob_exists(container_client, blob_path: str) -> bool:
    try:
        container_client.get_blob_client(blob_path).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False


def download_required_json(container_client, blob_path: str) -> Dict[str, Any]:
    data = container_client.download_blob(blob_path).readall()
    return json.loads(data)


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
    body = bet365_payload.get("response", {}).get("body", {})
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
    return None


def parse_silver_snapshot(
    manifest: Dict[str, Any],
    events_inplay_payload: Dict[str, Any],
    bet365_event_payload: Dict[str, Any],
) -> Dict[str, Any]:
    snapshot_id = manifest["snapshot_id"]
    snapshot_time_utc = manifest["snapshot_time_utc"]
    event_id = str(manifest["event_id"])
    fi = str(manifest["fi"])
    sport_id = str(manifest.get("sport_id", "3"))

    records = extract_bet365_records(bet365_event_payload)
    ev = next((r for r in records if r.get("type") == "EV"), {})
    matching_event = find_matching_event(events_inplay_payload, event_id, fi) or {}

    match_from_filter = manifest.get("match_from_filter", {})
    home = matching_event.get("home") or match_from_filter.get("home") or {}
    away = matching_event.get("away") or match_from_filter.get("away") or {}
    league = matching_event.get("league") or match_from_filter.get("league") or {}

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
        "score_summary_events": matching_event.get("ss") or match_from_filter.get("raw_item", {}).get("ss"),
        "score_summary_bet365": ev.get("SS"),
        "bet365_event_success": manifest.get("status", {}).get("bet365_event_success"),
        "events_inplay_success": manifest.get("status", {}).get("events_inplay_success"),
        "source_bronze_manifest_path": f"bronze/betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/manifest.json",
    }

    team_scores = []
    player_entries = []
    odds_records = []

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
            })

        if r_type == "TE" and r.get("NA") and not (r.get("SC") or r.get("S5")):
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

        if r_type in {"MA", "PA", "MG", "ML", "SE"} or any(k in r for k in ["OD", "FD", "HD", "HA"]):
            odds_records.append({
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time_utc,
                "event_id": event_id,
                "fi": fi,
                "record_type": r_type,
                "market_or_selection_name": r.get("NA"),
                "odds_decimal": r.get("OD") or r.get("FD"),
                "handicap": r.get("HD") or r.get("HA"),
                "raw": r,
            })

    return {
        "match_snapshot": match_snapshot,
        "team_scores": team_scores,
        "player_entries": player_entries,
        "odds_records": odds_records,
    }


def write_silver_outputs(silver_container, parsed: Dict[str, Any]) -> None:
    match = parsed["match_snapshot"]
    dt = datetime.fromisoformat(match["snapshot_time_utc"].replace("Z", "+00:00"))
    base = (
        f"cricket/inplay/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/"
        f"event_id={match['event_id']}/snapshot_id={match['snapshot_id']}"
    )

    upload_json(silver_container, f"{base}/match_snapshot.json", parsed["match_snapshot"], overwrite=True)
    upload_json(silver_container, f"{base}/team_scores.json", {"rows": parsed["team_scores"]}, overwrite=True)
    upload_json(silver_container, f"{base}/player_entries.json", {"rows": parsed["player_entries"]}, overwrite=True)
    upload_json(silver_container, f"{base}/odds_records.json", {"rows": parsed["odds_records"]}, overwrite=True)

    marker_path = f"cricket/control/processed_snapshots/{match['snapshot_id']}_{match['event_id']}_{match['fi']}.json"
    marker = {
        "processed_at_utc": utc_now().isoformat(),
        "snapshot_id": match["snapshot_id"],
        "event_id": match["event_id"],
        "fi": match["fi"],
        "silver_base_path": f"silver/{base}",
        "team_score_count": len(parsed["team_scores"]),
        "player_entry_count": len(parsed["player_entries"]),
        "odds_record_count": len(parsed["odds_records"]),
    }
    upload_json(silver_container, marker_path, marker, overwrite=True)


@app.timer_trigger(
    schedule="*/30 * * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def parse_cricket_bronze_to_silver(timer: func.TimerRequest) -> None:
    """Parse recent bronze in-play snapshots into silver JSON tables."""
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_SILVER_SNAPSHOTS_PER_RUN", 50)

    bronze = get_named_container_client("bronze")
    silver = get_named_container_client("silver")

    manifest_paths = list_manifest_paths(bronze, sport_id, max_per_run)
    processed = 0
    skipped = 0
    failed = 0

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
            events_inplay_payload = download_required_json(bronze, f"{base_path}/events_inplay.json")
            bet365_event_payload = download_required_json(bronze, f"{base_path}/bet365_event.json")

            parsed = parse_silver_snapshot(manifest, events_inplay_payload, bet365_event_payload)
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
