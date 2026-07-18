"""
Live Match Notifier — email and SMS alerts for live cricket events.

Two notification triggers:
  1. Match start: when a new match appears in the BetsAPI live feed for the first
     time, send match details (teams, league, current odds).
  2. New checkpoint prediction: when the win predictor generates a prediction for
     a checkpoint the user has not yet been notified about, send the win probability.

Channels:
  - Email via Azure Communication Services (ACS)
  - SMS via Twilio

Preferences stored in gold/cricket/config/notification_prefs.json.
Notification state (which events/checkpoints have been notified) stored in
gold/cricket/inplay/notification_state.json.

Environment variables:
  ACS_EMAIL_CONNECTION_STRING   — from Azure Communication Services
  ACS_EMAIL_SENDER              — "DoNotReply@<your-acs-domain>.azurecomm.net"
  NOTIFICATION_EMAIL            — recipient email (default: dasgupta.sagarnil@gmail.com)
  TWILIO_ACCOUNT_SID            — Twilio account SID
  TWILIO_AUTH_TOKEN             — Twilio auth token
  TWILIO_FROM_NUMBER            — Twilio sending phone number
  NOTIFICATION_PHONE            — recipient phone number (default: +31611012323)
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

_PREFS_BLOB   = "cricket/config/notification_prefs.json"
_STATE_BLOB   = "cricket/inplay/notification_state.json"

_DEFAULT_EMAIL = "dasgupta.sagarnil@gmail.com"
_DEFAULT_PHONE = "+31611012323"

_ALL_CHECKPOINTS = [
    "innings1-only",
    "innings2-2over",
    "innings2-6over",
    "innings2-10over",
    "innings2-16over",
]


# ── Preferences ───────────────────────────────────────────────────────────────

def _default_prefs() -> Dict:
    return {
        "email_enabled":      True,
        "sms_enabled":        True,
        "notify_match_start": True,
        "notify_checkpoints": list(_ALL_CHECKPOINTS),
    }


def _load_prefs(gold) -> Dict:
    try:
        raw = gold.get_blob_client(_PREFS_BLOB).download_blob().readall()
        return json.loads(raw)
    except Exception:
        return _default_prefs()


# ── Notification state ────────────────────────────────────────────────────────

def _load_state(gold) -> Dict:
    try:
        raw = gold.get_blob_client(_STATE_BLOB).download_blob().readall()
        return json.loads(raw)
    except Exception:
        return {"known_event_ids": [], "notified_checkpoints": {}}


def _save_state(gold, state: Dict) -> None:
    state["updated_utc"] = datetime.now(timezone.utc).isoformat()
    try:
        gold.get_blob_client(_STATE_BLOB).upload_blob(
            json.dumps(state, indent=2).encode(), overwrite=True
        )
    except Exception as exc:
        logging.warning(f"live_notifier: could not save notification state: {exc}")


# ── Email via ACS ─────────────────────────────────────────────────────────────

def _send_email(subject: str, body_html: str) -> bool:
    conn_str = os.environ.get("ACS_EMAIL_CONNECTION_STRING", "")
    sender   = os.environ.get("ACS_EMAIL_SENDER", "")
    to_addr  = os.environ.get("NOTIFICATION_EMAIL", _DEFAULT_EMAIL)

    if not conn_str or not sender:
        logging.warning("live_notifier: ACS email not configured (ACS_EMAIL_CONNECTION_STRING / ACS_EMAIL_SENDER missing)")
        return False

    try:
        from azure.communication.email import EmailClient  # type: ignore
        client  = EmailClient.from_connection_string(conn_str)
        message = {
            "senderAddress": sender,
            "recipients":    {"to": [{"address": to_addr}]},
            "content":       {"subject": subject, "html": body_html},
        }
        poller = client.begin_send(message)
        result = poller.result()
        logging.info(f"live_notifier: email sent to {to_addr} — {subject}")
        return True
    except Exception as exc:
        logging.error(f"live_notifier: email failed: {exc}")
        return False


# ── SMS via Twilio ─────────────────────────────────────────────────────────────

def _send_sms(body: str) -> bool:
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    auth_token  = os.environ.get("TWILIO_AUTH_TOKEN", "")
    from_num    = os.environ.get("TWILIO_FROM_NUMBER", "")
    to_num      = os.environ.get("NOTIFICATION_PHONE", _DEFAULT_PHONE)

    if not account_sid or not auth_token or not from_num:
        logging.warning("live_notifier: Twilio not configured — skipping WhatsApp")
        return False

    # Use WhatsApp channel via Twilio — works internationally from any Twilio number.
    # Sandbox from-number: +14155238886  (set TWILIO_FROM_NUMBER to this value)
    # Recipient must have joined the sandbox once: send the join code to +14155238886
    from_wa = f"whatsapp:{from_num}"
    to_wa   = f"whatsapp:{to_num}"

    try:
        from twilio.rest import Client  # type: ignore
        client  = Client(account_sid, auth_token)
        message = client.messages.create(body=body, from_=from_wa, to=to_wa)
        logging.info(f"live_notifier: WhatsApp sent to {to_wa} (sid={message.sid})")
        return True
    except Exception as exc:
        logging.error(f"live_notifier: WhatsApp failed: {exc}")
        return False


# ── Notification builders ─────────────────────────────────────────────────────

def _match_start_email_html(eid: str, accum: Dict) -> str:
    rows  = accum.get("rows") or []
    first = rows[-1] if rows else {}
    home  = accum.get("home_team_name", "Home")
    away  = accum.get("away_team_name", "Away")
    league = accum.get("league_name", "")
    bat_odds  = first.get("batting_team_odds")
    bowl_odds = first.get("bowling_team_odds")
    bat_team  = first.get("batting_team") or home
    bowl_team = first.get("bowling_team") or away
    score = first.get("score", 0)
    wkts  = first.get("wickets", 0)
    over  = first.get("over", "0.0")

    odds_html = ""
    if bat_odds or bowl_odds:
        odds_html = (
            f"<p><b>Odds:</b> {bat_team} {bat_odds or '—'} | {bowl_team} {bowl_odds or '—'}</p>"
        )

    return f"""
<html><body>
<h2>🏏 Match Now Live: {home} vs {away}</h2>
<p><b>League:</b> {league}</p>
<p><b>Event ID:</b> {eid}</p>
<p><b>Current score:</b> {bat_team} {score}/{wkts} ({over} ov)</p>
{odds_html}
<p><a href="https://func-ramanuj-display.azurewebsites.net/api/live/view">View live tracker →</a></p>
</body></html>
""".strip()


def _match_start_sms(eid: str, accum: Dict) -> str:
    home   = accum.get("home_team_name", "Home")
    away   = accum.get("away_team_name", "Away")
    league = accum.get("league_name", "")
    rows   = accum.get("rows") or []
    first  = rows[-1] if rows else {}
    bat_odds  = first.get("batting_team_odds")
    bowl_odds = first.get("bowling_team_odds")
    bat_team  = first.get("batting_team") or home
    bowl_team = first.get("bowling_team") or away
    odds_str  = f" | Odds: {bat_team}={bat_odds} {bowl_team}={bowl_odds}" if (bat_odds or bowl_odds) else ""
    return f"LIVE: {home} vs {away} ({league}){odds_str}"


def _checkpoint_email_html(eid: str, checkpoint: str, pred: Dict, accum: Dict) -> str:
    home  = accum.get("home_team_name", "?")
    away  = accum.get("away_team_name", "?")
    bat   = pred.get("bat_team") or home
    bowl  = pred.get("bowl_team") or away
    p_chase   = round((pred.get("prob_chase_wins") or 0) * 100)
    p_defend  = round((pred.get("prob_defends") or 0) * 100)
    inn1_score = pred.get("inn1_total_score", "—")
    inn2_score = pred.get("inn2_score_at_cp", "—")
    inn2_wkts  = pred.get("inn2_wickets_at_cp", "—")

    cp_label = checkpoint.replace("innings1", "After Inn 1").replace("innings2-", "Inn 2 after ").replace("over", " overs")

    score_context = (
        f"<p><b>Inn 1:</b> {bowl} set {inn1_score}</p>"
        if checkpoint == "innings1-only" else
        f"<p><b>Inn 1:</b> {bowl} set {inn1_score} | <b>Inn 2:</b> {bat} {inn2_score}/{inn2_wkts}</p>"
    )

    return f"""
<html><body>
<h2>📊 Win Predictor: {home} vs {away}</h2>
<p><b>Checkpoint:</b> {cp_label}</p>
{score_context}
<p><b>Chase wins (bat team wins):</b> {p_chase}%</p>
<p><b>Defend wins (bowl team wins):</b> {p_defend}%</p>
<p><b>Batting:</b> {bat} &nbsp; <b>Bowling:</b> {bowl}</p>
<p><a href="https://func-ramanuj-display.azurewebsites.net/api/live/view">View live tracker →</a></p>
</body></html>
""".strip()


def _checkpoint_sms(eid: str, checkpoint: str, pred: Dict, accum: Dict) -> str:
    home  = accum.get("home_team_name", "?")
    away  = accum.get("away_team_name", "?")
    p_chase  = round((pred.get("prob_chase_wins") or 0) * 100)
    p_defend = round((pred.get("prob_defends") or 0) * 100)
    cp_short = checkpoint.replace("innings1-only", "Inn1 end").replace("innings2-", "Inn2 ov").replace("over", "")
    inn1_score = pred.get("inn1_total_score", "?")
    return f"{home} vs {away} [{cp_short}] Inn1={inn1_score} | Chase {p_chase}% | Defend {p_defend}%"


# ── Main entry point ───────────────────────────────────────────────────────────

def process_notifications(
    gold,
    accumulators: Dict[str, Dict],
    win_results: Dict[str, List[Dict]],
) -> None:
    """
    Check for new matches and new checkpoint predictions; send configured
    notifications.  Updates notification state in gold.

    Args:
        gold            — gold container client
        accumulators    — event_id → accumulator dict (from betsapi_live_parser)
        win_results     — event_id → list of checkpoint prediction dicts
    """
    prefs = _load_prefs(gold)
    state = _load_state(gold)

    email_on   = prefs.get("email_enabled", True)
    sms_on     = prefs.get("sms_enabled", True)
    match_on   = prefs.get("notify_match_start", True)
    cp_enabled = set(prefs.get("notify_checkpoints", _ALL_CHECKPOINTS))

    known_ids: List[str] = state.setdefault("known_event_ids", [])
    notified_cps: Dict[str, List[str]] = state.setdefault("notified_checkpoints", {})

    state_changed = False

    for eid, accum in accumulators.items():
        # ── Match start notification ───────────────────────────────────────────
        if match_on and eid not in known_ids:
            logging.info(f"live_notifier: new match {eid} — sending start notification")
            subject  = f"Live cricket match started: {accum.get('home_team_name', '')} vs {accum.get('away_team_name', '')}"
            html_msg = _match_start_email_html(eid, accum)
            sms_msg  = _match_start_sms(eid, accum)
            if email_on:
                _send_email(subject, html_msg)
            if sms_on:
                _send_sms(sms_msg)
            known_ids.append(eid)
            state_changed = True

        # ── Checkpoint prediction notifications ────────────────────────────────
        preds = win_results.get(eid) or []
        notified_for_event = set(notified_cps.get(eid) or [])

        for pred in preds:
            cp = pred.get("checkpoint", "")
            if not cp or cp not in cp_enabled or cp in notified_for_event:
                continue

            logging.info(f"live_notifier: new checkpoint {cp} for {eid} — sending notification")
            subject  = f"Win prediction [{cp}]: {accum.get('home_team_name', '')} vs {accum.get('away_team_name', '')}"
            html_msg = _checkpoint_email_html(eid, cp, pred, accum)
            sms_msg  = _checkpoint_sms(eid, cp, pred, accum)
            if email_on:
                _send_email(subject, html_msg)
            if sms_on:
                _send_sms(sms_msg)
            notified_for_event.add(cp)
            state_changed = True

        if notified_for_event:
            notified_cps[eid] = sorted(notified_for_event)

    # Remove events no longer live to keep state compact
    active_ids = set(accumulators)
    known_ids[:] = [e for e in known_ids if e in active_ids or e in notified_cps]

    if state_changed:
        _save_state(gold, state)
