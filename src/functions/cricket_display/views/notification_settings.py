"""
Notification Settings — /api/notification/settings

GET:  render the settings form.
POST: save preferences to gold/cricket/config/notification_prefs.json
      and re-render the form with a confirmation message.

Preferences control which email/SMS alerts the live_ml function sends:
  - email_enabled / sms_enabled     — master toggles
  - notify_match_start              — alert when a match first goes live
  - notify_checkpoints              — list of checkpoint names that trigger alerts
"""
import urllib.parse
from datetime import datetime, timezone
from html import escape

import azure.functions as func

from .common import (
    json, logging, func,
    get_named_container_client, download_json, upload_json, utc_now,
)

_PREFS_BLOB = "cricket/config/notification_prefs.json"

_ALL_CHECKPOINTS = [
    ("innings1-only",   "After innings 1 (bat first team finishes)"),
    ("innings2-2over",  "Inn 2 after 2 overs"),
    ("innings2-6over",  "Inn 2 after 6 overs"),
    ("innings2-10over", "Inn 2 after 10 overs"),
    ("innings2-16over", "Inn 2 after 16 overs"),
]


def _default_prefs() -> dict:
    return {
        "email_enabled":      True,
        "sms_enabled":        True,
        "notify_match_start": True,
        "notify_checkpoints": [cp for cp, _ in _ALL_CHECKPOINTS],
    }


def _render_page(prefs: dict, saved: bool = False) -> str:
    enabled_cps = set(prefs.get("notify_checkpoints") or [])

    def ck(val: bool) -> str:
        return ' checked' if val else ''

    def cp_row(cp_id: str, label: str) -> str:
        chk = ck(cp_id in enabled_cps)
        return (
            f'<label class="pref-row">'
            f'<input type="checkbox" name="notify_checkpoints" value="{escape(cp_id)}"{chk}> '
            f'{escape(label)}'
            f'</label>'
        )

    cp_rows = "\n".join(cp_row(cp_id, label) for cp_id, label in _ALL_CHECKPOINTS)
    saved_banner = (
        '<div class="banner-ok">✓ Settings saved.</div>' if saved else ""
    )
    updated = prefs.get("updated_utc", "")

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Notification Settings</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 30px; max-width: 600px; }}
    h1   {{ font-size: 22px; margin-bottom: 6px; }}
    .subtitle {{ color:#666; font-size:13px; margin-bottom:24px; }}
    .section  {{ margin-bottom: 24px; }}
    .section h2 {{ font-size:16px; margin-bottom:10px; border-bottom:1px solid #ddd; padding-bottom:4px; }}
    .pref-row {{ display:block; margin-bottom:8px; cursor:pointer; }}
    .pref-row input {{ margin-right:8px; }}
    .banner-ok {{ background:#d4edda; border:1px solid #c3e6cb; color:#155724; padding:10px 14px;
                  border-radius:4px; margin-bottom:18px; }}
    button {{ background:#0052a3; color:white; border:none; padding:10px 22px;
              font-size:15px; border-radius:4px; cursor:pointer; margin-top:10px; }}
    button:hover {{ background:#003d7a; }}
    .back {{ margin-top:18px; font-size:13px; }}
    .muted {{ color:#888; font-size:12px; margin-top:6px; }}
  </style>
</head>
<body>
  <h1>Notification Settings</h1>
  <p class="subtitle">Configure when to receive email and SMS alerts for live cricket matches.</p>

  {saved_banner}

  <form method="POST" action="/api/notification/settings">
    <div class="section">
      <h2>Channels</h2>
      <label class="pref-row">
        <input type="checkbox" name="email_enabled"{ck(prefs.get('email_enabled', True))}> Enable email notifications
      </label>
      <label class="pref-row">
        <input type="checkbox" name="sms_enabled"{ck(prefs.get('sms_enabled', True))}> Enable SMS notifications
      </label>
    </div>

    <div class="section">
      <h2>Match start alert</h2>
      <label class="pref-row">
        <input type="checkbox" name="notify_match_start"{ck(prefs.get('notify_match_start', True))}>
        Notify when a match first appears in the live feed (teams, league, current odds)
      </label>
    </div>

    <div class="section">
      <h2>Win predictor checkpoint alerts</h2>
      <p class="muted">You'll be notified once per checkpoint, the first time a prediction is generated for it.</p>
      {cp_rows}
    </div>

    <button type="submit">Save settings</button>
  </form>

  {"<p class='muted'>Last saved: " + escape(updated) + "</p>" if updated else ""}
  <p class="back"><a href="/api/live/view">← Back to live matches</a></p>
</body>
</html>"""


def view_notification_settings_get(req: func.HttpRequest) -> func.HttpResponse:
    gold  = get_named_container_client("gold")
    prefs = download_json(gold, _PREFS_BLOB) or _default_prefs()
    return func.HttpResponse(_render_page(prefs), mimetype="text/html")


def view_notification_settings_post(req: func.HttpRequest) -> func.HttpResponse:
    gold = get_named_container_client("gold")

    raw_body = req.get_body().decode("utf-8", errors="replace")
    params   = urllib.parse.parse_qs(raw_body, keep_blank_values=False)

    def _flag(key: str) -> bool:
        return key in params

    selected_cps = [
        cp_id for cp_id, _ in _ALL_CHECKPOINTS
        if cp_id in params.get("notify_checkpoints", [])
    ]

    prefs = {
        "email_enabled":      _flag("email_enabled"),
        "sms_enabled":        _flag("sms_enabled"),
        "notify_match_start": _flag("notify_match_start"),
        "notify_checkpoints": selected_cps,
        "updated_utc":        datetime.now(timezone.utc).isoformat(),
    }

    try:
        upload_json(gold, _PREFS_BLOB, prefs, overwrite=True)
    except Exception as exc:
        logging.error(f"notification_settings: failed to save prefs: {exc}")

    return func.HttpResponse(_render_page(prefs, saved=True), mimetype="text/html")
