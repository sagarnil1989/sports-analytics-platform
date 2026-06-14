# League Management

## Opt-In Model

Capture is **opt-in by league**. Any league not in the allowed list is silently skipped when `capture_cricket_prematch_odds` runs. The allowed list is stored at:

```
gold/cricket/config/league_preferences.json
```

Format:
```json
{
  "allowed_league_ids": ["26431", "28769", "..."],
  "updated_at_utc": "2026-05-20T10:00:00+00:00"
}
```

This means:
- `/v3/events/upcoming` fetches **all leagues** unfiltered — stored in bronze
- `capture_cricket_prematch_odds` reads the upcoming control file and **only processes allowed leagues**
- Inplay bronze capture (`capture_cricket_inplay_snapshot`) is **not filtered by league** — it captures everything that `/v3/events/inplay` returns

---

## Management Page

```
https://func-ramanuj-display.azurewebsites.net/api/mgmt/leagues/view
```

Shows all known leagues with toggle switches. Toggle a league on to start capturing its prematch odds.

**Row colours:**
| Colour | Meaning |
|---|---|
| Green | Allowed — prematch capture active |
| Yellow + NEW badge | Seen in upcoming API but never captured — newly discovered league |
| White | Previously seen but currently disabled |

**Sort order:** Allowed leagues first, then new/upcoming-only, then disabled — within each group sorted by most recent match date descending.

---

## League Discovery

The page is computed live on every request. `collect_known_leagues()` reads from six sources:

| Source | Blob | Updated by | Frequency |
|---|---|---|---|
| `live` | `gold/cricket/matches/latest/index.json` | `discover_cricket_inplay` | Every 5s |
| `live_leagues` | `gold/cricket/leagues/index.json` | `discover_cricket_inplay` | Every 5s |
| `prematch` | `gold/cricket/prematch/latest/index.json` | `capture_cricket_prematch_odds` + gold build | Hourly |
| `prematch_leagues` | `gold/cricket/prematch/leagues/index.json` | Same | Hourly |
| `ended` | `bronze/cricket/ended/latest/index.json` | ADF `pl_build_ended_match` Activity 3 | Daily |
| `innings_tracker` | `gold/cricket/innings_tracker/index.json` | ADF gold rebuild | Daily |
| **`upcoming`** | `bronze/betsapi/control/upcoming_cricket/latest.json` | `discover_cricket_upcoming` | **Hourly** |

The `upcoming` source is key for discovery: a league starting tomorrow will appear on the management page (with a **NEW** badge) within one hour of the upcoming API returning it — before any match goes live. Toggle it on to start capturing prematch odds for that league immediately.

**Without this:** a new league would only appear on the page after a match went live (captured via inplay) or after a match ended (appeared in the ended index). You would have no prematch data for it.

---

## Toggling a League

On the management page, click the toggle switch next to any league. This calls:

```
POST /api/mgmt/leagues/toggle
Body: { "league_id": "26431", "league_name": "Indian Premier League", "enabled": true }
```

The change takes effect on the next run of `capture_cricket_prematch_odds` (hourly).

---

## Blocked Events

Individual event IDs can be blocked from appearing in the ended view (without disabling the whole league):

```
gold/cricket/config/blocked_event_ids.json
```

Blocked events are skipped by `discover_cricket_ended` (Activity 3 of `pl_build_ended_match`). There is no UI for this — edit the blob directly via Azure Storage Explorer or the Azure Portal.
