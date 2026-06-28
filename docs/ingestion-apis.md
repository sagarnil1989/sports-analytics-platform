# Ingestion APIs (BetsAPI)

All ingestion calls go through `call_betsapi(path, params)` in [util.py](../src/functions/cricket_ingestion/util.py).

- Base URL: `BETS_API_BASE_URL` env var, default `https://api.b365api.com`
- Auth: `token` query param, value from `BETS_API_TOKEN` env var
- Sport: `sport_id` query param, value from `SPORT_ID` env var, default `3` (cricket)

Full URL pattern: `{BETS_API_BASE_URL}{path}?{params}&token={BETS_API_TOKEN}`

## Discovery / prematch — [capture_prematch.py](../src/functions/cricket_ingestion/capture_prematch.py)

| Function | Endpoint | Full URL (default base) | Params |
|---|---|---|---|
| `bronze_discover_cricket_upcoming` | `/v3/events/upcoming` | `https://api.b365api.com/v3/events/upcoming` | `sport_id` |
| `bronze_capture_cricket_prematch_odds` | `/v4/bet365/prematch` | `https://api.b365api.com/v4/bet365/prematch` | `FI` |

## In-play — [capture_inplay.py](../src/functions/cricket_ingestion/capture_inplay.py)

| Function | Endpoint | Full URL (default base) | Params |
|---|---|---|---|
| `bronze_discover_cricket_inplay` | `/v3/events/inplay` | `https://api.b365api.com/v3/events/inplay` | `sport_id` |
| `bronze_capture_cricket_inplay_snapshot` | `/v3/events/inplay` | `https://api.b365api.com/v3/events/inplay` | `sport_id` |
| `bronze_capture_cricket_inplay_snapshot` | `/v1/event/view` | `https://api.b365api.com/v1/event/view` | `event_id` |
| `bronze_capture_cricket_inplay_snapshot` | `/v2/event/odds/summary` | `https://api.b365api.com/v2/event/odds/summary` | `event_id` |
| `bronze_capture_cricket_inplay_snapshot` | `/v2/event/odds` | `https://api.b365api.com/v2/event/odds` | `event_id` |
| `bronze_capture_cricket_inplay_snapshot` | `/v1/event/history` | `https://api.b365api.com/v1/event/history` | `event_id` |
| `bronze_capture_cricket_inplay_snapshot` | `/v1/event/stats_trend` | `https://api.b365api.com/v1/event/stats_trend` | `event_id` |
| `bronze_capture_cricket_inplay_snapshot` | `/v1/bet365/event` | `https://api.b365api.com/v1/bet365/event` | `FI` |
| `bronze_capture_cricket_inplay_snapshot` | `/v1/bet365/event` | `https://api.b365api.com/v1/bet365/event` | `FI`, `stats=1` |
| `bronze_capture_cricket_inplay_snapshot` | `/v1/bet365/event` | `https://api.b365api.com/v1/bet365/event` | `FI`, `lineup=1` |
| `bronze_capture_cricket_inplay_snapshot` | `/v1/bet365/event` | `https://api.b365api.com/v1/bet365/event` | `FI`, `raw=1` |

## Ended — [capture_ended.py](../src/functions/cricket_ingestion/capture_ended.py)

| Function | Endpoint | Full URL (default base) | Params |
|---|---|---|---|
| `bronze_capture_ended_event_view` | `/v1/event/view` | `https://api.b365api.com/v1/event/view` | `event_id` |

## Distinct endpoints called by ingestion (8 unique)

1. `https://api.b365api.com/v3/events/upcoming`
2. `https://api.b365api.com/v4/bet365/prematch`
3. `https://api.b365api.com/v3/events/inplay`
4. `https://api.b365api.com/v1/event/view`
5. `https://api.b365api.com/v2/event/odds/summary`
6. `https://api.b365api.com/v2/event/odds`
7. `https://api.b365api.com/v1/event/history`
8. `https://api.b365api.com/v1/event/stats_trend`
9. `https://api.b365api.com/v1/bet365/event` (called 4x with different params: bare, `stats=1`, `lineup=1`, `raw=1`)

Note: `cricket_display/storage.py` also has its own `call_betsapi`-equivalent for on-demand display lookups, separate from ingestion's bronze capture path.
