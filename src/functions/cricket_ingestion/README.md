# Cricket Ingestion Azure Functions

This Function App implements Option B:

- `/v1/bet365/inplay_filter?sport_id=3` discovers live cricket Bet365 IDs/FI.
- `/v3/events/inplay?sport_id=3` gets cleaner match state.
- `/v1/bet365/event?FI=<FI>&stats=1` gets Bet365 odds/markets.
- Raw responses are stored together under one `snapshot_id` in ADLS bronze.

## Local setup

```bash
cd src/functions/cricket_ingestion
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp local.settings.template.json local.settings.json
```

Edit `local.settings.json` and fill:

- `AzureWebJobsStorage`
- `DATA_STORAGE_CONNECTION_STRING`
- `BETS_API_TOKEN`

Run locally:

```bash
func start
```

## Deploy

```bash
func azure functionapp publish func-ramanuj
```

Then add the same settings in Azure Portal:

Function App → Settings → Environment variables

## Expected bronze paths

```text
bronze/betsapi/inplay_filter/sport_id=3/year=YYYY/month=MM/day=DD/hour=HH/...
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=<id>/fi=<fi>/snapshot_id=<timestamp>/events_inplay.json
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=<id>/fi=<fi>/snapshot_id=<timestamp>/bet365_event.json
bronze/betsapi/control/active_inplay_fi/latest.json
```

## API budget

Default configuration is conservative:

- inplay filter every 15 sec = 240 calls/hour
- inplay snapshot every 5 sec for max 1 match:
  - events/inplay = 720 calls/hour
  - bet365/event = 720 calls/hour

Total for 1 live match ≈ 1680 calls/hour, under your 1800/hour limit.
