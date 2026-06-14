# Sports Analytics Platform — Cricket

Collects live and upcoming cricket match data from BetsAPI, stores raw snapshots in Azure Data Lake, processes them through a bronze → silver → gold medallion pipeline, and exposes HTTP pages and JSON APIs for browsing leagues, matches, odds, and innings analysis.

---

## Architecture Overview

```
BetsAPI
  │
  ▼  Azure Function App (timer triggers, every 5s–1hr)
[bronze]  Raw API snapshots — inplay, prematch, upcoming
  │
  ▼  ADF pl_build_ended_match Activity 1 (daily 02:00 CET)
[silver]  Parsed, structured data — ended/inactive matches only
  │
  ▼  ADF pl_build_ended_match Activity 2 (chained)
[gold]    Innings tracker — innings_1_from_silver.json
  │
  ▼  ADF pl_build_ended_match Activity 3 (chained)
[bronze]  Ended match index — cricket/ended/latest/index.json
```

- **Function App** — bronze capture (every 5s) + all HTTP API routes
- **ADF + Databricks** — daily silver/gold processing for ended matches, no timeout ceiling
- **Manual backfill** — `pl_backfill` in ADF Studio for one match or full rebuild

See [`docs/architecture.md`](docs/architecture.md) for design decisions and costs.

---

## Key Pages

| URL | Purpose |
|---|---|
| `/api/matches/view` | Live matches |
| `/api/ended/view` | Ended matches with scores |
| `/api/prematch/view` | Upcoming matches with prematch odds |
| `/api/leagues/view` | All leagues |
| `/api/mgmt/leagues/view` | League management — enable/disable capture, discover new leagues |
| `/api/matches/{event_id}/innings-tracker/view` | Ball-by-ball innings analysis |
| `/api/matches/{event_id}/heatmap` | Market availability heatmap |

---

## Deploy

```bash
# Function App
cd src/functions/cricket_ingestion
func azure functionapp publish func-ramanuj

# Databricks notebooks + secrets
cd infra/7.databricks
terraform apply

# ADF pipelines + triggers
cd infra/8.adf-config
terraform apply
```

---

## Environment Variables

Required:

```
AzureWebJobsStorage
DATA_STORAGE_CONNECTION_STRING
BETS_API_TOKEN
BETS_API_BASE_URL=https://api.b365api.com
SPORT_ID=3
FUNCTIONS_WORKER_RUNTIME=python
```

Optional overrides:

```
MAX_LIVE_MATCHES=10
MAX_UPCOMING_MATCHES=100
UPCOMING_REQUIRE_BET365_ID=true
MAX_PREMATCH_ODDS_PER_RUN=100
MAX_PREMATCH_SNAPSHOTS_PER_RUN=200
```

---

## Local Development

```bash
cd src/functions/cricket_ingestion
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp local.settings.template.json local.settings.json
# edit local.settings.json with real credentials
func start
```

---

## Docs

| File | Contents |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | System design, component breakdown, costs |
| [`docs/pipelines.md`](docs/pipelines.md) | Function App timers + ADF pipelines reference |
| [`docs/STORAGE_LAYOUT.md`](docs/STORAGE_LAYOUT.md) | All bronze/silver/gold paths |
| [`docs/live-to-ended-flow.md`](docs/live-to-ended-flow.md) | How a match goes from live → ended view |
| [`docs/leagues.md`](docs/leagues.md) | League management, opt-in model, discovery |
| [`docs/CLAUDE.md`](docs/CLAUDE.md) | BetsAPI EV record JSON parsing spec |
| [`docs/cricket_market_heatmap.md`](docs/cricket_market_heatmap.md) | Heatmap feature spec |

