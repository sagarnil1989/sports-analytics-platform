# Ramanuj Cricket Odds Function App

This project collects live cricket match data and betting odds from BetsAPI, stores the raw data in Azure Storage, converts it into easier-to-use files, and exposes simple web pages for browsing leagues, matches, scores, odds history, and current Bet365 markets.

## In simple words

Think of the app like a small data factory with four jobs:

1. **Find live cricket matches**  
   It asks BetsAPI: “Which cricket matches are live now?”

2. **Capture match snapshots**  
   For every live match, it saves the score, teams, players, Bet365 event data, and odds history.

3. **Parse raw data into clean data**  
   The raw BetsAPI response is difficult to read. This step turns it into cleaner JSON files, such as match details, team scores, players, odds history, and current markets.

4. **Build fast website-ready pages**  
   It creates small index files so the website can load quickly without scanning thousands of old files.

## Why we changed the design

Earlier, the page below scanned all historical match files every time it opened:

```text
/api/matches/view
```

That became slow and caused gateway timeouts.

Now the timer job pre-builds small index files:

```text
gold/cricket/matches/latest/index.json
gold/cricket/leagues/index.json
gold/cricket/leagues/{league_id}/matches.json
```

The web pages now read these small files directly, so they should load much faster.

## Public web pages

After deployment, open these URLs in the browser.

### All leagues

```text
https://func-ramanuj.azurewebsites.net/api/leagues/view
```

This shows all leagues/tournaments, for example Indian Premier League.

### All matches

```text
https://func-ramanuj.azurewebsites.net/api/matches/view
```

This shows one row per match with:

- Event ID
- Bet365 FI
- Match name
- League name
- Score
- Current market count
- Current selection count
- Last snapshot time

### Matches for one league

```text
https://func-ramanuj.azurewebsites.net/api/leagues/{league_id}/matches/view
```

Example:

```text
https://func-ramanuj.azurewebsites.net/api/leagues/26431/matches/view
```

### One match page

```text
https://func-ramanuj.azurewebsites.net/api/matches/{event_id}/view
```

Example:

```text
https://func-ramanuj.azurewebsites.net/api/matches/11658858/view
```

This page shows:

- Match name
- Score
- Current Bet365 markets
- Odds history timeline

## Public JSON APIs

These are useful when you want raw JSON instead of HTML pages.

### All leagues JSON

```text
/api/leagues
```

### All matches JSON

```text
/api/matches
```

### Matches for one league JSON

```text
/api/leagues/{league_id}/matches
```

### One match JSON

```text
/api/matches/{event_id}
```

## BetsAPI flow

The intended flow is:

### Step 1: Find live cricket events

```text
/v3/events/inplay?sport_id=3
```

This gives live cricket matches. A typical event contains:

```json
{
  "id": "11658858",
  "sport_id": "3",
  "league": { "id": "26431", "name": "Indian Premier League" },
  "home": { "id": "263041", "name": "Delhi Capitals" },
  "away": { "id": "980409", "name": "Royal Challengers Bengaluru" },
  "ss": "2/1",
  "bet365_id": "193598177"
}
```

Important fields:

- `id` = BetsAPI event ID
- `bet365_id` = Bet365 FI ID
- `ss` = score summary

### Step 2: Get Bet365 current markets

```text
/v1/bet365/event?FI=<bet365_id>&stats=1
```

Example:

```text
/v1/bet365/event?FI=193598177&stats=1
```

This is where Bet365 markets are parsed.

Important Bet365 record types:

- `EV` = event header
- `TE` = team/player/score row
- `MG` = market group, such as Match Winner or Over Runs
- `MA` = market/column under the group, such as Over or Under
- `PA` = actual selection/price row
- `OD` = fractional odds, such as `13/8`

The code converts fractional odds to decimal odds.

Example:

```text
13/8  -> 2.625
1/2   -> 1.5
10/11 -> 1.909
```

### Step 3: Get odds history

```text
/v2/event/odds?event_id=<event_id>
```

This gives the odds timeline. It is useful to see how prices changed as the score changed.

## Storage layout

The app uses a bronze/silver/gold style layout.

### Bronze = raw data

Bronze contains the exact API responses from BetsAPI.

Examples:

```text
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=<event_id>/fi=<fi>/snapshot_id=<snapshot_id>/api_inplay_event_list.json
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=<event_id>/fi=<fi>/snapshot_id=<snapshot_id>/api_live_market_odds.json
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=<event_id>/fi=<fi>/snapshot_id=<snapshot_id>/api_event_odds.json
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=<event_id>/fi=<fi>/snapshot_id=<snapshot_id>/manifest.json
```

See `docs/STORAGE_LAYOUT.md` for the full list of all bronze, silver, and gold files and what each one contains.

### Silver = cleaned data

Silver contains parsed files that are easier to read.

Examples:

```text
silver/cricket/inplay/.../match_state.json
silver/cricket/inplay/.../team_scores.json
silver/cricket/inplay/.../player_entries.json
silver/cricket/inplay/.../market_odds.json
silver/cricket/inplay/.../active_markets.json
```

### Gold = website-ready data

Gold contains fast files used by the HTTP pages.

Examples:

```text
gold/cricket/matches/latest/index.json
gold/cricket/matches/latest/event_id=<event_id>/match_dashboard.json
gold/cricket/leagues/index.json
gold/cricket/leagues/<league_id>/matches.json
```

## Timer functions

The app has four timer functions.

| Function | Runs | What it does |
|---|---:|---|
| `discover_cricket_inplay` | every 15 seconds | Finds live cricket matches from `/v3/events/inplay` |
| `capture_cricket_inplay_snapshot` | every 5 seconds | Captures score, Bet365 event markets, and odds history |
| `parse_cricket_bronze_to_silver` | every 30 seconds | Parses raw bronze data into clean silver files |
| `build_cricket_gold_match_pages` | every 30 seconds | Builds fast gold index files and match pages |

## HTTP functions

The app has eight HTTP routes.

| Route | Purpose |
|---|---|
| `/api/matches` | All latest matches as JSON |
| `/api/matches/view` | All latest matches as an HTML page |
| `/api/matches/{event_id}` | One match as JSON |
| `/api/matches/{event_id}/view` | One match as an HTML page |
| `/api/leagues` | All leagues as JSON |
| `/api/leagues/view` | All leagues as an HTML page |
| `/api/leagues/{league_id}/matches` | Matches for one league as JSON |
| `/api/leagues/{league_id}/matches/view` | Matches for one league as an HTML page |

## Required app settings

Set these in Azure Function App environment variables and in `local.settings.json` for local testing.

```text
AzureWebJobsStorage
DATA_STORAGE_CONNECTION_STRING
BETS_API_TOKEN
BETS_API_BASE_URL=https://api.b365api.com
SPORT_ID=3
FUNCTIONS_WORKER_RUNTIME=python
```

Optional settings:

```text
MAX_LIVE_MATCHES=10
MAX_SILVER_SNAPSHOTS_PER_RUN=50
MAX_GOLD_EVENTS_PER_RUN=20
ENABLE_EVENTS_INPLAY_MATCH_STATE=true
```

## Local setup

```bash
cd src/functions/cricket_ingestion
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp local.settings.template.json local.settings.json
```

Edit `local.settings.json`, then run:

```bash
func start
```

## Deploy

```bash
func azure functionapp publish func-ramanuj
```

## How to check after deployment

### 1. Check functions are listed

In Azure Portal, open:

```text
Function App -> Functions
```

You should see four timer functions and eight HTTP functions.

### 2. Check live pages

Open:

```text
https://func-ramanuj.azurewebsites.net/api/leagues/view
https://func-ramanuj.azurewebsites.net/api/matches/view
```

### 3. Check one match

Open a match from the matches page, or use:

```text
https://func-ramanuj.azurewebsites.net/api/matches/<event_id>/view
```

### 4. Check whether current markets are parsed

Open the JSON page:

```text
https://func-ramanuj.azurewebsites.net/api/matches/<event_id>
```

Look for:

```json
"current_markets": {
  "selection_count": 50,
  "market_count": 10
}
```

If `selection_count` and `market_count` are greater than zero, Bet365 markets are being parsed.

## Important note about current markets

`/v1/bet365/event` does not always return markets for every match at every second. If the raw bronze file has no `MG`, `MA`, or `PA` rows with `OD`, then the current markets page will show zero. In that case, the app is not broken; the API response simply did not contain those markets at that snapshot.

To verify, open the bronze file:

```text
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=<event_id>/fi=<fi>/snapshot_id=<snapshot_id>/api_live_market_odds.json
```

Search for:

```text
"type": "MG"
"type": "PA"
"OD"
```

## API call budget warning

With the default settings:

- discovery every 15 seconds = about 240 calls/hour
- snapshot every 5 seconds = about 720 runs/hour
- each snapshot calls `/v1/bet365/event` and `/v2/event/odds` for each live match

If `MAX_LIVE_MATCHES=10`, calls can become high very quickly. Reduce `MAX_LIVE_MATCHES` or increase the timer interval if you need to stay under an API limit.



---

# New functionality: upcoming matches and prematch odds

You do not always have a live match available for testing. Because of that, the app now also supports **upcoming cricket matches** and **prematch odds**.

## New simple flow

In simple words:

1. **Find upcoming cricket matches**  
   The app calls:

   ```text
   /v3/events/upcoming?sport_id=3
   ```

   This returns future cricket matches. A useful row looks like this:

   ```json
   {
     "id": "11658832",
     "league": {"id": "26431", "name": "Indian Premier League"},
     "home": {"name": "Gujarat Titans"},
     "away": {"name": "Royal Challengers Bengaluru"},
     "bet365_id": "193712857"
   }
   ```

   Here:
   - `id` is our event ID.
   - `bet365_id` is the Bet365 FI value.
   - `league.name` is the tournament.
   - `home.name` and `away.name` are the teams.

2. **Use Bet365 FI to get prematch odds**  
   For each upcoming match that has `bet365_id`, the app calls:

   ```text
   /v4/bet365/prematch?FI=<bet365_id>
   ```

   Example:

   ```text
   /v4/bet365/prematch?FI=193712857
   ```

3. **Flatten the prematch odds**  
   The prematch response is deeply nested. The app converts it into simple rows:

   ```text
   Category        Market                Header     Selection     Odds
   1st_over        1st Over Total Runs   Over       8.5           1.80
   1st_over        1st Over Total Runs   Under      8.5           2.00
   ```

4. **Build fast web pages**  
   The app writes small gold files that the website reads quickly.

## New timer functions

The app now has these extra jobs:

### discover_cricket_upcoming

Runs every 15 minutes.

It calls:

```text
/v3/events/upcoming?sport_id=3
```

It stores:
- raw upcoming API response in bronze
- small latest upcoming control file

Important output:

```text
bronze/betsapi/control/upcoming_cricket/latest.json
```

### capture_cricket_prematch_odds

Runs every 30 minutes.

It reads the upcoming control file, takes the `bet365_id`, and calls:

```text
/v4/bet365/prematch?FI=<bet365_id>
```

It stores one prematch snapshot per match.

Example path:

```text
bronze/betsapi/prematch_snapshot/sport_id=3/event_id=<event_id>/fi=<bet365_id>/snapshot_id=<timestamp>/api_prematch_odds.json
```

### build_cricket_prematch_pages

Runs every 60 minutes.

It reads the bronze prematch snapshots and creates website-ready gold files.

Important output:

```text
gold/cricket/prematch/latest/index.json
gold/cricket/prematch/latest/event_id=<event_id>/prematch_dashboard.json
gold/cricket/prematch/leagues/index.json
gold/cricket/prematch/leagues/<league_id>/matches.json
```

### discover_cricket_ended

Runs every 60 minutes.

It calls:

```text
/v3/events/ended?sport_id=3
```

It creates a latest ended-match index:

```text
gold/cricket/ended/latest/index.json
```

This is the first step for later comparing prematch odds with final results.

## New browser pages

After deployment, open these URLs.

### Upcoming prematch matches

```text
https://func-ramanuj.azurewebsites.net/api/prematch/view
```

This shows:
- Event ID
- Bet365 FI
- Match name
- League
- Start time
- Number of prematch markets
- Number of odds selections

### One prematch match page

```text
https://func-ramanuj.azurewebsites.net/api/prematch/<event_id>/view
```

Example:

```text
https://func-ramanuj.azurewebsites.net/api/prematch/11658832/view
```

This shows all prematch odds in a readable table.

### Prematch leagues

```text
https://func-ramanuj.azurewebsites.net/api/prematch/leagues/view
```

This shows tournaments/leagues with upcoming prematch data.

### Prematch matches for one league

```text
https://func-ramanuj.azurewebsites.net/api/prematch/leagues/<league_id>/matches/view
```

Example:

```text
https://func-ramanuj.azurewebsites.net/api/prematch/leagues/26431/matches/view
```

### Ended cricket matches

```text
https://func-ramanuj.azurewebsites.net/api/ended/view
```

This shows recently ended matches and scores.

## New JSON APIs

These are useful for debugging.

### Prematch index JSON

```text
https://func-ramanuj.azurewebsites.net/api/prematch
```

### One prematch match JSON

```text
https://func-ramanuj.azurewebsites.net/api/prematch/<event_id>
```

### Prematch league index JSON

```text
https://func-ramanuj.azurewebsites.net/api/prematch/leagues
```

### Prematch matches for one league JSON

```text
https://func-ramanuj.azurewebsites.net/api/prematch/leagues/<league_id>/matches
```

### Ended matches JSON

```text
https://func-ramanuj.azurewebsites.net/api/ended
```

## New configuration values

Add these environment variables only if you want to override the defaults.

```text
MAX_UPCOMING_MATCHES=30
UPCOMING_REQUIRE_BET365_ID=true
MAX_PREMATCH_ODDS_PER_RUN=10
MAX_PREMATCH_SNAPSHOTS_PER_RUN=50
MAX_ENDED_MATCHES=50
```

Meaning:

- `MAX_UPCOMING_MATCHES`: how many upcoming matches to keep in the control file.
- `UPCOMING_REQUIRE_BET365_ID`: if true, only keep matches that have `bet365_id`.
- `MAX_PREMATCH_ODDS_PER_RUN`: how many prematch API calls to make per run.
- `MAX_PREMATCH_SNAPSHOTS_PER_RUN`: how many prematch snapshots to turn into gold pages per run.
- `MAX_ENDED_MATCHES`: how many ended matches to keep in the ended index.

## How to test when there are no live matches

Run this after deployment:

```text
https://func-ramanuj.azurewebsites.net/api/prematch/view
```

If data exists, click any match.

Then check:

```text
https://func-ramanuj.azurewebsites.net/api/prematch/<event_id>/view
```

Expected result:

```text
Prematch Markets > 0
Prematch Selections > 0
```

If you see zero markets, check the raw bronze file:

```text
bronze/betsapi/prematch_snapshot/sport_id=3/event_id=<event_id>/fi=<fi>/snapshot_id=<snapshot_id>/api_prematch_odds.json
```

If this file has odds but the page shows zero, the parser needs adjustment.  
If this file has no odds, the API returned no prematch odds for that match.

## Important note about win/loss settlement

The app now stores:
- prematch odds before the match
- ended match result after the match

This is enough to start building settlement logic later.

However, deciding whether **every market** won or lost is not simple because each market has different rules:

- Match Winner
- Total Runs
- 1st Over Runs
- Player Runs
- Wickets
- Method of Dismissal
- Handicap markets

So the current version **captures the data** and shows it clearly.  
The next step is to add settlement rules market by market.
