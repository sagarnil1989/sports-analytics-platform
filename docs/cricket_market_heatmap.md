# Cricket Betting Market Heatmap — Webpage Documentation

## Overview

A post-match analysis tool that visualises which betting markets were available at every ball of a completed cricket match. Built for analysts and traders who want to review market coverage across the full match timeline.

---

## Page Layout

The page is divided into three zones:

```
┌─────────────────────────────────────────────────────────┐
│                      Top Navigation                      │
├──────────────────────────────────┬──────────────────────┤
│                                  │                      │
│         Heatmap Panel            │   Detail Panel       │
│         (left, main)             │   (right, sidebar)   │
│                                  │                      │
├──────────────────────────────────┴──────────────────────┤
│                      Bottom Stats Bar                    │
└─────────────────────────────────────────────────────────┘
```

---

## Sections

### 1. Top Navigation Bar
Always visible. Contains:
- **Match title** — e.g. India vs Australia · T20I · Eden Gardens
- **Match status badge** — "Match ended" (green)
- **Final scores** — both teams' innings totals
- **Result chip** — e.g. "IND won by 3"

---

### 2. Heatmap Panel (Left)

The primary navigation surface of the page.

#### Innings Tabs
Three tabs to filter the heatmap view:
- `Full match` — all 120 balls across both innings
- `1st innings` — balls 1–60 only
- `2nd innings` — balls 61–120 only

#### Legend
| Colour | Meaning |
|---|---|
| Dark teal | Market was always open (core markets) |
| Mid teal | Market was mostly open |
| Light teal | Market was occasionally open |
| Grey | Market was unavailable at that ball |
| Blue outline | Currently selected ball |

#### The Heatmap Grid
- **Rows** = individual betting markets (e.g. Match winner, Next ball runs, Over total)
- **Columns** = every ball of the match, grouped by over
- **Over headers** are labelled (Ov 1, Ov 2 … Ov 20) — blue for 1st innings, coral for 2nd
- **Ball numbers** (1–6) appear below each over header
- **Wicket balls** are highlighted in green on the ball number row
- **Clicking any cell** selects that ball and loads the detail panel on the right

---

### 3. Detail Panel (Right)

Opens when a heatmap cell is clicked. Shows the full market state at that exact ball.

#### Header
- Ball reference — e.g. "Over 14, Ball 3"
- Innings badge (1st / 2nd)
- Wicket badge (if a wicket fell on that ball)
- Market count — e.g. "8 / 12 markets open"
- Live score snapshot at that ball — e.g. "IND 142/5 · Need 40 off 39"

#### Open Markets Section
Each available market is shown as a card containing:
- Market name
- Market type tag — `Match` / `Ball` / `Over` / `Player`
- All options with their decimal odds
- Favourite option highlighted in green

Market type tags and colours:
| Tag | Colour | Examples |
|---|---|---|
| Match | Blue | Match winner, Top batsman |
| Ball | Green | Next ball runs, Wicket this ball |
| Over | Amber | Over total runs, Wicket in over |
| Player | Purple | Partnership runs, Batsman 50 |

#### Unavailable Markets Section
Lists all markets that were **not open** at that ball as small grey pills — so it's immediately clear what was missing.

---

### 4. Bottom Stats Bar

A single line of match-level summary figures:
- Total balls in match
- Total markets tracked
- Average markets open per ball
- Number of wicket balls in the match

---

## Data Model

Each ball in the match has the following state:

```
Ball {
  index         : int          // 0–119
  innings       : 1 | 2
  over          : int          // 1–20
  ballInOver    : int          // 1–6
  isWicket      : boolean
  score         : string       // e.g. "142/5"
  markets[]     : Market[]     // all markets, with available flag
}

Market {
  name          : string
  tag           : "match" | "ball" | "over" | "player"
  available     : boolean      // at this ball
  options[]     : Option[]
}

Option {
  label         : string
  odds          : float        // decimal odds
}
```

---

## Market Types Tracked

| # | Market | Type | Availability |
|---|---|---|---|
| 1 | Match winner | Match | Always |
| 2 | Next ball runs | Ball | Always |
| 3 | Wicket this ball | Ball | Always |
| 4 | Over total runs | Over | Mostly |
| 5 | Wicket in over | Over | Mostly |
| 6 | Next boundary | Ball | Mostly |
| 7 | Partnership runs | Player | Mostly |
| 8 | Batsman 50 | Player | Occasionally |
| 9 | Top batsman (innings) | Match | Occasionally |
| 10 | Top bowler (innings) | Match | Occasionally |
| 11 | Session runs | Over | Occasionally |
| 12 | Fall of wicket method | Ball | Occasionally |

---

## User Interactions

| Action | Result |
|---|---|
| Click innings tab | Filters heatmap to show only that innings |
| Click a heatmap cell | Highlights cell; loads detail panel for that ball |
| Scroll heatmap horizontally | Navigate across all 120 balls |
| Read detail panel | See all open markets, options, odds, and unavailable markets |

---

## Future Features (Planned)

- **Pick tracker** — click an option in the detail panel to record your pick; saved picks show as won/lost overlaid on the heatmap
- **Market type filter** — filter heatmap rows by Match / Ball / Over / Player tag
- **Export** — download all picks and market data as CSV
- **Search** — jump to a specific over or ball directly
- **Odds movement** — show how a market's odds shifted across the match timeline

---

## Tech Notes

- **Framework:** Single-page app (React or plain HTML/JS)
- **Heatmap rendering:** CSS grid or HTML table with inline-coloured cells
- **State management:** Ball index as the single source of truth; detail panel derives from selected ball
- **Data source:** Post-match feed (static JSON per match); no live data required
- **Odds format:** Decimal odds throughout
