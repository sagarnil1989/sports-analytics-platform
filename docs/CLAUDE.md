# IPL Live Match JSON Parser — Spec

This project parses live cricket match state from a sports data feed that emits `EV` type JSON objects.
All parsing logic below was derived empirically by cross-referencing JSON snapshots with live Cricbuzz scorecards.

---

## Confirmed Fields

### Match Identity
| Field | Meaning | Parse |
|---|---|---|
| `NA` | Match name | String direct — e.g. `"Gujarat Titans vs RCB - T20"` |
| `CT` | Competition name | String direct — e.g. `"Indian Premier League"` |
| `C1` | Competition ID | String direct |
| `C2` | Match ID | String direct |

### Match State
| Field | Meaning | Parse |
|---|---|---|
| `SS` | Current score | Split on `/` → `[runs, wickets]` e.g. `"83/2"` → runs=83, wickets=2 |
| `S3` | Target (2nd innings only) | Empty string in 1st innings. Number string in 2nd innings e.g. `"156"` |
| `S5` | Total overs in match | Always `"20"` for T20 |
| `FS` | Match started | `"0"` = not started, `"1"` = in progress |
| `EX` | Run chase sentence | Only present in 2nd innings. Format: `"X require Y runs from Z balls"` |

### Innings Detection
- **1st innings**: `S3` is empty string `""`
- **2nd innings**: `S3` has a number value (the target set by 1st innings team)

---

## PG Field — Full Parsing Rule

The `PG` field is the most information-dense field. Format:

```
B1:B2:B3:B4:B5:B6#N:W:B
```

### Ball window `B1:B2:B3:B4:B5:B6`
- A **rolling 6-ball window** of recent deliveries
- **Newest ball is FIRST (leftmost)**, oldest is last (rightmost)
- Each new delivery pushes all balls one position right, oldest drops off
- Ball values:
  - `0`–`6` = runs scored off that ball
  - `W` = wicket (0 runs scored)
  - `1wd` = wide (not a legal delivery)
  - `1lb` = leg bye
  - `1nb` = no ball
- The window spans across over boundaries — the rightmost balls may belong to the previous over

### Suffix `#N:W:B`
| Part | Meaning |
|---|---|
| `N` | Currently IN the Nth over (1-indexed) |
| `W` | Count of wickets visible in the 6-ball window |
| `B` | Count of **legal balls** bowled in the **current over only** (excludes wides, no-balls) |

### Current over calculation
```
current_over = (N - 1).(B)
e.g. #16:6:5 → over 15.5
e.g. #5:1:0  → over 4.0  (over just completed)
e.g. #7:5:3  → over 6.3
```

### Over complete vs in-progress
- `B = 0` → the Nth over is about to start, `(N-1)` overs are fully complete
- `B = 1–5` → currently mid-over
- `B = 6` should not appear — a new `#N` increments when the over completes

### Wides/extras in the window
- Extras like `1wd`, `1lb`, `1nb` appear in the ball window but do **not** increment `B`
- So you may see more than `B` entries from the current over in the window if extras were bowled

---

## EX Field — 2nd Innings Over Verification

When `EX` is present (2nd innings), use it to cross-verify the over:

```
EX = "X require Y runs from Z balls"
balls_bowled = (S5 * 6) - Z        # e.g. (20*6) - 81 = 39
current_over = floor(39/6).(39%6)  # = 6.3
```

This is the most reliable source for exact over in the 2nd innings and can be used to verify `PG` parsing.

---

## Derived Stats

Once you have the core fields parsed, these can be computed:

```js
const [runs, wickets] = SS.split('/').map(Number)
const overs = (N - 1) + (B / 10)           // e.g. 15.5
const ballsBowled = (N - 1) * 6 + B
const ballsRemaining = S5 * 6 - ballsBowled
const currentRR = (runs / ballsBowled) * 6
const target = parseInt(S3) || null
const runsNeeded = target ? target - runs : null
const requiredRR = runsNeeded ? (runsNeeded / ballsRemaining) * 6 : null
const wicketsInHand = 10 - wickets
```

---

## Fields — Empty / Always Zero (Ignore for Now)

These fields have never carried meaningful data in observed IPL T20 samples:

**Always empty string**: `CK`, `LT`, `RI`, `S1`, `S2`, `S4`, `S6`, `TA`, `TU`, `XY`

**Always `"0"`**: `DC`, `FE`, `LB`, `ML`, `SB`, `TD`, `TM`, `TS`, `TT`, `XT`

### Fields to watch (may activate in edge cases)
| Field | Suspected meaning |
|---|---|
| `S1`, `S2`, `S4`, `S6` | Mirror `S3`/`S5` naming — may carry data in rain-affected or super over scenarios |
| `DC` | Likely DLS flag — activate in rain-affected matches |
| `SB` | Likely super over flag |

---

## Fields with Constant Values (Low Priority)

| Field | Observed | Suspected meaning |
|---|---|---|
| `S7` | Always `"1"` | Sport ID (cricket = 1) |
| `T1` | Always `"5"` | Sport/market type |
| `T2` | Always `"2"` | Market subtype |
| `CL` | Always `"3"` | Competition level/category |
| `ED` | Always `"1"` | Event day |
| `EL` | Always `"1"` | Event live flag |
| `MD` | Always `"1"` | Match day |
| `SV` | Always `"1"` | Data stream version |
| `VC` | `"1247"`, `"11206"`, `"21206"` | Changes across matches — possibly volatility or counter, meaning unknown |

---

## Complete Parse Example

Given:
```json
{
  "SS": "83/2",
  "S3": "156",
  "S5": "20",
  "EX": "Gujarat Titans require 73 runs from 81 balls",
  "PG": "4:6:1:1wd:6:6#7:5:3",
  "NA": "Gujarat Titans vs Royal Challengers Bengaluru - T20",
  "CT": "Indian Premier League"
}
```

Parsed output:
```
match:        Gujarat Titans vs RCB
competition:  Indian Premier League
innings:      2nd (S3 present)
score:        83/2
target:       156
over:         6.3  → #7, B=3 → (7-1).3
balls bowled: 39
runs needed:  73
balls left:   81
current RR:   12.77
required RR:  5.41
wickets left: 8
last balls:   4, 6, 1, 1wd, 6, 6  (newest first)
```

---

## Validation Checklist for New Snapshots

1. Parse `#N:W:B` → compute over as `(N-1).B`
2. If 2nd innings: cross-check with `EX` balls remaining → `(120 - Z) / 6`
3. Verify `SS` wickets match context (shouldn't decrease)
4. If `B=0` and ball window has 6 clean values → over boundary confirmed
5. Watch for extras in ball window — they don't count toward `B`
