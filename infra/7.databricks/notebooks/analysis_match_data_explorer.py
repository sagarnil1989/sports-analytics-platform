# Databricks notebook: analysis_match_data_explorer
# Full decoded match breakdown: batting scorecard, bowling scorecard,
# bookmaker line movement, win probability, ball analysis, chase data.
# Uses gold tracker rows + decoded silver team_scores (S6/S7/S8 fields).

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob", "matplotlib"], check=True)

# COMMAND ----------

dbutils.widgets.text("event_id", "11658818", "Event ID")
EVENT_ID = dbutils.widgets.get("event_id").strip()
print(f"Event ID: {EVENT_ID}")

# COMMAND ----------

import json, re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
silver = svc.get_container_client("silver")
gold   = svc.get_container_client("gold")

def _dl(container, path):
    try:
        return json.loads(container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

# ── Decoder helpers ────────────────────────────────────────────────────────────

def decode_s6(s6_raw):
    """S6 = 'striker:runs:balls#non_striker:runs:balls' — batting team only."""
    batsmen = []
    for part in str(s6_raw or "").split("#"):
        bits = part.split(":")
        if len(bits) >= 3 and bits[0].strip():
            try:
                batsmen.append({"name": bits[0].strip(), "runs": int(bits[1]), "balls": int(bits[2])})
            except (ValueError, IndexError):
                pass
    return batsmen

def decode_s8(s8_raw):
    """S8 = 'over_num#bowler_name#runs#wickets' — previous over summary (bowling team)."""
    parts = str(s8_raw or "").split("#")
    if len(parts) >= 4 and parts[1].strip():
        try:
            return {"over_num": int(parts[0]), "bowler": parts[1].strip(),
                    "runs": int(parts[2]), "wickets": int(parts[3])}
        except (ValueError, IndexError):
            pass
    return None

def decode_s7_bowler(s7_raw):
    """S7 on bowling team = 'Name#...' — current bowler name."""
    name = str(s7_raw or "").split("#")[0].strip()
    return name or None

def decode_s7_striker(s7_raw):
    """S7 on batting team = 'Name###' — actual striker name."""
    name = str(s7_raw or "").split("#")[0].strip()
    return name or None

def looks_like_id(name):
    """True if name is a numeric Bet365 player ID e.g. '315969]'."""
    return bool(re.match(r'^\d+\]?$', str(name or "").strip()))

def parse_ball_window(bw):
    """Convert ball_window list to dot/boundary/wicket counts."""
    dots = boundaries_4 = boundaries_6 = wickets = extras = 0
    for b in (bw or []):
        s = str(b).lower()
        if s == "w":
            wickets += 1
        elif "wd" in s or "nb" in s or "lb" in s or "b" in s:
            extras += 1
        elif s == "4":
            boundaries_4 += 1
        elif s == "6":
            boundaries_6 += 1
        elif s == "0":
            dots += 1
    return {"dots": dots, "fours": boundaries_4, "sixes": boundaries_6, "wickets": wickets, "extras": extras}

def silver_team_scores_path(event_id, snapshot_id):
    try:
        dt = datetime.strptime(snapshot_id, "%Y%m%dT%H%M%SZ")
        return (f"cricket/inplay/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
                f"/hour={dt.hour:02d}/event_id={event_id}/snapshot_id={snapshot_id}/team_scores.json")
    except Exception:
        return None

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 1 — MATCH HEADER (from gold tracker)
# ═══════════════════════════════════════════════════════════════════

tracker = (_dl(gold, f"cricket/innings_tracker/event_id={EVENT_ID}/innings_1_from_silver.json")
           or _dl(gold, f"cricket/innings_tracker/event_id={EVENT_ID}/innings_1.json"))

if not tracker:
    dbutils.notebook.exit(f"No gold tracker found for event_id={EVENT_ID}")

stadium = tracker.get("stadium_data") or {}
venue_str = stadium.get("name") or tracker.get("venue") or "Unknown"
city_str  = stadium.get("city") or ""
venue_full = ", ".join(p for p in [venue_str, city_str] if p)

def _is_transition_row(r):
    """True for innings-break ghost rows: innings=2 assigned but PG reset to 0.0 while score
    still holds the 1st innings total. A genuine 2nd innings snapshot at over=0.0 must have score=0."""
    if r.get("innings") != 2 or not r.get("score"):
        return False
    try:
        parts = str(r.get("over") or "0").split(".")
        return int(parts[0]) == 0 and (int(parts[1]) if len(parts) > 1 else 0) == 0
    except Exception:
        return False

rows = [r for r in tracker.get("rows", []) if not _is_transition_row(r)]
innings1 = [r for r in rows if r.get("innings") == 1]
innings2 = [r for r in rows if r.get("innings") == 2]

print("=" * 65)
print(f"  {tracker.get('match_name', 'Unknown')}")
print("=" * 65)
print(f"  League      : {tracker.get('league_name')}")
print(f"  Home        : {tracker.get('home_team_name')}")
print(f"  Away        : {tracker.get('away_team_name')}")
print(f"  Date        : {tracker.get('match_date_utc')}")
print(f"  Venue       : {venue_full}")
print(f"  1st innings : {innings1[-1].get('score') if innings1 else '?'}/{innings1[-1].get('wickets') if innings1 else '?'}  ({len(innings1)} data points)")
print(f"  2nd innings : {innings2[-1].get('score') if innings2 else '?'}/{innings2[-1].get('wickets') if innings2 else '?'}  ({len(innings2)} data points)" if innings2 else "  2nd innings : not yet")
print(f"  Actual total: {tracker.get('actual_total')}")
print(f"  Outcome     : {tracker.get('outcome')}  (actual vs bookmaker final line)")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 2 — LOAD SILVER team_scores FOR EACH GOLD ROW
# Decoded: batsmen (S6), current bowler (S7), previous over (S8)
# ═══════════════════════════════════════════════════════════════════

def load_silver_decoded(row):
    snap_id = row.get("snapshot_id")
    path = silver_team_scores_path(EVENT_ID, snap_id)
    if not path:
        return snap_id, None
    doc = _dl(silver, path)
    if not doc:
        return snap_id, None

    silver_rows = doc.get("rows", [])
    batting, bowling = None, None

    # Identify batting team: prefer PI="1", fallback to non-empty S6
    for r in silver_rows:
        raw_pi = str((r.get("raw") or {}).get("PI") or r.get("pi") or "")
        if raw_pi == "1":
            batting = r
        elif raw_pi == "0":
            bowling = r
    if batting is None:
        batting = next((r for r in silver_rows if r.get("s6")), None)
    if bowling is None:
        bowling = next((r for r in silver_rows if r != batting), None)

    batsmen_raw    = decode_s6((batting or {}).get("s6"))
    current_bowler = decode_s7_bowler((bowling or {}).get("s7"))
    prev_over      = decode_s8((bowling or {}).get("s8"))

    # Decode full S7 bowling stats — runs/wickets are CUMULATIVE for the innings
    # Used to reconstruct the last over (S8 misses it since no subsequent snapshot carries it)
    s7b_parts = str((bowling or {}).get("s7") or "").split("#")
    current_bowler_full = None
    if s7b_parts and s7b_parts[0].strip():
        try:
            current_bowler_full = {
                "name": s7b_parts[0].strip(),
                "over_num": int(s7b_parts[1]) if len(s7b_parts) > 1 and s7b_parts[1].strip().isdigit() else None,
                "balls_in_over": int(s7b_parts[2]) if len(s7b_parts) > 2 and s7b_parts[2].strip().isdigit() else None,
                "runs_cumulative": int(s7b_parts[3]) if len(s7b_parts) > 3 and s7b_parts[3].strip().isdigit() else None,
                "wickets_cumulative": int(s7b_parts[4]) if len(s7b_parts) > 4 and s7b_parts[4].strip().isdigit() else None,
            }
        except Exception:
            current_bowler_full = {"name": s7b_parts[0].strip()}

    # S7 on batting team = "striker_name###" — real name even when S6 has IDs
    striker_real_name = decode_s7_striker((batting or {}).get("s7"))

    # Build id → real name mapping entry from this snapshot
    id_map_entry = {}
    if striker_real_name and batsmen_raw and looks_like_id(batsmen_raw[0].get("name")):
        id_map_entry[batsmen_raw[0]["name"]] = striker_real_name

    # Apply override for striker
    batsmen = [dict(b) for b in batsmen_raw]
    if striker_real_name and batsmen and looks_like_id(batsmen[0].get("name")):
        batsmen[0]["name"] = striker_real_name

    return snap_id, {
        "batsmen": batsmen,
        "current_bowler": current_bowler,
        "current_bowler_full": current_bowler_full,
        "prev_over": prev_over,
        "batting_team_name": (batting or {}).get("name"),
        "id_map_entry": id_map_entry,
    }

print("Loading silver team_scores for all gold rows...")
silver_by_snap = {}
with ThreadPoolExecutor(max_workers=64) as ex:
    futs = {ex.submit(load_silver_decoded, r): r for r in rows}
    for fut in as_completed(futs):
        snap_id, decoded = fut.result()
        if decoded:
            silver_by_snap[snap_id] = decoded

print(f"Loaded silver data for {len(silver_by_snap)}/{len(rows)} rows")

# Build global ID → real name map from all snapshots (non-strikers get resolved when they later become striker)
id_to_name = {}
for sv in silver_by_snap.values():
    id_to_name.update(sv.get("id_map_entry") or {})
print(f"Resolved {len(id_to_name)} player ID → name mappings")

def resolve_name(raw_name):
    """Map Bet365 numeric player ID to real name if known, else return as-is."""
    return id_to_name.get(str(raw_name or "").strip(), str(raw_name or "").strip())

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — FULL OVER-BY-OVER TIMELINE
# ═══════════════════════════════════════════════════════════════════

timeline_rows = []
for r in rows:
    snap_id = r.get("snapshot_id")
    sv = silver_by_snap.get(snap_id) or {}
    batsmen = sv.get("batsmen") or []
    striker     = batsmen[0] if len(batsmen) > 0 else {}
    non_striker = batsmen[1] if len(batsmen) > 1 else {}

    bw = parse_ball_window(r.get("ball_window"))
    bw_str = " ".join(str(b) for b in (r.get("ball_window") or []))

    # Run rate
    over_float = None
    try:
        ov = str(r.get("over") or "0")
        parts = ov.split(".")
        completed = int(parts[0])
        balls_in  = int(parts[1]) if len(parts) > 1 else 0
        total_balls = completed * 6 + balls_in
        over_float = total_balls / 6
    except Exception:
        pass

    score = r.get("score") or 0
    rr = round(score / over_float, 2) if over_float and over_float > 0 else None

    # Required run rate (2nd innings)
    target = None
    rrr = None
    if r.get("innings") == 2:
        for gold_r in rows:
            if gold_r.get("innings") == 1:
                target_candidate = gold_r.get("score")
        if innings1:
            target = (innings1[-1].get("score") or 0) + 1
        if target and over_float is not None:
            balls_remaining = 120 - (over_float * 6)
            runs_needed = target - score
            rrr = round((runs_needed / balls_remaining) * 6, 2) if balls_remaining > 0 else None

    timeline_rows.append({
        "inn":        str(r.get("innings") or ""),
        "over":       str(r.get("over") or ""),
        "score":      str(score),
        "wkts":       str(r.get("wickets") or "0"),
        "rr":         str(rr) if rr else "",
        "rrr":        str(rrr) if rrr else "",
        "striker":    f"{resolve_name(striker.get('name',''))} {striker.get('runs','')}" + (f"({striker.get('balls','')})" if striker.get('balls') is not None else ""),
        "non_striker":f"{resolve_name(non_striker.get('name',''))} {non_striker.get('runs','')}" + (f"({non_striker.get('balls','')})" if non_striker.get('balls') is not None else ""),
        "bowler":     sv.get("current_bowler") or "",
        "line":       str(r.get("predicted_total") or ""),
        "ov_odds":    str(r.get("over_odds_at_line") or ""),
        "un_odds":    str(r.get("under_odds_at_line") or ""),
        "bat_odds":   str(round(r.get("batting_team_odds") or 0, 3)) if r.get("batting_team_odds") else "",
        "bowl_odds":  str(round(r.get("bowling_team_odds") or 0, 3)) if r.get("bowling_team_odds") else "",
        "balls":      bw_str,
    })

print("── Full over-by-over timeline ──")
tdf = pd.DataFrame(timeline_rows).astype(str)
display(spark.createDataFrame(tdf))

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — BATTING SCORECARD
# Track each batsman entry/dismissal across gold rows
# ═══════════════════════════════════════════════════════════════════

def build_batting_scorecard(gold_rows, sv_map, innings_num):
    inn_rows = [r for r in gold_rows if r.get("innings") == innings_num]
    if not inn_rows:
        return []

    seen = {}      # resolved_name → {entry_over, last_runs, last_balls}
    dismissed = []
    prev_names = set()

    for r in inn_rows:
        snap_id = r.get("snapshot_id")
        sv = sv_map.get(snap_id) or {}
        batsmen = sv.get("batsmen") or []
        # Resolve IDs to real names before any set operations
        curr_names = {resolve_name(b["name"]) for b in batsmen if b.get("name")}

        for name in curr_names - prev_names:
            seen[name] = {"entry_over": r.get("over"), "last_runs": 0, "last_balls": 0}

        for b in batsmen:
            name = resolve_name(b.get("name"))
            if name and name in seen:
                seen[name]["last_runs"]  = b.get("runs", 0)
                seen[name]["last_balls"] = b.get("balls", 0)

        for name in prev_names - curr_names:
            if name in seen:
                e = seen.pop(name)
                sr = round(e["last_runs"] / e["last_balls"] * 100, 1) if e["last_balls"] > 0 else 0
                dismissed.append({
                    "batsman": name, "runs": e["last_runs"], "balls": e["last_balls"],
                    "SR": sr, "entry_over": e["entry_over"], "status": "dismissed",
                })

        prev_names = curr_names

    for name, e in seen.items():
        sr = round(e["last_runs"] / e["last_balls"] * 100, 1) if e["last_balls"] > 0 else 0
        dismissed.append({
            "batsman": name, "runs": e["last_runs"], "balls": e["last_balls"],
            "SR": sr, "entry_over": e["entry_over"], "status": "not out",
        })

    def _ov_sort(ov_str):
        try:
            p = str(ov_str or "0").split(".")
            return int(p[0]) * 6 + (int(p[1]) if len(p) > 1 else 0)
        except Exception:
            return 0
    return sorted(dismissed, key=lambda x: _ov_sort(x.get("entry_over")))

for inn_num, label in [(1, "1st Innings"), (2, "2nd Innings")]:
    sc = build_batting_scorecard(rows, silver_by_snap, inn_num)
    if not sc:
        continue
    print(f"── Batting Scorecard — {label} ──")
    bdf = pd.DataFrame([{**r, "runs": str(r["runs"]), "balls": str(r["balls"]), "SR": str(r["SR"])} for r in sc]).astype(str)
    display(spark.createDataFrame(bdf))

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — BOWLING SCORECARD
# Reconstruct from S8 (previous over summaries) across all rows
# ═══════════════════════════════════════════════════════════════════

def build_bowling_scorecard(gold_rows, sv_map, innings_num):
    inn_rows = [r for r in gold_rows if r.get("innings") == innings_num]
    overs_by_num = {}  # over_num → {bowler, runs, wickets}

    def row_over_int(r):
        try:
            return int(str(r.get("over") or "0").split(".")[0])
        except Exception:
            return 0

    # Collect S8 from within this innings.
    # Guard: skip S8 where over_num > current row's over integer — that's carryover
    # from the previous innings (e.g. innings-1 final over visible in innings-2 early rows).
    for r in inn_rows:
        snap_id = r.get("snapshot_id")
        sv = sv_map.get(snap_id) or {}
        po = sv.get("prev_over")
        if not (po and po.get("bowler") and po.get("over_num") is not None):
            continue
        if po["over_num"] > row_over_int(r):
            continue  # carryover from previous innings — skip
        overs_by_num[po["over_num"]] = po  # last seen = most accurate

    # Recover the last over of this innings: S8 never appears in the same innings
    # after the final ball. The first rows of innings N+1 still carry S8 = last over
    # of innings N. Read that carryover as a recovery source.
    next_inn_rows = [r for r in gold_rows if r.get("innings") == innings_num + 1]
    existing_max = max(overs_by_num.keys(), default=0)
    for r in next_inn_rows[:20]:
        snap_id = r.get("snapshot_id")
        sv = sv_map.get(snap_id) or {}
        po = sv.get("prev_over")
        if po and po.get("bowler") and po.get("over_num") is not None:
            ov_num = po["over_num"]
            if ov_num not in overs_by_num and ov_num > existing_max:
                overs_by_num[ov_num] = po
            break  # first valid S8 from next innings is the last over of this innings

    if not overs_by_num:
        return [], {}

    # Aggregate by bowler — track first over for sort order
    bowler_agg = {}
    for over_num in sorted(overs_by_num):
        po = overs_by_num[over_num]
        name = po["bowler"]
        if name not in bowler_agg:
            bowler_agg[name] = {"overs": 0, "runs": 0, "wickets": 0, "over_list": [], "first_over": over_num}
        bowler_agg[name]["overs"]   += 1
        bowler_agg[name]["runs"]    += po["runs"]
        bowler_agg[name]["wickets"] += po["wickets"]
        bowler_agg[name]["over_list"].append(f"O{over_num}:{po['runs']}/{po['wickets']}")
        bowler_agg[name]["first_over"] = min(bowler_agg[name]["first_over"], over_num)

    result = []
    for name, d in sorted(bowler_agg.items(), key=lambda x: x[1]["first_over"]):
        eco = round(d["runs"] / d["overs"], 2) if d["overs"] > 0 else 0
        result.append({
            "bowler":    name,
            "overs":     str(d["overs"]),
            "runs":      str(d["runs"]),
            "wickets":   str(d["wickets"]),
            "economy":   str(eco),
            "breakdown": "  ".join(d["over_list"]),
        })
    return result, overs_by_num

for inn_num, label in [(1, "1st Innings"), (2, "2nd Innings")]:
    sc, overs_by_num = build_bowling_scorecard(rows, silver_by_snap, inn_num)
    inn_rows_for_inn = [r for r in rows if r.get("innings") == inn_num]
    if not sc:
        continue

    # ── Over-by-over timeline ──
    print(f"── Over-by-over — {label} ──")
    ov_table = []
    for ov_num in sorted(overs_by_num):
        po = overs_by_num[ov_num]
        bw = get_over_end_window(inn_rows_for_inn, ov_num)
        balls_str = " ".join(str(b) for b in bw) if bw else ""
        ov_table.append({
            "over":    str(ov_num),
            "bowler":  po["bowler"],
            "runs":    str(po["runs"]),
            "wickets": str(po["wickets"]),
            "balls":   balls_str,
        })
    if ov_table:
        display(spark.createDataFrame(pd.DataFrame(ov_table).astype(str)))

    print(f"── Bowling Scorecard — {label} ──")
    display(spark.createDataFrame(pd.DataFrame(sc).astype(str)))

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 6 — BOOKMAKER LINE & WIN PROBABILITY CHARTS
# ═══════════════════════════════════════════════════════════════════

def make_float(v):
    try: return float(v)
    except: return None

fig, axes = plt.subplots(2, 2, figsize=(18, 10))

for inn_num, col_idx in [(1, 0), (2, 1)]:
    inn_rows = [r for r in rows if r.get("innings") == inn_num]
    if not inn_rows:
        continue

    overs      = [make_float(str(r.get("over") or "0").replace(".", "") and
                  (lambda p: int(p[0]) + int(p[1])/6 if len(p)>1 else int(p[0]))(str(r.get("over","0")).split("."))) for r in inn_rows]
    # cleaner over calc
    def to_over_float(r):
        try:
            p = str(r.get("over","0")).split(".")
            return int(p[0]) + (int(p[1])/6 if len(p)>1 else 0)
        except: return 0
    overs      = [to_over_float(r) for r in inn_rows]
    lines      = [make_float(r.get("predicted_total")) for r in inn_rows]
    scores     = [make_float(r.get("score")) for r in inn_rows]
    bat_odds   = [make_float(r.get("batting_team_odds")) for r in inn_rows]
    bowl_odds  = [make_float(r.get("bowling_team_odds")) for r in inn_rows]
    over_odds  = [make_float(r.get("over_odds_at_line")) for r in inn_rows]
    under_odds = [make_float(r.get("under_odds_at_line")) for r in inn_rows]

    label = f"Innings {inn_num}"
    ax_line = axes[0][col_idx]
    ax_odds = axes[1][col_idx]

    # Top: bookmaker line vs actual score
    valid_line = [(o, l) for o, l in zip(overs, lines) if l is not None]
    if valid_line:
        ox, lx = zip(*valid_line)
        ax_line.plot(ox, lx, "b-", linewidth=1.5, label="Bookmaker line")
    ax_line.plot(overs, scores, "g-", linewidth=1.5, label="Actual score")
    if tracker.get("actual_total") and inn_num == 1:
        ax_line.axhline(tracker["actual_total"], color="red", linestyle="--", alpha=0.5, label=f"Final {tracker['actual_total']}")
    ax_line.set_title(f"{label} — Bookmaker Line vs Score", fontsize=11)
    ax_line.set_xlabel("Over")
    ax_line.set_ylabel("Runs")
    ax_line.legend(fontsize=8)
    ax_line.grid(True, alpha=0.3)

    # Bottom: win odds
    valid_bat  = [(o, v) for o, v in zip(overs, bat_odds)  if v]
    valid_bowl = [(o, v) for o, v in zip(overs, bowl_odds) if v]
    if valid_bat:
        ax_odds.plot(*zip(*valid_bat),  "b-", linewidth=1.5, label="Batting team odds")
    if valid_bowl:
        ax_odds.plot(*zip(*valid_bowl), "r-", linewidth=1.5, label="Bowling team odds")
    ax_odds.axhline(2.0, color="gray", linestyle="--", alpha=0.4, label="50/50 line")
    ax_odds.set_title(f"{label} — Win Odds Movement", fontsize=11)
    ax_odds.set_xlabel("Over")
    ax_odds.set_ylabel("Decimal odds")
    ax_odds.legend(fontsize=8)
    ax_odds.grid(True, alpha=0.3)

plt.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — PHASE BREAKDOWN
# Powerplay (1-6), Middle (7-15), Death (16-20) — both innings
# Derived from score/wicket deltas between consecutive gold rows.
# ball_window is a rolling 6-ball window and cannot be summed across
# rows — every delivery would be counted 6x. Score deltas are exact.
# ═══════════════════════════════════════════════════════════════════

def get_over_end_window(inn_rows, over_num):
    """Return ball_window from the snapshot when over `over_num` just completed.
    In our data, `over = '{over_num}.0'` means exactly over_num legal overs done
    (PG resets to N=over_num+1, B=0 between overs — always captured during over break).
    Fallback: last row whose over integer part == over_num - 1 (for match-ending overs)."""
    target = f"{over_num}.0"
    for r in inn_rows:
        if str(r.get("over") or "").strip() == target:
            return r.get("ball_window") or []
    # Fallback for last over (no subsequent "N.0" if innings ends mid-over or immediately)
    best_row = None
    for r in inn_rows:
        try:
            ov_int = int(str(r.get("over") or "0").split(".")[0])
            if ov_int == over_num - 1:
                best_row = r
        except Exception:
            pass
    return (best_row.get("ball_window") or []) if best_row else []

def phase_label(over_str):
    try:
        ov = int(str(over_str or "0").split(".")[0])
        if ov < 6:  return "Powerplay (1-6)"
        if ov < 15: return "Middle (7-15)"
        return "Death (16-20)"
    except:
        return "Unknown"

def _over_float(over_str):
    try:
        p = str(over_str or "0").split(".")
        return int(p[0]) + (int(p[1]) / 6 if len(p) > 1 else 0)
    except:
        return 0.0

PHASE_ORDER = ["Powerplay (1-6)", "Middle (7-15)", "Death (16-20)"]

PHASE_OVERS = {
    "Powerplay (1-6)": range(1, 7),
    "Middle (7-15)":   range(7, 16),
    "Death (16-20)":   range(16, 21),
}

def build_phase_breakdown(inn_rows):
    buckets = {p: {
        "start_score": None, "end_score": None,
        "start_wkts": None,  "end_wkts": None,
        "start_over": None,  "end_over": None,
        "fours": 0, "sixes": 0, "dots": 0, "singles": 0, "doubles": 0,
    } for p in PHASE_ORDER}

    # Score / wicket boundaries — from gold row progressions (exact)
    for r in inn_rows:
        phase = phase_label(r.get("over"))
        if phase not in buckets:
            continue
        b = buckets[phase]
        if b["start_score"] is None:
            b["start_score"] = r.get("score") or 0
            b["start_wkts"]  = r.get("wickets") or 0
            b["start_over"]  = r.get("over")
        b["end_score"] = r.get("score") or 0
        b["end_wkts"]  = r.get("wickets") or 0
        b["end_over"]  = r.get("over")

    # Ball-type stats — read ball_window once per over at "N.0" (between-over snapshot)
    # At that moment the window = last 6 deliveries of over N (newest first).
    # For overs with extras (>6 deliveries), the earliest ball(s) may be outside
    # the window, but boundaries almost always fall within the last 6 deliveries.
    for phase, ov_range in PHASE_OVERS.items():
        b = buckets[phase]
        if b["start_score"] is None:
            continue
        for ov_num in ov_range:
            window = get_over_end_window(inn_rows, ov_num)
            for ball in window:
                bs = str(ball).lower().strip()
                if any(x in bs for x in ("wd", "nb", "lb")):
                    pass  # extra — not a legal delivery
                elif bs in ("w", "0"):
                    b["dots"] += 1
                elif bs == "4":
                    b["fours"] += 1
                elif bs == "6":
                    b["sixes"] += 1
                elif bs == "1":
                    b["singles"] += 1
                elif bs == "2":
                    b["doubles"] += 1

    result = []
    for p in PHASE_ORDER:
        b = buckets[p]
        if b["start_score"] is None:
            continue
        runs    = b["end_score"] - b["start_score"]
        wickets = b["end_wkts"]  - b["start_wkts"]
        boundary_runs = b["fours"] * 4 + b["sixes"] * 6
        phase_overs = round(_over_float(b["end_over"]) - _over_float(b["start_over"]), 2)
        rr = round(runs / phase_overs, 2) if phase_overs > 0 else 0
        result.append({
            "phase":          p,
            "score_range":    f"{b['start_score']}/{b['start_wkts']} → {b['end_score']}/{b['end_wkts']}",
            "runs":           str(runs),
            "wickets":        str(wickets),
            "run_rate":       str(rr),
            "4s":             str(b["fours"]),
            "6s":             str(b["sixes"]),
            "boundary_runs":  str(boundary_runs),
            "singles":        str(b["singles"]),
            "doubles":        str(b["doubles"]),
            "dots":           str(b["dots"]),
        })
    return result

for inn_num, label in [(1, "1st Innings"), (2, "2nd Innings")]:
    inn_data = innings1 if inn_num == 1 else innings2
    if not inn_data:
        continue
    ph = build_phase_breakdown(inn_data)
    if not ph:
        continue
    print(f"── Phase breakdown — {label} ──")
    display(spark.createDataFrame(pd.DataFrame(ph).astype(str)))

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 8 — 2nd INNINGS CHASE ANALYSIS
# Run rate vs required rate, win probability movement
# ═══════════════════════════════════════════════════════════════════

if innings2:
    target = (innings1[-1].get("score") or 0) + 1 if innings1 else None
    print(f"── 2nd Innings Chase — Target: {target} ──")

    chase_rows = []
    for r in innings2:
        score = r.get("score") or 0
        ov_str = str(r.get("over") or "0")
        try:
            p = ov_str.split(".")
            balls_done = int(p[0]) * 6 + (int(p[1]) if len(p) > 1 else 0)
        except: balls_done = 0
        balls_left  = 120 - balls_done
        runs_needed = (target - score) if target else None
        crr = round(score / (balls_done / 6), 2) if balls_done > 0 else 0
        rrr = round((runs_needed / balls_left * 6), 2) if (runs_needed is not None and balls_left > 0) else None

        bat_odds = r.get("batting_team_odds")
        win_prob = round(1 / bat_odds * 100, 1) if bat_odds and bat_odds > 0 else None

        chase_rows.append({
            "over":        str(r.get("over")),
            "score":       str(score),
            "wkts":        str(r.get("wickets") or 0),
            "runs_needed": str(runs_needed) if runs_needed else "",
            "balls_left":  str(balls_left),
            "CRR":         str(crr),
            "RRR":         str(rrr) if rrr else "",
            "win_prob_%":  str(win_prob) if win_prob else "",
            "line":        str(r.get("predicted_total") or ""),
        })

    cdf = pd.DataFrame(chase_rows).astype(str)
    display(spark.createDataFrame(cdf))

    # Chart: CRR vs RRR
    fig, ax = plt.subplots(figsize=(14, 5))
    inn2_overs = []
    crr_vals, rrr_vals = [], []
    for cr in chase_rows:
        try:
            p = cr["over"].split(".")
            ov = int(p[0]) + (int(p[1]) / 6 if len(p) > 1 else 0)
            inn2_overs.append(ov)
            crr_vals.append(float(cr["CRR"]) if cr["CRR"] else None)
            rrr_vals.append(float(cr["RRR"]) if cr["RRR"] else None)
        except: pass

    ax.plot(inn2_overs, crr_vals, "g-", linewidth=1.5, label="Current RR")
    ax.plot(inn2_overs, rrr_vals, "r-", linewidth=1.5, label="Required RR")
    ax.fill_between(inn2_overs,
                    [c or 0 for c in crr_vals],
                    [r or 0 for r in rrr_vals],
                    where=[(c or 0) >= (r or 0) for c, r in zip(crr_vals, rrr_vals)],
                    alpha=0.15, color="green", label="Chasing ahead")
    ax.fill_between(inn2_overs,
                    [c or 0 for c in crr_vals],
                    [r or 0 for r in rrr_vals],
                    where=[(c or 0) < (r or 0) for c, r in zip(crr_vals, rrr_vals)],
                    alpha=0.15, color="red", label="Behind target")
    ax.set_title(f"2nd Innings Chase — CRR vs RRR (target {target})", fontsize=12)
    ax.set_xlabel("Over")
    ax.set_ylabel("Run rate")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    display(fig)
    plt.close(fig)

else:
    print("2nd innings not available for this event.")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# SECTION 9 — BOOKMAKER ACCURACY SUMMARY
# Was the market's over/under line right? At what over did it converge?
# ═══════════════════════════════════════════════════════════════════

actual = tracker.get("actual_total")
outcome = tracker.get("outcome")

if actual and innings1:
    print("── Bookmaker line accuracy (1st innings) ──")
    accuracy_rows = []
    for r in innings1:
        line = r.get("predicted_total")
        if not line:
            continue
        diff = round(actual - line, 1)
        accuracy_rows.append({
            "over":       str(r.get("over")),
            "score":      str(r.get("score")),
            "line":       str(line),
            "diff":       str(diff),
            "market_call": "OVER" if diff > 0 else ("UNDER" if diff < 0 else "PUSH"),
            "correct":    "YES" if (diff > 0 and outcome == "over") or (diff < 0 and outcome == "under") else "NO",
        })
    display(spark.createDataFrame(pd.DataFrame(accuracy_rows).astype(str)))
    print(f"\nFinal outcome: actual={actual}, outcome={outcome}")
    correct_count = sum(1 for r in accuracy_rows if r["correct"] == "YES")
    print(f"Market was pointing correctly at {correct_count}/{len(accuracy_rows)} snapshots ({round(correct_count/len(accuracy_rows)*100)}%)")
