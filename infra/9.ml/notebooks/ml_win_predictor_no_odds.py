# Databricks notebook: ml — win predictor no-odds (v1)
#
# Identical to ml_win_predictor but with all market-odds features removed.
# Trains on: scores, run rates, wickets, venue, teams — no bat/bowl odds.
#
# ── What this notebook does ──────────────────────────────────────────────────
# Trains three binary classifiers (who wins the T20 match?) with progressively
# more match information available at prediction time:
#
#   innings1-only   — full innings 1 known, innings 2 not yet started
#   innings2-2over  — innings 1 complete + first 2 overs of the chase
#   innings2-6over  — innings 1 complete + first 6 overs of the chase (powerplay)
#
# ── Algorithm strategy ───────────────────────────────────────────────────────
# Phase 1 (current, < 200 matches):
#   Primary   : XGBoost — handles correlated, interacting features via tree splits
#   Comparison: Random Forest — more stable feature importances on small datasets
#   Feature pruning: after first XGBoost pass, drop zero-importance features,
#                    retrain both algorithms on the pruned set
#
# Phase 2 (future, >= 500 matches):
#   Add LSTM — treats each over as a timestep, learns temporal dependencies
#   (80/0 at over 6 vs 65/3 at over 6 are different histories, not just states)
#   LSTM skeleton is at the bottom of this notebook — activate when ready
#
# ── Features per model ───────────────────────────────────────────────────────
# Categorical  : venue, innings-1 batting team, innings-1 bowling team
# Raw per-over : runs, wickets, bat_odds, bowl_odds at each over boundary
# Composite    : pre-baked interaction features (e.g. runs-per-wicket-in-hand)
#                that capture what the model would otherwise have to discover
# Innings 2    : cumulative chase state (CRR, RRR, rr_diff, runs_needed)
#
# ── Label ────────────────────────────────────────────────────────────────────
# chasing_won = 1 if innings-2 final score > innings-1 final score, else 0
#
# ── Train/test split ─────────────────────────────────────────────────────────
# Cutoff date comes from gold/ml/train_config.json (train_cutoff_date) — the
# single shared cutoff used by every ML notebook in pl_ml_and_hypothesis.
# Train : match_date_utc <  cutoff
# Test  : match_date_utc >= cutoff

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "pyarrow", "pandas",
    "xgboost", "scikit-learn",
], check=True)

# COMMAND ----------

import json, io, pickle, tempfile, os
import numpy as np
import pandas as pd
import mlflow
import mlflow.xgboost
import mlflow.sklearn
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc    = BlobServiceClient.from_connection_string(conn_str)
gold   = svc.get_container_client("gold")
silver = svc.get_container_client("silver")

from datetime import datetime, timedelta, timezone
IMPORTANCE_THRESHOLD  = 0.005   # drop features below 0.5% of total importance

# Load the shared train/test cutoff (same blob used by every ML notebook).
# Falls back to a rolling 7-day window if no config has been saved yet.
try:
    _cfg = json.loads(gold.get_blob_client(
        "ml/train_config.json"
    ).download_blob().readall())
    TRAIN_CUTOFF = _cfg.get("train_cutoff_date") or ""
except Exception:
    TRAIN_CUTOFF = ""

if not TRAIN_CUTOFF:
    TRAIN_CUTOFF = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    print(f"No shared config found — using rolling cutoff: {TRAIN_CUTOFF}")
else:
    print(f"Using shared train_cutoff_date from gold/ml/train_config.json: {TRAIN_CUTOFF}")

def _dl(path):
    try:
        return json.loads(gold.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 1 — Load all T20 gold trackers
# ═══════════════════════════════════════════════════════════════════

blobs = [b.name for b in gold.list_blobs(name_starts_with="event_id=")
         if b.name.endswith("/innings_tracker.json")]

def _dl_tracker(path):
    t = _dl(path)
    if t is not None and not t.get("event_id"):
        ep = path.split("/")[0]  # "event_id=XXXXX"
        if ep.startswith("event_id="):
            t["event_id"] = ep.replace("event_id=", "")
    return t

trackers = []
with ThreadPoolExecutor(max_workers=32) as ex:
    futs = {ex.submit(_dl_tracker, b): b for b in blobs}
    for fut in as_completed(futs):
        t = fut.result()
        if t:
            trackers.append(t)

def _max_over(rows):
    mx = 0
    for r in rows:
        try:
            mx = max(mx, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    return mx

t20_trackers = [t for t in trackers if 15 <= _max_over(t.get("rows") or []) <= 20]

print(f"Total trackers loaded : {len(trackers)}")
print(f"T20 trackers          : {len(t20_trackers)}")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 2 — Feature extraction helpers
# ═══════════════════════════════════════════════════════════════════

def _parse_over(r):
    try:
        s = str(r.get("over") or "0")
        parts = s.split(".")
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    except Exception:
        return 0, 0

def state_after_n_overs(inn_rows, n):
    """
    Best snapshot of {score, wickets, bat_odds, bowl_odds} immediately
    after n complete overs.
    Priority: exact boundary row > last row of previous over > any row up to n.
    """
    exact, prev_ov, any_ov = [], [], []
    for r in inn_rows:
        ov_int, ov_ball = _parse_over(r)
        if ov_int == n and ov_ball == 0:
            exact.append(r)
        if ov_int == n - 1:
            prev_ov.append(r)
        if ov_int <= n:
            any_ov.append(r)
    r = (exact[0] if exact
         else prev_ov[-1] if prev_ov
         else any_ov[-1]  if any_ov
         else None)
    if r is None:
        return None
    return {
        "score":     r.get("score")   or 0,
        "wickets":   r.get("wickets") or 0,
        "bat_odds":  r.get("batting_team_odds"),
        "bowl_odds": r.get("bowling_team_odds"),
    }

def per_over_breakdown(inn_rows, innings_num, max_over):
    """
    For each over k=1..max_over:
      runs scored in that single over
      wickets in that single over
      batting team odds at end of that over   ← market's real-time view
      bowling team odds at end of that over
    """
    result = {}
    prev_score = prev_wickets = 0
    for k in range(1, max_over + 1):
        s = state_after_n_overs(inn_rows, k)
        if s is not None:
            result[f"inn{innings_num}_ov{k}_runs"]      = max(0, s["score"]   - prev_score)
            result[f"inn{innings_num}_ov{k}_wkts"]      = max(0, s["wickets"] - prev_wickets)
            result[f"inn{innings_num}_ov{k}_bat_odds"]  = s["bat_odds"]
            result[f"inn{innings_num}_ov{k}_bowl_odds"] = s["bowl_odds"]
            prev_score   = s["score"]
            prev_wickets = s["wickets"]
        else:
            for suffix in ["_runs", "_wkts", "_bat_odds", "_bowl_odds"]:
                result[f"inn{innings_num}_ov{k}{suffix}"] = None
    return result

def chase_aggregate(inn_rows, inn1_score, over_target):
    """Cumulative chase state at end of over_target."""
    s = state_after_n_overs(inn_rows, over_target)
    if s is None:
        return None
    score      = s["score"]
    wickets    = s["wickets"]
    balls_done = over_target * 6
    balls_left = 120 - balls_done
    runs_needed = (inn1_score + 1) - score
    crr = round(score / (balls_done / 6), 4) if balls_done > 0 else 0
    rrr = round(runs_needed / (balls_left / 6), 4) if balls_left > 0 else 99
    return {
        "score": score, "wickets": wickets,
        "crr": crr, "rrr": rrr,
        "rr_diff": round(crr - rrr, 4),
        "runs_needed": runs_needed,
    }

def _fmt_score_part(part):
    """Format "163/9(20)" → "163/9 (20 ov)", passthrough if no overs."""
    import re as _re
    m = _re.match(r'^(\d+(?:/\d+)?)\s*\((\d+\.?\d*)\)', part.strip())
    if m:
        return f"{m.group(1)} ({m.group(2)} ov)"
    return part.strip()

def parse_score_summary(tracker, inn1_bat_team):
    """
    Parse the authoritative final scores from the tracker's score_summary field.
    This is the same source used by the ended/view page — set from the Bet365
    events API, not from snapshot rows. Format after normalisation: "inn1,inn2"
    where each part is "runs/wickets(overs)" e.g. "196/4(20),87/10(18.3)".

    Returns (inn1_runs, inn2_runs) or (None, None) if unparseable.
    """
    raw = (tracker.get("score_summary_events")
           or tracker.get("score_summary_bet365")
           or tracker.get("score_summary") or "")
    raw = raw.replace("-", ",").strip()
    if not raw or "," not in raw:
        return None, None
    parts = raw.split(",", 1)
    # discover_cricket_ended swaps parts when away team batted first —
    # replicate that logic so inn1 always means the team that batted first
    home = str(tracker.get("home_team_name") or "").strip()
    away = str(tracker.get("away_team_name") or "").strip()
    if inn1_bat_team and away and inn1_bat_team == away:
        parts = [parts[1].strip(), parts[0].strip()]
    try:
        inn1_runs = int(parts[0].split("/")[0].strip())
        inn2_runs = int(parts[1].split("/")[0].strip())
        return inn1_runs, inn2_runs
    except Exception:
        return None, None

def parse_inn1_display(tracker, inn1_bat_team):
    """Return display string for inn1 final score with overs, e.g. '163/9 (20 ov)'."""
    import re as _re
    raw = (tracker.get("score_summary_events")
           or tracker.get("score_summary_bet365")
           or tracker.get("score_summary") or "")
    raw = raw.replace("-", ",").strip()
    if not raw or "," not in raw:
        return None
    parts = raw.split(",", 1)
    home = str(tracker.get("home_team_name") or "").strip()
    away = str(tracker.get("away_team_name") or "").strip()
    if inn1_bat_team and away and inn1_bat_team == away:
        parts = [parts[1].strip(), parts[0].strip()]
    return _fmt_score_part(parts[0])

def parse_inn2_display(tracker, inn1_bat_team):
    """Return display string for inn2 final score with overs, e.g. '87/10 (18.3 ov)'."""
    raw = (tracker.get("score_summary_events")
           or tracker.get("score_summary_bet365")
           or tracker.get("score_summary") or "")
    raw = raw.replace("-", ",").strip()
    if not raw or "," not in raw:
        return None
    parts = raw.split(",", 1)
    home = str(tracker.get("home_team_name") or "").strip()
    away = str(tracker.get("away_team_name") or "").strip()
    if inn1_bat_team and away and inn1_bat_team == away:
        parts = [parts[1].strip(), parts[0].strip()]
    return _fmt_score_part(parts[1])

def batting_dominance(event_id, silver_client):
    """max_SR minus avg_SR across inn1 batsmen (min 5 balls faced)."""
    import re as _re
    prefix = f"silver/cricket/inplay/state/event_id={event_id}/"
    try:
        state_blobs = [b.name for b in silver_client.list_blobs(name_starts_with=prefix)
                       if "/state_1_" in b.name]
    except Exception:
        return None
    if not state_blobs:
        return None

    def _load_state(path):
        try:
            return json.loads(silver_client.get_blob_client(path).download_blob().readall())
        except Exception:
            return None

    player_max = {}  # player_id → (max_runs, balls)
    pattern = _re.compile(r'\[([^\]]+)#(\d+)\]:(\d+):(\d+)')

    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(_load_state, b): b for b in state_blobs}
        for fut in as_completed(futs):
            snap = fut.result()
            if not snap:
                continue
            items = snap if isinstance(snap, list) else [snap]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("TY") != "TE" or item.get("PI") != "1":
                    continue
                s6 = str(item.get("S6") or "")
                for m in pattern.finditer(s6):
                    pid = m.group(2)
                    runs  = int(m.group(3))
                    balls = int(m.group(4))
                    if runs > player_max.get(pid, (-1, 0))[0]:
                        player_max[pid] = (runs, balls)

    batsmen = [(r, b) for r, b in player_max.values() if b >= 5]
    if len(batsmen) < 2:
        return None
    srs = [(r / b) * 100 for r, b in batsmen]
    return round(max(srs) - (sum(srs) / len(srs)), 2)


def match_label(tracker, inn1_bat_team, rows):
    """
    Authoritative win label derived from score_summary (same source as ended/view).
    Falls back to rows[-1] comparison only if score_summary is absent.
    """
    inn1_runs, inn2_runs = parse_score_summary(tracker, inn1_bat_team)
    if inn1_runs is not None and inn2_runs is not None:
        return 1 if inn2_runs > inn1_runs else 0
    # fallback: last snapshot row
    inn1 = [r for r in rows if r.get("innings") == 1]
    inn2 = [r for r in rows if r.get("innings") == 2]
    if not inn1 or not inn2:
        return None
    return 1 if (inn2[-1].get("score") or 0) > (inn1[-1].get("score") or 0) else 0

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 3 — Build one row per match (raw features + phase states)
# ═══════════════════════════════════════════════════════════════════

records = []
skipped = 0

for t in t20_trackers:
    rows       = t.get("rows") or []
    event_id   = str(t.get("event_id") or "")
    date_str   = str(t.get("match_date_utc") or "")[:10]
    match_name = str(t.get("match_name") or "")
    league     = str(t.get("league_name") or "")
    _sd        = t.get("stadium_data") or {}
    venue      = str(t.get("venue") or _sd.get("name") or t.get("stadium") or league or "unknown").strip() or "unknown"

    inn1_rows = [r for r in rows if r.get("innings") == 1]
    inn2_rows = [r for r in rows if r.get("innings") == 2]
    if not inn1_rows or not inn2_rows:
        skipped += 1; continue

    inn1_bat_team = ""
    for r in inn1_rows:
        if r.get("batting_team"):
            inn1_bat_team = str(r["batting_team"]).strip(); break

    label = match_label(t, inn1_bat_team, rows)
    if label is None:
        skipped += 1; continue
    home = str(t.get("home_team_name") or "").strip()
    away = str(t.get("away_team_name") or "").strip()
    inn1_bowl_team = away if inn1_bat_team == home else home

    # Authoritative final scores from score_summary (same source as ended/view page)
    # Falls back to last snapshot row if summary is missing
    auth_inn1, auth_inn2 = parse_score_summary(t, inn1_bat_team)
    inn1_final         = inn1_rows[-1]
    inn1_total_score   = auth_inn1 if auth_inn1 is not None else (inn1_final.get("score") or 0)
    inn1_total_wickets = inn1_final.get("wickets") or 0
    inn1_score_display = parse_inn1_display(t, inn1_bat_team) or str(inn1_total_score)
    inn2_score_display = parse_inn2_display(t, inn1_bat_team) or (str(auth_inn2) if auth_inn2 is not None else None)

    # Phase state snapshots for composite features
    pp_state  = state_after_n_overs(inn1_rows, 6)   # end of powerplay
    mid_state = state_after_n_overs(inn1_rows, 15)  # end of middle overs
    ov1_state = state_after_n_overs(inn1_rows, 1)   # after over 1 (for odds swing)

    inn1_pp_score    = pp_state["score"]   if pp_state else None
    inn1_pp_wickets  = pp_state["wickets"] if pp_state else None
    inn1_ov1_bat_odds= ov1_state["bat_odds"] if ov1_state else None
    inn1_pp_bat_odds = pp_state["bat_odds"]  if pp_state else None

    mid_score   = mid_state["score"]   if mid_state else None
    mid_wickets = mid_state["wickets"] if mid_state else None

    # Per-over breakdowns
    inn1_overs    = per_over_breakdown(inn1_rows, 1, 20)
    inn2_overs_16 = per_over_breakdown(inn2_rows, 2, 16)

    cs2  = chase_aggregate(inn2_rows, inn1_total_score, 2)
    cs6  = chase_aggregate(inn2_rows, inn1_total_score, 6)
    cs10 = chase_aggregate(inn2_rows, inn1_total_score, 10)
    cs16 = chase_aggregate(inn2_rows, inn1_total_score, 16)

    inn2_ov2_state  = state_after_n_overs(inn2_rows, 2)
    inn2_ov6_state  = state_after_n_overs(inn2_rows, 6)
    inn2_ov10_state = state_after_n_overs(inn2_rows, 10)
    inn2_ov16_state = state_after_n_overs(inn2_rows, 16)

    # Last bat_odds of innings 1 (from over 20 data)
    inn1_last_bat_odds = inn1_overs.get("inn1_ov20_bat_odds")

    split = "train" if date_str < TRAIN_CUTOFF else "test"

    is_womens = 1 if ("women" in match_name.lower() or "(w)" in match_name.lower()) else 0

    # Day-of-week of the match
    is_weekend = 0
    try:
        is_weekend = 1 if datetime.strptime(date_str, "%Y-%m-%d").weekday() >= 5 else 0
    except Exception:
        pass

    # ── Venue region ─────────────────────────────────────────────────
    _combined = f"{venue} {league} {match_name}".lower()
    _asia_kws = ["india","pakistan","sri lanka","bangladesh","afghanistan","nepal",
                 "karachi","lahore","mumbai","delhi","chennai","kolkata","bangalore",
                 "hyderabad","ahmedabad","dubai","sharjah","abu dhabi","colombo",
                 "dhaka","chittagong","kandy","pallekele","galle","dambulla","mirpur"]
    _uk_kws   = ["england","london","manchester","birmingham","leeds","bristol",
                 "nottingham","chester","lord","edgbaston","headingley",
                 "trent bridge","old trafford","the oval","county","t20 blast",
                 "hundred","vitality"]
    _aus_kws  = ["australia","sydney","melbourne","brisbane","perth","adelaide",
                 "hobart","darwin","scg","mcg","gabba","waca","big bash","bbl"]
    _wi_kws   = ["west indies","barbados","jamaica","trinidad","guyana","antigua",
                 "st lucia","grenada","dominica","providence","kensington","sabina","cpl"]
    _sa_kws   = ["south africa","johannesburg","cape town","durban","pretoria",
                 "centurion","bloemfontein","newlands","wanderers","supersport","sa20"]
    _nz_kws   = ["new zealand","auckland","christchurch","wellington","hamilton",
                 "dunedin","eden park"]

    if   any(k in _combined for k in _asia_kws): venue_region = "Asia"
    elif any(k in _combined for k in _uk_kws):   venue_region = "UK"
    elif any(k in _combined for k in _aus_kws):  venue_region = "Australia"
    elif any(k in _combined for k in _wi_kws):   venue_region = "West Indies"
    elif any(k in _combined for k in _sa_kws):   venue_region = "South Africa"
    elif any(k in _combined for k in _nz_kws):   venue_region = "New Zealand"
    else:                                          venue_region = "Other"

    # Asia + UAE = subcontinental dew risk
    is_subcontinental = 1 if venue_region == "Asia" else 0

    # Evening match: UTC hour >= 13 → evening in subcontinent (19:30 IST+)
    # or late afternoon in UK/Aus. Good dew-factor proxy.
    is_evening = 0
    try:
        import re as _re_inner
        _hm = _re_inner.search(r'[T ](\d{2}):', str(t.get("match_date_utc") or ""))
        if _hm:
            is_evening = 1 if int(_hm.group(1)) >= 13 else 0
    except Exception:
        pass

    # Tournament stage
    _ml = match_name.lower(); _ll = league.lower()
    if   "final"   in _ml or "final"   in _ll:                  tourney_stage = "Final"
    elif any(k in _ml or k in _ll for k in ["semi","  sf "]):   tourney_stage = "Knockout"
    elif any(k in _ml or k in _ll for k in ["quarter","qf"]):   tourney_stage = "Knockout"
    elif any(k in _ml or k in _ll for k in ["group","super 8","super 12","qualifier",
                                              "league phase"]):  tourney_stage = "Group"
    elif any(k in _ll for k in ["t20 blast","ipl","big bash","cpl","psl","sa20",
                                  "hundred","major league","shpageeza","lpl",
                                  "vitality","international league"]):
                                                                  tourney_stage = "League"
    else:                                                         tourney_stage = "Bilateral"

    rec = {
        "event_id":   event_id,  "match_name": match_name,
        "match_date": date_str,  "split":      split,
        # categorical
        "venue":            venue,
        "inn1_bat_team":    inn1_bat_team  or "unknown",
        "inn1_bowl_team":   inn1_bowl_team or "unknown",
        "venue_region":     venue_region,
        "tournament_stage": tourney_stage,
        # match-level flags
        "is_womens_match":   is_womens,
        "is_weekend_match":  is_weekend,
        "is_evening_match":  is_evening,
        "is_subcontinental": is_subcontinental,

        # innings 1 aggregate
        "inn1_total_score":   inn1_total_score,
        "inn1_total_wickets": inn1_total_wickets,
        "inn1_score_display": inn1_score_display,
        # innings 2 final (authoritative from score_summary)
        "inn2_total_score":   auth_inn2,
        "inn2_score_display": inn2_score_display,
        # phase snapshots (used by composite features)
        "inn1_pp_score":    inn1_pp_score,
        "inn1_pp_wickets":  inn1_pp_wickets,
        "inn1_mid_score":   mid_score,
        "inn1_mid_wickets": mid_wickets,
        "inn1_ov1_bat_odds":  inn1_ov1_bat_odds,
        "inn1_pp_bat_odds":   inn1_pp_bat_odds,
        "inn1_last_bat_odds": inn1_last_bat_odds,
        # inn2 state snapshots
        "inn2_ov2_bat_odds":  inn2_ov2_state["bat_odds"]  if inn2_ov2_state  else None,
        "inn2_ov6_bat_odds":  inn2_ov6_state["bat_odds"]  if inn2_ov6_state  else None,
        "inn2_ov10_bat_odds": inn2_ov10_state["bat_odds"] if inn2_ov10_state else None,
        "inn2_ov16_bat_odds": inn2_ov16_state["bat_odds"] if inn2_ov16_state else None,
        # label
        "chasing_won": label,
    }

    rec.update(inn1_overs)
    rec.update(inn2_overs_16)

    for k in ["score", "wickets", "crr", "rrr", "rr_diff", "runs_needed"]:
        rec[f"inn2_ov2_{k}"]  = cs2[k]  if cs2  else None
        rec[f"inn2_ov6_{k}"]  = cs6[k]  if cs6  else None
        rec[f"inn2_ov10_{k}"] = cs10[k] if cs10 else None
        rec[f"inn2_ov16_{k}"] = cs16[k] if cs16 else None

    rec["inn1_bat_dominance"] = batting_dominance(event_id, silver) if event_id else None

    records.append(rec)

# ── Team form (win rate last 5 matches) and H2H ──────────────────
# Must be computed after all records are collected so each record
# can look back at earlier matches in the same dataset.

_sorted_recs = sorted(records, key=lambda r: r.get("match_date", ""))

# Build per-team result history
_team_hist: dict = {}
for _r in _sorted_recs:
    _bat = _r.get("inn1_bat_team"); _bowl = _r.get("inn1_bowl_team")
    _won = _r.get("chasing_won");   _d    = _r.get("match_date")
    if _won is None or not _d or not _bat or not _bowl:
        continue
    _bat_won = 0 if _won else 1   # chasing_won=0 → batting (inn1) team won
    for _tm, _w in [(_bat, _bat_won), (_bowl, 1 - _bat_won)]:
        if _tm not in _team_hist: _team_hist[_tm] = []
        _team_hist[_tm].append((_d, _w))

# Build head-to-head history: key = frozenset{teamA, teamB} → [(date, bat_team_won)]
_h2h_hist: dict = {}
for _r in _sorted_recs:
    _bat = _r.get("inn1_bat_team"); _bowl = _r.get("inn1_bowl_team")
    _won = _r.get("chasing_won");   _d    = _r.get("match_date")
    if _won is None or not _d or not _bat or not _bowl:
        continue
    _key = frozenset({_bat, _bowl})
    if _key not in _h2h_hist: _h2h_hist[_key] = []
    _h2h_hist[_key].append((_d, _bat, 0 if _won else 1))   # (date, bat_team, bat_won)

def _team_form(team, before_date, n=5):
    hist = [w for d, w in _team_hist.get(team, []) if d < before_date]
    tail = hist[-n:]
    return round(sum(tail) / len(tail), 3) if tail else None

def _h2h_rate(bat, bowl, before_date, n=10):
    key = frozenset({bat, bowl})
    hist = [(d, b, w) for d, b, w in _h2h_hist.get(key, []) if d < before_date]
    tail = hist[-n:]
    if not tail: return None
    bat_wins = sum(1 for _, b, w in tail if b == bat and w == 1)
    return round(bat_wins / len(tail), 3)

for _r in _sorted_recs:
    _d   = _r.get("match_date", "")
    _bat = _r.get("inn1_bat_team",  "")
    _bowl= _r.get("inn1_bowl_team", "")
    _r["bat_team_form5"]  = _team_form(_bat,  _d)
    _r["bowl_team_form5"] = _team_form(_bowl, _d)
    _r["h2h_bat_win_rate"] = _h2h_rate(_bat, _bowl, _d)

records = _sorted_recs   # preserve sorted order

df = pd.DataFrame(records)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 4
#
# These pre-bake feature interactions that XGBoost would otherwise have
# to discover from scratch — helping with small datasets.
#
# Key insight: "80/0 at over 6" is fundamentally different from "65/3
# at over 6" even though the score is higher. The runs-per-wicket-in-hand
# captures this joint effect in one number: 80/(10-0)=8.0 vs 65/(10-3)=9.3.
# That single number encodes both the score and the wickets simultaneously.
# ═══════════════════════════════════════════════════════════════════

def safe_div(a, b, default=0.0):
    try:
        return round(a / b, 4) if b and b != 0 else default
    except Exception:
        return default

# Innings 1 composite features
df["inn1_pp_rp_wkt"]     = df.apply(
    lambda r: safe_div(r["inn1_pp_score"]  or 0, 10 - (r["inn1_pp_wickets"]  or 0)), axis=1)

df["inn1_mid_rp_wkt"]    = df.apply(
    lambda r: safe_div(
        ((r["inn1_mid_score"] or 0) - (r["inn1_pp_score"] or 0)),
        10 - (r["inn1_mid_wickets"] or 0)
    ), axis=1)

# Standalone phase wicket counts — mirror the death overs pattern for all three phases.
# inn1_pp_wickets already in df (used for composites); just needs to be in the feature set.
# inn1_mid_wickets_only = wickets lost in overs 7–15 only (not cumulative).
df["inn1_mid_wickets_only"] = df.apply(
    lambda r: max(0, (r["inn1_mid_wickets"] or 0) - (r["inn1_pp_wickets"] or 0)), axis=1)

df["inn1_death_runs"]    = df.apply(
    lambda r: max(0, (r["inn1_total_score"] or 0) - (r["inn1_mid_score"] or 0)), axis=1)

df["inn1_death_wickets"] = df.apply(
    lambda r: max(0, (r["inn1_total_wickets"] or 0) - (r["inn1_mid_wickets"] or 0)), axis=1)

df["inn1_pressure"]      = df.apply(
    lambda r: safe_div(r["inn1_total_score"] or 0, 10 - (r["inn1_total_wickets"] or 0)), axis=1)

# How much did the market move across the entire innings 1?
# Positive = market swung toward batting team (they batted well)
# Negative = market swung toward fielding team (batting team struggled)
df["inn1_odds_swing_full"] = df.apply(
    lambda r: safe_div(
        (r["inn1_last_bat_odds"] or 0) - (r["inn1_ov1_bat_odds"] or 0), 1, default=None
    ), axis=1)

df["inn1_odds_swing_pp"]   = df.apply(
    lambda r: safe_div(
        (r["inn1_pp_bat_odds"] or 0) - (r["inn1_ov1_bat_odds"] or 0), 1, default=None
    ), axis=1)

# Innings 2 composite features
# How much did the market shift from end of innings 1 to the chase?
df["inn2_ov2_rp_wkt"]      = df.apply(
    lambda r: safe_div(r["inn2_ov2_score"] or 0, 10 - (r["inn2_ov2_wickets"] or 0)), axis=1)
df["inn2_ov2_odds_swing"]  = df.apply(
    lambda r: safe_div((r["inn2_ov2_bat_odds"] or 0) - (r["inn1_last_bat_odds"] or 0), 1, default=None), axis=1)
df["inn2_ov2_runs_needed_per_wkt"] = df.apply(
    lambda r: safe_div(r["inn2_ov2_runs_needed"] or 0, 10 - (r["inn2_ov2_wickets"] or 0)), axis=1)
# chase_difficulty = RRR × (wickets_fallen + 1)
# Amplifies the signal when both RRR is extreme AND wickets are down.
# A low-wicket team at RRR 8 scores very differently from RRR 8 with 7 wickets gone.
# This avoids over-relying on collinear rrr/crr/rr_diff/runs_needed individually.
df["inn2_ov2_chase_difficulty"] = df.apply(
    lambda r: round((r["inn2_ov2_rrr"] or 0) * ((r["inn2_ov2_wickets"] or 0) + 1), 3), axis=1)

df["inn2_ov6_rp_wkt"]      = df.apply(
    lambda r: safe_div(r["inn2_ov6_score"] or 0, 10 - (r["inn2_ov6_wickets"] or 0)), axis=1)
df["inn2_ov6_odds_swing"]  = df.apply(
    lambda r: safe_div((r["inn2_ov6_bat_odds"] or 0) - (r["inn1_last_bat_odds"] or 0), 1, default=None), axis=1)
df["inn2_ov6_runs_needed_per_wkt"] = df.apply(
    lambda r: safe_div(r["inn2_ov6_runs_needed"] or 0, 10 - (r["inn2_ov6_wickets"] or 0)), axis=1)
df["inn2_ov6_chase_difficulty"] = df.apply(
    lambda r: round((r["inn2_ov6_rrr"] or 0) * ((r["inn2_ov6_wickets"] or 0) + 1), 3), axis=1)

df["inn2_ov10_rp_wkt"]     = df.apply(
    lambda r: safe_div(r["inn2_ov10_score"] or 0, 10 - (r["inn2_ov10_wickets"] or 0)), axis=1)
df["inn2_ov10_odds_swing"] = df.apply(
    lambda r: safe_div((r["inn2_ov10_bat_odds"] or 0) - (r["inn1_last_bat_odds"] or 0), 1, default=None), axis=1)
df["inn2_ov10_runs_needed_per_wkt"] = df.apply(
    lambda r: safe_div(r["inn2_ov10_runs_needed"] or 0, 10 - (r["inn2_ov10_wickets"] or 0)), axis=1)
df["inn2_ov10_chase_difficulty"] = df.apply(
    lambda r: round((r["inn2_ov10_rrr"] or 0) * ((r["inn2_ov10_wickets"] or 0) + 1), 3), axis=1)

df["inn2_ov16_rp_wkt"]     = df.apply(
    lambda r: safe_div(r["inn2_ov16_score"] or 0, 10 - (r["inn2_ov16_wickets"] or 0)), axis=1)
df["inn2_ov16_odds_swing"] = df.apply(
    lambda r: safe_div((r["inn2_ov16_bat_odds"] or 0) - (r["inn1_last_bat_odds"] or 0), 1, default=None), axis=1)
df["inn2_ov16_runs_needed_per_wkt"] = df.apply(
    lambda r: safe_div(r["inn2_ov16_runs_needed"] or 0, 10 - (r["inn2_ov16_wickets"] or 0)), axis=1)
df["inn2_ov16_chase_difficulty"] = df.apply(
    lambda r: round((r["inn2_ov16_rrr"] or 0) * ((r["inn2_ov16_wickets"] or 0) + 1), 3), axis=1)

# Categorical encoding
for col in ["venue", "inn1_bat_team", "inn1_bowl_team", "venue_region", "tournament_stage"]:
    df[col] = df[col].astype("category")

train_df = df[df["split"] == "train"].reset_index(drop=True)
test_df  = df[df["split"] == "test"].reset_index(drop=True)

# Guard: if df somehow acquired duplicate column names, drop extras now
_dup = [c for c in train_df.columns if list(train_df.columns).count(c) > 1]
if _dup:
    print(f"WARNING: duplicate columns detected and removed: {list(dict.fromkeys(_dup))}")
    train_df = train_df.loc[:, ~train_df.columns.duplicated(keep="first")]
    test_df  = test_df.loc[:,  ~test_df.columns.duplicated(keep="first")]
else:
    print(f"Column check OK — {len(train_df.columns)} unique columns")

print(f"\n{'='*60}")
print(f"  Total T20 matches      : {len(df)}")
print(f"  Skipped (incomplete)   : {skipped}")
print(f"  Train (before {TRAIN_CUTOFF}) : {len(train_df)}")
print(f"  Test  (from   {TRAIN_CUTOFF}) : {len(test_df)}")
if len(train_df) > 0:
    print(f"  Train label split      : {dict(train_df['chasing_won'].value_counts())}")
if len(test_df) > 0:
    print(f"  Test  label split      : {dict(test_df['chasing_won'].value_counts())}")
print(f"{'='*60}\n")

display(df[["match_name", "match_date", "split",
            "inn1_bat_team", "inn1_total_score", "inn1_pressure",
            "inn1_pp_rp_wkt", "inn1_odds_swing_full",
            "inn2_ov6_rp_wkt", "inn2_ov6_rr_diff", "inn2_ov6_odds_swing",
            "inn2_ov6_chase_difficulty",
            "chasing_won"]])

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 5 — Feature sets
# ═══════════════════════════════════════════════════════════════════

CAT_FEATURES = ["venue", "inn1_bat_team", "inn1_bowl_team", "venue_region", "tournament_stage"]

# Context features — venue environment, match timing, team form, head-to-head history.
# These capture the "before first ball" prior that XGBoost/CatBoost cannot learn from
# per-over snapshots alone (dew factor, team momentum, historical matchups).
CONTEXT_FEATURES = [
    # venue_region and tournament_stage are already in CAT_FEATURES — omitted here to avoid duplicate columns
    "is_subcontinental",  # 1 if Asia — strongest single dew-factor proxy
    "is_evening_match",   # 1 if match started after 13:00 UTC (evening in subcontinent)
    "bat_team_form5",     # batting team win rate in last 5 matches (momentum)
    "bowl_team_form5",    # bowling team win rate in last 5 matches
    "h2h_bat_win_rate",   # bat team win rate vs bowl team (last 10 head-to-head meetings)
]

MATCH_FLAGS  = ["is_womens_match", "is_weekend_match"]

COMPOSITE_INN1 = [
    "inn1_pp_rp_wkt",
    "inn1_pp_wickets",
    "inn1_mid_rp_wkt",
    "inn1_mid_wickets_only",
    "inn1_death_runs",
    "inn1_death_wickets",
    "inn1_pressure",
    "inn1_bat_dominance",
]

COMPOSITE_INN2_OV2 = [
    "inn2_ov2_rp_wkt",
    "inn2_ov2_runs_needed_per_wkt",
    "inn2_ov2_chase_difficulty",
]

COMPOSITE_INN2_OV6 = [
    "inn2_ov6_rp_wkt",
    "inn2_ov6_runs_needed_per_wkt",
    "inn2_ov6_chase_difficulty",
]

COMPOSITE_INN2_OV10 = [
    "inn2_ov10_rp_wkt",
    "inn2_ov10_runs_needed_per_wkt",
    "inn2_ov10_chase_difficulty",
]

COMPOSITE_INN2_OV16 = [
    "inn2_ov16_rp_wkt",
    "inn2_ov16_runs_needed_per_wkt",
    "inn2_ov16_chase_difficulty",
]

# No-odds: runs and wickets per over only — no market odds columns
RAW_INN1 = (
    ["inn1_total_score", "inn1_total_wickets"] +
    [f"inn1_ov{n}_runs" for n in range(1, 21)] +
    [f"inn1_ov{n}_wkts" for n in range(1, 21)]
)

INN1_BASE = CAT_FEATURES + MATCH_FLAGS + CONTEXT_FEATURES + COMPOSITE_INN1 + RAW_INN1

CHASE_OV2 = (
    COMPOSITE_INN2_OV2 +
    [f"inn2_ov{n}_runs" for n in range(1, 3)] +
    [f"inn2_ov{n}_wkts" for n in range(1, 3)] +
    ["inn2_ov2_score", "inn2_ov2_wickets",
     "inn2_ov2_crr",   "inn2_ov2_rrr",
     "inn2_ov2_rr_diff", "inn2_ov2_runs_needed"]
)

CHASE_OV6 = (
    COMPOSITE_INN2_OV6 +
    [f"inn2_ov{n}_runs" for n in range(1, 7)] +
    [f"inn2_ov{n}_wkts" for n in range(1, 7)] +
    ["inn2_ov6_score", "inn2_ov6_wickets",
     "inn2_ov6_crr",   "inn2_ov6_rrr",
     "inn2_ov6_rr_diff", "inn2_ov6_runs_needed"]
)

CHASE_OV10 = (
    COMPOSITE_INN2_OV10 +
    [f"inn2_ov{n}_runs" for n in range(1, 11)] +
    [f"inn2_ov{n}_wkts" for n in range(1, 11)] +
    ["inn2_ov10_score", "inn2_ov10_wickets",
     "inn2_ov10_crr",   "inn2_ov10_rrr",
     "inn2_ov10_rr_diff", "inn2_ov10_runs_needed"]
)

CHASE_OV16 = (
    COMPOSITE_INN2_OV16 +
    [f"inn2_ov{n}_runs" for n in range(1, 17)] +
    [f"inn2_ov{n}_wkts" for n in range(1, 17)] +
    ["inn2_ov16_score", "inn2_ov16_wickets",
     "inn2_ov16_crr",   "inn2_ov16_rrr",
     "inn2_ov16_rr_diff", "inn2_ov16_runs_needed"]
)
INN1_FEATURES      = INN1_BASE
INN2_OV2_FEATURES  = INN1_BASE + CHASE_OV2
INN2_OV6_FEATURES  = INN1_BASE + CHASE_OV6
INN2_OV10_FEATURES = INN1_BASE + CHASE_OV10
INN2_OV16_FEATURES = INN1_BASE + CHASE_OV16

print(f"innings1-only    : {len(INN1_FEATURES)} features")
print(f"innings2-2over   : {len(INN2_OV2_FEATURES)} features")
print(f"innings2-6over   : {len(INN2_OV6_FEATURES)} features")
print(f"innings2-10over  : {len(INN2_OV10_FEATURES)} features")
print(f"innings2-16over  : {len(INN2_OV16_FEATURES)} features")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 6 — Shared helpers: prepare_X, importance table
# ═══════════════════════════════════════════════════════════════════

def prepare_X(df_in, feature_cols, train_medians=None):
    """Fill NAs and label-encode categoricals. Returns float DataFrame + medians dict."""
    feature_cols = list(dict.fromkeys(feature_cols))  # deduplicate feature list
    X = df_in[feature_cols].copy()
    if X.columns.duplicated().any():                  # guard if df_in itself had dup cols
        X = X.loc[:, ~X.columns.duplicated(keep="first")]
    medians = {}
    for col in feature_cols:
        col_s = X[col]
        if isinstance(col_s, pd.DataFrame):
            col_s = col_s.iloc[:, 0]
        if hasattr(col_s.dtype, "categories") or col_s.dtype == object:
            enc = (train_medians or {}).get(col)
            if not isinstance(enc, dict):
                _, uniq = pd.factorize(col_s)
                enc = {v: i for i, v in enumerate(uniq)}
            X[col] = col_s.map(enc).fillna(-1).astype(int)
            medians[col] = enc
        else:
            fill = (train_medians or {}).get(col)
            if fill is None or isinstance(fill, dict):
                fill = col_s.median()
            X[col] = col_s.fillna(fill)
            medians[col] = fill
    return X.astype(float), medians

def make_fi_df(importances, feature_cols):
    fi = pd.Series(importances, index=feature_cols).sort_values(ascending=False)
    total = fi.sum()
    return pd.DataFrame({
        "rank":         range(1, len(fi) + 1),
        "feature":      fi.index,
        "importance":   fi.values.round(5),
        "pct_of_total": (fi.values / total * 100).round(2) if total > 0 else [0.0] * len(fi),
    })

def print_results(model_name, algo, acc, auc, cm, fi_df):
    print(f"\n  [{algo}] {model_name}")
    print(f"    Accuracy : {acc:.3f}  ({int(acc * (cm.sum() if cm is not None else 0))} correct)")
    if auc:
        print(f"    ROC-AUC  : {auc:.3f}")
    if cm is not None and cm.shape == (2, 2):
        print(f"    Defended correctly : {cm[0][0]}  |  Chase won (wrong) : {cm[0][1]}")
        print(f"    Chase missed       : {cm[1][0]}  |  Chase correct     : {cm[1][1]}")
    if fi_df is not None:
        print(f"\n    Top 10 features:")
        for _, row in fi_df.head(10).iterrows():
            print(f"      {int(row['rank']):<4} {row['feature']:<42} {row['pct_of_total']:>6.2f}%")
    print()

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 7 — XGBoost: train → prune → retrain
#
# Pass 1: train on all features
# Pass 2: drop features below IMPORTANCE_THRESHOLD (rarely used by any tree)
# Pass 3: retrain on pruned feature set
#
# This handles the "too many features, small dataset" problem by letting
# the model tell us which features carry no signal.
# ═══════════════════════════════════════════════════════════════════

XGB_PARAMS = {
    "n_estimators":      200,
    "max_depth":         3,
    "learning_rate":     0.05,
    "subsample":         0.8,
    "colsample_bytree":  0.6,
    "min_child_weight":  2,
    "random_state":      42,
    "n_jobs":            -1,
    "eval_metric":       "logloss",
    # enable_categorical omitted — categoricals are label-encoded in prepare_X
}

def xgb_train_pruned(model_name, feature_cols, train_df, test_df):
    """
    1. Train XGBoost on full feature_cols.
    2. Identify features with importance < IMPORTANCE_THRESHOLD — drop them.
    3. Retrain on pruned set.
    4. Return (model, acc, auc, fi_df, pruned_feature_cols).
    """
    print(f"\n{'─'*60}")
    print(f"  XGBoost — {model_name}  ({len(feature_cols)} features)")

    if len(train_df) < 5:
        print(f"  SKIP: only {len(train_df)} training matches")
        return None, None, None, None, feature_cols, []

    # Pass 1 — full feature set
    X_train, train_meds = prepare_X(train_df, feature_cols)
    y_train = train_df["chasing_won"]
    X_test,  _          = prepare_X(test_df,  feature_cols, train_meds)
    y_test  = test_df["chasing_won"]

    m1 = xgb.XGBClassifier(**XGB_PARAMS)
    m1.fit(X_train.to_numpy(), y_train.to_numpy())
    fi1 = pd.Series(m1.feature_importances_, index=feature_cols)
    total1 = fi1.sum()
    pct1   = fi1 / total1 if total1 > 0 else fi1

    dropped = pct1[pct1 < IMPORTANCE_THRESHOLD].index.tolist()
    kept    = pct1[pct1 >= IMPORTANCE_THRESHOLD].index.tolist()
    print(f"  Pass 1: {len(dropped)} features dropped (importance < {IMPORTANCE_THRESHOLD*100:.1f}%)")
    print(f"          {len(kept)} features kept → retraining")

    if not kept:
        kept = feature_cols   # safety: keep all if pruning removed everything

    # Pass 2 — pruned feature set
    X_train2, train_meds2 = prepare_X(train_df, kept)
    X_test2,  _           = prepare_X(test_df,  kept, train_meds2)

    m2 = xgb.XGBClassifier(**XGB_PARAMS)
    m2.fit(X_train2.to_numpy(), y_train.to_numpy())

    y_pred  = m2.predict(X_test2.to_numpy())
    y_proba = m2.predict_proba(X_test2.to_numpy())[:, 1]
    acc = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba) if len(y_test.unique()) > 1 else None
    cm  = confusion_matrix(y_test, y_pred)
    fi_df = make_fi_df(m2.feature_importances_, kept)

    # Train-set performance (shows memorisation vs generalisation gap)
    y_train_pred  = m2.predict(X_train2.to_numpy())
    y_train_proba = m2.predict_proba(X_train2.to_numpy())[:, 1]
    train_acc = accuracy_score(y_train, y_train_pred)
    train_auc = roc_auc_score(y_train, y_train_proba) if len(y_train.unique()) > 1 else None

    print_results(model_name, "XGBoost (pruned)", acc, auc, cm, fi_df)
    print(f"  Train accuracy: {train_acc:.3f}  Train AUC: {f'{train_auc:.3f}' if train_auc else 'n/a'}")
    print(f"\n  Full importance table:")
    display(fi_df)

    score_ctx = [c for c in [
        "inn1_total_score", "inn1_score_display",
        "inn2_ov2_score", "inn2_ov2_wickets",
        "inn2_ov6_score", "inn2_ov6_wickets",
        "inn2_ov10_score", "inn2_ov10_wickets",
        "inn2_ov16_score", "inn2_ov16_wickets",
        "inn2_total_score", "inn2_score_display",
    ] if c in test_df.columns]

    # Top features for the what-if panel (top 20 by importance, original values)
    top_feats = fi_df.head(20)["feature"].tolist()

    def _feat_vals(df_slice, idx_list):
        """Extract original (pre-encoding) feature values for what-if display."""
        result = []
        for idx in idx_list:
            row = {}
            for feat in top_feats:
                if feat not in df_slice.columns:
                    continue
                v = df_slice.iloc[idx][feat]
                try:
                    if pd.isna(v):
                        v = None
                    elif hasattr(v, 'item'):
                        v = v.item()
                    elif isinstance(v, float):
                        v = round(v, 4)
                except Exception:
                    pass
                row[feat] = v
            result.append(row)
        return result

    # Test predictions
    preds = test_df[["event_id", "match_name", "match_date", "inn1_bat_team", "inn1_bowl_team",
                      "chasing_won"] + score_ctx].copy()
    preds["predicted"] = y_pred
    preds["confidence_pct"] = [
        round(p * 100, 1) if pred == 1 else round((1 - p) * 100, 1)
        for p, pred in zip(y_proba, y_pred)
    ]
    preds["correct"] = (y_pred == y_test).map({True: True, False: False})
    preds_records = preds.rename(columns={"inn1_total_score": "inn1_score",
                                          "inn2_total_score": "inn2_score"}).to_dict("records")
    feat_vals_test = _feat_vals(test_df.reset_index(drop=True), list(range(len(test_df))))
    for rec, fv in zip(preds_records, feat_vals_test):
        rec["feature_values"] = fv

    # Train predictions
    score_ctx_train = [c for c in score_ctx if c in train_df.columns]
    train_preds = train_df[["event_id", "match_name", "match_date", "inn1_bat_team", "inn1_bowl_team",
                             "chasing_won"] + score_ctx_train].copy()
    train_preds["predicted"] = y_train_pred
    train_preds["confidence_pct"] = [
        round(p * 100, 1) if pred == 1 else round((1 - p) * 100, 1)
        for p, pred in zip(y_train_proba, y_train_pred)
    ]
    train_preds["correct"] = (y_train_pred == y_train.to_numpy())
    train_preds_records = train_preds.rename(columns={"inn1_total_score": "inn1_score",
                                                       "inn2_total_score": "inn2_score"}).to_dict("records")
    feat_vals_train = _feat_vals(train_df.reset_index(drop=True), list(range(len(train_df))))
    for rec, fv in zip(train_preds_records, feat_vals_train):
        rec["feature_values"] = fv

    # train_meds2 maps each kept feature to its categorical label-encoding dict
    # (for string columns) or its training median (for numeric columns).
    # Returned so callers can save it alongside the model for inference at serve time.
    return m2, acc, auc, train_acc, train_auc, fi_df, kept, preds_records, train_preds_records, train_meds2

if len(train_df) < 5:
    raise ValueError(f"Only {len(train_df)} training matches — need at least 5.")
if len(test_df) < 2:
    raise ValueError(f"Only {len(test_df)} test matches — need at least 2.")

xgb_inn1,  xgb_acc_inn1,  xgb_auc_inn1,  xgb_train_acc_inn1,  xgb_train_auc_inn1,  xgb_fi_inn1,  pruned_inn1,  preds_inn1,  train_preds_inn1,  enc_inn1  = xgb_train_pruned("innings1-only",   INN1_FEATURES,      train_df, test_df)
xgb_ov2,   xgb_acc_ov2,   xgb_auc_ov2,   xgb_train_acc_ov2,   xgb_train_auc_ov2,   xgb_fi_ov2,   pruned_ov2,   preds_ov2,   train_preds_ov2,   enc_ov2   = xgb_train_pruned("innings2-2over",  INN2_OV2_FEATURES,  train_df, test_df)
xgb_ov6,   xgb_acc_ov6,   xgb_auc_ov6,   xgb_train_acc_ov6,   xgb_train_auc_ov6,   xgb_fi_ov6,   pruned_ov6,   preds_ov6,   train_preds_ov6,   enc_ov6   = xgb_train_pruned("innings2-6over",  INN2_OV6_FEATURES,  train_df, test_df)
xgb_ov10,  xgb_acc_ov10,  xgb_auc_ov10,  xgb_train_acc_ov10,  xgb_train_auc_ov10,  xgb_fi_ov10,  pruned_ov10,  preds_ov10,  train_preds_ov10,  enc_ov10  = xgb_train_pruned("innings2-10over", INN2_OV10_FEATURES, train_df, test_df)
xgb_ov16,  xgb_acc_ov16,  xgb_auc_ov16,  xgb_train_acc_ov16,  xgb_train_auc_ov16,  xgb_fi_ov16,  pruned_ov16,  preds_ov16,  train_preds_ov16,  enc_ov16  = xgb_train_pruned("innings2-16over", INN2_OV16_FEATURES, train_df, test_df)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 8 — Random Forest (same pruned feature sets as XGBoost)
#
# Why run both?
#   XGBoost builds trees sequentially, each correcting previous errors.
#   Random Forest builds trees independently and averages them.
#   On small datasets, Random Forest importances are more stable —
#   running the same notebook twice gives more consistent rankings.
#   Use RF importances to double-check which features XGBoost ranked highly.
# ═══════════════════════════════════════════════════════════════════

RF_PARAMS = {
    "n_estimators": 300,
    "max_depth":    4,
    "min_samples_leaf": 2,
    "random_state": 42,
    "n_jobs":       -1,
}

def rf_train(model_name, feature_cols, train_df, test_df):
    print(f"\n{'─'*60}")
    print(f"  Random Forest — {model_name}  ({len(feature_cols)} features)")

    if len(train_df) < 5:
        return None, None, None, None

    feature_cols = list(dict.fromkeys(feature_cols))  # deduplicate, preserve order
    def _dtype(df, c):
        s = df[c]; return s.dtype if isinstance(s, pd.Series) else s.iloc[:, 0].dtype
    # RF does not support pandas Categorical — label encode for it
    cat_cols = [c for c in feature_cols if hasattr(_dtype(train_df, c), "categories")]
    num_cols = [c for c in feature_cols if c not in cat_cols]

    def prep_rf(df_in, cat_encoders=None):
        X = df_in[feature_cols].copy()
        encoders = cat_encoders or {}
        for col in cat_cols:
            if col not in encoders:
                codes, uniq = pd.factorize(X[col])
                encoders[col] = {v: i for i, v in enumerate(uniq)}
            X[col] = X[col].map(encoders[col]).fillna(-1).astype(int)
        for col in num_cols:
            med = df_in[col].median() if cat_encoders is None else X[col].median()
            X[col] = X[col].fillna(med)
        return X.astype(float), encoders

    X_train, enc = prep_rf(train_df)
    X_test,  _   = prep_rf(test_df, enc)
    y_train = train_df["chasing_won"]
    y_test  = test_df["chasing_won"]

    rf = RandomForestClassifier(**RF_PARAMS)
    rf.fit(X_train, y_train)

    y_pred  = rf.predict(X_test)
    y_proba = rf.predict_proba(X_test)[:, 1]
    acc = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba) if len(y_test.unique()) > 1 else None
    cm  = confusion_matrix(y_test, y_pred)
    fi_df = make_fi_df(rf.feature_importances_, feature_cols)

    y_train_pred  = rf.predict(X_train)
    y_train_proba = rf.predict_proba(X_train)[:, 1]
    train_acc = accuracy_score(y_train, y_train_pred)
    train_auc = roc_auc_score(y_train, y_train_proba) if len(y_train.unique()) > 1 else None

    print_results(model_name, "Random Forest", acc, auc, cm, fi_df)
    print(f"  Train accuracy: {train_acc:.3f}  Train AUC: {f'{train_auc:.3f}' if train_auc else 'n/a'}")
    display(fi_df)
    return rf, acc, auc, train_acc, train_auc, fi_df

rf_inn1, rf_acc_inn1, rf_auc_inn1, rf_train_acc_inn1, rf_train_auc_inn1, rf_fi_inn1 = rf_train("innings1-only",   pruned_inn1,  train_df, test_df)
rf_ov2,  rf_acc_ov2,  rf_auc_ov2,  rf_train_acc_ov2,  rf_train_auc_ov2,  rf_fi_ov2  = rf_train("innings2-2over",  pruned_ov2,   train_df, test_df)
rf_ov6,  rf_acc_ov6,  rf_auc_ov6,  rf_train_acc_ov6,  rf_train_auc_ov6,  rf_fi_ov6  = rf_train("innings2-6over",  pruned_ov6,   train_df, test_df)
rf_ov10, rf_acc_ov10, rf_auc_ov10, rf_train_acc_ov10, rf_train_auc_ov10, rf_fi_ov10 = rf_train("innings2-10over", pruned_ov10,  train_df, test_df)
rf_ov16, rf_acc_ov16, rf_auc_ov16, rf_train_acc_ov16, rf_train_auc_ov16, rf_fi_ov16 = rf_train("innings2-16over", pruned_ov16,  train_df, test_df)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 9
# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 8b — CatBoost (runs in parallel with XGBoost + Random Forest)
#
# Advantage over XGBoost: native categorical handling for venue, team
# names, venue_region and tournament_stage — no manual label encoding.
# CatBoost uses ordered target statistics so even rare venues (few
# matches) get a meaningful probability rather than an arbitrary int.
# ═══════════════════════════════════════════════════════════════════

subprocess.run(["pip", "install", "--quiet", "catboost"], check=True)
from catboost import CatBoostClassifier, Pool as CatPool

CB_PARAMS = {
    "iterations":    300,
    "learning_rate": 0.05,
    "depth":         4,
    "random_seed":   42,
    "verbose":       0,
    "eval_metric":   "Accuracy",
    "loss_function": "Logloss",
}

def cb_train(model_name, feature_cols, train_df, test_df):
    print(f"\n{'─'*60}")
    print(f"  CatBoost — {model_name}  ({len(feature_cols)} features)")

    if len(train_df) < 5:
        return None, None, None, None, None, None

    feature_cols = list(dict.fromkeys(feature_cols))  # deduplicate, preserve order
    def _dtype(df, c):
        s = df[c]; return s.dtype if isinstance(s, pd.Series) else s.iloc[:, 0].dtype
    cat_cols = [c for c in feature_cols if hasattr(_dtype(train_df, c), "categories")]
    cat_idx  = [feature_cols.index(c) for c in cat_cols]

    def prep_cb(df_in, medians=None):
        X = df_in[feature_cols].copy()
        # CatBoost expects strings for categoricals, fill NA numerics with median
        for col in cat_cols:
            X[col] = X[col].astype(str).replace("nan", "unknown")
        num_cols = [c for c in feature_cols if c not in cat_cols]
        meds = medians or {}
        for col in num_cols:
            if col not in meds:
                meds[col] = float(df_in[col].median()) if df_in[col].notna().any() else 0.0
            X[col] = X[col].fillna(meds[col])
        return X, meds

    X_train, meds = prep_cb(train_df)
    X_test,  _    = prep_cb(test_df, meds)
    y_train = train_df["chasing_won"].values
    y_test  = test_df["chasing_won"].values

    train_pool = CatPool(X_train, y_train, cat_features=cat_idx)
    test_pool  = CatPool(X_test,  y_test,  cat_features=cat_idx)

    cb = CatBoostClassifier(**CB_PARAMS)
    cb.fit(train_pool, eval_set=test_pool, use_best_model=True)

    y_pred       = cb.predict(test_pool)
    y_proba      = cb.predict_proba(test_pool)[:, 1]
    y_train_pred = cb.predict(train_pool)
    y_train_prob = cb.predict_proba(train_pool)[:, 1]

    acc       = accuracy_score(y_test,  y_pred)
    auc       = roc_auc_score(y_test,  y_proba)      if len(np.unique(y_test))  > 1 else None
    train_acc = accuracy_score(y_train, y_train_pred)
    train_auc = roc_auc_score(y_train, y_train_prob) if len(np.unique(y_train)) > 1 else None
    cm        = confusion_matrix(y_test, y_pred)

    fi_df = pd.DataFrame({
        "feature":    feature_cols,
        "importance": cb.get_feature_importance(),
    }).sort_values("importance", ascending=False).reset_index(drop=True)
    fi_df["rank"]       = range(1, len(fi_df) + 1)
    fi_df["pct_of_total"] = (fi_df["importance"] / fi_df["importance"].sum() * 100).round(3)

    print_results(model_name, "CatBoost", acc, auc, cm, fi_df)
    print(f"  Train accuracy: {train_acc:.3f}  Train AUC: {f'{train_auc:.3f}' if train_auc else 'n/a'}")
    display(fi_df[fi_df["importance"] > 0])
    return cb, acc, auc, train_acc, train_auc, fi_df

cb_inn1, cb_acc_inn1, cb_auc_inn1, cb_train_acc_inn1, cb_train_auc_inn1, cb_fi_inn1 = cb_train("innings1-only",   INN1_FEATURES,      train_df, test_df)
cb_ov2,  cb_acc_ov2,  cb_auc_ov2,  cb_train_acc_ov2,  cb_train_auc_ov2,  cb_fi_ov2  = cb_train("innings2-2over",  INN2_OV2_FEATURES,  train_df, test_df)
cb_ov6,  cb_acc_ov6,  cb_auc_ov6,  cb_train_acc_ov6,  cb_train_auc_ov6,  cb_fi_ov6  = cb_train("innings2-6over",  INN2_OV6_FEATURES,  train_df, test_df)
cb_ov10, cb_acc_ov10, cb_auc_ov10, cb_train_acc_ov10, cb_train_auc_ov10, cb_fi_ov10 = cb_train("innings2-10over", INN2_OV10_FEATURES, train_df, test_df)
cb_ov16, cb_acc_ov16, cb_auc_ov16, cb_train_acc_ov16, cb_train_auc_ov16, cb_fi_ov16 = cb_train("innings2-16over", INN2_OV16_FEATURES, train_df, test_df)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 8c — Hidden Markov Model (HMM)
#
# HMM treats each match as a TIME SERIES — it reads over-by-over
# observations and learns hidden "momentum states" that transition
# as the match progresses:
#   State 0 → "Struggling"   (few runs, wickets falling)
#   State 1 → "Competitive"  (run-rate on target, some pressure)
#   State 2 → "Dominating"   (big overs, no wickets)
#
# Two separate HMMs are trained:
#   hmm_chase  → fitted on matches where the chasing team WON
#   hmm_defend → fitted on matches where the defending team WON
#
# Prediction: score each match's sequence under both HMMs.
# The log-likelihood ratio (LLR = log P(seq|chase) - log P(seq|defend))
# determines the prediction: LLR > 0 → chase wins.
#
# Observation vector per over: [incremental_runs, incremental_wickets]
# (deliberately kept small — HMM excels at temporal patterns,
#  not raw numerical magnitudes)
#
# Why alongside XGBoost (not instead of it):
#   XGBoost at over 6 sees: "score=80/1, crr=13.3" (a STATE snapshot)
#   HMM at over 6 sees:     "ov1:12/0 → ov2:8/0 → ov3:14/1 → ov4:6/1
#                             → ov5:10/0 → ov6:14/0" (a TRAJECTORY)
#   The trajectory contains information the snapshot cannot — e.g.,
#   a wicket in over 3 recovered by clean hitting in overs 4-6 is
#   very different from a smooth 80/1 with no scares.
# ═══════════════════════════════════════════════════════════════════

subprocess.run(["pip", "install", "--quiet", "hmmlearn"], check=True)
from hmmlearn.hmm import GaussianHMM

def _hmm_sequences(df, over_prefix, max_over):
    """
    Build per-match observation sequences for hmmlearn.
    Each observation = [incremental_runs, incremental_wickets] at that over.
    Returns (X_stacked, lengths) ready for hmm.fit() / hmm.score().
    """
    seqs, lengths = [], []
    for _, row in df.iterrows():
        obs = []
        for ov in range(1, max_over + 1):
            r = float(row.get(f"{over_prefix}_ov{ov}_runs", 0) or 0)
            w = float(row.get(f"{over_prefix}_ov{ov}_wkts", 0) or 0)
            obs.append([r, w])
        seqs.append(np.array(obs, dtype=float))
        lengths.append(len(obs))
    return np.vstack(seqs), lengths


def hmm_train_eval(model_name, over_prefix, max_over, train_df, test_df):
    print(f"\n{'─'*60}")
    n_states = max(2, min(3, max_over))   # 2 states for short sequences, 3 for 6+
    print(f"  HMM — {model_name}  (n_states={n_states}, obs_len={max_over})")

    chase_tr  = train_df[train_df["chasing_won"] == 1]
    defend_tr = train_df[train_df["chasing_won"] == 0]
    if len(chase_tr) < n_states + 1 or len(defend_tr) < n_states + 1:
        print("  SKIP: too few training samples per class")
        return None, None, None, None

    X_chase,  L_chase  = _hmm_sequences(chase_tr,  over_prefix, max_over)
    X_defend, L_defend = _hmm_sequences(defend_tr, over_prefix, max_over)

    hmm_c = GaussianHMM(n_components=n_states, covariance_type="diag",
                         n_iter=200, random_state=42)
    hmm_d = GaussianHMM(n_components=n_states, covariance_type="diag",
                         n_iter=200, random_state=42)
    try:
        hmm_c.fit(X_chase,  L_chase)
        hmm_d.fit(X_defend, L_defend)
    except Exception as e:
        print(f"  HMM fit failed: {e}")
        return None, None, None, None

    y_true, y_pred, y_prob = [], [], []
    for _, row in test_df.iterrows():
        obs = []
        for ov in range(1, max_over + 1):
            r = float(row.get(f"{over_prefix}_ov{ov}_runs", 0) or 0)
            w = float(row.get(f"{over_prefix}_ov{ov}_wkts", 0) or 0)
            obs.append([r, w])
        X_seq = np.array(obs, dtype=float)
        try:
            ll_c = hmm_c.score(X_seq, [len(X_seq)])
            ll_d = hmm_d.score(X_seq, [len(X_seq)])
            llr  = ll_c - ll_d
            prob = float(1 / (1 + np.exp(-llr / max_over)))  # sigmoid scaled
        except Exception:
            llr, prob = 0.0, 0.5
        y_true.append(int(row["chasing_won"]))
        y_pred.append(1 if llr > 0 else 0)
        y_prob.append(prob)

    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    y_prob = np.array(y_prob)

    acc = accuracy_score(y_true, y_pred)
    auc = roc_auc_score(y_true, y_prob) if len(np.unique(y_true)) > 1 else None
    cm  = confusion_matrix(y_true, y_pred)
    print_results(model_name, "HMM", acc, auc, cm, fi_df=None)
    return hmm_c, hmm_d, acc, auc


# Inn1 only — observe full innings 1 trajectory
hmm_c_inn1,  hmm_d_inn1,  hmm_acc_inn1,  hmm_auc_inn1  = hmm_train_eval("innings1-only",   "inn1", 20, train_df, test_df)
# Inn2 checkpoints — observe chase trajectory up to each over
hmm_c_ov2,   hmm_d_ov2,   hmm_acc_ov2,   hmm_auc_ov2   = hmm_train_eval("innings2-2over",  "inn2", 2,  train_df, test_df)
hmm_c_ov6,   hmm_d_ov6,   hmm_acc_ov6,   hmm_auc_ov6   = hmm_train_eval("innings2-6over",  "inn2", 6,  train_df, test_df)
hmm_c_ov10,  hmm_d_ov10,  hmm_acc_ov10,  hmm_auc_ov10  = hmm_train_eval("innings2-10over", "inn2", 10, train_df, test_df)
hmm_c_ov16,  hmm_d_ov16,  hmm_acc_ov16,  hmm_auc_ov16  = hmm_train_eval("innings2-16over", "inn2", 16, train_df, test_df)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 8d — LSTM (Bidirectional, small-data adapted)
#
# LSTM reads each match as a TIME SERIES of overs — every over is a
# timestep, not just another flat column. This is the key difference
# from XGBoost/CatBoost:
#
#   XGBoost: sees "over_6_score=80" as one number
#   LSTM:    reads ov1→ov2→ov3→ov4→ov5→ov6 and builds a memory of
#            the trajectory — was the momentum accelerating? Did a
#            wicket in ov3 pause the momentum? Did recovery follow?
#
# Small-data adaptations (170 matches, ideal threshold is 300+):
#   - Single Bidirectional LSTM, 16 units (not 128)
#     Bidirectional means it reads the sequence both forwards AND
#     backwards — more context with fewer parameters.
#   - Dropout 0.4 — aggressive regularisation to prevent memorisation
#   - Early stopping patience=15 — stops when val_loss plateaus
#   - Data augmentation: 2 extra noisy copies of each training sequence
#     (noise_std=0.05) — triples effective training set
#   - Masking layer: zero-padded overs are ignored by the LSTM
#
# Observation per over (timestep):
#   [inc_runs/20, inc_wickets/2, cum_score/200, pressure/20]
#   (normalised so all features are roughly 0–1 range)
#
# IMPORTANT: with 170 matches the LSTM may not reliably beat XGBoost.
# Its accuracy here is an early signal — at 300+ matches it should
# surpass tree-based models because it sees what they cannot: momentum.
# ═══════════════════════════════════════════════════════════════════

try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import Bidirectional, LSTM as KerasLSTM, Dense, Dropout, Masking
    from tensorflow.keras.callbacks import EarlyStopping
    from tensorflow.keras.optimizers import Adam

    print(f"TensorFlow {tf.__version__} — building LSTM models")

    _LSTM_UNITS   = 16
    _DENSE_UNITS  = 8
    _DROPOUT      = 0.4
    _LR           = 0.001
    _BATCH        = 16
    _MAX_EP       = 150
    _PATIENCE     = 15
    _NOISE_STD    = 0.05

    def _build_lstm_seqs(df, inn1_overs, inn2_overs):
        """
        Build padded (n_matches, timesteps, 4) array.
        Inn1 overs first, then inn2 overs (if any).
        Feature vector per timestep: [inc_runs, inc_wkts, cum_metric_a, cum_metric_b]
        All values normalised to ~[0, 1].
        """
        seqs = []
        for _, row in df.iterrows():
            seq = []
            total = float(row.get("inn1_total_score", 0) or 0)
            pressure = float(row.get("inn1_pressure", 0) or 0)
            for ov in range(1, inn1_overs + 1):
                r = float(row.get(f"inn1_ov{ov}_runs", 0) or 0)
                w = float(row.get(f"inn1_ov{ov}_wkts", 0) or 0)
                seq.append([r / 20.0, w / 2.0, total / 200.0, pressure / 20.0])
            for ov in range(1, inn2_overs + 1):
                r   = float(row.get(f"inn2_ov{ov}_runs", 0) or 0)
                w   = float(row.get(f"inn2_ov{ov}_wkts", 0) or 0)
                crr = float(row.get(f"inn2_ov{inn2_overs}_crr", 0) or 0) if inn2_overs else 0
                rrr = float(row.get(f"inn2_ov{inn2_overs}_rrr", 0) or 0) if inn2_overs else 0
                seq.append([r / 20.0, w / 2.0, crr / 20.0, rrr / 20.0])
            seqs.append(seq)
        T = max(len(s) for s in seqs)
        pad = np.array([s + [[0.0, 0.0, 0.0, 0.0]] * (T - len(s)) for s in seqs],
                       dtype=np.float32)
        return pad

    def _augment_seqs(X, y, noise_std=_NOISE_STD, copies=2):
        """Triple training set with noise augmentation."""
        np.random.seed(42)
        parts_X = [X]
        parts_y = [y]
        for _ in range(copies):
            noise = np.random.normal(0, noise_std, X.shape).astype(np.float32)
            parts_X.append(np.clip(X + noise, 0.0, None))
            parts_y.append(y)
        return np.vstack(parts_X), np.concatenate(parts_y)

    def lstm_train_eval(model_name, inn1_overs, inn2_overs, train_df, test_df):
        print(f"\n{'─'*60}")
        print(f"  LSTM (Bidirectional) — {model_name}  "
              f"(seq_len={inn1_overs + inn2_overs}, train={len(train_df)}, test={len(test_df)})")

        if len(train_df) < 10:
            print("  SKIP: too few training samples")
            return None, None, None

        X_tr = _build_lstm_seqs(train_df, inn1_overs, inn2_overs)
        X_te = _build_lstm_seqs(test_df,  inn1_overs, inn2_overs)
        y_tr = train_df["chasing_won"].values.astype(np.float32)
        y_te = test_df["chasing_won"].values.astype(np.float32)

        X_tr_aug, y_tr_aug = _augment_seqs(X_tr, y_tr)

        T, F = X_tr.shape[1], X_tr.shape[2]
        tf.random.set_seed(42)

        model = Sequential([
            Masking(mask_value=0.0, input_shape=(T, F)),
            Bidirectional(KerasLSTM(_LSTM_UNITS)),
            Dropout(_DROPOUT),
            Dense(_DENSE_UNITS, activation="relu"),
            Dropout(_DROPOUT / 2),
            Dense(1, activation="sigmoid"),
        ])
        model.compile(optimizer=Adam(_LR),
                      loss="binary_crossentropy", metrics=["accuracy"])

        es = EarlyStopping(monitor="val_loss", patience=_PATIENCE,
                           restore_best_weights=True, verbose=0)
        hist = model.fit(X_tr_aug, y_tr_aug,
                         validation_data=(X_te, y_te),
                         epochs=_MAX_EP, batch_size=_BATCH,
                         callbacks=[es], verbose=0)

        y_prob = model.predict(X_te, verbose=0).flatten()
        y_pred = (y_prob >= 0.5).astype(int)
        acc    = accuracy_score(y_te, y_pred)
        auc    = roc_auc_score(y_te, y_prob) if len(np.unique(y_te)) > 1 else None
        cm     = confusion_matrix(y_te, y_pred)
        ep     = len(hist.history["loss"])

        print_results(model_name, f"LSTM (ep={ep})", acc, auc, cm, fi_df=None)
        return model, acc, auc

    lstm_inn1, lstm_acc_inn1, lstm_auc_inn1 = lstm_train_eval("innings1-only",   20, 0,  train_df, test_df)
    lstm_ov2,  lstm_acc_ov2,  lstm_auc_ov2  = lstm_train_eval("innings2-2over",  20, 2,  train_df, test_df)
    lstm_ov6,  lstm_acc_ov6,  lstm_auc_ov6  = lstm_train_eval("innings2-6over",  20, 6,  train_df, test_df)
    lstm_ov10, lstm_acc_ov10, lstm_auc_ov10 = lstm_train_eval("innings2-10over", 20, 10, train_df, test_df)
    lstm_ov16, lstm_acc_ov16, lstm_auc_ov16 = lstm_train_eval("innings2-16over", 20, 16, train_df, test_df)

except Exception as _lstm_err:
    print(f"\n[LSTM] Skipped — {type(_lstm_err).__name__}: {_lstm_err}")
    lstm_acc_inn1 = lstm_auc_inn1 = None
    lstm_acc_ov2  = lstm_auc_ov2  = None
    lstm_acc_ov6  = lstm_auc_ov6  = None
    lstm_acc_ov10 = lstm_auc_ov10 = None
    lstm_acc_ov16 = lstm_auc_ov16 = None

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 9 — Algorithm comparison table (XGBoost + RF + CatBoost)
# ═══════════════════════════════════════════════════════════════════

def _fmt(v):
    return f"{v:.3f}" if v is not None else "n/a"

print("\n" + "="*80)
print(f"  {'Model':<20}  {'Algorithm':<16}  {'Test Acc':>9}  {'Test AUC':>9}  {'Features':>8}")
print("  " + "─"*74)
rows_cmp = [
    ("innings1-only",   "XGBoost",     xgb_acc_inn1, xgb_auc_inn1, len(pruned_inn1)),
    ("innings1-only",   "CatBoost",    cb_acc_inn1,  cb_auc_inn1,  len(INN1_FEATURES)),
    ("innings1-only",   "Rand Forest", rf_acc_inn1,  rf_auc_inn1,  len(pruned_inn1)),
    ("innings1-only",   "HMM",         hmm_acc_inn1,  hmm_auc_inn1,  "20-over seq"),
    ("innings1-only",   "LSTM (BiDir)",lstm_acc_inn1, lstm_auc_inn1, "20-over seq"),
    ("innings2-2over",  "XGBoost",     xgb_acc_ov2,  xgb_auc_ov2,  len(pruned_ov2)),
    ("innings2-2over",  "CatBoost",    cb_acc_ov2,   cb_auc_ov2,   len(INN2_OV2_FEATURES)),
    ("innings2-2over",  "Rand Forest", rf_acc_ov2,   rf_auc_ov2,   len(pruned_ov2)),
    ("innings2-2over",  "HMM",         hmm_acc_ov2,  hmm_auc_ov2,  "2-over seq"),
    ("innings2-6over",  "XGBoost",     xgb_acc_ov6,  xgb_auc_ov6,  len(pruned_ov6)),
    ("innings2-6over",  "CatBoost",    cb_acc_ov6,   cb_auc_ov6,   len(INN2_OV6_FEATURES)),
    ("innings2-6over",  "Rand Forest", rf_acc_ov6,   rf_auc_ov6,   len(pruned_ov6)),
    ("innings2-6over",  "HMM",         hmm_acc_ov6,  hmm_auc_ov6,  "6-over seq"),
    ("innings2-10over", "XGBoost",     xgb_acc_ov10, xgb_auc_ov10, len(pruned_ov10)),
    ("innings2-10over", "CatBoost",    cb_acc_ov10,  cb_auc_ov10,  len(INN2_OV10_FEATURES)),
    ("innings2-10over", "Rand Forest", rf_acc_ov10,  rf_auc_ov10,  len(pruned_ov10)),
    ("innings2-10over", "HMM",         hmm_acc_ov10, hmm_auc_ov10, "10-over seq"),
    ("innings2-10over", "LSTM (BiDir)",lstm_acc_ov10, lstm_auc_ov10, "10-over seq"),
    ("innings2-16over", "XGBoost",     xgb_acc_ov16, xgb_auc_ov16, len(pruned_ov16)),
    ("innings2-16over", "CatBoost",    cb_acc_ov16,  cb_auc_ov16,  len(INN2_OV16_FEATURES)),
    ("innings2-16over", "Rand Forest", rf_acc_ov16,  rf_auc_ov16,  len(pruned_ov16)),
    ("innings2-16over", "HMM",         hmm_acc_ov16, hmm_auc_ov16, "16-over seq"),
    ("innings2-16over", "LSTM (BiDir)",lstm_acc_ov16, lstm_auc_ov16, "16-over seq"),
]
for model_nm, algo, acc, auc, nfeat in rows_cmp:
    print(f"  {model_nm:<20}  {algo:<16}  {_fmt(acc):>9}  {_fmt(auc):>9}  {str(nfeat):>12}")
print("="*80)
 — Algorithm comparison table
# ═══════════════════════════════════════════════════════════════════

def _fmt(v):
    return f"{v:.3f}" if v is not None else "n/a"

print("\n" + "="*72)
print(f"  {'Model':<20}  {'Algorithm':<16}  {'Accuracy':>9}  {'ROC-AUC':>8}  {'Features':>8}")
print("  " + "─"*68)
rows_cmp = [
    ("innings1-only",   "XGBoost",       xgb_acc_inn1, xgb_auc_inn1, len(pruned_inn1)),
    ("innings1-only",   "Random Forest", rf_acc_inn1,  rf_auc_inn1,  len(pruned_inn1)),
    ("innings2-2over",  "XGBoost",       xgb_acc_ov2,  xgb_auc_ov2,  len(pruned_ov2)),
    ("innings2-2over",  "Random Forest", rf_acc_ov2,   rf_auc_ov2,   len(pruned_ov2)),
    ("innings2-6over",  "XGBoost",       xgb_acc_ov6,  xgb_auc_ov6,  len(pruned_ov6)),
    ("innings2-6over",  "Random Forest", rf_acc_ov6,   rf_auc_ov6,   len(pruned_ov6)),
    ("innings2-10over", "XGBoost",       xgb_acc_ov10, xgb_auc_ov10, len(pruned_ov10)),
    ("innings2-10over", "Random Forest", rf_acc_ov10,  rf_auc_ov10,  len(pruned_ov10)),
    ("innings2-16over", "XGBoost",       xgb_acc_ov16, xgb_auc_ov16, len(pruned_ov16)),
    ("innings2-16over", "Random Forest", rf_acc_ov16,  rf_auc_ov16,  len(pruned_ov16)),
]
for model_nm, algo, acc, auc, nfeat in rows_cmp:
    print(f"  {model_nm:<20}  {algo:<16}  {_fmt(acc):>9}  {_fmt(auc):>8}  {nfeat:>8}")
print("="*72)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 10 — Log best model per innings variant to MLflow
# ═══════════════════════════════════════════════════════════════════

current_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{current_user}/cricket-win-predictor-no-odds")

def log_xgb(run_name, model, acc, auc, feature_cols, reg_name, fi_df):
    if model is None:
        return
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(XGB_PARAMS)
        mlflow.log_param("algorithm",     "XGBoost")
        mlflow.log_param("feature_count", len(feature_cols))
        mlflow.log_param("train_cutoff",  TRAIN_CUTOFF)
        mlflow.log_metric("test_accuracy", acc)
        if auc:
            mlflow.log_metric("test_roc_auc", auc)
        mlflow.log_metric("train_matches", len(train_df))
        mlflow.log_metric("test_matches",  len(test_df))
        if fi_df is not None:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                             prefix=f"{reg_name}_fi_") as f:
                fi_df.to_csv(f, index=False); tmp = f.name
            mlflow.log_artifact(tmp, artifact_path="feature_importance")
            os.unlink(tmp)
        mlflow.xgboost.log_model(model, artifact_path="model",
                                 registered_model_name=reg_name)
    print(f"  Logged XGBoost {reg_name}  acc={acc:.3f}")

def log_rf(run_name, model, acc, auc, feature_cols, reg_name, fi_df):
    if model is None:
        return
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(RF_PARAMS)
        mlflow.log_param("algorithm",     "RandomForest")
        mlflow.log_param("feature_count", len(feature_cols))
        mlflow.log_param("train_cutoff",  TRAIN_CUTOFF)
        mlflow.log_metric("test_accuracy", acc)
        if auc:
            mlflow.log_metric("test_roc_auc", auc)
        mlflow.log_metric("train_matches", len(train_df))
        mlflow.log_metric("test_matches",  len(test_df))
        if fi_df is not None:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                             prefix=f"{reg_name}_rf_fi_") as f:
                fi_df.to_csv(f, index=False); tmp = f.name
            mlflow.log_artifact(tmp, artifact_path="feature_importance")
            os.unlink(tmp)
        mlflow.sklearn.log_model(model, artifact_path="model",
                                 registered_model_name=f"{reg_name}-rf")
    print(f"  Logged RF      {reg_name}-rf  acc={acc:.3f}")

log_xgb("innings1-only-xgb",   xgb_inn1, xgb_acc_inn1, xgb_auc_inn1, pruned_inn1,  "innings1-only",   xgb_fi_inn1)
log_xgb("innings2-2over-xgb",  xgb_ov2,  xgb_acc_ov2,  xgb_auc_ov2,  pruned_ov2,   "innings2-2over",  xgb_fi_ov2)
log_xgb("innings2-6over-xgb",  xgb_ov6,  xgb_acc_ov6,  xgb_auc_ov6,  pruned_ov6,   "innings2-6over",  xgb_fi_ov6)
log_xgb("innings2-10over-xgb", xgb_ov10, xgb_acc_ov10, xgb_auc_ov10, pruned_ov10,  "innings2-10over", xgb_fi_ov10)
log_xgb("innings2-16over-xgb", xgb_ov16, xgb_acc_ov16, xgb_auc_ov16, pruned_ov16,  "innings2-16over", xgb_fi_ov16)

log_rf("innings1-only-rf",   rf_inn1, rf_acc_inn1, rf_auc_inn1, pruned_inn1,  "innings1-only",   rf_fi_inn1)
log_rf("innings2-2over-rf",  rf_ov2,  rf_acc_ov2,  rf_auc_ov2,  pruned_ov2,   "innings2-2over",  rf_fi_ov2)
log_rf("innings2-6over-rf",  rf_ov6,  rf_acc_ov6,  rf_auc_ov6,  pruned_ov6,   "innings2-6over",  rf_fi_ov6)
log_rf("innings2-10over-rf", rf_ov10, rf_acc_ov10, rf_auc_ov10, pruned_ov10,  "innings2-10over", rf_fi_ov10)
log_rf("innings2-16over-rf", rf_ov16, rf_acc_ov16, rf_auc_ov16, pruned_ov16,  "innings2-16over", rf_fi_ov16)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 10b — Save XGBoost models to blob storage
#
# Saves each pruned XGBoost model alongside its feature list and encodings
# to gold/cricket/ml_features/t20/live_models/win_predictor/ so the
# cricket_display Function App can load them for what-if inference and
# the future cricket_live_ml Function App can run live predictions.
# Written using pickle, same format as the DBFS saves above.
# ═══════════════════════════════════════════════════════════════════

_BLOB_MODEL_PREFIX = "cricket/ml_features/t20/live_models/win_predictor_no_odds"

_checkpoints_to_save = [
    ("innings1-only",   xgb_inn1,  pruned_inn1,  enc_inn1),
    ("innings2-2over",  xgb_ov2,   pruned_ov2,   enc_ov2),
    ("innings2-6over",  xgb_ov6,   pruned_ov6,   enc_ov6),
    ("innings2-10over", xgb_ov10,  pruned_ov10,  enc_ov10),
    ("innings2-16over", xgb_ov16,  pruned_ov16,  enc_ov16),
]

for _name, _model, _features, _encodings in _checkpoints_to_save:
    if _model is None:
        print(f"[blob save] skip {_name} — model is None (too few training samples)")
        continue
    _buf = io.BytesIO()
    pickle.dump({"model": _model, "features": _features, "encodings": _encodings}, _buf)
    _buf.seek(0)
    _path = f"{_BLOB_MODEL_PREFIX}/{_name}.pkl"
    gold.get_blob_client(_path).upload_blob(_buf, overwrite=True)
    print(f"[blob save] gold/{_path}  features={len(_features)}")

# Also save a plain-JSON copy of the innings1-only encodings for human inspection
# (the other checkpoints share the same categorical structure, only numeric medians differ).
if enc_inn1:
    _cat_only = {k: v for k, v in enc_inn1.items() if isinstance(v, dict)}
    gold.get_blob_client(f"{_BLOB_MODEL_PREFIX}/cat_encodings.json").upload_blob(
        json.dumps(_cat_only, indent=2).encode(), overwrite=True
    )
    print(f"[blob save] gold/{_BLOB_MODEL_PREFIX}/cat_encodings.json")

print("Model blob save complete.")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 10c — Model insight plots (SHAP, PDP, Decision Tree)
#
# For each checkpoint generates three PNG files and saves them to:
#   gold/cricket/ml_features/t20/model_insights/win_predictor/latest/
#   gold/cricket/ml_features/t20/model_insights/win_predictor/history/{YYYY-MM-DD}/
#
# Also updates history_index.json so the insights page can list past runs
# and show how the model changed over time.
# ═══════════════════════════════════════════════════════════════════

subprocess.run(["pip", "install", "--quiet", "shap"], check=True)

import shap
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from io import BytesIO as _BytesIO

_INSIGHT_PREFIX = "cricket/ml_features/t20/model_insights/win_predictor_no_odds"
_RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
_RUN_TS   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _upload_fig(fig, path):
    buf = _BytesIO()
    fig.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    buf.seek(0)
    gold.get_blob_client(path).upload_blob(buf.read(), overwrite=True)
    plt.close(fig)

def _save_insight(fig, filename):
    _upload_fig(fig, f"{_INSIGHT_PREFIX}/latest/{filename}")
    _upload_fig(fig, f"{_INSIGHT_PREFIX}/history/{_RUN_DATE}/{filename}")

_insight_checkpoints = [
    ("innings1-only",   xgb_inn1,  pruned_inn1,  xgb_fi_inn1,  xgb_acc_inn1,  xgb_auc_inn1,  enc_inn1),
    ("innings2-2over",  xgb_ov2,   pruned_ov2,   xgb_fi_ov2,   xgb_acc_ov2,   xgb_auc_ov2,   enc_ov2),
    ("innings2-6over",  xgb_ov6,   pruned_ov6,   xgb_fi_ov6,   xgb_acc_ov6,   xgb_auc_ov6,   enc_ov6),
    ("innings2-10over", xgb_ov10,  pruned_ov10,  xgb_fi_ov10,  xgb_acc_ov10,  xgb_auc_ov10,  enc_ov10),
    ("innings2-16over", xgb_ov16,  pruned_ov16,  xgb_fi_ov16,  xgb_acc_ov16,  xgb_auc_ov16,  enc_ov16),
]

_run_accuracy = {}

for _nm, _mdl, _feats, _fi_df, _acc, _auc, _enc in _insight_checkpoints:
    if _mdl is None:
        print(f"[insights] skip {_nm} — model is None")
        continue

    _run_accuracy[_nm] = {"xgb_test_accuracy": _r(_acc), "xgb_test_auc": _r(_auc)}
    print(f"\n[insights] {_nm}  ({len(_feats)} features)")

    _X_tr, _tr_meds = prepare_X(train_df, _feats)
    _X_te, _        = prepare_X(test_df,  _feats, _tr_meds)

    # 1. SHAP Summary Plot
    try:
        _expl     = shap.TreeExplainer(_mdl)
        _shap_arr = _expl.shap_values(_X_te.to_numpy())
        _fig = plt.figure(figsize=(10, 8))
        shap.summary_plot(_shap_arr, _X_te, feature_names=list(_feats),
                          show=False, max_display=20, plot_type="dot")
        plt.title(f"SHAP Feature Impact — {_nm}\n"
                  "Red = high value  Blue = low value   Right = pushes toward Chase",
                  fontsize=11, pad=10)
        plt.tight_layout()
        _save_insight(plt.gcf(), f"shap_summary_{_nm}.png")
        print(f"  [ok] shap summary")
    except Exception as _e:
        print(f"  [warn] shap: {_e}")

    # 2. Partial Dependence Plots — top 5 numeric features
    try:
        from sklearn.inspection import PartialDependenceDisplay
        _cat_set  = {"venue", "inn1_bat_team", "inn1_bowl_team"}
        _fi_rows  = _fi_df.to_dict("records") if _fi_df is not None else []
        _top_feats = [r["feature"] for r in _fi_rows
                      if r["feature"] in list(_feats) and r["feature"] not in _cat_set][:5]
        _feat_idx  = [list(_feats).index(f) for f in _top_feats]
        if _feat_idx:
            _fig, _axes = plt.subplots(1, len(_feat_idx), figsize=(4 * len(_feat_idx) + 1, 4))
            if len(_feat_idx) == 1:
                _axes = [_axes]
            PartialDependenceDisplay.from_estimator(
                _mdl, _X_tr.to_numpy(), _feat_idx,
                feature_names=list(_feats), ax=_axes, kind="average"
            )
            for _ax, _fn in zip(_axes, _top_feats):
                _ax.set_title(_fn, fontsize=9)
                _ax.set_ylabel("P(Chase wins)")
            _fig.suptitle(f"Partial Dependence — {_nm}\n"
                          "Each curve shows effect of ONE feature (others held at median)",
                          fontsize=11)
            plt.tight_layout()
            _save_insight(_fig, f"pdp_{_nm}.png")
            print(f"  [ok] pdp  ({', '.join(_top_feats)})")
    except Exception as _e:
        print(f"  [warn] pdp: {_e}")

    # 3. First Decision Tree (tree #0, depth capped for readability)
    try:
        _fig, _ax = plt.subplots(figsize=(26, 12))
        xgb.plot_tree(_mdl, num_trees=0, ax=_ax, rankdir='LR')
        _ax.set_title(f"Decision Tree #0 — {_nm}\n"
                      "leaf value > 0 → Chase wins,  leaf value < 0 → Defended",
                      fontsize=11)
        _save_insight(_fig, f"tree0_{_nm}.png")
        print(f"  [ok] tree0")
    except Exception as _e:
        print(f"  [warn] tree0: {_e}")

# Update history index
try:
    _idx_path = f"{_INSIGHT_PREFIX}/history_index.json"
    try:
        _idx = json.loads(gold.get_blob_client(_idx_path).download_blob().readall())
    except Exception:
        _idx = {"runs": []}
    _idx["runs"] = [r for r in _idx["runs"] if r.get("date") != _RUN_DATE]
    _idx["runs"].insert(0, {
        "date":          _RUN_DATE,
        "generated_at":  _RUN_TS,
        "train_matches": int(len(train_df)),
        "test_matches":  int(len(test_df)),
        "train_cutoff":  TRAIN_CUTOFF,
        "accuracy":      _run_accuracy,
    })
    gold.get_blob_client(_idx_path).upload_blob(
        json.dumps(_idx, indent=2).encode(), overwrite=True)
    print(f"\n[insights] history index → {len(_idx['runs'])} runs recorded")
except Exception as _e:
    print(f"\n[insights] history index update failed (non-fatal): {_e}")

print("\nStep 10c complete.")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 11 — Write summary to gold
# ═══════════════════════════════════════════════════════════════════



def _r(v): return round(v, 3) if v is not None else None

def _model_entry(name, desc,
                 xgb_acc, xgb_auc, xgb_train_acc, xgb_train_auc,
                 rf_acc,  rf_auc,  rf_train_acc,  rf_train_auc,
                 cb_acc,  cb_auc,  cb_train_acc,  cb_train_auc,
                 hmm_acc, hmm_auc,
                 lstm_acc, lstm_auc,
                 feats, fi_df, preds_records, train_preds_records):
    return {
        "name": name, "description": desc,
        "feature_count": len(feats),
        "xgb": {
            "test_accuracy":  _r(xgb_acc),
            "test_roc_auc":   _r(xgb_auc),
            "train_accuracy": _r(xgb_train_acc),
            "train_roc_auc":  _r(xgb_train_auc),
        },
        "rf": {
            "test_accuracy":  _r(rf_acc),
            "test_roc_auc":   _r(rf_auc),
            "train_accuracy": _r(rf_train_acc),
            "train_roc_auc":  _r(rf_train_auc),
        },
        "cb": {
            "test_accuracy":  _r(cb_acc),
            "test_roc_auc":   _r(cb_auc),
            "train_accuracy": _r(cb_train_acc),
            "train_roc_auc":  _r(cb_train_auc),
        },
        "hmm": {
            "test_accuracy": _r(hmm_acc),
            "test_roc_auc":  _r(hmm_auc),
            "note": "Gaussian HMM — generative sequence model (no training-set accuracy; HMMs are generative, not discriminative)",
        },
        "feature_importance": (
            fi_df[["rank","feature","importance","pct_of_total"]].to_dict("records")
            if fi_df is not None else []
        ),
        "test_predictions":  preds_records,
        "train_predictions": train_preds_records,
    }

summary = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "train_cutoff":     TRAIN_CUTOFF,
    "train_matches":    int(len(train_df)),
    "test_matches":     int(len(test_df)),
    "algorithms": {
        "current": ["XGBoost (pruned)", "CatBoost", "Random Forest", "HMM (Gaussian)", "LSTM (Bidirectional, small-data)"],
        "future":  ["LSTM-XGBoost hybrid", "Transformer at 1000+ matches"],
    },
    "models": [
        _model_entry("innings1-only",   "Full innings-1 breakdown; team, venue and context",
                     xgb_acc_inn1,  xgb_auc_inn1,  xgb_train_acc_inn1,  xgb_train_auc_inn1,
                     rf_acc_inn1,   rf_auc_inn1,   rf_train_acc_inn1,   rf_train_auc_inn1,
                     cb_acc_inn1,   cb_auc_inn1,   cb_train_acc_inn1,   cb_train_auc_inn1,
                     hmm_acc_inn1,  hmm_auc_inn1,
                     lstm_acc_inn1, lstm_auc_inn1,
                     pruned_inn1,   xgb_fi_inn1,   preds_inn1,  train_preds_inn1),
        _model_entry("innings2-2over",  "Innings-1 + chase through over 2",
                     xgb_acc_ov2,   xgb_auc_ov2,   xgb_train_acc_ov2,   xgb_train_auc_ov2,
                     rf_acc_ov2,    rf_auc_ov2,    rf_train_acc_ov2,    rf_train_auc_ov2,
                     cb_acc_ov2,    cb_auc_ov2,    cb_train_acc_ov2,    cb_train_auc_ov2,
                     hmm_acc_ov2,   hmm_auc_ov2,
                     lstm_acc_ov2,  lstm_auc_ov2,
                     pruned_ov2,    xgb_fi_ov2,    preds_ov2,   train_preds_ov2),
        _model_entry("innings2-6over",  "Innings-1 + chase through over 6 (powerplay)",
                     xgb_acc_ov6,   xgb_auc_ov6,   xgb_train_acc_ov6,   xgb_train_auc_ov6,
                     rf_acc_ov6,    rf_auc_ov6,    rf_train_acc_ov6,    rf_train_auc_ov6,
                     cb_acc_ov6,    cb_auc_ov6,    cb_train_acc_ov6,    cb_train_auc_ov6,
                     hmm_acc_ov6,   hmm_auc_ov6,
                     lstm_acc_ov6,  lstm_auc_ov6,
                     pruned_ov6,    xgb_fi_ov6,    preds_ov6,   train_preds_ov6),
        _model_entry("innings2-10over", "Innings-1 + chase through over 10 (halfway)",
                     xgb_acc_ov10,  xgb_auc_ov10,  xgb_train_acc_ov10,  xgb_train_auc_ov10,
                     rf_acc_ov10,   rf_auc_ov10,   rf_train_acc_ov10,   rf_train_auc_ov10,
                     cb_acc_ov10,   cb_auc_ov10,   cb_train_acc_ov10,   cb_train_auc_ov10,
                     hmm_acc_ov10,  hmm_auc_ov10,
                     lstm_acc_ov10, lstm_auc_ov10,
                     pruned_ov10,   xgb_fi_ov10,   preds_ov10,  train_preds_ov10),
        _model_entry("innings2-16over", "Innings-1 + chase through over 16 (death approaching)",
                     xgb_acc_ov16,  xgb_auc_ov16,  xgb_train_acc_ov16,  xgb_train_auc_ov16,
                     rf_acc_ov16,   rf_auc_ov16,   rf_train_acc_ov16,   rf_train_auc_ov16,
                     cb_acc_ov16,   cb_auc_ov16,   cb_train_acc_ov16,   cb_train_auc_ov16,
                     hmm_acc_ov16,  hmm_auc_ov16,
                     lstm_acc_ov16, lstm_auc_ov16,
                     pruned_ov16,   xgb_fi_ov16,   preds_ov16,  train_preds_ov16),
    ],
}

gold.get_blob_client("cricket/ml_features/t20/win_predictor_no_odds_summary.json").upload_blob(
    json.dumps(summary, indent=2).encode(), overwrite=True
)
print(f"Summary → gold/cricket/ml_features/t20/win_predictor_no_odds_summary.json")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 12 — LSTM skeleton
#
# ── WHY LSTM ────────────────────────────────────────────────────────
# XGBoost and Random Forest see all 20 overs simultaneously as a flat
# list of numbers. They have no concept of order — over 1 and over 20
# are equally "near" each other.
#
# An LSTM processes the innings as a TIME SERIES: it reads over 1,
# updates its internal memory, reads over 2, updates again, and so on.
# By over 6 it has already seen what happened in overs 1–5 and can
# weigh over 6's events in that context.
#
# This is exactly what a cricket analyst does — they don't evaluate
# "80 runs in 6 overs" in isolation; they evaluate it knowing whether
# those 80 came smoothly or via 3 dropped catches and 2 no-balls.
#
# ── WHEN TO ACTIVATE ────────────────────────────────────────────────
# ACTIVATE WHEN: len(train_df) >= 500
#
# With fewer matches:
#   - The LSTM has more parameters than training examples → memorises data
#   - XGBoost will reliably outperform it
#   - Feature importances disappear (LSTM is a black box)
#
# With 500+ matches:
#   - LSTM temporal memory becomes a genuine advantage
#   - Can model "team was 30/3 at over 3 but recovered to 80/3 by over 6"
#     in a way flat features cannot
#
# ── DATA SHAPE FOR LSTM ─────────────────────────────────────────────
# Each innings becomes a 3-D tensor:
#   (matches, timesteps, features_per_timestep)
#   e.g. innings1-only: (500, 20, 4)  ← 20 overs × [runs, wkts, bat_odds, bowl_odds]
#
# The per-over data already collected in this pipeline is EXACTLY the
# right format — no restructuring needed.
#
# ── TO ACTIVATE ─────────────────────────────────────────────────────
# 1. Remove the triple-quote block below (uncomment the code)
# 2. Run: pip install torch  (or tensorflow — swap model definition)
# 3. Tune hidden_size and dropout based on validation loss
# ═══════════════════════════════════════════════════════════════════

LSTM_ACTIVATE_THRESHOLD = 500
if len(train_df) >= LSTM_ACTIVATE_THRESHOLD:
    print(f"✓ {len(train_df)} training matches — LSTM threshold met. Uncomment Step 12 code.")
else:
    print(f"✗ LSTM not yet active. Have {len(train_df)} training matches, need {LSTM_ACTIVATE_THRESHOLD}.")
    print(f"  Continue collecting data. XGBoost + Random Forest are the right tools for now.")

"""
# ══════════════════════════════════════════════════════════════════
# LSTM IMPLEMENTATION — UNCOMMENT WHEN TRAINING MATCHES >= 500
# ══════════════════════════════════════════════════════════════════

import subprocess
subprocess.run(["pip", "install", "--quiet", "torch"], check=True)
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

# ── Reshape data into (matches, overs, features_per_over) ─────────
# Each over contributes 4 features: runs, wkts, bat_odds, bowl_odds

def build_lstm_tensor(df_in, innings_num, max_over, fill_median_from=None):
    over_features = []
    for n in range(1, max_over + 1):
        cols = [f"inn{innings_num}_ov{n}_runs", f"inn{innings_num}_ov{n}_wkts",
                f"inn{innings_num}_ov{n}_bat_odds", f"inn{innings_num}_ov{n}_bowl_odds"]
        chunk = df_in[cols].copy()
        if fill_median_from is not None:
            for col in cols:
                chunk[col] = chunk[col].fillna(fill_median_from[col].median())
        else:
            for col in cols:
                chunk[col] = chunk[col].fillna(chunk[col].median())
        over_features.append(chunk.values)   # shape: (matches, 4)
    # Stack to (matches, overs, 4) then transpose to (matches, overs, 4) — already correct
    tensor = np.stack(over_features, axis=1).astype(np.float32)
    return tensor

class CricketLSTM(nn.Module):
    def __init__(self, input_size=4, hidden_size=32, num_layers=2, dropout=0.3):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers,
                            batch_first=True, dropout=dropout)
        self.fc   = nn.Linear(hidden_size, 1)

    def forward(self, x):
        # x: (batch, timesteps, features)
        out, _ = self.lstm(x)
        last    = out[:, -1, :]   # take the final timestep output
        return torch.sigmoid(self.fc(last)).squeeze(1)

# innings1-only LSTM (20 timesteps × 4 features)
X_train_lstm = build_lstm_tensor(train_df, 1, 20)
X_test_lstm  = build_lstm_tensor(test_df,  1, 20, fill_median_from=train_df)
y_train_t    = torch.tensor(train_df["chasing_won"].values, dtype=torch.float32)
y_test_t     = torch.tensor(test_df["chasing_won"].values,  dtype=torch.float32)

dataset  = TensorDataset(torch.tensor(X_train_lstm), y_train_t)
loader   = DataLoader(dataset, batch_size=32, shuffle=True)

lstm_model = CricketLSTM(input_size=4, hidden_size=32, num_layers=2, dropout=0.3)
optimizer  = torch.optim.Adam(lstm_model.parameters(), lr=1e-3)
criterion  = nn.BCELoss()

lstm_model.train()
for epoch in range(100):
    for X_batch, y_batch in loader:
        optimizer.zero_grad()
        loss = criterion(lstm_model(X_batch), y_batch)
        loss.backward()
        optimizer.step()
    if (epoch + 1) % 20 == 0:
        print(f"  Epoch {epoch+1:3d}  loss={loss.item():.4f}")

lstm_model.eval()
with torch.no_grad():
    proba = lstm_model(torch.tensor(X_test_lstm)).numpy()
    preds = (proba >= 0.5).astype(int)

lstm_acc = accuracy_score(y_test_t.numpy(), preds)
lstm_auc = roc_auc_score(y_test_t.numpy(), proba) if len(np.unique(y_test_t.numpy())) > 1 else None
print(f"  LSTM innings1-only — Accuracy={lstm_acc:.3f}  ROC-AUC={lstm_auc:.3f if lstm_auc else 'n/a'}")

# Log LSTM to MLflow
with mlflow.start_run(run_name="innings1-only-lstm"):
    mlflow.log_param("architecture", "LSTM(hidden=32, layers=2, dropout=0.3)")
    mlflow.log_param("input_shape",  "(matches, 20, 4)")
    mlflow.log_metric("test_accuracy", lstm_acc)
    if lstm_auc:
        mlflow.log_metric("test_roc_auc", lstm_auc)
    mlflow.log_metric("train_matches", len(train_df))
    # mlflow.pytorch.log_model(lstm_model, "model")   # uncomment if mlflow.pytorch installed

# ══════════════════════════════════════════════════════════════════
# END LSTM BLOCK
# ══════════════════════════════════════════════════════════════════
"""

print("\nNotebook complete.")
