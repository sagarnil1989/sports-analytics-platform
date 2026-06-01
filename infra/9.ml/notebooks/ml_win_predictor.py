# Databricks notebook: ml — win predictor (v3)
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
# Train : match_date_utc < 2026-05-23
# Test  : match_date_utc >= 2026-05-23

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "pyarrow", "pandas",
    "xgboost", "scikit-learn",
], check=True)

# COMMAND ----------

import json, io, tempfile, os
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
TRAIN_CUTOFF          = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
IMPORTANCE_THRESHOLD  = 0.005   # drop features below 0.5% of total importance

def _dl(path):
    try:
        return json.loads(gold.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 1 — Load all T20 gold trackers
# ═══════════════════════════════════════════════════════════════════

prefix = "cricket/innings_tracker/"
blobs  = [b.name for b in gold.list_blobs(name_starts_with=prefix)
          if b.name.endswith("innings_1_from_silver.json")]

trackers = []
with ThreadPoolExecutor(max_workers=32) as ex:
    futs = {ex.submit(_dl, b): b for b in blobs}
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

def parse_score_summary(tracker, inn1_bat_team):
    """
    Parse the authoritative final scores from the tracker's score_summary field.
    This is the same source used by the ended/view page — set from the Bet365
    events API, not from snapshot rows. Format after normalisation: "inn1,inn2"
    where each part is "runs/wickets" e.g. "196/4,87/10".

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

    rec = {
        "event_id":   event_id,  "match_name": match_name,
        "match_date": date_str,  "split":      split,
        # categorical
        "venue":          venue,
        "inn1_bat_team":  inn1_bat_team  or "unknown",
        "inn1_bowl_team": inn1_bowl_team or "unknown",
        # match-level flags
        "is_womens_match": is_womens,
        # innings 1 aggregate
        "inn1_total_score":   inn1_total_score,
        "inn1_total_wickets": inn1_total_wickets,
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

df = pd.DataFrame(records)

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 4 — Composite features
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
for col in ["venue", "inn1_bat_team", "inn1_bowl_team"]:
    df[col] = df[col].astype("category")

train_df = df[df["split"] == "train"].reset_index(drop=True)
test_df  = df[df["split"] == "test"].reset_index(drop=True)

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

CAT_FEATURES = ["venue", "inn1_bat_team", "inn1_bowl_team"]
MATCH_FLAGS  = ["is_womens_match"]

COMPOSITE_INN1 = [
    "inn1_pp_rp_wkt",          # runs per wicket in hand at end of powerplay
    "inn1_pp_wickets",         # wickets lost in powerplay (overs 1–6) — standalone
    "inn1_mid_rp_wkt",         # runs per wicket in hand through middle overs
    "inn1_mid_wickets_only",   # wickets lost in middle overs only (overs 7–15) — standalone
    "inn1_death_runs",         # runs in death overs (16–20)
    "inn1_death_wickets",      # wickets lost in death overs (16–20)
    "inn1_pressure",           # total runs per wicket in hand — overall innings quality
    "inn1_odds_swing_full",    # market movement across all 20 overs
    "inn1_odds_swing_pp",      # market movement across powerplay only
    "inn1_bat_dominance",      # max_SR − avg_SR across inn1 batsmen — one player carrying vs collective effort
]

COMPOSITE_INN2_OV2 = [
    "inn2_ov2_rp_wkt",               # chase runs per wicket in hand at over 2
    "inn2_ov2_odds_swing",           # market shift from innings 1 end to chase over 2
    "inn2_ov2_runs_needed_per_wkt",  # runs still needed per wicket remaining — chase pressure at over 2
    "inn2_ov2_chase_difficulty",     # RRR × (wickets_fallen+1) — joint signal for near-impossible chases
]

COMPOSITE_INN2_OV6 = [
    "inn2_ov6_rp_wkt",               # chase runs per wicket in hand at over 6
    "inn2_ov6_odds_swing",           # market shift from innings 1 end to chase over 6
    "inn2_ov6_runs_needed_per_wkt",  # runs still needed per wicket remaining — chase pressure at over 6
    "inn2_ov6_chase_difficulty",     # RRR × (wickets_fallen+1) — joint signal for near-impossible chases
]

COMPOSITE_INN2_OV10 = [
    "inn2_ov10_rp_wkt",
    "inn2_ov10_odds_swing",
    "inn2_ov10_runs_needed_per_wkt",
    "inn2_ov10_chase_difficulty",    # RRR × (wickets_fallen+1) — joint signal for near-impossible chases
]

COMPOSITE_INN2_OV16 = [
    "inn2_ov16_rp_wkt",
    "inn2_ov16_odds_swing",
    "inn2_ov16_runs_needed_per_wkt",
    "inn2_ov16_chase_difficulty",    # RRR × (wickets_fallen+1) — joint signal for near-impossible chases
]

# Raw per-over features (runs, wkts, bat_odds, bowl_odds per over)
RAW_INN1 = (
    ["inn1_total_score", "inn1_total_wickets"] +
    [f"inn1_ov{n}_runs"      for n in range(1, 21)] +
    [f"inn1_ov{n}_wkts"      for n in range(1, 21)] +
    [f"inn1_ov{n}_bat_odds"  for n in range(1, 21)] +
    [f"inn1_ov{n}_bowl_odds" for n in range(1, 21)]
)

INN1_BASE = CAT_FEATURES + MATCH_FLAGS + COMPOSITE_INN1 + RAW_INN1

CHASE_OV2 = (
    COMPOSITE_INN2_OV2 +
    [f"inn2_ov{n}_runs"      for n in range(1, 3)] +
    [f"inn2_ov{n}_wkts"      for n in range(1, 3)] +
    [f"inn2_ov{n}_bat_odds"  for n in range(1, 3)] +
    [f"inn2_ov{n}_bowl_odds" for n in range(1, 3)] +
    ["inn2_ov2_score", "inn2_ov2_wickets",
     "inn2_ov2_crr",   "inn2_ov2_rrr",
     "inn2_ov2_rr_diff", "inn2_ov2_runs_needed"]
)

CHASE_OV6 = (
    COMPOSITE_INN2_OV6 +
    [f"inn2_ov{n}_runs"      for n in range(1, 7)] +
    [f"inn2_ov{n}_wkts"      for n in range(1, 7)] +
    [f"inn2_ov{n}_bat_odds"  for n in range(1, 7)] +
    [f"inn2_ov{n}_bowl_odds" for n in range(1, 7)] +
    ["inn2_ov6_score", "inn2_ov6_wickets",
     "inn2_ov6_crr",   "inn2_ov6_rrr",
     "inn2_ov6_rr_diff", "inn2_ov6_runs_needed"]
)

CHASE_OV10 = (
    COMPOSITE_INN2_OV10 +
    [f"inn2_ov{n}_runs"      for n in range(1, 11)] +
    [f"inn2_ov{n}_wkts"      for n in range(1, 11)] +
    [f"inn2_ov{n}_bat_odds"  for n in range(1, 11)] +
    [f"inn2_ov{n}_bowl_odds" for n in range(1, 11)] +
    ["inn2_ov10_score", "inn2_ov10_wickets",
     "inn2_ov10_crr",   "inn2_ov10_rrr",
     "inn2_ov10_rr_diff", "inn2_ov10_runs_needed"]
)

CHASE_OV16 = (
    COMPOSITE_INN2_OV16 +
    [f"inn2_ov{n}_runs"      for n in range(1, 17)] +
    [f"inn2_ov{n}_wkts"      for n in range(1, 17)] +
    [f"inn2_ov{n}_bat_odds"  for n in range(1, 17)] +
    [f"inn2_ov{n}_bowl_odds" for n in range(1, 17)] +
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
    X = df_in[feature_cols].copy()
    medians = {}
    for col in feature_cols:
        if pd.api.types.is_categorical_dtype(X[col]) or X[col].dtype == object:
            # use stored encoder from train pass, or build one from this column
            enc = (train_medians or {}).get(col)
            if not isinstance(enc, dict):
                _, uniq = pd.factorize(X[col])
                enc = {v: i for i, v in enumerate(uniq)}
            X[col] = X[col].map(enc).fillna(-1).astype(int)
            medians[col] = enc
        else:
            fill = (train_medians or {}).get(col)
            if fill is None or isinstance(fill, dict):
                fill = X[col].median()
            X[col] = X[col].fillna(fill)
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
        "inn1_total_score",
        "inn2_ov2_score", "inn2_ov2_wickets",
        "inn2_ov6_score", "inn2_ov6_wickets",
        "inn2_ov10_score", "inn2_ov10_wickets",
        "inn2_ov16_score", "inn2_ov16_wickets",
    ] if c in test_df.columns]

    # Test predictions
    preds = test_df[["event_id", "match_name", "match_date", "inn1_bat_team", "inn1_bowl_team",
                      "chasing_won"] + score_ctx].copy()
    preds["predicted"] = y_pred
    preds["confidence_pct"] = [
        round(p * 100, 1) if pred == 1 else round((1 - p) * 100, 1)
        for p, pred in zip(y_proba, y_pred)
    ]
    preds["correct"] = (y_pred == y_test).map({True: True, False: False})
    preds_records = preds.rename(columns={"inn1_total_score": "inn1_score"}).to_dict("records")

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
    train_preds_records = train_preds.rename(columns={"inn1_total_score": "inn1_score"}).to_dict("records")

    return m2, acc, auc, train_acc, train_auc, fi_df, kept, preds_records, train_preds_records

if len(train_df) < 5:
    raise ValueError(f"Only {len(train_df)} training matches — need at least 5.")
if len(test_df) < 2:
    raise ValueError(f"Only {len(test_df)} test matches — need at least 2.")

xgb_inn1,  xgb_acc_inn1,  xgb_auc_inn1,  xgb_train_acc_inn1,  xgb_train_auc_inn1,  xgb_fi_inn1,  pruned_inn1,  preds_inn1,  train_preds_inn1  = xgb_train_pruned("innings1-only",   INN1_FEATURES,      train_df, test_df)
xgb_ov2,   xgb_acc_ov2,   xgb_auc_ov2,   xgb_train_acc_ov2,   xgb_train_auc_ov2,   xgb_fi_ov2,   pruned_ov2,   preds_ov2,   train_preds_ov2   = xgb_train_pruned("innings2-2over",  INN2_OV2_FEATURES,  train_df, test_df)
xgb_ov6,   xgb_acc_ov6,   xgb_auc_ov6,   xgb_train_acc_ov6,   xgb_train_auc_ov6,   xgb_fi_ov6,   pruned_ov6,   preds_ov6,   train_preds_ov6   = xgb_train_pruned("innings2-6over",  INN2_OV6_FEATURES,  train_df, test_df)
xgb_ov10,  xgb_acc_ov10,  xgb_auc_ov10,  xgb_train_acc_ov10,  xgb_train_auc_ov10,  xgb_fi_ov10,  pruned_ov10,  preds_ov10,  train_preds_ov10  = xgb_train_pruned("innings2-10over", INN2_OV10_FEATURES, train_df, test_df)
xgb_ov16,  xgb_acc_ov16,  xgb_auc_ov16,  xgb_train_acc_ov16,  xgb_train_auc_ov16,  xgb_fi_ov16,  pruned_ov16,  preds_ov16,  train_preds_ov16  = xgb_train_pruned("innings2-16over", INN2_OV16_FEATURES, train_df, test_df)

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

    # RF does not support pandas Categorical — label encode for it
    cat_cols = [c for c in feature_cols if pd.api.types.is_categorical_dtype(train_df[c])]
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
# STEP 9 — Algorithm comparison table
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
mlflow.set_experiment(f"/Users/{current_user}/cricket-win-predictor")

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
# STEP 11 — Write summary to gold
# ═══════════════════════════════════════════════════════════════════



def _r(v): return round(v, 3) if v is not None else None

def _model_entry(name, desc,
                 xgb_acc, xgb_auc, xgb_train_acc, xgb_train_auc,
                 rf_acc,  rf_auc,  rf_train_acc,  rf_train_auc,
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
        "current": ["XGBoost (pruned)", "Random Forest"],
        "future":  ["LSTM — activate at 500+ matches, see notebook Step 12"],
    },
    "models": [
        _model_entry("innings1-only",   "Full innings-1 breakdown; team and venue",
                     xgb_acc_inn1,  xgb_auc_inn1,  xgb_train_acc_inn1,  xgb_train_auc_inn1,
                     rf_acc_inn1,   rf_auc_inn1,   rf_train_acc_inn1,   rf_train_auc_inn1,
                     pruned_inn1,   xgb_fi_inn1,   preds_inn1,  train_preds_inn1),
        _model_entry("innings2-2over",  "Innings-1 + chase through over 2",
                     xgb_acc_ov2,   xgb_auc_ov2,   xgb_train_acc_ov2,   xgb_train_auc_ov2,
                     rf_acc_ov2,    rf_auc_ov2,    rf_train_acc_ov2,    rf_train_auc_ov2,
                     pruned_ov2,    xgb_fi_ov2,    preds_ov2,   train_preds_ov2),
        _model_entry("innings2-6over",  "Innings-1 + chase through over 6 (powerplay)",
                     xgb_acc_ov6,   xgb_auc_ov6,   xgb_train_acc_ov6,   xgb_train_auc_ov6,
                     rf_acc_ov6,    rf_auc_ov6,    rf_train_acc_ov6,    rf_train_auc_ov6,
                     pruned_ov6,    xgb_fi_ov6,    preds_ov6,   train_preds_ov6),
        _model_entry("innings2-10over", "Innings-1 + chase through over 10 (halfway)",
                     xgb_acc_ov10,  xgb_auc_ov10,  xgb_train_acc_ov10,  xgb_train_auc_ov10,
                     rf_acc_ov10,   rf_auc_ov10,   rf_train_acc_ov10,   rf_train_auc_ov10,
                     pruned_ov10,   xgb_fi_ov10,   preds_ov10,  train_preds_ov10),
        _model_entry("innings2-16over", "Innings-1 + chase through over 16 (death approaching)",
                     xgb_acc_ov16,  xgb_auc_ov16,  xgb_train_acc_ov16,  xgb_train_auc_ov16,
                     rf_acc_ov16,   rf_auc_ov16,   rf_train_acc_ov16,   rf_train_auc_ov16,
                     pruned_ov16,   xgb_fi_ov16,   preds_ov16,  train_preds_ov16),
    ],
}

gold.get_blob_client("cricket/ml_features/t20/win_predictor_summary.json").upload_blob(
    json.dumps(summary, indent=2).encode(), overwrite=True
)
print(f"Summary → gold/cricket/ml_features/t20/win_predictor_summary.json")

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
