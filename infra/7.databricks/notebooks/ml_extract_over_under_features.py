# Databricks notebook: ml_extract_over_under_features
#
# Phase 1 of the Over/Under Innings Total Predictor.
#
# Scans all completed T20 matches in gold, extracts checkpoint features and
# labels for two markets:
#
#   1. Innings Total     — checkpoints: inn1 over 2, 4, 6, 8, 10, 12, 14, 16
#   2. First 12 Overs    — checkpoints: inn1 over 2, 4, 6, 8
#
# For each (event, market, checkpoint_over) we record:
#   - match state features at that over  (score, wickets, run rates, betting line, odds)
#   - label (1=OVER, 0=UNDER, skipped for pushes)
#
# Output:
#   gold/ml/over_under_training_data.csv   — one row per (event, market, checkpoint)
#
# Run manually in Databricks whenever you want to refresh the training set.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import sys, os, types, json, csv, io, math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

def _load_from_dbfs(module_name, dbfs_path):
    content = dbutils.fs.head(dbfs_path, 500000)
    mod = types.ModuleType(module_name)
    mod.__file__ = dbfs_path
    sys.modules[module_name] = mod
    exec(compile(content, dbfs_path, "exec"), mod.__dict__)
    return mod

_src = "dbfs:/FileStore/cricket-pipeline/src"

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc    = BlobServiceClient.from_connection_string(conn_str)
gold   = svc.get_container_client("gold")
silver = svc.get_container_client("silver")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

IT_CHECKPOINTS   = [2, 4, 6, 8, 10, 12, 14, 16]   # innings total
F12_CHECKPOINTS  = [2, 4, 6, 8]                    # first 12 overs

# Market template / group identifiers for First 12 Overs in active_markets rows
_F12_GROUP_ID    = "29"
_F12_TMPL_ID     = "30171"
_F12_NAME_SUBSTR = "first 12"   # match against market_group_name.lower()

# Tolerance: accept any row within this many balls of the checkpoint
_BALL_TOLERANCE = 5   # ±5 balls from the target checkpoint ball count

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _download_json(container, path: str) -> Optional[Dict]:
    try:
        return json.loads(container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None


def _over_to_balls(over_str: Optional[str]) -> Optional[int]:
    """Convert '6.3' → 39 balls. Returns None if unparseable."""
    if not over_str:
        return None
    try:
        parts = str(over_str).split(".")
        overs = int(parts[0])
        balls = int(parts[1]) if len(parts) > 1 else 0
        return overs * 6 + balls
    except Exception:
        return None


def _balls_to_over_float(balls: int) -> float:
    return balls // 6 + (balls % 6) / 10.0


def _find_checkpoint_row(
    rows: List[Dict],
    checkpoint_over: int,
    innings: int = 1,
) -> Optional[Dict]:
    """
    Find the tracker row that best represents the state just after
    checkpoint_over complete overs in the given innings.

    Strategy: among rows in the target innings, find the one whose ball
    count is closest to checkpoint_over * 6, within _BALL_TOLERANCE.
    If multiple rows tie, prefer the one with the higher ball count
    (i.e. deepest into the over).
    """
    target_balls = checkpoint_over * 6
    best_row   = None
    best_dist  = _BALL_TOLERANCE + 1

    for r in rows:
        if r.get("innings") != innings:
            continue
        balls = _over_to_balls(r.get("over"))
        if balls is None:
            continue
        dist = abs(balls - target_balls)
        if dist < best_dist or (dist == best_dist and balls > _over_to_balls(best_row.get("over") if best_row else "0")):
            best_dist = dist
            best_row  = r

    return best_row


def _find_score_at_over(rows: List[Dict], target_over: int, innings: int = 1) -> Optional[int]:
    """
    Find the score at the end of target_over (i.e. target_over * 6 balls).
    Returns None if no row found within tolerance.
    """
    row = _find_checkpoint_row(rows, target_over, innings=innings)
    if row is None:
        return None
    return row.get("score")


def _is_t20(rows: List[Dict]) -> bool:
    """True if max completed over in inn1 is between 15 and 20."""
    max_over = 0
    for r in rows:
        if r.get("innings") != 1:
            continue
        try:
            max_over = max(max_over, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    return 15 <= max_over <= 20


def _implied_prob(decimal_odds: Optional[float]) -> Optional[float]:
    """Raw implied probability (1 / decimal_odds). Not margin-adjusted."""
    if not decimal_odds or decimal_odds <= 0:
        return None
    return round(1.0 / decimal_odds, 4)


def _linear_slope(xs: List[float], ys: List[float]) -> Optional[float]:
    """Ordinary least-squares slope for a small list of (x, y) pairs."""
    n = len(xs)
    if n < 2:
        return None
    sum_x  = sum(xs);  sum_y  = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_xx = sum(x * x for x in xs)
    denom  = n * sum_xx - sum_x * sum_x
    if denom == 0:
        return 0.0
    return round((n * sum_xy - sum_x * sum_y) / denom, 4)


# ── HARD RULE: no future-checkpoint leakage ─────────────────────────────────
# A row labeled "as of Over N" must NEVER contain a feature value derived from
# overs after N. This is enforced structurally below: the loop bound is
# `range(1, cp + 1)` — it physically cannot read past `cp`. Do not "helpfully"
# widen this loop or pass a larger cp when extracting a row's own features.
# ml_train_over_under.py runs a runtime guard on the output CSV that re-checks
# this invariant and raises if it is ever violated.

def _extract_trajectory_features(
    inn1_rows: List[Dict],
    cp: int,
    market_total_balls: int = 120,
) -> Dict:
    """
    Extract per-over trajectory features from over 1 through cp ONLY.
    Returns a flat dict covering per-over values (ov{k}_*) and trajectory summaries.
    market_total_balls: 120 for innings_total (20 overs), 72 for first_12.
    """
    result: Dict = {}

    prev_score: Optional[int] = 0
    lines: List[Tuple[int, float]]        = []
    vs_pace_vals: List[Tuple[int, float]] = []
    over_runs: List[float]                = []

    for k in range(1, cp + 1):
        row_k = _find_checkpoint_row(inn1_rows, k, innings=1)

        if row_k is None:
            result[f"ov{k}_runs"]    = None
            result[f"ov{k}_cumwkts"] = None
            result[f"ov{k}_line"]    = None
            result[f"ov{k}_vs_pace"] = None
            prev_score = None
            continue

        score_k   = row_k.get("score")
        wickets_k = row_k.get("wickets") or 0
        balls_k   = _over_to_balls(row_k.get("over")) or (k * 6)
        line_k    = row_k.get("predicted_total")

        runs_k: Optional[float] = None
        if score_k is not None and prev_score is not None:
            runs_k = max(0, score_k - prev_score)
            over_runs.append(float(runs_k))

        result[f"ov{k}_runs"]    = runs_k
        result[f"ov{k}_cumwkts"] = wickets_k
        result[f"ov{k}_line"]    = line_k

        vs_pace_k: Optional[float] = None
        if line_k is not None and score_k is not None and balls_k > 0:
            expected  = float(line_k) * balls_k / market_total_balls
            vs_pace_k = round(score_k - expected, 2)
            vs_pace_vals.append((k, vs_pace_k))
        result[f"ov{k}_vs_pace"] = vs_pace_k

        if line_k is not None:
            lines.append((k, float(line_k)))

        prev_score = score_k

    # ── Line trajectory summaries ─────────────────────────────────────────────
    if lines:
        lkeys = [k for k, _ in lines]
        lvals = [v for _, v in lines]
        result["line_ov1"]          = lvals[0]
        result["line_drift_total"]  = round(lvals[-1] - lvals[0], 2) if len(lvals) >= 2 else 0.0
        result["line_trend_slope"]  = _linear_slope(lkeys, lvals)
        diffs = [lvals[i + 1] - lvals[i] for i in range(len(lvals) - 1)]
        result["pct_overs_line_up"] = round(
            sum(1 for d in diffs if d > 0) / len(diffs), 3
        ) if diffs else None
        result["max_line_jump"]     = round(max((d for d in diffs if d > 0), default=0.0), 2)
        if len(lvals) >= 4:
            mid = len(lvals) // 2
            result["line_accel"] = round((lvals[-1] - lvals[mid]) - (lvals[mid] - lvals[0]), 2)
        else:
            result["line_accel"] = None
    else:
        for _fk in ("line_ov1", "line_drift_total", "line_trend_slope",
                    "pct_overs_line_up", "max_line_jump", "line_accel"):
            result[_fk] = None

    # ── Score-vs-pace trajectory ──────────────────────────────────────────────
    result["score_vs_pace_at_ov2"] = next((v for k, v in vs_pace_vals if k == 2), None)
    result["score_vs_pace_trend"]  = (
        _linear_slope([k for k, _ in vs_pace_vals], [v for _, v in vs_pace_vals])
        if len(vs_pace_vals) >= 2 else None
    )

    # ── Momentum ─────────────────────────────────────────────────────────────
    result["recent_rr_2"]   = round(sum(over_runs[-2:]) / 2, 2) if len(over_runs) >= 2 else None
    result["recent_rr_4"]   = round(sum(over_runs[-4:]) / 4, 2) if len(over_runs) >= 4 else None
    result["max_over_runs"] = max(over_runs) if over_runs else None
    result["min_over_runs"] = min(over_runs) if over_runs else None
    if len(over_runs) >= 4:
        fh_avg = sum(over_runs[: len(over_runs) // 2]) / (len(over_runs) // 2)
        result["rr_trend"] = round(result["recent_rr_2"] - fh_avg, 2) if result["recent_rr_2"] is not None else None
    else:
        result["rr_trend"] = None

    # ── Wicket pattern ────────────────────────────────────────────────────────
    first_wkt = cp + 1
    for k in range(1, cp + 1):
        if (result.get(f"ov{k}_cumwkts") or 0) > 0:
            first_wkt = k
            break
    result["first_wkt_over"] = first_wkt

    # ── Powerplay (always available for cp >= 6) ──────────────────────────────
    if cp >= 6:
        row_pp = _find_checkpoint_row(inn1_rows, 6, innings=1)
        result["pp_score"]   = row_pp.get("score")           if row_pp else None
        result["pp_wickets"] = (row_pp.get("wickets") or 0)  if row_pp else None
    else:
        result["pp_score"]   = None
        result["pp_wickets"] = None

    return result


def _run_rate(score: Optional[int], balls: int) -> Optional[float]:
    if score is None or balls == 0:
        return None
    return round(score * 6 / balls, 3)


def _required_rr(target: float, score: Optional[int], balls_remaining: int) -> Optional[float]:
    """Runs per over needed to reach target from current score."""
    if score is None or balls_remaining <= 0:
        return None
    runs_needed = target - score
    overs_left  = balls_remaining / 6.0
    return round(runs_needed / overs_left, 3)


# ---------------------------------------------------------------------------
# First-12-Overs market extraction from silver active_markets.json
# ---------------------------------------------------------------------------

def _get_first12_line(event_id: str, snapshot_id: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Read silver active_markets.json for a snapshot and return
    (line, over_odds, under_odds) for the "Runs in First 12 Overs" market.
    Returns (None, None, None) if market not present.
    """
    path = f"event_id={event_id}/snapshot_id={snapshot_id}/active_markets.json"
    data = _download_json(silver, path)
    if not data:
        return None, None, None

    rows = data.get("rows", [])

    # Filter to First 12 rows only (grp_id=29, template=30171, name contains "first 12")
    f12_rows = [
        r for r in rows
        if str(r.get("market_group_id") or "") == _F12_GROUP_ID
        and str(r.get("market_template_id") or "") == _F12_TMPL_ID
        and _F12_NAME_SUBSTR in str(r.get("market_group_name") or "").lower()
    ]
    if not f12_rows:
        return None, None, None

    # Among the at-line rows (selection_order == "1" or closest to evens)
    # Find the Over and Under rows, pick the pair with the closest odds to each other.
    by_line: Dict[str, Dict] = {}
    for r in f12_rows:
        sel  = str(r.get("selection_name") or "").strip().lower()
        odds = r.get("odds_decimal")
        line = r.get("handicap") or r.get("line")
        if not line or not odds:
            continue
        try:
            line_val = float(line)
        except Exception:
            continue
        key = str(line_val)
        by_line.setdefault(key, {})
        if sel == "over":
            by_line[key]["over"] = (odds, line_val)
        elif sel == "under":
            by_line[key]["under"] = (odds, line_val)

    best_line_val  = None
    best_over_odds = None
    best_under_odds = None
    best_diff = float("inf")

    for key, sides in by_line.items():
        if "over" not in sides or "under" not in sides:
            continue
        ov_odds, lv = sides["over"]
        un_odds, _  = sides["under"]
        diff = abs(ov_odds - un_odds)
        if diff < best_diff:
            best_diff       = diff
            best_line_val   = lv
            best_over_odds  = ov_odds
            best_under_odds = un_odds

    return best_line_val, best_over_odds, best_under_odds


# ---------------------------------------------------------------------------
# Per-event feature extraction
# ---------------------------------------------------------------------------

def extract_rows_for_event(event_id: str, tracker: Dict, train_cutoff: str = "") -> List[Dict]:
    """
    Build training rows for one completed T20 match.
    Returns a list of dicts, one per (market, checkpoint_over).
    """
    rows_all: List[Dict] = tracker.get("rows") or []
    if not rows_all:
        return []

    # Filter to T20 innings 1 only
    if not _is_t20(rows_all):
        return []

    inn1_rows = [r for r in rows_all if r.get("innings") == 1]
    if not inn1_rows:
        return []

    actual_total: Optional[int] = tracker.get("actual_total")
    outcome: Optional[str]      = tracker.get("outcome")  # "over"/"under"/"push"
    if actual_total is None or outcome not in ("over", "under", "push"):
        return []  # match not fully resolved

    match_date = str(tracker.get("match_date_utc") or "")[:10]   # "2025-09-01"
    if train_cutoff and match_date:
        split = "train" if match_date < train_cutoff else "test"
    else:
        split = "train"

    # Day-of-week — weekend matches can have different crowd/pitch/intent dynamics.
    is_weekend = 0
    try:
        is_weekend = 1 if datetime.strptime(match_date, "%Y-%m-%d").weekday() >= 5 else 0  # Sat=5, Sun=6
    except Exception:
        pass

    # Batting team in inn1 (first row with batting_team set); bowling team is whichever
    # of home/away is not the inn1 batting team.
    batting_team_inn1 = next(
        (r.get("batting_team") for r in inn1_rows if r.get("batting_team")), None
    )
    _home = tracker.get("home_team_name")
    _away = tracker.get("away_team_name")
    if batting_team_inn1 and _home and str(batting_team_inn1).strip() == str(_home).strip():
        bowling_team_inn1 = _away
    elif batting_team_inn1 and _away and str(batting_team_inn1).strip() == str(_away).strip():
        bowling_team_inn1 = _home
    else:
        bowling_team_inn1 = next(
            (r.get("bowling_team") for r in inn1_rows if r.get("bowling_team")), None
        )

    common = {
        "event_id":           event_id,
        "league_id":          tracker.get("league_id"),
        "league_name":        tracker.get("league_name"),
        "match_name":         tracker.get("match_name"),
        "match_date_utc":     tracker.get("match_date_utc"),
        "venue":              tracker.get("venue"),
        "home_team":          tracker.get("home_team_name"),
        "away_team":          tracker.get("away_team_name"),
        "batting_team_inn1":  batting_team_inn1,
        "bowling_team_inn1":  bowling_team_inn1,
        "gender":             tracker.get("gender", "M"),
        "is_weekend_match":   is_weekend,
        "actual_inn1_total":  actual_total,
        "inn1_outcome":       outcome,
        "split":              split,
    }

    output_rows = []

    # ── Market 1: Innings Total ───────────────────────────────────────────────
    for cp in IT_CHECKPOINTS:
        row = _find_checkpoint_row(inn1_rows, cp, innings=1)
        if row is None:
            continue

        line      = row.get("predicted_total")
        ov_odds   = row.get("over_odds_at_line")
        un_odds   = row.get("under_odds_at_line")
        score     = row.get("score")
        wickets   = row.get("wickets")
        balls     = _over_to_balls(row.get("over")) or (cp * 6)

        if line is None or score is None:
            continue

        balls_remaining = max(0, 120 - balls)
        label = (1 if actual_total > line else 0) if outcome != "push" else None
        if label is None:
            continue  # skip pushes for now (rare: actual == exact line)

        _traj = _extract_trajectory_features(inn1_rows, cp, market_total_balls=120)
        output_rows.append({
            **common,
            "market":           "innings_total",
            "checkpoint_over":  cp,
            "over_str":         row.get("over"),
            "balls_completed":  balls,
            "balls_remaining":  balls_remaining,
            "score":            score,
            "wickets":          wickets,
            "wickets_in_hand":  (10 - (wickets or 0)),
            "betting_line":     line,
            "score_vs_line_pace": round(score - line * balls / 120, 2) if line else None,
            "run_rate":         _run_rate(score, balls),
            "rr_required":      _required_rr(line, score, balls_remaining),
            "over_odds":        ov_odds,
            "under_odds":       un_odds,
            "implied_prob_over": _implied_prob(ov_odds),
            "batting_team_win_odds":  row.get("batting_team_odds"),
            "bowling_team_win_odds":  row.get("bowling_team_odds"),
            "snapshot_id":      row.get("snapshot_id"),
            "actual_value":     actual_total,
            "actual_margin":    round(actual_total - line, 1),
            "label":            label,
            **_traj,
        })

    # ── Market 2: First 12 Overs ─────────────────────────────────────────────
    # Actual first-12 score = batting score at end of over 12
    actual_first12 = _find_score_at_over(inn1_rows, 12, innings=1)

    for cp in F12_CHECKPOINTS:
        row = _find_checkpoint_row(inn1_rows, cp, innings=1)
        if row is None:
            continue

        snapshot_id = row.get("snapshot_id")
        if not snapshot_id:
            continue

        line, ov_odds, un_odds = _get_first12_line(event_id, snapshot_id)
        if line is None:
            continue  # market not available at this checkpoint

        score   = row.get("score")
        wickets = row.get("wickets")
        balls   = _over_to_balls(row.get("over")) or (cp * 6)

        if score is None or actual_first12 is None:
            continue

        # Target is 12 overs = 72 balls
        balls_remaining_in_12 = max(0, 72 - balls)
        label = (1 if actual_first12 > line else 0) if actual_first12 != line else None
        if label is None:
            continue

        _traj = _extract_trajectory_features(inn1_rows, cp, market_total_balls=72)
        output_rows.append({
            **common,
            "market":           "first_12_overs",
            "checkpoint_over":  cp,
            "over_str":         row.get("over"),
            "balls_completed":  balls,
            "balls_remaining":  balls_remaining_in_12,
            "score":            score,
            "wickets":          wickets,
            "wickets_in_hand":  (10 - (wickets or 0)),
            "betting_line":     line,
            "score_vs_line_pace": round(score - line * balls / 72, 2),
            "run_rate":         _run_rate(score, balls),
            "rr_required":      _required_rr(line, score, balls_remaining_in_12),
            "over_odds":        ov_odds,
            "under_odds":       un_odds,
            "implied_prob_over": _implied_prob(ov_odds),
            "batting_team_win_odds":  row.get("batting_team_odds"),
            "bowling_team_win_odds":  row.get("bowling_team_odds"),
            "snapshot_id":      snapshot_id,
            "actual_value":     actual_first12,
            "actual_margin":    round(actual_first12 - line, 1),
            "label":            label,
            **_traj,
        })

    return output_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# COMMAND ----------

dbutils.widgets.text("event_id", "")
try:
    event_id_filter = dbutils.widgets.get("event_id").strip() or None
except Exception:
    event_id_filter = None

print(f"[ml_extract] event_id_filter={event_id_filter or '(all T20 completed matches)'}")

# COMMAND ----------

# Load train/test split config from gold/ml/train_config.json — the single
# shared cutoff used by every ML notebook in pl_ml_and_hypothesis.
# Format: {"train_cutoff_date": "2025-12-31"}
# Rows with match_date_utc < train_cutoff_date → split="train", else → split="test"
# If config absent, all rows are "train".
_train_config = _download_json(gold, "ml/train_config.json") or {}
_train_cutoff  = (_train_config.get("train_cutoff_date") or "").strip()
print(f"[ml_extract] train_cutoff_date={_train_cutoff or '(none — all rows = train)'}")

# COMMAND ----------

started_at = datetime.now(timezone.utc)

# Collect event_ids with a gold innings_tracker
if event_id_filter:
    event_ids = [event_id_filter]
else:
    event_ids = []
    for blob in gold.list_blobs(name_starts_with="event_id="):
        if not blob.name.endswith("/innings_tracker.json"):
            continue
        parts = blob.name.split("/")
        eid_part = next((p for p in parts if p.startswith("event_id=")), None)
        if eid_part:
            event_ids.append(eid_part[9:])

print(f"[ml_extract] {len(event_ids)} events found in gold")

# COMMAND ----------

all_rows    = []
n_processed = 0
n_skipped   = 0

for eid in event_ids:
    tracker = _download_json(gold, f"event_id={eid}/innings_tracker.json")
    if not tracker:
        n_skipped += 1
        continue

    rows = extract_rows_for_event(eid, tracker, train_cutoff=_train_cutoff)
    if rows:
        all_rows.extend(rows)
        n_processed += 1
    else:
        n_skipped += 1

print(f"[ml_extract] Processed {n_processed} events, skipped {n_skipped}")
print(f"[ml_extract] Total training rows: {len(all_rows)}")

# COMMAND ----------

if not all_rows:
    print("[ml_extract] No rows produced — check that completed T20 matches exist in gold.")
    dbutils.notebook.exit("no_data")

# Write as CSV to gold/ml/over_under_training_data.csv
_TRAJ_SUMMARY_FIELDS = [
    "line_ov1", "line_drift_total", "line_trend_slope", "pct_overs_line_up",
    "max_line_jump", "line_accel",
    "score_vs_pace_at_ov2", "score_vs_pace_trend",
    "recent_rr_2", "recent_rr_4", "rr_trend",
    "max_over_runs", "min_over_runs", "first_wkt_over",
    "pp_score", "pp_wickets",
]
_PER_OVER_FIELDS = [
    f"ov{k}_{s}"
    for k in range(1, 17)
    for s in ("runs", "cumwkts", "line", "vs_pace")
]

fieldnames = [
    "event_id", "league_id", "league_name", "match_name", "match_date_utc", "venue",
    "home_team", "away_team", "batting_team_inn1", "bowling_team_inn1", "gender", "is_weekend_match", "split",
    "market", "checkpoint_over", "over_str", "snapshot_id",
    "balls_completed", "balls_remaining",
    "score", "wickets", "wickets_in_hand",
    "betting_line", "score_vs_line_pace", "run_rate", "rr_required",
    "over_odds", "under_odds", "implied_prob_over",
    "batting_team_win_odds", "bowling_team_win_odds",
    "actual_inn1_total", "inn1_outcome",
    "actual_value", "actual_margin",
    "label",
    *_TRAJ_SUMMARY_FIELDS,
    *_PER_OVER_FIELDS,
]

buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
writer.writeheader()
writer.writerows(all_rows)
csv_bytes = buf.getvalue().encode("utf-8")

output_path = "ml/over_under_training_data.csv"
gold.get_blob_client(output_path).upload_blob(csv_bytes, overwrite=True)
print(f"[ml_extract] Written: gold/{output_path}  ({len(csv_bytes):,} bytes, {len(all_rows)} rows)")

# COMMAND ----------

elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()

# Summary by market × checkpoint
from collections import defaultdict
counts = defaultdict(int)
label_counts = defaultdict(lambda: {"over": 0, "under": 0})
for r in all_rows:
    key = (r["market"], r["checkpoint_over"])
    counts[key] += 1
    label_counts[key]["over" if r["label"] == 1 else "under"] += 1

n_train = sum(1 for r in all_rows if r.get("split") == "train")
n_test  = sum(1 for r in all_rows if r.get("split") == "test")
print(f"\n── Summary ── {elapsed:.0f}s")
print(f"Split: train={n_train}  test={n_test}  (cutoff={_train_cutoff or 'none'})")
print(f"{'Market':<20} {'CP':>4} {'N':>6} {'OVER%':>7}")
print("-" * 42)
for (mkt, cp) in sorted(counts.keys()):
    n  = counts[(mkt, cp)]
    ov = label_counts[(mkt, cp)]["over"]
    print(f"{mkt:<20} {cp:>4} {n:>6}  {100*ov/n:>5.1f}%")

dbutils.notebook.exit(json.dumps({
    "events_processed":  n_processed,
    "total_rows":        len(all_rows),
    "n_train":           n_train,
    "n_test":            n_test,
    "duration_seconds":  round(elapsed, 1),
}))
