# Databricks notebook: ml — feature extraction
# Reads all ended-match gold/cricket/innings_tracker/ files (T20 only).
# Flattens rows[] into one row per gold snapshot with engineered features.
# Writes Parquet to gold/cricket/ml_features/t20/features.parquet
#
# Run manually or as Activity 1 in pl_ml_retrain (weekly).
# Re-running is always safe — output is a full overwrite.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob", "pyarrow", "pandas"], check=True)

# COMMAND ----------

import json, io
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc  = BlobServiceClient.from_connection_string(conn_str)
gold = svc.get_container_client("gold")

def _dl(path):
    try:
        return json.loads(gold.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

# Load the shared train/test cutoff (same blob used by every ML notebook).
# Rows with match_date_utc < cutoff -> split="train", else split="test".
# If absent, every row is "train" (no held-out set yet).
_train_config = _dl("ml/train_config.json") or {}
TRAIN_CUTOFF  = (_train_config.get("train_cutoff_date") or "").strip()
print(f"[ml_feature_extraction] train_cutoff_date={TRAIN_CUTOFF or '(none — all rows = train)'}")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 1 — Discover all ended-match gold tracker files
# ═══════════════════════════════════════════════════════════════════

blobs = [
    b.name for b in gold.list_blobs(name_starts_with="event_id=")
    if b.name.endswith("/innings_tracker.json")
]

print(f"Discovered {len(blobs)} ended-match tracker files")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 2 — Load all trackers in parallel
# ═══════════════════════════════════════════════════════════════════

trackers = []
with ThreadPoolExecutor(max_workers=32) as ex:
    futs = {ex.submit(_dl, b): b for b in blobs}
    for fut in as_completed(futs):
        t = fut.result()
        if t:
            trackers.append(t)

print(f"Loaded {len(trackers)} tracker files")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 3 — Filter to T20 and flatten rows into feature DataFrame
# ═══════════════════════════════════════════════════════════════════

def _parse_ball_window(bw):
    fours = sixes = wickets = dots = extras = singles = doubles = 0
    for b in (bw or []):
        s = str(b).lower().strip()
        if any(x in s for x in ("wd", "nb", "lb")):
            extras += 1
        elif s == "w":
            wickets += 1
            dots += 1
        elif s == "0":
            dots += 1
        elif s == "4":
            fours += 1
        elif s == "6":
            sixes += 1
        elif s == "1":
            singles += 1
        elif s == "2":
            doubles += 1
    return fours, sixes, wickets, dots, extras, singles, doubles

all_rows = []
stats = {"skipped_not_t20": 0, "skipped_no_label": 0, "matches_used": 0}

for t in trackers:
    rows = t.get("rows") or []
    if not rows:
        continue

    # T20 detection: max over across all rows must be in 15–20 range
    max_over = 0
    for r in rows:
        try:
            max_over = max(max_over, int(str(r.get("over") or "0").split(".")[0]))
        except Exception:
            pass
    if not (15 <= max_over <= 20):
        stats["skipped_not_t20"] += 1
        continue

    actual_total = t.get("actual_total")
    outcome      = t.get("outcome")
    # Exclude "push" — ambiguous label for binary classification
    if actual_total is None or outcome not in ("over", "under"):
        stats["skipped_no_label"] += 1
        continue

    event_id      = str(t.get("event_id") or "")
    match_name    = str(t.get("match_name") or "")
    league        = str(t.get("league_name") or "")
    home_team     = str(t.get("home_team_name") or "")
    away_team     = str(t.get("away_team_name") or "")
    outcome_bin   = 1 if outcome == "over" else 0
    is_womens     = 1 if ("women" in match_name.lower() or "(w)" in match_name.lower()) else 0
    match_date    = str(t.get("match_date_utc") or "")[:10]
    split         = ("train" if (not TRAIN_CUTOFF or not match_date or match_date < TRAIN_CUTOFF)
                      else "test")
    stats["matches_used"] += 1

    for r in rows:
        over_str = str(r.get("over") or "0")
        try:
            parts       = over_str.split(".")
            over_num    = int(parts[0])
            balls_in_ov = int(parts[1]) if len(parts) > 1 else 0
        except Exception:
            continue

        balls_elapsed   = over_num * 6 + balls_in_ov
        balls_remaining = 120 - balls_elapsed
        if balls_elapsed == 0:
            continue  # skip pre-first-ball rows

        score   = r.get("score") or 0
        wickets = r.get("wickets") or 0
        innings = r.get("innings") or 1

        run_rate   = round(score / (balls_elapsed / 6), 4) if balls_elapsed > 0 else 0.0
        proj_score = round(score / balls_elapsed * 120, 2) if balls_elapsed > 0 else 0.0

        bat_odds   = r.get("batting_team_odds")
        bowl_odds  = r.get("bowling_team_odds")
        imp_prob   = round(1 / bat_odds, 4) if bat_odds and bat_odds > 0 else None
        pred_total = r.get("predicted_total")
        ov_odds    = r.get("over_odds_at_line")
        un_odds    = r.get("under_odds_at_line")
        score_vs_line = round(score - pred_total, 2) if pred_total is not None else None

        f4, f6, fw, fd, fx, f1, f2 = _parse_ball_window(r.get("ball_window"))

        all_rows.append({
            # identifiers (not model features — used for GroupKFold and debugging)
            "event_id":          event_id,
            "match_name":        match_name,
            "league":            league,
            "home_team":         home_team,
            "away_team":         away_team,
            "batting_team":      str(r.get("batting_team") or ""),
            "match_date_utc":    match_date,
            "split":             split,
            # match-level flags
            "is_womens_match":   is_womens,
            # snapshot position
            "innings":           innings,
            "over":              over_str,
            "over_num":          over_num,
            "balls_in_over":     balls_in_ov,
            "balls_elapsed":     balls_elapsed,
            "balls_remaining":   balls_remaining,
            # match state
            "score":             score,
            "wickets":           wickets,
            # derived
            "run_rate":          run_rate,
            "projected_score":   proj_score,
            # bookmaker signals
            "batting_team_odds": bat_odds,
            "bowling_team_odds": bowl_odds,
            "implied_prob_bat":  imp_prob,
            "predicted_total":   pred_total,
            "over_odds":         ov_odds,
            "under_odds":        un_odds,
            "score_vs_line":     score_vs_line,
            # ball window decomposed
            "bw_fours":          f4,
            "bw_sixes":          f6,
            "bw_wickets":        fw,
            "bw_dots":           fd,
            "bw_extras":         fx,
            "bw_singles":        f1,
            "bw_doubles":        f2,
            # labels
            "actual_total":      actual_total,
            "outcome":           outcome,
            "outcome_bin":       outcome_bin,
        })

df = pd.DataFrame(all_rows)

print(f"\n{'='*55}")
print(f"  Matches used        : {stats['matches_used']}")
print(f"  Skipped (not T20)   : {stats['skipped_not_t20']}")
print(f"  Skipped (no label)  : {stats['skipped_no_label']}")
print(f"  Total feature rows  : {len(df):,}")
if not df.empty:
    print(f"  Innings 1 rows      : {(df['innings']==1).sum():,}")
    print(f"  Innings 2 rows      : {(df['innings']==2).sum():,}")
    print(f"  Outcome split       : {dict(df['outcome'].value_counts())}")
    print(f"  Train/test split    : {dict(df['split'].value_counts())}  (cutoff={TRAIN_CUTOFF or 'none'})")
print(f"{'='*55}\n")
if not df.empty:
    display(df.describe())

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════════
# STEP 4 — Write Parquet to gold
# ═══════════════════════════════════════════════════════════════════

if df.empty:
    print("No data to write — skipping Parquet upload.")
else:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    out_path = "cricket/ml_features/t20/features.parquet"
    gold.get_blob_client(out_path).upload_blob(buf, overwrite=True)

    print(f"Written {len(df):,} rows  →  gold/{out_path}")
    print(f"\nColumn dtypes:\n{df.dtypes.to_string()}")
