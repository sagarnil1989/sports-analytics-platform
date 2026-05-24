# Databricks notebook: analysis_snapshot_timeline
# Declare an event_id and see every silver-processed snapshot linked to match state.
# Shows score progression, over-by-over, ball windows, market counts, and dedup gaps.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob", "matplotlib"], check=True)

# COMMAND ----------

dbutils.widgets.text("event_id", "94912171", "Event ID")
EVENT_ID = dbutils.widgets.get("event_id").strip()
print(f"Event ID: {EVENT_ID}")

# COMMAND ----------

import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc      = BlobServiceClient.from_connection_string(conn_str)
bronze   = svc.get_container_client("bronze")
silver   = svc.get_container_client("silver")

def _dl(container, path):
    try:
        return json.loads(container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

# COMMAND ----------
# ── 1. Find all processed-snapshot markers for this event ─────────────────────

prefix = "cricket/control/processed_snapshots/"
markers = []
for blob in silver.list_blobs(name_starts_with=prefix):
    fname = blob.name.split("/")[-1]          # e.g. 20260501T123456Z_11658820_194752823.json
    parts = fname.replace(".json", "").split("_")
    if len(parts) >= 2 and parts[1] == EVENT_ID:
        markers.append({
            "marker_path": blob.name,
            "snapshot_id": parts[0],
            "event_id":    parts[1],
            "fi":          parts[2] if len(parts) > 2 else None,
            "last_modified": blob.last_modified,
        })

markers.sort(key=lambda x: x["snapshot_id"])
print(f"Processed snapshots found: {len(markers)}")
if not markers:
    dbutils.notebook.exit(f"No processed snapshots found for event_id={EVENT_ID}")

# COMMAND ----------
# ── 2. Load marker payloads + innings_snapshot in parallel ────────────────────

def _load_marker(m):
    doc = _dl(silver, m["marker_path"]) or {}
    base = doc.get("silver_base_path", "").lstrip("silver/")
    snap = _dl(silver, f"{base}/innings_snapshot.json") if base else None
    return {**m, "marker": doc, "innings_snap": snap}

rows = []
with ThreadPoolExecutor(max_workers=64) as ex:
    futs = {ex.submit(_load_marker, m): m for m in markers}
    for fut in as_completed(futs):
        rows.append(fut.result())

rows.sort(key=lambda x: x["snapshot_id"])

# COMMAND ----------
# ── 3. Load bronze manifests for this event (to detect dedup gaps) ────────────

bronze_prefix = f"betsapi/inplay_snapshot/sport_id=3/event_id={EVENT_ID}/"
bronze_snapshots = []
for blob in bronze.list_blobs(name_starts_with=bronze_prefix):
    if blob.name.endswith("/manifest.json"):
        parts = blob.name.split("/")
        sid = next((p.replace("snapshot_id=", "") for p in parts if p.startswith("snapshot_id=")), None)
        if sid:
            bronze_snapshots.append({"snapshot_id": sid, "last_modified": blob.last_modified})

bronze_snapshots.sort(key=lambda x: x["snapshot_id"])
print(f"Bronze manifests found : {len(bronze_snapshots)}")
print(f"Silver processed       : {len(rows)}")

# COMMAND ----------
# ── 4. Build display table ────────────────────────────────────────────────────

import pandas as pd

records = []
prev_time = None

for r in rows:
    snap   = r["innings_snap"] or {}
    marker = r["marker"]

    snap_time_str = snap.get("snapshot_time_utc") or r["last_modified"].isoformat()
    try:
        snap_time = datetime.fromisoformat(snap_time_str.replace("Z", "+00:00"))
    except Exception:
        snap_time = r["last_modified"]

    gap_s = round((snap_time - prev_time).total_seconds()) if prev_time else None
    prev_time = snap_time

    ball_window = snap.get("ball_window") or []
    ball_str    = " ".join(str(b) for b in ball_window) if ball_window else "-"

    records.append({
        "snapshot_id":  r["snapshot_id"],
        "time_utc":     snap_time.strftime("%Y-%m-%d %H:%M:%S"),
        "innings":      str(snap.get("innings") or ""),
        "over":         str(snap.get("over") or ""),
        "score":        str(snap.get("score") or ""),
        "runs":         str(snap.get("runs") if snap.get("runs") is not None else ""),
        "wickets":      str(snap.get("wickets") if snap.get("wickets") is not None else ""),
        "target":       str(snap.get("target") if snap.get("target") is not None else ""),
        "batting_team": (snap.get("batting_team") or "")[:20],
        "ball_window":  ball_str,
        "markets":      str(marker.get("current_market_selection_count", 0)),
        "gap_prev_s":   str(gap_s) if gap_s is not None else "",
    })

df = pd.DataFrame(records)

# Keep numeric versions for charts — separate from the display-safe string df
def _int(v):
    try: return int(v)
    except: return None

df["_runs"]    = df["runs"].apply(_int)
df["_wickets"] = df["wickets"].apply(_int)
df["_innings"] = df["innings"].apply(_int)
df["_gap_s"]   = df["gap_prev_s"].apply(lambda v: float(v) if v else None)
df["_markets"] = df["markets"].apply(_int)

first_snap = rows[0]["innings_snap"] or {}
print(f"\nMatch: {first_snap.get('match_name', 'Unknown')}")
print(f"League: {first_snap.get('league_name', 'Unknown')}")
print(f"Date range: {df['time_utc'].iloc[0]}  →  {df['time_utc'].iloc[-1]}")

# display-safe: all strings, Arrow has no trouble with uniform str columns
display_df = df[["snapshot_id","time_utc","innings","over","score","batting_team","ball_window","markets","gap_prev_s"]]
display(spark.createDataFrame(display_df))

# COMMAND ----------
# ── 5. Dedup summary ──────────────────────────────────────────────────────────

gaps = df["_gap_s"].dropna()
dedup_gaps   = gaps[gaps > 10]
normal_gaps  = gaps[gaps <= 10]

print("── Dedup analysis ──")
print(f"  Total silver rows         : {len(df)}")
print(f"  Bronze manifests          : {len(bronze_snapshots)}")
print(f"  Gaps >10s (dedup active)  : {len(dedup_gaps)}")
print(f"  Avg gap (normal)          : {normal_gaps.mean():.1f}s" if len(normal_gaps) else "  Avg gap (normal) : n/a")
print(f"  Max gap                   : {gaps.max():.0f}s  ({gaps.max()/60:.1f} min)" if len(gaps) else "  Max gap: n/a")

gap_df = pd.DataFrame({
    "gap_bucket": ["<=5s (real-time)", "6-30s (dedup)", "31-120s (long dedup)", ">120s (halted)"],
    "count": [
        str(int((gaps <= 5).sum())),
        str(int(((gaps > 5) & (gaps <= 30)).sum())),
        str(int(((gaps > 30) & (gaps <= 120)).sum())),
        str(int((gaps > 120).sum())),
    ]
})
display(spark.createDataFrame(gap_df))

# COMMAND ----------
# ── 6. Score progression chart ────────────────────────────────────────────────

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

plot_df = df[df["_runs"].notna()].copy()
plot_df["snap_dt"] = pd.to_datetime(plot_df["time_utc"])

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), gridspec_kw={"height_ratios": [3, 1]})

# ── Top: run progression ──────────────────────────────────────────────────────
colors = {1: "#2196F3", 2: "#FF5722"}
for inn in [1, 2]:
    seg = plot_df[plot_df["_innings"] == inn]
    if seg.empty:
        continue
    ax1.plot(seg["snap_dt"], seg["_runs"], color=colors[inn], linewidth=1.5,
             label=f"Innings {inn}")
    prev_w = seg["_wickets"].shift(1, fill_value=seg["_wickets"].iloc[0])
    wicket_rows = seg[prev_w < seg["_wickets"]]
    if not wicket_rows.empty:
        ax1.scatter(wicket_rows["snap_dt"], wicket_rows["_runs"],
                    color="red", zorder=5, s=50, label="Wicket" if inn == 1 else "_")

ax1.set_ylabel("Runs", fontsize=12)
ax1.set_title(f"Score Progression — event_id={EVENT_ID}", fontsize=14)
ax1.legend()
ax1.grid(True, alpha=0.3)
ax1.tick_params(axis="x", rotation=30)

# ── Bottom: market selection count ───────────────────────────────────────────
ax2.bar(plot_df["snap_dt"], plot_df["_markets"].fillna(0), color="#9C27B0", alpha=0.6, width=0.001)
ax2.set_ylabel("Markets", fontsize=10)
ax2.set_xlabel("Time (UTC)", fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.tick_params(axis="x", rotation=30)

plt.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------
# ── 7. Over-by-over summary table ─────────────────────────────────────────────

# One row per over — last snapshot in that over
over_df = df[df["over"].str.match(r"^\d+\.\d+$", na=False)].copy()
over_df["over_num"] = over_df["over"].apply(lambda x: int(x.split(".")[0]))
over_summary = (
    over_df.sort_values("snapshot_id")
           .groupby(["innings", "over_num"])
           .last()
           .reset_index()[["innings", "over_num", "over", "score", "ball_window", "markets"]]
           .sort_values(["innings", "over_num"])
           .astype(str)
)
print("── Over-by-over summary (last snapshot per over) ──")
display(spark.createDataFrame(over_summary))
