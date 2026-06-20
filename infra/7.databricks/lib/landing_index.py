"""
landing_index — Landing zone shared library (Azure Batch + Databricks).

SIDAP-inspired pattern: index new bronze manifests into a per-run landing index
file so that bronze_to_silver reads only the delta, not all of bronze.

Watermark lifecycle (failure-safe):
  1. index_new_snapshots  → scan_bronze_to_landing() writes landing/{run_id}/index.json
                            — watermark NOT updated yet
  2. bronze_to_silver / silver_to_gold / discover_cricket_ended run
  3. update_watermark     → update_watermark_from_index() reads index.json, advances watermark
  4. on ANY failure       → cleanup_landing() deletes landing/{run_id}/index.json
                            → since watermark was never advanced, next run re-scans

Functions:
  get_watermark(landing)                           → (cutoff_dt, run_id) | (None, None)
  set_watermark(landing, run_id, cutoff, n, n2)    → None
  update_watermark_from_index(landing, run_id)     → None  (reads index.json for metadata)
  scan_bronze_to_landing(bronze, landing, ...)     → (scan_start_utc, n_entries, n_blobs)
  read_landing_index(landing, run_id)              → {event_id: [(path, lm, fi, sid)...]}
  delete_landing_index(landing, run_id)            → None  (cleanup on failure)
"""

import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta

_WATERMARK_PATH    = "control/watermarks.json"
_SCAN_BUFFER_MIN   = 120  # re-scan 2 h before last cutoff to catch late-arriving blobs


def get_watermark(landing):
    """Return (cutoff_datetime, last_run_id) or (None, None) on first run."""
    try:
        raw = json.loads(landing.get_blob_client(_WATERMARK_PATH).download_blob().readall())
        cutoff = raw.get("last_scan_cutoff_utc")
        if cutoff:
            return datetime.fromisoformat(cutoff), raw.get("last_run_id")
    except Exception:
        pass
    return None, None


def set_watermark(landing, run_id, scan_start_utc, new_entries, blobs_scanned):
    """Write watermark after a successful index run."""
    payload = {
        "last_scan_cutoff_utc": scan_start_utc.isoformat(),
        "last_run_id":          run_id,
        "updated_at_utc":       datetime.now(timezone.utc).isoformat(),
        "new_entries":          new_entries,
        "blobs_scanned":        blobs_scanned,
    }
    landing.get_blob_client(_WATERMARK_PATH).upload_blob(
        json.dumps(payload, indent=2).encode(), overwrite=True
    )


def scan_bronze_to_landing(bronze, landing, sport_id, run_id, event_id_filter=None):
    """
    Scan bronze manifests modified since the watermark (minus 2h buffer).
    Writes a single landing/{run_id}/index.json containing all new entries.
    Returns (scan_start_utc, n_entries_written, n_blobs_scanned).
    """
    scan_start_utc = datetime.now(timezone.utc)
    prev_cutoff, _ = get_watermark(landing)

    if event_id_filter:
        # Backfill mode: ignore the watermark. The caller wants a specific event
        # regardless of when its blobs were last modified. Applying the watermark
        # cutoff would silently skip events whose bronze data predates the watermark.
        effective_cutoff = None
        print(f"[index] Backfill mode (event_id={event_id_filter}) — watermark bypassed, full scan for this event")
    elif prev_cutoff:
        effective_cutoff = prev_cutoff - timedelta(minutes=_SCAN_BUFFER_MIN)
        print(f"[index] Watermark : {prev_cutoff.isoformat()}")
        print(f"[index] Cutoff    : {effective_cutoff.isoformat()}  (watermark − {_SCAN_BUFFER_MIN} min)")
    else:
        effective_cutoff = None
        print("[index] No watermark — full bronze scan (first run)")

    prefix  = f"betsapi/inplay_snapshot/sport_id={sport_id}/"
    entries = []
    blobs_scanned = 0

    for blob in bronze.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith("/manifest.json"):
            continue
        # Watermark filter — skip older blobs
        if effective_cutoff and blob.last_modified and blob.last_modified <= effective_cutoff:
            continue

        parts = blob.name.split("/")
        eid = next((p[9:]  for p in parts if p.startswith("event_id=")),    None)
        fi  = next((p[3:]  for p in parts if p.startswith("fi=")),          None)
        sid = next((p[12:] for p in parts if p.startswith("snapshot_id=")), None)

        if not eid or not sid:
            continue
        if event_id_filter and eid != event_id_filter:
            continue

        blobs_scanned += 1
        entries.append({
            "event_id":              eid,
            "snapshot_id":           sid,
            "fi":                    fi or "",
            "manifest_path":         blob.name,
            "blob_last_modified_utc": blob.last_modified.isoformat() if blob.last_modified else None,
        })

    index_path = f"{run_id}/index.json"
    landing.get_blob_client(index_path).upload_blob(
        json.dumps({
            "run_id":         run_id,
            "indexed_at_utc": scan_start_utc.isoformat(),
            "entry_count":    len(entries),
            "blobs_scanned":  blobs_scanned,
            "entries":        entries,
        }).encode(),
        overwrite=True,
    )
    print(f"[index] Scanned {blobs_scanned} manifests → wrote {len(entries)} entries → landing/{index_path}")
    return scan_start_utc, len(entries), blobs_scanned


def update_watermark_from_index(landing, run_id):
    """
    Read landing/{run_id}/index.json and advance the watermark.
    Called by the update_watermark step at the END of a successful pipeline run.
    Skipped in backfill mode (caller checks EVENT_ID and exits early).
    """
    index_path = f"{run_id}/index.json"
    raw = json.loads(landing.get_blob_client(index_path).download_blob().readall())
    scan_start_utc = datetime.fromisoformat(raw["indexed_at_utc"])
    new_entries    = raw.get("entry_count", 0)
    blobs_scanned  = raw.get("blobs_scanned", 0)
    set_watermark(landing, run_id, scan_start_utc, new_entries, blobs_scanned)
    print(f"[update_watermark] Watermark advanced to {scan_start_utc.isoformat()} "
          f"(entries={new_entries}, blobs={blobs_scanned})")


def delete_landing_index(landing, run_id):
    """
    Delete landing/{run_id}/index.json.
    Called by the cleanup_landing step on any pipeline failure so the next run
    re-scans from the unchanged watermark.
    """
    index_path = f"{run_id}/index.json"
    try:
        landing.get_blob_client(index_path).delete_blob()
        print(f"[cleanup_landing] Deleted landing/{index_path}")
    except Exception as ex:
        # Blob may not exist if index_new_snapshots itself failed — that's fine.
        print(f"[cleanup_landing] Delete skipped (blob may not exist): {ex}")


def read_landing_index(landing, run_id):
    """
    Load landing/{run_id}/index.json and return it in the same dict format
    expected by bronze_to_silver:
      { event_id: [(manifest_path, last_modified_dt, fi, snapshot_id), ...] }
    """
    index_path = f"{run_id}/index.json"
    try:
        raw = json.loads(landing.get_blob_client(index_path).download_blob().readall())
    except Exception as e:
        raise FileNotFoundError(f"Landing index not found at landing/{index_path}: {e}")

    result = defaultdict(list)
    for entry in raw.get("entries", []):
        eid          = entry.get("event_id")
        sid          = entry.get("snapshot_id")
        fi           = entry.get("fi", "")
        manifest_path = entry.get("manifest_path")
        lm_str       = entry.get("blob_last_modified_utc")

        if not eid or not sid or not manifest_path:
            continue

        try:
            lm = datetime.fromisoformat(lm_str) if lm_str else None
        except Exception:
            lm = None

        result[eid].append((manifest_path, lm, fi, sid))

    return result
