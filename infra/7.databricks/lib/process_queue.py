"""
process_queue — Process-queue shared library (Azure Batch + Databricks).

Replaces the watermark-based landing index pattern with lightweight pending
markers written by the ingestion function at capture time.  The key benefit:
  - OLD: scan 300k+ bronze blobs every pipeline run (~800 s)
  - NEW: list only the pending markers written since the last run (seconds)

Pending marker location:
  process-queue/pending/event_id={eid}/{snapshot_id}.json

In-progress file location (one per pipeline run):
  process-queue/in-progress/{run_id}.json

Last-successful-run sentinel:
  process-queue/last-successful-run.json

Functions:
  read_pending_markers(pq, event_id_filter=None)
      → {eid: [(manifest_path, captured_at_dt_or_None, fi, sid), ...]}
  write_in_progress(pq, run_id, events_dict, n_blobs_scanned=0)
      → None
  read_in_progress(pq, run_id)
      → {eid: [(manifest_path, captured_at_dt_or_None, fi, sid), ...]}
  delete_pending_for_events(pq, succeeded_event_ids)
      → int  (number of markers deleted)
  write_last_successful_run(pq, run_id, n_processed)
      → None
"""

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FI_RE = re.compile(r"/fi=([^/]+)/")


def _extract_fi(manifest_path: str) -> str:
    """Parse the fi= segment from a manifest_path string. Returns '' if not found."""
    m = _FI_RE.search(manifest_path)
    return m.group(1) if m else ""


def _parse_captured_at(raw: Optional[str]) -> Optional[datetime]:
    """Parse a captured_at_utc string to a timezone-aware datetime, or None."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _download_json(pq, path: str):
    """Download and JSON-parse a blob; return None on any error."""
    try:
        return json.loads(pq.get_blob_client(path).download_blob().readall())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_pending_markers(pq, event_id_filter: Optional[str] = None) -> dict:
    """
    List process-queue/pending/ and parse each marker blob.

    Returns:
        {event_id: [(manifest_path, captured_at_dt_or_None, fi, sid), ...]}

    If event_id_filter is set, only markers under pending/event_id={filter}/
    are returned.
    """
    prefix = (
        f"pending/event_id={event_id_filter}/"
        if event_id_filter
        else "pending/"
    )

    result = defaultdict(list)
    n_blobs = 0

    for blob in pq.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith(".json"):
            continue

        raw = _download_json(pq, blob.name)
        if not raw:
            continue

        eid = raw.get("event_id")
        sid = raw.get("snapshot_id")
        manifest_path = raw.get("manifest_path")

        if not eid or not sid or not manifest_path:
            continue

        fi = raw.get("fi") or _extract_fi(manifest_path)
        captured_at = _parse_captured_at(raw.get("captured_at_utc"))

        result[eid].append((manifest_path, captured_at, fi, sid))
        n_blobs += 1

    print(f"[process_queue] read_pending_markers: {n_blobs} markers → {len(result)} events")
    return result


def write_in_progress(pq, run_id: str, events_dict: dict, n_blobs_scanned: int = 0) -> None:
    """
    Write process-queue/in-progress/{run_id}.json from the events_dict returned
    by read_pending_markers (or built by backfill scan).

    The JSON shape matches the existing landing index so that read_in_progress
    can be a drop-in replacement for read_landing_index in bronze_to_silver.

    events_dict format:
        {event_id: [(manifest_path, captured_at_dt_or_None, fi, sid), ...]}
    """
    entries = []
    for eid, manifests in events_dict.items():
        for manifest_path, captured_at, fi, sid in manifests:
            entries.append({
                "event_id":        eid,
                "snapshot_id":     sid,
                "fi":              fi or "",
                "manifest_path":   manifest_path,
                "captured_at_utc": captured_at.isoformat() if captured_at else None,
            })

    payload = {
        "run_id":        run_id,
        "indexed_at_utc": datetime.now(timezone.utc).isoformat(),
        "entry_count":   len(entries),
        "blobs_scanned": n_blobs_scanned,
        "entries":       entries,
    }

    path = f"in-progress/{run_id}.json"
    pq.get_blob_client(path).upload_blob(
        json.dumps(payload, indent=2).encode(),
        overwrite=True,
    )
    print(f"[process_queue] in-progress written: {path}  ({len(entries)} entries)")


def read_in_progress(pq, run_id: str) -> dict:
    """
    Load process-queue/in-progress/{run_id}.json and return it in the same dict
    format expected by bronze_to_silver:
        {event_id: [(manifest_path, captured_at_dt_or_None, fi, sid), ...]}

    captured_at_dt may be None for migrated events (marker had captured_at_utc=null).
    """
    path = f"in-progress/{run_id}.json"
    try:
        raw = _download_json(pq, path)
        if raw is None:
            raise FileNotFoundError(f"In-progress file not found: process-queue/{path}")
    except FileNotFoundError:
        raise
    except Exception as e:
        raise FileNotFoundError(f"Could not read process-queue/{path}: {e}")

    result = defaultdict(list)
    for entry in raw.get("entries", []):
        eid          = entry.get("event_id")
        sid          = entry.get("snapshot_id")
        fi           = entry.get("fi", "")
        manifest_path = entry.get("manifest_path")
        captured_at  = _parse_captured_at(entry.get("captured_at_utc"))

        if not eid or not sid or not manifest_path:
            continue

        result[eid].append((manifest_path, captured_at, fi, sid))

    print(f"[process_queue] read_in_progress({run_id}): {len(result)} events")
    return result


def delete_pending_for_events(pq, succeeded_event_ids) -> int:
    """
    Delete all pending markers for the given event_ids.

    Lists process-queue/pending/event_id={eid}/ for each eid and deletes
    every .json blob found there.

    Returns the total number of blobs deleted.
    """
    total_deleted = 0
    for eid in succeeded_event_ids:
        prefix = f"pending/event_id={eid}/"
        blobs_to_delete = [
            b.name
            for b in pq.list_blobs(name_starts_with=prefix)
            if b.name.endswith(".json")
        ]
        for blob_name in blobs_to_delete:
            try:
                pq.get_blob_client(blob_name).delete_blob()
                total_deleted += 1
            except Exception as ex:
                print(f"[process_queue] delete_pending: could not delete {blob_name}: {ex}")

    print(f"[process_queue] delete_pending_for_events: deleted {total_deleted} markers "
          f"across {len(list(succeeded_event_ids))} events")
    return total_deleted


def write_last_successful_run(pq, run_id: str, n_processed: int) -> None:
    """
    Write process-queue/last-successful-run.json after a successful pipeline run.
    Provides a simple audit trail of the most recent clean run.
    """
    payload = {
        "run_id":              run_id,
        "completed_at_utc":    datetime.now(timezone.utc).isoformat(),
        "entries_processed":   n_processed,
    }
    pq.get_blob_client("last-successful-run.json").upload_blob(
        json.dumps(payload, indent=2).encode(),
        overwrite=True,
    )
    print(f"[process_queue] last-successful-run.json written (run_id={run_id}, n={n_processed})")
