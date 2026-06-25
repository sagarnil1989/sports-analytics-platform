# Databricks notebook: Hypothesis 3 — Inn1 Pre-Match Score Over/Under
# Run manually after pl_build_ended_match (and gold_build_prematch_pages) has completed.
# Scans all ended T20 matches, compares the bet365 pre-match "1st Innings Score"
# Over/Under line against the actual innings-1 total, writes results to gold.

# COMMAND ----------

import subprocess
subprocess.run([
    "pip", "install", "--quiet",
    "azure-storage-blob", "azure-storage-file-datalake", "requests", "azure-functions"
], check=True)

# COMMAND ----------

import sys, os, types

os.environ["DATA_STORAGE_CONNECTION_STRING"] = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
os.environ["SPORT_ID"]                       = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

def _load_from_dbfs(module_name, dbfs_path):
    """Read module source from DBFS via REST API (no FUSE mount, works on Shared clusters)."""
    content = dbutils.fs.head(dbfs_path, 500000)
    mod = types.ModuleType(module_name)
    mod.__file__ = dbfs_path
    sys.modules[module_name] = mod
    exec(compile(content, dbfs_path, "exec"), mod.__dict__)
    return mod

_src = "dbfs:/FileStore/cricket-pipeline/src"
_load_from_dbfs("util", f"{_src}/util.py")
_load_from_dbfs("league_config", f"{_src}/league_config.py")
_load_from_dbfs("hypothesis", f"{_src}/hypothesis.py")

# COMMAND ----------

from hypothesis import extract_inn1_prematch_over
from util import get_named_container_client, upload_json

result = extract_inn1_prematch_over()

# Write aggregate index so the display function can read it.
gold = get_named_container_client("gold")
upload_json(gold, "cricket/hypothesis/inn1_prematch_over.json", result, overwrite=True)

print(f"T20 matches scanned      : {result['total_t20_matches']}")
print(f"No pre-match market      : {result['no_prematch_market_count']}")
print(f"Eligible (line available): {result['eligible_matches']}")
print(f"OVER                     : {result['over_count']}")
print(f"UNDER                    : {result['under_count']}")
print(f"PUSH                     : {result['push_count']}")
print(f"OVER %                   : {result['over_pct']}%")
