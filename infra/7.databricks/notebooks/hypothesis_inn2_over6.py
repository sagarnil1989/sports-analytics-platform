# Databricks notebook: Hypothesis 1 — Inn2 Over-6 Favourite Wins
# Run manually after pl_build_ended_match has completed for the day.
# Scans all ended matches, finds the innings-2 over-6 snapshot for each,
# records match-winner odds and actual winner, writes results to gold.

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

from hypothesis import extract_inn2_over6_favorite
result = extract_inn2_over6_favorite()

print(f"Matches processed : {result['total_matches']}")
print(f"Eligible (odds available): {result['eligible_matches']}")
print(f"Favourite won     : {result['favorite_won_count']}")
print(f"Favourite lost    : {result['favorite_lost_count']}")
print(f"No odds data      : {result['no_odds_data_count']}")
print(f"Win %             : {result['favorite_win_pct']}%")
