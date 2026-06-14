# Databricks notebook: Hypothesis 2 — Strategic Timeout Wicket
# Run manually after pl_build_ended_match has completed for the day.
# Scans all ended matches, detects strategic timeouts (game paused > 2 min),
# checks whether a wicket fell in the over that immediately resumed.

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

from hypothesis import extract_timeout_wicket
result = extract_timeout_wicket()

print(f"Total timeouts detected : {result['total_timeouts_detected']}")
print(f"Eligible (over data)    : {result['eligible_timeouts']}")
print(f"Wicket in resumed over  : {result['wicket_in_resumed_over_count']}")
print(f"No wicket               : {result['no_wicket_count']}")
print(f"Unknown                 : {result['unknown_count']}")
print(f"Wicket %                : {result['wicket_pct']}%")
