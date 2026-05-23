storage.py — env, blob, BetsAPI utils (no deps)
cricwebsite_db.py — PostgreSQL writer
leagues.py — league preferences
innings_tracker.py — innings accumulator logic
bronze.py — bronze_* functions (runs in Function App)
silver.py — silver_* functions (runs in Databricks via ADF)
gold.py — gold_* functions (runs in Databricks via ADF)
prematch.py — bronze_*/gold_* prematch functions
views.py — view_* HTTP handlers + gold_rebuild_ended_matches (runs in Databricks via ADF)
function_app.py — thin entry point: 5 bronze timers + HTTP routes only (silver/gold moved to ADF + Databricks)
