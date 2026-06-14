# Function App Split Plan

## Problem
A syntax error anywhere in `views.py` (3,500 lines) kills all timer triggers —
data capture stops. Display and ingestion have completely different risk profiles
and should be isolated.

## Target Architecture

### Two Function Apps, one App Service Plan
| App | Azure name | Purpose |
|---|---|---|
| Ingestion | `func-ramanuj-ingestion` | Timer triggers + raw JSON data endpoints |
| Display | `func-ramanuj-display` | All HTML pages and display API routes |

Same App Service Plan = zero extra plan cost. Only addition: one extra Storage
Account for the display app (~$1–2/month).

---

## Phase 1 — Split views.py into a package ✅ COMPLETE

**From:** `src/functions/cricket_ingestion/views.py` (3,657 lines, single file, renamed to `views_legacy.py`)

**To:** `src/functions/cricket_ingestion/views/` package (10 modules + `__init__.py`)

| Module | Contains |
|---|---|
| `_common.py` | All shared imports + `build_simple_table_page()` |
| `prematch.py` | All `view_prematch_*` functions (9 functions) |
| `ended.py` | `view_ended_matches`, `view_ended_matches_html` |
| `matches.py` | Live matches, leagues, `view_match_page_html`, lineage (10 functions) |
| `innings_tracker.py` | `view_innings_tracker_html`, `view_silver_innings_tracker_html`, `view_innings_tracker_analytics` |
| `heatmap.py` | `view_market_heatmap_html` |
| `detailed_analysis.py` | `view_detailed_analysis_html`, `view_match_live_markets`, `view_match_markets_raw` + helpers |
| `admin.py` | `view_admin_*`, rebuild helpers, `gold_rebuild_ended_matches`, `auto_rebuild_ended_innings` |
| `home.py` | `view_home` |
| `ml.py` | `view_ml_win_predictor_html` |
| `__init__.py` | Imports all 33 functions — `function_app.py` unchanged |

A syntax error in `ml.py` breaks only the ML page. All other pages and all
timer triggers keep running. All 33 view functions import verified with `python3`.

---

## Phase 2 — Two separate source directories ✅ COMPLETE

**`src/functions/cricket_display/`** created with:

| File | Purpose |
|---|---|
| `function_app.py` | All 33 HTTP routes — no timer triggers |
| `views/` | Copy of the views package (10 modules) |
| `storage.py`, `silver.py`, `leagues.py`, `innings_tracker.py` | Shared support modules |
| `host.json` | Same settings as ingestion (10 min timeout) |
| `requirements.txt` | `azure-functions`, `azure-storage-blob`, `requests` (no `psycopg2`) |
| `local.settings.json` | Template — needs display storage account connection string |

**`src/functions/cricket_ingestion/`** unchanged for now — both apps are
identical until Phase 5 strips the views from ingestion.

---

## Phase 3 — Terraform (`infra/3.function-app/`) ✅ COMPLETE

**`func-ramanuj` was later renamed to `func-ramanuj-ingestion`** (2026-06-10) to match naming convention.

New resources added to `main.tf`:

| Resource | Name | Notes |
|---|---|---|
| `azurerm_storage_account.display_storage` | `stramanujdispweu` | Required per Function App; LRS Standard |
| `azurerm_linux_function_app.display` | `func-ramanuj-display` | Same plan (`plan-ramanuj-func`), no timer triggers |
| `azurerm_role_assignment.display_function_data_lake_contributor` | — | Read/write gold blob |
| `azurerm_role_assignment.display_function_key_vault_secrets_user` | — | Read `BET365-API-TOKEN` from Key Vault |

New keys added to `env/local.config.json`:
- `display_function_app_name`: `func-ramanuj-display`
- `display_storage_account_name`: `stramanujdispweu`

`terraform validate` passes. Run `terraform apply` in `infra/3.function-app/` to provision.

---

## Phase 4 — URL

Display app URL: `func-ramanuj-display.azurewebsites.net`
Ingestion app URL: `func-ramanuj-ingestion.azurewebsites.net`
No custom domain needed — update bookmarks only.

---

## Risk and Order

| Step | Risk | Status |
|---|---|---|
| 1. Split views.py into package | Medium | ✅ Complete |
| 2. Create cricket_display directory | Low | ✅ Complete |
| 3. Terraform new Function App | Low | ✅ Complete — run terraform apply |
| 4. Deploy and test display app | Low | ✅ Complete |
| 5. Strip views from ingestion app | Low | ✅ Complete |
| 6. Deploy and test ingestion app | Low | ✅ Complete |
