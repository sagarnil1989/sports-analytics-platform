"""Inn1 score predictor feature matrix — all matches × features, three cutoff tabs."""
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from .common import json, logging, os, func, get_named_container_client


def _parse_over(r):
    try:
        s = str(r.get("over") or "0"); parts = s.split(".")
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    except Exception:
        return 0, 0


def _state_after_n(inn_rows, n):
    exact, prev_ov, any_ov = [], [], []
    for r in inn_rows:
        ov_int, ov_ball = _parse_over(r)
        if ov_int == n and ov_ball == 0: exact.append(r)
        if ov_int == n - 1:              prev_ov.append(r)
        if ov_int <= n:                  any_ov.append(r)
    r = exact[0] if exact else prev_ov[-1] if prev_ov else any_ov[-1] if any_ov else None
    if r is None:
        return None
    return {"score": r.get("score") or 0, "wickets": r.get("wickets") or 0,
            "bat_odds": r.get("batting_team_odds"), "bowl_odds": r.get("bowling_team_odds")}


def _per_over(inn_rows, from_ov, to_ov):
    result = {}
    prev_s = prev_w = 0
    if from_ov > 1:
        s0 = _state_after_n(inn_rows, from_ov - 1)
        if s0: prev_s, prev_w = s0["score"], s0["wickets"]
    for k in range(from_ov, to_ov + 1):
        s = _state_after_n(inn_rows, k)
        if s:
            result[f"ov{k}_runs"]     = max(0, s["score"]   - prev_s)
            result[f"ov{k}_wkts"]     = max(0, s["wickets"] - prev_w)
            result[f"ov{k}_bat_odds"] = s["bat_odds"]
            result[f"ov{k}_bowl_odds"]= s["bowl_odds"]
            prev_s, prev_w = s["score"], s["wickets"]
        else:
            for suf in ["_runs","_wkts","_bat_odds","_bowl_odds"]:
                result[f"ov{k}{suf}"] = None
    return result


def _safe_div(a, b):
    try:
        return round(a / b, 3) if b and b != 0 else 0.0
    except Exception:
        return 0.0


def _event_id_from_path(path):
    m = re.search(r"event_id=(\d+)", path)
    return m.group(1) if m else ""


def _parse_score_summary(t, inn1_bat_team):
    raw = (t.get("score_summary_events") or t.get("score_summary_bet365")
           or t.get("score_summary") or "")
    raw = raw.replace("-", ",").strip()
    if not raw or "," not in raw:
        return None
    parts = raw.split(",", 1)
    home = str(t.get("home_team_name") or "").strip()
    away = str(t.get("away_team_name") or "").strip()
    if inn1_bat_team and away and inn1_bat_team == away:
        parts = [parts[1].strip(), parts[0].strip()]
    try:
        return int(parts[0].split("/")[0].strip())
    except Exception:
        return None


def _build_record(t, train_cutoff, blob_path=""):
    rows      = t.get("rows") or []
    inn1_rows = [r for r in rows if r.get("innings") == 1]
    if not inn1_rows:
        return None

    event_id = str(t.get("event_id") or _event_id_from_path(blob_path) or "")
    inn1_bat_team = next((str(r["batting_team"]).strip() for r in inn1_rows if r.get("batting_team")), "")
    home = str(t.get("home_team_name") or "").strip()
    away = str(t.get("away_team_name") or "").strip()
    inn1_bowl_team = away if inn1_bat_team == home else home

    _sd = t.get("stadium_data") or {}
    _n  = _sd.get("name", "")
    venue = f"{_n}, {_sd.get('city','')}".strip(", ") if _n else str(
        t.get("venue") or t.get("stadium") or t.get("league_name") or "unknown")

    auth_inn1 = _parse_score_summary(t, inn1_bat_team)
    inn1_final = inn1_rows[-1]
    inn1_total_score   = auth_inn1 if auth_inn1 is not None else (inn1_final.get("score") or 0)
    inn1_total_wickets = inn1_final.get("wickets") or 0

    if inn1_total_score == 0:
        return None

    date_str = str(t.get("match_date_utc") or "")[:10]

    s6  = _state_after_n(inn1_rows, 6)
    s10 = _state_after_n(inn1_rows, 10)
    s16 = _state_after_n(inn1_rows, 16)
    s1  = _state_after_n(inn1_rows, 1)

    ov1_6  = _per_over(inn1_rows, 1, 6)
    ov7_10 = _per_over(inn1_rows, 7, 10)
    ov11_16= _per_over(inn1_rows, 11, 16)

    sc6  = s6["score"]   if s6  else None
    wk6  = s6["wickets"] if s6  else None
    sc10 = s10["score"]  if s10 else None
    wk10 = s10["wickets"]if s10 else None
    sc16 = s16["score"]  if s16 else None
    wk16 = s16["wickets"]if s16 else None

    rec = {
        "event_id":    event_id,
        "match_name":  str(t.get("match_name") or ""),
        "match_date":  date_str,
        "split":       "train" if date_str < train_cutoff else "test",
        "inn1_score_final":   inn1_total_score,
        "inn1_wickets_final": inn1_total_wickets,
        "venue":          venue,
        "inn1_bat_team":  inn1_bat_team,
        "inn1_bowl_team": inn1_bowl_team,
        # snapshots
        "inn1_score_at_6":    sc6,  "inn1_wkts_at_6":  wk6,
        "inn1_score_at_10":   sc10, "inn1_wkts_at_10": wk10,
        "inn1_score_at_16":   sc16, "inn1_wkts_at_16": wk16,
        # odds at key points
        "inn1_ov1_bat_odds":  s1["bat_odds"]  if s1  else None,
        "inn1_ov6_bat_odds":  s6["bat_odds"]  if s6  else None,
        "inn1_ov10_bat_odds": s10["bat_odds"] if s10 else None,
        "inn1_ov16_bat_odds": s16["bat_odds"] if s16 else None,
        # composites
        "inn1_pp_rp_wkt":               _safe_div(sc6  or 0, 10 - (wk6  or 0)),
        "inn1_mid_rp_wkt":              _safe_div((sc10 or 0) - (sc6 or 0), 10 - (wk10 or 0)),
        "inn1_mid_wkts_only":           max(0, (wk10 or 0) - (wk6 or 0)),
        "inn1_death_start_rp_wkt":      _safe_div((sc16 or 0) - (sc10 or 0), 10 - (wk16 or 0)),
        "inn1_death_start_wkts_only":   max(0, (wk16 or 0) - (wk10 or 0)),
        "inn1_pp_odds_swing":           ((s6["bat_odds"] or 0) - (s1["bat_odds"] or 0)) if s6 and s1 and s6["bat_odds"] and s1["bat_odds"] else None,
        "inn1_mid_odds_swing":          ((s10["bat_odds"] or 0) - (s6["bat_odds"] or 0)) if s10 and s6 and s10["bat_odds"] and s6["bat_odds"] else None,
        "inn1_death_start_odds_swing":  ((s16["bat_odds"] or 0) - (s10["bat_odds"] or 0)) if s16 and s10 and s16["bat_odds"] and s10["bat_odds"] else None,
    }
    rec.update({f"inn1_{k}": v for k, v in ov1_6.items()})
    rec.update({f"inn1_{k}": v for k, v in ov7_10.items()})
    rec.update({f"inn1_{k}": v for k, v in ov11_16.items()})
    return rec


META_COLS = ["event_id", "match_name", "match_date", "split",
             "inn1_score_final", "inn1_wickets_final"]

COLS_AT6 = (
    ["venue", "inn1_bat_team", "inn1_bowl_team",
     "inn1_score_at_6", "inn1_wkts_at_6",
     "inn1_pp_rp_wkt", "inn1_ov1_bat_odds", "inn1_ov6_bat_odds", "inn1_pp_odds_swing"] +
    [f"inn1_ov{n}{s}" for n in range(1, 7) for s in ["_runs","_wkts","_bat_odds","_bowl_odds"]]
)

COLS_AT10 = COLS_AT6 + (
    ["inn1_score_at_10", "inn1_wkts_at_10",
     "inn1_mid_rp_wkt", "inn1_mid_wkts_only", "inn1_ov10_bat_odds", "inn1_mid_odds_swing"] +
    [f"inn1_ov{n}{s}" for n in range(7, 11) for s in ["_runs","_wkts","_bat_odds","_bowl_odds"]]
)

COLS_AT16 = COLS_AT10 + (
    ["inn1_score_at_16", "inn1_wkts_at_16",
     "inn1_death_start_rp_wkt", "inn1_death_start_wkts_only",
     "inn1_ov16_bat_odds", "inn1_death_start_odds_swing"] +
    [f"inn1_ov{n}{s}" for n in range(11, 17) for s in ["_runs","_wkts","_bat_odds","_bowl_odds"]]
)

CUTOFFS = [
    ("at-over-6",  "Powerplay (overs 1–6)", COLS_AT6),
    ("at-over-10", "Halfway (overs 1–10)",  COLS_AT10),
    ("at-over-16", "Death start (overs 1–16)", COLS_AT16),
]


def _fmt(v):
    if v is None: return ""
    if isinstance(v, float): return f"{v:.3f}".rstrip("0").rstrip(".")
    return str(v)


def view_ml_score_matrix_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        cutoff_param = req.params.get("cutoff", "at-over-6")
        gold   = get_named_container_client("gold")
        train_cutoff = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")

        blobs  = [b.name for b in gold.list_blobs(name_starts_with="event_id=")
                  if b.name.endswith("/innings_tracker.json")]

        def _dl(path):
            try:
                return json.loads(gold.get_blob_client(path).download_blob().readall())
            except Exception:
                return None

        trackers = []
        with ThreadPoolExecutor(max_workers=32) as ex:
            futs = {ex.submit(_dl, b): b for b in blobs}
            for fut in as_completed(futs):
                t = fut.result()
                if t:
                    trackers.append((t, futs[fut]))

        def _max_over(rows):
            mx = 0
            for r in rows:
                try: mx = max(mx, int(str(r.get("over") or "0").split(".")[0]))
                except Exception: pass
            return mx

        t20 = [(t, path) for t, path in trackers if 15 <= _max_over(t.get("rows") or []) <= 20]

        records = []
        for t, path in t20:
            rec = _build_record(t, train_cutoff, blob_path=path)
            if rec:
                records.append(rec)

        records.sort(key=lambda r: r["match_date"])

        # pick active cutoff
        active_name, active_desc, active_cols = next(
            (c for c in CUTOFFS if c[0] == cutoff_param), CUTOFFS[0])

        n_train = sum(1 for r in records if r["split"] == "train")
        n_test  = sum(1 for r in records if r["split"] == "test")

        # tab links
        tabs = ""
        for cname, cdesc, _ in CUTOFFS:
            active = "font-weight:bold;border-bottom:3px solid #0066cc;" if cname == active_name else ""
            tabs += f'<a href="?cutoff={cname}" style="margin-right:20px;text-decoration:none;color:#0066cc;{active}">{cdesc}</a>'

        # table
        header = "".join(f"<th class='meta'>{c}</th>" for c in META_COLS)
        header += "".join(f"<th>{c}</th>" for c in active_cols)

        rows_html = []
        for rec in records:
            cls   = "test" if rec["split"] == "test" else "train"
            cells = "".join(f"<td class='meta'>{_fmt(rec.get(c,''))}</td>" for c in META_COLS)
            cells += "".join(f"<td>{_fmt(rec.get(c,''))}</td>" for c in active_cols)
            rows_html.append(f"<tr class='{cls}'>{cells}</tr>")

        html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Score Feature Matrix</title>
<style>
  body {{ font-family: monospace; font-size: 11px; margin: 12px; }}
  h2 {{ margin-bottom: 6px; font-family: Arial; }}
  .tabs {{ margin: 12px 0 16px 0; font-family: Arial; }}
  .wrap {{ overflow: auto; max-height: 70vh; }}
  table {{ border-collapse: collapse; white-space: nowrap; }}
  th, td {{ border: 1px solid #ccc; padding: 3px 7px; }}
  th {{ background:#333; color:#fff; position:sticky; top:0; z-index:2; font-size:10px; }}
  th.meta {{ background:#555; }}
  td.meta {{ background:#f5f5f5 !important; font-weight:bold; }}
  tr.train td {{ background:#e8f5e9; }}
  tr.test  td {{ background:#fce4ec; }}
  nav {{ margin-bottom:16px; font-size:14px; font-family:Arial; }}
  nav a {{ color:#0066cc; text-decoration:none; margin-right:16px; }}
</style>
</head>
<body>
<nav>
  <a href="/api/home">Home</a>
  <a href="/api/live/view" style="color:#c00;font-weight:bold;">🔴 Live</a>
  <a href="/api/ml/score-predictor">Score Predictor</a>
  <a href="/api/ml/win-predictor">Win Predictor</a>
  <a href="/api/ml/glossary">Glossary</a>
</nav>
<h2>Inn1 Score — Feature Matrix</h2>
<div style="font-size:12px;font-family:Arial;margin-bottom:8px;">
  Train cutoff: <b>{train_cutoff}</b> &nbsp;|&nbsp;
  <b>{n_train}</b> train &nbsp;|&nbsp; <b>{n_test}</b> test &nbsp;|&nbsp;
  <b>{len(active_cols)}</b> features for <b>{active_name}</b>
  &nbsp;|&nbsp; <span style="background:#e8f5e9;padding:2px 8px;">■ train</span>
  &nbsp;<span style="background:#fce4ec;padding:2px 8px;">■ test</span>
</div>
<div class="tabs">{tabs}</div>
<div class="wrap">
<table>
<thead><tr>{header}</tr></thead>
<tbody>{''.join(rows_html)}</tbody>
</table>
</div>
</body>
</html>"""
        return func.HttpResponse(html, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.exception("score_matrix error")
        return func.HttpResponse(f"Error: {e}", status_code=500)
