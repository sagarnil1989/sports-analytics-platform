"""ML feature matrix page — all T20 matches × all 126 features, selected features highlighted."""
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from ._common import json, logging, os, func, get_named_container_client

# ── selected features from last MLflow run (read from summary JSON at request time) ──

def _load_selected(gold):
    try:
        raw = gold.get_blob_client("cricket/ml_features/t20/win_predictor_summary.json").download_blob().readall()
        summary = json.loads(raw)
        result = {}
        for m in summary.get("models", []):
            result[m["name"]] = [f["feature"] for f in m.get("feature_importance", [])]
        return result
    except Exception:
        return {}


def _parse_over(r):
    try:
        s = str(r.get("over") or "0"); parts = s.split(".")
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    except Exception:
        return 0, 0


def _state_after_n_overs(inn_rows, n):
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


def _per_over(inn_rows, inn_num, max_over):
    result = {}; prev_score = prev_wickets = 0
    for k in range(1, max_over + 1):
        s = _state_after_n_overs(inn_rows, k)
        if s is not None:
            result[f"inn{inn_num}_ov{k}_runs"]      = max(0, s["score"]   - prev_score)
            result[f"inn{inn_num}_ov{k}_wkts"]      = max(0, s["wickets"] - prev_wickets)
            result[f"inn{inn_num}_ov{k}_bat_odds"]  = s["bat_odds"]
            result[f"inn{inn_num}_ov{k}_bowl_odds"] = s["bowl_odds"]
            prev_score = s["score"]; prev_wickets = s["wickets"]
        else:
            for suf in ["_runs", "_wkts", "_bat_odds", "_bowl_odds"]:
                result[f"inn{inn_num}_ov{k}{suf}"] = None
    return result


def _chase_agg(inn_rows, inn1_score, ov):
    s = _state_after_n_overs(inn_rows, ov)
    if s is None:
        return None
    bd = ov * 6; bl = 120 - bd; rn = (inn1_score + 1) - s["score"]
    crr = round(s["score"] / (bd / 6), 4) if bd > 0 else 0
    rrr = round(rn / (bl / 6), 4)         if bl > 0 else 99
    return {"score": s["score"], "wickets": s["wickets"], "crr": crr, "rrr": rrr,
            "rr_diff": round(crr - rrr, 4), "runs_needed": rn}


def _safe_div(a, b, default=0.0):
    try:
        return round(a / b, 4) if b and b != 0 else default
    except Exception:
        return default


def _parse_score_summary(t, inn1_bat_team):
    raw = (t.get("score_summary_events") or t.get("score_summary_bet365") or t.get("score_summary") or "")
    raw = raw.replace("-", ",").strip()
    if not raw or "," not in raw:
        return None, None
    parts = raw.split(",", 1)
    away = str(t.get("away_team_name") or "").strip()
    if inn1_bat_team and away and inn1_bat_team == away:
        parts = [parts[1].strip(), parts[0].strip()]
    try:
        return int(parts[0].split("/")[0].strip()), int(parts[1].split("/")[0].strip())
    except Exception:
        return None, None


def _event_id_from_path(path: str) -> str:
    """Extract event_id from blob path e.g. cricket/innings_tracker/event_id=11658831/..."""
    m = re.search(r"event_id=(\d+)", path)
    return m.group(1) if m else ""


def _batting_dominance(event_id: str, silver) -> float | None:
    """max_SR minus avg_SR across inn1 batsmen (min 5 balls). Returns None if insufficient data."""
    if not event_id:
        return None
    prefix = f"silver/cricket/inplay/state/event_id={event_id}/"
    try:
        state_blobs = [b.name for b in silver.list_blobs(name_starts_with=prefix)
                       if "/state_1_" in b.name]
    except Exception:
        return None
    if not state_blobs:
        return None

    def _load(path):
        try:
            return json.loads(silver.get_blob_client(path).download_blob().readall())
        except Exception:
            return None

    player_max = {}  # player_id → (max_runs, balls)
    pattern = re.compile(r'\[([^\]]+)#(\d+)\]:(\d+):(\d+)')

    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(_load, b): b for b in state_blobs}
        for fut in as_completed(futs):
            snap = fut.result()
            if not snap:
                continue
            items = snap if isinstance(snap, list) else [snap]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("TY") != "TE" or item.get("PI") != "1":
                    continue
                s6 = str(item.get("S6") or "")
                for m in pattern.finditer(s6):
                    pid = m.group(2)
                    runs  = int(m.group(3))
                    balls = int(m.group(4))
                    if runs > player_max.get(pid, (-1, 0))[0]:
                        player_max[pid] = (runs, balls)

    batsmen = [(r, b) for r, b in player_max.values() if b >= 5]
    if len(batsmen) < 2:
        return None
    srs = [(r / b) * 100 for r, b in batsmen]
    return round(max(srs) - (sum(srs) / len(srs)), 2)


def _build_record(t, train_cutoff, blob_path: str = "", bat_dominance: float | None = None):
    rows      = t.get("rows") or []
    inn1_rows = [r for r in rows if r.get("innings") == 1]
    inn2_rows = [r for r in rows if r.get("innings") == 2]
    if not inn1_rows or not inn2_rows:
        return None

    # event_id: prefer field in JSON, fall back to extracting from blob path
    event_id = str(t.get("event_id") or _event_id_from_path(blob_path) or "")

    inn1_bat_team = next((str(r["batting_team"]).strip() for r in inn1_rows if r.get("batting_team")), "")
    home  = str(t.get("home_team_name") or "").strip()
    away  = str(t.get("away_team_name") or "").strip()
    inn1_bowl_team = away if inn1_bat_team == home else home
    _sd   = t.get("stadium_data") or {}
    _sd_name = _sd.get("name", "")
    _sd_city = _sd.get("city", "")
    if _sd_name:
        venue = f"{_sd_name}, {_sd_city}".strip(", ")
    else:
        venue = str(t.get("venue") or t.get("stadium") or f"[{t.get('league_name','unknown')}]").strip() or "unknown"

    auth_inn1, auth_inn2 = _parse_score_summary(t, inn1_bat_team)

    inn1_final         = inn1_rows[-1]
    inn1_total_wickets = inn1_final.get("wickets") or 0
    inn1_total_score   = auth_inn1 if auth_inn1 is not None else (inn1_final.get("score") or 0)
    inn2_total_score   = auth_inn2 if auth_inn2 is not None else (inn2_rows[-1].get("score") or 0)
    label              = 1 if inn2_total_score > inn1_total_score else 0
    date_str           = str(t.get("match_date_utc") or "")[:10]

    pp_state  = _state_after_n_overs(inn1_rows, 6)
    mid_state = _state_after_n_overs(inn1_rows, 15)
    ov1_state = _state_after_n_overs(inn1_rows, 1)

    inn1_pp_score     = pp_state["score"]    if pp_state else None
    inn1_pp_wickets   = pp_state["wickets"]  if pp_state else None
    inn1_ov1_bat_odds = ov1_state["bat_odds"] if ov1_state else None
    inn1_pp_bat_odds  = pp_state["bat_odds"]  if pp_state else None
    mid_score         = mid_state["score"]    if mid_state else None
    mid_wickets       = mid_state["wickets"]  if mid_state else None

    inn1_overs   = _per_over(inn1_rows, 1, 20)
    inn2_overs_6 = _per_over(inn2_rows, 2, 6)
    cs2 = _chase_agg(inn2_rows, inn1_total_score, 2)
    cs6 = _chase_agg(inn2_rows, inn1_total_score, 6)

    inn1_last_bat_odds = inn1_overs.get("inn1_ov20_bat_odds")
    inn2_ov2_state = _state_after_n_overs(inn2_rows, 2)
    inn2_ov6_state = _state_after_n_overs(inn2_rows, 6)
    inn2_ov2_bat_odds = inn2_ov2_state["bat_odds"] if inn2_ov2_state else None
    inn2_ov6_bat_odds = inn2_ov6_state["bat_odds"] if inn2_ov6_state else None

    rec = {
        "event_id":         event_id,
        "match_name":       str(t.get("match_name") or ""),
        "match_date":       date_str,
        "split":            "train" if date_str < train_cutoff else "test",
        "inn1_score_final": f"{inn1_total_score}/{inn1_total_wickets}",
        "inn2_score_final": str(inn2_total_score),
        "chasing_won":      label,
        # categoricals
        "venue":            venue,
        "inn1_bat_team":    inn1_bat_team,
        "inn1_bowl_team":   inn1_bowl_team,
        # inn1 composites
        "inn1_pp_rp_wkt":        _safe_div(inn1_pp_score or 0, 10 - (inn1_pp_wickets or 0)),
        "inn1_pp_wickets":        inn1_pp_wickets,
        "inn1_mid_rp_wkt":       _safe_div((mid_score or 0) - (inn1_pp_score or 0), 10 - (mid_wickets or 0)),
        "inn1_mid_wickets_only":  max(0, (mid_wickets or 0) - (inn1_pp_wickets or 0)),
        "inn1_death_runs":        max(0, (inn1_total_score or 0) - (mid_score or 0)),
        "inn1_death_wickets":     max(0, (inn1_total_wickets or 0) - (mid_wickets or 0)),
        "inn1_pressure":          _safe_div(inn1_total_score or 0, 10 - (inn1_total_wickets or 0)),
        "inn1_odds_swing_full":   _safe_div((inn1_last_bat_odds or 0) - (inn1_ov1_bat_odds or 0), 1, default=None),
        "inn1_odds_swing_pp":     _safe_div((inn1_pp_bat_odds or 0) - (inn1_ov1_bat_odds or 0), 1, default=None),
        "inn1_bat_dominance":     bat_dominance,
        # inn1 aggregates
        "inn1_total_score":   inn1_total_score,
        "inn1_total_wickets": inn1_total_wickets,
    }

    rec.update(inn1_overs)
    rec.update(inn2_overs_6)

    rec["inn2_ov2_rp_wkt"]              = _safe_div(cs2["score"] or 0, 10 - (cs2["wickets"] or 0)) if cs2 else None
    rec["inn2_ov2_odds_swing"]          = _safe_div((inn2_ov2_bat_odds or 0) - (inn1_last_bat_odds or 0), 1, default=None) if inn2_ov2_bat_odds and inn1_last_bat_odds else None
    rec["inn2_ov2_runs_needed_per_wkt"] = _safe_div(cs2["runs_needed"] or 0, 10 - (cs2["wickets"] or 0)) if cs2 else None
    rec["inn2_ov2_chase_difficulty"]    = round((cs2["rrr"] or 0) * ((cs2["wickets"] or 0) + 1), 3) if cs2 else None
    rec["inn2_ov6_rp_wkt"]              = _safe_div(cs6["score"] or 0, 10 - (cs6["wickets"] or 0)) if cs6 else None
    rec["inn2_ov6_odds_swing"]          = _safe_div((inn2_ov6_bat_odds or 0) - (inn1_last_bat_odds or 0), 1, default=None) if inn2_ov6_bat_odds and inn1_last_bat_odds else None
    rec["inn2_ov6_runs_needed_per_wkt"] = _safe_div(cs6["runs_needed"] or 0, 10 - (cs6["wickets"] or 0)) if cs6 else None
    rec["inn2_ov6_chase_difficulty"]    = round((cs6["rrr"] or 0) * ((cs6["wickets"] or 0) + 1), 3) if cs6 else None

    for k in ["score", "wickets", "crr", "rrr", "rr_diff", "runs_needed"]:
        rec[f"inn2_ov2_{k}"] = cs2[k] if cs2 else None
        rec[f"inn2_ov6_{k}"] = cs6[k] if cs6 else None

    return rec


FEATURE_COLS = (
    ["venue", "inn1_bat_team", "inn1_bowl_team",
     "inn1_pp_rp_wkt", "inn1_pp_wickets",
     "inn1_mid_rp_wkt", "inn1_mid_wickets_only",
     "inn1_death_runs", "inn1_death_wickets",
     "inn1_pressure", "inn1_odds_swing_full", "inn1_odds_swing_pp",
     "inn1_bat_dominance",
     "inn1_total_score", "inn1_total_wickets"] +
    [f"inn1_ov{n}{s}" for n in range(1, 21) for s in ["_runs", "_wkts", "_bat_odds", "_bowl_odds"]] +
    ["inn2_ov2_rp_wkt", "inn2_ov2_odds_swing", "inn2_ov2_runs_needed_per_wkt", "inn2_ov2_chase_difficulty",
     "inn2_ov6_rp_wkt", "inn2_ov6_odds_swing", "inn2_ov6_runs_needed_per_wkt", "inn2_ov6_chase_difficulty"] +
    [f"inn2_ov{n}{s}" for n in range(1, 7) for s in ["_runs", "_wkts", "_bat_odds", "_bowl_odds"]] +
    [f"inn2_ov2_{k}" for k in ["score", "wickets", "crr", "rrr", "rr_diff", "runs_needed"]] +
    [f"inn2_ov6_{k}" for k in ["score", "wickets", "crr", "rrr", "rr_diff", "runs_needed"]]
)

META_COLS = ["event_id", "match_name", "match_date", "split",
             "inn1_score_final", "inn2_score_final", "chasing_won"]


def _render_html(records, selected, train_cutoff):
    model_short = {"innings1-only": "M1", "innings2-2over": "M2", "innings2-6over": "M3"}

    def sel_badge(col):
        return ",".join(short for m, short in model_short.items() if col in selected.get(m, []))

    def fmt(v):
        if v is None: return ""
        if isinstance(v, float): return f"{v:.3f}".rstrip("0").rstrip(".")
        return str(v)

    rows_html = []
    for rec in records:
        cls = "test" if rec["split"] == "test" else "train"
        cells = [f"<td class='meta'>{fmt(rec.get(c,''))}</td>" for c in META_COLS]
        for c in FEATURE_COLS:
            badge = sel_badge(c)
            td_cls = " class='sel'" if badge else ""
            cells.append(f"<td{td_cls}>{fmt(rec.get(c,''))}</td>")
        rows_html.append(f"<tr class='{cls}'>{''.join(cells)}</tr>")

    header_cells = [f"<th class='meta'>{c}</th>" for c in META_COLS]
    for c in FEATURE_COLS:
        badge = sel_badge(c)
        th_cls = " class='sel'" if badge else ""
        label  = f"{c}<br><small>{badge}</small>" if badge else c
        header_cells.append(f"<th{th_cls}>{label}</th>")

    n_train = sum(1 for r in records if r["split"] == "train")
    n_test  = sum(1 for r in records if r["split"] == "test")

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>ML Feature Matrix</title>
<style>
  body {{ font-family: monospace; font-size: 11px; margin: 12px; }}
  h2   {{ margin-bottom: 6px; }}
  .badge {{ display:inline-block; padding:1px 6px; border-radius:3px; margin:1px; font-size:10px; font-weight:bold; }}
  .m1 {{ background:#bbdefb; }} .m2 {{ background:#c8e6c9; }} .m3 {{ background:#ffe0b2; }}
  .wrap {{ overflow: auto; max-height: 60vh; }}
  table {{ border-collapse: collapse; white-space: nowrap; }}
  th, td {{ border: 1px solid #ccc; padding: 3px 7px; }}
  th {{ background:#333; color:#fff; position:sticky; top:0; z-index:2; font-size:10px; }}
  th.sel {{ background:#b8860b; }}
  th.meta {{ background:#555; }}
  td.meta {{ background:#f5f5f5 !important; font-weight:bold; }}
  tr.train td     {{ background:#e8f5e9; }}
  tr.train td.sel {{ background:#c8e6c9; }}
  tr.test  td     {{ background:#fce4ec; }}
  tr.test  td.sel {{ background:#f8bbd0; }}
  .explainer {{ font-size:12px; border:1px solid #ccc; border-radius:6px; padding:12px 16px; margin:10px 0 14px 0; background:#fafafa; }}
  .explainer h3 {{ margin:0 0 8px 0; font-size:13px; }}
  .explainer table {{ box-shadow:none; width:auto; }}
  .explainer th {{ background:#555; font-size:11px; position:static; }}
  .explainer td {{ background:white !important; font-size:11px; }}
  details summary {{ cursor:pointer; font-weight:bold; font-size:12px; margin-bottom:6px; }}
</style>
</head>
<body>
<h2>ML Feature Matrix — All T20 Matches</h2>

<div style="font-size:12px; margin-bottom:8px;">
  Train cutoff: <b>{train_cutoff}</b> &nbsp;|&nbsp;
  <b>{n_train}</b> train &nbsp;|&nbsp; <b>{n_test}</b> test &nbsp;|&nbsp; <b>{len(FEATURE_COLS)}</b> features
  &nbsp;|&nbsp; <span style="background:#e8f5e9;padding:2px 8px;">■ train row</span>
  &nbsp;<span style="background:#fce4ec;padding:2px 8px;">■ test row</span>
  &nbsp;<span style="background:#c8e6c9;padding:2px 8px;">■ selected feature</span>
  &nbsp;|&nbsp; Models: <span class="badge m1">M1</span> innings1-only
  <span class="badge m2">M2</span> innings2-2over
  <span class="badge m3">M3</span> innings2-6over
</div>

<details>
<summary>▶ Feature explanations (click to expand)</summary>
<div class="explainer">
<h3>Composite / Aggregate features — what the value means</h3>
<table>
<thead><tr><th>Feature</th><th>Formula</th><th>What a HIGH value means</th><th>What a LOW value means</th></tr></thead>
<tbody>
<tr><td>inn1_pp_rp_wkt</td><td>pp_score / (10 − pp_wickets)</td><td>Many runs scored per wicket still in hand after powerplay — dominant start</td><td>Few runs per wicket remaining — either low score or many wickets lost in pp</td></tr>
<tr><td>inn1_pp_wickets</td><td>wickets lost in overs 1–6</td><td>Bowling team took many early wickets — powerplay went to fielders</td><td>Batting team survived powerplay with wickets intact</td></tr>
<tr><td>inn1_mid_rp_wkt</td><td>(mid_score − pp_score) / (10 − mid_wickets)</td><td>Middle overs were productive per wicket still available at over 15</td><td>Middle overs were quiet or expensive in wickets</td></tr>
<tr><td>inn1_mid_wickets_only</td><td>mid_wickets − pp_wickets (overs 7–15 only)</td><td>Many wickets fell in the middle overs — consolidation phase failed</td><td>Batting team kept wickets through middle overs — platform built</td></tr>
<tr><td>inn1_death_runs</td><td>total_score − mid_score (overs 16–20)</td><td>Big finish in the death overs</td><td>Collapsed or conservative in final 5 overs</td></tr>
<tr><td>inn1_death_wickets</td><td>total_wickets − mid_wickets</td><td>Lost many wickets in death overs — batting under pressure</td><td>Batted through death overs without losing wickets</td></tr>
<tr><td>inn1_bat_dominance</td><td>max_SR − avg_SR across inn1 batsmen (≥5 balls)</td><td>One batter scored at a far higher rate than the rest — individual brilliance carrying the innings</td><td>All batsmen scored at similar rates — collective team effort</td></tr>
<tr><td>inn1_pressure</td><td>total_score / (10 − total_wickets)</td><td>Runs were costly in wickets — e.g. 196/8 = 98. Big score but nearly all out</td><td>Runs came cheaply — e.g. 180/2 = 22.5. Dominant innings with wickets to spare</td></tr>
<tr><td>inn1_odds_swing_full</td><td>last_bat_odds − ov1_bat_odds (whole innings)</td><td>Market moved toward batting team across whole innings — they batted well</td><td>Market drifted away — batting team underperformed expectations</td></tr>
<tr><td>inn1_odds_swing_pp</td><td>pp_bat_odds − ov1_bat_odds (overs 1–6)</td><td>Powerplay went well for batting team, market shifted in their favour</td><td>Batting team lost early wickets or scored slowly — market moved against them</td></tr>
<tr><td>inn2_ov2_rp_wkt</td><td>inn2_score_at_ov2 / (10 − inn2_wickets_at_ov2)</td><td>Chase is going well — scoring quickly with wickets in hand after 2 overs</td><td>Struggling early — either few runs or wickets lost</td></tr>
<tr><td>inn2_ov2_odds_swing</td><td>inn2_ov2_bat_odds − inn1_last_bat_odds</td><td>Market swung toward chasing team after 2 overs — chase looking comfortable</td><td>Market moved toward defending team — early wickets or slow start</td></tr>
<tr><td>inn2_ov2_runs_needed_per_wkt</td><td>runs_needed / (10 − wickets_at_ov2)</td><td>Chasing team needs many runs per wicket remaining — under pressure</td><td>Plenty of wickets, manageable target — comfortable chase</td></tr>
<tr><td>inn2_ov2_chase_difficulty</td><td>rrr × (wickets_fallen + 1)</td><td>High RRR <em>and</em> early wickets — jointly signals near-impossible chase (e.g. 51/5 at ov6 chasing 254 → ~87)</td><td>Low value = comfortable chase with wickets in hand and manageable rate</td></tr>
<tr><td>inn2_ov6_rp_wkt</td><td>inn2_score_at_ov6 / (10 − inn2_wickets_at_ov6)</td><td>End of powerplay: scoring well with wickets intact — chase on track</td><td>Either fell behind the rate or lost wickets in the powerplay</td></tr>
<tr><td>inn2_ov6_odds_swing</td><td>inn2_ov6_bat_odds − inn1_last_bat_odds</td><td>Chasing team now market favourite vs where they started — strong powerplay</td><td>Defending team moved into favouritism after bowling well in pp</td></tr>
<tr><td>inn2_ov6_runs_needed_per_wkt</td><td>runs_needed / (10 − wickets_at_ov6)</td><td>Need many runs per remaining wicket — high pressure chase</td><td>Low required runs per wicket — comfortable position</td></tr>
<tr><td>inn2_ov6_chase_difficulty</td><td>rrr × (wickets_fallen + 1)</td><td>High RRR <em>and</em> early wickets — jointly signals near-impossible chase (e.g. 51/5 at ov6 chasing 254 → ~87)</td><td>Low value = comfortable chase with wickets in hand and manageable rate</td></tr>
<tr><td>inn2_ovN_crr</td><td>score / (N overs)</td><td>Chasing team scoring above 10 rpo — aggressive</td><td>Below required rate, may need acceleration</td></tr>
<tr><td>inn2_ovN_rrr</td><td>runs_needed / overs_remaining</td><td>High required rate — target is stiff or chase has fallen behind</td><td>Low required rate — chase is comfortable</td></tr>
<tr><td>inn2_ovN_rr_diff</td><td>crr − rrr</td><td>Positive = chasing team ahead of the rate — favoured to win</td><td>Negative = behind the rate — need to accelerate</td></tr>
<tr><td>inn2_ovN_runs_needed</td><td>(inn1_score + 1) − inn2_score</td><td>Large number = big target still to chase</td><td>Small number = nearly there</td></tr>
<tr><td>inn1_ovN_bat_odds / bowl_odds</td><td>Live market odds at end of over N</td><td>bat_odds &gt; 2.0 = market thinks bowling side will win</td><td>bat_odds &lt; 2.0 = batting side favoured at that point</td></tr>
<tr><td>inn2_ovN_bat_odds / bowl_odds</td><td>Live market odds during the chase</td><td>bat_odds close to 1.0 = chasing team strong favourite</td><td>bat_odds &gt; 3.0 = chasing team unlikely to win</td></tr>
<tr><td>venue / inn1_bat_team / inn1_bowl_team</td><td>Categorical identifiers</td><td colspan="2">Used to capture team strength and home advantage. [League name] in brackets = venue unknown, using league as fallback.</td></tr>
</tbody>
</table>
</div>
</details>

<div class="wrap">
<table>
<thead><tr>{''.join(header_cells)}</tr></thead>
<tbody>{''.join(rows_html)}</tbody>
</table>
</div>
</body>
</html>"""


def view_ml_feature_matrix_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold   = get_named_container_client("gold")
        silver = get_named_container_client("silver")

        selected    = _load_selected(gold)
        train_cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")

        prefix = "cricket/innings_tracker/"
        blobs  = [b.name for b in gold.list_blobs(name_starts_with=prefix)
                  if b.name.endswith("innings_1_from_silver.json")]

        def _dl(path):
            try:
                return json.loads(gold.get_blob_client(path).download_blob().readall())
            except Exception:
                return None

        # store (tracker, blob_path) so event_id can be extracted from path as fallback
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

        # Pre-compute bat dominance for all matches in one parallel batch.
        # Doing this here (not inside _build_record) avoids spawning a new thread
        # pool per match — a single pool handles all silver reads concurrently.
        def _eid(t, path):
            return str(t.get("event_id") or _event_id_from_path(path) or "")

        eids = [_eid(t, path) for t, path in t20]
        with ThreadPoolExecutor(max_workers=32) as ex:
            dom_futs = {ex.submit(_batting_dominance, eid, silver): eid for eid in eids if eid}
            dom_results = {}
            for fut in as_completed(dom_futs):
                dom_results[dom_futs[fut]] = fut.result()

        records = []
        for t, path in t20:
            eid = _eid(t, path)
            rec = _build_record(t, train_cutoff, blob_path=path,
                                bat_dominance=dom_results.get(eid))
            if rec:
                records.append(rec)

        records.sort(key=lambda r: r["match_date"])

        html = _render_html(records, selected, train_cutoff)
        return func.HttpResponse(html, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.exception("feature_matrix error")
        return func.HttpResponse(f"Error: {e}", status_code=500)
