from .common import (
    json, logging, escape, Any, Dict, List, Optional,
    func,
    download_json, get_named_container_client,
    build_simple_table_page,
    parse_ss_final_scores,
)
import re as _re


def _da_decode_s6(s6_raw: str):
    # [PLAYERFULLNAME#198836] contains '#' which is the s6 separator.
    # Normalize these to just the numeric ID before splitting on '#'.
    s6_clean = _re.sub(r'\[PLAYERFULLNAME#(\d+)\]', r'\1', str(s6_raw or ""))
    batsmen = []
    for part in s6_clean.split("#"):
        bits = part.split(":")
        if len(bits) >= 3 and bits[0].strip():
            try:
                batsmen.append({"name": bits[0].strip(), "runs": int(bits[1]), "balls": int(bits[2])})
            except (ValueError, IndexError):
                pass
    return batsmen


def _da_decode_s8(s8_raw: str):
    parts = str(s8_raw or "").split("#")
    if len(parts) >= 4 and parts[1].strip():
        try:
            return {"over_num": int(parts[0]), "bowler": parts[1].strip(),
                    "runs": int(parts[2]), "wickets": int(parts[3])}
        except (ValueError, IndexError):
            pass
    return None


def _da_decode_s7_bowl(s7_raw: str):
    """Parse S7 from the bowling/fielding team (PI=0).
    Format: 'Name#over_num#balls#runs#wickets'
    over_num uses the same cricket convention as S8 (first over = 1).
    """
    parts = str(s7_raw or "").split("#")
    if len(parts) >= 5 and parts[0].strip():
        try:
            return {
                "name":     parts[0].strip(),
                "over_num": int(parts[1]),
                "balls":    int(parts[2]),
                "runs":     int(parts[3]),
                "wickets":  int(parts[4]),
            }
        except (ValueError, IndexError):
            pass
    return None


def _da_looks_like_id(name: str) -> bool:
    return bool(_re.match(r'^\d+$', str(name or "").strip()))


def _da_load_row(row: dict):
    """Decode batting/bowling data from gold-embedded S6/S8 fields."""
    snap_id = row.get("snapshot_id")
    s6      = row.get("s6")
    s7_bat  = row.get("s7_bat")
    s8      = row.get("s8")
    s7_bowl = row.get("s7_bowl")

    if not s6 and not s8 and not s7_bowl:
        return snap_id, None

    batsmen_raw    = _da_decode_s6(s6)
    prev_over      = _da_decode_s8(s8)
    curr_over_bowl = _da_decode_s7_bowl(s7_bowl)
    striker_real   = str(s7_bat or "").split("#")[0].strip() or None

    id_map_entry: dict = {}
    if striker_real and batsmen_raw and _da_looks_like_id(batsmen_raw[0].get("name")):
        id_map_entry[batsmen_raw[0]["name"]] = striker_real

    batsmen = [dict(b) for b in batsmen_raw]
    if striker_real and batsmen and _da_looks_like_id(batsmen[0].get("name")):
        batsmen[0]["name"] = striker_real

    current_bowler = curr_over_bowl["name"] if curr_over_bowl else None

    return snap_id, {
        "batsmen":        batsmen,
        "current_bowler": current_bowler,
        "prev_over":      prev_over,
        "curr_over_bowl": curr_over_bowl,
        "id_map_entry":   id_map_entry,
    }


def view_detailed_analysis_html(req: func.HttpRequest) -> func.HttpResponse:
    """Per-match detailed analysis: batting/bowling scorecards, phase breakdown, chase."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        event_id = req.route_params.get("event_id", "")
        gold_c   = get_named_container_client("gold")

        tracker = download_json(gold_c, f"event_id={event_id}/innings_tracker.json")
        if not tracker:
            return func.HttpResponse(
                f"<h2>No gold data for event {escape(event_id)}.</h2>"
                "<p>Run the batch pipeline first.</p>",
                status_code=404, mimetype="text/html",
            )

        match_name = escape(str(tracker.get("match_name") or f"Match {event_id}"))
        league     = escape(str(tracker.get("league_name") or ""))
        match_date = escape(str(tracker.get("match_date_utc") or "")[:16].replace("T", " "))
        stadium    = tracker.get("stadium_data") or {}
        venue_str  = escape(", ".join(p for p in [
            stadium.get("name") or tracker.get("venue") or "",
            stadium.get("city") or "",
        ] if p))

        # ── filter transition rows (innings-break ghost) ──────────────────────
        # Ghost snapshot: innings=2 assigned because S3 set, but PG already reset to 0.0
        # so score still shows 1st innings total (score > 0, over = 0.0).
        def _is_transition(r):
            if r.get("innings") != 2 or not r.get("score"):
                return False
            try:
                pts = str(r.get("over") or "0").split(".")
                return int(pts[0]) == 0 and (int(pts[1]) if len(pts) > 1 else 0) == 0
            except Exception:
                return False

        all_rows  = [r for r in tracker.get("rows", []) if not _is_transition(r)]
        inn1_rows = [r for r in all_rows if r.get("innings") == 1]
        inn2_rows = [r for r in all_rows if r.get("innings") == 2]

        # ── pre-filter mislabeled end-of-inn1 rows from inn2_rows ────────────
        # At the final ball of inn1 BetsAPI sets S3 (target) before PG resets,
        # tagging those rows innings=2 with the inn1 final score and over ~19.x.
        # They appear first in time-order in inn2_rows and corrupt both the score
        # resolution heuristic (Issue 5) and the phase boundary inference (Issue 9)
        # if not removed before those steps run.
        # inn1_rows are never contaminated so inn1_rows[-1] is a safe estimate.
        _inn1_pre_score = inn1_rows[-1].get("score") if inn1_rows else None
        _inn1_pre_wkts  = inn1_rows[-1].get("wickets") if inn1_rows else None
        if _inn1_pre_score is not None:
            inn2_rows = [r for r in inn2_rows
                         if not (r.get("score") == _inn1_pre_score
                                 and r.get("wickets") == _inn1_pre_wkts)]

        # ── authoritative final scores ────────────────────────────────────────
        # score_summary_events for completed matches comes from /v1/event/view
        # which returns scores in innings order (inn1 first, inn2 second) —
        # the same source used by the matches table Final Score column.
        # Use parse_ss_final_scores directly, same as the matches table, so
        # both views show identical scores without a home/away swap heuristic.
        _fs = parse_ss_final_scores(tracker.get("score_summary_events") or "")
        inn1_runs = _fs["inn1_runs"]
        inn1_wkts = _fs["inn1_wickets"]
        inn2_runs = _fs["inn2_runs"]
        inn2_wkts = _fs["inn2_wickets"]

        # Fall back to last captured snapshot when score_summary_events is absent
        # (live match in progress, or old gold data without score_summary_events).
        if inn1_runs is None:
            inn1_runs = inn1_rows[-1].get("score") if inn1_rows else None
            inn1_wkts = inn1_rows[-1].get("wickets") if inn1_rows else None
        if inn2_runs is None:
            inn2_runs = inn2_rows[-1].get("score") if inn2_rows else None
            inn2_wkts = inn2_rows[-1].get("wickets") if inn2_rows else None

        # Second-pass filter with authoritative scores in case inn1_pre_score
        # differed from the authoritative inn1_runs (e.g. last snapshot was partial).
        if inn1_runs is not None and inn1_wkts is not None:
            inn2_rows = [r for r in inn2_rows
                         if not (r.get("score") == inn1_runs and r.get("wickets") == inn1_wkts)]

        target = (inn1_runs + 1) if inn1_runs is not None else None

        # ── decode embedded gold S6/S8 per row in parallel ───────────────────
        silver_by_snap: dict = {}
        with ThreadPoolExecutor(max_workers=32) as ex:
            futs = {ex.submit(_da_load_row, r): r for r in all_rows}
            for fut in as_completed(futs):
                sid, dec = fut.result()
                if dec:
                    silver_by_snap[sid] = dec

        # Build ID → name map from all snapshots
        id_to_name: dict = {}
        for sv in silver_by_snap.values():
            id_to_name.update(sv.get("id_map_entry") or {})

        def resolve(name):
            return id_to_name.get(str(name or "").strip(), str(name or "").strip())

        # ── helpers ───────────────────────────────────────────────────────────
        def ov_int(r):
            try:
                return int(str(r.get("over") or "0").split(".")[0])
            except Exception:
                return 0

        def ov_float(ov_str):
            try:
                p = str(ov_str or "0").split(".")
                return int(p[0]) + (int(p[1]) / 6 if len(p) > 1 else 0)
            except Exception:
                return 0.0

        def ov_sort_key(ov_str):
            try:
                p = str(ov_str or "0").split(".")
                return int(p[0]) * 6 + (int(p[1]) if len(p) > 1 else 0)
            except Exception:
                return 0

        def get_over_end_window(inn_rows_local, over_num):
            target_ov = f"{over_num}.0"
            for r in inn_rows_local:
                if str(r.get("over") or "").strip() == target_ov:
                    return r.get("ball_window") or []
            best = None
            for r in inn_rows_local:
                if ov_int(r) == over_num - 1:
                    best = r
            return (best.get("ball_window") or []) if best else []

        # ── batting scorecard ─────────────────────────────────────────────────
        def build_batting(inn_rows_local):
            seen: dict = {}      # name → {entry_over, runs, balls, _order}
            order_ctr  = [0]
            dismissed  = []
            prev_list: list = [] # ordered (striker first from s6)

            for r in inn_rows_local:
                # over 0.0 = no balls bowled yet; s6 often has PLAYERFULLNAME placeholders
                if str(r.get("over") or "").strip() in ("0.0", "0"):
                    continue
                sv = silver_by_snap.get(r.get("snapshot_id")) or {}
                batsmen = sv.get("batsmen") or []
                if not batsmen:
                    continue

                curr_list = [resolve(b["name"]) for b in batsmen if b.get("name")]
                curr_set  = set(curr_list)
                prev_set  = set(prev_list)

                # New batsmen: add in s6 order (striker is first in list)
                for name in curr_list:
                    if name not in prev_set and name not in seen:
                        seen[name] = {"entry_over": r.get("over"), "runs": 0, "balls": 0,
                                      "_order": order_ctr[0]}
                        order_ctr[0] += 1

                # Update stats
                for b in batsmen:
                    nm = resolve(b.get("name"))
                    if nm in seen:
                        seen[nm]["runs"]  = b.get("runs", 0)
                        seen[nm]["balls"] = b.get("balls", 0)

                # Dismissals: players in prev but not curr
                for name in prev_set - curr_set:
                    if name in seen:
                        e = seen.pop(name)
                        sr = round(e["runs"] / e["balls"] * 100, 1) if e["balls"] else 0
                        dismissed.append({"batsman": name, "runs": e["runs"], "balls": e["balls"],
                                          "SR": sr, "entry_over": e["entry_over"],
                                          "status": "dismissed", "_order": e["_order"]})
                prev_list = curr_list

            for nm, e in seen.items():
                sr = round(e["runs"] / e["balls"] * 100, 1) if e["balls"] else 0
                dismissed.append({"batsman": nm, "runs": e["runs"], "balls": e["balls"],
                                  "SR": sr, "entry_over": e["entry_over"],
                                  "status": "not out", "_order": e["_order"]})

            # Dedup: same batsman can appear twice if they temporarily vanished from s6
            # (e.g. incomplete snapshot). Keep earliest order; keep highest-balls stats.
            merged: dict = {}
            for entry in dismissed:
                name = entry["batsman"]
                if name not in merged:
                    merged[name] = entry.copy()
                else:
                    ex = merged[name]
                    if entry["_order"] < ex["_order"]:
                        merged[name]["_order"]     = entry["_order"]
                        merged[name]["entry_over"] = entry["entry_over"]
                    if entry["balls"] > ex["balls"]:
                        merged[name]["runs"]   = entry["runs"]
                        merged[name]["balls"]  = entry["balls"]
                        merged[name]["SR"]     = entry["SR"]
                        merged[name]["status"] = entry["status"]

            return sorted(merged.values(), key=lambda x: x.get("_order", 999))

        # ── bowling scorecard ─────────────────────────────────────────────────
        def build_bowling(inn_rows_local, all_rows_local, innings_num):
            overs_by_num: dict = {}
            for r in inn_rows_local:
                sv = silver_by_snap.get(r.get("snapshot_id")) or {}
                po = sv.get("prev_over")
                if not (po and po.get("bowler") and po.get("over_num") is not None):
                    continue
                if po["over_num"] > ov_int(r):
                    continue  # carryover from previous innings
                overs_by_num[po["over_num"]] = po

            # Recover last over from first rows of next innings S8 carryover
            next_rows    = [r for r in all_rows_local if r.get("innings") == innings_num + 1]
            existing_max = max(overs_by_num.keys(), default=0)
            for r in next_rows[:20]:
                sv = silver_by_snap.get(r.get("snapshot_id")) or {}
                po = sv.get("prev_over")
                if po and po.get("bowler") and po.get("over_num") is not None:
                    ov_num = po["over_num"]
                    if ov_num not in overs_by_num and ov_num > existing_max:
                        overs_by_num[ov_num] = po
                    break

            # S7_bowl fallback: fills in overs where S8 was never captured.
            # S7_bowl tracks the CURRENT over's running stats at each snapshot.
            # Iterating in row order means the last update per over_num holds
            # the final ball count for that over — identical data to what S8
            # would have reported at the start of the next over.
            s7_last: dict = {}
            for r in inn_rows_local:
                sv = silver_by_snap.get(r.get("snapshot_id")) or {}
                cob = sv.get("curr_over_bowl")
                if not cob or not cob.get("name"):
                    continue
                s7_last[cob["over_num"]] = cob
            for ov_num, cob in s7_last.items():
                if ov_num not in overs_by_num:
                    overs_by_num[ov_num] = {
                        "over_num": ov_num,
                        "bowler":   cob["name"],
                        "runs":     cob["runs"],
                        "wickets":  cob["wickets"],
                    }

            bowler_agg: dict = {}
            for over_num in sorted(overs_by_num):
                po = overs_by_num[over_num]
                nm = po["bowler"]
                if nm not in bowler_agg:
                    bowler_agg[nm] = {"overs": 0, "runs": 0, "wickets": 0,
                                      "first_over": over_num, "over_list": []}
                bowler_agg[nm]["overs"]    += 1
                bowler_agg[nm]["runs"]     += po["runs"]
                bowler_agg[nm]["wickets"]  += po["wickets"]
                bowler_agg[nm]["over_list"].append(f"O{over_num}:{po['runs']}/{po['wickets']}")
                bowler_agg[nm]["first_over"] = min(bowler_agg[nm]["first_over"], over_num)

            result = []
            for nm, d in sorted(bowler_agg.items(), key=lambda x: x[1]["first_over"]):
                eco = round(d["runs"] / d["overs"], 2) if d["overs"] else 0
                result.append({"bowler": nm, "overs": d["overs"], "runs": d["runs"],
                               "wickets": d["wickets"], "economy": eco,
                               "breakdown": "  ".join(d["over_list"])})
            return result, overs_by_num

        # ── phase breakdown ───────────────────────────────────────────────────
        PHASE_OVERS = {
            "Powerplay (1-6)": range(1, 7),
            "Middle (7-15)":   range(7, 16),
            "Death (16-20)":   range(16, 21),
        }

        def phase_label(over_str):
            try:
                ov = int(str(over_str or "0").split(".")[0])
                if ov < 6:  return "Powerplay (1-6)"
                if ov < 15: return "Middle (7-15)"
                return "Death (16-20)"
            except Exception:
                return "Unknown"

        def build_phases(inn_rows_local, auth_runs, auth_wkts):
            buckets = {p: {"start_score": None, "end_score": None,
                           "start_wkts": None, "end_wkts": None,
                           "start_over": None, "end_over": None,
                           "fours": 0, "sixes": 0, "dots": 0, "singles": 0, "doubles": 0}
                       for p in PHASE_OVERS}
            for r in inn_rows_local:
                ph = phase_label(r.get("over"))
                if ph not in buckets:
                    continue
                b = buckets[ph]
                if b["start_score"] is None:
                    b["start_score"] = r.get("score") or 0
                    b["start_wkts"]  = r.get("wickets") or 0
                    b["start_over"]  = r.get("over")
                b["end_score"] = r.get("score") or 0
                b["end_wkts"]  = r.get("wickets") or 0
                b["end_over"]  = r.get("over")
            for ph, ov_range in PHASE_OVERS.items():
                b = buckets[ph]
                if b["start_score"] is None:
                    continue
                for ov_num in ov_range:
                    for ball in get_over_end_window(inn_rows_local, ov_num):
                        bs = str(ball).lower().strip()
                        if any(x in bs for x in ("wd", "nb", "lb")):
                            pass
                        elif bs in ("w", "0"):
                            b["dots"] += 1
                        elif bs == "4":
                            b["fours"] += 1
                        elif bs == "6":
                            b["sixes"] += 1
                        elif bs == "1":
                            b["singles"] += 1
                        elif bs == "2":
                            b["doubles"] += 1
            # Infer phase boundary scores when the exact X.0 snapshot is missing.
            # For each non-Death phase, find the first snapshot of the next phase and
            # subtract the runs from balls already bowled in that new over to recover
            # the true score at the end of the previous phase.
            _ph_order = list(PHASE_OVERS.keys())
            for _ci, _cph in enumerate(_ph_order[:-1]):
                _cb = buckets[_cph]
                if _cb["end_score"] is None:
                    continue
                _nph = _ph_order[_ci + 1]
                _fn  = next((r for r in inn_rows_local if phase_label(r.get("over")) == _nph), None)
                if _fn is None:
                    continue
                # How many legal balls have been bowled in the new over at this snapshot?
                try:
                    _legal = int(str(_fn.get("over") or "0").split(".")[1])
                except (ValueError, IndexError):
                    _legal = 0
                # Walk ball_window (newest first) counting runs/wickets for those legal balls.
                _bw       = _fn.get("ball_window") or []
                _rsub     = 0
                _wsub     = 0
                _seen_leg = 0
                for _bl in _bw:
                    if _seen_leg >= _legal:
                        break
                    _bs = str(_bl).strip().lower()
                    if _bs == "w":
                        _wsub     += 1
                        _seen_leg += 1
                    elif any(x in _bs for x in ("wd", "nb", "lb")):
                        _m = _re.match(r'^(\d+)', _bs)
                        _rsub += int(_m.group(1)) if _m else 0
                        # extras don't consume legal ball count
                    else:
                        try:
                            _rsub += int(_bs)
                        except ValueError:
                            pass
                        _seen_leg += 1
                _inf_score = (_fn.get("score") or 0) - _rsub
                _inf_wkts  = (_fn.get("wickets") or 0) - _wsub
                # Only update if the inferred score is higher — the inference gives
                # the score at the phase boundary, which can't be less than any
                # snapshot taken within that phase.
                if _inf_score > _cb["end_score"]:
                    _cb["end_score"] = _inf_score
                    _cb["end_wkts"]  = _inf_wkts

            result = []
            for ph in PHASE_OVERS:
                b = buckets[ph]
                if b["start_score"] is None:
                    continue
                end_score = b["end_score"]
                end_wkts  = b["end_wkts"]
                # Correct the Death phase end score with the authoritative final score.
                # The last captured snapshot may be missing a few balls at innings end.
                if "Death" in ph and auth_runs is not None and auth_runs >= end_score:
                    end_score = auth_runs
                    end_wkts  = auth_wkts if auth_wkts is not None else end_wkts
                runs    = end_score - b["start_score"]
                wickets = end_wkts  - b["start_wkts"]
                br      = b["fours"] * 4 + b["sixes"] * 6
                ph_ovs  = round(ov_float(b["end_over"]) - ov_float(b["start_over"]), 2)
                rr      = round(runs / ph_ovs, 2) if ph_ovs > 0 else 0
                result.append({"phase": ph, "runs": runs, "wickets": wickets, "rr": rr,
                               "fours": b["fours"], "sixes": b["sixes"], "boundary_runs": br,
                               "singles": b["singles"], "doubles": b["doubles"], "dots": b["dots"],
                               "score_range": f"{b['start_score']}/{b['start_wkts']} → {end_score}/{end_wkts}"})
            return result

        # ── build all sections ────────────────────────────────────────────────
        bat1   = build_batting(inn1_rows)
        bat2   = build_batting(inn2_rows)
        bowl1, overs1 = build_bowling(inn1_rows, all_rows, 1)
        bowl2, overs2 = build_bowling(inn2_rows, all_rows, 2)
        phase1 = build_phases(inn1_rows, inn1_runs, inn1_wkts)
        phase2 = build_phases(inn2_rows, inn2_runs, inn2_wkts)

        # ── HTML rendering helpers ────────────────────────────────────────────
        def thead(*cols):
            return "<thead><tr>" + "".join(f"<th>{c}</th>" for c in cols) + "</tr></thead>"

        def trow(*cells, cls=""):
            row_cls = f' class="{cls}"' if cls else ""
            return "<tr" + row_cls + ">" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"

        def section(title, content):
            return f'<div class="section"><h2>{title}</h2>{content}</div>'

        def table(head_html, body_rows, cls=""):
            t_cls = f' class="{cls}"' if cls else ""
            return f'<table{t_cls}>{head_html}<tbody>{"".join(body_rows)}</tbody></table>'

        def batting_html(sc, inn_label):
            if not sc:
                return f"<p>No batting data for {inn_label}.</p>"
            rows_html = []
            for b in sc:
                status_cls = "not-out" if b["status"] == "not out" else ""
                rows_html.append(trow(
                    escape(b["batsman"]),
                    b["runs"], b["balls"],
                    f'{b["SR"]:.1f}',
                    escape(str(b.get("entry_over") or "")),
                    escape(b["status"]),
                    cls=status_cls,
                ))
            return table(thead("Batsman", "Runs", "Balls", "SR", "Entry Over", "Status"), rows_html)

        def bowling_section_html(bowl_sc, overs_by_num, inn_rows_local, inn_label):
            if not bowl_sc:
                return f"<p>No bowling data for {inn_label}.</p>"
            ov_rows = []
            for ov_num in sorted(overs_by_num):
                po = overs_by_num[ov_num]
                bw = get_over_end_window(inn_rows_local, ov_num)
                balls_str = escape(" ".join(str(b) for b in bw)) if bw else "—"
                ov_rows.append(trow(str(ov_num), escape(po["bowler"]),
                                    str(po["runs"]), str(po["wickets"]), balls_str))
            ov_tbl = table(
                thead("Over", "Bowler", "Runs", "Wkts", "Ball sequence"),
                ov_rows, cls="over-table",
            )
            bowl_rows = [
                trow(escape(b["bowler"]), str(b["overs"]), str(b["runs"]),
                     str(b["wickets"]), f'{b["economy"]:.2f}', escape(b["breakdown"]))
                for b in bowl_sc
            ]
            bowl_tbl = table(
                thead("Bowler", "Overs", "Runs", "Wkts", "Economy", "Breakdown"),
                bowl_rows,
            )
            return f'<h3>Over by Over</h3>{ov_tbl}<h3>Bowling Summary</h3>{bowl_tbl}'

        def phase_html(phases, inn_label):
            if not phases:
                return f"<p>No phase data for {inn_label}.</p>"
            rows_html = [
                trow(escape(p["phase"]), p["runs"], p["wickets"],
                     f'{p["rr"]:.2f}', p["fours"], p["sixes"],
                     p["boundary_runs"], p["singles"], p["doubles"], p["dots"],
                     escape(p["score_range"]))
                for p in phases
            ]
            return table(
                thead("Phase", "Runs", "Wkts", "RR", "4s", "6s", "Boundary Runs",
                      "1s", "2s", "Dots", "Score Range"),
                rows_html,
            )

        def chase_html():
            if not inn2_rows or not target:
                return ""
            rows_html = []
            for r in inn2_rows:
                score  = r.get("score") or 0
                ov_str = str(r.get("over") or "0")
                try:
                    p  = ov_str.split(".")
                    bd = int(p[0]) * 6 + (int(p[1]) if len(p) > 1 else 0)
                except Exception:
                    bd = 0
                bl  = 120 - bd
                rn  = target - score
                crr = round(score / (bd / 6), 2) if bd > 0 else 0
                rrr = round(rn / bl * 6, 2) if bl > 0 else 0
                bat_odds = r.get("batting_team_odds")
                wp = round(1 / bat_odds * 100, 1) if bat_odds and bat_odds > 0 else None
                rows_html.append(trow(
                    ov_str, str(score), str(r.get("wickets") or 0),
                    str(rn), str(bl), f"{crr:.2f}", f"{rrr:.2f}",
                    f"{wp:.1f}%" if wp else "—",
                ))
            return table(
                thead("Over", "Score", "Wkts", "Needed", "Balls Left", "CRR", "RRR",
                      "Win% (implied)"),
                rows_html,
            )

        # ── assemble HTML ─────────────────────────────────────────────────────
        s1_score   = f"{inn1_runs}/{inn1_wkts}" if inn1_runs is not None else ("—" if not inn1_rows else "?/?")
        s2_score   = f"{inn2_runs}/{inn2_wkts}" if inn2_runs is not None else ("—" if not inn2_rows else "?/?")
        target_str = f"Target: {target}" if target else ""

        nav = (
            f'<a href="/api/matches/{escape(event_id)}/view">← Match</a> | '
            f'<a href="/api/matches/{escape(event_id)}/innings-tracker/view">Innings Tracker</a> | '
            f'<a href="/api/matches/{escape(event_id)}/heatmap">Heatmap</a>'
        )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Detailed Analysis — {match_name}</title>
<style>
  body{{font-family:system-ui,sans-serif;margin:0;padding:16px;background:#f5f5f5;color:#222;}}
  h1{{margin:0 0 4px;font-size:1.3rem;}}
  .meta{{color:#555;font-size:.85rem;margin-bottom:4px;}}
  nav{{margin:10px 0 18px;font-size:.9rem;}}
  nav a{{color:#2563eb;text-decoration:none;margin-right:4px;}}
  nav a:hover{{text-decoration:underline;}}
  .scoreline{{display:flex;gap:24px;margin:12px 0 20px;flex-wrap:wrap;}}
  .score-box{{background:#fff;border-radius:8px;padding:12px 20px;box-shadow:0 1px 4px rgba(0,0,0,.1);}}
  .score-box .label{{font-size:.75rem;color:#888;text-transform:uppercase;letter-spacing:.05em;}}
  .score-box .val{{font-size:1.6rem;font-weight:700;margin-top:2px;}}
  .section{{background:#fff;border-radius:8px;padding:16px 20px;margin-bottom:20px;
            box-shadow:0 1px 4px rgba(0,0,0,.08);}}
  .section h2{{margin:0 0 12px;font-size:1rem;color:#444;border-bottom:1px solid #eee;
               padding-bottom:6px;}}
  .section h3{{margin:16px 0 8px;font-size:.9rem;color:#666;}}
  table{{width:100%;border-collapse:collapse;font-size:.85rem;}}
  th{{background:#1e293b;color:#fff;padding:8px 10px;text-align:left;position:sticky;top:0;
      font-weight:600;font-size:.8rem;white-space:nowrap;}}
  td{{padding:7px 10px;border-bottom:1px solid #f0f0f0;}}
  tr:hover td{{background:#f8faff;}}
  .not-out td{{color:#15803d;font-weight:600;}}
  .over-table td:first-child{{font-weight:700;color:#2563eb;width:40px;}}
  .over-table td:last-child{{font-family:monospace;font-size:.8rem;color:#555;}}
  .tab-bar{{display:flex;gap:8px;margin-bottom:16px;}}
  .tab{{padding:6px 14px;border-radius:20px;border:1px solid #d1d5db;background:#fff;
        cursor:pointer;font-size:.85rem;color:#555;}}
  .tab.active{{background:#2563eb;color:#fff;border-color:#2563eb;}}
  @media(max-width:600px){{td,th{{padding:5px 6px;font-size:.78rem;}}}}
</style>
</head>
<body>
<h1>{match_name}</h1>
<div class="meta">{league}</div>
<div class="meta">{match_date}{(' — ' + venue_str) if venue_str else ''}</div>
<nav>{nav}</nav>

<div class="scoreline">
  <div class="score-box"><div class="label">1st Innings</div><div class="val">{s1_score}</div></div>
  {'<div class="score-box"><div class="label">2nd Innings</div><div class="val">' + s2_score + '</div></div>' if inn2_rows else ''}
  {'<div class="score-box"><div class="label">Target</div><div class="val">' + str(target) + '</div></div>' if target else ''}
</div>

<div class="tab-bar">
  <button class="tab active" onclick="showInn(1,this)">1st Innings</button>
  {'<button class="tab" onclick="showInn(2,this)">2nd Innings</button>' if inn2_rows else ''}
</div>

<div id="inn-1">
  {section("Batting — 1st Innings", batting_html(bat1, "1st Innings"))}
  {section("Bowling — 1st Innings", bowling_section_html(bowl1, overs1, inn1_rows, "1st Innings"))}
  {section("Phase Breakdown — 1st Innings", phase_html(phase1, "1st Innings"))}
</div>

{'<div id="inn-2" style="display:none">' if inn2_rows else ''}
  {'  ' + section("Batting — 2nd Innings", batting_html(bat2, "2nd Innings")) if inn2_rows else ''}
  {'  ' + section("Bowling — 2nd Innings", bowling_section_html(bowl2, overs2, inn2_rows, "2nd Innings")) if inn2_rows else ''}
  {'  ' + section("Phase Breakdown — 2nd Innings", phase_html(phase2, "2nd Innings")) if inn2_rows else ''}
  {'  ' + section(f"Chase Analysis — Target {target}", chase_html()) if inn2_rows else ''}
{'</div>' if inn2_rows else ''}

<script>
function showInn(n, btn) {{
  document.getElementById('inn-1').style.display = n === 1 ? '' : 'none';
  var d2 = document.getElementById('inn-2');
  if (d2) d2.style.display = n === 2 ? '' : 'none';
  document.querySelectorAll('.tab').forEach(function(t){{t.classList.remove('active');}});
  btn.classList.add('active');
}}
</script>
</body>
</html>"""

        return func.HttpResponse(html, status_code=200, mimetype="text/html")

    except Exception as ex:
        logging.exception("Failed to render detailed analysis")
        return func.HttpResponse(f"Error: {escape(str(ex))}", status_code=500)


def view_match_live_markets(req: func.HttpRequest) -> func.HttpResponse:
    """Redirects to market heatmap (live markets page removed)."""
    event_id = req.route_params.get("event_id", "")
    return func.HttpResponse(
        status_code=302,
        headers={"Location": f"/api/matches/{event_id}/heatmap"},
    )


def view_match_markets_raw(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        page = (
            download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_dashboard.json")
            or download_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
        )
        if not page:
            return func.HttpResponse(json.dumps({"error": "No gold data for this event"}), status_code=404, mimetype="application/json")
        fi = (page.get("snapshot") or {}).get("fi")
        if not fi:
            return func.HttpResponse(json.dumps({"error": "FI not found in gold data for this event"}), status_code=404, mimetype="application/json")
        return func.HttpResponse(json.dumps({"event_id": event_id, "fi": fi}, indent=2), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to fetch raw markets")
        return func.HttpResponse(json.dumps({"error": str(ex)}), status_code=500, mimetype="application/json")
