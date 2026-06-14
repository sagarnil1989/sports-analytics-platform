import re
from typing import Any, Dict, List, Optional

from storage import safe_float


def extract_bet365_records(bet365_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = bet365_payload.get("response", {}).get("body") or {}
    results = body.get("results", [])
    if results and isinstance(results[0], list):
        return results[0]
    if isinstance(results, list):
        return results
    return []


def fractional_odds_to_decimal(value: Any) -> Optional[float]:
    """Convert Bet365 fractional odds such as 13/8 to decimal."""
    if value is None or value == "":
        return None
    raw = str(value).strip()
    if "/" not in raw:
        return safe_float(raw)
    try:
        numerator, denominator = raw.split("/", 1)
        denominator_float = float(denominator)
        if denominator_float == 0:
            return None
        return round(1 + (float(numerator) / denominator_float), 3)
    except Exception:
        return None


def extract_market_template_id_from_it(it_value: Any) -> Optional[str]:
    if not it_value:
        return None
    match = re.search(r"-(\d+)-H", str(it_value))
    return match.group(1) if match else None


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def extract_bet365_current_markets(
    bet365_event_payload: Dict[str, Any],
    snapshot_id: str,
    snapshot_time_utc: str,
    event_id: str,
    fi: str,
    score_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Parse all current Bet365 markets from a /v1/bet365/event payload.

    Bet365 event data is a flat ordered stream:
    MG = market group, MA = market/column, PA = selection/price row.
    """
    records = extract_bet365_records(bet365_event_payload)
    rows: List[Dict[str, Any]] = []

    current_group: Dict[str, Any] = {}
    current_market: Dict[str, Any] = {}
    line_by_template_id: Dict[str, str] = {}
    line_by_order: Dict[str, str] = {}

    for r in records:
        if not isinstance(r, dict):
            continue

        r_type = r.get("type")

        if r_type == "MG":
            current_group = {
                "market_group_id": clean_text(r.get("ID")),
                "market_group_name": clean_text(r.get("NA")) or clean_text(r.get("IT")) or "Unknown Market",
                "market_group_order": clean_text(r.get("OR")),
                "market_group_suspended": clean_text(r.get("SU")),
                "market_group_raw": r,
            }
            current_market = {}
            line_by_template_id = {}
            line_by_order = {}
            continue

        if r_type == "MA":
            ma_name = clean_text(r.get("NA"))
            current_market = {
                "market_id": clean_text(r.get("ID")),
                "market_name": ma_name,
                "market_it": clean_text(r.get("IT")),
                "market_py": clean_text(r.get("PY")),
                "market_sy": clean_text(r.get("SY")),
                "market_order": clean_text(r.get("OR")),
                "market_raw": r,
            }
            continue

        if r_type != "PA":
            continue

        odds_fractional = clean_text(r.get("OD"))

        if not odds_fractional:
            line_value = clean_text(r.get("NA"))
            if line_value:
                template_id = clean_text(r.get("MA")) or extract_market_template_id_from_it(r.get("IT"))
                if template_id:
                    line_by_template_id[template_id] = line_value
                order = clean_text(r.get("OR"))
                if order:
                    line_by_order[order] = line_value
            continue

        selection_order = clean_text(r.get("OR"))
        template_id = clean_text(r.get("MA"))
        line_value = clean_text(r.get("HA")) or clean_text(r.get("HD"))
        if not line_value and template_id:
            line_value = line_by_template_id.get(template_id)
        if not line_value and selection_order:
            line_value = line_by_order.get(selection_order)

        market_group_name = current_group.get("market_group_name") or "Unknown Market"
        market_name = current_market.get("market_name")
        selection_name = clean_text(r.get("NA"))

        if not selection_name:
            selection_name = market_name or market_group_name

        display_selection_name = selection_name
        if line_value and selection_name and line_value not in selection_name:
            display_selection_name = f"{selection_name} {line_value}"

        suspended_raw = clean_text(r.get("SU"))
        is_suspended = suspended_raw == "0" or current_group.get("market_group_suspended") == "0"

        rows.append({
            "snapshot_id": snapshot_id,
            "snapshot_time_utc": snapshot_time_utc,
            "event_id": event_id,
            "fi": fi,
            "score_from_match_snapshot": score_context.get("score"),
            "record_type": r_type,
            "market_group_id": current_group.get("market_group_id"),
            "market_group_name": market_group_name,
            "market_group_order": current_group.get("market_group_order"),
            "market_group_suspended": current_group.get("market_group_suspended"),
            "market_id": current_market.get("market_id"),
            "market_name": market_name,
            "market_order": current_market.get("market_order"),
            "market_template_id": template_id,
            "selection_id": clean_text(r.get("ID")),
            "selection_name": selection_name,
            "display_selection_name": display_selection_name,
            "selection_order": selection_order,
            "odds_fractional": odds_fractional,
            "odds_decimal": fractional_odds_to_decimal(odds_fractional),
            "odds": odds_fractional,
            "line": line_value,
            "handicap": line_value,
            "suspended": is_suspended,
            "suspended_raw": suspended_raw,
            "raw": r,
        })

    return rows
