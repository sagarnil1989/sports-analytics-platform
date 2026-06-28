"""Shared imports and utilities used across all view modules."""
import json
import logging
import os
from html import escape
from typing import Any, Dict, List, Optional

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError

from storage import (
    call_betsapi,
    download_json,
    download_required_json,
    format_unix_ts,
    get_named_container_client,
    parse_ss_final_scores,
    safe_float,
    upload_json,
    utc_now,
)
from bet365_parser import extract_bet365_current_markets
from league_config import collect_known_leagues, load_disabled_league_ids, save_league_preferences
from innings_tracker_writer import extract_innings_snapshot


def adf_activity_badge(*activities: str, pipeline: str = "pl_ml_and_hypothesis") -> str:
    """Small note showing which ADF pipeline/activity populates this page's data.
    Activity names match the page name, not the underlying notebook/script filename —
    see infra/8.adf-config/main.tf pl_ml_and_hypothesis for the full mapping."""
    acts = " + ".join(f"<code>{escape(a)}</code>" for a in activities)
    return (
        f'<p style="color:#888;font-size:11px;margin:0 0 10px;">'
        f'Populated by ADF pipeline <code>{escape(pipeline)}</code> → activity {acts}'
        f'</p>'
    )


def build_simple_table_page(title: str, headers: List[str], rows_html: str, back_link: Optional[str] = None) -> str:
    back_html = f'<p><a href="{escape(back_link)}">← Back</a></p>' if back_link else ""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{escape(title)}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
            h1 {{ margin-bottom: 8px; }}
            .hint {{ color: #666; margin-bottom: 20px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 8px #ddd; }}
            th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; vertical-align: top; }}
            th {{ background: #222; color: white; position: sticky; top: 0; }}
            a {{ color: #0066cc; font-weight: bold; text-decoration: none; }}
            .pill {{ display: inline-block; padding: 3px 8px; background: #eee; border-radius: 999px; }}
        </style>
    </head>
    <body>
        {back_html}
        <h1>{escape(title)}</h1>
        <div class="hint">This page reads a small pre-built gold index file, so it should load quickly.</div>
        <table id="matchTable">
            <thead><tr>{''.join(f'<th>{escape(h)}</th>' for h in headers)}</tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </body>
    </html>
    """
