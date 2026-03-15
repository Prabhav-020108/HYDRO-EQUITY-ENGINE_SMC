"""
Hydro-Equity Engine — Phase 4a
backend/routers/alerts.py

GET /alerts/active?scenario=<baseline|leak|valve|surge>
  → Returns V5 CLPS alerts formatted for the Leaflet dashboard.
  Protected: requires Bearer token.
  ward_officer: filtered to their zone_id (server-side).

Response format matches what index.html renderAlerts() expects:
  { alerts: [{title, body, level, zone, zone_id_short, clps, dominant_signal, db_alert_id}],
    scenario, total }
"""

import os, json
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text

from backend.auth import get_current_user
from backend.database import engine

router = APIRouter(tags=["Analytics"])

OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')

# Zone ID → display name + short ID (matches ZONES array in index.html)
ZONE_MAP = {
    'zone_1': {'nm': 'Zone 1', 'short': 'z1'},
    'zone_2': {'nm': 'Zone 2', 'short': 'z2'},
    'zone_3': {'nm': 'Zone 3', 'short': 'z3'},
    'zone_4': {'nm': 'Zone 4', 'short': 'z4'},
    'zone_5': {'nm': 'Zone 5', 'short': 'z5'},
    'zone_6': {'nm': 'Zone 6', 'short': 'z6'},
    'zone_7': {'nm': 'Zone 7', 'short': 'z7'},
    'zone_8': {'nm': 'Zone 8', 'short': 'z8'},
}

# Dominant signal → human-readable body text
SIGNAL_BODY = {
    'PDR_n': 'Sudden pressure drop detected — dispatch field team to inspect.',
    'FPI':   'Flow-pressure imbalance — probable pipe leakage in distribution network.',
    'NFA':   'Night flow anomaly — inspect for unauthorized extraction between 01:00–04:00.',
    'DDI':   'Demand deviation from expected pattern — check valve status and consumption.',
}

# Scenario → human-readable title suffix
SCENARIO_SUFFIX = {
    'baseline': 'Anomaly',
    'leak':     'Leak Alert',
    'valve':    'Valve Alert',
    'surge':    'Surge Alert',
}


@router.get(
    "/alerts/active",
    summary="Active Leak & Anomaly Alerts (CLPS)",
    description=(
        "Returns V5 CLPS alerts for the requested scenario. "
        "ward_officer sees only their assigned zone. "
        "Response includes db_alert_id for resolution workflow (requires db_migrate.py to have been run)."
    )
)
def get_active_alerts(
    scenario: str = Query(
        default='baseline',
        description="Scenario name: baseline | leak | valve | surge"
    ),
    current_user: dict = Depends(get_current_user)
):
    # ── Load v5_alerts.json ───────────────────────────────────────
    path = os.path.join(OUTPUTS_DIR, "v5_alerts.json")
    if not os.path.exists(path):
        return {
            "alerts":   [],
            "scenario": scenario,
            "total":    0,
            "error":    "Run V5 first — v5_alerts.json not found in outputs/"
        }

    with open(path, encoding='utf-8') as f:
        data = json.load(f)

    # ── Extract alerts for requested scenario ─────────────────────
    raw = []
    if isinstance(data, dict):
        raw = data.get(scenario, data.get('baseline', []))
    elif isinstance(data, list):
        raw = data

    # ── Role-based zone filtering (server-side) ───────────────────
    role    = current_user.get('role', '')
    zone_id = current_user.get('zone_id')
    if role == 'ward_officer' and zone_id:
        raw = [a for a in raw if str(a.get('zone_id', '')) == str(zone_id)]

    # ── Fetch db_alert_ids from PostgreSQL ────────────────────────
    # Maps zone_id → alert_id for resolution workflow (Acknowledge/Resolve)
    db_ids: dict = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT alert_id, zone_id FROM alerts WHERE scenario = :scen"),
                {'scen': scenario}
            ).fetchall()
            for row in rows:
                db_ids[row[1]] = row[0]
    except Exception:
        pass  # PostgreSQL not available or alerts table empty — use 0 as fallback

    # ── Format each alert for the dashboard ──────────────────────
    suffix = SCENARIO_SUFFIX.get(scenario, 'Alert')
    formatted = []
    for a in raw:
        zid  = str(a.get('zone_id', ''))
        zm   = ZONE_MAP.get(zid, {
            'nm':    zid.replace('_', ' ').title(),
            'short': zid.replace('zone_', 'z')
        })
        sig  = str(a.get('dominant_signal', 'PDR_n'))
        lvl  = str(a.get('severity', 'moderate') or 'moderate')
        clps = float(a.get('clps', 0) or 0)

        formatted.append({
            'title':           f"{zm['nm']} · {sig} {suffix}",
            'body':            SIGNAL_BODY.get(sig, f"Anomaly detected: dominant signal {sig}."),
            'level':           lvl,
            'zone':            zm['nm'],
            'zone_id_short':   zm['short'],
            'zone_id':         zid,
            'clps':            round(clps, 3),
            'dominant_signal': sig,
            'probable_nodes':  a.get('probable_node_ids', []),
            'db_alert_id':     db_ids.get(zid, 0),
        })

    # Sort by CLPS descending
    formatted.sort(key=lambda x: x['clps'], reverse=True)

    return {
        'alerts':   formatted,
        'scenario': scenario,
        'total':    len(formatted)
    }