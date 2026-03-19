"""
Hydro-Equity Engine / Dhara — Phase 4b + M1
backend/routers/alerts.py

Bible Reference: Section 3 M1 — "New REST Endpoints"

ENDPOINTS IN THIS FILE:
  GET  /alerts/active?scenario=<baseline|leak|valve|surge>
                      &status=<new|acknowledged|resolve_requested|resolved>
       → Existing endpoint. Added optional ?status= filter (no breaking change).
       → ward_officer sees only their zone_id server-side.

  POST /alerts/{id}/acknowledge
       → engineer role only
       → Transition: new/fired → acknowledged
       → Saves acknowledged_by + acknowledged_at

  POST /alerts/{id}/request-resolution
       → field_operator role only
       → Transition: acknowledged → resolve_requested
       → Saves resolution_report

  POST /alerts/{id}/accept-resolution
       → engineer role only
       → Transition: resolve_requested → resolved
       → Saves resolved_at

  POST /alerts/{id}/reject-resolution
       → engineer role only
       → Transition: resolve_requested → acknowledged  (sends back)
       → Increments rejected_count

  POST /alerts/{id}/resolve
       → Backward-compat alias kept for existing dashboards
       → engineer role only
       → Works from both acknowledged and resolve_requested states

NOTE: The old POST /alerts/{alert_id}/acknowledge and /resolve that were
      defined in backend/app.py have been REMOVED from app.py and live here
      exclusively. This avoids route conflicts.
"""

import os
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from backend.auth import get_current_user
from backend.database import engine

router = APIRouter(tags=["Analytics"])

OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')

# ── Zone display maps ─────────────────────────────────────────────
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

SIGNAL_BODY = {
    'PDR_n': 'Sudden pressure drop detected — dispatch field team to inspect.',
    'FPI':   'Flow-pressure imbalance — probable pipe leakage in distribution network.',
    'NFA':   'Night flow anomaly — inspect for unauthorized extraction between 01:00–04:00.',
    'DDI':   'Demand deviation from expected pattern — check valve status and consumption.',
}

SCENARIO_SUFFIX = {
    'baseline': 'Anomaly',
    'leak':     'Leak Alert',
    'valve':    'Valve Alert',
    'surge':    'Surge Alert',
}


# ── Pydantic request bodies ───────────────────────────────────────

class AlertActionRequest(BaseModel):
    notes: Optional[str] = None


class ResolutionRequest(BaseModel):
    report: Optional[str] = None
    notes:  Optional[str] = None


# ══════════════════════════════════════════════════════════════════
#  GET /alerts/active
#  EXISTING — added optional ?status= query param. No breaking change.
#  Without ?status= → reads from v5_alerts.json (existing behavior)
#  With    ?status= → queries PostgreSQL alerts table by state
# ══════════════════════════════════════════════════════════════════

@router.get(
    "/alerts/active",
    summary="Active Alerts — with optional state filter (M1)",
    description=(
        "Returns V5 CLPS alerts for the requested scenario. "
        "Optional ?status= filter (new|acknowledged|resolve_requested|resolved) "
        "queries the PostgreSQL state machine instead of the JSON file. "
        "ward_officer sees only their assigned zone. "
        "Without ?status=, existing behavior is fully preserved."
    )
)
def get_active_alerts(
    scenario: str = Query(
        default='baseline',
        description="Scenario name: baseline | leak | valve | surge"
    ),
    status: Optional[str] = Query(
        default=None,
        description="State filter: new | acknowledged | resolve_requested | resolved"
    ),
    current_user: dict = Depends(get_current_user)
):
    role    = current_user.get('role', '')
    zone_id = current_user.get('zone_id')

    # ── Branch A: status filter provided → query PostgreSQL ──────────
    if status is not None:
        try:
            with engine.connect() as conn:
                base_query = """
                    SELECT alert_id, zone_id, clps, severity, dominant_signal,
                           probable_nodes, scenario, status,
                           acknowledged_at, acknowledged_by,
                           resolution_report, resolved_at,
                           rejected_count, notes, created_at
                    FROM alerts
                    WHERE status = :st
                """
                params = {'st': status}

                if role == 'ward_officer' and zone_id:
                    base_query += " AND zone_id = :zid"
                    params['zid'] = zone_id

                base_query += " ORDER BY clps DESC NULLS LAST"

                rows = conn.execute(text(base_query), params).fetchall()

            alerts_list = []
            for r in rows:
                zid      = str(r[1] or '')
                zm       = ZONE_MAP.get(zid, {
                    'nm':    zid.replace('_', ' ').title(),
                    'short': zid.replace('zone_', 'z'),
                })
                sig      = str(r[4] or 'PDR_n')
                clps_val = float(r[2] or 0)
                scen     = str(r[6] or 'baseline')

                alerts_list.append({
                    'db_alert_id':       r[0],
                    'zone_id':           zid,
                    'zone':              zm['nm'],
                    'zone_id_short':     zm['short'],
                    'clps':              round(clps_val, 3),
                    'severity':          str(r[3] or 'moderate'),
                    'dominant_signal':   sig,
                    'title':             f"{zm['nm']} · {sig} {SCENARIO_SUFFIX.get(scen, 'Alert')}",
                    'body':              SIGNAL_BODY.get(sig, f"Anomaly detected: {sig}."),
                    'level':             'HIGH' if clps_val > 0.5 else 'moderate',
                    'status':            str(r[7] or 'new'),
                    'acknowledged_at':   r[8].isoformat() if r[8] else None,
                    'acknowledged_by':   str(r[9]) if r[9] else None,
                    'resolution_report': str(r[10]) if r[10] else None,
                    'resolved_at':       r[11].isoformat() if r[11] else None,
                    'rejected_count':    int(r[12] or 0),
                    'notes':             str(r[13]) if r[13] else None,
                    'probable_nodes':    [],
                    'scenario':          scen,
                    'created_at':        r[14].isoformat() if r[14] else None,
                })

            return {
                'alerts':        alerts_list,
                'scenario':      scenario,
                'total':         len(alerts_list),
                'status_filter': status,
            }

        except Exception as e:
            return {
                'alerts':        [],
                'scenario':      scenario,
                'total':         0,
                'status_filter': status,
                'error':         f"DB query failed: {e}",
            }

    # ── Branch B: no status filter → existing JSON-file behavior ────
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

    raw = []
    if isinstance(data, dict):
        raw = data.get(scenario, data.get('baseline', []))
    elif isinstance(data, list):
        raw = data

    # Server-side zone filter for ward officers
    if role == 'ward_officer' and zone_id:
        raw = [a for a in raw if str(a.get('zone_id', '')) == str(zone_id)]

    # Look up db_alert_ids from PostgreSQL (for Ack/Resolve buttons)
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
        pass  # PostgreSQL not available — fallback to 0

    suffix = SCENARIO_SUFFIX.get(scenario, 'Alert')
    formatted = []
    for a in raw:
        zid  = str(a.get('zone_id', ''))
        zm   = ZONE_MAP.get(zid, {
            'nm':    zid.replace('_', ' ').title(),
            'short': zid.replace('zone_', 'z'),
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
            'status':          'new',  # file-based alerts default to 'new'
        })

    formatted.sort(key=lambda x: x['clps'], reverse=True)

    return {
        'alerts':   formatted,
        'scenario': scenario,
        'total':    len(formatted),
    }


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/acknowledge
#  Bible: "engineer → status = acknowledged, save acknowledged_by + acknowledged_at"
#  State: new | fired → acknowledged
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/acknowledge",
    summary="Acknowledge alert — engineer only (M1)"
)
def acknowledge_alert(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(
            status_code=403,
            detail="engineer role required to acknowledge alerts."
        )

    username = current_user.get('sub', 'engineer')

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status           = 'acknowledged',
                    acknowledged_at  = :ts,
                    acknowledged_by  = :by,
                    notes            = COALESCE(:notes, notes)
                WHERE alert_id = :id
                  AND status IN ('new', 'fired')
                RETURNING alert_id, zone_id, status
            """), {
                'ts':    datetime.utcnow(),
                'by':    username,
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

        if row:
            return {
                'success':  True,
                'alert_id': alert_id,
                'status':   'acknowledged',
                'acknowledged_by': username,
            }
        return {
            'success': False,
            'error':   'Alert not found or not in new/fired state.',
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/request-resolution
#  Bible: "field_operator → status = resolve_requested, save resolution_report"
#  State: acknowledged → resolve_requested
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/request-resolution",
    summary="Field operator files resolution report (M1)"
)
def request_resolution(
    alert_id: int,
    body: ResolutionRequest = ResolutionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role != 'field_operator':
        raise HTTPException(
            status_code=403,
            detail="field_operator role required to request resolution."
        )

    report_text = body.report or body.notes or ''

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status            = 'resolve_requested',
                    resolution_report = :report,
                    notes             = COALESCE(:notes, notes)
                WHERE alert_id = :id
                  AND status   = 'acknowledged'
                RETURNING alert_id, status
            """), {
                'report': report_text,
                'notes':  body.notes,
                'id':     alert_id,
            })
            row = result.fetchone()
            conn.commit()

        if row:
            return {
                'success':  True,
                'alert_id': alert_id,
                'status':   'resolve_requested',
            }
        return {
            'success': False,
            'error':   'Alert not found or not in acknowledged state.',
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/accept-resolution
#  Bible: "engineer → status = resolved, save resolved_at"
#  State: resolve_requested → resolved
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/accept-resolution",
    summary="Engineer accepts field resolution (M1)"
)
def accept_resolution(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(
            status_code=403,
            detail="engineer role required to accept resolution."
        )

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status      = 'resolved',
                    resolved_at = :ts,
                    notes       = COALESCE(:notes, notes)
                WHERE alert_id = :id
                  AND status   = 'resolve_requested'
                RETURNING alert_id, status
            """), {
                'ts':    datetime.utcnow(),
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

        if row:
            return {
                'success':  True,
                'alert_id': alert_id,
                'status':   'resolved',
            }
        return {
            'success': False,
            'error':   'Alert not found or not in resolve_requested state.',
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/reject-resolution
#  Bible: "engineer → status = acknowledged (sends back to field operator)"
#  State: resolve_requested → acknowledged   + rejected_count += 1
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/reject-resolution",
    summary="Engineer rejects field resolution — sends back (M1)"
)
def reject_resolution(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(
            status_code=403,
            detail="engineer role required to reject resolution."
        )

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status            = 'acknowledged',
                    resolution_report = NULL,
                    rejected_count    = COALESCE(rejected_count, 0) + 1,
                    notes             = COALESCE(:notes, notes)
                WHERE alert_id = :id
                  AND status   = 'resolve_requested'
                RETURNING alert_id, status, rejected_count
            """), {
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

        if row:
            return {
                'success':        True,
                'alert_id':       alert_id,
                'status':         'acknowledged',
                'rejected_count': int(row[2] or 0),
                'message':        (
                    'Resolution rejected. Alert returned to acknowledged state. '
                    'Field operator must re-submit when ready.'
                ),
            }
        return {
            'success': False,
            'error':   'Alert not found or not in resolve_requested state.',
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/resolve
#  BACKWARD COMPATIBILITY — kept so existing engineer_dashboard.html
#  and index.html continue to work without changes (Bible M1 constraint).
#  Maps to accept-resolution logic. Works from acknowledged OR resolve_requested.
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/resolve",
    summary="Resolve alert — backward-compat alias (engineer only)"
)
def resolve_alert_compat(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    """
    Backward compatibility shim.
    Old dashboards call POST /alerts/{id}/resolve directly.
    This endpoint accepts from both 'acknowledged' and 'resolve_requested' states
    so existing Ack → Resolve flows on old dashboard still work.
    """
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(
            status_code=403,
            detail="engineer role required."
        )

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status      = 'resolved',
                    resolved_at = :ts,
                    notes       = COALESCE(:notes, notes)
                WHERE alert_id = :id
                  AND status IN ('acknowledged', 'resolve_requested')
                RETURNING alert_id, status
            """), {
                'ts':    datetime.utcnow(),
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

        if row:
            return {'success': True, 'alert_id': alert_id, 'status': 'resolved'}
        return {
            'success': False,
            'error':   'Alert not found or not in resolvable state.',
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))