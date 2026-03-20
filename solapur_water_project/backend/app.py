"""
Hydro-Equity Engine / Dhara — Phase 4b + M1 + M3 + N2
backend/routers/alerts.py

N2 CHANGES (all in this file):
  Task 1 — No caching in GET /alerts/active. Branch B reads v5_alerts.json
            via data_provider on every single request. No persistent dicts.
            (Confirmed: the existing code already had no persistent cache.
             No functional change needed for Task 1.)

  Task 2 — All 4 state-change POST endpoints now return the full updated
            alert object in their response, not just {success: true}.
            Return shape: {success: true, alert: {alert_id, zone_id, status,
            clps, dominant_signal, acknowledged_at, acknowledged_by,
            resolution_report, resolved_at, ...}}

  Task 3 — New GET /alerts/{id} single-alert endpoint added.
            Returns one alert by alert_id. All authenticated roles.
            ward_officer is restricted to their own zone_id only.

ROUTE ORDER (critical for FastAPI path matching):
  GET  /alerts/active        — literal path, registered FIRST
  GET  /alerts/{alert_id}    — path param, registered SECOND
  POST /alerts/{id}/acknowledge
  POST /alerts/{id}/request-resolution
  POST /alerts/{id}/accept-resolution
  POST /alerts/{id}/reject-resolution
  POST /alerts/{id}/resolve  (backward-compat alias)

ENDPOINTS:
  GET  /alerts/active?scenario=<baseline|leak|valve|surge>
                      &status=<new|acknowledged|resolve_requested|resolved>
  GET  /alerts/{alert_id}                 (NEW — N2 Task 3, all roles)
  POST /alerts/{id}/acknowledge           (engineer only)
  POST /alerts/{id}/request-resolution    (field_operator only)
  POST /alerts/{id}/accept-resolution     (engineer only)
  POST /alerts/{id}/reject-resolution     (engineer only)
  POST /alerts/{id}/resolve               (backward-compat alias, engineer only)
"""

import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from backend.auth import get_current_user
from backend.database import engine
from backend import data_provider

router = APIRouter(tags=["Analytics"])

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
#  INTERNAL HELPER — N2 Task 2 + Task 3
#  Fetches a single alert row from DB and returns it as a clean dict.
#  Used by all POST endpoints (Task 2) and the new GET endpoint (Task 3).
#  Called INSIDE an already-open connection so the conn is passed in.
# ══════════════════════════════════════════════════════════════════

def _fetch_alert_by_id(conn, alert_id: int) -> Optional[dict]:
    """
    Fetches one alert row by alert_id using an existing DB connection.
    Returns a dict with all relevant fields, or None if not found.

    Fields returned match the N2 spec exactly:
      alert_id, zone_id, status, clps, dominant_signal,
      acknowledged_at, acknowledged_by, resolution_report, resolved_at
    Plus useful extras: severity, notes, created_at.
    """
    row = conn.execute(text("""
        SELECT alert_id,
               zone_id,
               clps,
               severity,
               dominant_signal,
               status,
               acknowledged_at,
               acknowledged_by,
               resolution_report,
               resolved_at,
               notes,
               created_at
        FROM   alerts
        WHERE  alert_id = :aid
    """), {"aid": alert_id}).fetchone()

    if not row:
        return None

    return {
        "alert_id":          row[0],
        "zone_id":           str(row[1] or ""),
        "clps":              round(float(row[2] or 0), 3),
        "severity":          str(row[3] or "moderate"),
        "dominant_signal":   str(row[4] or ""),
        "status":            str(row[5] or "new"),
        "acknowledged_at":   row[6].isoformat()  if row[6]  else None,
        "acknowledged_by":   str(row[7])          if row[7]  else None,
        "resolution_report": str(row[8])          if row[8]  else None,
        "resolved_at":       row[9].isoformat()   if row[9]  else None,
        "notes":             str(row[10])          if row[10] else None,
        "created_at":        row[11].isoformat()  if row[11] else None,
    }


# ══════════════════════════════════════════════════════════════════
#  GET /alerts/active
#  REGISTERED FIRST — must come before GET /alerts/{alert_id}
#  so FastAPI does not try to parse "active" as an integer.
#
#  N2 Task 1: No caching. Branch B calls data_provider.get_alerts()
#  on every request — that function opens and reads v5_alerts.json
#  fresh each time. No module-level dict, no persistent state.
# ══════════════════════════════════════════════════════════════════

@router.get(
    "/alerts/active",
    summary="Active Alerts — with optional state filter (M1 + N2)",
    description=(
        "Returns V5 CLPS alerts for the requested scenario. "
        "Optional ?status= filter (new|acknowledged|resolve_requested|resolved) "
        "queries the PostgreSQL state machine. "
        "Without ?status=, reads v5_alerts.json fresh on every call (no caching). "
        "ward_officer sees only their assigned zone. "
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

    # ── Branch B: no status filter → read from v5_alerts.json via data_provider ──
    # N2 Task 1: data_provider.get_alerts() opens the file fresh on every call.
    # There is NO caching here — no module-level variable, no persistent dict.
    raw = data_provider.get_alerts(scenario)

    if raw is None:
        return {
            "alerts":   [],
            "scenario": scenario,
            "total":    0,
            "error":    "Run V5 first — v5_alerts.json not found in outputs/"
        }

    # Server-side zone filter for ward officers
    if role == 'ward_officer' and zone_id:
        raw = [a for a in raw if str(a.get('zone_id', '')) == str(zone_id)]

    # Look up db_alert_ids from PostgreSQL (for Ack/Resolve buttons in frontend)
    # This dict is built fresh per request — it is NOT a cache, it is request-scoped.
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
            'status':          'new',
        })

    formatted.sort(key=lambda x: x['clps'], reverse=True)

    return {
        'alerts':   formatted,
        'scenario': scenario,
        'total':    len(formatted),
    }


# ══════════════════════════════════════════════════════════════════
#  GET /alerts/{alert_id}   — N2 Task 3 (NEW)
#  REGISTERED SECOND — after /alerts/active so "active" is not
#  mistakenly parsed as an integer alert_id.
#
#  Returns a single alert by ID. All authenticated roles.
#  ward_officer: restricted to their own zone_id only.
# ══════════════════════════════════════════════════════════════════

@router.get(
    "/alerts/{alert_id}",
    summary="Get single alert by ID — all authenticated roles (N2 Task 3)"
)
def get_alert_by_id(
    alert_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Returns the full alert object for a given alert_id.
    All authenticated roles can access.
    ward_officer is restricted to their own zone_id only.

    Response: {"alert": {alert_id, zone_id, status, clps, dominant_signal,
                         acknowledged_at, acknowledged_by, resolution_report,
                         resolved_at, severity, notes, created_at}}
    """
    try:
        with engine.connect() as conn:
            alert = _fetch_alert_by_id(conn, alert_id)

        if not alert:
            raise HTTPException(
                status_code=404,
                detail=f"Alert {alert_id} not found."
            )

        # ward_officer: only show alerts from their own zone
        role         = current_user.get('role', '')
        user_zone_id = current_user.get('zone_id')
        if role == 'ward_officer' and user_zone_id:
            if alert['zone_id'] != str(user_zone_id):
                raise HTTPException(
                    status_code=403,
                    detail=(
                        f"Access denied. Alert {alert_id} belongs to zone "
                        f"'{alert['zone_id']}', but your assigned zone is '{user_zone_id}'."
                    )
                )

        return {"alert": alert}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error fetching alert {alert_id}: {e}"
        )


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/acknowledge   (engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/acknowledge",
    summary="Acknowledge alert — engineer only (M1 + N2)"
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
                SET status          = 'acknowledged',
                    acknowledged_at = :ts,
                    acknowledged_by = :by,
                    notes           = COALESCE(:notes, notes)
                WHERE alert_id = :id
                  AND status IN ('new', 'fired')
                RETURNING alert_id
            """), {
                'ts':    datetime.utcnow(),
                'by':    username,
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

            if not row:
                return {
                    'success': False,
                    'error':   'Alert not found or not in new/fired state.'
                }

            # N2 Task 2: fetch full updated alert object and return it
            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success':         True,
            'acknowledged_by': username,
            'alert':           alert,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/request-resolution   (field_operator only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/request-resolution",
    summary="Field operator files resolution report (M1 + N2)"
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
                RETURNING alert_id
            """), {
                'report': report_text,
                'notes':  body.notes,
                'id':     alert_id,
            })
            row = result.fetchone()
            conn.commit()

            if not row:
                return {
                    'success': False,
                    'error':   'Alert not found or not in acknowledged state.'
                }

            # N2 Task 2: fetch full updated alert object and return it
            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success': True,
            'alert':   alert,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/accept-resolution   (engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/accept-resolution",
    summary="Engineer accepts field resolution (M1 + N2)"
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
                RETURNING alert_id
            """), {
                'ts':    datetime.utcnow(),
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

            if not row:
                return {
                    'success': False,
                    'error':   'Alert not found or not in resolve_requested state.'
                }

            # N2 Task 2: fetch full updated alert object and return it
            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success': True,
            'alert':   alert,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/reject-resolution   (engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/reject-resolution",
    summary="Engineer rejects field resolution — sends back (M1 + N2)"
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
                RETURNING alert_id, rejected_count
            """), {
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

            if not row:
                return {
                    'success': False,
                    'error':   'Alert not found or not in resolve_requested state.'
                }

            rejected_count = int(row[1] or 0)

            # N2 Task 2: fetch full updated alert object and return it
            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success':        True,
            'rejected_count': rejected_count,
            'message': (
                'Resolution rejected. Alert returned to acknowledged state. '
                'Field operator must re-submit when ready.'
            ),
            'alert':          alert,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/resolve   (backward-compat alias, engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
#  Kept so existing engineer_dashboard.html and index.html
#  continue to work without any changes (M1 backward-compat rule).
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/resolve",
    summary="Resolve alert — backward-compat alias (engineer only, M1 + N2)"
)
def resolve_alert_compat(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
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
                RETURNING alert_id
            """), {
                'ts':    datetime.utcnow(),
                'notes': body.notes,
                'id':    alert_id,
            })
            row = result.fetchone()
            conn.commit()

            if not row:
                return {
                    'success': False,
                    'error':   'Alert not found or not in resolvable state.'
                }

            # N2 Task 2: fetch full updated alert object and return it
            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success': True,
            'alert':   alert,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))