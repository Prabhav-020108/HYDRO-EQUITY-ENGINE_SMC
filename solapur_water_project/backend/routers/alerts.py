# """
# Hydro-Equity Engine / Dhara — Phase 4b + M1 + M3
# backend/routers/alerts.py

# M3 CHANGE: Branch B of get_active_alerts() now reads v5_alerts.json
#            via data_provider.get_alerts() instead of opening the file directly.
#            All other logic is identical to M1.

# ENDPOINTS IN THIS FILE:
#   GET  /alerts/active?scenario=<baseline|leak|valve|surge>
#                       &status=<new|acknowledged|resolve_requested|resolved>
#   POST /alerts/{id}/acknowledge          (engineer only)
#   POST /alerts/{id}/request-resolution   (field_operator only)
#   POST /alerts/{id}/accept-resolution    (engineer only)
#   POST /alerts/{id}/reject-resolution    (engineer only)
#   POST /alerts/{id}/resolve              (backward-compat alias, engineer only)
# """

# import os
# from datetime import datetime
# from typing import Optional

# from fastapi import APIRouter, Depends, Query, HTTPException
# from pydantic import BaseModel
# from sqlalchemy import text

# from backend.auth import get_current_user
# from backend.database import engine
# from backend import data_provider

# router = APIRouter(tags=["Analytics"])

# # ── Zone display maps ─────────────────────────────────────────────
# ZONE_MAP = {
#     'zone_1': {'nm': 'Zone 1', 'short': 'z1'},
#     'zone_2': {'nm': 'Zone 2', 'short': 'z2'},
#     'zone_3': {'nm': 'Zone 3', 'short': 'z3'},
#     'zone_4': {'nm': 'Zone 4', 'short': 'z4'},
#     'zone_5': {'nm': 'Zone 5', 'short': 'z5'},
#     'zone_6': {'nm': 'Zone 6', 'short': 'z6'},
#     'zone_7': {'nm': 'Zone 7', 'short': 'z7'},
#     'zone_8': {'nm': 'Zone 8', 'short': 'z8'},
# }

# SIGNAL_BODY = {
#     'PDR_n': 'Sudden pressure drop detected — dispatch field team to inspect.',
#     'FPI':   'Flow-pressure imbalance — probable pipe leakage in distribution network.',
#     'NFA':   'Night flow anomaly — inspect for unauthorized extraction between 01:00–04:00.',
#     'DDI':   'Demand deviation from expected pattern — check valve status and consumption.',
# }

# SCENARIO_SUFFIX = {
#     'baseline': 'Anomaly',
#     'leak':     'Leak Alert',
#     'valve':    'Valve Alert',
#     'surge':    'Surge Alert',
# }


# # ── Pydantic request bodies ───────────────────────────────────────

# class AlertActionRequest(BaseModel):
#     notes: Optional[str] = None


# class ResolutionRequest(BaseModel):
#     report: Optional[str] = None
#     notes:  Optional[str] = None


# # ══════════════════════════════════════════════════════════════════
# #  GET /alerts/active
# # ══════════════════════════════════════════════════════════════════

# @router.get(
#     "/alerts/active",
#     summary="Active Alerts — with optional state filter (M1)",
# )
# def get_active_alerts(
#     scenario: str = Query(default='baseline'),
#     status: Optional[str] = Query(default=None),
#     current_user: dict = Depends(get_current_user)
# ):
#     role    = current_user.get('role', '')
#     zone_id = current_user.get('zone_id')

#     # ── Branch A: status filter provided → query PostgreSQL ──────────
#     if status is not None:
#         try:
#             with engine.connect() as conn:
#                 base_query = """
#                     SELECT alert_id, zone_id, clps, severity, dominant_signal,
#                            probable_nodes, scenario, status,
#                            acknowledged_at, acknowledged_by,
#                            resolution_report, resolved_at,
#                            rejected_count, notes, created_at
#                     FROM alerts
#                     WHERE status = :st
#                 """
#                 params = {'st': status}

#                 if role == 'ward_officer' and zone_id:
#                     base_query += " AND zone_id = :zid"
#                     params['zid'] = zone_id

#                 base_query += " ORDER BY clps DESC NULLS LAST"

#                 rows = conn.execute(text(base_query), params).fetchall()

#             alerts_list = []
#             for r in rows:
#                 zid      = str(r[1] or '')
#                 zm       = ZONE_MAP.get(zid, {
#                     'nm':    zid.replace('_', ' ').title(),
#                     'short': zid.replace('zone_', 'z'),
#                 })
#                 sig      = str(r[4] or 'PDR_n')
#                 clps_val = float(r[2] or 0)
#                 scen     = str(r[6] or 'baseline')

#                 alerts_list.append({
#                     'db_alert_id':       r[0],
#                     'zone_id':           zid,
#                     'zone':              zm['nm'],
#                     'zone_id_short':     zm['short'],
#                     'clps':              round(clps_val, 3),
#                     'severity':          str(r[3] or 'moderate'),
#                     'dominant_signal':   sig,
#                     'title':             f"{zm['nm']} · {sig} {SCENARIO_SUFFIX.get(scen, 'Alert')}",
#                     'body':              SIGNAL_BODY.get(sig, f"Anomaly detected: {sig}."),
#                     'level':             'HIGH' if clps_val > 0.5 else 'moderate',
#                     'status':            str(r[7] or 'new'),
#                     'acknowledged_at':   r[8].isoformat() if r[8] else None,
#                     'acknowledged_by':   str(r[9]) if r[9] else None,
#                     'resolution_report': str(r[10]) if r[10] else None,
#                     'resolved_at':       r[11].isoformat() if r[11] else None,
#                     'rejected_count':    int(r[12] or 0),
#                     'notes':             str(r[13]) if r[13] else None,
#                     'probable_nodes':    [],
#                     'scenario':          scen,
#                     'created_at':        r[14].isoformat() if r[14] else None,
#                 })

#             return {
#                 'alerts':        alerts_list,
#                 'scenario':      scenario,
#                 'total':         len(alerts_list),
#                 'status_filter': status,
#             }

#         except Exception as e:
#             return {
#                 'alerts':        [],
#                 'scenario':      scenario,
#                 'total':         0,
#                 'status_filter': status,
#                 'error':         f"DB query failed: {e}",
#             }

#     # ── Branch B: no status filter → read from v5_alerts.json via data_provider ──
#     # M3 CHANGE: replaced direct open() + json.load() with data_provider.get_alerts()
#     raw = data_provider.get_alerts(scenario)

#     if raw is None:
#         return {
#             "alerts":   [],
#             "scenario": scenario,
#             "total":    0,
#             "error":    "Run V5 first — v5_alerts.json not found in outputs/"
#         }

#     # Server-side zone filter for ward officers
#     if role == 'ward_officer' and zone_id:
#         raw = [a for a in raw if str(a.get('zone_id', '')) == str(zone_id)]

#     # Look up db_alert_ids from PostgreSQL (for Ack/Resolve buttons)
#     db_ids: dict = {}
#     try:
#         with engine.connect() as conn:
#             rows = conn.execute(
#                 text("SELECT alert_id, zone_id FROM alerts WHERE scenario = :scen"),
#                 {'scen': scenario}
#             ).fetchall()
#             for row in rows:
#                 db_ids[row[1]] = row[0]
#     except Exception:
#         pass  # PostgreSQL not available — fallback to 0

#     suffix = SCENARIO_SUFFIX.get(scenario, 'Alert')
#     formatted = []
#     for a in raw:
#         zid  = str(a.get('zone_id', ''))
#         zm   = ZONE_MAP.get(zid, {
#             'nm':    zid.replace('_', ' ').title(),
#             'short': zid.replace('zone_', 'z'),
#         })
#         sig  = str(a.get('dominant_signal', 'PDR_n'))
#         lvl  = str(a.get('severity', 'moderate') or 'moderate')
#         clps = float(a.get('clps', 0) or 0)

#         formatted.append({
#             'title':           f"{zm['nm']} · {sig} {suffix}",
#             'body':            SIGNAL_BODY.get(sig, f"Anomaly detected: dominant signal {sig}."),
#             'level':           lvl,
#             'zone':            zm['nm'],
#             'zone_id_short':   zm['short'],
#             'zone_id':         zid,
#             'clps':            round(clps, 3),
#             'dominant_signal': sig,
#             'probable_nodes':  a.get('probable_node_ids', []),
#             'db_alert_id':     db_ids.get(zid, 0),
#             'status':          'new',
#         })

#     formatted.sort(key=lambda x: x['clps'], reverse=True)

#     return {
#         'alerts':   formatted,
#         'scenario': scenario,
#         'total':    len(formatted),
#     }


# # ══════════════════════════════════════════════════════════════════
# #  POST /alerts/{id}/acknowledge   (engineer only)
# # ══════════════════════════════════════════════════════════════════

# @router.post("/alerts/{alert_id}/acknowledge", summary="Acknowledge alert — engineer only (M1)")
# def acknowledge_alert(
#     alert_id: int,
#     body: AlertActionRequest = AlertActionRequest(),
#     current_user: dict = Depends(get_current_user)
# ):
#     role = current_user.get('role', '')
#     if role not in ('engineer',):
#         raise HTTPException(status_code=403, detail="engineer role required to acknowledge alerts.")

#     username = current_user.get('sub', 'engineer')

#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status           = 'acknowledged',
#                     acknowledged_at  = :ts,
#                     acknowledged_by  = :by,
#                     notes            = COALESCE(:notes, notes)
#                 WHERE alert_id = :id
#                   AND status IN ('new', 'fired')
#                 RETURNING alert_id, zone_id, status
#             """), {'ts': datetime.utcnow(), 'by': username, 'notes': body.notes, 'id': alert_id})
#             row = result.fetchone()
#             conn.commit()

#         if row:
#             return {'success': True, 'alert_id': alert_id, 'status': 'acknowledged',
#                     'acknowledged_by': username}
#         return {'success': False, 'error': 'Alert not found or not in new/fired state.'}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # ══════════════════════════════════════════════════════════════════
# #  POST /alerts/{id}/request-resolution   (field_operator only)
# # ══════════════════════════════════════════════════════════════════

# @router.post("/alerts/{alert_id}/request-resolution",
#              summary="Field operator files resolution report (M1)")
# def request_resolution(
#     alert_id: int,
#     body: ResolutionRequest = ResolutionRequest(),
#     current_user: dict = Depends(get_current_user)
# ):
#     role = current_user.get('role', '')
#     if role != 'field_operator':
#         raise HTTPException(status_code=403, detail="field_operator role required.")

#     report_text = body.report or body.notes or ''

#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status            = 'resolve_requested',
#                     resolution_report = :report,
#                     notes             = COALESCE(:notes, notes)
#                 WHERE alert_id = :id AND status = 'acknowledged'
#                 RETURNING alert_id, status
#             """), {'report': report_text, 'notes': body.notes, 'id': alert_id})
#             row = result.fetchone()
#             conn.commit()

#         if row:
#             return {'success': True, 'alert_id': alert_id, 'status': 'resolve_requested'}
#         return {'success': False, 'error': 'Alert not found or not in acknowledged state.'}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # ══════════════════════════════════════════════════════════════════
# #  POST /alerts/{id}/accept-resolution   (engineer only)
# # ══════════════════════════════════════════════════════════════════

# @router.post("/alerts/{alert_id}/accept-resolution",
#              summary="Engineer accepts field resolution (M1)")
# def accept_resolution(
#     alert_id: int,
#     body: AlertActionRequest = AlertActionRequest(),
#     current_user: dict = Depends(get_current_user)
# ):
#     role = current_user.get('role', '')
#     if role not in ('engineer',):
#         raise HTTPException(status_code=403, detail="engineer role required.")

#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status      = 'resolved',
#                     resolved_at = :ts,
#                     notes       = COALESCE(:notes, notes)
#                 WHERE alert_id = :id AND status = 'resolve_requested'
#                 RETURNING alert_id, status
#             """), {'ts': datetime.utcnow(), 'notes': body.notes, 'id': alert_id})
#             row = result.fetchone()
#             conn.commit()

#         if row:
#             return {'success': True, 'alert_id': alert_id, 'status': 'resolved'}
#         return {'success': False, 'error': 'Alert not found or not in resolve_requested state.'}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # ══════════════════════════════════════════════════════════════════
# #  POST /alerts/{id}/reject-resolution   (engineer only)
# # ══════════════════════════════════════════════════════════════════

# @router.post("/alerts/{alert_id}/reject-resolution",
#              summary="Engineer rejects field resolution — sends back (M1)")
# def reject_resolution(
#     alert_id: int,
#     body: AlertActionRequest = AlertActionRequest(),
#     current_user: dict = Depends(get_current_user)
# ):
#     role = current_user.get('role', '')
#     if role not in ('engineer',):
#         raise HTTPException(status_code=403, detail="engineer role required.")

#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status            = 'acknowledged',
#                     resolution_report = NULL,
#                     rejected_count    = COALESCE(rejected_count, 0) + 1,
#                     notes             = COALESCE(:notes, notes)
#                 WHERE alert_id = :id AND status = 'resolve_requested'
#                 RETURNING alert_id, status, rejected_count
#             """), {'notes': body.notes, 'id': alert_id})
#             row = result.fetchone()
#             conn.commit()

#         if row:
#             return {'success': True, 'alert_id': alert_id, 'status': 'acknowledged',
#                     'rejected_count': int(row[2] or 0),
#                     'message': 'Resolution rejected. Alert returned to acknowledged state.'}
#         return {'success': False, 'error': 'Alert not found or not in resolve_requested state.'}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # ══════════════════════════════════════════════════════════════════
# #  POST /alerts/{id}/resolve   (backward-compat alias, engineer only)
# # ══════════════════════════════════════════════════════════════════

# @router.post("/alerts/{alert_id}/resolve",
#              summary="Resolve alert — backward-compat alias (engineer only)")
# def resolve_alert_compat(
#     alert_id: int,
#     body: AlertActionRequest = AlertActionRequest(),
#     current_user: dict = Depends(get_current_user)
# ):
#     role = current_user.get('role', '')
#     if role not in ('engineer',):
#         raise HTTPException(status_code=403, detail="engineer role required.")

#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status      = 'resolved',
#                     resolved_at = :ts,
#                     notes       = COALESCE(:notes, notes)
#                 WHERE alert_id = :id
#                   AND status IN ('acknowledged', 'resolve_requested')
#                 RETURNING alert_id, status
#             """), {'ts': datetime.utcnow(), 'notes': body.notes, 'id': alert_id})
#             row = result.fetchone()
#             conn.commit()

#         if row:
#             return {'success': True, 'alert_id': alert_id, 'status': 'resolved'}
#         return {'success': False, 'error': 'Alert not found or not in resolvable state.'}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))





"""
Hydro-Equity Engine / Dhara — Phase 4b + M1 + M3 + N2
backend/routers/alerts.py

N2 CHANGES (Person A deliverables):
  Task 1 — No caching in GET /alerts/active. Branch B reads v5_alerts.json
            via data_provider on every single request. No persistent dicts.

  Task 2 — All 4 state-change POST endpoints now return the full updated
            alert object in their response.
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
               created_at,
               resolved_by
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
        "resolved_by":       str(row[12])         if row[12] else None,
    }


# ══════════════════════════════════════════════════════════════════
#  GET /alerts/active
#  REGISTERED FIRST — must come before GET /alerts/{alert_id}
#
#  N2 Task 1: No caching. Branch B calls data_provider.get_alerts()
#  on every request — that function opens and reads v5_alerts.json
#  fresh each time. No module-level dict, no persistent state.
# ══════════════════════════════════════════════════════════════════

@router.get(
    "/alerts/active",
    summary="Active Alerts — with optional state filter (M1 + N2)",
)
def get_active_alerts(
    scenario: str = Query(default='baseline'),
    status: Optional[str] = Query(default=None),
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
                           rejected_count, notes, created_at, resolved_by
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
                    'resolved_by':       str(r[15]) if r[15] else None,
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
    # There is NO caching — no module-level variable, no persistent dict.
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

    # Look up db_alert_ids from PostgreSQL (for Ack/Resolve buttons)
    db_ids: dict = {}
    db_statuses: dict = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT alert_id, zone_id, status,
                           acknowledged_by, resolution_report,
                           acknowledged_at, resolved_at, resolution_photo, resolved_by
                    FROM alerts
                    WHERE scenario = :scen
                """),
                {'scen': scenario}
            ).fetchall()
            for row in rows:
                db_ids[row[1]] = row[0]
                db_statuses[row[1]] = {
                    'status':            str(row[2] or 'new'),
                    'acknowledged_by':   str(row[3]) if row[3] else None,
                    'resolution_report': str(row[4]) if row[4] else None,
                    'acknowledged_at':   row[5].isoformat() if row[5] else None,
                    'resolved_at':       row[6].isoformat() if row[6] else None,
                    'resolution_photo':  str(row[7]) if row[7] else None,
                    'resolved_by':       str(row[8]) if row[8] else None,
                }
    except Exception:
        pass  # PostgreSQL not available — fallback to defaults

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

        # Merge real lifecycle status from DB (N2: no more hardcoded 'new')
        db_info = db_statuses.get(zid, {})

        formatted.append({
            'title':             f"{zm['nm']} · {sig} {suffix}",
            'body':              SIGNAL_BODY.get(sig, f"Anomaly detected: dominant signal {sig}."),
            'level':             lvl,
            'zone':              zm['nm'],
            'zone_id_short':     zm['short'],
            'zone_id':           zid,
            'clps':              round(clps, 3),
            'dominant_signal':   sig,
            'probable_nodes':    a.get('probable_node_ids', []),
            'db_alert_id':       db_ids.get(zid, 0),
            # N2: status comes from DB, not hardcoded 'new'
            'status':            db_info.get('status', 'new'),
            'acknowledged_by':   db_info.get('acknowledged_by'),
            'resolution_report': db_info.get('resolution_report'),
            'acknowledged_at':   db_info.get('acknowledged_at'),
            'resolved_at':       db_info.get('resolved_at'),
            'resolution_photo':  db_info.get('resolution_photo'),
            'resolved_by':       db_info.get('resolved_by'),
        })

    formatted.sort(key=lambda x: x['clps'], reverse=True)

    return {
        'alerts':   formatted,
        'scenario': scenario,
        'total':    len(formatted),
    }


# ══════════════════════════════════════════════════════════════════
#  GET /alerts/{alert_id}   — N2 Task 3 (NEW)
#  REGISTERED SECOND — after /alerts/active
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
    All authenticated roles. ward_officer restricted to own zone.
    """
    try:
        with engine.connect() as conn:
            alert = _fetch_alert_by_id(conn, alert_id)

        if not alert:
            raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found.")

        role         = current_user.get('role', '')
        user_zone_id = current_user.get('zone_id')
        if role == 'ward_officer' and user_zone_id:
            if alert['zone_id'] != str(user_zone_id):
                raise HTTPException(
                    status_code=403,
                    detail=(
                        f"Access denied. Alert {alert_id} belongs to zone "
                        f"'{alert['zone_id']}', but your zone is '{user_zone_id}'."
                    )
                )

        return {"alert": alert}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error fetching alert {alert_id}: {e}")


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/acknowledge   (engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post("/alerts/{alert_id}/acknowledge",
             summary="Acknowledge alert — engineer only (M1 + N2)")
def acknowledge_alert(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(status_code=403, detail="engineer role required to acknowledge alerts.")

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
                return {'success': False, 'error': 'Alert not found or not in new/fired state.'}

            # N2 Task 2: fetch full updated alert object
            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success':         True,
            'status':          'acknowledged',
            'acknowledged_by': username,
            'alert':           alert,   # ← N2: full object for frontend card update
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/request-resolution   (field_operator only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post("/alerts/{alert_id}/request-resolution",
             summary="Field operator files resolution report (M1 + N2)")
def request_resolution(
    alert_id: int,
    body: ResolutionRequest = ResolutionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role != 'field_operator':
        raise HTTPException(status_code=403, detail="field_operator role required.")

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
            """), {'report': report_text, 'notes': body.notes, 'id': alert_id})
            row = result.fetchone()
            conn.commit()

            if not row:
                return {'success': False, 'error': 'Alert not found or not in acknowledged state.'}

            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success': True,
            'status':  'resolve_requested',
            'alert':   alert,   # ← N2: full object
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/accept-resolution   (engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post("/alerts/{alert_id}/accept-resolution",
             summary="Engineer accepts field resolution (M1 + N2)")
def accept_resolution(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(status_code=403, detail="engineer role required.")

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
            """), {'ts': datetime.utcnow(), 'notes': body.notes, 'id': alert_id})
            row = result.fetchone()
            conn.commit()

            if not row:
                return {'success': False, 'error': 'Alert not found or not in resolve_requested state.'}

            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success': True,
            'status':  'resolved',
            'alert':   alert,   # ← N2: full object
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/reject-resolution   (engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
# ══════════════════════════════════════════════════════════════════

@router.post("/alerts/{alert_id}/reject-resolution",
             summary="Engineer rejects field resolution — sends back (M1 + N2)")
def reject_resolution(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(status_code=403, detail="engineer role required.")

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
            """), {'notes': body.notes, 'id': alert_id})
            row = result.fetchone()
            conn.commit()

            if not row:
                return {'success': False, 'error': 'Alert not found or not in resolve_requested state.'}

            rejected_count = int(row[1] or 0)
            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success':        True,
            'status':         'acknowledged',
            'rejected_count': rejected_count,
            'message': 'Resolution rejected. Alert returned to acknowledged state.',
            'alert':          alert,   # ← N2: full object
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  POST /alerts/{id}/resolve   (backward-compat alias, engineer only)
#  N2 Task 2: Returns full updated alert object after state change.
#  Kept so existing dashboards that call /resolve continue to work.
# ══════════════════════════════════════════════════════════════════

@router.post("/alerts/{alert_id}/resolve",
             summary="Resolve alert — backward-compat alias (engineer only, M1 + N2)")
def resolve_alert_compat(
    alert_id: int,
    body: AlertActionRequest = AlertActionRequest(),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer',):
        raise HTTPException(status_code=403, detail="engineer role required.")

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
            """), {'ts': datetime.utcnow(), 'notes': body.notes, 'id': alert_id})
            row = result.fetchone()
            conn.commit()

            if not row:
                return {'success': False, 'error': 'Alert not found or not in resolvable state.'}

            alert = _fetch_alert_by_id(conn, alert_id)

        return {
            'success': True,
            'status':  'resolved',
            'alert':   alert,   # ← N2: full object
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))