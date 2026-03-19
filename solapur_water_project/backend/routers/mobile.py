"""
Hydro-Equity Engine / Dhara — Phase 4b M2 + M4
backend/routers/mobile.py

Mobile API Router for Field Operators.
All /mobile/* endpoints require Bearer token.
Only field_operator role can call most endpoints (403 for all other roles).
Zone filtering is server-side only — derived from JWT zone_id claim.

M4 CHANGE to /mobile/zone-status:
  - hei now sourced via data_provider.get_zone_status() instead of direct file read
  - active_alert_count now counts ONLY status='acknowledged' (was all active statuses)
  - Returns 0 with a logged warning if DB is unavailable (graceful fallback)
  - Response shape: {zone_id, hei, active_alert_count}

ENDPOINTS:
    GET  /mobile/profile
    GET  /mobile/alerts
    POST /mobile/alerts/{id}/start
    POST /mobile/alerts/{id}/resolve
    GET  /mobile/zone-status   ← updated in M4
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text

from backend.auth import get_current_user
from backend.database import engine
from backend import data_provider

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mobile", tags=["Mobile — Field Operator"])


# ── Pydantic request bodies ───────────────────────────────────────────────────

class StartWorkRequest(BaseModel):
    note: Optional[str] = None


class MobileResolveRequest(BaseModel):
    report: Optional[str] = None
    notes:  Optional[str] = None


# ── Internal helpers ──────────────────────────────────────────────────────────

def _require_field_operator(current_user: dict) -> dict:
    """Raises HTTP 403 if the caller is not a field_operator."""
    role = current_user.get("role", "")
    if role != "field_operator":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Access denied. This endpoint is for field_operator role only. "
                f"Your role: '{role}'."
            ),
        )
    return current_user


# ══════════════════════════════════════════════════════════════════════════════
#  GET /mobile/profile
#  Reads JWT only — no DB call. All authenticated roles allowed.
# ══════════════════════════════════════════════════════════════════════════════

@router.get(
    "/profile",
    summary="Field operator profile from JWT (no DB call)",
)
def get_mobile_profile(
    current_user: dict = Depends(get_current_user),
):
    return {
        "username":  current_user.get("sub", ""),
        "zone_id":   current_user.get("zone_id"),
        "role":      current_user.get("role", ""),
        "full_name": current_user.get("full_name", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  GET /mobile/alerts
#  field_operator role ONLY.
#  Returns alerts WHERE zone_id = JWT.zone_id AND status = "acknowledged".
# ══════════════════════════════════════════════════════════════════════════════

@router.get(
    "/alerts",
    summary="Acknowledged alerts for field operator's zone (field_operator only)",
)
def get_mobile_alerts(
    current_user: dict = Depends(get_current_user),
):
    _require_field_operator(current_user)

    zone_id = current_user.get("zone_id")
    if not zone_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Your account does not have a zone_id assigned. "
                "Contact the system administrator to assign a zone."
            ),
        )

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT alert_id, zone_id, clps, severity, dominant_signal,
                           probable_nodes, scenario, status,
                           acknowledged_at, acknowledged_by,
                           resolution_report, rejected_count,
                           notes, created_at
                    FROM alerts
                    WHERE zone_id = :zone_id
                      AND status  = 'acknowledged'
                    ORDER BY clps DESC NULLS LAST
                """),
                {"zone_id": zone_id},
            ).fetchall()

        alerts = []
        for r in rows:
            alerts.append({
                "alert_id":          r[0],
                "zone_id":           r[1],
                "clps":              round(float(r[2] or 0), 3),
                "severity":          str(r[3] or "moderate"),
                "dominant_signal":   str(r[4] or ""),
                "probable_nodes":    r[5] or "",
                "scenario":          str(r[6] or "baseline"),
                "status":            str(r[7] or "acknowledged"),
                "acknowledged_at":   r[8].isoformat()  if r[8]  else None,
                "acknowledged_by":   str(r[9])          if r[9]  else None,
                "resolution_report": str(r[10])          if r[10] else None,
                "rejected_count":    int(r[11] or 0),
                "notes":             str(r[12])          if r[12] else None,
                "created_at":        r[13].isoformat()  if r[13] else None,
            })

        return {
            "alerts":  alerts,
            "total":   len(alerts),
            "zone_id": zone_id,
            "filter":  "status=acknowledged",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error fetching mobile alerts: {e}",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  POST /mobile/alerts/{id}/start
#  field_operator role ONLY.
#  Appends "work started" note — NO state change.
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/start",
    summary="Mark work started on an alert (field_operator only, no state change)",
)
def start_alert_work(
    alert_id: int,
    body: StartWorkRequest = StartWorkRequest(),
    current_user: dict = Depends(get_current_user),
):
    _require_field_operator(current_user)

    zone_id  = current_user.get("zone_id")
    username = current_user.get("sub", "field_operator")
    ts       = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    base_note = f"work started by {username} at {ts}"
    if body.note:
        base_note = f"{base_note}. Field note: {body.note}"

    try:
        with engine.connect() as conn:
            existing = conn.execute(
                text("SELECT alert_id, zone_id, status FROM alerts WHERE alert_id = :aid"),
                {"aid": alert_id},
            ).fetchone()

            if not existing:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Alert {alert_id} not found.",
                )

            if zone_id and str(existing[1]) != str(zone_id):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"Alert {alert_id} belongs to zone '{existing[1]}', "
                        f"but your zone is '{zone_id}'."
                    ),
                )

            if str(existing[2]) != "acknowledged":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Alert {alert_id} is in state '{existing[2]}'. "
                        f"Can only mark 'work started' on acknowledged alerts."
                    ),
                )

            conn.execute(
                text("""
                    UPDATE alerts
                    SET notes = CASE
                        WHEN notes IS NULL OR notes = '' THEN :note
                        ELSE notes || ' | ' || :note
                    END
                    WHERE alert_id = :aid
                """),
                {"note": base_note, "aid": alert_id},
            )
            conn.commit()

        return {
            "success":    True,
            "alert_id":   alert_id,
            "note_added": base_note,
            "status":     "acknowledged",
            "message":    "Work started note recorded. Alert status unchanged.",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error on start: {e}",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  POST /mobile/alerts/{id}/resolve
#  field_operator role ONLY.
#  Calls M1 request-resolution: acknowledged → resolve_requested
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/resolve",
    summary="Field operator files resolution report (field_operator only)",
)
def mobile_resolve_alert(
    alert_id: int,
    body: MobileResolveRequest = MobileResolveRequest(),
    current_user: dict = Depends(get_current_user),
):
    _require_field_operator(current_user)

    zone_id     = current_user.get("zone_id")
    report_text = body.report or body.notes or ""

    try:
        with engine.connect() as conn:
            existing = conn.execute(
                text("SELECT alert_id, zone_id, status FROM alerts WHERE alert_id = :aid"),
                {"aid": alert_id},
            ).fetchone()

            if not existing:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Alert {alert_id} not found.",
                )

            if zone_id and str(existing[1]) != str(zone_id):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"Alert {alert_id} belongs to zone '{existing[1]}', "
                        f"but your zone is '{zone_id}'."
                    ),
                )

            if str(existing[2]) != "acknowledged":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Alert {alert_id} is in state '{existing[2]}'. "
                        f"Resolution can only be requested from 'acknowledged' state."
                    ),
                )

            result = conn.execute(
                text("""
                    UPDATE alerts
                    SET status            = 'resolve_requested',
                        resolution_report = :report
                    WHERE alert_id = :aid AND status = 'acknowledged'
                    RETURNING alert_id, status
                """),
                {"report": report_text, "aid": alert_id},
            )
            row = result.fetchone()
            conn.commit()

        if row:
            return {
                "success":  True,
                "alert_id": alert_id,
                "status":   "resolve_requested",
                "message":  "Resolution report submitted. Waiting for engineer to accept.",
            }
        return {"success": False, "error": "Alert not found or not in acknowledged state."}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error on mobile resolve: {e}",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  GET /mobile/zone-status   (M4 — updated)
#
#  Returns {zone_id, hei, active_alert_count}
#    hei               — from data_provider.get_zone_status()  (V4 output)
#    active_alert_count — COUNT alerts WHERE zone_id=jwt.zone_id
#                         AND status='acknowledged'
#    If DB unavailable  — active_alert_count = 0, warning logged
#    field_operator role ONLY
# ══════════════════════════════════════════════════════════════════════════════

@router.get(
    "/zone-status",
    summary="Field Operator — Zone HEI and acknowledged alert count (M4)",
    description=(
        "Returns {zone_id, hei, active_alert_count} for the field operator's zone. "
        "hei comes from data_provider.get_zone_status() (v4_zone_status.json). "
        "active_alert_count = alerts with status='acknowledged' for this zone. "
        "Returns active_alert_count=0 with a logged warning if DB is unavailable. "
        "field_operator role only."
    )
)
def get_zone_status(
    current_user: dict = Depends(get_current_user),
):
    """
    M4 spec:
      - field_operator role required
      - hei from data_provider.get_zone_status()
      - active_alert_count = COUNT WHERE zone_id=jwt.zone_id AND status='acknowledged'
      - graceful 0 + warning log if DB unavailable
      - response: {zone_id, hei, active_alert_count}
    """
    _require_field_operator(current_user)

    zone_id = current_user.get("zone_id")
    if not zone_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No zone_id assigned to this field operator account. "
                "Contact administrator to assign a zone."
            )
        )

    # ── Step 1: Get HEI from data_provider (V4 output file) ──────────
    all_zones = data_provider.get_zone_status()
    hei = 0.0
    for z in all_zones:
        if str(z.get('zone_id', '')) == str(zone_id):
            hei = float(z.get('hei', 0.0) or 0.0)
            break
    else:
        # zone not found in V4 output — V4 may not have been run yet
        logger.warning(
            "[mobile/zone-status] zone_id '%s' not found in v4_zone_status.json "
            "— returning hei=0.0. Run python scripts/v4_equity_minimal.py to populate.",
            zone_id
        )

    # ── Step 2: Get active_alert_count from PostgreSQL ────────────────
    # Only count status='acknowledged' (M4 spec)
    active_alert_count = 0
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT COUNT(*)
                    FROM   alerts
                    WHERE  zone_id = :zone_id
                      AND  status  = 'acknowledged'
                """),
                {"zone_id": zone_id}
            ).scalar()
            active_alert_count = int(result or 0)
    except Exception as exc:
        # DB unavailable — return 0 gracefully and log a warning (M4 spec)
        logger.warning(
            "[mobile/zone-status] DB unavailable, returning active_alert_count=0. "
            "Error: %s", exc
        )
        active_alert_count = 0

    # ── Step 3: Return M4 spec response shape ─────────────────────────
    return {
        "zone_id":            zone_id,
        "hei":                round(hei, 4),
        "active_alert_count": active_alert_count,
    }