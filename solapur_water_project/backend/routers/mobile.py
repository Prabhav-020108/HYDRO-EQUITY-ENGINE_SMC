"""
Hydro-Equity Engine / Dhara — Phase 4b M2
backend/routers/mobile.py

Mobile API Router for Field Operators.
All /mobile/* endpoints require Bearer token.
Only field_operator role can call /mobile/alerts (403 for all other roles).
Zone filtering is server-side only — derived from JWT zone_id claim.

Bible Reference: Section 3 M2 — "Create the /mobile/* API router for field operators"

ENDPOINTS:
    GET  /mobile/profile
         Reads JWT only (no DB call).
         Returns {username, zone_id, role}.
         All authenticated roles allowed (field_operator, engineer, etc.).

    GET  /mobile/alerts
         field_operator role ONLY — 403 for all other roles.
         Returns alerts WHERE zone_id = JWT.zone_id AND status = "acknowledged".
         Zone filtering is server-side from JWT (never trust client-supplied zone).

    POST /mobile/alerts/{id}/start
         field_operator role ONLY.
         Appends note "work started" to the alert notes column.
         No state change — status stays "acknowledged".
         Returns {success, alert_id, note_added}.

    POST /mobile/alerts/{id}/resolve
         field_operator role ONLY.
         Calls the M1 request-resolution logic:
           status: acknowledged → resolve_requested
           Saves resolution_report from request body.
         Returns {success, alert_id, status}.

    GET  /mobile/zone-status
         field_operator role ONLY.
         Returns {zone_id, hei, status, active_alert_count}.
         HEI is read from outputs/v4_zone_status.json (no DB call for HEI).
         active_alert_count = count of alerts WHERE zone_id = JWT.zone_id
                              AND status IN ('new', 'acknowledged', 'resolve_requested').
"""

import os
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text

from backend.auth import get_current_user
from backend.database import engine

router = APIRouter(prefix="/mobile", tags=["Mobile — Field Operator"])

# Path to outputs directory (two levels up from this file)
OUTPUTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "outputs")
)
V4_STATUS_PATH = os.path.join(OUTPUTS_DIR, "v4_zone_status.json")


# ── Pydantic request bodies ───────────────────────────────────────────────────

class StartWorkRequest(BaseModel):
    note: Optional[str] = None  # optional extra note; "work started" is always appended


class MobileResolveRequest(BaseModel):
    report: Optional[str] = None   # field operator's work notes / resolution report
    notes:  Optional[str] = None   # alias — accept either key for flexibility


# ── Internal helpers ──────────────────────────────────────────────────────────

def _require_field_operator(current_user: dict) -> dict:
    """
    Raises HTTP 403 if the caller is not a field_operator.
    Returns current_user dict on success.
    Called explicitly in each endpoint that is field_operator-only.
    """
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


def _get_zone_hei(zone_id: str) -> dict:
    """
    Reads v4_zone_status.json and returns the HEI record for the given zone_id.
    Returns a default dict if the file is missing or the zone is not found.
    """
    if not os.path.exists(V4_STATUS_PATH):
        return {"zone_id": zone_id, "hei": None, "status": "unknown"}
    try:
        with open(V4_STATUS_PATH, encoding="utf-8") as f:
            zones = json.load(f)
        if isinstance(zones, list):
            for z in zones:
                if str(z.get("zone_id", "")) == str(zone_id):
                    return {
                        "zone_id": zone_id,
                        "hei":     round(float(z.get("hei", 0) or 0), 4),
                        "status":  str(z.get("status", "unknown")),
                    }
    except Exception:
        pass
    return {"zone_id": zone_id, "hei": None, "status": "unknown"}


# ══════════════════════════════════════════════════════════════════════════════
#  GET /mobile/profile
#  Reads JWT only — no DB call.
#  All authenticated roles allowed.
# ══════════════════════════════════════════════════════════════════════════════

@router.get(
    "/profile",
    summary="Field operator profile from JWT (no DB call)",
    description=(
        "Returns {username, zone_id, role} decoded directly from the Bearer token. "
        "No database call is made. All authenticated roles may call this endpoint."
    ),
)
def get_mobile_profile(
    current_user: dict = Depends(get_current_user),
):
    """Returns the JWT payload fields relevant to the mobile app."""
    return {
        "username":  current_user.get("sub", ""),
        "zone_id":   current_user.get("zone_id"),
        "role":      current_user.get("role", ""),
        "full_name": current_user.get("full_name", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  GET /mobile/alerts
#  field_operator role ONLY — 403 for all other roles.
#  Returns alerts WHERE zone_id = JWT.zone_id AND status = "acknowledged".
#  Zone filtering is strictly server-side (derived from JWT).
# ══════════════════════════════════════════════════════════════════════════════

@router.get(
    "/alerts",
    summary="Acknowledged alerts for field operator's zone (field_operator only)",
    description=(
        "Returns all alerts in 'acknowledged' state for the zone_id in the JWT. "
        "Only field_operator role may call this endpoint (403 for engineer, ward_officer, etc.). "
        "Zone is derived server-side from the JWT — the client cannot supply or override it."
    ),
)
def get_mobile_alerts(
    current_user: dict = Depends(get_current_user),
):
    """
    Query: SELECT * FROM alerts WHERE zone_id = :jwt_zone AND status = 'acknowledged'
    Returns a list of alert dicts with fields needed by the field operator app.
    """
    _require_field_operator(current_user)

    zone_id = current_user.get("zone_id")
    if not zone_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Your account does not have a zone_id assigned. "
                "Contact the system administrator to assign a zone to your field_operator account."
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
                "acknowledged_at":   r[8].isoformat() if r[8] else None,
                "acknowledged_by":   str(r[9]) if r[9] else None,
                "resolution_report": str(r[10]) if r[10] else None,
                "rejected_count":    int(r[11] or 0),
                "notes":             str(r[12]) if r[12] else None,
                "created_at":        r[13].isoformat() if r[13] else None,
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
#  Appends "work started" to notes — NO state change.
#  Status stays "acknowledged".
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/start",
    summary="Mark work started on an alert (field_operator only, no state change)",
    description=(
        "Appends 'work started' note to the alert. "
        "Status is NOT changed — alert remains 'acknowledged'. "
        "field_operator role only. "
        "Bible reference: POST /mobile/alerts/{id}/start — adds note 'work started' — no state change."
    ),
)
def start_alert_work(
    alert_id: int,
    body: StartWorkRequest = StartWorkRequest(),
    current_user: dict = Depends(get_current_user),
):
    """
    Appends "work started [timestamp]" to the alert notes column.
    The alert must belong to the field operator's zone (server-side check).
    Status stays 'acknowledged' — this is a note-only operation.
    """
    _require_field_operator(current_user)

    zone_id  = current_user.get("zone_id")
    username = current_user.get("sub", "field_operator")
    ts       = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Build the note text
    base_note = f"work started by {username} at {ts}"
    if body.note:
        base_note = f"{base_note}. Field note: {body.note}"

    try:
        with engine.connect() as conn:
            # Verify the alert exists, belongs to this zone, and is in 'acknowledged' state
            existing = conn.execute(
                text("""
                    SELECT alert_id, zone_id, status
                    FROM alerts
                    WHERE alert_id = :aid
                """),
                {"aid": alert_id},
            ).fetchone()

            if not existing:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Alert {alert_id} not found.",
                )

            # Zone enforcement: field operator can only act on their own zone
            if zone_id and str(existing[1]) != str(zone_id):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"Alert {alert_id} belongs to zone '{existing[1]}', "
                        f"but your zone is '{zone_id}'. "
                        f"Zone-scoped access enforced server-side."
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

            # Append note — concatenate with any existing notes
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
            "status":     "acknowledged",   # unchanged
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
#  Calls M1 request-resolution logic:
#    state: acknowledged → resolve_requested
#    Saves resolution_report.
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/alerts/{alert_id}/resolve",
    summary="Field operator files resolution report (field_operator only)",
    description=(
        "Transitions alert from 'acknowledged' → 'resolve_requested'. "
        "Saves the field operator's resolution_report. "
        "This calls the same M1 request-resolution logic as POST /alerts/{id}/request-resolution. "
        "field_operator role only. Zone is verified server-side from JWT."
    ),
)
def mobile_resolve_alert(
    alert_id: int,
    body: MobileResolveRequest = MobileResolveRequest(),
    current_user: dict = Depends(get_current_user),
):
    """
    Bible M2: POST /mobile/alerts/{id}/resolve →
      calls the request-resolution logic from M1.
    Equivalent to POST /alerts/{id}/request-resolution but accessible
    from the /mobile/ prefix so the field operator app uses a consistent base URL.
    Zone is validated server-side.
    """
    _require_field_operator(current_user)

    zone_id      = current_user.get("zone_id")
    report_text  = body.report or body.notes or ""

    try:
        with engine.connect() as conn:
            # Verify alert exists and zone matches before acting
            existing = conn.execute(
                text("""
                    SELECT alert_id, zone_id, status
                    FROM alerts
                    WHERE alert_id = :aid
                """),
                {"aid": alert_id},
            ).fetchone()

            if not existing:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Alert {alert_id} not found.",
                )

            # Zone enforcement
            if zone_id and str(existing[1]) != str(zone_id):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"Alert {alert_id} belongs to zone '{existing[1]}', "
                        f"but your zone is '{zone_id}'. "
                        f"Zone-scoped access enforced server-side."
                    ),
                )

            if str(existing[2]) != "acknowledged":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Alert {alert_id} is in state '{existing[2]}'. "
                        f"Resolution can only be requested from 'acknowledged' state. "
                        f"Ask the engineer to acknowledge it first."
                    ),
                )

            # M1 request-resolution transition: acknowledged → resolve_requested
            result = conn.execute(
                text("""
                    UPDATE alerts
                    SET status            = 'resolve_requested',
                        resolution_report = :report
                    WHERE alert_id = :aid
                      AND status   = 'acknowledged'
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
                "message":  (
                    "Resolution report submitted. "
                    "Waiting for engineer to accept or reject."
                ),
            }

        # Should not reach here given the state check above, but be safe
        return {
            "success": False,
            "error":   "Alert not found or not in acknowledged state.",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error on mobile resolve: {e}",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  GET /mobile/zone-status
#  field_operator role ONLY.
#  Returns {zone_id, hei, hei_status, active_alert_count}.
#  HEI read from v4_zone_status.json (file read, no DB for HEI).
#  active_alert_count from DB: alerts in (new, acknowledged, resolve_requested).
# ══════════════════════════════════════════════════════════════════════════════

@router.get(
    "/zone-status",
    summary="Zone HEI and active alert count for field operator's zone (field_operator only)",
    description=(
        "Returns the zone HEI score (from v4_zone_status.json) and the count of active alerts "
        "(status IN new, acknowledged, resolve_requested) for the JWT zone_id. "
        "field_operator role only."
    ),
)
def get_mobile_zone_status(
    current_user: dict = Depends(get_current_user),
):
    """
    Returns:
        zone_id            — from JWT
        hei                — from v4_zone_status.json
        hei_status         — severe | moderate | equitable | over
        active_alert_count — count from DB: status IN ('new','acknowledged','resolve_requested')
    """
    _require_field_operator(current_user)

    zone_id = current_user.get("zone_id")
    if not zone_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Your field_operator account has no zone_id assigned. "
                "Ask the system administrator to set your zone."
            ),
        )

    # ── HEI from v4_zone_status.json (file read, not DB) ─────────────────────
    hei_data = _get_zone_hei(zone_id)

    # ── Active alert count from DB ────────────────────────────────────────────
    active_alert_count = 0
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT COUNT(*)
                    FROM alerts
                    WHERE zone_id = :zone_id
                      AND status  IN ('new', 'acknowledged', 'resolve_requested')
                """),
                {"zone_id": zone_id},
            ).fetchone()
            active_alert_count = int(result[0] or 0)
    except Exception as e:
        # Non-fatal — return 0 if DB unavailable
        active_alert_count = 0

    return {
        "zone_id":            zone_id,
        "hei":                hei_data.get("hei"),
        "hei_status":         hei_data.get("status", "unknown"),
        "active_alert_count": active_alert_count,
    }