"""
Dhara — Complaint Management System
backend/routers/complaints.py

All complaint lifecycle endpoints for engineer dashboard + citizen app.

ENDPOINTS:
  GET  /complaints              — Active complaints list (engineer/ward)
  GET  /complaints/count        — Unacknowledged badge count
  GET  /complaints/audit        — Resolved complaints log
  GET  /complaints/{id}         — Full detail with photo
  POST /complaints/{id}/acknowledge  — open → acknowledged (engineer)
  POST /complaints/{id}/resolve      — acknowledged → resolved (engineer)
  POST /citizen/complaint/{id}/status      — Public: get status by ID
  POST /citizen/complaint/{id}/mark-fixed  — Public: citizen marks fixed
"""

import logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from backend.auth import get_current_user
from backend.database import engine

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Complaints"])


class AcknowledgeBody(BaseModel):
    notes: Optional[str] = None


class MarkFixedBody(BaseModel):
    complaint_id: str


# ─────────────────────────────────────────────────────────────
#  INTERNAL HELPER — build a clean complaint dict from a DB row
# ─────────────────────────────────────────────────────────────

def _row_to_dict(r, include_photo: bool = False) -> dict:
    zid = str(r[1] or "")
    d = {
        "complaint_id":    str(r[0]),
        "zone_id":         zid,
        "zone_name":       "Zone {}".format(zid.replace("zone_", "")),
        "problem_type":    str(r[2] or ""),
        "description":     str(r[3] or ""),
        "has_photo":       bool(r[4]),
        "status":          str(r[5] or "open"),
        "created_at":      r[6].isoformat() if r[6] else None,
        "updated_at":      r[7].isoformat() if r[7] else None,
        "acknowledged_by": str(r[8]) if len(r) > 8 and r[8] else None,
        "acknowledged_at": r[9].isoformat() if len(r) > 9 and r[9] else None,
    }
    if include_photo:
        d["photo_b64"] = str(r[4]) if r[4] else None
        d["has_photo"] = bool(r[4])
    return d


SELECT_COLS = """
    complaint_id, zone_id, problem_type, description,
    photo_b64, status, created_at, updated_at,
    acknowledged_by, acknowledged_at
"""


# ─────────────────────────────────────────────────────────────
#  GET /complaints/count
# ─────────────────────────────────────────────────────────────

@router.get("/complaints/count")
def get_complaint_count(current_user: dict = Depends(get_current_user)):
    """Badge count — number of open (unacknowledged) complaints."""
    role    = current_user.get("role", "")
    zone_id = current_user.get("zone_id")
    try:
        with engine.connect() as conn:
            if role == "ward_officer" and zone_id:
                n = conn.execute(
                    text("SELECT COUNT(*) FROM citizen_complaints WHERE zone_id = :z AND status IN ('open', 'not_resolved')"),
                    {"z": zone_id}
                ).scalar()
            else:
                n = conn.execute(
                    text("SELECT COUNT(*) FROM citizen_complaints WHERE status IN ('open', 'not_resolved')")
                ).scalar()
        return {"count": int(n or 0)}
    except Exception as exc:
        logger.warning("[complaints/count] %s", exc)
        return {"count": 0}


# ─────────────────────────────────────────────────────────────
#  GET /complaints/audit  — MUST be before /{complaint_id}
# ─────────────────────────────────────────────────────────────

@router.get("/complaints/audit")
def get_complaint_audit(current_user: dict = Depends(get_current_user)):
    """Resolved complaints — audit log, last 100 entries."""
    role    = current_user.get("role", "")
    zone_id = current_user.get("zone_id")
    try:
        with engine.connect() as conn:
            if role == "ward_officer" and zone_id:
                rows = conn.execute(text(f"""
                    SELECT {SELECT_COLS}
                    FROM citizen_complaints
                    WHERE zone_id = :z AND status = 'resolved'
                    ORDER BY updated_at DESC NULLS LAST LIMIT 100
                """), {"z": zone_id}).fetchall()
            else:
                rows = conn.execute(text(f"""
                    SELECT {SELECT_COLS}
                    FROM citizen_complaints
                    WHERE status = 'resolved'
                    ORDER BY updated_at DESC NULLS LAST LIMIT 100
                """)).fetchall()
        return {"complaints": [_row_to_dict(r) for r in rows], "total": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
#  GET /complaints  — active list
# ─────────────────────────────────────────────────────────────

@router.get("/complaints")
def get_complaints(current_user: dict = Depends(get_current_user)):
    """All non-resolved complaints. ward_officer: own zone only."""
    role    = current_user.get("role", "")
    zone_id = current_user.get("zone_id")
    try:
        with engine.connect() as conn:
            if role == "ward_officer" and zone_id:
                rows = conn.execute(text(f"""
                    SELECT {SELECT_COLS}
                    FROM citizen_complaints
                    WHERE zone_id = :z AND status NOT IN ('resolved', 'expired')
                    ORDER BY created_at DESC
                """), {"z": zone_id}).fetchall()
            else:
                rows = conn.execute(text(f"""
                    SELECT {SELECT_COLS}
                    FROM citizen_complaints
                    WHERE status NOT IN ('resolved', 'expired')
                    ORDER BY created_at DESC
                """)).fetchall()
        return {"complaints": [_row_to_dict(r) for r in rows], "total": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
#  GET /complaints/{id}
# ─────────────────────────────────────────────────────────────

@router.get("/complaints/{complaint_id}")
def get_complaint_detail(
    complaint_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Full complaint detail including photo_b64."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(f"""
                SELECT {SELECT_COLS} FROM citizen_complaints WHERE complaint_id = :id
            """), {"id": complaint_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Complaint not found")
        return _row_to_dict(row, include_photo=True)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
#  POST /complaints/{id}/acknowledge  — open → acknowledged
# ─────────────────────────────────────────────────────────────

@router.post("/complaints/{complaint_id}/acknowledge")
def acknowledge_complaint(
    complaint_id: str,
    body: AcknowledgeBody = AcknowledgeBody(),
    current_user: dict = Depends(get_current_user),
):
    """Engineer acknowledges a complaint. Transition: open → acknowledged."""
    role = current_user.get("role", "")
    if role not in ("engineer", "ward_officer", "commissioner"):
        raise HTTPException(status_code=403, detail="Not authorised.")
    username = current_user.get("sub", "staff")
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                UPDATE citizen_complaints
                SET    status          = 'acknowledged',
                       acknowledged_by = :by,
                       acknowledged_at = NOW(),
                       updated_at      = NOW()
                WHERE  complaint_id = :id AND status IN ('open', 'not_resolved')
                RETURNING complaint_id
            """), {"id": complaint_id, "by": username}).fetchone()
            conn.commit()
        if row:
            return {"success": True, "complaint_id": complaint_id,
                    "status": "acknowledged", "acknowledged_by": username}
        return {"success": False, "error": "Not found or already acknowledged."}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))



# ─────────────────────────────────────────────────────────────
#  PUBLIC: GET /citizen/complaint/{id}/status
#  (replaces the inline route in app.py — keep BOTH if the
#   inline one still exists, they will coexist harmlessly)
# ─────────────────────────────────────────────────────────────

@router.get("/citizen/complaint/{complaint_id}/status")
def get_citizen_complaint_status_v2(complaint_id: str):
    """Public — citizen polls their complaint status by ID."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT complaint_id, problem_type, status,
                           created_at, updated_at, acknowledged_by
                    FROM citizen_complaints WHERE complaint_id = :id
                """),
                {"id": complaint_id},
            ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Complaint not found")
        return {
            "complaint_id":    str(row[0]),
            "problem_type":    str(row[1] or ""),
            "status":          str(row[2] or "open"),
            "created_at":      row[3].isoformat() if row[3] else None,
            "updated_at":      row[4].isoformat() if row[4] else None,
            "acknowledged_by": str(row[5]) if row[5] else None,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
#  PUBLIC: POST /citizen/complaint/{id}/mark-fixed
# ─────────────────────────────────────────────────────────────

@router.post("/citizen/complaint/{complaint_id}/mark-fixed")
def citizen_mark_fixed(complaint_id: str):
    """
    Public — citizen marks their acknowledged complaint as resolved.
    Transition: acknowledged → resolved.
    No auth required (citizen-facing).
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                UPDATE citizen_complaints
                SET    status     = 'resolved',
                       updated_at = NOW()
                WHERE  complaint_id = :id AND status = 'acknowledged'
                RETURNING complaint_id
            """), {"id": complaint_id}).fetchone()
            conn.commit()
        if row:
            return {"success": True, "complaint_id": complaint_id, "status": "resolved"}
        return {"success": False,
                "error": "Complaint not found or not yet acknowledged by SMC."}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
#  PUBLIC: POST /citizen/complaint/{id}/not-resolved
#  Citizen disputes resolution. acknowledged → not_resolved.
#  This keeps the complaint ACTIVE (stays in My Complaints).
#  Engineer sees it immediately on next poll (15s) with red DISPUTED badge.
# ─────────────────────────────────────────────────────────────

@router.post("/citizen/complaint/{complaint_id}/not-resolved")
def citizen_mark_not_resolved(complaint_id: str):
    """
    Public — citizen disputes that their complaint was resolved.
    Transition: acknowledged → not_resolved
    Complaint remains active (not moved to history/archive).
    Engineer dashboard reflects this on next 15-second poll.
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                UPDATE citizen_complaints
                SET    status       = 'not_resolved',
                       disputed_at  = NOW(),
                       updated_at   = NOW()
                WHERE  complaint_id = :id
                  AND  status       = 'acknowledged'
                RETURNING complaint_id
            """), {"id": complaint_id}).fetchone()
            conn.commit()
        if row:
            return {
                "success":      True,
                "complaint_id": complaint_id,
                "status":       "not_resolved",
                "message":      "Your concern has been registered. SMC will review and re-acknowledge."
            }
        return {
            "success": False,
            "error":   "Complaint not found or not in acknowledged state."
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
#  PUBLIC: POST /citizen/complaints/expire-old
#  Called by citizen app on load and every 60 seconds.
#  Marks open complaints older than 24h as 'expired'.
#  Returns list of newly expired complaint IDs.
# ─────────────────────────────────────────────────────────────

@router.post("/citizen/complaints/expire-old")
def expire_old_complaints():
    """
    Public — marks all 'open' complaints older than 24 hours as 'expired'.
    Safe to call repeatedly (idempotent). Returns expired IDs for client-side
    history migration.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                UPDATE citizen_complaints
                SET    status           = 'expired',
                       updated_at       = NOW(),
                       expiry_notified  = TRUE
                WHERE  status      = 'open'
                  AND  created_at  < NOW() - INTERVAL '24 hours'
                RETURNING complaint_id
            """)).fetchall()
            conn.commit()
        expired_ids = [str(r[0]) for r in rows]
        return {"expired": expired_ids, "count": len(expired_ids)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
