"""
Hydro-Equity Engine — Phase 4b
backend/routers/citizen.py

Public citizen endpoints (no auth required):
  POST /citizen/complaint  → submit a complaint (no auth)
  GET  /citizen/zones      → city supply status summary (no auth)

IMPORTANT: These endpoints must never expose infrastructure data,
valve IDs, pipe coordinates, or any operational data.
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import text
from datetime import datetime

from backend.database import engine

router = APIRouter(prefix="/citizen", tags=["Citizen (Public)"])


# ── Request schema ─────────────────────────────────────────────────
class ComplaintRequest(BaseModel):
    zone_id:       str
    problem_type:  str       # No Water | Low Pressure | Dirty Water | Pipe Burst | Meter Issue | Other
    landmark:      Optional[str] = ""
    description:   Optional[str] = ""
    contact:       Optional[str] = ""   # optional phone/name

    class Config:
        json_schema_extra = {
            "example": {
                "zone_id":      "zone_1",
                "problem_type": "Low Pressure",
                "landmark":     "Near Gandhi Chowk",
                "description":  "No water since morning",
                "contact":      "9876543210"
            }
        }


# ── POST /citizen/complaint ─────────────────────────────────────────
@router.post(
    "/complaint",
    summary="Submit a citizen water supply complaint — PUBLIC",
    description=(
        "Submit a water supply complaint. No login required. "
        "The complaint is saved with an 'open' status and visible to "
        "engineers and ward officers in their dashboards. "
        "No infrastructure data is required or returned."
    )
)
def submit_complaint(req: ComplaintRequest):
    # Sanitize inputs — no infrastructure data should ever go in
    allowed_types = {
        'No Water', 'Low Pressure', 'Dirty Water',
        'Pipe Burst', 'Meter Issue', 'Billing Issue', 'Other'
    }
    ptype = req.problem_type if req.problem_type in allowed_types else 'Other'

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                INSERT INTO citizen_complaints
                    (zone_id, problem_type, landmark, description, contact, status)
                VALUES (:z, :pt, :lm, :desc, :ct, 'open')
                RETURNING complaint_id, created_at
            """), {
                'z':    req.zone_id,
                'pt':   ptype,
                'lm':   req.landmark or '',
                'desc': req.description or '',
                'ct':   req.contact or '',
            })
            row = result.fetchone()
            conn.commit()

        return {
            "success":      True,
            "complaint_id": row[0],
            "message":      (
                "Your complaint has been registered. "
                "The municipal team will review it within 24 hours. "
                f"Reference ID: {row[0]}"
            ),
            "status":       "open",
            "submitted_at": row[1].isoformat() if row[1] else datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {
            "success": False,
            "message": "Could not save complaint. Please try again.",
            "error":   str(e),
        }


# ── GET /citizen/zones ─────────────────────────────────────────────
@router.get(
    "/zones",
    summary="City zone supply status — PUBLIC",
    description=(
        "Returns a simple zone-level supply status summary. "
        "No infrastructure data, no technical details. "
        "Public endpoint — no authentication required."
    )
)
def get_zone_status():
    """Returns city-wide zone supply status from citizen_recs (plain language only)."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (zone_id)
                    zone_id, supply_status, advisory_text
                FROM citizen_recs
                ORDER BY zone_id, created_at DESC
            """)).fetchall()

            # Also get open complaint counts per zone
            complaint_rows = conn.execute(text("""
                SELECT zone_id, COUNT(*) as cnt
                FROM citizen_complaints
                WHERE status = 'open'
                GROUP BY zone_id
            """)).fetchall()

        complaint_map = {r[0]: int(r[1]) for r in complaint_rows}

        zones = []
        for r in rows:
            zones.append({
                "zone_id":          r[0],
                "zone_name":        f"Zone {(r[0] or '').replace('zone_', '')}",
                "supply_status":    r[1] or 'Unknown',
                "advisory":         r[2] or '',
                "open_complaints":  complaint_map.get(r[0], 0),
            })

        return {
            "zones":        zones,
            "total_zones":  len(zones),
            "last_updated": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {
            "zones": [],
            "error": f"Status data unavailable. Run V7 first. ({e})"
        }