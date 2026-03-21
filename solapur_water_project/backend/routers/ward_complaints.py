from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text
from backend.auth import get_current_user
from backend.database import engine

router = APIRouter(prefix="/ward/complaints", tags=["Ward Officer — Complaints"])

def _require_ward_officer(current_user: dict) -> dict:
    role = current_user.get("role", "")
    if role != "ward_officer":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied. Ward Officer role required.")
    return current_user

class ComplaintStatusUpdate(BaseModel):
    status: str

@router.get("")
def get_ward_complaints(current_user: dict = Depends(get_current_user)):
    _require_ward_officer(current_user)
    zone_id = current_user.get("zone_id")
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT complaint_id, zone_id, problem_type, photo_b64, description, contact, status, created_at, updated_at FROM citizen_complaints WHERE zone_id = :zone_id ORDER BY created_at DESC"),
                {"zone_id": zone_id}
            ).fetchall()
            
        complaints = []
        for r in rows:
            complaints.append({
                "complaint_id": str(r[0]),
                "zone_id": str(r[1]),
                "problem_type": str(r[2]),
                "photo_b64": str(r[3]) if r[3] else None,
                "description": str(r[4]) if r[4] else None,
                "contact": str(r[5]) if r[5] else None,
                "status": str(r[6]),
                "created_at": r[7].isoformat() if r[7] else None,
                "updated_at": r[8].isoformat() if r[8] else None,
            })
        return complaints
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{complaint_id}/status")
def update_complaint_status(complaint_id: str, body: ComplaintStatusUpdate, current_user: dict = Depends(get_current_user)):
    _require_ward_officer(current_user)
    if body.status not in ["acknowledged", "resolved"]:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Status must be acknowledged or resolved.")
        
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    UPDATE citizen_complaints 
                    SET status = :status, updated_at = NOW() 
                    WHERE complaint_id = :id 
                    RETURNING complaint_id, zone_id, problem_type, photo_b64, description, contact, status, created_at, updated_at
                """),
                {"status": body.status, "id": complaint_id}
            ).fetchone()
            conn.commit()
            
            if not row:
                raise HTTPException(status_code=404, detail="Complaint not found")
                
            updated_complaint = {
                "complaint_id": str(row[0]),
                "zone_id": str(row[1]),
                "problem_type": str(row[2]),
                "photo_b64": str(row[3]) if row[3] else None,
                "description": str(row[4]) if row[4] else None,
                "contact": str(row[5]) if row[5] else None,
                "status": str(row[6]),
                "created_at": row[7].isoformat() if row[7] else None,
                "updated_at": row[8].isoformat() if row[8] else None,
            }
            return updated_complaint
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/field-history")
def get_ward_field_history(current_user: dict = Depends(get_current_user)):
    _require_ward_officer(current_user)
    zone_id = current_user.get("zone_id")
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT alert_id, zone_id, dominant_signal, status, 
                           resolved_at, resolution_report, resolved_by
                    FROM alerts 
                    WHERE zone_id = :zone_id 
                    AND resolved_by IS NOT NULL
                    ORDER BY resolved_at DESC NULLS LAST 
                    LIMIT 20
                """),
                {"zone_id": zone_id}
            ).fetchall()
            
        history = []
        for r in rows:
            history.append({
                "alert_id": r[0],
                "zone_id": str(r[1]),
                "dominant_signal": str(r[2] or ""),
                "status": str(r[3]),
                "resolved_at": r[4].isoformat() if r[4] else None,
                "resolution_report": str(r[5]) if r[5] else None,
                "resolved_by": str(r[6]) if r[6] else None
            })
        return history
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
