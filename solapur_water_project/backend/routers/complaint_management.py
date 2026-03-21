from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from datetime import datetime

from backend.auth import get_current_user
from backend.database import engine

router = APIRouter()

@router.get("/engineer/complaints", tags=["Engineer — Complaint Management"])
def get_engineer_complaints(
    zone_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role not in ('engineer', 'commissioner'):
        raise HTTPException(status_code=403, detail="engineer role required.")
    
    try:
        with engine.connect() as conn:
            # Build query
            base_query = """
                SELECT complaint_id, zone_id, problem_type, description,
                       contact, status, photo_b64, created_at, updated_at,
                       acknowledged_at, acknowledged_by
                FROM citizen_complaints
                WHERE 1=1
            """
            params = {}
            if zone_id:
                base_query += " AND zone_id = :zid"
                params['zid'] = zone_id
            if status:
                base_query += " AND status = :st"
                params['st'] = status
            
            base_query += " ORDER BY created_at DESC"
            
            rows = conn.execute(text(base_query), params).fetchall()
            
            complaints = []
            unread_count = 0
            for r in rows:
                c_status = str(r[5] or 'open')
                if c_status == 'open':
                    unread_count += 1
                
                complaints.append({
                    "complaint_id": r[0],
                    "zone_id": str(r[1] or ""),
                    "problem_type": str(r[2] or ""),
                    "description": str(r[3] or ""),
                    "contact": str(r[4] or ""),
                    "status": c_status,
                    "photo_b64": str(r[6]) if r[6] else None,
                    "created_at": r[7].isoformat() if r[7] else None,
                    "updated_at": r[8].isoformat() if r[8] else None,
                    "acknowledged_at": r[9].isoformat() if r[9] else None,
                    "acknowledged_by": str(r[10]) if r[10] else None
                })
            
            return {
                "complaints": complaints,
                "total": len(complaints),
                "unread_count": unread_count
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/engineer/complaints/{complaint_id}/acknowledge")
def acknowledge_complaint(
    complaint_id: int,
    current_user: dict = Depends(get_current_user)
):
    role = current_user.get('role', '')
    if role != 'engineer':
        raise HTTPException(status_code=403, detail="engineer role required.")
        
    username = current_user.get('sub', 'engineer')
    zone_id = 'unknown'
    
    try:
        with engine.connect() as conn:
            # Get zone_id for audit
            row = conn.execute(text("SELECT zone_id FROM citizen_complaints WHERE complaint_id = :id"), {"id": complaint_id}).fetchone()
            if row:
                zone_id = str(row[0] or 'unknown')
                
            result = conn.execute(text("""
                UPDATE citizen_complaints 
                SET status='acknowledged', updated_at=NOW(), acknowledged_at=NOW(), acknowledged_by=:username
                WHERE complaint_id=:id AND status='open'
            """), {"id": complaint_id, "username": username})
            
            if result.rowcount == 0:
                raise HTTPException(status_code=400, detail="Complaint not found or not in open state.")
                
            conn.execute(text("""
                INSERT INTO audit_log (event_type, zone_id, details, user_role, logged_at)
                VALUES ('complaint_acknowledged', :zone, :details, 'engineer', NOW())
            """), {
                "zone": zone_id,
                "details": f"Complaint {complaint_id} acknowledged"
            })
            conn.commit()
            
        return {"success": True, "complaint_id": complaint_id, "status": "acknowledged"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/citizen/complaint/{complaint_id}/mark-fixed", tags=["Public"])
def citizen_mark_fixed(complaint_id: str):
    try:
        # Get zone_id for audit
        zone_id = 'unknown'
        with engine.connect() as conn:
            row = conn.execute(text("SELECT zone_id FROM citizen_complaints WHERE complaint_id = :id"), {"id": complaint_id}).fetchone()
            if row:
                zone_id = str(row[0] or 'unknown')
                
            result = conn.execute(text("""
                UPDATE citizen_complaints 
                SET status='resolved', updated_at=NOW(), resolved_at=NOW()
                WHERE complaint_id=:id AND status='acknowledged'
            """), {"id": complaint_id})
            
            if result.rowcount == 0:
                raise HTTPException(status_code=400, detail="Complaint not found or not in acknowledged state.")
                
            conn.execute(text("""
                INSERT INTO audit_log (event_type, zone_id, details, user_role, logged_at)
                VALUES ('citizen_marked_fixed', :zone, :details, 'public', NOW())
            """), {
                "zone": zone_id,
                "details": f"Complaint {complaint_id} marked as fixed by citizen"
            })
            conn.commit()
            
        return {"success": True, "complaint_id": complaint_id, "status": "resolved"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/engineer/complaints/{complaint_id}/archive")
def archive_complaint(complaint_id: int, current_user: dict = Depends(get_current_user)):
    role = current_user.get('role', '')
    if role != 'engineer':
        raise HTTPException(status_code=403, detail="engineer role required.")
        
    zone_id = 'unknown'
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT zone_id FROM citizen_complaints WHERE complaint_id = :id"), {"id": complaint_id}).fetchone()
            if row:
                zone_id = str(row[0] or 'unknown')
                
            result = conn.execute(text("""
                UPDATE citizen_complaints 
                SET status='archived', updated_at=NOW() 
                WHERE complaint_id=:id AND status='resolved'
            """), {"id": complaint_id})
            
            if result.rowcount == 0:
                raise HTTPException(status_code=400, detail="Complaint not found or not in resolved state.")
                
            conn.execute(text("""
                INSERT INTO audit_log (event_type, zone_id, details, user_role, logged_at)
                VALUES ('complaint_archived', :zone, :details, 'engineer', NOW())
            """), {
                "zone": zone_id,
                "details": f"Complaint {complaint_id} archived"
            })
            conn.commit()
            
        return {"success": True, "complaint_id": complaint_id, "status": "archived"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/engineer/complaints/audit-log")
def get_complaints_audit_log(current_user: dict = Depends(get_current_user)):
    role = current_user.get('role', '')
    if role not in ('engineer', 'commissioner'):
        raise HTTPException(status_code=403, detail="engineer or commissioner role required.")
        
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT log_id, event_type, zone_id, alert_id, user_role, details, logged_at
                FROM audit_log 
                WHERE event_type LIKE 'complaint_%' OR event_type = 'citizen_marked_fixed'
                ORDER BY logged_at DESC 
                LIMIT 200
            """)).fetchall()
            
            log = []
            for r in rows:
                log.append({
                    "log_id": r[0],
                    "event_type": str(r[1] or ""),
                    "zone_id": str(r[2] or ""),
                    "alert_id": r[3],
                    "user_role": str(r[4] or ""),
                    "details": str(r[5] or ""),
                    "logged_at": r[6].isoformat() if r[6] else None
                })
                
        return {"log": log, "count": len(log)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
