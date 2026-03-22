from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from typing import Optional

from backend.database import engine
from backend.auth import get_current_user, create_access_token, verify_password

router = APIRouter(tags=["CitizenAuth"])

class LoginRequest(BaseModel):
    phone: str
    password: str

@router.post("/citizen/auth/login")
def login_citizen(req: LoginRequest):
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT citizen_id, phone, hashed_password, name, zone_id, zone_name, is_active, created_at
                FROM citizens WHERE phone = :phone
            """), 
            {"phone": req.phone}
        ).fetchone()
        
    if not row:
        raise HTTPException(status_code=401, detail="Invalid phone or password")
        
    citizen_id, phone, hashed_password, name, zone_id, zone_name, is_active, created_at = row
    
    if not verify_password(req.password, hashed_password):
        raise HTTPException(status_code=401, detail="Invalid phone or password")
        
    if not is_active:
        raise HTTPException(status_code=401, detail="Account is inactive")
        
    access_token = create_access_token(
        data={
            "sub": phone,
            "citizen_id": str(citizen_id),
            "name": name,
            "zone_id": zone_id,
            "zone_name": zone_name,
            "role": "citizen"
        }
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "name": name,
        "phone": phone,
        "zone_id": zone_id,
        "zone_name": zone_name,
        "created_at": created_at.isoformat() if created_at else None
    }

@router.get("/citizen/auth/me")
def get_me(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") != "citizen":
        raise HTTPException(status_code=403, detail="Not authorized")
        
    return {
        "citizen_id": current_user.get("citizen_id"),
        "phone": current_user.get("sub"),
        "name": current_user.get("name"),
        "zone_id": current_user.get("zone_id"),
        "zone_name": current_user.get("zone_name"),
    }
