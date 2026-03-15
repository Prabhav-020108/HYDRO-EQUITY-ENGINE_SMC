"""
Hydro-Equity Engine — Phase 4a
backend/schemas.py
Pydantic models for request validation and response serialization.
"""

from pydantic import BaseModel
from typing import Optional


# ── Request Schemas ────────────────────────────────────────────────

class LoginRequest(BaseModel):
    """Body for POST /auth/login"""
    username: str
    password: str

    class Config:
        json_schema_extra = {
            "example": {
                "username": "engineer1",
                "password": "demo@1234"
            }
        }


# ── Response Schemas ───────────────────────────────────────────────

class TokenResponse(BaseModel):
    """Response from POST /auth/login"""
    access_token: str
    token_type:   str           # always "bearer"
    role:         str           # engineer | ward_officer | commissioner | field_operator
    zone_id:      Optional[str] = None   # null for engineer/commissioner
    full_name:    Optional[str] = None
    username:     str

    class Config:
        json_schema_extra = {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
                "role": "engineer",
                "zone_id": None,
                "full_name": "Prabhav Tiwari (Engineer)",
                "username": "engineer1"
            }
        }


class UserInfo(BaseModel):
    """Response from GET /auth/me"""
    user_id:   str
    username:  str
    role:      str
    zone_id:   Optional[str] = None
    full_name: Optional[str] = None
    is_active: bool


class ErrorResponse(BaseModel):
    """Standard error response"""
    detail: str