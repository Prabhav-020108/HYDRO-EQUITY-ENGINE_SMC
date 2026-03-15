"""
Hydro-Equity Engine — Phase 4a
backend/auth.py
JWT token generation/validation + bcrypt password hashing.
FastAPI dependency functions: get_current_user, require_roles.

FIX: Uses bcrypt directly (no passlib) to avoid the passlib/bcrypt 4.x
     incompatibility error: 'module bcrypt has no attribute __about__'
"""

import os
import bcrypt
from datetime import datetime, timedelta
from typing import Optional

from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv

# ── Load .env from project root ────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SECRET_KEY                = os.getenv('SECRET_KEY', 'CHANGE_THIS_IN_ENV_FILE')
ALGORITHM                 = os.getenv('ALGORITHM', 'HS256')
ACCESS_TOKEN_EXPIRE_HOURS = int(os.getenv('ACCESS_TOKEN_EXPIRE_HOURS', '8'))


# ── Password hashing (direct bcrypt — no passlib) ──────────────────

def get_password_hash(password: str) -> str:
    """Hash a plain-text password using bcrypt. Returns a UTF-8 string."""
    salt   = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against its stored bcrypt hash."""
    return bcrypt.checkpw(
        plain_password.encode('utf-8'),
        hashed_password.encode('utf-8')
    )


# ── JWT token creation ─────────────────────────────────────────────

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a signed JWT token.
    data should include: sub (username), user_id, role, zone_id, full_name
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta if expires_delta
        else timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    """
    Decode and validate a JWT token.
    Returns payload dict on success, None on failure.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


# ── FastAPI HTTP Bearer scheme ─────────────────────────────────────
http_bearer = HTTPBearer(auto_error=True)


# ── FastAPI dependency: get current user ───────────────────────────
def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(http_bearer)
) -> dict:
    """
    FastAPI dependency — extracts and validates the Bearer token.
    Returns the decoded JWT payload (dict) on success.
    Raises HTTP 401 if token is missing, expired, or invalid.
    """
    token   = credentials.credentials
    payload = decode_token(token)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token is invalid or expired. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not payload.get("sub"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token payload is malformed.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


# ── FastAPI dependency: role-based access control ─────────────────
def require_roles(*allowed_roles: str):
    """
    FastAPI dependency factory for role-based access control.

    Usage:
        @router.get("/admin-only")
        def admin_route(user = Depends(require_roles("engineer", "commissioner"))):
            ...

    Raises HTTP 403 if the user's role is not in allowed_roles.
    """
    def role_checker(current_user: dict = Depends(get_current_user)) -> dict:
        user_role = current_user.get("role", "")
        if user_role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Access denied. Your role '{user_role}' is not permitted here. "
                    f"Required: {', '.join(allowed_roles)}"
                )
            )
        return current_user
    return role_checker