"""
Hydro-Equity Engine — Phase 4a
backend/routers/auth_router.py
Authentication endpoints:
  POST /auth/login  → verify credentials, return JWT
  GET  /auth/me     → return current user info from token
"""

from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy import text

from backend.database import engine
from backend.schemas import LoginRequest, TokenResponse, UserInfo
from backend.auth import verify_password, create_access_token, get_current_user

router = APIRouter(prefix="/auth", tags=["Authentication"])


# ── POST /auth/login ───────────────────────────────────────────────
@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Login and receive a JWT token",
    description=(
        "Submit username and password (JSON body). "
        "On success, returns a Bearer token. "
        "Use this token in the Authorization header for all protected endpoints."
    )
)
def login(request: LoginRequest):
    """
    Authenticate user credentials against the database.
    Returns JWT with: access_token, token_type, role, zone_id, full_name, username.
    """
    # ── 1. Fetch user record from PostgreSQL ──────────────────────
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    user_id::text  AS user_id,
                    username,
                    hashed_password,
                    role,
                    zone_id,
                    full_name,
                    is_active
                FROM users
                WHERE username = :username
            """),
            {"username": request.username}
        ).fetchone()

    # ── 2. User not found ─────────────────────────────────────────
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password."
        )

    user = dict(row._mapping)

    # ── 3. Account disabled ───────────────────────────────────────
    if not user.get("is_active", True):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="This account has been disabled. Contact your administrator."
        )

    # ── 4. Wrong password ─────────────────────────────────────────
    if not verify_password(request.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password."
        )

    # ── 5. Build JWT payload ──────────────────────────────────────
    token_payload = {
        "sub":       user["username"],
        "user_id":   user["user_id"],
        "role":      user["role"],
        "zone_id":   user.get("zone_id"),
        "full_name": user.get("full_name", user["username"]),
    }

    access_token = create_access_token(token_payload)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        role=user["role"],
        zone_id=user.get("zone_id"),
        full_name=user.get("full_name", user["username"]),
        username=user["username"]
    )


# ── GET /auth/me ───────────────────────────────────────────────────
@router.get(
    "/me",
    response_model=UserInfo,
    summary="Get current logged-in user info",
    description="Returns the decoded user info from the Bearer token. Requires valid token."
)
def get_me(current_user: dict = Depends(get_current_user)):
    """
    Returns current user info extracted from the JWT payload.
    No database call — reads directly from the token.
    """
    return UserInfo(
        user_id=current_user.get("user_id", ""),
        username=current_user.get("sub", ""),
        role=current_user.get("role", ""),
        zone_id=current_user.get("zone_id"),
        full_name=current_user.get("full_name"),
        is_active=True
    )