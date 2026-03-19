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
    # ── 1. Fetch user record (Dev bypass support) ──────────────────
    import os
    user = None

    if os.getenv("AUTH_DEV_MODE") == "1":
        # Dev Mode: In-memory authentication to bypass missing PostgreSQL
        DEMO_USERS = {
            "engineer1":     {"user_id": "dev_eng_1",    "username": "engineer1",     "role": "engineer",       "zone_id": None,      "full_name": "Demo Engineer",          "is_active": True},
            "field_op1":     {"user_id": "dev_fop_1",    "username": "field_op1",     "role": "field_operator", "zone_id": None,      "full_name": "Demo Field Operator",    "is_active": True},
            "field_op_z1":   {"user_id": "dev_fop_z1",   "username": "field_op_z1",   "role": "field_operator", "zone_id": "zone_1",  "full_name": "Field Operator Zone 1",  "is_active": True},
            "field_op_z2":   {"user_id": "dev_fop_z2",   "username": "field_op_z2",   "role": "field_operator", "zone_id": "zone_2",  "full_name": "Field Operator Zone 2",  "is_active": True},
            "field_op_z3":   {"user_id": "dev_fop_z3",   "username": "field_op_z3",   "role": "field_operator", "zone_id": "zone_3",  "full_name": "Field Operator Zone 3",  "is_active": True},
            "field_op_z4":   {"user_id": "dev_fop_z4",   "username": "field_op_z4",   "role": "field_operator", "zone_id": "zone_4",  "full_name": "Field Operator Zone 4",  "is_active": True},
            "field_op_z5":   {"user_id": "dev_fop_z5",   "username": "field_op_z5",   "role": "field_operator", "zone_id": "zone_5",  "full_name": "Field Operator Zone 5",  "is_active": True},
            "field_op_z6":   {"user_id": "dev_fop_z6",   "username": "field_op_z6",   "role": "field_operator", "zone_id": "zone_6",  "full_name": "Field Operator Zone 6",  "is_active": True},
            "field_op_z7":   {"user_id": "dev_fop_z7",   "username": "field_op_z7",   "role": "field_operator", "zone_id": "zone_7",  "full_name": "Field Operator Zone 7",  "is_active": True},
            "field_op_z8":   {"user_id": "dev_fop_z8",   "username": "field_op_z8",   "role": "field_operator", "zone_id": "zone_8",  "full_name": "Field Operator Zone 8",  "is_active": True},
            "ward_z1":       {"user_id": "dev_ward_1",   "username": "ward_z1",       "role": "ward_officer",   "zone_id": "zone_1",  "full_name": "Ward Officer — Zone 1",  "is_active": True},
            "ward_z2":       {"user_id": "dev_ward_2",   "username": "ward_z2",       "role": "ward_officer",   "zone_id": "zone_2",  "full_name": "Ward Officer — Zone 2",  "is_active": True},
            "ward_z3":       {"user_id": "dev_ward_3",   "username": "ward_z3",       "role": "ward_officer",   "zone_id": "zone_3",  "full_name": "Ward Officer — Zone 3",  "is_active": True},
            "ward_z4":       {"user_id": "dev_ward_4",   "username": "ward_z4",       "role": "ward_officer",   "zone_id": "zone_4",  "full_name": "Ward Officer — Zone 4",  "is_active": True},
            "ward_z5":       {"user_id": "dev_ward_5",   "username": "ward_z5",       "role": "ward_officer",   "zone_id": "zone_5",  "full_name": "Ward Officer — Zone 5",  "is_active": True},
            "ward_z6":       {"user_id": "dev_ward_6",   "username": "ward_z6",       "role": "ward_officer",   "zone_id": "zone_6",  "full_name": "Ward Officer — Zone 6",  "is_active": True},
            "ward_z7":       {"user_id": "dev_ward_7",   "username": "ward_z7",       "role": "ward_officer",   "zone_id": "zone_7",  "full_name": "Ward Officer — Zone 7",  "is_active": True},
            "ward_z8":       {"user_id": "dev_ward_8",   "username": "ward_z8",       "role": "ward_officer",   "zone_id": "zone_8",  "full_name": "Ward Officer — Zone 8",  "is_active": True},
            "commissioner1": {"user_id": "dev_comm_1",   "username": "commissioner1", "role": "commissioner",   "zone_id": None,      "full_name": "Demo Commissioner",      "is_active": True},
        }
        
        if request.username in DEMO_USERS:
            if request.password == "demo123":
                user = DEMO_USERS[request.username]
            else:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password.")
        else:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password.")
    else:
        # Standard Database Flow
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT user_id::text AS user_id, username, hashed_password, role, zone_id, full_name, is_active
                    FROM users WHERE username = :username
                """),
                {"username": request.username}
            ).fetchone()

        if row is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password.")

        user = dict(row._mapping)

        if not user.get("is_active", True):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Account disabled.")

        if not verify_password(request.password, user["hashed_password"]):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password.")


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