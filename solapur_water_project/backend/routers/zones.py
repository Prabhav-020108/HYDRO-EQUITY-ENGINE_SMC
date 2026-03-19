"""
Hydro-Equity Engine — Phase 4a (M3 refactor)
backend/routers/zones.py
GET /zones → returns v4_zone_status.json via data_provider
Protected: requires valid JWT token.
Role filtering (server-side):
  engineer / commissioner / field_operator → all zones
  ward_officer → only their assigned zone_id
"""

from fastapi import APIRouter, Depends

from backend.auth import get_current_user
from backend import data_provider

router = APIRouter(tags=["Analytics"])


@router.get(
    "/zones",
    summary="Zone Hydraulic Equity Index (HEI) scores",
    description=(
        "Returns zone equity data from V4 (v4_zone_status.json). "
        "ward_officer role returns only their assigned zone. "
        "All other roles receive all zones."
    )
)
def get_zones(current_user: dict = Depends(get_current_user)):
    """
    Returns HEI zone status for the requesting user's scope.
    ward_officer: filtered to their zone_id (server-side enforced).
    """
    data = data_provider.get_zone_status()

    if not data:
        return {"error": "Run V4 first — v4_zone_status.json not found in outputs/"}

    # ── Role-based filtering ──────────────────────────────────────
    role    = current_user.get("role", "")
    zone_id = current_user.get("zone_id")

    if role == "ward_officer" and zone_id:
        filtered = [z for z in data if str(z.get("zone_id", "")) == str(zone_id)]
        return filtered

    # All other roles: full data
    return data