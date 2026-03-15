"""
Hydro-Equity Engine — Phase 4a
backend/routers/zones.py
GET /zones → returns v4_zone_status.json
Protected: requires valid JWT token.
Role filtering (server-side):
  engineer / commissioner / field_operator → all zones
  ward_officer → only their assigned zone_id
"""

import os
import json
from fastapi import APIRouter, Depends, HTTPException

from backend.auth import get_current_user

router = APIRouter(tags=["Analytics"])

# Path to outputs folder (one level up from backend/)
OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')


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
    path = os.path.join(OUTPUTS_DIR, "v4_zone_status.json")

    if not os.path.exists(path):
        return {"error": "Run V4 first — v4_zone_status.json not found in outputs/"}

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # ── Role-based filtering ──────────────────────────────────────
    role    = current_user.get("role", "")
    zone_id = current_user.get("zone_id")

    if role == "ward_officer" and zone_id:
        # Ward officers see ONLY their assigned zone (server-side enforced)
        if isinstance(data, list):
            filtered = [z for z in data if str(z.get("zone_id", "")) == str(zone_id)]
            if not filtered:
                # Return empty list with explanation if zone not found in data
                return []
            return filtered

    # All other roles: full data
    return data