"""
Hydro-Equity Engine — Phase 4a
backend/routers/burst.py
GET /burst-risk/top10 → returns v6_burst_top10.json (Pipe Stress Score top-10)
Protected: requires valid JWT token.
All roles receive the same data (no zone filtering — burst risk is city-wide concern).
"""

import os
import json
from fastapi import APIRouter, Depends

from backend.auth import get_current_user

router = APIRouter(tags=["Analytics"])

OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')


@router.get(
    "/burst-risk/top10",
    summary="Top 10 Burst-Risk Pipe Segments (PSS)",
    description=(
        "Returns the top 10 pipe segments by Pipe Stress Score (PSS) from V6. "
        "Each entry includes GPS coordinates, material, age, dominant factor, "
        "and a human-readable risk summary. All authenticated roles have access."
    )
)
def get_burst_risk_top10(current_user: dict = Depends(get_current_user)):
    """
    Returns top-10 burst risk pipe segments.
    All authenticated roles receive this data — no zone filtering applied.
    PSS formula: 0.40 × PSI_n + 0.35 × CFF_n + 0.25 × ADF
    """
    path = os.path.join(OUTPUTS_DIR, "v6_burst_top10.json")

    if not os.path.exists(path):
        return {"error": "Run V6 first — v6_burst_top10.json not found in outputs/"}

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    return data