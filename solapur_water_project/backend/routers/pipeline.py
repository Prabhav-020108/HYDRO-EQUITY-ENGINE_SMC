"""
Hydro-Equity Engine — Phase 4a
backend/routers/pipeline.py
GET /pipeline → returns pipeline.geojson (pipe network shapes)
PUBLIC endpoint — no authentication required.
(GeoJSON network topology is not sensitive operational data.)
"""

import os
import json
from fastapi import APIRouter

router = APIRouter(tags=["Public"])

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'Data')


@router.get(
    "/pipeline",
    summary="Solapur pipeline network GeoJSON",
    description=(
        "Returns the full Solapur Municipal Corporation pipe network as GeoJSON. "
        "This is a public endpoint — no authentication required. "
        "Used by the Leaflet map in frontend/index.html."
    )
)
def get_pipeline():
    """
    Returns pipeline.geojson from Data/ folder.
    Public — no auth required (network topology only, no operational data).
    """
    path = os.path.join(DATA_DIR, "pipeline.geojson")

    if not os.path.exists(path):
        return {
            "error": "pipeline.geojson not found in Data/ folder.",
            "hint": "Make sure Data/pipeline.geojson exists (extracted from SMC GIS)."
        }

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    return data