"""
Hydro-Equity Engine — Phase 4a
backend/routers/infrastructure.py

GET /infrastructure  → ESR, storage tank, water source, WTP locations
PUBLIC — used by Leaflet map in all dashboards.

Reads Data/infrastructure_points.csv produced by V1 (v1_data_foundation.py).
Falls back gracefully if file not found (map just skips infra markers).
"""

import os, csv
from fastapi import APIRouter

router = APIRouter(tags=["Public"])

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'Data')

FEATURE_META = {
    'water_source': {'label': 'Water Source',        'color': '#1B5E20'},
    'storage_tank': {'label': 'Storage Tank / ESR',  'color': '#1565C0'},
    'raw_station':  {'label': 'Raw Water Station',   'color': '#0D5FA8'},
    'wtp':          {'label': 'Water Treatment Plant','color': '#6A1B9A'},
}


@router.get(
    "/infrastructure",
    summary="Infrastructure markers — ESR, tanks, water sources",
    description=(
        "Returns infrastructure point locations from Data/infrastructure_points.csv. "
        "Public endpoint — no authentication required. "
        "Used by Leaflet map to render ESR, storage tank, and water source markers."
    )
)
def get_infrastructure():
    path = os.path.join(DATA_DIR, 'infrastructure_points.csv')
    if not os.path.exists(path):
        return {
            "markers": [],
            "total":   0,
            "warning": (
                "infrastructure_points.csv not found in Data/. "
                "Run scripts/v1_data_foundation.py first. "
                "Map will fall back to hard-coded ESR positions."
            )
        }

    markers = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                try:
                    lat = float(row.get('lat', 0) or 0)
                    lon = float(row.get('lon', 0) or 0)
                    if lat == 0 and lon == 0:
                        continue
                    ftype = str(row.get('feature_type', 'unknown')).strip().lower()
                    meta  = FEATURE_META.get(ftype, {'label': ftype.title(), 'color': '#8A96A4'})
                    markers.append({
                        'node_id':      row.get('node_id', ''),
                        'lat':          lat,
                        'lon':          lon,
                        'feature_type': ftype,
                        'zone_id':      row.get('zone_id', ''),
                        'label':        meta['label'],
                        'color':        meta['color'],
                    })
                except (ValueError, KeyError):
                    continue
    except Exception as e:
        return {'markers': [], 'total': 0, 'error': str(e)}

    return {'markers': markers, 'total': len(markers)}