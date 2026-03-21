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
import json as _json

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

@router.get(
    "/nrw",
    summary="Estimated NRW percentage — public endpoint",
    description=(
        "Returns the Non-Revenue Water estimate from outputs/v4_equity_minimal.json. "
        "Falls back to '18% (baseline estimate)' if file or key is missing. "
        "Public — no authentication required."
    )
)
def get_nrw():
    """
    Public endpoint. Returns NRW value as a plain string like '18%' or '18% (baseline estimate)'.
    Reads from outputs/v4_equity_minimal.json — the field written by V4 analytics engine.
    """
    import json as _json_inner
    outputs_path = os.path.join(DATA_DIR, '..', 'outputs', 'v4_equity_minimal.json')
    outputs_path = os.path.normpath(outputs_path)

    if not os.path.exists(outputs_path):
        return {"nrw": "18% (baseline estimate)", "source": "fallback"}

    try:
        with open(outputs_path, encoding='utf-8') as f:
            data = _json_inner.load(f)

        nrw = (
            data.get('nrw_pct') or
            data.get('nrw') or
            data.get('estimated_nrw')
        )

        if nrw is not None:
            if isinstance(nrw, (int, float)):
                val = float(nrw)
                if val <= 1.0:
                    val *= 100
                return {"nrw": f"{val:.1f}%", "source": "v4_equity_minimal.json"}
            return {"nrw": str(nrw), "source": "v4_equity_minimal.json"}

    except Exception:
        pass

    return {"nrw": "18% (baseline estimate)", "source": "fallback"}

from pydantic import BaseModel
from sqlalchemy import text
from backend.database import engine
import math

class CitizenLocateRequest(BaseModel):
    lat: float
    lon: float

def is_point_in_polygon(point: list, polygon: list) -> bool:
    x, y = point[0], point[1]
    inside = False
    n = len(polygon)
    if n == 0:
        return False
    p1x, p1y = polygon[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xints = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside

@router.post("/citizen/locate")
def locate_citizen(req: CitizenLocateRequest):
    closest_zone = {"zone_id": "zone_1", "zone_name": "Zone 1", "detection_method": "nearest_centroid"}
    try:
        min_dist = float('inf')
        matched_zones = []

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT zone_id, polygon_coords, centroid_lat, centroid_lon FROM zone_polygons")).fetchall()

        for r in rows:
            z_id = str(r[0])
            z_name = z_id.replace('zone_', 'Zone ').title() if z_id.startswith('zone_') else z_id
            coords_str = r[1]
            c_lat = float(r[2] or 0)
            c_lon = float(r[3] or 0)

            dist = math.hypot(req.lat - c_lat, req.lon - c_lon)
            if dist < min_dist:
                min_dist = dist
                closest_zone = {"zone_id": z_id, "zone_name": z_name, "detection_method": "nearest_centroid"}

            if coords_str:
                polygon = _json.loads(coords_str)
                if is_point_in_polygon([req.lon, req.lat], polygon):
                    matched_zones.append({"zone_id": z_id, "zone_name": z_name})

        if len(matched_zones) == 1:
            z = matched_zones[0]
            z["detection_method"] = "polygon"
            return z

        return closest_zone
    except Exception:
        return closest_zone