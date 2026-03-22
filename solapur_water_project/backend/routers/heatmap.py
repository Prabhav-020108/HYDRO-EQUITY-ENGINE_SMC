"""
Dhara Hydro-Equity Engine — Citizen Heatmap API
backend/routers/heatmap.py

Geospatial complaint density data for Leaflet.heat overlays
on Commissioner and Ward Officer dashboards.

ENDPOINTS:
  GET /heatmap/citizen-alerts           all zones  (commissioner / engineer)
  GET /heatmap/citizen-alerts/{zone}    one zone   (ward officer)
  GET /heatmap/zone-summary             red-zone flags per zone
"""

import hashlib
import logging
from typing import Optional, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from backend.auth import get_current_user
from backend.database import engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/heatmap", tags=["Heatmap"])

ACTIVE_STATUSES = ('open', 'acknowledged', 'not_resolved')
RED_ZONE_THRESHOLD = 10

PROBLEM_LABELS = {
    'no_water':       'No Water Supply',
    'low_pressure':   'Low Pressure',
    'dirty_water':    'Dirty / Contaminated Water',
    'pipe_leak':      'Visible Pipe Leak',
    'billing':        'Billing Dispute',
    'tanker_request': 'Emergency Tanker Request',
}

ZONE_CENTROIDS = {
    'zone_1': (17.7038, 75.9065), 'zone_2': (17.7038, 75.9430),
    'zone_3': (17.6690, 75.8700), 'zone_4': (17.6690, 75.9065),
    'zone_5': (17.6690, 75.9430), 'zone_6': (17.6342, 75.8700),
    'zone_7': (17.6342, 75.9065), 'zone_8': (17.6342, 75.9430),
}


def _jitter(complaint_id: int, base_lat: float, base_lon: float):
    h1 = int(hashlib.md5(str(complaint_id).encode()).hexdigest(), 16)
    h2 = int(hashlib.md5((str(complaint_id) + 'x').encode()).hexdigest(), 16)
    lat = base_lat + ((h1 % 1000) / 1000.0 * 0.008 - 0.004)
    lon = base_lon + ((h2 % 1000) / 1000.0 * 0.008 - 0.004)
    return round(float(lat), 6), round(float(lon), 6)


def _intensity(age_seconds: float) -> float:
    age_hours = age_seconds / 3600.0
    return max(0.10, round(float(1.0 - (age_hours / 24.0)), 3))


def _fetch_points(zone_id: Optional[str] = None) -> list:
    sql = """
        SELECT
            cc.complaint_id,
            cc.zone_id,
            cc.problem_type,
            cc.status,
            cc.lat,
            cc.lon,
            EXTRACT(EPOCH FROM (NOW() - cc.created_at)) AS age_seconds,
            zp.centroid_lat,
            zp.centroid_lon
        FROM  citizen_complaints cc
        LEFT JOIN zone_polygons zp ON cc.zone_id = zp.zone_id
        WHERE cc.status     IN :statuses
          AND cc.created_at > NOW() - INTERVAL '24 hours'
    """
    params: dict[str, Any] = {"statuses": ACTIVE_STATUSES}
    if zone_id:
        sql += " AND cc.zone_id = :zone_id"
        params["zone_id"] = zone_id
    sql += " ORDER BY cc.created_at DESC"

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
    except Exception as exc:
        logger.error("[heatmap] DB query failed: %s", exc)
        return []

    points = []
    for r in rows:
        complaint_id = r[0]
        z_id         = str(r[1] or '')
        prob_type    = str(r[2] or 'unknown')
        status_val   = str(r[3] or 'open')
        raw_lat      = r[4]
        raw_lon      = r[5]
        age_sec      = float(r[6] or 0)
        c_lat        = float(r[7]) if r[7] else None
        c_lon        = float(r[8]) if r[8] else None

        if raw_lat and raw_lon:
            lat, lon = float(raw_lat), float(raw_lon)
        elif c_lat and c_lon:
            lat, lon = _jitter(complaint_id, c_lat, c_lon)
        elif z_id in ZONE_CENTROIDS:
            base = ZONE_CENTROIDS[z_id]
            lat, lon = _jitter(complaint_id, base[0], base[1])
        else:
            continue

        points.append({
            "complaint_id":  complaint_id,
            "lat":           lat,
            "lon":           lon,
            "intensity":     _intensity(age_sec),
            "zone_id":       z_id,
            "problem_type":  prob_type,
            "problem_label": PROBLEM_LABELS.get(prob_type, prob_type.replace('_', ' ').title()),
            "status":        status_val,
            "age_hours":     round(float(age_sec / 3600), 1),
        })
    return points


@router.get("/citizen-alerts", summary="All-zone heatmap points (Commissioner)")
def get_heatmap_all_zones(current_user: dict = Depends(get_current_user)):
    role = current_user.get("role", "")
    if role not in ("commissioner", "engineer"):
        raise HTTPException(status_code=403, detail="commissioner or engineer role required.")
    points = _fetch_points()
    return {"points": points, "total": len(points), "scope": "city", "threshold": RED_ZONE_THRESHOLD}


@router.get("/citizen-alerts/{zone_id}", summary="Single-zone heatmap points (Ward Officer)")
def get_heatmap_one_zone(zone_id: str, current_user: dict = Depends(get_current_user)):
    role      = current_user.get("role", "")
    user_zone = current_user.get("zone_id")
    if role == "ward_officer" and user_zone and user_zone != zone_id:
        raise HTTPException(status_code=403, detail=f"Access denied. Your zone is '{user_zone}'.")
    if role not in ("ward_officer", "engineer", "commissioner"):
        raise HTTPException(status_code=403, detail="Insufficient role.")
    points = _fetch_points(zone_id=zone_id)
    return {"points": points, "total": len(points), "scope": "zone",
            "zone_id": zone_id, "threshold": RED_ZONE_THRESHOLD}


@router.get("/zone-summary", summary="Per-zone complaint counts and red-zone flags")
def get_zone_summary(current_user: dict = Depends(get_current_user)):
    role      = current_user.get("role", "")
    user_zone = current_user.get("zone_id")
    sql = """
        SELECT zone_id,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE problem_type = 'no_water')       AS no_water,
               COUNT(*) FILTER (WHERE problem_type = 'low_pressure')   AS low_pressure,
               COUNT(*) FILTER (WHERE problem_type = 'dirty_water')    AS dirty_water,
               COUNT(*) FILTER (WHERE problem_type = 'pipe_leak')      AS pipe_leak,
               COUNT(*) FILTER (WHERE problem_type = 'billing')        AS billing,
               COUNT(*) FILTER (WHERE problem_type = 'tanker_request') AS tanker_request
        FROM  citizen_complaints
        WHERE status     IN :statuses
          AND created_at > NOW() - INTERVAL '24 hours'
        GROUP BY zone_id
        ORDER BY total DESC
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"statuses": ACTIVE_STATUSES}).fetchall()
    except Exception as exc:
        logger.error("[heatmap/zone-summary] DB error: %s", exc)
        return {"zones": [], "red_zones": []}

    zones, red_zones = [], []
    for r in rows:
        z_id  = str(r[0] or '')
        total = int(r[1] or 0)
        if role == "ward_officer" and user_zone and z_id != user_zone:
            continue
        is_red = total >= RED_ZONE_THRESHOLD
        if is_red:
            red_zones.append(z_id)
        zones.append({
            "zone_id":    z_id,
            "zone_name":  "Zone {}".format(z_id.replace("zone_", "")),
            "total":      total,
            "is_red_zone": is_red,
            "breakdown": {
                "no_water":       int(r[2] or 0),
                "low_pressure":   int(r[3] or 0),
                "dirty_water":    int(r[4] or 0),
                "pipe_leak":      int(r[5] or 0),
                "billing":        int(r[6] or 0),
                "tanker_request": int(r[7] or 0),
            }
        })
    return {"zones": zones, "red_zones": red_zones, "threshold": RED_ZONE_THRESHOLD}
