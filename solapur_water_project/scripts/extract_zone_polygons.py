"""
Dhara — Hydro-Equity Engine
scripts/extract_zone_polygons.py

Reads Data/pipe_segments.csv to extract zone boundary polygons.
For each zone_id (zone_1 through zone_8, skipping zone_unknown):
  1. Collect all pipe endpoint coordinates (start_lon, start_lat, end_lon, end_lat)
  2. Compute convex hull using scipy.spatial.ConvexHull
     (falls back to bounding box if scipy is unavailable)
  3. Compute centroid as mean of all collected points
  4. Upsert into zone_polygons table via INSERT ... ON CONFLICT (zone_id) DO UPDATE
  5. Stores polygon_coords as JSON string [[lon, lat], ...]

Run: python scripts/extract_zone_polygons.py
"""

import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from sqlalchemy import text
from backend.database import engine

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(ROOT, 'Data', 'pipe_segments.csv')

VALID_ZONES = {f'zone_{i}' for i in range(1, 9)}  # zone_1 through zone_8


def _convex_hull_vertices(points: list) -> list:
    """
    Compute convex hull vertices from a list of [lon, lat] points.
    Returns ordered list of [lon, lat] hull vertices.
    Falls back to 4-corner bounding box if scipy is unavailable or hull fails.
    """
    if len(points) < 3:
        return points

    # ── Try scipy ConvexHull ──────────────────────────────────────
    try:
        from scipy.spatial import ConvexHull
        import numpy as np
        pts = np.array(points)
        hull = ConvexHull(pts)
        vertices = pts[hull.vertices].tolist()
        return vertices
    except ImportError:
        pass  # scipy not available — fall through to bounding box
    except Exception:
        pass  # degenerate points or other scipy error — fall through

    # ── Fallback: 4-corner bounding box ──────────────────────────
    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    return [
        [min_lon, min_lat],
        [max_lon, min_lat],
        [max_lon, max_lat],
        [min_lon, max_lat],
    ]


def extract_and_upsert():
    print("=" * 62)
    print("  extract_zone_polygons.py — Zone Boundary Extractor")
    print("=" * 62)

    # ── Load CSV ──────────────────────────────────────────────────
    if not os.path.exists(CSV_PATH):
        print(f"\n  ❌  File not found: {CSV_PATH}")
        print("  Make sure Data/pipe_segments.csv exists.")
        sys.exit(1)

    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"\n  ❌  Could not read CSV: {e}")
        sys.exit(1)

    required_cols = {'start_lon', 'start_lat', 'end_lon', 'end_lat', 'zone_id'}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        print(f"\n  ❌  Missing columns in CSV: {missing_cols}")
        print(f"  Found columns: {list(df.columns)}")
        sys.exit(1)

    print(f"\n  Loaded {len(df)} pipe segments.")

    # ── Build zone → points mapping ───────────────────────────────
    zone_points: dict = {}
    skipped = 0

    for _, row in df.iterrows():
        zone_id = str(row.get('zone_id', '')).strip()
        if zone_id not in VALID_ZONES:
            skipped += 1
            continue

        # Each pipe contributes both endpoints
        try:
            slon = float(row['start_lon'])
            slat = float(row['start_lat'])
            elon = float(row['end_lon'])
            elat = float(row['end_lat'])
        except (ValueError, TypeError):
            continue

        if zone_id not in zone_points:
            zone_points[zone_id] = []
        zone_points[zone_id].append([slon, slat])
        zone_points[zone_id].append([elon, elat])

    print(f"  Skipped {skipped} rows (zone_unknown or invalid zone).")
    print(f"  Processing {len(zone_points)} valid zones...\n")

    # ── Compute hull + centroid, upsert to DB ─────────────────────
    with engine.connect() as conn:
        for zone_id in sorted(zone_points.keys()):
            pts = zone_points[zone_id]
            if not pts:
                print(f"  [SKIP] {zone_id}: no points")
                continue

            # Deduplicate points
            pts_unique = list(set(map(tuple, pts)))
            pts_list = [list(p) for p in pts_unique]

            # Centroid = mean of all collected points (before hull reduction)
            centroid_lon = sum(p[0] for p in pts_list) / len(pts_list)
            centroid_lat = sum(p[1] for p in pts_list) / len(pts_list)

            # Convex hull (or fallback bounding box)
            hull_vertices = _convex_hull_vertices(pts_list)
            polygon_json = json.dumps(hull_vertices)

            try:
                conn.execute(text("""
                    INSERT INTO zone_polygons
                        (zone_id, polygon_coords, centroid_lat, centroid_lon, updated_at)
                    VALUES
                        (:zone_id, :polygon_coords, :centroid_lat, :centroid_lon, NOW())
                    ON CONFLICT (zone_id) DO UPDATE SET
                        polygon_coords = EXCLUDED.polygon_coords,
                        centroid_lat   = EXCLUDED.centroid_lat,
                        centroid_lon   = EXCLUDED.centroid_lon,
                        updated_at     = NOW()
                """), {
                    'zone_id':       zone_id,
                    'polygon_coords': polygon_json,
                    'centroid_lat':  round(centroid_lat, 6),
                    'centroid_lon':  round(centroid_lon, 6),
                })
                conn.commit()
                print(
                    f"  [OK] {zone_id:<12}  "
                    f"centroid=({centroid_lat:.5f}, {centroid_lon:.5f})  "
                    f"vertices={len(hull_vertices)}"
                )
            except Exception as e:
                print(f"  [ERROR] {zone_id}: {e}")

    print()
    print("=" * 62)
    print(f"  ✅  Zone polygons extracted and upserted for {len(zone_points)} zones.")
    print("  Next: Serve via GET /zones/polygons endpoint (if implemented).")
    print("=" * 62)


if __name__ == '__main__':
    extract_and_upsert()
