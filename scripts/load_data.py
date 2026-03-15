"""
load_data.py  ·  place in: scripts/
Generates Data/nodes_with_elevation.csv from Data/pipeline.geojson.

Changes from original:
  - Tolerance-based merging: rounds to 4 decimal places before dedup.
  - Adds: node_id, node_type, zone_id columns.
  - Preserves existing elevation generation (uniform random, seed=42).

Run:  python scripts/load_data.py
"""

import numpy as np
import json
import os
import pandas as pd
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "Data")

print("=" * 62)
print("  load_data.py · Node Generation with Tolerance Merging")
print("=" * 62)

# ── Step 1: Load pipeline GeoJSON ────────────────────────────────
with open(os.path.join(DATA, "pipeline.geojson")) as f:
    pipeline_data = json.load(f)

print(f"  Loaded pipeline.geojson: {len(pipeline_data['features'])} features")

# ── Step 2: Also load pipe_segments.csv if it exists, for zone inference
#    (pipe_segments.csv is produced by v1_data_foundation.py)
pipe_segments_path = os.path.join(DATA, "pipe_segments.csv")
pipe_seg_df = None
if os.path.exists(pipe_segments_path):
    pipe_seg_df = pd.read_csv(pipe_segments_path)
    print(f"  Loaded pipe_segments.csv: {len(pipe_seg_df)} rows (for zone inference)")
else:
    print("  ⚠  pipe_segments.csv not found — zone inference will use raw GeoJSON")

# ── Step 3: Extract all pipe endpoint coordinates with zone info ──
raw_nodes = []  # list of (lon, lat, zone)

for i, feature in enumerate(pipeline_data["features"]):
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"][0]  # MultiLineString → first line

    start = coords[0]
    end = coords[-1]

    # Get zone from pipe_segments.csv if available, else from raw GeoJSON
    if pipe_seg_df is not None and i < len(pipe_seg_df):
        zone = str(pipe_seg_df.iloc[i]["zone_id"])
    else:
        raw_zone = props.get("Water Zone", props.get("WaterZone", props.get("Zone", "")))
        zone = str(raw_zone).strip() if raw_zone else "zone_unknown"

    raw_nodes.append({"lon": start[0], "lat": start[1], "zone": zone})
    raw_nodes.append({"lon": end[0],   "lat": end[1],   "zone": zone})

print(f"  Raw endpoints extracted: {len(raw_nodes)}")

# ── Step 4: Tolerance-based merging (≈0.0001° = 4 decimal rounding) ──
# Group by rounded coordinates; pick representative full-precision coords.
raw_df = pd.DataFrame(raw_nodes)
raw_df["lon_r"] = raw_df["lon"].round(4)
raw_df["lat_r"] = raw_df["lat"].round(4)

# For each rounded group, pick the first full-precision coordinate
# and collect all zone votes for majority-zone inference.
grouped = raw_df.groupby(["lon_r", "lat_r"])

unique_nodes = []
for (lon_r, lat_r), group in grouped:
    # Representative coordinates: first occurrence (full precision)
    rep = group.iloc[0]
    # Zone: majority vote
    zone_counts = Counter(group["zone"])
    majority_zone = zone_counts.most_common(1)[0][0]
    unique_nodes.append({
        "lon": rep["lon"],
        "lat": rep["lat"],
        "lon_r": lon_r,
        "lat_r": lat_r,
        "zone_id": majority_zone,
    })

nodes_df = pd.DataFrame(unique_nodes)
print(f"  After tolerance merging (4-decimal): {len(nodes_df)} unique nodes (from {len(raw_df)} endpoints)")

# ── Step 5: Assign node_id and node_type ──────────────────────────
nodes_df = nodes_df.reset_index(drop=True)
nodes_df["node_id"] = nodes_df.index  # integer ID = row index
nodes_df["node_type"] = "junction"

# ── Step 6: Elevation (preserve original logic: uniform random, seed=42)
np.random.seed(42)
nodes_df["elevation"] = np.random.uniform(440, 470, len(nodes_df))

# ── Step 7: Save ──────────────────────────────────────────────────
# Column order: node_id, lon, lat, elevation, node_type, zone_id, lon_r, lat_r
out_cols = ["node_id", "lon", "lat", "elevation", "node_type", "zone_id", "lon_r", "lat_r"]
nodes_df[out_cols].to_csv(os.path.join(DATA, "nodes_with_elevation.csv"), index=False)

print(f"\n  ✓  Saved Data/nodes_with_elevation.csv ({len(nodes_df)} nodes)")
print(f"     Columns: {', '.join(out_cols)}")
print(f"     Elevation range: {nodes_df['elevation'].min():.1f}–{nodes_df['elevation'].max():.1f} m (simulated)")
zones_found = sorted(nodes_df["zone_id"].unique())
print(f"     Zones: {zones_found}")

# ── Step 8: Enrich pipe_segments.csv with start_node_id / end_node_id ──
pipe_segments_path = os.path.join(DATA, "pipe_segments.csv")
if os.path.exists(pipe_segments_path):
    pipes = pd.read_csv(pipe_segments_path)

    # Build lookup: (lon_r, lat_r) → node_id
    node_lookup = {}
    for _, row in nodes_df.iterrows():
        key = (round(row["lon_r"], 4), round(row["lat_r"], 4))
        node_lookup[key] = int(row["node_id"])

    def find_node(lon, lat):
        key = (round(lon, 4), round(lat, 4))
        return node_lookup.get(key, None)

    pipes["start_node_id"] = pipes.apply(lambda r: find_node(r["start_lon"], r["start_lat"]), axis=1)
    pipes["end_node_id"]   = pipes.apply(lambda r: find_node(r["end_lon"],   r["end_lat"]),   axis=1)

    matched_s = pipes["start_node_id"].notna().sum()
    matched_e = pipes["end_node_id"].notna().sum()
    unmatched = len(pipes) - matched_s + len(pipes) - matched_e

    pipes["start_node_id"] = pipes["start_node_id"].astype("Int64")
    pipes["end_node_id"]   = pipes["end_node_id"].astype("Int64")

    pipes.to_csv(pipe_segments_path, index=False)
    print(f"\n  ✓  Enriched pipe_segments.csv with start_node_id, end_node_id")
    print(f"     Matched: {matched_s}/{len(pipes)} start, {matched_e}/{len(pipes)} end")
    if unmatched > 0:
        print(f"     ⚠  {unmatched} unmatched endpoints (left as null)")
else:
    print("\n  ⚠  pipe_segments.csv not found — run v1_data_foundation.py first, then re-run this script")

print()