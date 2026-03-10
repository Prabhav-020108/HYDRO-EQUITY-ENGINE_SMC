# """
# v1_data_foundation.py  ·  place in: scripts/
# V1 — Data Foundation
# Team Devsters · Hydro-Equity Engine · SAMVED-2026

# What this does:
#   1. Parses ALL 4 GeoJSON files (pipeline, water_source, storage_tank, raw_station)
#   2. Cleans the pipeline data (standardizes zones, materials, flags nulls)
#   3. Builds pipe_segments table with age/material assumptions (needed by V6)
#   4. Builds zone_demand table (needed by V3 and V5)
#   5. Saves everything to CSV files in Data/

# Run:  python scripts/v1_data_foundation.py
# """

# import json
# import os
# import pandas as pd
# import numpy as np

# # ── Paths ──────────────────────────────────────────────────────
# ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DATA = os.path.join(ROOT, "Data")
# OUT  = DATA   # save outputs back into Data/ so all scripts find them easily

# print("=" * 62)
# print("  V1 · Data Foundation · Hydro-Equity Engine")
# print("=" * 62)


# # ─────────────────────────────────────────────────────────────
# # STEP 1 — Parse all GeoJSON files
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 1 — Parsing GeoJSON files...")

# def load_geojson(filename):
#     path = os.path.join(DATA, filename)
#     if not os.path.exists(path):
#         print(f"  ⚠  Not found: {filename}")
#         return None
#     with open(path) as f:
#         data = json.load(f)
#     print(f"  ✓  {filename}: {len(data['features'])} features")
#     return data

# pipeline_data    = load_geojson("pipeline.geojson")
# water_source_data = load_geojson("water_source.geojson")
# storage_tank_data = load_geojson("storage_tank.geojson")
# raw_station_data  = load_geojson("raw_station.geojson")


# # ─────────────────────────────────────────────────────────────
# # STEP 2 — Clean and standardise pipeline data
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 2 — Cleaning pipeline data...")

# def standardise_zone(z):
#     """Convert 'Zone 5', '5', 'Z5', 'zone_5' → 'zone_5'"""
#     if pd.isna(z) or str(z).strip() == "":
#         return "zone_unknown"
#     z = str(z).strip().lower()
#     z = z.replace("zone ", "zone_").replace("z", "zone_", 1) if z.startswith("z") else z
#     # Remove extra spaces
#     z = "_".join(z.split())
#     # Ensure format is zone_N
#     if not z.startswith("zone_"):
#         z = "zone_" + z
#     return z

# def standardise_material(m):
#     """Convert 'Cast Iron', 'cast iron', 'CI' → 'CI'"""
#     if pd.isna(m) or str(m).strip() == "":
#         return "Unknown"
#     m = str(m).strip().upper()
#     if m in ["CAST IRON", "CASTIRON", "CI", "C.I."]:
#         return "CI"
#     if m in ["DUCTILE IRON", "DUCTILEIRON", "DI", "D.I."]:
#         return "DI"
#     if m in ["PVC", "POLYVINYL CHLORIDE", "UPVC", "CPVC"]:
#         return "PVC"
#     if m in ["GI", "GALVANIZED IRON", "GALVANISED IRON"]:
#         return "GI"
#     if m in ["AC", "ASBESTOS CEMENT", "ASBESTOS"]:
#         return "AC"
#     if m in ["MS", "MILD STEEL", "STEEL"]:
#         return "MS"
#     return m  # keep as-is if unknown

# # Build pipe_rows from pipeline GeoJSON
# diam_key = None
# pipe_rows = []

# for i, feat in enumerate(pipeline_data["features"]):
#     props  = feat["properties"]
#     geom   = feat["geometry"]

#     # Auto-detect diameter key (handles both "Diameter(m)" and "Diameter(m")
#     if diam_key is None:
#         for k in props:
#             if "diameter" in k.lower():
#                 diam_key = k
#                 break
#         if diam_key:
#             print(f"  ℹ  Detected diameter key: '{diam_key}'")

#     coords = geom["coordinates"][0]   # MultiLineString → first line
#     start  = coords[0]
#     end    = coords[-1]

#     raw_diam = props.get(diam_key, None) if diam_key else None
#     raw_zone = props.get("Water Zone", props.get("WaterZone", props.get("Zone", None)))
#     raw_mat  = props.get("Material", props.get("material", None))
#     raw_len  = props.get("Length(m)", props.get("length", None))

#     # Data quality flag
#     quality = "complete"
#     if raw_diam is None or str(raw_diam).strip() in ["", "None", "null"]:
#         quality = "missing_diameter"
#     if raw_zone is None or str(raw_zone).strip() in ["", "None", "null"]:
#         quality = "missing_zone" if quality == "complete" else quality + "+missing_zone"

#     pipe_rows.append({
#         "segment_id":       i,
#         "start_lon":        round(start[0], 6),
#         "start_lat":        round(start[1], 6),
#         "end_lon":          round(end[0],   6),
#         "end_lat":          round(end[1],   6),
#         "material":         standardise_material(raw_mat),
#         "diameter_m":       max(float(raw_diam) if raw_diam and str(raw_diam).strip() not in ["", "None"] else 0.05, 0.05),
#         "length_m":         max(float(raw_len)  if raw_len  and str(raw_len).strip()  not in ["", "None"] else 1.0,  1.0),
#         "zone_id":          standardise_zone(raw_zone),
#         "pipeline_type":    str(props.get("Pipeline T", props.get("PipelineType", "Unknown"))),
#         "data_quality_flag": quality,
#     })

# pipes_df = pd.DataFrame(pipe_rows)

# print(f"  ✓  Total pipe segments:  {len(pipes_df)}")
# print(f"  ✓  Complete records:     {(pipes_df['data_quality_flag']=='complete').sum()}")
# print(f"  ⚠  Missing diameter:    {pipes_df['data_quality_flag'].str.contains('missing_diameter').sum()}")
# print(f"  ⚠  Missing zone:        {pipes_df['data_quality_flag'].str.contains('missing_zone').sum()}")
# print(f"  ✓  Unique zones found:   {sorted(pipes_df['zone_id'].unique())}")
# print(f"  ✓  Materials found:      {sorted(pipes_df['material'].unique())}")


# # ─────────────────────────────────────────────────────────────
# # STEP 3 — Extract pipe junction nodes
# # (nodes_with_elevation.csv already exists with simulated elevation)
# # We just verify it and add zone info if possible
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 3 — Verifying nodes_with_elevation.csv...")

# nodes_path = os.path.join(DATA, "nodes_with_elevation.csv")
# if os.path.exists(nodes_path):
#     nodes_df = pd.read_csv(nodes_path)
#     nodes_df["lon"] = nodes_df["lon"].round(6)
#     nodes_df["lat"] = nodes_df["lat"].round(6)
#     print(f"  ✓  Nodes loaded: {len(nodes_df)} nodes")
#     print(f"  ✓  Elevation range: {nodes_df['elevation'].min():.1f}–{nodes_df['elevation'].max():.1f} m (simulated)")
# else:
#     print("  ✗  nodes_with_elevation.csv not found! Run load_data.py first.")
#     exit(1)


# # ─────────────────────────────────────────────────────────────
# # STEP 4 — Elevation already simulated (440–470m)
# # No API call needed
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 4 — Elevation: using simulated values (440–470m) ✓")


# # ─────────────────────────────────────────────────────────────
# # STEP 5 — Zone demand estimation
# # Using Census 2011 Solapur population + CPHEEO 135L/person/day
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 5 — Zone demand estimation...")

# # Solapur Municipal Corporation total population (Census 2011): ~951,558
# # Estimate 2024 population: × (1.012)^13 ≈ 1.17 growth factor
# SOLAPUR_POP_2011      = 951558
# GROWTH_FACTOR         = (1.012) ** 13          # 13 years from 2011 to 2024
# SOLAPUR_POP_2024      = int(SOLAPUR_POP_2011 * GROWTH_FACTOR)
# CPHEEO_LPCD           = 135                    # litres per capita per day
# SECONDS_PER_DAY       = 86400

# print(f"  Solapur 2011 population: {SOLAPUR_POP_2011:,}")
# print(f"  Solapur 2024 estimated:  {SOLAPUR_POP_2024:,}")

# # Get unique zones from pipeline data
# unique_zones = [z for z in pipes_df["zone_id"].unique() if z != "zone_unknown"]
# n_zones      = len(unique_zones)
# print(f"  Zones found: {n_zones} — {unique_zones}")

# # Distribute population equally per zone (simplified — no ward-level census)
# pop_per_zone  = SOLAPUR_POP_2024 / max(n_zones, 1)
# daily_lps     = (pop_per_zone * CPHEEO_LPCD) / SECONDS_PER_DAY

# # Count pipe segments per zone to understand zone size
# zone_pipe_counts = pipes_df.groupby("zone_id").size().reset_index(name="pipe_count")
# total_pipes       = zone_pipe_counts["pipe_count"].sum()

# zone_demand_rows = []
# for _, row in zone_pipe_counts.iterrows():
#     zone = row["zone_id"]
#     if zone == "zone_unknown":
#         continue
#     # Weight demand by proportion of pipes in zone
#     weight     = row["pipe_count"] / total_pipes
#     zone_pop   = int(SOLAPUR_POP_2024 * weight)
#     zone_daily = (zone_pop * CPHEEO_LPCD) / SECONDS_PER_DAY

#     zone_demand_rows.append({
#         "zone_id":                zone,
#         "estimated_population":   zone_pop,
#         "daily_demand_litres":    int(zone_pop * CPHEEO_LPCD),
#         "base_lps":               round(zone_daily, 4),
#         "peak_morning_lps":       round(zone_daily * 2.5, 4),    # 6–8 AM: ×2.5
#         "peak_evening_lps":       round(zone_daily * 2.0, 4),    # 5–8 PM: ×2.0
#         "offpeak_lps":            round(zone_daily * 0.05, 4),   # Night:  ×0.05
#         "pipe_count":             row["pipe_count"],
#     })

# zone_demand_df = pd.DataFrame(zone_demand_rows)
# print(f"  ✓  Zone demand table created: {len(zone_demand_df)} zones")
# print(zone_demand_df[["zone_id","estimated_population","base_lps","peak_morning_lps"]].to_string(index=False))


# # ─────────────────────────────────────────────────────────────
# # STEP 6 — Age and material assumptions
# # Add assumed_age, design_lifespan, hw_c_value to pipe_segments
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 6 — Age & material assumptions...")

# # Material lookup table (from Architecture Bible)
# MATERIAL_SPECS = {
#     "CI":      {"assumed_age_years": 35, "design_lifespan_years": 50,  "hw_c_value": 100},
#     "DI":      {"assumed_age_years": 15, "design_lifespan_years": 60,  "hw_c_value": 130},
#     "PVC":     {"assumed_age_years": 10, "design_lifespan_years": 25,  "hw_c_value": 150},
#     "GI":      {"assumed_age_years": 30, "design_lifespan_years": 40,  "hw_c_value": 100},
#     "AC":      {"assumed_age_years": 40, "design_lifespan_years": 50,  "hw_c_value": 100},
#     "MS":      {"assumed_age_years": 25, "design_lifespan_years": 45,  "hw_c_value": 110},
#     "Unknown": {"assumed_age_years": 35, "design_lifespan_years": 50,  "hw_c_value": 100},  # conservative = CI
# }

# def get_mat_spec(material, field):
#     spec = MATERIAL_SPECS.get(material, MATERIAL_SPECS["Unknown"])
#     return spec[field]

# pipes_df["assumed_age_years"]      = pipes_df["material"].apply(lambda m: get_mat_spec(m, "assumed_age_years"))
# pipes_df["design_lifespan_years"]  = pipes_df["material"].apply(lambda m: get_mat_spec(m, "design_lifespan_years"))
# pipes_df["hw_c_value"]             = pipes_df["material"].apply(lambda m: get_mat_spec(m, "hw_c_value"))

# # Material distribution summary
# mat_dist = pipes_df["material"].value_counts()
# print("  Material distribution:")
# for mat, count in mat_dist.items():
#     spec = MATERIAL_SPECS.get(mat, MATERIAL_SPECS["Unknown"])
#     print(f"    {mat:10s}: {count:6d} pipes | age={spec['assumed_age_years']}yr | lifespan={spec['design_lifespan_years']}yr | HW-C={spec['hw_c_value']}")


# # ─────────────────────────────────────────────────────────────
# # STEP 7 — Parse point infrastructure into DataFrames
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 7 — Parsing point infrastructure...")

# def parse_points(geojson_data, label):
#     if geojson_data is None:
#         return pd.DataFrame()
#     rows = []
#     for feat in geojson_data["features"]:
#         geom = feat["geometry"]
#         if geom["type"] != "Point":
#             continue
#         rows.append({
#             "feature_type": label,
#             "lon": geom["coordinates"][0],
#             "lat": geom["coordinates"][1],
#             **feat["properties"]
#         })
#     return pd.DataFrame(rows)

# water_sources_df = parse_points(water_source_data, "water_source")
# storage_tanks_df = parse_points(storage_tank_data, "storage_tank")
# raw_stations_df  = parse_points(raw_station_data,  "raw_station")

# print(f"  ✓  Water sources: {len(water_sources_df)}")
# print(f"  ✓  Storage tanks: {len(storage_tanks_df)}")
# print(f"  ✓  Raw stations:  {len(raw_stations_df)}")


# # ─────────────────────────────────────────────────────────────
# # STEP 8 — Save all CSVs
# # ─────────────────────────────────────────────────────────────
# print("\n[V1] Step 8 — Saving CSV files to Data/...")

# # pipe_segments.csv — used by V3 (hw_c_value) and V6 (age/lifespan)
# pipes_df.to_csv(os.path.join(OUT, "pipe_segments.csv"), index=False)
# print(f"  ✓  pipe_segments.csv           ({len(pipes_df)} rows)")

# # zone_demand.csv — used by V3 and V5
# zone_demand_df.to_csv(os.path.join(OUT, "zone_demand.csv"), index=False)
# print(f"  ✓  zone_demand.csv             ({len(zone_demand_df)} rows)")

# # point_features.csv — all infrastructure points combined
# all_points = pd.concat([water_sources_df, storage_tanks_df, raw_stations_df], ignore_index=True)
# if not all_points.empty:
#     all_points.to_csv(os.path.join(OUT, "infrastructure_points.csv"), index=False)
#     print(f"  ✓  infrastructure_points.csv   ({len(all_points)} rows)")

# # V1 completion summary
# print("\n" + "=" * 62)
# print("  V1 COMPLETE — Files saved to Data/")
# print("=" * 62)
# print(f"  pipe_segments.csv     → {len(pipes_df)} segments with age + HW-C")
# print(f"  zone_demand.csv       → {len(zone_demand_df)} zones with demand estimates")
# print(f"  nodes_with_elevation.csv → {len(nodes_df)} nodes (already existed)")
# print(f"  infrastructure_points.csv → {len(all_points)} infra points")
# print()
# print("  ✓  V1 outputs ready. V3 can now run.")
# print("  Next step:  python scripts/simulation_engine.py")


"""
v1_data_foundation.py  ·  place in: scripts/
V1 — Data Foundation
Team Devsters · Hydro-Equity Engine · SAMVED-2026

What this does:
  1. Parses ALL 4 GeoJSON files (pipeline, water_source, storage_tank, raw_station)
  2. Cleans the pipeline data (standardizes zones, materials, flags nulls)
  3. Builds pipe_segments table with age/material assumptions (needed by V6)
  4. Builds zone_demand table (needed by V3 and V5)
  5. Saves everything to CSV files in Data/

Run:  python scripts/v1_data_foundation.py
"""

import json
import os
import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "Data")
OUT  = DATA   # save outputs back into Data/ so all scripts find them easily

print("=" * 62)
print("  V1 · Data Foundation · Hydro-Equity Engine")
print("=" * 62)


# ─────────────────────────────────────────────────────────────
# STEP 1 — Parse all GeoJSON files
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 1 — Parsing GeoJSON files...")

def load_geojson(filename):
    path = os.path.join(DATA, filename)
    if not os.path.exists(path):
        print(f"  ⚠  Not found: {filename}")
        return None
    with open(path) as f:
        data = json.load(f)
    print(f"  ✓  {filename}: {len(data['features'])} features")
    return data

pipeline_data    = load_geojson("pipeline.geojson")
water_source_data = load_geojson("water_source.geojson")
storage_tank_data = load_geojson("storage_tank.geojson")
raw_station_data  = load_geojson("raw_station.geojson")


# ─────────────────────────────────────────────────────────────
# STEP 2 — Clean and standardise pipeline data
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 2 — Cleaning pipeline data...")

def standardise_zone(z):
    """Convert 'Zone 5', '5', 'Z5', 'zone_5' → 'zone_5'"""
    if pd.isna(z) or str(z).strip() == "":
        return "zone_unknown"
    z = str(z).strip().lower()
    z = z.replace("zone ", "zone_").replace("z", "zone_", 1) if z.startswith("z") else z
    # Remove extra spaces
    z = "_".join(z.split())
    # Ensure format is zone_N
    if not z.startswith("zone_"):
        z = "zone_" + z
    return z

def standardise_material(m):
    """Convert GIS full-name formats to short codes.
    
    SMC GIS stores materials as 'CAST IRON (CI)', 'DUCTILE IRON (DI)', etc.
    Exact-match lookup first; substring fallback catches any remaining variants.
    """
    if pd.isna(m) or str(m).strip() == "":
        return "Unknown"
    m = str(m).strip().upper()

    # Exact matches (short codes + full GIS names)
    if m in ["CI", "C.I.", "CAST IRON", "CASTIRON", "CAST IRON (CI)"]:
        return "CI"
    if m in ["DI", "D.I.", "DUCTILE IRON", "DUCTILEIRON", "DUCTILE IRON (DI)"]:
        return "DI"
    if m in ["PVC", "UPVC", "CPVC", "POLYVINYL CHLORIDE",
             "POLY VINYL CHLORIDE", "POLY VINYL CHLORIDE (PVC)"]:
        return "PVC"
    if m in ["GI", "GALVANIZED IRON", "GALVANISED IRON", "GALVANISED IRON (GI)"]:
        return "GI"
    if m in ["AC", "ASBESTOS CEMENT", "ASBESTOS", "ASBESTOS CEMENT (AC)"]:
        return "AC"
    if m in ["MS", "MILD STEEL", "STEEL", "MILD STEEL (MS)"]:
        return "MS"
    if m in ["CC", "CEMENT CONCRETE", "CEMENT CONCRETE (CC)", "RCC", "CONCRETE"]:
        return "CC"

    # Substring fallback — handles any format with the material name embedded
    if "CAST IRON"       in m or "(CI)" in m: return "CI"
    if "DUCTILE"         in m or "(DI)" in m: return "DI"
    if "POLY VINYL"      in m or "(PVC)" in m: return "PVC"
    if "GALVANI"         in m or "(GI)" in m: return "GI"
    if "ASBESTOS"        in m or "(AC)" in m: return "AC"
    if "MILD STEEL"      in m or "(MS)" in m: return "MS"
    if "CEMENT CONCRETE" in m or "(CC)" in m: return "CC"

    return "Unknown"

# Build pipe_rows from pipeline GeoJSON
diam_key = None
pipe_rows = []

for i, feat in enumerate(pipeline_data["features"]):
    props  = feat["properties"]
    geom   = feat["geometry"]

    # Auto-detect diameter key (handles both "Diameter(m)" and "Diameter(m")
    if diam_key is None:
        for k in props:
            if "diameter" in k.lower():
                diam_key = k
                break
        if diam_key:
            print(f"  ℹ  Detected diameter key: '{diam_key}'")

    coords = geom["coordinates"][0]   # MultiLineString → first line
    start  = coords[0]
    end    = coords[-1]

    raw_diam = props.get(diam_key, None) if diam_key else None
    raw_zone = props.get("Water Zone", props.get("WaterZone", props.get("Zone", None)))
    raw_mat  = props.get("Material", props.get("material", None))
    raw_len  = props.get("Length(m)", props.get("length", None))

    # Data quality flag
    quality = "complete"
    if raw_diam is None or str(raw_diam).strip() in ["", "None", "null"]:
        quality = "missing_diameter"
    if raw_zone is None or str(raw_zone).strip() in ["", "None", "null"]:
        quality = "missing_zone" if quality == "complete" else quality + "+missing_zone"

    pipe_rows.append({
        "segment_id":       i,
        "start_lon":        round(start[0], 6),
        "start_lat":        round(start[1], 6),
        "end_lon":          round(end[0],   6),
        "end_lat":          round(end[1],   6),
        "material":         standardise_material(raw_mat),
        "diameter_m":       max(float(raw_diam) if raw_diam and str(raw_diam).strip() not in ["", "None"] else 0.05, 0.05),
        "length_m":         max(float(raw_len)  if raw_len  and str(raw_len).strip()  not in ["", "None"] else 1.0,  1.0),
        "zone_id":          standardise_zone(raw_zone),
        "pipeline_type":    str(props.get("Pipeline T", props.get("PipelineType", "Unknown"))),
        "data_quality_flag": quality,
    })

pipes_df = pd.DataFrame(pipe_rows)

print(f"  ✓  Total pipe segments:  {len(pipes_df)}")
print(f"  ✓  Complete records:     {(pipes_df['data_quality_flag']=='complete').sum()}")
print(f"  ⚠  Missing diameter:    {pipes_df['data_quality_flag'].str.contains('missing_diameter').sum()}")
print(f"  ⚠  Missing zone:        {pipes_df['data_quality_flag'].str.contains('missing_zone').sum()}")
print(f"  ✓  Unique zones found:   {sorted(pipes_df['zone_id'].unique())}")
print(f"  ✓  Materials found:      {sorted(pipes_df['material'].unique())}")


# ─────────────────────────────────────────────────────────────
# STEP 3 — Extract pipe junction nodes
# (nodes_with_elevation.csv already exists with simulated elevation)
# We just verify it and add zone info if possible
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 3 — Verifying nodes_with_elevation.csv...")

nodes_path = os.path.join(DATA, "nodes_with_elevation.csv")
if os.path.exists(nodes_path):
    nodes_df = pd.read_csv(nodes_path)
    nodes_df["lon"] = nodes_df["lon"].round(6)
    nodes_df["lat"] = nodes_df["lat"].round(6)
    print(f"  ✓  Nodes loaded: {len(nodes_df)} nodes")
    print(f"  ✓  Elevation range: {nodes_df['elevation'].min():.1f}–{nodes_df['elevation'].max():.1f} m (simulated)")
else:
    print("  ✗  nodes_with_elevation.csv not found! Run load_data.py first.")
    exit(1)


# ─────────────────────────────────────────────────────────────
# STEP 4 — Elevation already simulated (440–470m)
# No API call needed
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 4 — Elevation: using simulated values (440–470m) ✓")


# ─────────────────────────────────────────────────────────────
# STEP 5 — Zone demand estimation
# Using Census 2011 Solapur population + CPHEEO 135L/person/day
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 5 — Zone demand estimation...")

# Solapur Municipal Corporation total population (Census 2011): ~951,558
# Estimate 2024 population: × (1.012)^13 ≈ 1.17 growth factor
SOLAPUR_POP_2011      = 951558
GROWTH_FACTOR         = (1.012) ** 13          # 13 years from 2011 to 2024
SOLAPUR_POP_2024      = int(SOLAPUR_POP_2011 * GROWTH_FACTOR)
CPHEEO_LPCD           = 135                    # litres per capita per day
SECONDS_PER_DAY       = 86400

print(f"  Solapur 2011 population: {SOLAPUR_POP_2011:,}")
print(f"  Solapur 2024 estimated:  {SOLAPUR_POP_2024:,}")

# Get unique zones from pipeline data
unique_zones = [z for z in pipes_df["zone_id"].unique() if z != "zone_unknown"]
n_zones      = len(unique_zones)
print(f"  Zones found: {n_zones} — {unique_zones}")

# Distribute population equally per zone (simplified — no ward-level census)
pop_per_zone  = SOLAPUR_POP_2024 / max(n_zones, 1)
daily_lps     = (pop_per_zone * CPHEEO_LPCD) / SECONDS_PER_DAY

# Count pipe segments per zone to understand zone size
zone_pipe_counts = pipes_df.groupby("zone_id").size().reset_index(name="pipe_count")
total_pipes       = zone_pipe_counts["pipe_count"].sum()

zone_demand_rows = []
for _, row in zone_pipe_counts.iterrows():
    zone = row["zone_id"]
    if zone == "zone_unknown":
        continue
    # Weight demand by proportion of pipes in zone
    weight     = row["pipe_count"] / total_pipes
    zone_pop   = int(SOLAPUR_POP_2024 * weight)
    zone_daily = (zone_pop * CPHEEO_LPCD) / SECONDS_PER_DAY

    zone_demand_rows.append({
        "zone_id":                zone,
        "estimated_population":   zone_pop,
        "daily_demand_litres":    int(zone_pop * CPHEEO_LPCD),
        "base_lps":               round(zone_daily, 4),
        "peak_morning_lps":       round(zone_daily * 2.5, 4),    # 6–8 AM: ×2.5
        "peak_evening_lps":       round(zone_daily * 2.0, 4),    # 5–8 PM: ×2.0
        "offpeak_lps":            round(zone_daily * 0.05, 4),   # Night:  ×0.05
        "pipe_count":             row["pipe_count"],
    })

zone_demand_df = pd.DataFrame(zone_demand_rows)
print(f"  ✓  Zone demand table created: {len(zone_demand_df)} zones")
print(zone_demand_df[["zone_id","estimated_population","base_lps","peak_morning_lps"]].to_string(index=False))


# ─────────────────────────────────────────────────────────────
# STEP 6 — Age and material assumptions
# Add assumed_age, design_lifespan, hw_c_value to pipe_segments
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 6 — Age & material assumptions...")

# Material lookup table (from Architecture Bible)
MATERIAL_SPECS = {
    "CI":      {"assumed_age_years": 35, "design_lifespan_years": 50,  "hw_c_value": 100},
    "DI":      {"assumed_age_years": 15, "design_lifespan_years": 60,  "hw_c_value": 130},
    "PVC":     {"assumed_age_years": 10, "design_lifespan_years": 25,  "hw_c_value": 150},
    "GI":      {"assumed_age_years": 30, "design_lifespan_years": 40,  "hw_c_value": 100},
    "AC":      {"assumed_age_years": 40, "design_lifespan_years": 50,  "hw_c_value": 100},
    "MS":      {"assumed_age_years": 25, "design_lifespan_years": 45,  "hw_c_value": 110},
    "CC":      {"assumed_age_years": 40, "design_lifespan_years": 60,  "hw_c_value": 90},   # Cement Concrete
    "Unknown": {"assumed_age_years": 35, "design_lifespan_years": 50,  "hw_c_value": 100},  # conservative = CI
}

def get_mat_spec(material, field):
    spec = MATERIAL_SPECS.get(material, MATERIAL_SPECS["Unknown"])
    return spec[field]

pipes_df["assumed_age_years"]      = pipes_df["material"].apply(lambda m: get_mat_spec(m, "assumed_age_years"))
pipes_df["design_lifespan_years"]  = pipes_df["material"].apply(lambda m: get_mat_spec(m, "design_lifespan_years"))
pipes_df["hw_c_value"]             = pipes_df["material"].apply(lambda m: get_mat_spec(m, "hw_c_value"))

# Material distribution summary
mat_dist = pipes_df["material"].value_counts()
print("  Material distribution:")
for mat, count in mat_dist.items():
    spec = MATERIAL_SPECS.get(mat, MATERIAL_SPECS["Unknown"])
    print(f"    {mat:10s}: {count:6d} pipes | age={spec['assumed_age_years']}yr | lifespan={spec['design_lifespan_years']}yr | HW-C={spec['hw_c_value']}")


# ─────────────────────────────────────────────────────────────
# STEP 7 — Parse point infrastructure into DataFrames
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 7 — Parsing point infrastructure...")

def parse_points(geojson_data, label):
    """Extract point locations from GeoJSON.
    
    Handles:
      - Point        → direct coordinates
      - MultiPolygon → centroid of all rings (SMC water_source.geojson is MultiPolygon)
      - Polygon      → centroid of outer ring
    """
    if geojson_data is None:
        return pd.DataFrame()
    rows = []
    for feat in geojson_data["features"]:
        geom = feat["geometry"]
        lon, lat = None, None

        if geom["type"] == "Point":
            lon = geom["coordinates"][0]
            lat = geom["coordinates"][1]

        elif geom["type"] == "Polygon":
            # Centroid of outer ring
            ring = geom["coordinates"][0]
            lon = sum(c[0] for c in ring) / len(ring)
            lat = sum(c[1] for c in ring) / len(ring)

        elif geom["type"] == "MultiPolygon":
            # Centroid across all polygons' outer rings
            all_pts = [c for poly in geom["coordinates"] for c in poly[0]]
            lon = sum(c[0] for c in all_pts) / len(all_pts)
            lat = sum(c[1] for c in all_pts) / len(all_pts)

        if lon is not None:
            rows.append({
                "feature_type": label,
                "lon": lon,
                "lat": lat,
                **feat["properties"]
            })
    return pd.DataFrame(rows)

water_sources_df = parse_points(water_source_data, "water_source")
storage_tanks_df = parse_points(storage_tank_data, "storage_tank")
raw_stations_df  = parse_points(raw_station_data,  "raw_station")

print(f"  ✓  Water sources: {len(water_sources_df)}")
print(f"  ✓  Storage tanks: {len(storage_tanks_df)}")
print(f"  ✓  Raw stations:  {len(raw_stations_df)}")


# ─────────────────────────────────────────────────────────────
# STEP 8 — Save all CSVs
# ─────────────────────────────────────────────────────────────
print("\n[V1] Step 8 — Saving CSV files to Data/...")

# pipe_segments.csv — used by V3 (hw_c_value) and V6 (age/lifespan)
pipes_df.to_csv(os.path.join(OUT, "pipe_segments.csv"), index=False)
print(f"  ✓  pipe_segments.csv           ({len(pipes_df)} rows)")

# zone_demand.csv — used by V3 and V5
zone_demand_df.to_csv(os.path.join(OUT, "zone_demand.csv"), index=False)
print(f"  ✓  zone_demand.csv             ({len(zone_demand_df)} rows)")

# point_features.csv — all infrastructure points combined
all_points = pd.concat([water_sources_df, storage_tanks_df, raw_stations_df], ignore_index=True)
if not all_points.empty:
    all_points.to_csv(os.path.join(OUT, "infrastructure_points.csv"), index=False)
    print(f"  ✓  infrastructure_points.csv   ({len(all_points)} rows)")

# V1 completion summary
print("\n" + "=" * 62)
print("  V1 COMPLETE — Files saved to Data/")
print("=" * 62)
print(f"  pipe_segments.csv     → {len(pipes_df)} segments with age + HW-C")
print(f"  zone_demand.csv       → {len(zone_demand_df)} zones with demand estimates")
print(f"  nodes_with_elevation.csv → {len(nodes_df)} nodes (already existed)")
print(f"  infrastructure_points.csv → {len(all_points)} infra points")
print()
print("  ✓  V1 outputs ready. V3 can now run.")
print("  Next step:  python scripts/simulation_engine.py")