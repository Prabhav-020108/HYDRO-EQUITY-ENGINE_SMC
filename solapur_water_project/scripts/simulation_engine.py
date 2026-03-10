# """
# simulation_engine.py  ·  place in: scripts/
# V3 — Network Graph & Hydraulic Simulation
# Team Devsters · Hydro-Equity Engine · SAMVED-2026

# Run directly:   python scripts/simulation_engine.py
# Import from backend:
#     import sys; sys.path.insert(0, '.')
#     from scripts.simulation_engine import build_model, run_simulation
# """

# import json, copy, os
# import pandas as pd
# import numpy as np
# import networkx as nx
# import wntr

# # ── Paths relative to project root ─────────────────────────────
# ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DATA = os.path.join(ROOT, "Data")
# OUT  = os.path.join(ROOT, "outputs")
# os.makedirs(OUT, exist_ok=True)

# # ─────────────────────────────────────────────────────────────
# # ★ ZONE FILTER — ONLY CHANGE FROM PREVIOUS VERSION
# #   Running the full city (10,160 pipes) causes disconnected
# #   components which crash EPANET. Zone 5 has 2,311 pipes and
# #   is the largest connected zone — ideal for demo.
# #   Change to "all" only when you want the full (slow) run.
# # ─────────────────────────────────────────────────────────────
# DEMO_ZONE = "5"    # ← set to "all" to run every zone (slow, may crash)


# # ─────────────────────────────────────────────────────────────
# # DEMAND PATTERN — 96 timesteps × 15 min = 24 hours
# # SHARED WITH V5 — NFA signal depends on the 0.05× night value
# # ─────────────────────────────────────────────────────────────
# DEMAND_PATTERN = []
# for _h in range(24):
#     if   0  <= _h < 6:   _m = 0.05
#     elif 6  <= _h < 8:   _m = 2.50
#     elif 8  <= _h < 17:  _m = 1.00
#     elif 17 <= _h < 20:  _m = 2.00
#     else:                _m = 0.05
#     DEMAND_PATTERN.extend([_m] * 4)
# assert len(DEMAND_PATTERN) == 96


# def _find_node_id(coord, nodes_df):
#     lon = round(coord[0], 6)
#     lat = round(coord[1], 6)
#     row = nodes_df[(nodes_df["lon"] == lon) & (nodes_df["lat"] == lat)]
#     return row.index[0] if len(row) > 0 else None


# def _load_points(geojson_path, label):
#     if not os.path.exists(geojson_path):
#         print(f"[V3] ⚠  Not found: {geojson_path}")
#         return pd.DataFrame()
#     with open(geojson_path) as f:
#         data = json.load(f)
#     rows = []
#     for feat in data.get("features", []):
#         geom = feat["geometry"]
#         if geom["type"] != "Point":
#             continue
#         rows.append({
#             "lon": geom["coordinates"][0],
#             "lat": geom["coordinates"][1],
#             "label": label,
#             **feat["properties"]
#         })
#     df = pd.DataFrame(rows)
#     print(f"[V3] Loaded {len(df):>4} {label} features")
#     return df


# # ─────────────────────────────────────────────────────────────
# # STEP 1 — Build NetworkX DiGraph (shared with V4, V5, V7)
# # ─────────────────────────────────────────────────────────────
# def build_graph(nodes_df, pipes_df):
#     """Directed graph — MUST be DiGraph, not Graph (V4 and V7 need directed paths)."""
#     G = nx.DiGraph()
#     for idx, row in nodes_df.iterrows():
#         G.add_node(f"J{idx}",
#                    lat=float(row["lat"]),
#                    lon=float(row["lon"]),
#                    elevation=float(row["elevation"]),
#                    zone=str(row.get("zone", "unknown")))
#     for idx, row in pipes_df.iterrows():
#         s = f"J{int(row['start_node'])}"
#         e = f"J{int(row['end_node'])}"
#         if G.has_node(s) and G.has_node(e):
#             G.add_edge(s, e,
#                        segment_id=f"P{idx}",
#                        diameter=float(row["diameter"]),
#                        length=float(row["length"]),
#                        material=str(row.get("material", "Unknown")),
#                        hw_c=120)
#     print(f"[V3] Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
#     comps = list(nx.weakly_connected_components(G))
#     if len(comps) > 1:
#         largest = len(max(comps, key=len))
#         print(f"[V3] ⚠  {len(comps)} disconnected components — largest: {largest} nodes")
#     else:
#         print(f"[V3] ✓  Graph is fully connected")
#     return G


# # ─────────────────────────────────────────────────────────────
# # STEP 2 — Build WNTR Model
# # ─────────────────────────────────────────────────────────────
# def build_model(zone_filter=DEMO_ZONE):
#     """
#     Builds a WNTR model filtered to one zone for stability.
#     zone_filter: "5" for Zone 5 only, "all" for full city.

#     Returns: (wn, nodes_df, pipes_df, connected_ids, G,
#                water_sources_df, storage_tanks_df, raw_stations_df)
#     """
#     print(f"\n[V3] ── Building model (zone_filter='{zone_filter}') ─────────────")

#     # Load raw data
#     with open(os.path.join(DATA, "pipeline.geojson")) as f:
#         pipeline_raw = json.load(f)

#     nodes_df = pd.read_csv(os.path.join(DATA, "nodes_with_elevation.csv"))
#     nodes_df["lon"] = nodes_df["lon"].round(6)
#     nodes_df["lat"] = nodes_df["lat"].round(6)

#     water_sources_df = _load_points(os.path.join(DATA, "water_source.geojson"), "water_source")
#     storage_tanks_df = _load_points(os.path.join(DATA, "storage_tank.geojson"), "storage_tank")
#     raw_stations_df  = _load_points(os.path.join(DATA, "raw_station.geojson"),  "raw_station")

#     # ── ★ ZONE FILTER (the key fix for stability) ──────────────
#     if zone_filter == "all":
#         features = pipeline_raw["features"]
#         print(f"[V3] Using ALL {len(features)} pipe features")
#     else:
#         features = [
#             f for f in pipeline_raw["features"]
#             if str(f["properties"].get("Water Zone", "")).strip() == zone_filter
#         ]
#         print(f"[V3] Filtered to Zone {zone_filter}: {len(features)} pipe features")

#     if len(features) == 0:
#         print(f"[V3] ✗  No features found for zone '{zone_filter}'! Check zone name.")
#         return None

#     # Build pipes DataFrame
#     pipes = []
#     for feat in features:
#         props  = feat["properties"]
#         coords = feat["geometry"]["coordinates"][0]
#         diam   = float(props.get("Diameter(m", props.get("Diameter(m)", 0.05)) or 0.05)
#         length = float(props.get("Length(m)",  1.0) or 1.0)
#         pipes.append({
#             "start":    coords[0],
#             "end":      coords[-1],
#             "length":   max(length, 1.0),
#             "diameter": max(diam,   0.05),
#             "material": str(props.get("Material", "Unknown")),
#             "zone":     str(props.get("Water Zone", "unknown")),
#         })
#     pipes_df = pd.DataFrame(pipes)

#     # Match pipe endpoints to node IDs
#     pipes_df["start_node"] = pipes_df["start"].apply(lambda c: _find_node_id(c, nodes_df))
#     pipes_df["end_node"]   = pipes_df["end"].apply(lambda c: _find_node_id(c, nodes_df))
#     pipes_df = pipes_df.dropna(subset=["start_node", "end_node"])
#     pipes_df["start_node"] = pipes_df["start_node"].astype(int)
#     pipes_df["end_node"]   = pipes_df["end_node"].astype(int)
#     pipes_df = pipes_df[pipes_df["start_node"] != pipes_df["end_node"]]

#     connected_ids = set(pipes_df["start_node"]) | set(pipes_df["end_node"])
#     print(f"[V3] Valid pipes: {len(pipes_df)} | Connected nodes: {len(connected_ids)}")

#     conn_nodes_df = nodes_df[nodes_df.index.isin(connected_ids)]

#     # Build NetworkX graph
#     G = build_graph(conn_nodes_df, pipes_df)

#     # ── WNTR Water Network Model ────────────────────────────────
#     wn = wntr.network.WaterNetworkModel()

#     # Time settings: 24 hours at 15-minute intervals = 96 timesteps
#     wn.options.time.duration           = 86400   # 24 hours in seconds
#     wn.options.time.hydraulic_timestep = 900     # 15 minutes in seconds
#     wn.options.time.report_timestep    = 900

#     # Hydraulic settings
#     wn.options.hydraulic.demand_model      = "PDD"   # Pressure-Driven Demand (correct for intermittent supply)
#     wn.options.hydraulic.required_pressure = 10      # Minimum acceptable pressure (metres)
#     wn.options.hydraulic.minimum_pressure  = 0
#     wn.options.hydraulic.headloss          = "H-W"   # Hazen-Williams

#     # Add demand pattern (shared with V5)
#     wn.add_pattern("DailyPattern", DEMAND_PATTERN)

#     # Add junction nodes — ONLY connected nodes (isolated nodes crash EPANET)
#     for idx, row in conn_nodes_df.iterrows():
#         wn.add_junction(
#             name=f"J{idx}",
#             base_demand=0.001,            # small base demand per node (m³/s)
#             demand_pattern="DailyPattern",
#             elevation=float(row["elevation"])
#         )

#     # Add pipes — skip duplicate node-pairs (same-direction pair only once)
#     added_pairs = set()
#     pipes_added = 0
#     for idx, row in pipes_df.iterrows():
#         pair = tuple(sorted([row["start_node"], row["end_node"]]))
#         if pair in added_pairs:
#             continue
#         added_pairs.add(pair)
#         wn.add_pipe(
#             name=f"P{idx}",
#             start_node_name=f"J{int(row['start_node'])}",
#             end_node_name=f"J{int(row['end_node'])}",
#             length=float(row["length"]),
#             diameter=float(row["diameter"]),
#             roughness=120,      # Hazen-Williams C (will be overridden below per material)
#             minor_loss=0.0,
#             initial_status="OPEN"
#         )
#         pipes_added += 1
#     print(f"[V3] WNTR pipes added: {pipes_added}")

#     # ── Add water sources as reservoirs ────────────────────────
#     res_count = 0

#     # Try to use real water source locations from GeoJSON
#     if not water_sources_df.empty:
#         for i, src in water_sources_df.head(3).iterrows():
#             # Find the closest node in the connected set
#             dists = conn_nodes_df.apply(
#                 lambda r: abs(r["lon"] - src["lon"]) + abs(r["lat"] - src["lat"]), axis=1
#             )
#             nidx  = dists.idxmin()
#             nelev = float(conn_nodes_df.loc[nidx, "elevation"])
#             wn.add_reservoir(f"WaterSource_{res_count}", base_head=nelev + 20)
#             wn.add_pipe(
#                 name=f"SourcePipe_{res_count}",
#                 start_node_name=f"WaterSource_{res_count}",
#                 end_node_name=f"J{nidx}",
#                 length=10, diameter=0.5, roughness=120,
#                 minor_loss=0.0, initial_status="OPEN"
#             )
#             res_count += 1
#         print(f"[V3] Added {res_count} real water-source reservoirs")

#     # Always add a fallback reservoir at the highest-elevation node
#     # (guarantees at least one supply point even if zone has no nearby water source)
#     hi_idx  = conn_nodes_df["elevation"].idxmax()
#     hi_elev = float(conn_nodes_df.loc[hi_idx, "elevation"])
#     wn.add_reservoir("Source_Fallback", base_head=hi_elev + 20)
#     wn.add_pipe(
#         name="SourcePipe_Fallback",
#         start_node_name="Source_Fallback",
#         end_node_name=f"J{hi_idx}",
#         length=10, diameter=0.5, roughness=120,
#         minor_loss=0.0, initial_status="OPEN"
#     )
#     print(f"[V3] Fallback reservoir → J{hi_idx} (elevation={hi_elev:.1f}m, head={hi_elev+20:.1f}m)")

#     total_junctions = len(wn.junction_name_list)
#     total_pipes_wntr = len(wn.pipe_name_list)
#     print(f"[V3] WNTR model: {total_junctions} junctions, {total_pipes_wntr} pipes, {len(wn.reservoir_name_list)} reservoirs")

#     return (wn, nodes_df, pipes_df, connected_ids, G,
#             water_sources_df, storage_tanks_df, raw_stations_df)


# # ─────────────────────────────────────────────────────────────
# # STEP 3 — Run Simulation (saves pressure + flow CSVs)
# # ─────────────────────────────────────────────────────────────
# def run_simulation(wn, scenario_label="baseline", save_csv=True):
#     """
#     Runs WNTR EpanetSimulator. Returns (pressure_df, flow_df).
#     pressure_df: rows=timesteps (0-95), columns=node names (J0, J1, ...)
#     flow_df:     rows=timesteps (0-95), columns=pipe names (P0, P1, ...)
#     """
#     print(f"\n[V3] Running simulation: {scenario_label} ...")
#     try:
#         sim     = wntr.sim.EpanetSimulator(wn)
#         results = sim.run_sim()

#         p_df = results.node["pressure"]   # DataFrame: timesteps × nodes
#         f_df = results.link["flowrate"]   # DataFrame: timesteps × pipes

#         print(f"[V3] ✓ {scenario_label}: {len(p_df)} timesteps × {len(p_df.columns)} nodes")

#         # Validation checks (from Bible Step 4)
#         p_mean = p_df.mean()
#         neg_count = (p_mean < 0).sum()
#         if neg_count > 0:
#             print(f"[V3] ⚠  {neg_count} nodes have negative avg pressure — consider reducing demands")
#         else:
#             print(f"[V3] ✓  No negative pressures — validation passed")

#         print(f"[V3]    Pressure range: {p_mean.min():.1f} – {p_mean.max():.1f} m (avg across timesteps)")

#         if save_csv:
#             p_path = os.path.join(OUT, f"pressure_{scenario_label}.csv")
#             f_path = os.path.join(OUT, f"flow_{scenario_label}.csv")
#             p_df.to_csv(p_path)
#             f_df.to_csv(f_path)
#             print(f"[V3] ✓  Saved: outputs/pressure_{scenario_label}.csv")
#             print(f"[V3] ✓  Saved: outputs/flow_{scenario_label}.csv")

#         return p_df, f_df

#     except Exception as e:
#         print(f"[V3] ✗ {scenario_label} FAILED: {e}")
#         print("[V3]   Tip: If 'system unbalanced', reduce base_demand in build_model() from 0.001 to 0.0001")
#         return None, None


# # ─────────────────────────────────────────────────────────────
# # STEP 4 — Apply Scenario (deep copy — never mutates base model)
# # ─────────────────────────────────────────────────────────────
# def apply_scenario(wn_base, scenario_type, params=None):
#     """
#     Creates a modified copy of the model for each anomaly scenario.

#     scenario_type options:
#       "leak"         — reduce one mid-network pipe diameter by 30%
#       "valve_close"  — close one upstream pipe
#       "demand_surge" — multiply all demands by 1.5×
#     """
#     wn     = copy.deepcopy(wn_base)
#     params = params or {}

#     # Get regular pipe names (exclude source pipes)
#     pipes = [n for n in wn.pipe_name_list
#              if not n.startswith(("SourcePipe", "WaterSource"))]

#     if scenario_type == "leak":
#         # Pick a mid-network pipe as the leak location
#         target = params.get("pipe", pipes[len(pipes) // 2])
#         pipe   = wn.get_link(target)
#         orig   = pipe.diameter
#         pipe.diameter = orig * (1 - params.get("reduction", 0.30))
#         print(f"[V3] LEAK: {target} diameter {orig:.4f}→{pipe.diameter:.4f}m (−30%)")

#     elif scenario_type == "valve_close":
#         # Close a pipe near the start of the network
#         target = params.get("pipe", pipes[min(10, len(pipes) - 1)])
#         wn.get_link(target).initial_status = wntr.network.LinkStatus.Closed
#         print(f"[V3] VALVE_CLOSE: {target} set to Closed")

#     elif scenario_type == "demand_surge":
#         # Multiply all junction demands by 1.5×
#         surge = params.get("surge_factor", 1.5)
#         for jn in wn.junction_name_list:
#             for d in wn.get_node(jn).demand_timeseries_list:
#                 d.base_value *= surge
#         print(f"[V3] DEMAND_SURGE: all junctions × {surge}")

#     return wn


# # ─────────────────────────────────────────────────────────────
# # STEP 5 — Run All 3 Anomaly Scenarios
# # ─────────────────────────────────────────────────────────────
# def run_all_scenarios(wn_base):
#     """
#     Runs the 3 mandatory scenarios from the Architecture Bible.
#     Returns dict: {label: (pressure_df, flow_df)}
#     """
#     print("\n[V3] ── Running 3 anomaly scenarios ─────────────────────")
#     out = {}
#     scenarios = [
#         ("scenario_A_leak",         "leak"),
#         ("scenario_B_valve_close",  "valve_close"),
#         ("scenario_C_demand_surge", "demand_surge"),
#     ]
#     for label, stype in scenarios:
#         wn_scenario = apply_scenario(wn_base, stype)
#         p, f = run_simulation(wn_scenario, label, save_csv=True)
#         if p is not None:
#             out[label] = (p, f)
#         else:
#             print(f"[V3] ⚠  Scenario '{label}' failed — skipping")

#     print(f"\n[V3] ✓  Scenarios complete: {list(out.keys())}")
#     return out


# # ─────────────────────────────────────────────────────────────
# # STEP 6 — Load Cached CSV Results (used by app.py backend)
# # ─────────────────────────────────────────────────────────────
# def load_results(label="baseline"):
#     """Load saved pressure/flow CSVs. Returns (pressure_df, flow_df) or (None, None)."""
#     try:
#         p = pd.read_csv(os.path.join(OUT, f"pressure_{label}.csv"), index_col=0)
#         f = pd.read_csv(os.path.join(OUT, f"flow_{label}.csv"),     index_col=0)
#         print(f"[V3] Loaded results: pressure_{label}.csv ({len(p)} rows × {len(p.columns)} cols)")
#         return p, f
#     except FileNotFoundError:
#         return None, None


# # ─────────────────────────────────────────────────────────────
# # STEP 7 — Single-Timestep Stats (for backend API clock)
# # ─────────────────────────────────────────────────────────────
# def get_timestep_stats(pressure_df, flow_df, timestep=0):
#     """Returns a dict with pressure/flow summary for one timestep."""
#     if pressure_df is None:
#         return {}
#     t     = min(timestep, len(pressure_df) - 1)
#     p_row = pressure_df.iloc[t]
#     f_row = flow_df.iloc[t] if flow_df is not None else pd.Series(dtype=float)
#     h, m  = divmod(t * 15, 60)
#     return {
#         "timestep":          t,
#         "time_label":        f"{h:02d}:{m:02d}",
#         "demand_multiplier": DEMAND_PATTERN[t],
#         "pressure": {
#             "min":  round(float(p_row.min()),  2),
#             "max":  round(float(p_row.max()),  2),
#             "mean": round(float(p_row.mean()), 2),
#         },
#         "flowrate": {
#             "min":  round(float(f_row.abs().min()),  6) if len(f_row) else 0,
#             "max":  round(float(f_row.abs().max()),  6) if len(f_row) else 0,
#             "mean": round(float(f_row.abs().mean()), 6) if len(f_row) else 0,
#         }
#     }


# # ─────────────────────────────────────────────────────────────
# # MAIN — run with: python scripts/simulation_engine.py
# # ─────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     print("=" * 62)
#     print("  V3 · Hydro-Equity Engine · Hydraulic Simulation Engine")
#     print(f"  Zone filter: '{DEMO_ZONE}'")
#     print("=" * 62)

#     # Build model
#     result = build_model(zone_filter=DEMO_ZONE)
#     if result is None:
#         print("✗ Model build failed.")
#         exit(1)

#     (wn, nodes_df, pipes_df, connected_ids, G,
#      water_sources_df, storage_tanks_df, raw_stations_df) = result

#     # Save EPANET .inp file
#     inp_path = os.path.join(OUT, "solapur_network.inp")
#     wntr.network.write_inpfile(wn, inp_path)
#     print(f"\n[V3] Saved EPANET model: outputs/solapur_network.inp")

#     # Run baseline simulation
#     pressure_df, flow_df = run_simulation(wn, "baseline", save_csv=True)

#     if pressure_df is not None:
#         # Run the 3 mandatory anomaly scenarios
#         run_all_scenarios(wn)

#         # Print 24-hour summary
#         print("\n── 24-hour Pressure Summary ────────────────────────────────")
#         print(f"  {'Time':>6}  {'Mult':>5}  {'Min P':>7}  {'Max P':>7}  {'Avg P':>7}")
#         for t_i in [0, 24, 32, 48, 68, 80, 95]:
#             s = get_timestep_stats(pressure_df, flow_df, t_i)
#             p = s["pressure"]
#             print(f"  {s['time_label']:>6}  ×{s['demand_multiplier']:.2f}  "
#                   f"{p['min']:>7.1f}m  {p['max']:>7.1f}m  {p['mean']:>7.1f}m")

#         print("\n" + "=" * 62)
#         print("  ✓  V3 COMPLETE — All outputs saved to outputs/")
#         print("     pressure_baseline.csv")
#         print("     flow_baseline.csv")
#         print("     pressure_scenario_A_leak.csv")
#         print("     pressure_scenario_B_valve_close.csv")
#         print("     pressure_scenario_C_demand_surge.csv")
#         print("     solapur_network.inp")
#         print("=" * 62)
#         print("\n  Next step: python backend/app.py")
#     else:
#         print("\n✗ Baseline simulation failed.")
#         print("  Try: change DEMO_ZONE to a different zone number (1–8)")
#         print("  Or:  reduce base_demand in build_model() from 0.001 to 0.0001")

"""
simulation_engine.py  ·  place in: scripts/
V3 — Network Graph & Hydraulic Simulation
Team Devsters · Hydro-Equity Engine · SAMVED-2026

Run directly:   python scripts/simulation_engine.py
Import from backend:
    import sys; sys.path.insert(0, '.')
    from scripts.simulation_engine import build_model, run_simulation
"""

import json, copy, os
import pandas as pd
import numpy as np
import networkx as nx
import wntr

# ── Paths relative to project root ─────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "Data")
OUT  = os.path.join(ROOT, "outputs")
os.makedirs(OUT, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# ★ ZONE FILTER — ONLY CHANGE FROM PREVIOUS VERSION
#   Running the full city (10,160 pipes) causes disconnected
#   components which crash EPANET. Zone 5 has 2,311 pipes and
#   is the largest connected zone — ideal for demo.
#   Change to "all" only when you want the full (slow) run.
# ─────────────────────────────────────────────────────────────
DEMO_ZONE = "1"    # Zone 1 has largest LCC (820 pipes) — best for demo    # ← set to "all" to run every zone (slow, may crash)


# ─────────────────────────────────────────────────────────────
# DEMAND PATTERN — 96 timesteps × 15 min = 24 hours
# SHARED WITH V5 — NFA signal depends on the 0.05× night value
# ─────────────────────────────────────────────────────────────
DEMAND_PATTERN = []
for _h in range(24):
    if   0  <= _h < 6:   _m = 0.05
    elif 6  <= _h < 8:   _m = 2.50
    elif 8  <= _h < 17:  _m = 1.00
    elif 17 <= _h < 20:  _m = 2.00
    else:                _m = 0.05
    DEMAND_PATTERN.extend([_m] * 4)
assert len(DEMAND_PATTERN) == 96


def _find_node_id(coord, nodes_df):
    # 4-decimal rounding (~11m tolerance) dramatically improves connectivity.
    # 6-decimal rounding (~0.1m) causes most adjacent pipes to be disconnected
    # because GIS pipe endpoints don't share exact float coordinates.
    lon = round(coord[0], 4)
    lat = round(coord[1], 4)
    row = nodes_df[(nodes_df["lon"] == lon) & (nodes_df["lat"] == lat)]
    return row.index[0] if len(row) > 0 else None


def _load_points(geojson_path, label):
    """Load point locations from GeoJSON.
    Handles Point, Polygon, and MultiPolygon (centroid).
    SMC water_source.geojson is MultiPolygon — this fix resolves 0-features-loaded.
    """
    if not os.path.exists(geojson_path):
        print(f"[V3] ⚠  Not found: {geojson_path}")
        return pd.DataFrame()
    with open(geojson_path) as f:
        data = json.load(f)
    rows = []
    for feat in data.get("features", []):
        geom = feat["geometry"]
        lon, lat = None, None
        if geom["type"] == "Point":
            lon = geom["coordinates"][0]
            lat = geom["coordinates"][1]
        elif geom["type"] == "Polygon":
            ring = geom["coordinates"][0]
            lon = sum(c[0] for c in ring) / len(ring)
            lat = sum(c[1] for c in ring) / len(ring)
        elif geom["type"] == "MultiPolygon":
            all_pts = [c for poly in geom["coordinates"] for c in poly[0]]
            lon = sum(c[0] for c in all_pts) / len(all_pts)
            lat = sum(c[1] for c in all_pts) / len(all_pts)
        if lon is not None:
            rows.append({"lon": lon, "lat": lat, "label": label, **feat["properties"]})
    df = pd.DataFrame(rows)
    print(f"[V3] Loaded {len(df):>4} {label} features")
    return df


# ─────────────────────────────────────────────────────────────
# STEP 1 — Build NetworkX DiGraph (shared with V4, V5, V7)
# ─────────────────────────────────────────────────────────────
def build_graph(nodes_df, pipes_df):
    """Directed graph — MUST be DiGraph, not Graph (V4 and V7 need directed paths)."""
    G = nx.DiGraph()
    for idx, row in nodes_df.iterrows():
        G.add_node(f"J{idx}",
                   lat=float(row["lat"]),
                   lon=float(row["lon"]),
                   elevation=float(row["elevation"]),
                   zone=str(row.get("zone", "unknown")))
    for idx, row in pipes_df.iterrows():
        s = f"J{int(row['start_node'])}"
        e = f"J{int(row['end_node'])}"
        if G.has_node(s) and G.has_node(e):
            G.add_edge(s, e,
                       segment_id=f"P{idx}",
                       diameter=float(row["diameter"]),
                       length=float(row["length"]),
                       material=str(row.get("material", "Unknown")),
                       hw_c=120)
    print(f"[V3] Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    comps = list(nx.weakly_connected_components(G))
    if len(comps) > 1:
        largest = len(max(comps, key=len))
        print(f"[V3] ⚠  {len(comps)} disconnected components — largest: {largest} nodes")
    else:
        print(f"[V3] ✓  Graph is fully connected")
    return G


# ─────────────────────────────────────────────────────────────
# STEP 2 — Build WNTR Model
# ─────────────────────────────────────────────────────────────
def build_model(zone_filter=DEMO_ZONE):
    """
    Builds a WNTR model filtered to one zone for stability.
    zone_filter: "5" for Zone 5 only, "all" for full city.

    Returns: (wn, nodes_df, pipes_df, connected_ids, G,
               water_sources_df, storage_tanks_df, raw_stations_df)
    """
    print(f"\n[V3] ── Building model (zone_filter='{zone_filter}') ─────────────")

    # Load raw data
    with open(os.path.join(DATA, "pipeline.geojson")) as f:
        pipeline_raw = json.load(f)

    nodes_df = pd.read_csv(os.path.join(DATA, "nodes_with_elevation.csv"))
    nodes_df["lon"] = nodes_df["lon"].round(4)   # match _find_node_id tolerance
    nodes_df["lat"] = nodes_df["lat"].round(4)

    water_sources_df = _load_points(os.path.join(DATA, "water_source.geojson"), "water_source")
    storage_tanks_df = _load_points(os.path.join(DATA, "storage_tank.geojson"), "storage_tank")
    raw_stations_df  = _load_points(os.path.join(DATA, "raw_station.geojson"),  "raw_station")

    # ── ★ ZONE FILTER (the key fix for stability) ──────────────
    if zone_filter == "all":
        features = pipeline_raw["features"]
        print(f"[V3] Using ALL {len(features)} pipe features")
    else:
        features = [
            f for f in pipeline_raw["features"]
            if str(f["properties"].get("Water Zone", "")).strip() == zone_filter
        ]
        print(f"[V3] Filtered to Zone {zone_filter}: {len(features)} pipe features")

    if len(features) == 0:
        print(f"[V3] ✗  No features found for zone '{zone_filter}'! Check zone name.")
        return None

    # Build pipes DataFrame
    pipes = []
    for feat in features:
        props  = feat["properties"]
        coords = feat["geometry"]["coordinates"][0]
        diam   = float(props.get("Diameter(m", props.get("Diameter(m)", 0.05)) or 0.05)
        length = float(props.get("Length(m)",  1.0) or 1.0)
        pipes.append({
            "start":    coords[0],
            "end":      coords[-1],
            "length":   max(length, 1.0),
            "diameter": max(diam,   0.05),
            "material": str(props.get("Material", "Unknown")),
            "zone":     str(props.get("Water Zone", "unknown")),
        })
    pipes_df = pd.DataFrame(pipes)

    # Match pipe endpoints to node IDs
    pipes_df["start_node"] = pipes_df["start"].apply(lambda c: _find_node_id(c, nodes_df))
    pipes_df["end_node"]   = pipes_df["end"].apply(lambda c: _find_node_id(c, nodes_df))
    pipes_df = pipes_df.dropna(subset=["start_node", "end_node"])
    pipes_df["start_node"] = pipes_df["start_node"].astype(int)
    pipes_df["end_node"]   = pipes_df["end_node"].astype(int)
    pipes_df = pipes_df[pipes_df["start_node"] != pipes_df["end_node"]]

    connected_ids = set(pipes_df["start_node"]) | set(pipes_df["end_node"])
    print(f"[V3] Valid pipes: {len(pipes_df)} | Connected nodes: {len(connected_ids)}")

    # ── Keep ONLY the Largest Connected Component (LCC) ────────
    # GIS pipes were digitised independently, creating many disconnected clusters.
    # With 1 reservoir and N-1 clusters that have demand but no supply path,
    # EPANET hits "system unbalanced" at every timestep and fails.
    # Fix: extract LCC and simulate only the connected subnetwork.
    G_check = nx.Graph()
    for _, row in pipes_df.iterrows():
        G_check.add_edge(int(row["start_node"]), int(row["end_node"]))
    comps        = list(nx.connected_components(G_check))
    largest_comp = max(comps, key=len)
    n_discarded  = len(comps) - 1
    print(f"[V3] Connectivity: {len(comps)} components — keeping LCC ({len(largest_comp)} nodes), discarding {n_discarded} mini-clusters")
    pipes_df      = pipes_df[
        pipes_df["start_node"].isin(largest_comp) &
        pipes_df["end_node"].isin(largest_comp)
    ]
    connected_ids = set(pipes_df["start_node"]) | set(pipes_df["end_node"])
    print(f"[V3] LCC: {len(pipes_df)} pipes | {len(connected_ids)} nodes — fully connected ✓")

    conn_nodes_df = nodes_df[nodes_df.index.isin(connected_ids)]

    # Build NetworkX graph
    G = build_graph(conn_nodes_df, pipes_df)

    # ── WNTR Water Network Model ────────────────────────────────
    wn = wntr.network.WaterNetworkModel()

    # Time settings: 24 hours at 15-minute intervals = 96 timesteps
    wn.options.time.duration           = 86400   # 24 hours in seconds
    wn.options.time.hydraulic_timestep = 900     # 15 minutes in seconds
    wn.options.time.report_timestep    = 900

    # Hydraulic settings
    wn.options.hydraulic.demand_model      = "PDD"   # Pressure-Driven Demand (correct for intermittent supply)
    wn.options.hydraulic.required_pressure = 10      # Minimum acceptable pressure (metres)
    wn.options.hydraulic.minimum_pressure  = 0
    wn.options.hydraulic.headloss          = "H-W"   # Hazen-Williams

    # Add demand pattern (shared with V5)
    wn.add_pattern("DailyPattern", DEMAND_PATTERN)

    # Add junction nodes — ONLY connected nodes (isolated nodes crash EPANET)
    for idx, row in conn_nodes_df.iterrows():
        wn.add_junction(
            name=f"J{idx}",
            base_demand=0.001,            # small base demand per node (m³/s)
            demand_pattern="DailyPattern",
            elevation=float(row["elevation"])
        )

    # Add pipes — skip duplicate node-pairs (same-direction pair only once)
    added_pairs = set()
    pipes_added = 0
    for idx, row in pipes_df.iterrows():
        pair = tuple(sorted([row["start_node"], row["end_node"]]))
        if pair in added_pairs:
            continue
        added_pairs.add(pair)
        wn.add_pipe(
            name=f"P{idx}",
            start_node_name=f"J{int(row['start_node'])}",
            end_node_name=f"J{int(row['end_node'])}",
            length=float(row["length"]),
            diameter=float(row["diameter"]),
            roughness=120,      # Hazen-Williams C (will be overridden below per material)
            minor_loss=0.0,
            initial_status="OPEN"
        )
        pipes_added += 1
    print(f"[V3] WNTR pipes added: {pipes_added}")

    # ── Add water sources as reservoirs ────────────────────────
    res_count = 0

    # Try to use real water source locations from GeoJSON
    if not water_sources_df.empty:
        for i, src in water_sources_df.head(3).iterrows():
            # Find the closest node in the connected set
            dists = conn_nodes_df.apply(
                lambda r: abs(r["lon"] - src["lon"]) + abs(r["lat"] - src["lat"]), axis=1
            )
            nidx  = dists.idxmin()
            nelev = float(conn_nodes_df.loc[nidx, "elevation"])
            wn.add_reservoir(f"WaterSource_{res_count}", base_head=nelev + 20)
            wn.add_pipe(
                name=f"SourcePipe_{res_count}",
                start_node_name=f"WaterSource_{res_count}",
                end_node_name=f"J{nidx}",
                length=10, diameter=0.5, roughness=120,
                minor_loss=0.0, initial_status="OPEN"
            )
            res_count += 1
        print(f"[V3] Added {res_count} real water-source reservoirs")

    # Always add a fallback reservoir at the highest-elevation node
    # (guarantees at least one supply point even if zone has no nearby water source)
    hi_idx  = conn_nodes_df["elevation"].idxmax()
    hi_elev = float(conn_nodes_df.loc[hi_idx, "elevation"])
    wn.add_reservoir("Source_Fallback", base_head=hi_elev + 20)
    wn.add_pipe(
        name="SourcePipe_Fallback",
        start_node_name="Source_Fallback",
        end_node_name=f"J{hi_idx}",
        length=10, diameter=0.5, roughness=120,
        minor_loss=0.0, initial_status="OPEN"
    )
    print(f"[V3] Fallback reservoir → J{hi_idx} (elevation={hi_elev:.1f}m, head={hi_elev+20:.1f}m)")

    total_junctions = len(wn.junction_name_list)
    total_pipes_wntr = len(wn.pipe_name_list)
    print(f"[V3] WNTR model: {total_junctions} junctions, {total_pipes_wntr} pipes, {len(wn.reservoir_name_list)} reservoirs")

    return (wn, nodes_df, pipes_df, connected_ids, G,
            water_sources_df, storage_tanks_df, raw_stations_df)


# ─────────────────────────────────────────────────────────────
# STEP 3 — Run Simulation (saves pressure + flow CSVs)
# ─────────────────────────────────────────────────────────────
def run_simulation(wn, scenario_label="baseline", save_csv=True):
    """
    Runs WNTR EpanetSimulator. Returns (pressure_df, flow_df).
    pressure_df: rows=timesteps (0-95), columns=node names (J0, J1, ...)
    flow_df:     rows=timesteps (0-95), columns=pipe names (P0, P1, ...)
    """
    print(f"\n[V3] Running simulation: {scenario_label} ...")
    try:
        sim     = wntr.sim.EpanetSimulator(wn)
        results = sim.run_sim()

        p_df = results.node["pressure"]   # DataFrame: timesteps × nodes
        f_df = results.link["flowrate"]   # DataFrame: timesteps × pipes

        print(f"[V3] ✓ {scenario_label}: {len(p_df)} timesteps × {len(p_df.columns)} nodes")

        # Validation checks (from Bible Step 4)
        p_mean = p_df.mean()
        neg_count = (p_mean < 0).sum()
        if neg_count > 0:
            print(f"[V3] ⚠  {neg_count} nodes have negative avg pressure — consider reducing demands")
        else:
            print(f"[V3] ✓  No negative pressures — validation passed")

        print(f"[V3]    Pressure range: {p_mean.min():.1f} – {p_mean.max():.1f} m (avg across timesteps)")

        if save_csv:
            p_path = os.path.join(OUT, f"pressure_{scenario_label}.csv")
            f_path = os.path.join(OUT, f"flow_{scenario_label}.csv")
            p_df.to_csv(p_path)
            f_df.to_csv(f_path)
            print(f"[V3] ✓  Saved: outputs/pressure_{scenario_label}.csv")
            print(f"[V3] ✓  Saved: outputs/flow_{scenario_label}.csv")

        return p_df, f_df

    except Exception as e:
        print(f"[V3] ✗ {scenario_label} FAILED: {e}")
        print("[V3]   Tip: If 'system unbalanced', reduce base_demand in build_model() from 0.001 to 0.0001")
        return None, None


# ─────────────────────────────────────────────────────────────
# STEP 4 — Apply Scenario (deep copy — never mutates base model)
# ─────────────────────────────────────────────────────────────
def apply_scenario(wn_base, scenario_type, params=None):
    """
    Creates a modified copy of the model for each anomaly scenario.

    scenario_type options:
      "leak"         — reduce one mid-network pipe diameter by 30%
      "valve_close"  — close one upstream pipe
      "demand_surge" — multiply all demands by 1.5×
    """
    wn     = copy.deepcopy(wn_base)
    params = params or {}

    # Get regular pipe names (exclude source pipes)
    pipes = [n for n in wn.pipe_name_list
             if not n.startswith(("SourcePipe", "WaterSource"))]

    if scenario_type == "leak":
        # Pick a mid-network pipe as the leak location
        target = params.get("pipe", pipes[len(pipes) // 2])
        pipe   = wn.get_link(target)
        orig   = pipe.diameter
        pipe.diameter = orig * (1 - params.get("reduction", 0.30))
        print(f"[V3] LEAK: {target} diameter {orig:.4f}→{pipe.diameter:.4f}m (−30%)")

    elif scenario_type == "valve_close":
        # Close a pipe near the start of the network
        target = params.get("pipe", pipes[min(10, len(pipes) - 1)])
        wn.get_link(target).initial_status = wntr.network.LinkStatus.Closed
        print(f"[V3] VALVE_CLOSE: {target} set to Closed")

    elif scenario_type == "demand_surge":
        # Multiply all junction demands by 1.5×
        surge = params.get("surge_factor", 1.5)
        for jn in wn.junction_name_list:
            for d in wn.get_node(jn).demand_timeseries_list:
                d.base_value *= surge
        print(f"[V3] DEMAND_SURGE: all junctions × {surge}")

    return wn


# ─────────────────────────────────────────────────────────────
# STEP 5 — Run All 3 Anomaly Scenarios
# ─────────────────────────────────────────────────────────────
def run_all_scenarios(wn_base):
    """
    Runs the 3 mandatory scenarios from the Architecture Bible.
    Returns dict: {label: (pressure_df, flow_df)}
    """
    print("\n[V3] ── Running 3 anomaly scenarios ─────────────────────")
    out = {}
    scenarios = [
        ("scenario_A_leak",         "leak"),
        ("scenario_B_valve_close",  "valve_close"),
        ("scenario_C_demand_surge", "demand_surge"),
    ]
    for label, stype in scenarios:
        wn_scenario = apply_scenario(wn_base, stype)
        p, f = run_simulation(wn_scenario, label, save_csv=True)
        if p is not None:
            out[label] = (p, f)
        else:
            print(f"[V3] ⚠  Scenario '{label}' failed — skipping")

    print(f"\n[V3] ✓  Scenarios complete: {list(out.keys())}")
    return out


# ─────────────────────────────────────────────────────────────
# STEP 6 — Load Cached CSV Results (used by app.py backend)
# ─────────────────────────────────────────────────────────────
def load_results(label="baseline"):
    """Load saved pressure/flow CSVs. Returns (pressure_df, flow_df) or (None, None)."""
    try:
        p = pd.read_csv(os.path.join(OUT, f"pressure_{label}.csv"), index_col=0)
        f = pd.read_csv(os.path.join(OUT, f"flow_{label}.csv"),     index_col=0)
        print(f"[V3] Loaded results: pressure_{label}.csv ({len(p)} rows × {len(p.columns)} cols)")
        return p, f
    except FileNotFoundError:
        return None, None


# ─────────────────────────────────────────────────────────────
# STEP 7 — Single-Timestep Stats (for backend API clock)
# ─────────────────────────────────────────────────────────────
def get_timestep_stats(pressure_df, flow_df, timestep=0):
    """Returns a dict with pressure/flow summary for one timestep."""
    if pressure_df is None:
        return {}
    t     = min(timestep, len(pressure_df) - 1)
    p_row = pressure_df.iloc[t]
    f_row = flow_df.iloc[t] if flow_df is not None else pd.Series(dtype=float)
    h, m  = divmod(t * 15, 60)
    return {
        "timestep":          t,
        "time_label":        f"{h:02d}:{m:02d}",
        "demand_multiplier": DEMAND_PATTERN[t],
        "pressure": {
            "min":  round(float(p_row.min()),  2),
            "max":  round(float(p_row.max()),  2),
            "mean": round(float(p_row.mean()), 2),
        },
        "flowrate": {
            "min":  round(float(f_row.abs().min()),  6) if len(f_row) else 0,
            "max":  round(float(f_row.abs().max()),  6) if len(f_row) else 0,
            "mean": round(float(f_row.abs().mean()), 6) if len(f_row) else 0,
        }
    }


# ─────────────────────────────────────────────────────────────
# MAIN — run with: python scripts/simulation_engine.py
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 62)
    print("  V3 · Hydro-Equity Engine · Hydraulic Simulation Engine")
    print(f"  Zone filter: '{DEMO_ZONE}'")
    print("=" * 62)

    # Build model
    result = build_model(zone_filter=DEMO_ZONE)
    if result is None:
        print("✗ Model build failed.")
        exit(1)

    (wn, nodes_df, pipes_df, connected_ids, G,
     water_sources_df, storage_tanks_df, raw_stations_df) = result

    # Save EPANET .inp file
    inp_path = os.path.join(OUT, "solapur_network.inp")
    wntr.network.write_inpfile(wn, inp_path)
    print(f"\n[V3] Saved EPANET model: outputs/solapur_network.inp")

    # Run baseline simulation
    pressure_df, flow_df = run_simulation(wn, "baseline", save_csv=True)

    if pressure_df is not None:
        # Run the 3 mandatory anomaly scenarios
        run_all_scenarios(wn)

        # Print 24-hour summary
        print("\n── 24-hour Pressure Summary ────────────────────────────────")
        print(f"  {'Time':>6}  {'Mult':>5}  {'Min P':>7}  {'Max P':>7}  {'Avg P':>7}")
        for t_i in [0, 24, 32, 48, 68, 80, 95]:
            s = get_timestep_stats(pressure_df, flow_df, t_i)
            p = s["pressure"]
            print(f"  {s['time_label']:>6}  ×{s['demand_multiplier']:.2f}  "
                  f"{p['min']:>7.1f}m  {p['max']:>7.1f}m  {p['mean']:>7.1f}m")

        print("\n" + "=" * 62)
        print("  ✓  V3 COMPLETE — All outputs saved to outputs/")
        print("     pressure_baseline.csv")
        print("     flow_baseline.csv")
        print("     pressure_scenario_A_leak.csv")
        print("     pressure_scenario_B_valve_close.csv")
        print("     pressure_scenario_C_demand_surge.csv")
        print("     solapur_network.inp")
        print("=" * 62)
        print("\n  Next step: python backend/app.py")
    else:
        print("\n✗ Baseline simulation failed.")
        print("  Try: change DEMO_ZONE to a different zone number (1–8)")
        print("  Or:  reduce base_demand in build_model() from 0.001 to 0.0001")