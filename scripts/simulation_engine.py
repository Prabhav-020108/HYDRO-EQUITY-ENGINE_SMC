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

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "Data")
OUT  = os.path.join(ROOT, "outputs")
os.makedirs(OUT, exist_ok=True)

DEMO_ZONE = "1"

DEMAND_PATTERN = []
for _h in range(24):
    if   0  <= _h < 6:   _m = 0.05
    elif 6  <= _h < 8:   _m = 2.50
    elif 8  <= _h < 17:  _m = 1.00
    elif 17 <= _h < 20:  _m = 2.00
    else:                _m = 0.05
    DEMAND_PATTERN.extend([_m] * 4)
assert len(DEMAND_PATTERN) == 96


def _load_points(geojson_path, label):
    """Load point locations from GeoJSON. Handles Point, Polygon, and MultiPolygon (centroid)."""
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


def build_graph(nodes_df, pipes_df):
    """Directed graph — MUST be DiGraph, not Graph (V4 and V7 need directed paths)."""
    G = nx.DiGraph()
    for _, row in nodes_df.iterrows():
        nid = int(row["node_id"])
        G.add_node(f"J{nid}",
                   lat=float(row["lat"]),
                   lon=float(row["lon"]),
                   elevation=float(row["elevation"]),
                   zone=str(row.get("zone_id", "unknown")))
    for _, row in pipes_df.iterrows():
        s = f"J{int(row['start_node_id'])}"
        e = f"J{int(row['end_node_id'])}"
        hw_c = float(row["hw_c_value"]) if "hw_c_value" in row and pd.notna(row["hw_c_value"]) else 120
        if G.has_node(s) and G.has_node(e):
            G.add_edge(s, e,
                       segment_id=f"P{int(row['segment_id'])}",
                       diameter=float(row["diameter_m"]),
                       length=float(row["length_m"]),
                       material=str(row.get("material", "Unknown")),
                       hw_c=hw_c)
    print(f"[V3] Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    comps = list(nx.weakly_connected_components(G))
    if len(comps) > 1:
        largest = len(max(comps, key=len))
        print(f"[V3] ⚠  {len(comps)} disconnected components — largest: {largest} nodes")
    else:
        print(f"[V3] ✓  Graph is fully connected")
    return G


def build_model(zone_filter=DEMO_ZONE):
    """
    Builds a WNTR model from V1 CSV outputs.
    zone_filter: "1" for Zone 1 only, "all" for full city.

    Returns: (wn, nodes_df, pipes_df, connected_ids, G,
               water_sources_df, storage_tanks_df, raw_stations_df)
    """
    print(f"\n[V3] ── Building model (zone_filter='{zone_filter}') ─────────────")

    # ── Load V1 CSVs ──────────────────────────────────────────────
    all_nodes_df = pd.read_csv(os.path.join(DATA, "nodes_with_elevation.csv"))
    all_pipes_df = pd.read_csv(os.path.join(DATA, "pipe_segments.csv"))
    print(f"[V3] Loaded V1 CSVs: {len(all_nodes_df)} nodes, {len(all_pipes_df)} pipes")

    # Load zone_demand for per-zone base demands
    zone_demand_path = os.path.join(DATA, "zone_demand.csv")
    zone_demand_df = None
    if os.path.exists(zone_demand_path):
        zone_demand_df = pd.read_csv(zone_demand_path)
        print(f"[V3] Loaded zone_demand.csv: {len(zone_demand_df)} zones")

    # Load infrastructure GeoJSON (for reservoir placement)
    water_sources_df = _load_points(os.path.join(DATA, "water_source.geojson"), "water_source")
    storage_tanks_df = _load_points(os.path.join(DATA, "storage_tank.geojson"), "storage_tank")
    raw_stations_df  = _load_points(os.path.join(DATA, "raw_station.geojson"),  "raw_station")

    # ── Zone filtering ────────────────────────────────────────────
    # Ensure start_node_id and end_node_id exist
    if "start_node_id" not in all_pipes_df.columns:
        print("[V3] ✗  pipe_segments.csv missing start_node_id. Run load_data.py + add_node_ids first.")
        return None

    # Drop pipes with missing node IDs
    pipes_df = all_pipes_df.dropna(subset=["start_node_id", "end_node_id"]).copy()
    pipes_df["start_node_id"] = pipes_df["start_node_id"].astype(int)
    pipes_df["end_node_id"]   = pipes_df["end_node_id"].astype(int)

    if zone_filter == "all":
        print(f"[V3] Using ALL {len(pipes_df)} pipe segments")
    else:
        zone_key = f"zone_{zone_filter}"
        pipes_df = pipes_df[pipes_df["zone_id"] == zone_key]
        print(f"[V3] Filtered to {zone_key}: {len(pipes_df)} pipe segments")

    if len(pipes_df) == 0:
        print(f"[V3] ✗  No pipes found for zone_filter='{zone_filter}'! Check zone name.")
        return None

    # Remove self-loops
    pipes_df = pipes_df[pipes_df["start_node_id"] != pipes_df["end_node_id"]]

    # ── Largest Connected Component ───────────────────────────────
    connected_ids = set(pipes_df["start_node_id"]) | set(pipes_df["end_node_id"])
    print(f"[V3] Valid pipes: {len(pipes_df)} | Connected nodes: {len(connected_ids)}")

    G_check = nx.Graph()
    for _, row in pipes_df.iterrows():
        G_check.add_edge(int(row["start_node_id"]), int(row["end_node_id"]))
    comps        = list(nx.connected_components(G_check))
    largest_comp = max(comps, key=len)
    n_discarded  = len(comps) - 1
    print(f"[V3] Connectivity: {len(comps)} components — keeping LCC ({len(largest_comp)} nodes), discarding {n_discarded} mini-clusters")
    pipes_df = pipes_df[
        pipes_df["start_node_id"].isin(largest_comp) &
        pipes_df["end_node_id"].isin(largest_comp)
    ]
    connected_ids = set(pipes_df["start_node_id"]) | set(pipes_df["end_node_id"])
    print(f"[V3] LCC: {len(pipes_df)} pipes | {len(connected_ids)} nodes — fully connected ✓")

    # Filter nodes to only those in connected set
    conn_nodes_df = all_nodes_df[all_nodes_df["node_id"].isin(connected_ids)].copy()

    # ── Build NetworkX graph from V1 data ─────────────────────────
    G = build_graph(conn_nodes_df, pipes_df)

    # ── Compute per-node base demand from zone_demand.csv ─────────
    node_demands = {}
    if zone_demand_df is not None:
        # Count nodes per zone to distribute base_lps evenly
        zone_node_counts = conn_nodes_df["zone_id"].value_counts().to_dict()
        zone_base_lps = dict(zip(zone_demand_df["zone_id"], zone_demand_df["base_lps"]))
        for _, nrow in conn_nodes_df.iterrows():
            nid = int(nrow["node_id"])
            zid = str(nrow["zone_id"])
            zone_lps = zone_base_lps.get(zid, 0.001)
            n_in_zone = zone_node_counts.get(zid, 1)
            # Convert L/s to m³/s and divide among nodes in zone
            node_demands[nid] = (zone_lps / 1000.0) / max(n_in_zone, 1)
    default_demand = 0.001 / 1000.0  # fallback: 0.001 L/s → m³/s

    # ── Build WNTR model ─────────────────────────────────────────
    wn = wntr.network.WaterNetworkModel()

    wn.options.time.duration           = 86400
    wn.options.time.hydraulic_timestep = 900
    wn.options.time.report_timestep    = 900

    wn.options.hydraulic.demand_model      = "PDD"
    wn.options.hydraulic.required_pressure = 10
    wn.options.hydraulic.minimum_pressure  = 0
    wn.options.hydraulic.headloss          = "H-W"

    wn.add_pattern("DailyPattern", DEMAND_PATTERN)

    for _, row in conn_nodes_df.iterrows():
        nid = int(row["node_id"])
        bd = node_demands.get(nid, default_demand)
        wn.add_junction(
            name=f"J{nid}",
            base_demand=bd,
            demand_pattern="DailyPattern",
            elevation=float(row["elevation"])
        )

    added_pairs = set()
    pipes_added = 0
    for _, row in pipes_df.iterrows():
        pair = tuple(sorted([int(row["start_node_id"]), int(row["end_node_id"])]))
        if pair in added_pairs:
            continue
        added_pairs.add(pair)
        hw_c = float(row["hw_c_value"]) if "hw_c_value" in row and pd.notna(row["hw_c_value"]) else 120
        wn.add_pipe(
            name=f"P{int(row['segment_id'])}",
            start_node_name=f"J{int(row['start_node_id'])}",
            end_node_name=f"J{int(row['end_node_id'])}",
            length=float(row["length_m"]),
            diameter=float(row["diameter_m"]),
            roughness=hw_c,
            minor_loss=0.0,
            initial_status="OPEN"
        )
        pipes_added += 1
    print(f"[V3] WNTR pipes added: {pipes_added}")

    # ── Reservoirs from water sources ─────────────────────────────
    res_count = 0

    if not water_sources_df.empty:
        for i, src in water_sources_df.head(3).iterrows():
            dists = conn_nodes_df.apply(
                lambda r: abs(r["lon"] - src["lon"]) + abs(r["lat"] - src["lat"]), axis=1
            )
            nidx  = int(conn_nodes_df.loc[dists.idxmin(), "node_id"])
            nelev = float(conn_nodes_df.loc[dists.idxmin(), "elevation"])
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

    hi_row  = conn_nodes_df.loc[conn_nodes_df["elevation"].idxmax()]
    hi_nid  = int(hi_row["node_id"])
    hi_elev = float(hi_row["elevation"])
    wn.add_reservoir("Source_Fallback", base_head=hi_elev + 20)
    wn.add_pipe(
        name="SourcePipe_Fallback",
        start_node_name="Source_Fallback",
        end_node_name=f"J{hi_nid}",
        length=10, diameter=0.5, roughness=120,
        minor_loss=0.0, initial_status="OPEN"
    )
    print(f"[V3] Fallback reservoir → J{hi_nid} (elevation={hi_elev:.1f}m, head={hi_elev+20:.1f}m)")

    total_junctions = len(wn.junction_name_list)
    total_pipes_wntr = len(wn.pipe_name_list)
    print(f"[V3] WNTR model: {total_junctions} junctions, {total_pipes_wntr} pipes, {len(wn.reservoir_name_list)} reservoirs")

    # Return uses all_nodes_df (full set) as second element for backward compat
    return (wn, all_nodes_df, pipes_df, connected_ids, G,
            water_sources_df, storage_tanks_df, raw_stations_df)


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

        p_df = results.node["pressure"]
        f_df = results.link["flowrate"]

        print(f"[V3] ✓ {scenario_label}: {len(p_df)} timesteps × {len(p_df.columns)} nodes")

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
        print("[V3]   Tip: If 'system unbalanced', reduce base_demand or check network connectivity")
        return None, None


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

    pipes = [n for n in wn.pipe_name_list
             if not n.startswith(("SourcePipe", "WaterSource"))]

    if scenario_type == "leak":
        target = params.get("pipe", pipes[len(pipes) // 2])
        pipe   = wn.get_link(target)
        orig   = pipe.diameter
        pipe.diameter = orig * (1 - params.get("reduction", 0.30))
        print(f"[V3] LEAK: {target} diameter {orig:.4f}→{pipe.diameter:.4f}m (−30%)")

    elif scenario_type == "valve_close":
        target = params.get("pipe", pipes[min(10, len(pipes) - 1)])
        wn.get_link(target).initial_status = wntr.network.LinkStatus.Closed
        print(f"[V3] VALVE_CLOSE: {target} set to Closed")

    elif scenario_type == "demand_surge":
        surge = params.get("surge_factor", 1.5)
        for jn in wn.junction_name_list:
            for d in wn.get_node(jn).demand_timeseries_list:
                d.base_value *= surge
        print(f"[V3] DEMAND_SURGE: all junctions × {surge}")

    return wn


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


def load_results(label="baseline"):
    """Load saved pressure/flow CSVs. Returns (pressure_df, flow_df) or (None, None)."""
    try:
        p = pd.read_csv(os.path.join(OUT, f"pressure_{label}.csv"), index_col=0)
        f = pd.read_csv(os.path.join(OUT, f"flow_{label}.csv"),     index_col=0)
        print(f"[V3] Loaded results: pressure_{label}.csv ({len(p)} rows × {len(p.columns)} cols)")
        return p, f
    except FileNotFoundError:
        return None, None


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


if __name__ == "__main__":
    import sys
    # Usage: python scripts/simulation_engine.py [zone_filter]
    #   zone_filter: "1" (default, Zone-1 demo), "all" (full city), or any zone number
    chosen_zone = sys.argv[1] if len(sys.argv) > 1 else DEMO_ZONE
    prefix = "fullcity_" if chosen_zone == "all" else ""

    print("=" * 62)
    print("  V3 · Hydro-Equity Engine · Hydraulic Simulation Engine")
    print(f"  Zone filter: '{chosen_zone}'")
    print("=" * 62)

    result = build_model(zone_filter=chosen_zone)
    if result is None:
        print("✗ Model build failed.")
        exit(1)

    (wn, nodes_df, pipes_df, connected_ids, G,
     water_sources_df, storage_tanks_df, raw_stations_df) = result

    inp_name = f"{prefix}solapur_network.inp"
    inp_path = os.path.join(OUT, inp_name)
    wntr.network.write_inpfile(wn, inp_path)
    print(f"\n[V3] Saved EPANET model: outputs/{inp_name}")

    pressure_df, flow_df = run_simulation(wn, f"{prefix}baseline", save_csv=True)

    if pressure_df is not None:
        # Run anomaly scenarios with same prefix
        print("\n[V3] ── Running 3 anomaly scenarios ─────────────────────")
        scenarios = [
            (f"{prefix}scenario_A_leak",         "leak"),
            (f"{prefix}scenario_B_valve_close",  "valve_close"),
            (f"{prefix}scenario_C_demand_surge", "demand_surge"),
        ]
        for label, stype in scenarios:
            wn_scenario = apply_scenario(wn, stype)
            run_simulation(wn_scenario, label, save_csv=True)

        print("\n── 24-hour Pressure Summary ────────────────────────────────")
        print(f"  {'Time':>6}  {'Mult':>5}  {'Min P':>7}  {'Max P':>7}  {'Avg P':>7}")
        for t_i in [0, 24, 32, 48, 68, 80, 95]:
            s = get_timestep_stats(pressure_df, flow_df, t_i)
            p = s["pressure"]
            print(f"  {s['time_label']:>6}  ×{s['demand_multiplier']:.2f}  "
                  f"{p['min']:>7.1f}m  {p['max']:>7.1f}m  {p['mean']:>7.1f}m")

        print("\n" + "=" * 62)
        print("  ✓  V3 COMPLETE — All outputs saved to outputs/")
        print(f"     {prefix}pressure_baseline.csv")
        print(f"     {prefix}flow_baseline.csv")
        print(f"     {prefix}pressure_scenario_A_leak.csv")
        print(f"     {prefix}pressure_scenario_B_valve_close.csv")
        print(f"     {prefix}pressure_scenario_C_demand_surge.csv")
        print(f"     {inp_name}")
        print("=" * 62)
        print("\n  Next step: python backend/app.py")
    else:
        print("\n✗ Baseline simulation failed.")
        print("  Try: change zone filter (1–8) or check network connectivity")