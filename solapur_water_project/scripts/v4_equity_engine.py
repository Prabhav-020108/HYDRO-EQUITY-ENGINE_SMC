"""
Hydro-Equity Engine — V4 Equity Computation
Solapur Municipal Corporation | SAMVED-2026
"""

import pandas as pd
import numpy as np
import os, json, math

# ── Paths ──
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(ROOT_DIR, "Data")
OUT_DIR  = os.path.join(ROOT_DIR, "outputs")

# Zone definitions (matches your backend/frontend)
ZONES = [
    {'id': 'z1', 'name': 'Zone 1', 'lat': 17.710, 'lon': 75.882},
    {'id': 'z2', 'name': 'Zone 2', 'lat': 17.700, 'lon': 75.910},
    {'id': 'z3', 'name': 'Zone 3', 'lat': 17.690, 'lon': 75.935},
    {'id': 'z4', 'name': 'Zone 4', 'lat': 17.675, 'lon': 75.898},
    {'id': 'z5', 'name': 'Zone 5', 'lat': 17.665, 'lon': 75.920},
    {'id': 'z6', 'name': 'Zone 6', 'lat': 17.680, 'lon': 75.870},
    {'id': 'z7', 'name': 'Zone 7', 'lat': 17.695, 'lon': 75.958},
    {'id': 'z8', 'name': 'Zone 8', 'lat': 17.655, 'lon': 75.940},
]

SCENARIOS = {
    "baseline": "pressure_baseline.csv",
    "leak": "pressure_scenario_A_leak.csv",
    "valve": "pressure_scenario_B_valve_close.csv",
    "surge": "pressure_scenario_C_demand_surge.csv"
}

def hei_status(h):
    if h > 1.30: return 'over'
    if h >= 0.85: return 'equitable'
    if h >= 0.70: return 'moderate'
    return 'severe'

def compute_equity():
    print("="*50)
    print(" V4 Equity Engine — Processing Network Fairness")
    print("="*50)

    # 1. Load nodes and assign them to nearest zone
    nodes_path = os.path.join(DATA_DIR, "nodes_with_elevation.csv")
    if not os.path.exists(nodes_path):
        print("ERROR: nodes_with_elevation.csv missing.")
        return

    nodes_df = pd.read_csv(nodes_path)

    # 2. Map nodes to zones and calculate distance from zone center
    # (Tail-end nodes are the ones furthest from the center/ESR)
    zone_node_map = {z['id']: [] for z in ZONES}

    for idx, row in nodes_df.iterrows():
        lat, lon = row['lat'], row['lon']
        node_name = f"J{idx}"

        # Find closest zone center
        dists = [(z['id'], (z['lat']-lat)**2 + (z['lon']-lon)**2) for z in ZONES]
        dists.sort(key=lambda x: x[1])
        closest_zone = dists[0][0]
        distance_to_center = dists[0][1]

        zone_node_map[closest_zone].append({
            'node': node_name,
            'dist': distance_to_center
        })

    # 3. Identify the 15% "Tail-End" nodes for each zone
    zone_tail_ends = {}
    zone_core_nodes = {}

    for zid, n_list in zone_node_map.items():
        # Sort by distance descending (furthest first)
        n_list.sort(key=lambda x: x['dist'], reverse=True)
        tail_count = max(1, int(len(n_list) * 0.15))

        zone_tail_ends[zid] = [x['node'] for x in n_list[:tail_count]]
        zone_core_nodes[zid] = [x['node'] for x in n_list[tail_count:]]

    # 4. Compute HEI for every scenario
    final_output = {}

    for scen, filename in SCENARIOS.items():
        csv_path = os.path.join(OUT_DIR, filename)
        if not os.path.exists(csv_path):
            continue

        print(f"Processing {scen}...")
        p_df = pd.read_csv(csv_path, index_col=0)

        # We'll calculate the 24-hour average (mean across all timesteps)
        avg_pressures = p_df.mean()

        scenario_results = []

        for z in ZONES:
            zid = z['id']
            # Get available nodes that actually exist in the simulation CSV
            tail_nodes = [n for n in zone_tail_ends[zid] if n in avg_pressures.index]
            core_nodes = [n for n in zone_core_nodes[zid] if n in avg_pressures.index]

            if len(tail_nodes) == 0 or len(core_nodes) == 0:
                # Fallback if zone wasn't fully simulated (e.g., Zone 2-8 in demo mode)
                scenario_results.append({
                    'id': zid, 'name': z['name'], 'lat': z['lat'], 'lon': z['lon'],
                    'pressure_avg': 0.0, 'pressure_tailend': 0.0, 'hei': 0.0, 'status': 'severe'
                })
                continue

            p_tail = avg_pressures[tail_nodes].mean()
            p_core = avg_pressures[core_nodes].mean()

            # Avoid division by zero
            p_core = max(p_core, 0.1)
            p_tail = max(p_tail, 0.0)

            hei = round(min(1.55, p_tail / p_core), 3)

            scenario_results.append({
                'id': zid, 'name': z['name'], 'lat': z['lat'], 'lon': z['lon'],
                'pressure_avg': round(p_core, 1),
                'pressure_tailend': round(p_tail, 1),
                'hei': hei,
                'status': hei_status(hei)
            })

        # Calculate CWEI (City-Wide Equity Index)
        valid_heis = [r['hei'] for r in scenario_results if r['hei'] > 0]
        cwei = round(sum(valid_heis) / len(valid_heis), 3) if valid_heis else 0.0

        final_output[scen] = {
            "zones": scenario_results,
            "cwei": cwei,
            "scenario": scen
        }

    # 5. Save to JSON
    out_path = os.path.join(OUT_DIR, "v4_equity.json")
    with open(out_path, "w") as f:
        json.dump(final_output, f, indent=2)

    print(f"\n✓ V4 COMPLETE. Saved equity scores to outputs/v4_equity.json")

if __name__ == "__main__":
    compute_equity()
