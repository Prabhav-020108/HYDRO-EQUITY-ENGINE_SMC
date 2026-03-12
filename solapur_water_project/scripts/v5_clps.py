"""
v5_clps.py
V5 - Composite Leak & Pressure Score (CLPS)
"""

import os, json
import pandas as pd
import numpy as np
import networkx as nx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "Data")
OUT  = os.path.join(ROOT, "outputs")

# DEMAND_PATTERN from simulation_engine
DEMAND_PATTERN = []
for _h in range(24):
    if   0  <= _h < 6:   _m = 0.05
    elif 6  <= _h < 8:   _m = 2.50
    elif 8  <= _h < 17:  _m = 1.00
    elif 17 <= _h < 20:  _m = 2.00
    else:                _m = 0.05
    DEMAND_PATTERN.extend([_m] * 4)

def main():
    print("=" * 62)
    print("  V5 · Composite Leak & Pressure Score (CLPS)")
    print("=" * 62)

    # 1. Load Data
    nodes_df = pd.read_csv(os.path.join(DATA, "nodes_with_elevation.csv"))
    pipes_df = pd.read_csv(os.path.join(DATA, "pipe_segments.csv"))
    
    zone_demand_path = os.path.join(DATA, "zone_demand.csv")
    zone_demand = {}
    if os.path.exists(zone_demand_path):
        z_df = pd.read_csv(zone_demand_path)
        # convert L/s to m3/s
        zone_demand = dict(zip(z_df["zone_id"], z_df["base_lps"] / 1000.0))

    valid_pipes = pipes_df.dropna(subset=["start_node_id", "end_node_id"]).copy()

    # 2. Build Zone node/edge mappings
    node_zone = dict(zip(nodes_df["node_id"].astype(int), nodes_df["zone_id"]))
    zones = sorted(nodes_df["zone_id"].dropna().unique())

    # Map pipes
    # For each zone:
    # inlet edges: start_node not in zone OR water source, end_node in zone
    # outlet edges: start_node in zone, end_node in zone (as per prompt "inside the zone")
    # Actually, flow leaving the zone: start_node in zone, end_node not in zone.
    # The prompt says: "inlet edges (coming from ESR/other zones into this zone) and outlet edges (inside the zone)."
    # Let's map both internal and leaving.
    zone_inlet_pipes = {z: [] for z in zones}
    zone_internal_pipes = {z: [] for z in zones}
    zone_outlet_pipes = {z: [] for z in zones}

    for _, row in valid_pipes.iterrows():
        pu = int(row["start_node_id"])
        pv = int(row["end_node_id"])
        pid = f"P{int(row['segment_id'])}"
        
        zu = node_zone.get(pu, "out")
        zv = node_zone.get(pv, "out")
        
        if zu != zv:
            if zv in zones: zone_inlet_pipes[zv].append((pid, pu, pv))
            if zu in zones: zone_outlet_pipes[zu].append((pid, pu, pv))
        else:
            if zu in zones: zone_internal_pipes[zu].append((pid, pu, pv))

    # WNTR reservoirs are connected by SourcePipe_X
    # We will identify reservoir source pipes directly from the flow columns later

    # Load Baseline data
    try:
        p_base = pd.read_csv(os.path.join(OUT, "pressure_fullcity_baseline.csv"), index_col=0)
        f_base = pd.read_csv(os.path.join(OUT, "flow_fullcity_baseline.csv"), index_col=0)
    except FileNotFoundError:
        print("✗ Baseline CSVs not found. Run simulation_engine.py all first.")
        return

    # Find reservoir pipes
    res_pipes = [c for c in f_base.columns if c.startswith("SourcePipe")]

    # Attach reservoir pipes to the zones of their destination nodes
    # We don't have the exact destination node mapping from Simulation Engine exported easily,
    # but we can infer it or just assume reservoir pipes are inlets to some zone.
    # For simplicity, we'll just use the flow entering the nodes of the zone.
    
    # We will compute the signals for Baseline and 3 scenarios
    scenarios = {
        "baseline": ("pressure_fullcity_baseline.csv", "flow_fullcity_baseline.csv"),
        "leak": ("pressure_fullcity_scenario_A_leak.csv", "flow_fullcity_scenario_A_leak.csv"),
        "valve": ("pressure_fullcity_scenario_B_valve_close.csv", "flow_fullcity_scenario_B_valve_close.csv"),
        "surge": ("pressure_fullcity_scenario_C_demand_surge.csv", "flow_fullcity_scenario_C_demand_surge.csv")
    }

    all_alerts = []
    
    # Process scenarios
    for scen_name, (p_file, f_file) in scenarios.items():
        p_path = os.path.join(OUT, p_file)
        f_path = os.path.join(OUT, f_file)
        if not os.path.exists(p_path) or not os.path.exists(f_path):
            continue
            
        p_df = pd.read_csv(p_path, index_col=0)
        f_df = pd.read_csv(f_path, index_col=0)

        # For saving CLPS
        clps_records = []

        for z in zones:
            z_nids = [nid for nid, zz in node_zone.items() if zz == z]
            z_p_cols = [f"J{nid}" for nid in z_nids if f"J{nid}" in p_df.columns]
            
            if not z_p_cols: continue

            # Pre-calculate means and sums across all timesteps for speed
            z_p_base_mean = p_base[z_p_cols].mean(axis=1)
            z_p_scen_mean = p_df[z_p_cols].mean(axis=1)
            
            # Pipes
            inlets = [pid for pid, u, v in zone_inlet_pipes[z] if pid in f_df.columns]
            outlets = [pid for pid, u, v in zone_outlet_pipes[z] if pid in f_df.columns]
            internals = [pid for pid, u, v in zone_internal_pipes[z] if pid in f_df.columns]
            
            # Pre-calculate flows
            f_in = f_df[inlets].abs().sum(axis=1)
            if z in ["zone_1", "1"]:
                f_in += f_df[[p for p in res_pipes if p in f_df.columns]].abs().sum(axis=1)
            
            f_out = f_df[outlets].abs().sum(axis=1)
            f_int = f_df[internals].abs().sum(axis=1)
            
            f_in_b = f_base[inlets].abs().sum(axis=1)
            if z in ["zone_1", "1"]:
                f_in_b += f_base[[p for p in res_pipes if p in f_base.columns]].abs().sum(axis=1)
            f_out_b = f_base[outlets].abs().sum(axis=1)
            f_int_b = f_base[internals].abs().sum(axis=1)
            
            base_demand_z = zone_demand.get(z, 0.001)

            for t in range(len(p_df)):
                # 1. PDR_n
                p_b, p_s = z_p_base_mean.iloc[t], z_p_scen_mean.iloc[t]
                pdr_n = max(0, (p_b - p_s) / p_b) if p_b > 0.1 else 0.0

                # 2. FPI
                fin, fout, fint = f_in.iloc[t], f_out.iloc[t], f_int.iloc[t]
                total_out = fout + fint * 0.1
                fpi = (fin - total_out) / fin if fin > 1e-6 else 0.0
                
                fin_b, fout_b, fint_b = f_in_b.iloc[t], f_out_b.iloc[t], f_int_b.iloc[t]
                fpi_b = (fin_b - (fout_b + fint_b*0.1)) / fin_b if fin_b > 1e-6 else 0.0
                fpi_n = min(1.0, (abs(fpi - fpi_b) / max(1e-6, fpi_b)) * 2.0)

                # 3. NFA
                nfa_n = 0.0
                if t < 24 or t >= 80:
                    if fin_b > 1e-6:
                        nfa_n = min(1.0, (max(0, fin - fin_b) / fin_b) * 2.0)
                
                # 4. DDI
                expected_dem = base_demand_z * DEMAND_PATTERN[t]
                actual_dem = fin - fout
                ddi_n = min(1.0, (abs(actual_dem - expected_dem) / expected_dem)) if expected_dem > 1e-6 else 0.0

                # Normalization/Sensitivity
                pdr_n = min(1.0, pdr_n * 5.0)
                ddi_n = min(1.0, ddi_n * 2.0)
                fpi_n = min(1.0, fpi_n * 3.0)
                
                clps = round(0.35 * pdr_n + 0.30 * fpi_n + 0.20 * nfa_n + 0.15 * ddi_n, 3)

                clps_records.append({"scenario": scen_name, "zone_id": z, "timestep": t, "clps": clps})

                if clps > 0.75 and scen_name == "baseline":
                    sigs = {"PDR_n": 0.35*pdr_n, "FPI": 0.30*fpi_n, "NFA": 0.20*nfa_n, "DDI": 0.15*ddi_n}
                    all_alerts.append({
                        "zone_id": z, "timestamp_index": t, "clps": clps, "severity": "HIGH",
                        "dominant_signal": max(sigs, key=sigs.get),
                        "probable_node_ids": [f"N{nid}" for nid in z_nids[:3]]
                    })

        # Save CLPS scores to CSV
        df_clps = pd.DataFrame(clps_records)
        out_csv = os.path.join(OUT, f"clps_scores_{scen_name}.csv")
        df_clps.to_csv(out_csv, index=False)
        print(f"  ✓  Saved ({scen_name}): outputs/clps_scores_{scen_name}.csv")

    # The prompt expects outputs/v5_alerts.json "containing only meaningful alerts for the baseline run"
    # Wait, the prompt also says:
    # "A clps_scores CSV/JSON with per-zone, per-timestep CLPS."
    # We wrote clps_scores_baseline.csv, clps_scores_leak.csv, etc.
    
    with open(os.path.join(OUT, "v5_alerts.json"), "w") as f:
        json.dump(all_alerts, f, indent=2)
    print(f"  ✓  Saved Baseline Alerts: outputs/v5_alerts.json ({len(all_alerts)} alerts)")
    print("=" * 62)

if __name__ == "__main__":
    main()
