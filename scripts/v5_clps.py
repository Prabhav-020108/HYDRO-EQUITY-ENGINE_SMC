"""
v5_clps.py  ·  scripts/
V5 - Composite Leak & Pressure Score (CLPS)
Phase 3.5 FINAL — generates alerts per scenario.

Thresholds:
  baseline : 0.08  (lower to surface structural stress signals)
  leak     : 0.10  (pressure decay detectable at low threshold)
  valve    : 0.10  (flow imbalance from valve closure)
  surge    : 0.12  (demand deviation dominant signal)
"""

import os, json
import pandas as pd
import numpy as np
import networkx as nx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "Data")
OUT  = os.path.join(ROOT, "outputs")

DEMAND_PATTERN = []
for _h in range(24):
    if   0  <= _h < 6:  _m = 0.05
    elif 6  <= _h < 8:  _m = 2.50
    elif 8  <= _h < 17: _m = 1.00
    elif 17 <= _h < 20: _m = 2.00
    else:               _m = 0.05
    DEMAND_PATTERN.extend([_m] * 4)

# Per-scenario alert threshold
THRESHOLDS = {
    "baseline": 0.08,
    "leak":     0.10,
    "valve":    0.10,
    "surge":    0.12,
}


def main():
    print("=" * 62)
    print("  V5 · Composite Leak & Pressure Score (CLPS)")
    print("=" * 62)

    # ── Load data ──────────────────────────────────────────────
    nodes_df = pd.read_csv(os.path.join(DATA, "nodes_with_elevation.csv"))
    pipes_df = pd.read_csv(os.path.join(DATA, "pipe_segments.csv"))

    zone_demand = {}
    zdpath = os.path.join(DATA, "zone_demand.csv")
    if os.path.exists(zdpath):
        z_df = pd.read_csv(zdpath)
        zone_demand = dict(zip(z_df["zone_id"], z_df["base_lps"] / 1000.0))

    valid_pipes = pipes_df.dropna(subset=["start_node_id", "end_node_id"]).copy()

    node_zone = dict(zip(nodes_df["node_id"].astype(int), nodes_df["zone_id"]))
    zones     = sorted(nodes_df["zone_id"].dropna().unique())

    # ── Build zone pipe mappings ───────────────────────────────
    zone_inlet_pipes    = {z: [] for z in zones}
    zone_internal_pipes = {z: [] for z in zones}
    zone_outlet_pipes   = {z: [] for z in zones}

    for _, row in valid_pipes.iterrows():
        pu  = int(row["start_node_id"])
        pv  = int(row["end_node_id"])
        pid = f"P{int(row['segment_id'])}"
        zu  = node_zone.get(pu, "out")
        zv  = node_zone.get(pv, "out")
        if zu != zv:
            if zv in zones: zone_inlet_pipes[zv].append((pid, pu, pv))
            if zu in zones: zone_outlet_pipes[zu].append((pid, pu, pv))
        else:
            if zu in zones: zone_internal_pipes[zu].append((pid, pu, pv))

    # ── Load baseline CSVs ─────────────────────────────────────
    try:
        p_base = pd.read_csv(os.path.join(OUT, "pressure_fullcity_baseline.csv"), index_col=0)
        f_base = pd.read_csv(os.path.join(OUT, "flow_fullcity_baseline.csv"),     index_col=0)
    except FileNotFoundError:
        print("✗ Baseline CSVs not found. Run: python scripts/simulation_engine.py all")
        return

    res_pipes = [c for c in f_base.columns if c.startswith("SourcePipe")]

    scenarios = {
        "baseline": ("pressure_fullcity_baseline.csv",              "flow_fullcity_baseline.csv"),
        "leak":     ("pressure_fullcity_scenario_A_leak.csv",       "flow_fullcity_scenario_A_leak.csv"),
        "valve":    ("pressure_fullcity_scenario_B_valve_close.csv","flow_fullcity_scenario_B_valve_close.csv"),
        "surge":    ("pressure_fullcity_scenario_C_demand_surge.csv","flow_fullcity_scenario_C_demand_surge.csv"),
    }

    alerts_by_scenario = {k: [] for k in scenarios}

    # ── Process each scenario ──────────────────────────────────
    for scen_name, (p_file, f_file) in scenarios.items():
        p_path = os.path.join(OUT, p_file)
        f_path = os.path.join(OUT, f_file)
        if not os.path.exists(p_path) or not os.path.exists(f_path):
            print(f"  [SKIP] {scen_name} — files not found")
            continue

        p_df = pd.read_csv(p_path, index_col=0)
        f_df = pd.read_csv(f_path, index_col=0)

        threshold  = THRESHOLDS.get(scen_name, 0.10)
        clps_rows  = []
        # Track best alert per zone to deduplicate at generation time
        best_per_zone = {}

        for z in zones:
            z_nids  = [nid for nid, zz in node_zone.items() if zz == z]
            z_pcols = [f"J{nid}" for nid in z_nids if f"J{nid}" in p_df.columns]
            if not z_pcols:
                continue

            z_p_base_mean = p_base[z_pcols].mean(axis=1)
            z_p_scen_mean = p_df[z_pcols].mean(axis=1)

            inlets    = [pid for pid, u, v in zone_inlet_pipes[z]    if pid in f_df.columns]
            outlets   = [pid for pid, u, v in zone_outlet_pipes[z]   if pid in f_df.columns]
            internals = [pid for pid, u, v in zone_internal_pipes[z] if pid in f_df.columns]

            f_in   = f_df[inlets].abs().sum(axis=1)
            f_out  = f_df[outlets].abs().sum(axis=1)
            f_int  = f_df[internals].abs().sum(axis=1)
            f_in_b = f_base[inlets].abs().sum(axis=1)
            f_out_b= f_base[outlets].abs().sum(axis=1)
            f_int_b= f_base[internals].abs().sum(axis=1)

            if z in ["zone_1", "1"]:
                rp_in_scen = f_df[[p for p in res_pipes if p in f_df.columns]].abs().sum(axis=1)
                rp_in_base = f_base[[p for p in res_pipes if p in f_base.columns]].abs().sum(axis=1)
                f_in   = f_in   + rp_in_scen
                f_in_b = f_in_b + rp_in_base

            base_demand_z = zone_demand.get(z, 0.001)
            num_steps     = min(len(p_df), len(DEMAND_PATTERN))

            for t in range(num_steps):
                p_b = float(z_p_base_mean.iloc[t])
                p_s = float(z_p_scen_mean.iloc[t])

                # Signal 1 — PDR_n: pressure decay relative to baseline
                pdr_n = max(0.0, (p_b - p_s) / p_b) if p_b > 0.1 else 0.0

                # Signal 2 — FPI: flow-pressure imbalance vs baseline
                fin    = float(f_in.iloc[t])
                fout   = float(f_out.iloc[t])
                fint   = float(f_int.iloc[t])
                fin_b  = float(f_in_b.iloc[t])
                fout_b = float(f_out_b.iloc[t])
                fint_b = float(f_int_b.iloc[t])

                fpi   = (fin  - (fout  + fint  * 0.1)) / fin   if fin   > 1e-6 else 0.0
                fpi_b = (fin_b - (fout_b + fint_b * 0.1)) / fin_b if fin_b > 1e-6 else 0.0
                fpi_n = min(1.0, abs(fpi - fpi_b) / max(1e-6, abs(fpi_b)) * 2.0)

                # Signal 3 — NFA: night flow anomaly (01:00–04:00 only)
                nfa_n = 0.0
                if t < 24 or t >= 80:
                    if fin_b > 1e-6:
                        nfa_n = min(1.0, max(0.0, fin - fin_b) / fin_b * 2.0)

                # Signal 4 — DDI: demand deviation index
                expected = base_demand_z * DEMAND_PATTERN[t]
                actual   = fin - fout
                ddi_n    = min(1.0, abs(actual - expected) / expected) if expected > 1e-6 else 0.0

                # Amplify signals for sensitivity
                pdr_n = min(1.0, pdr_n * 5.0)
                fpi_n = min(1.0, fpi_n * 3.0)
                ddi_n = min(1.0, ddi_n * 2.0)

                clps = round(0.35 * pdr_n + 0.30 * fpi_n + 0.20 * nfa_n + 0.15 * ddi_n, 3)
                clps_rows.append({"scenario": scen_name, "zone_id": z, "timestep": t, "clps": clps})

                if clps > threshold:
                    sigs = {
                        "PDR_n": 0.35 * pdr_n,
                        "FPI":   0.30 * fpi_n,
                        "NFA":   0.20 * nfa_n,
                        "DDI":   0.15 * ddi_n,
                    }
                    alert = {
                        "zone_id":           z,
                        "timestamp_index":   t,
                        "clps":              clps,
                        "severity":          "HIGH" if clps > 0.5 else "moderate",
                        "dominant_signal":   max(sigs, key=sigs.get),
                        "probable_node_ids": [f"N{nid}" for nid in z_nids[:3]],
                    }
                    # Keep highest CLPS alert per zone
                    if z not in best_per_zone or clps > best_per_zone[z]["clps"]:
                        best_per_zone[z] = alert

        # Collect best alert per zone into scenario list
        alerts_by_scenario[scen_name] = sorted(
            best_per_zone.values(), key=lambda x: x["clps"], reverse=True
        )

        df_clps = pd.DataFrame(clps_rows)
        df_clps.to_csv(os.path.join(OUT, f"clps_scores_{scen_name}.csv"), index=False)
        print(f"  ✓  {scen_name}: {len(alerts_by_scenario[scen_name])} zone alerts  "
              f"(threshold={threshold}, {len(clps_rows)} timesteps scored)")

    # ── Save ───────────────────────────────────────────────────
    with open(os.path.join(OUT, "v5_alerts.json"), "w") as f:
        json.dump(alerts_by_scenario, f, indent=2)

    total = sum(len(v) for v in alerts_by_scenario.values())
    print(f"  ✓  Saved: outputs/v5_alerts.json  ({total} total alerts)")
    print("=" * 62)


if __name__ == "__main__":
    main()