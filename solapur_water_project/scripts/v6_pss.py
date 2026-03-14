"""
v6_pss.py
V6 - Pipe Stress Score (PSS) & Burst Prediction
"""

import os, json
import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "Data")
OUT  = os.path.join(ROOT, "outputs")

def get_design_pressure(material, diameter_m):
    """
    Design pressure lookup (in meters of head).
    Appropriated from standard municipal engineering guidelines.
    """
    mat = str(material).upper().strip()
    if mat == "DI": return 100.0
    if mat == "MS": return 100.0
    if mat == "PVC": return 60.0
    if mat == "AC": return 60.0
    if mat == "GI": return 60.0
    if mat == "CI":
        if diameter_m >= 0.2: return 80.0
        else: return 60.0
    return 60.0  # Unknown/fallback

def risk_level(pss):
    if pss >= 0.75: return "HIGH"
    if pss >= 0.50: return "MODERATE"
    return "LOW"

def main():
    print("=" * 62)
    print("  V6 · Pipe Stress Score (PSS) & Burst Prediction")
    print("=" * 62)

    # 1. Load Data
    pipes_df = pd.read_csv(os.path.join(DATA, "pipe_segments.csv"))
    valid_pipes = pipes_df.dropna(subset=["start_node_id", "end_node_id", "segment_id"]).copy()
    valid_pipes["start_node_id"] = valid_pipes["start_node_id"].astype(int)
    valid_pipes["end_node_id"] = valid_pipes["end_node_id"].astype(int)
    valid_pipes["segment_id"] = valid_pipes["segment_id"].astype(int)

    try:
        p_base = pd.read_csv(os.path.join(OUT, "pressure_fullcity_baseline.csv"), index_col=0)
    except FileNotFoundError:
        print("✗ Baseline CSV not found. Run simulation_engine.py all first.")
        return

    # 2. Compute Max/Min Pressures per Node
    node_p_max = p_base.max()
    node_p_min = p_base.min()
    node_p_max_dict = {col: val for col, val in zip(node_p_max.index, node_p_max.values)}
    node_p_min_dict = {col: val for col, val in zip(node_p_min.index, node_p_min.values)}

    pss_records = []
    top_pipes = []

    for _, row in valid_pipes.iterrows():
        pid = row["segment_id"]
        u = f"J{row['start_node_id']}"
        v = f"J{row['end_node_id']}"

        u_max = node_p_max_dict.get(u, 0)
        v_max = node_p_max_dict.get(v, 0)
        u_min = node_p_min_dict.get(u, 0)
        v_min = node_p_min_dict.get(v, 0)

        pipe_p_max = (u_max + v_max) / 2.0
        pipe_p_min = (u_min + v_min) / 2.0

        material = row.get("material", "Unknown")
        dia = row.get("diameter_m", 0.1)
        p_design = get_design_pressure(material, dia)

        # Signal 1: PSI_n (Pressure Stress Index)
        psi_val = pipe_p_max / p_design
        psi_n = min(1.0, max(0.0, psi_val))

        # Signal 2: CFF_n (Cyclic Fatigue Factor)
        # Driven by daily pressure variation
        cff_val = max(0, pipe_p_max - pipe_p_min) / (p_design * 0.5) # normal var is 50% design
        cff_n = min(1.0, cff_val)

        # Signal 3: ADF (Age Degradation Factor)
        age = float(row.get("assumed_age_years", 35))
        life = float(row.get("design_lifespan_years", 50))
        if life > 0:
            adf_val = age / life
        else:
            adf_val = 1.0
        adf = min(1.0, max(0.0, adf_val))

        # PSS = 0.40 * PSI_n + 0.35 * CFF_n + 0.25 * ADF
        pss = 0.40 * psi_n + 0.35 * cff_n + 0.25 * adf
        pss = round(float(pss), 3)

        sigs = {
            "PSI_n": 0.40 * psi_n,
            "CFF_n": 0.35 * cff_n,
            "ADF":   0.25 * adf
        }
        dom = max(sigs, key=sigs.get)
        r_level = risk_level(pss)

        pss_records.append({
            "segment_id": pid,
            "pss": pss,
            "risk_level": r_level,
            "dominant_factor": dom,
            "psi_n": round(float(psi_n),3),
            "cff_n": round(float(cff_n),3),
            "adf": round(float(adf),3)
        })

        top_pipes.append({
            "segment_id": pid,
            "pss": pss,
            "risk_level": r_level,
            "dominant_factor": dom,
            "start_lon": row["start_lon"],
            "start_lat": row["start_lat"],
            "end_lon": row["end_lon"],
            "end_lat": row["end_lat"],
            "material": material,
            "age": age,
            "description": f"High burst risk identified ({r_level}). Dominant factor: {dom}."
        })

    # Save Pipe Stress Scores
    df_pss = pd.DataFrame(pss_records)
    out_csv = os.path.join(OUT, "v6_pipe_stress_scores.csv")
    df_pss.to_csv(out_csv, index=False)
    print(f"  ✓  Saved ({len(df_pss)} pipes): outputs/v6_pipe_stress_scores.csv")

    # Save Top 10 High Risk Pipes
    top_pipes.sort(key=lambda x: x["pss"], reverse=True)
    top_10 = top_pipes[:10]
    out_json = os.path.join(OUT, "v6_burst_top10.json")
    with open(out_json, "w") as f:
        json.dump(top_10, f, indent=2)
    print(f"  ✓  Saved Top 10 Risks: outputs/v6_burst_top10.json")
    print("=" * 62)

if __name__ == "__main__":
    main()
