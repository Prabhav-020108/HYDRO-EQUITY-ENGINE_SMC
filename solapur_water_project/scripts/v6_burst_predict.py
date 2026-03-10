"""
v6_burst_predict.py
Owner: Yashraj
Purpose: Calculates Pipe Stress Score (PSS) using PSI, CFF, and ADF.
Strictly adheres to the Hydro-Equity Engine Project Bible.
"""

import os, json, random
import pandas as pd
import wntr

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "outputs")

def calculate_burst_risks():
    print("="*50)
    print(" V6: BURST PREDICTION ENGINE (PSS)")
    print("="*50)

    inp_path = os.path.join(OUT, "solapur_network.inp")
    base_path = os.path.join(OUT, "pressure_baseline.csv")

    if not os.path.exists(inp_path) or not os.path.exists(base_path):
        print("✗ Fatal: WNTR outputs missing. Run V3 first.")
        return

    print("[V6] Loading WNTR Model...")
    wn = wntr.network.WaterNetworkModel(inp_path)
    df_base = pd.read_csv(base_path, index_col=0)

    # Pre-calculate node metrics
    p_max = df_base.max()
    p_mean = df_base.mean()

    # Calculate zero-crossings (how many times pressure crosses the mean) for CFF
    p_centered = df_base.subtract(p_mean)
    cycles_24h = ((p_centered.shift(1) < 0) & (p_centered >= 0)).sum()

    random.seed(42) # Consistent demo results
    results = []

    print(f"[V6] Scoring {len(wn.pipe_name_list)} pipes...")
    for p_name in wn.pipe_name_list:
        pipe = wn.get_link(p_name)
        n_start = pipe.start_node_name

        if n_start not in p_max:
            continue

        node_max = p_max[n_start]
        annual_cycles = cycles_24h[n_start] * 365

        # Bible logic: Material determines age, lifespan, design pressure, fatigue limit
        mat = random.choice(['CI', 'DI', 'PVC'])
        if mat == 'CI':
            age, lifespan, p_design, fatigue = 35, 50, 60.0, 100000
        elif mat == 'DI':
            age, lifespan, p_design, fatigue = 15, 60, 160.0, 500000
        else: # PVC
            age, lifespan, p_design, fatigue = 10, 25, 60.0, 50000

        # 1. Pressure Surge Index (PSI)
        psi = (node_max - p_design) / p_design
        psi_n = max(0.0, psi)

        # 2. Cycle Fatigue Factor (CFF)
        cff = (annual_cycles * age) / fatigue
        cff_n = min(cff, 2.0)

        # 3. Age Degradation Factor (ADF)
        adf = min(1.0, age / lifespan)

        # 4. Pipe Stress Score (PSS)
        pss = (0.40 * psi_n) + (0.35 * cff_n) + (0.25 * adf)

        # Determine dominant factor for recommendations
        factors = [(psi_n*0.4, 'Pressure Surge'), (cff_n*0.35, 'Cycle Fatigue'), (adf*0.25, 'Age Degradation')]
        dom_val, dom_name = max(factors)

        # Ensure we have coordinates for map pinning
        n_node = wn.get_node(n_start)
        lat = round(n_node.coordinates[1], 4) if n_node.coordinates else 17.655
        lon = round(n_node.coordinates[0], 4) if n_node.coordinates else 75.875

        results.append({
            'segment_id': p_name, 'material': mat, 'assumed_age': age,
            'lat': lat, 'lon': lon,
            'psi_n': round(psi_n, 3), 'cff_n': round(cff_n, 3), 'adf': round(adf, 3), 'pss': round(pss, 3),
            'risk_level': 'HIGH' if pss > 0.80 else 'MODERATE' if pss > 0.55 else 'LOW',
            'dominant_factor': dom_name,
            'summary': f"{mat}, ~{age}yr, PSS: {pss:.2f}"
        })

    # Sort and take top 10 as per Bible
    df_burst = pd.DataFrame(results).sort_values('pss', ascending=False).head(10)
    top_10 = df_burst.to_dict(orient='records')

    for i, r in enumerate(top_10):
        r['rank'] = i + 1

    out_file = os.path.join(OUT, 'v6_burst.json')
    with open(out_file, 'w') as f:
        json.dump({'segments': top_10}, f, indent=2)

    if len(top_10) > 0:
        print(f"[V6] ✓ Top Risk Pipe: {top_10[0]['segment_id']} (PSS: {top_10[0]['pss']})")
    print(f"[V6] Mission Complete. JSON written to {out_file}")

if __name__ == "__main__":
    calculate_burst_risks()
