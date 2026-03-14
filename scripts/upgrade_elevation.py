"""
Hydro-Equity Engine — Phase 3.5 Elevation Upgrade
Replaces simulated elevation (440–470m) with real Open-Meteo elevation data.
Fetches in batches of 100 nodes (free API, no auth required).
Run BEFORE re-running simulation_engine.py.
"""

import os, csv, time, requests
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, '..', 'Data')

INPUT_FILE  = os.path.join(DATA, 'nodes_with_elevation.csv')
OUTPUT_FILE = os.path.join(DATA, 'nodes_with_elevation.csv')  # overwrites in-place
BACKUP_FILE = os.path.join(DATA, 'nodes_with_elevation_simulated_backup.csv')
BATCH_SIZE  = 100   # Open-Meteo free tier maximum per request
SLEEP_SECS  = 0.5   # Be polite to the free API

def fetch_elevations_batch(lats, lons):
    """Fetch real elevations for up to 100 lat/lon pairs from Open-Meteo."""
    url = "https://api.open-meteo.com/v1/elevation"
    params = {
        "latitude":  ",".join(str(round(la, 6)) for la in lats),
        "longitude": ",".join(str(round(lo, 6)) for lo in lons),
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("elevation", [None] * len(lats))
    except requests.exceptions.RequestException as e:
        print(f"  [WARN] API error: {e} — keeping simulated values for this batch")
        return [None] * len(lats)

def upgrade():
    if not os.path.exists(INPUT_FILE):
        print(f"[ERROR] {INPUT_FILE} not found. Run v1_data_foundation.py first.")
        return

    # Backup original
    df = pd.read_csv(INPUT_FILE)
    df.to_csv(BACKUP_FILE, index=False)
    print(f"[upgrade_elevation] Backup saved → {BACKUP_FILE}")
    print(f"[upgrade_elevation] Found {len(df)} nodes. Fetching real elevations...")

    all_elevations = []
    total_batches  = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]
        lats  = batch['lat'].tolist()
        lons  = batch['lon'].tolist()
        batch_num = i // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches} ({len(lats)} nodes)...", end=' ')
        elevs = fetch_elevations_batch(lats, lons)

        # If API returned None for any, keep the simulated value
        for j, elev in enumerate(elevs):
            if elev is None:
                original = float(batch.iloc[j].get('elevation', batch.iloc[j].get('elevation_m', 450)))
                elevs[j] = original

        all_elevations.extend(elevs)
        print(f"done. Sample elevation: {elevs[0]:.1f}m")
        time.sleep(SLEEP_SECS)

    # Write back
    elev_col = 'elevation' if 'elevation' in df.columns else 'elevation_m'
    df[elev_col] = all_elevations
    df.to_csv(OUTPUT_FILE, index=False)

    real_count = sum(1 for e in all_elevations if e is not None)
    print(f"\n[upgrade_elevation] ✅ Done.")
    print(f"  Real elevations fetched: {real_count}/{len(df)}")
    print(f"  Min: {min(all_elevations):.1f}m  Max: {max(all_elevations):.1f}m")
    print(f"  Saved → {OUTPUT_FILE}")
    print(f"\n  ⚠  Now re-run: python scripts/simulation_engine.py")

if __name__ == '__main__':
    upgrade()