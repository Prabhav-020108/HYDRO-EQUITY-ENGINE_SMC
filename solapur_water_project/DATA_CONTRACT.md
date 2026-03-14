# DATA_CONTRACT.md
# Team Devsters · Hydro-Equity Engine · SAMVED-2026
# This is the single source of truth for all vertical owners.
# Read this BEFORE writing any code that touches data.

---

## 1. File Locations

All data files live in the `Data/` folder at the project root.

| File | Produced By | Consumed By | Description |
|------|------------|-------------|-------------|
| `pipeline.geojson` | SMC GIS (raw) | V1 | Raw pipeline network |
| `water_source.geojson` | SMC GIS (raw) | V1, V3 | Water treatment plants / sources |
| `storage_tank.geojson` | SMC GIS (raw) | V1, V3 | ESRs and storage tanks |
| `raw_station.geojson` | SMC GIS (raw) | V1, V3 | Raw water pumping stations |
| `nodes_with_elevation.csv` | V1 (load_data.py) | V3, V4 | Junction nodes with simulated elevation |
| `pipe_segments.csv` | V1 (v1_data_foundation.py) | V3, V6 | Cleaned pipes with age + HW-C |
| `zone_demand.csv` | V1 (v1_data_foundation.py) | V3, V5 | Zone-wise demand estimates |
| `infrastructure_points.csv` | V1 (v1_data_foundation.py) | V9 | All point infrastructure combined |

Simulation outputs live in `outputs/`:

| File | Produced By | Consumed By | Description |
|------|------------|-------------|-------------|
| `pressure_baseline.csv` | V3 | V4, V5, V2 | Node pressures, 96 timesteps |
| `flow_baseline.csv` | V3 | V5, V6, V2 | Pipe flowrates, 96 timesteps |
| `pressure_scenario_A_leak.csv` | V3 | V5 | Leak scenario pressures |
| `pressure_scenario_B_valve_close.csv` | V3 | V5 | Valve closure pressures |
| `pressure_scenario_C_demand_surge.csv` | V3 | V5 | Demand surge pressures |
| `solapur_network.inp` | V3 | V2, V8 | EPANET model file |

---

## 2. Table Schemas

### nodes_with_elevation.csv
| Column | Type | Unit | Description |
|--------|------|------|-------------|
| (index) | int | — | Row index — used as node ID in model as `J{index}` |
| lon | float | degrees | Longitude, rounded to 6 decimal places |
| lat | float | degrees | Latitude, rounded to 6 decimal places |
| elevation | float | metres | **SIMULATED** — uniform random 440–470m (seed=42) |

**Note:** Elevation is simulated. Real SRTM elevation should replace this when available.

### pipe_segments.csv
| Column | Type | Unit | Description |
|--------|------|------|-------------|
| segment_id | int | — | Row index |
| start_lon | float | degrees | Start node longitude (6 decimal places) |
| start_lat | float | degrees | Start node latitude (6 decimal places) |
| end_lon | float | degrees | End node longitude (6 decimal places) |
| end_lat | float | degrees | End node latitude (6 decimal places) |
| material | str | — | Standardised: CI, DI, PVC, GI, AC, MS, Unknown |
| diameter_m | float | metres | Pipe diameter. Min 0.05m enforced. |
| length_m | float | metres | Pipe length. Min 1.0m enforced. |
| zone_id | str | — | Standardised: `zone_1`, `zone_2`, … `zone_unknown` |
| pipeline_type | str | — | Raw pipeline type from GIS |
| data_quality_flag | str | — | `complete`, `missing_diameter`, `missing_zone` |
| assumed_age_years | int | years | See material assumptions below |
| design_lifespan_years | int | years | See material assumptions below |
| hw_c_value | int | — | Hazen-Williams C coefficient |

### zone_demand.csv
| Column | Type | Unit | Description |
|--------|------|------|-------------|
| zone_id | str | — | Zone identifier |
| estimated_population | int | persons | 2024 estimate (Census 2011 × 1.012^13) |
| daily_demand_litres | int | L/day | estimated_population × 135 L/day |
| base_lps | float | L/s | daily_demand / 86400 |
| peak_morning_lps | float | L/s | base_lps × 2.5 (6–8 AM) |
| peak_evening_lps | float | L/s | base_lps × 2.0 (5–8 PM) |
| offpeak_lps | float | L/s | base_lps × 0.05 (night) |
| pipe_count | int | — | Number of pipe segments in zone |

### pressure_baseline.csv / pressure_scenario_*.csv
| Column | Type | Description |
|--------|------|-------------|
| (index) | int | Timestep index 0–95 (each = 15 minutes) |
| J0, J1, J2, … | float | Pressure in metres at each junction node |

- Row 0 = 00:00, Row 24 = 06:00, Row 32 = 08:00, Row 48 = 12:00, Row 68 = 17:00, Row 80 = 20:00, Row 95 = 23:45

### flow_baseline.csv / flow_scenario_*.csv
| Column | Type | Description |
|--------|------|-------------|
| (index) | int | Timestep index 0–95 |
| P0, P1, P2, … | float | Flow rate in m³/s for each pipe |
| Source_Fallback, … | float | Reservoir link flowrates |

---

## 3. Demand Pattern (SHARED WITH V5)

96 timesteps × 15 minutes = 24 hours.

```
Hour 0–5   (00:00–05:59)  →  0.05×  (night)
Hour 6–7   (06:00–07:59)  →  2.50×  (morning peak)
Hour 8–16  (08:00–16:59)  →  1.00×  (daytime)
Hour 17–19 (17:00–19:59)  →  2.00×  (evening peak)
Hour 20–23 (20:00–23:59)  →  0.05×  (night)
```

**V5 uses 0.05× to define the night baseline for NFA signal (timesteps 4–16 = 1AM–4AM).**

---

## 4. Material Assumptions

| Material | Assumed Age | Design Lifespan | HW-C | Notes |
|----------|-------------|-----------------|------|-------|
| CI (Cast Iron) | 35 years | 50 years | 100 | Conservative / most common in SMC |
| DI (Ductile Iron) | 15 years | 60 years | 130 | Newer installation |
| PVC | 10 years | 25 years | 150 | Most recent |
| GI (Galvanised Iron) | 30 years | 40 years | 100 | |
| AC (Asbestos Cement) | 40 years | 50 years | 100 | |
| MS (Mild Steel) | 25 years | 45 years | 110 | |
| Unknown | 35 years | 50 years | 100 | Falls back to CI (conservative) |

---

## 5. Node Matching Convention

- Node coordinates are rounded to **6 decimal places** before matching.
- Node name in WNTR model = `J{index}` where index is the CSV row number.
- Reservoir names = `WaterSource_0`, `WaterSource_1`, … and `Source_Fallback`.
- Pipe names in WNTR = `P{index}` where index is the pipe_segments row number.

---

## 6. Key Assumptions & Known Limitations

| # | Assumption | Impact | Fix When Available |
|---|-----------|--------|-------------------|
| 1 | Elevation is simulated (440–470m uniform random, seed=42) | Pressure distribution unrealistic | Replace with SRTM or SMC survey data |
| 2 | Population distributed by pipe count per zone | Zone demand estimates approximate | Use ward-level Census data |
| 3 | All pipe ages are assumed by material type | V6 burst risk scores approximate | Use SMC pipe installation records |
| 4 | EPANET demand model = PDD (pressure-driven) | Conservative | Appropriate for intermittent supply |
| 5 | Reservoir head = highest-elevation node + 20m | Simplification | Use actual ESR tank levels |
| 6 | No real sensor/SCADA data | Full simulation mode | Connect when hackathon data provided |

---

## 7. How to Run Phase 1

```bash
# Step 1 — Environment setup (one time only)
cd solapur_water_project
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install pandas numpy networkx wntr flask flask-cors

# Step 2 — V1: Run data foundation (generates pipe_segments + zone_demand)
python scripts/v1_data_foundation.py

# Step 3 — V3: Run simulation engine (generates pressure/flow CSVs + scenarios)
python scripts/simulation_engine.py

# Step 4 — Start backend API
python backend/app.py

# Step 5 — Open frontend (in browser)
# Open: frontend/index.html  OR  visit http://localhost:5000
```

---

*Last updated: Phase 1 completion — Team Devsters, SAMVED-2026*