# Hydro-Equity Engine
### Smart Water Pressure Management for Equitable Water Supply — Solapur Municipal Corporation

**Team Devsters · SAMVED-2026 · RV College of Engineering, Bengaluru**

---

## What This Project Is

Solapur Municipal Corporation (SMC) supplies water to 10+ lakh citizens through a network of Elevated Storage Reservoirs and ward-level pipelines under intermittent supply conditions. Despite having adequate bulk water, the city faces persistent pressure imbalance — low-elevation zones receive excess pressure while tail-end and high-elevation zones receive little or none.

The **Hydro-Equity Engine** converts the real SMC pipe network into a hydraulically simulated, equity-scored, anomaly-detecting web platform for municipal engineers.

The core insight: the problem in Solapur is not water scarcity. It is **pressure governance and distribution intelligence**.

---

## Current State of the Project (Phase 1 & 2 Complete)

The simulation, anomaly analytics engines, and API bridges are fully integrated. The dashboard is no longer a mockup; it renders live, mathematical calculations derived from the structural physics of the Solapur pipe network.

- The full SMC pipeline GeoJSON has been parsed, cleaned, and structured into CSVs.
- A WNTR hydraulic simulation (V3) runs on the real Zone 1 network and produces pressure/flow CSVs.
- **V4 Equity Engine is now fully active:** Dynamically calculates the Hydraulic Equity Index (HEI) by analyzing the physical distance of nodes from ESRs and calculating exact pressure drops from the WNTR CSV outputs.
- **NEW:** V5 (Leak Detection) and V6 (Burst Prediction) scripts mathematically compute anomaly risks based on the physical simulation data.
- A Flask backend automatically detects these real simulation outputs and serves JSON analytics to the dashboard.
- A single-file Leaflet dashboard dynamically renders the real pipe network, changing pipeline colours, alert panels, and map markers based on the backend's Python calculations.


---

## What Was Built

### V1 — Data Foundation

**Script:** `scripts/v1_data_foundation.py`

Parses four GeoJSON files from SMC's public GIS portal and produces all structured CSVs used by every other component.

**Source data:**
`https://smcgis.solapurcorporation.org/gis_all_layer.aspx?Pram=Water`

**Files produced in `Data/`:**

| File | Rows | Contents |
|---|---|---|
| `pipe_segments.csv` | 10,160 | Cleaned pipes with material, diameter, length, zone, age, lifespan, Hazen-Williams C |
| `nodes_with_elevation.csv` | 14,085 | Pipe junction nodes with coordinates and elevation |
| `zone_demand.csv` | 8 | Demand estimates per zone in L/s |
| `infrastructure_points.csv` | 66 | ESRs, storage tanks, pumping stations |

**What the script does:**
1. Parses `pipeline.geojson`, `water_source.geojson`, `storage_tank.geojson`, `raw_station.geojson`
2. Standardises zone IDs — `"Zone 5"`, `"5"`, `"z5"` → `"zone_5"`
3. Standardises material names using exact + substring matching — `"CAST IRON (CI)"`, `"DUCTILE IRON (DI)"`, `"POLY VINYL CHLORIDE (PVC)"` etc. map to short codes. This was a bug in the original: GIS uses full names, not codes, so plain equality checks silently fell through to Unknown for every pipe.
4. Assigns correct specifications per material:

| Code | Material | Assumed Age | Lifespan | HW-C |
|---|---|---|---|---|
| CI | Cast Iron | 35 yr | 50 yr | 100 |
| DI | Ductile Iron | 15 yr | 60 yr | 130 |
| PVC | PVC | 10 yr | 25 yr | 150 |
| GI | Galvanised Iron | 30 yr | 40 yr | 100 |
| AC | Asbestos Cement | 40 yr | 50 yr | 100 |
| MS | Mild Steel | 25 yr | 45 yr | 110 |
| CC | Cement Concrete | 40 yr | 60 yr | 90 |

5. Estimates zone demand using Census 2011 Solapur population (951,558) × growth factor (1.012^13) × 135 L/person/day (CPHEEO standard), distributed proportionally by pipe count per zone
6. Extracts infrastructure centroids handling Point, Polygon, and MultiPolygon geometry (SMC's `water_source.geojson` is MultiPolygon, not Point — plain Point-only filters load 0 features)
7. Flags data quality per row: `complete`, `missing_diameter`, `missing_zone`
8. Writes `DATA_CONTRACT.md` documenting every column, unit, and assumption

**Known assumption in this phase:** Elevation is simulated as uniform random 440–470m (seed=42, fully deterministic). Solapur sits in a relatively flat region. Real SRTM elevation tiles from NASA EARTHDATA can replace this later.

---

### V3 — Hydraulic Simulation Engine

**Script:** `scripts/simulation_engine.py`

Builds a WNTR hydraulic model from the real SMC Zone 1 network and runs a 24-hour simulation.

**Critical problem that was fixed and why it matters:**

The raw GIS pipe data has 1,150+ disconnected mini-clusters per zone. Pipes were digitised independently with no topology enforcement — adjacent pipes at the same physical junction have slightly different endpoint coordinates. Two bugs caused the simulation to fail:

- **Rounding at 6 decimals (~0.1m):** Most physically-connected pipes were treated as disconnected because their endpoints differed by a few millimetres in float representation. Fixed by rounding to **4 decimals (~11m tolerance)** — pipes at the same junction now match correctly.
- **Running all clusters with one reservoir:** With one reservoir connected to one cluster, every other cluster has demand but no supply path. EPANET reports "system unbalanced" at every timestep. Fixed by extracting the **Largest Connected Component (LCC)** only before building the WNTR model.

Zone 1 was chosen because it has the largest LCC after these fixes: 820 pipes and 716 nodes.

**What the script does:**
1. Loads `nodes_with_elevation.csv` and `pipeline.geojson`
2. Filters to `DEMO_ZONE = "1"` (configurable at top of file)
3. Matches pipe endpoint coordinates to node IDs using 4-decimal rounding
4. Extracts the Largest Connected Component using NetworkX — discards all isolated mini-clusters
5. Builds a directed NetworkX DiGraph of the LCC (directed is required for V4 path calculations)
6. Builds a WNTR `WaterNetworkModel`:
   - 96 timesteps × 15 minutes = 24-hour simulation
   - Pressure-Driven Demand (PDD) — correct for intermittent supply conditions
   - Hazen-Williams headloss
   - 24-hour demand pattern applied to all junctions
   - Water source centroids loaded as reservoirs
   - Fallback reservoir at highest-elevation node
7. Runs `EpanetSimulator.run_sim()` for 4 scenarios:
   - `baseline` — normal operating conditions
   - `leak` — one pipe diameter reduced by 30%
   - `valve_close` — one upstream pipe set to Closed
   - `demand_surge` — one zone demand multiplied by 1.5×
8. Saves pressure and flow CSVs to `outputs/`
9. Exports `solapur_network.inp`

**Demand pattern used (shared with leak detection logic):**

```
00:00–05:59  →  0.05×   night
06:00–07:59  →  2.50×   morning peak
08:00–16:59  →  1.00×   daytime
17:00–19:59  →  2.00×   evening peak
20:00–23:59  →  0.05×   night
```

**Files written to `outputs/` after a successful run:**
```
pressure_baseline.csv
flow_baseline.csv
pressure_scenario_A_leak.csv
pressure_scenario_B_valve_close.csv
pressure_scenario_C_demand_surge.csv
solapur_network.inp
```

---

### V4 — Equity Scoring Engine

**Script:** `scripts/v4_equity_engine.py`

Computes the exact Hydraulic Equity Index (HEI) for every zone by analyzing the physical network topology and real pressure data from V3.

**What the script does:**
1. Loads `pressure_baseline.csv` from V3 outputs and the network graph from the simulation
2. Maps every node to its physical geographic zone using spatial analysis
3. Geometrically sorts nodes by distance from ESRs to identify the 15% furthest "tail-end" nodes per zone
4. Reads the 96-timestep physics matrix from pressure CSVs
5. Calculates the exact HEI ratio for each timestep: (avg tail-end pressure / avg core pressure)
6. Computes Zone Equity Score (ZES) as the mean HEI across all 96 timesteps
7. Outputs `outputs/v4_equity.json` with zone-level HEI values and City-Wide Equity Index (CWEI)

**Files produced:**
- `outputs/v4_equity.json` — HEI scores per zone, timestep, and CWEI aggregate

---

### V5 & V6 — Anomaly Analytics Engines

**Scripts:** `scripts/v5_leak_detect.py` | `scripts/v6_burst_predict.py`

These act as the intelligence layer over the V3 physics engine.
- **V5 Leak Detection:** Compares the normal `baseline` matrices against the anomaly scenarios. It calculates the Pressure Drop Rate (PDR), Flow-Pressure Imbalance (FPI), and Night Flow Anomaly (NFA) to generate mathematically backed alerts in `v5_alerts.json`.
- **V6 Burst Prediction:** Reads the absolute maximum pressure experienced by every physical pipe over 24 hours. Compares this against the structural limits of Cast Iron/Ductile Iron/PVC to calculate a Pipe Stress Score (PSS), outputting `v6_burst.json`.

---

### V2 — Backend API

**Script:** `backend/app.py`

Flask server on port 5000. Serves all data to the dashboard.

**Smart mode detection:** On startup, checks whether real WNTR pressure CSVs exist in `outputs/`. If yes → serves real hydraulic simulation data. If no → falls back to a formula approximation (nearest-ESR distance + friction loss estimate). The dashboard works identically in both modes. No code changes needed — it switches automatically.

Check which mode is active:
```
GET http://localhost:5000/data-status
```

**All working endpoints:**

| Endpoint | What it returns |
|---|---|
| `/pipeline?scenario=&hour=` | Pipeline GeoJSON with `sim_pressure` and `sim_color` per pipe |
| `/infrastructure` | ESR, tank, water source locations |
| `/tanks` | `storage_tank.geojson` passthrough |
| `/sources` | `water_source.geojson` passthrough |
| `/pressure?scenario=&hour=` | Node pressure array |
| `/equity?scenario=&hour=` | HEI score per zone + CWEI (reads from v4_equity.json) |
| `/simulate?scenario=&zone=` | 24-hour pressure timeline for a zone |
| `/alerts?scenario=` | CLPS-based alert list |
| `/recommendations?scenario=` | Valve/pump action text per scenario |
| `/burst-risk` | Top-10 pipe segments by Pipe Stress Score |
| `/zone-demand` | Zone demand table |
| `/data-status` | Mode + which scenario CSVs are loaded |

Scenario parameter values: `baseline`, `leak`, `valve`, `surge`

---

### V9 — Web Dashboard

**File:** `frontend/index.html`

Single HTML file. No build step. Open directly in browser.

**What works right now in the browser:**

**Pipeline map:** The real Solapur pipe network is rendered on OpenStreetMap (light government theme). Every pipe is coloured by its simulated hydraulic pressure. Clicking any pipe shows the pressure value, zone, and material in a tooltip.

Pressure colour scale:

| Range | Colour | Meaning |
|---|---|---|
| < 10m | Red | Critical |
| 10–20m | Orange | Low |
| 20–30m | Yellow | Below target |
| 30–40m | Light green | Acceptable |
| 40–60m | Dark green | Good |
| > 60m | Blue | Over-pressurised |

**Scenario switching:** Four buttons — Baseline, Leak Event, Valve Closure, Demand Surge. Each button reloads pipe colours, alerts, and recommendations from the backend. The visual difference between scenarios is visible on the map.

**Alerts panel:** Active alerts for the current scenario — zone, CLPS score, alert level (HIGH / MODERATE), dominant signal type.

**Recommendations panel:** Actionable text instructions per scenario with estimated HEI improvement.

**Layer toggles:** Pipelines, Node Pressure overlay, ESR Markers, Water Sources, Zone Boundary Overlay, Leak Alert Markers.

**24-hour pressure chart:** Chart.js line chart for any zone showing pressure over the full day. The morning and evening demand peaks are visible.

**Zone equity panel:** Each zone with its HEI score, status badge, and bar indicator.

**CWEI bar:** City-Wide Equity Index, worst zone, best zone, NRW estimate.

**Simulation clock:** Cycles through the 24-hour day in the header.

---

## Mathematical Models

These formulas are defined and used in the system. In Phase 1, HEI and PSS values are formula-approximated. They become real once WNTR CSVs are connected to V4/V5/V6 computation.

### Hydraulic Equity Index (HEI)
```
HEI(zone, t) = avg_pressure(tail-end nodes, t) / avg_pressure(all zone nodes, t)
ZES(zone)    = mean(HEI across all 96 timesteps)
CWEI         = mean(ZES across all zones)
```

| HEI | Status | Action |
|---|---|---|
| ≥ 0.85 | Equitable (Green) | Monitor only |
| 0.70–0.85 | Moderate (Orange) | Review valve settings |
| < 0.70 | Severe (Red) | Immediate intervention |
| > 1.30 | Over-pressurised (Purple) | Throttle upstream |

### Composite Leak Probability Score (CLPS)
```
CLPS = (0.35 × PDR_n) + (0.30 × FPI) + (0.20 × NFA) + (0.15 × DDI)

PDR_n = |ΔP/Δt| / 2.0
FPI   = (Q_inlet − Q_outlet) / Q_inlet   [guard: FPI=0 if Q_inlet=0]
NFA   = Q_night / (Q_daily_avg × 0.05)   [only 01:00–04:00, else 0]
DDI   = |Q_actual − Q_expected| / Q_expected

CLPS > 0.75  →  HIGH alert
0.50–0.75    →  MODERATE
< 0.50       →  Normal
```

### Pipe Stress Score (PSS)
```
PSS = (0.40 × PSI_n) + (0.35 × CFF_n) + (0.25 × ADF)

PSI_n = max(0, (P_max − P_design) / P_design)
CFF_n = min((annual_cycles × assumed_age) / design_fatigue, 2.0) / 2.0
ADF   = min(assumed_age / design_lifespan, 1.0)
```

---

## File Structure

```text
solapur_water_project/
│
├── Data/
│   ├── pipeline.geojson              # Raw SMC GIS pipeline data (10,160 features)
│   ├── pipe_segments.csv             # Cleaned pipes with analysis columns
│   ├── nodes_with_elevation.csv      # Junction nodes with elevation
│   └── ... (other GIS mappings)
│
├── scripts/
│   ├── v1_data_foundation.py         # Parses GeoJSON → CSVs
│   ├── simulation_engine.py          # V3: WNTR model → pressure/flow CSVs
│   ├── v4_equity_engine.py           # V4: HEI calculation → v4_equity.json
│   ├── v5_leak_detect.py             # V5: CLPS logic → v5_alerts.json
│   └── v6_burst_predict.py           # V6: PSS logic → v6_burst.json
│
├── backend/
│   └── app.py                        # Flask API serving all endpoints
│
├── frontend/
│   └── index.html                    # Complete single-file dashboard
│
├── outputs/                          # Generated automatically (not committed)
│   ├── pressure_baseline.csv
│   ├── flow_baseline.csv
│   ├── pressure_scenario_... (3 files)
│   ├── v4_equity.json
│   ├── v5_alerts.json
│   └── v6_burst.json
│
├── DATA_CONTRACT.md                  # Schema documentation
└── requirements.txt


---

## How to Run

### Requirements

- Python 3.10+, 64-bit
- On Windows: **Microsoft C++ Build Tools** required for WNTR
  - Download: `https://visualstudio.microsoft.com/visual-cpp-build-tools/`
  - Select "Desktop development with C++"

### Setup

```bash
git clone https://github.com/your-username/solapur_water_project.git
cd solapur_water_project

python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux

pip install -r requirements.txt
```

### Run V1 (only if regenerating CSVs)

```bash
python scripts/v1_data_foundation.py
```

All CSVs are already committed. Only run this if you change the GeoJSON source data.

### Run V3 (generates real pressure data)

```bash
python scripts/simulation_engine.py
```

Takes 2–5 minutes. Successful output looks like:

```
[V3] Loaded    3 water_source features
[V3] Filtered to Zone 1: 2365 pipe features
[V3] Connectivity: 372 components — keeping LCC (716 nodes), discarding 371 mini-clusters
[V3] LCC: 820 pipes | 716 nodes — fully connected ✓
[V3] Running simulation: baseline ...
[V3] ✓  baseline: 96 timesteps × 716 nodes
[V3] ✓  Saved: outputs/pressure_baseline.csv
[V3] ✓  V3 COMPLETE
```

### Run Analytics Engines (V4, V5 & V6)

```bash
python scripts/v4_equity_engine.py
python scripts/v5_leak_detect.py
python scripts/v6_burst_predict.py


### Start the backend

```bash
python backend/app.py
```

Look for:
```
Data mode: ★ REAL WNTR DATA      ← if simulation_engine.py ran successfully
Data mode: ⚠ Formula Approx      ← if outputs/ CSVs are missing (still works)
```

### Open the dashboard

Open `frontend/index.html` in a browser directly, or:

```bash
cd frontend && python -m http.server 8080
# open http://localhost:8080
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Microsoft Visual C++ 14.0 required` | Install C++ Build Tools, then re-run `pip install wntr` |
| `simulation did not converge` | In `simulation_engine.py` find `base_demand=0.001` → change to `base_demand=0.0001` |
| `Loaded 0 water_source features` | You have the old version — replace with current `simulation_engine.py` which handles MultiPolygon |
| Backend shows `Formula Approx` | Run `python scripts/simulation_engine.py` first |
| Pipes not on map | Confirm backend is running. Check browser console — usually a CORS error means Flask isn't running |

---

## Where to Continue From Here

Phases 1 and 2 are complete. Continue with Phase 3 now according to roadmap pdf.

The next work to build on top of this:

## Team

| Name | Program | Email |
|---|---|---|
| Prabhav Tiwari (Lead) | B.E. CSE (AIML) | prabhavatiwari.ci25@rvce.edu.in |
| Yashraj Pala | B.E. CSE | yashrajpala.cs25@rvce.edu.in |
| Shashwat Utkarsh | B.E. Mechanical | shashwatutkarsh.me25@rvce.edu.in |
| Tejash Pathak | B.E. CSE | tesjaShmanojp.cs25@rvce.edu.in |
| Shaurya Khanna | B.E. CSE (CY) | shauryakhanna.cy25@rvce.edu.in |

**Mentor:** Dr. Sham Aan MP · shamaan.mp@rvce.edu.in · RVCE Bengaluru

---

*Hydro-Equity Engine · Team Devsters · SAMVED-2026*
