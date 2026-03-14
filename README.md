# Hydro-Equity Engine
### Smart Water Pressure Management for Equitable Water Supply — Solapur Municipal Corporation

**Team Devsters · SAMVED-2026 · RV College of Engineering, Bengaluru**

---

## What This Project Is

Solapur Municipal Corporation (SMC) supplies water to 10+ lakh citizens through a network of Elevated Storage Reservoirs and ward-level pipelines under intermittent supply conditions. Despite having adequate bulk water, the city faces persistent pressure imbalance — low-elevation zones receive excess pressure while tail-end and high-elevation zones receive little or none.

The **Hydro-Equity Engine** converts the real SMC pipe network into a hydraulically simulated, equity-scored, anomaly-detecting web platform for municipal engineers.

The core insight: the problem in Solapur is not water scarcity. It is **pressure governance and distribution intelligence**.

---

## Project Scope and Phases

- **Phase 1 – Data foundation & simulation (V1, V3)** – implemented.
- **Phase 2 – Analytics engines (V4 HEI, V5 CLPS, V6 PSS)** – implemented.
- **Phase 3 – Backend & dashboard wiring (V2, V9)** – implemented. 
- **Phase 4+ – V7, V8, V10, V11, V12, V13** – planned / not implemented yet. (Includes V7 recommendation engine, V8 scenario panel, V10 governance, V11 auth, V12 mobile, V13 theft intelligence).

---

## Current Architecture

- **Data:** GeoJSON → V1 → CSVs.
- **Simulation:** V3 WNTR → `pressure_baseline.csv`, `flow_baseline.csv` (and scenario CSVs).
- **Analytics:** V4, V5, V6 → `v4_zone_status.json`, `v5_alerts.json`, `v6_burst_top10.json`.
- **Backend (V2 Flask/FastAPI):** Exposes endpoints including `/health`, `/zones`, `/alerts/active`, and `/burst-risk/top10`.
- **Frontend (V9 Dashboard):** Calls these endpoints to render the interactive map and analytics panels.

---

## Project Structure

```text
solapur_water_project/
├── backend/
│   └── app.py                        # Flask API server
├── backend_fastapi/                  # Experimental FastAPI backend
├── Data/                             # Input GeoJSON and generated V1 CSVs
├── docs/                             
│   └── Hydro_Equity_Engine_Architecture_Bible_v3.pdf
├── frontend/
│   └── index.html                    # V9 Dashboard UI
├── frontend_v2/                      # Next-gen interactive dashboard
├── outputs/                          # Generated simulation/analytics CSVs and JSONs
├── scripts/
│   ├── load_data.py
│   ├── simulation_engine.py          # V3 Engine
│   ├── v1_data_foundation.py         # V1 Engine
│   ├── v4_equity_minimal.py          # V4 Engine
│   ├── v5_clps.py                    # V5 Engine
│   ├── v6_pss.py                     # V6 Engine
│   └── verify_phase3.py              # Phase 3 integration tests
└── DATA_CONTRACT.md                  # Comprehensive schema definitions
```

---

## Hydraulic Security Measures (Anti-Theft & Tampering)

- **NFA (Night Flow Anomaly):** Detects unauthorized water extraction by monitoring flows during off-peak hours (01:00–04:00). Anomalies flagged when night flow exceeds 5% of daily average.
- **FPI (Flow-Pressure Imbalance):** Detects pipe tampering or illegal connections by comparing inlet vs. outlet flow. Significant imbalance indicates unauthorized offtake or valve manipulation.
- **PDR (Pressure Drop Rate):** Detects sudden blockages or ruptures by measuring rate of pressure change over time. Rapid drops indicate active leaks; gradual drops indicate structural degradation.

---

## Current Feature Set

### Implemented
- Real HEI (Hydraulic Equity Index) heatmap and city equity scores via the V4 engine.
- Real leak alerts when the V5 engine has generated data.
- Real burst-risk layer generated from the V6 engine.
- Simulation clock to visualize the 24-hour cycle.

### Illustrative Only
- **Recommendation cards** (from RECS) – currently illustrative, pending V7 recommendation engine.
- **24-hour pressure timeline** – currently uses static curves for illustrative purposes.
- **Scenario buttons** (Leak Event, Valve Close, Demand Surge) – currently act as visual scenario selectors, not a full V8 async simulation.

### Future Work
- V7 recommendation engine, V8 full async scenario panel, V10 governance, V11 auth, V12 mobile app, V13 theft intelligence.

---

## What Was Built

### V1 — Data Foundation
**Script:** `scripts/v1_data_foundation.py`
Parses four GeoJSON files from SMC's public GIS portal and produces structured CSVs for simulation. Produces `pipe_segments.csv`, `nodes_with_elevation.csv`, `zone_demand.csv`, and `infrastructure_points.csv`.

*Note: Elevation is currently simulated (440–470m). Real DEM data and a PostgreSQL/TimescaleDB migration are planned.*

### V3 — Hydraulic Simulation Engine
**Script:** `scripts/simulation_engine.py`
Builds a NetworkX graph and WNTR hydraulic model from the V1 CSVs, runs a 24-hour simulation, and outputs pressure and flow physical matrices to the `outputs/` directory. Handles disconnected clusters by analyzing the Largest Connected Component (LCC).

### V4 — Equity Scoring Engine
**Script:** `scripts/v4_equity_minimal.py`
Computes the exact Hydraulic Equity Index (HEI) for every zone by analyzing the physical network topology and real pressure data from V3. Outputs `outputs/v4_zone_status.json` and `v4_equity_minimal.json`.

### V5 & V6 — Anomaly Analytics Engines
**Scripts:** `scripts/v5_clps.py` | `scripts/v6_pss.py`
- **V5 Leak Detection:** Uses physical simulation data to calculate Composite Leak Probability Score (CLPS) and generates `outputs/v5_alerts.json`.
- **V6 Burst Prediction:** Uses pipe attributes and pressure data to calculate Pipe Stress Score (PSS) and generates `outputs/v6_burst_top10.json`.

---

## How to Run

### 1. Prerequisites
- Python 3.x (64-bit)
- (Windows only) Microsoft C++ Build Tools (required for WNTR)
- Git

```bash
# Clone the repository and navigate to the project directory
git clone https://github.com/Prabhav-020108/HYDRO-EQUITY-ENGINE_SMC.git
cd solapur_water_project

# Create and activate a virtual environment
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Mac/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Generate Analytics (Phase 1 & 2)
Generate simulation and analytics data (when needed) from the repository root:

```bash
cd scripts
python simulation_engine.py       # V3 – baseline + scenarios
python v4_equity_minimal.py       # V4 – HEI / CWEI
python v5_clps.py                 # V5 – CLPS leak alerts
python v6_pss.py                  # V6 – burst risk
```

*Expected output files in `outputs/`: `pressure_baseline.csv`, `flow_baseline.csv`, scenario CSVs, `v4_zone_status.json`, `v5_alerts.json`, and `v6_burst_top10.json`.*

### 3. Start Backend
The Flask backend runs on `http://localhost:5000` and automatically exposes endpoints like `/health`, `/zones`, `/alerts/active`, and `/burst-risk/top10`.

```bash
# From the project root, navigate to the backend
cd backend
python app.py
```

### 4. Start Frontend
The dashboard calls the backend endpoints and renders interactive layers, equity bars, alerts panels, and burst risk indicators.

```bash
# Open a new terminal, activate virtualenv, navigate to frontend
cd frontend
python -m http.server 3000
```
Dashboard URL: [http://localhost:3000](http://localhost:3000)

---

## How to Test Integration

### Automated Verification
A script is provided to verify the Phase-3 integration by comparing backend API responses against the generated JSON analytics outputs.

```bash
cd scripts
python verify_phase3.py
```
*Expected output: `RESULT: ALL CHECKS PASS (OK)`*

### Manual Engine-Off Tests
To verify the frontend gracefully handles missing analytics data:
1. Rename or move `outputs/v4_zone_status.json`, `outputs/v5_alerts.json`, and `outputs/v6_burst_top10.json`.
2. Reload the dashboard.
3. Verify that the UI reports `"Run V[X] first"` instead of showing fake data (e.g., the Burst Risk layer or Equity panel should display the fallback/error message).

---

## Mathematical Models

**Hydraulic Equity Index (HEI):**
```text
HEI(zone, t) = avg_pressure(tail-end nodes, t) / avg_pressure(all zone nodes, t)
```

**Composite Leak Probability Score (CLPS):**
```text
CLPS = (0.35 × PDR_n) + (0.30 × FPI) + (0.20 × NFA) + (0.15 × DDI)
```

**Pipe Stress Score (PSS):**
```text
PSS = (0.40 × PSI_n) + (0.35 × CFF_n) + (0.25 × ADF)
```

---

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
