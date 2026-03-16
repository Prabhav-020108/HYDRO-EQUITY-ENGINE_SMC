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
- **Phase 4a/b – Recommendations & Dev Auth (V7, V11)** – implemented.
- **Upcoming Phases** – planned/upcoming as per Architecture Bible v5 (Scenario Panel V8, Governance V10, Production Auth, etc.).

---

## Current Architecture

- **Data:** GeoJSON → V1 → CSVs.
- **Simulation:** V3 WNTR → `outputs/pressure_baseline.csv`, `flow_baseline.csv` (Postgres-free for 4b).
- **Analytics:** V4, V5, V6 → `Outputs` layer file formats (`v4_zone_status.json`, etc.).
- **Recommendations (V7):** Trigger Engine evaluates limits dynamically into `v7_recommendations.json`.
- **FastAPI Backend:** Orchestrates JWT-scoped role endpoints strictly on Mode: Postgres-free Bypass (Port 8000).
- **Frontend:** Modular dashboards strictly partitioned per-user identity payload rules.

---

## Project Structure

```text
solapur_water_project/
├── backend/
│   └── app.py                        # FastAPI API server
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
- **Analytics (V4-V6)**: Real HEI heatmaps, edge triggers on dynamic physical Simulation data routines.
- **V7 Recommendations**: Role-Partitioned advisories trigger mapped outcomes fully written off structure.
- **Dev Auth Guard (V11)**: Environment bypassed login utilizing isolated stationary memory dict structures.
- Simulation clock visualizer to render standard operations.

### Future Work / Illustrative
- **24-hour pressure timeline** – utilizes reference setups.
- **Scenario Buttons** (Leak, Valve Close, Surge) – actions act as visual scenario templates pending V8 full async simulation.
- **PostgreSQL Governance Implementation** – scheduled for future governance phases.

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

## Quickstart (Phase 4b Dev Mode)

Follow these steps to spin up the analytics, backend, and frontend in **Postgres-free Dev Mode**.

### 1. Prerequisites
- Python 3.x (64-bit)
- (Windows only) Microsoft C++ Build Tools (required for WNTR)
- Git

```bash
# Clone the repository
git clone https://github.com/Prabhav-020108/HYDRO-EQUITY-ENGINE_SMC.git
cd solapur_water_project

# Create and activate a virtual environment
python -m venv .venv
# On Windows PowerShell:
.venv\Scripts\activate
# On Mac/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Generate Data & Analytics
Run the analytics pipeline in the following order. Outputs will be written to `solapur_water_project/outputs/` (no PostgreSQL required).

```bash
cd scripts
python load_data.py
python simulation_engine.py all
python v4_equity_minimal.py
python v5_clps.py
python v6_pss.py
```

### 3. Start Backend in Dev-Auth Mode
Set the `AUTH_DEV_MODE=1` environment variable to bypass PostgreSQL for local testing. This enables full dashboard operations using purely isolation-mode file-based structures.

```powershell
# Windows PowerShell
cd solapur_water_project
$env:AUTH_DEV_MODE = '1'
python -m uvicorn backend.app:app --port 8000 --reload
```

*Verify setup*: `http://localhost:8000/health` or `/recommendations/citizen` should return `200`.

### 4. Start Frontend
With the backend running on `8000`, boot up the local file router on port `3000`.

```bash
cd solapur_water_project/frontend
python -m http.server 3000
```

---

## Authentication & Demo Users

When running in **Dev Auth Mode** (`AUTH_DEV_MODE=1`), use the following hardcoded profiles to access isolation dashboard layers securely:

| Role | Username | Password | Notes |
| :--- | :--- | :--- | :--- |
| **Engineer** | `engineer1` | `demo123` | Full strategic advisory |
| **Field Operator** | `field_op1` | `demo123` | Shares engineer stream |
| **Ward Officer** | `ward_z1` | `demo123` | Zone 1-scoped restricted views |
| **Commissioner** | `commissioner1` | `demo123` | High-level city index visibility |

### Frontend URLs
- **Login**: [http://localhost:3000/login.html](http://localhost:3000/login.html)
- **Engineer**: [http://localhost:3000/engineer_dashboard.html](http://localhost:3000/engineer_dashboard.html)
- **Ward**: [http://localhost:3000/ward_dashboard.html](http://localhost:3000/ward_dashboard.html)
- **Commissioner**: [http://localhost:3000/commissioner_dashboard.html](http://localhost:3000/commissioner_dashboard.html)
- **Citizen (Public)**: [http://localhost:3000/index.html](http://localhost:3000/index.html)

*Note: Postgres is omitted for the current Phase 4b dev mode run cycle, but introduced on later scenarios addressing governance compliance scripts.*

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
