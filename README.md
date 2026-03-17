# Hydro-Equity Engine
### Smart Water Pressure Management for Equitable Water Supply — Solapur Municipal Corporation

**Team Devsters · SAMVED-2026 · RV College of Engineering, Bengaluru**

---

## What This Project Is

Solapur Municipal Corporation (SMC) supplies water to 10+ lakh citizens through a network of Elevated Storage Reservoirs and ward-level pipelines under intermittent supply conditions. Despite having adequate bulk water, the city faces persistent pressure imbalance — low-elevation zones receive excess pressure while tail-end and high-elevation zones receive little or none.

The **Hydro-Equity Engine** converts the real SMC pipe network into a hydraulically simulated, equity-scored, anomaly-detecting, role-partitioned governance platform for municipal engineers, ward officers, commissioners, and citizens.

The core insight: the problem in Solapur is not water scarcity. It is **pressure governance and distribution intelligence**.

---

## Project Scope and Phases

| Phase | Status | What Was Done |
|---|---|---|
| **Phase 1** — Data foundation & simulation (V1, V3) | ✅ Complete | GeoJSON ETL, WNTR hydraulic model, baseline + 3 anomaly scenarios |
| **Phase 2** — Analytics engines (V4, V5, V6) | ✅ Complete | HEI equity scoring, CLPS leak detection, PSS burst prediction |
| **Phase 3** — Backend & dashboard wiring (V2, V9) | ✅ Complete | FastAPI endpoints, real V4/V5/V6 data, no synthetic fallbacks |
| **Phase 3.5** — PostgreSQL migration + resolution workflow | ✅ Complete | Alert lifecycle, DB schema, elevation enrichment |
| **Phase 4a** — Authentication & role management (V11) | ✅ Complete | JWT login, bcrypt, role-based routing to 4 dashboards |
| **Phase 4b** — V7 Role-Partitioned Engine + all dashboards | ✅ **COMPLETE** | See below |
| **Phase 4c** — Mobile app (V12), Theft detection (V13), Scenario panel (V8) | ⏳ Upcoming | |
| **Phase 5** — Final integration & demo polish | ⏳ Upcoming | |

---

## Phase 4b — What Was Built ✅

### V7 Role-Partitioned Recommendation Engine

The V7 engine reads live analytics from V4 (HEI equity), V5 (CLPS leak alerts), and V6 (PSS burst risk) and generates role-specific recommendations simultaneously into four channels:

| Channel | Table | Audience | Contains |
|---|---|---|---|
| Engineer | `engineer_recs` | Engineer, Field Operator | Valve IDs, pipe IDs, urgency levels, HEI gain estimates |
| Ward | `ward_recs` | Ward Officer (zone-filtered) | Plain-language escalation notes, service reliability notes |
| Commissioner | `commissioner_recs` | Commissioner | City summary, worst zones, budget flags |
| Citizen | `citizen_recs` | Public (no auth) | Supply status, plain advisories — zero infra data |

**Five V7 triggers:**
- **Trigger A (Equity)** → all four channels (HEI < 0.70 severe, < 0.85 moderate, > 1.30 over-pressure)
- **Trigger B (Leak)** → engineer + ward (CLPS-driven, signal-specific dispatch instructions)
- **Trigger C (Burst)** → engineer + commissioner (PSS-driven, pipe IDs, age, material)
- **Trigger D (Citizen)** → citizen only (plain language, distinct per-zone advisory)
- **Trigger E (Theft)** → stub for Phase 4c V13

**Fallback chain:** PostgreSQL → `v7_recommendations.json` → live generation from V4/V5/V6 (always works in dev mode)

### Four Role Dashboards (Phase 4b)

| Dashboard | URL | Features |
|---|---|---|
| **Engineer** | `engineer_dashboard.html` | Full map, HEI heatmap, collapsible alerts + recs, Ack/Resolve buttons, 4 scenarios, 24h timeline |
| **Ward Officer** | `ward_dashboard.html` | Zone-scoped map, zone equity status, zone-filtered alerts + V7 ward recs |
| **Commissioner** | `commissioner_dashboard.html` | CWEI gauge, city zone rankings, collapsible strategic recs, PDF report download |
| **Citizen** | `citizen_panel.html` | Public portal (no auth), 8-zone supply status, distinct advisories, complaint form |

### Alert Lifecycle (Phase 4b)

Acknowledge/Resolve buttons are live in `engineer_dashboard.html`:
- **With PostgreSQL:** calls `/alerts/{id}/acknowledge` and `/alerts/{id}/resolve` — full DB lifecycle
- **Dev mode (file-based):** provides visual feedback on the button itself — no popup errors

### Citizen Portal (Public)

- No authentication required — open in browser directly
- Always shows exactly 8 zones with distinct per-zone advisories
- Important Advisories capped at 3 unique zones
- Complaint form submits to `POST /citizen/complaint` — returns reference ID

### Weekly PDF Report

Commissioner dashboard has a **Download Weekly Report (PDF)** button that calls `GET /reports/weekly` and streams a real PDF (requires `pip install reportlab`). Contains: CWEI summary, zone HEI table, alert summary, burst risk segments.

---

## Current Architecture

```
SMC GeoJSON Files (GIS Portal)
        ↓
V1 Data Foundation → pipe_segments.csv · nodes_with_elevation.csv · zone_demand.csv
        ↓
V3 WNTR Hydraulic Simulation → fullcity_pressure_*.csv · fullcity_flow_*.csv
        ↓               ↓              ↓
V4 HEI           V5 CLPS         V6 PSS
v4_zone_status.json  v5_alerts.json  v6_burst_top10.json
        ↓               ↓              ↓
        └───────── V7 Rule Engine ─────────┘
                         ↓ (4 channels simultaneously)
        engineer_recs  ward_recs  commissioner_recs  citizen_recs
                         ↓
              FastAPI Backend (Port 8000)
              JWT Auth · Role-gated endpoints · APScheduler
                         ↓
        ┌────────────────┬───────────────────┬─────────────────┐
  Engineer          Ward Officer        Commissioner     Citizen Portal
  Dashboard          Dashboard           Dashboard        (Public)
```

---

## Project Structure

```text
solapur_water_project/
├── backend/
│   ├── app.py                        # FastAPI server (V7 auto-schedules every 5 min)
│   ├── auth.py                       # JWT + bcrypt authentication
│   ├── database.py                   # SQLAlchemy engine
│   └── routers/
│       ├── auth_router.py            # POST /auth/login, GET /auth/me
│       ├── zones.py                  # GET /zones → V4 HEI scores
│       ├── alerts.py                 # GET /alerts/active
│       ├── burst.py                  # GET /burst-risk/top10
│       ├── pipeline.py               # GET /pipeline (public GeoJSON)
│       ├── infrastructure.py         # GET /infrastructure (public markers)
│       ├── recommendations.py        # GET /recommendations/* (V7, all roles)
│       ├── citizen.py                # POST /citizen/complaint, GET /citizen/zones
│       └── reports.py                # GET /reports/weekly (PDF)
│
├── Data/                             # Input GeoJSON and generated V1 CSVs
│
├── docs/
│   └── Hydro_Equity_Engine_Architecture_Bible_v5.pdf
│
├── frontend/
│   ├── login.html                    # JWT login page (all roles)
│   ├── engineer_dashboard.html       # Phase 4b engineer workspace
│   ├── ward_dashboard.html           # Phase 4b ward officer view
│   ├── commissioner_dashboard.html   # Phase 4b commissioner view
│   ├── citizen_panel.html            # Phase 4b public citizen portal
│   └── field_operator_dashboard.html # Field operator (Phase 4c mobile upgrade)
│
├── outputs/                          # Generated simulation/analytics outputs
│   ├── v4_zone_status.json           # HEI per zone
│   ├── v4_equity_minimal.json        # CWEI + zone trends
│   ├── v5_alerts.json                # CLPS alerts by scenario
│   ├── v6_burst_top10.json           # Top 10 burst risk segments
│   ├── v7_recommendations.json       # V7 cached recommendations (all 4 channels)
│   └── fullcity_*.csv                # Full-city pressure/flow simulation results
│
├── scripts/
│   ├── load_data.py                  # Step 0: load GeoJSON data
│   ├── simulation_engine.py          # V3: WNTR hydraulic simulation
│   ├── v1_data_foundation.py         # V1: GeoJSON → CSV ETL
│   ├── v4_equity_minimal.py          # V4: HEI equity scoring
│   ├── v5_clps.py                    # V5: CLPS leak detection
│   ├── v6_pss.py                     # V6: PSS burst prediction
│   ├── v7_recommendations.py         # V7: role-partitioned rec engine (DB mode)
│   ├── db_setup_phase4b.py           # Creates V7 DB tables
│   ├── seed_users.py                 # Seeds demo users into PostgreSQL
│   └── rebuild.py                    # Manually trigger V7 rebuild
│
├── DATA_CONTRACT.md                  # Schema definitions for all data files
├── requirements.txt
└── README.md                         # This file
```

---

## Hydraulic Security Measures (Anti-Theft & Tampering)

- **NFA (Night Flow Anomaly):** Detects unauthorized water extraction by monitoring flows during off-peak hours (01:00–04:00). Anomalies flagged when night flow exceeds 5% of daily average.
- **FPI (Flow-Pressure Imbalance):** Detects pipe tampering or illegal connections by comparing inlet vs. outlet flow. Significant imbalance indicates unauthorized offtake or valve manipulation.
- **PDR (Pressure Drop Rate):** Detects sudden blockages or ruptures by measuring rate of pressure change over time. Rapid drops indicate active leaks; gradual drops indicate structural degradation.

V13 Water Theft Intelligence (WTRS per zone, tanker extraction pattern) is planned for Phase 4c.

---

## Quickstart (Phase 4b Dev Mode — Postgres-Free)

Follow these steps to run the full platform locally without PostgreSQL.

### 1. Prerequisites
- Python 3.10+ (64-bit)
- (Windows only) Microsoft C++ Build Tools (required for WNTR)
- Git

```bash
# Clone the repository
git clone https://github.com/Prabhav-020108/HYDRO-EQUITY-ENGINE_SMC.git
cd solapur_water_project

# Create and activate virtual environment
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# For PDF report generation (optional but recommended):
pip install reportlab
```

### 2. Generate Data & Analytics Pipeline
Run scripts in order — outputs go to `solapur_water_project/outputs/`:

```bash
cd scripts
python load_data.py
python simulation_engine.py all
python v4_equity_minimal.py
python v5_clps.py
python v6_pss.py
# V7 generates automatically on backend startup — or run manually:
# python v7_recommendations.py
```

### 3. Start Backend (Dev Auth Mode — No PostgreSQL)

```powershell
# Windows PowerShell
cd solapur_water_project
$env:AUTH_DEV_MODE = '1'
python -m uvicorn backend.app:app --port 8000 --reload
```

```bash
# Mac/Linux
cd solapur_water_project
AUTH_DEV_MODE=1 python -m uvicorn backend.app:app --port 8000 --reload
```

V7 recommendation engine **auto-runs on startup** and every 5 minutes via APScheduler.

Verify: `http://localhost:8000/health` → `{"status":"ok","phase":"4b"}`

### 4. Start Frontend

```bash
cd solapur_water_project/frontend
python -m http.server 3000
```

---

## Authentication & Demo Users

When running in **Dev Auth Mode** (`AUTH_DEV_MODE=1`), all passwords are `demo@1234`:

| Role | Username | Password | Dashboard URL |
|---|---|---|---|
| **Engineer** | `engineer1` | `demo@1234` | `engineer_dashboard.html` |
| **Ward Officer** | `ward_z1` | `demo@1234` | `ward_dashboard.html` (Zone 1 scoped) |
| **Commissioner** | `commissioner1` | `demo@1234` | `commissioner_dashboard.html` |
| **Field Operator** | `field_op1` | `demo@1234` | `field_operator_dashboard.html` |
| **Citizen** | *(no login)* | — | `citizen_panel.html` (open directly) |

### Frontend URLs (Frontend server on port 3000)

| Page | URL |
|---|---|
| Login | `http://localhost:3000/login.html` |
| Engineer Dashboard | `http://localhost:3000/engineer_dashboard.html` |
| Ward Dashboard | `http://localhost:3000/ward_dashboard.html` |
| Commissioner Dashboard | `http://localhost:3000/commissioner_dashboard.html` |
| Citizen Portal (Public) | `http://localhost:3000/citizen_panel.html` |

---

## API Endpoints

### Public (no auth)
| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Health check |
| GET | `/health` | Health check JSON |
| GET | `/pipeline` | Pipeline GeoJSON (Solapur network) |
| GET | `/infrastructure` | ESR / tank / source markers |
| GET | `/recommendations/citizen` | V7 citizen supply advisories |
| POST | `/citizen/complaint` | Submit water supply complaint |
| GET | `/citizen/zones` | Zone supply status summary |

### Protected (Bearer JWT required)
| Method | Endpoint | Role | Description |
|---|---|---|---|
| POST | `/auth/login` | All | Returns JWT + role |
| GET | `/auth/me` | All | Current user info |
| GET | `/zones` | All | V4 HEI scores (ward: zone-filtered) |
| GET | `/alerts/active?scenario=` | All | V5 CLPS alerts |
| POST | `/alerts/{id}/acknowledge` | Engineer | Alert lifecycle |
| POST | `/alerts/{id}/resolve` | Engineer | Alert lifecycle |
| GET | `/burst-risk/top10` | All | V6 PSS burst risk |
| GET | `/recommendations/engineer` | Engineer, Field Operator | V7 engineer channel |
| GET | `/recommendations/ward` | Ward Officer | V7 ward channel (zone-filtered) |
| GET | `/recommendations/commissioner` | Commissioner | V7 commissioner channel |
| GET | `/recommendations/updated-at` | All | Last V7 run timestamp |
| POST | `/recommendations/rebuild` | Engineer, Commissioner | Trigger V7 re-run |
| GET | `/reports/weekly` | Engineer, Commissioner | Download weekly PDF report |

---

## Mathematical Models

**Hydraulic Equity Index (HEI):**
```
HEI(zone, t) = avg_pressure(tail-end nodes, t) / avg_pressure(all zone nodes, t)

HEI ≥ 0.85        → Equitable     (Green)
0.70 ≤ HEI < 0.85 → Moderate      (Orange)
HEI < 0.70         → Severe        (Red)
HEI > 1.30         → Over-pressure (Purple)
```

**City-Wide Equity Index (CWEI):**
```
CWEI = mean(HEI across all zones)
```

**Composite Leak Probability Score (CLPS):**
```
CLPS = (0.35 × PDR_n) + (0.30 × FPI) + (0.20 × NFA) + (0.15 × DDI)

PDR_n  = Pressure Drop Rate (normalized)
FPI    = Flow-Pressure Imbalance
NFA    = Night Flow Anomaly (01:00–04:00 only)
DDI    = Demand Deviation Index
```

**Pipe Stress Score (PSS):**
```
PSS = (0.40 × PSI_n) + (0.35 × CFF_n) + (0.25 × ADF)

PSI_n = Pressure Surge Index
CFF_n = Cycle Fatigue Factor
ADF   = Age Degradation Factor
```

---

## What's Next — Phase 4c

- **V12 Mobile App (React Native/Expo):** FieldOperatorApp (QR valve verification, GPS proximity, alert reception) + CitizenApp (complaint submission, supply status)
- **V13 Water Theft Intelligence:** WTRS per zone, tanker extraction pattern (TEP), routes HIGH alerts to engineer + commissioner via V7 Trigger E
- **V8 Scenario Panel:** WNTR parameter sliders embedded in EngineerDashboard — before/after HEI comparison

---

## Team

| Name | Program | Email |
|---|---|---|
| Prabhav Tiwari (Lead) | B.E. CSE (AIML) | prabhavatiwari.ci25@rvce.edu.in |
| Yashraj Pala | B.E. CSE | yashrajpala.cs25@rvce.edu.in |
| Shashwat Utkarsh | B.E. Mechanical | shashwatutkarsh.me25@rvce.edu.in |
| Tejash Pathak | B.E. CSE | tejashmanojp.cs25@rvce.edu.in |
| Shaurya Khanna | B.E. CSE (CY) | shauryakhanna.cy25@rvce.edu.in |

**Mentor:** Dr. Sham Aan MP · shamaan.mp@rvce.edu.in · RVCE Bengaluru

---

*Hydro-Equity Engine · Team Devsters · SAMVED-2026 · Phase 4b Complete*