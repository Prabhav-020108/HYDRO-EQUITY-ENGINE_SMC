# Dhara / Hydro-Equity Engine
### Smart Water Pressure Management for Equitable Water Supply — Solapur Municipal Corporation

---

## 1. Project Overview
The **Dhara (Hydro-Equity Engine)** converts the real Solapur Municipal Corporation (SMC) pipe network into a hydraulically simulated, equity-scored, anomaly-detecting, role-partitioned governance platform. 

**The Problem:** Solapur supplies water to 10+ lakh citizens through an intermittent supply network. Despite having adequate bulk water, the city faces severe pressure imbalance—low-elevation zones receive excess pressure while tail-end zones receive little or none. 

**The Solution:** Rather than adding more water, Dhara focuses on **pressure governance and distribution intelligence**. It uses WNTR hydraulic simulations and custom analytics to detect leaks, predict burst risks, and measure supply equity, routing actionable insights to the right personnel.

---

## 2. System Architecture
**Data Flow:**
SMC GeoJSON Files → `V1` → `V3` → `V4` / `V5` / `V6` → `V7` → `API` → `Frontend`

**Role-Based Access (5 Roles):**
1. **engineer**: Full system view, alert lifecycle management, hydraulic metrics.
2. **ward_officer**: Zone-scoped view, service reliability notes.
3. **commissioner**: High-level city overview, CWEI equity gauge, PDF reports.
4. **field_operator**: Mobile-friendly view for on-ground tasks and resolving alerts.
5. **citizen**: Public portal for supply status and submitting complaints.

**Backend Stack:** 
FastAPI, PostgreSQL, APScheduler (for V7 rules), JWT + Bcrypt authentication.

**Frontend 5 HTML Pages:**
- `engineer_dashboard.html`: Full map, HEI heatmap, collapsible alerts, Ack/Resolve workflow.
- `ward_dashboard.html`: Zone-scoped map, zone-filtered alerts and recommendations.
- `commissioner_dashboard.html`: CWEI gauge, city zone rankings, PDF report download.
- `field_operator_app.html`: Mobile-first SPA for field workers to start and resolve tasks.
- `index.html`: Public Citizen Portal for 8-zone supply status and complaint submission.
*(Note: `login.html` provides the JWT authentication gateway for non-citizen roles).*

---

## 3. Prerequisites
Ensure the following exact versions and tools are installed:
- **Python:** 3.11 (64-bit)
- **PostgreSQL:** 18
- **Git Bash:** Required for Windows environments (to run integration tests).
- **Required pip packages:** (from `requirements.txt`)
  `fastapi`, `uvicorn`, `sqlalchemy`, `psycopg2-binary`, `passlib`, `bcrypt`, `pyjwt`, `pandas`, `wntr`, `reportlab`

---

## 4. Setup — One Time Only

**1. Database Initialization Pipeline:**
From the `solapur_water_project` directory, run the setup scripts in order:
```bash
python scripts/db_setup.py
python scripts/db_setup_phase4b.py
python scripts/db_setup_alerts.py
python scripts/seed_users.py
```

**2. Environment Variables:**
Create a `.env` file in the `solapur_water_project` directory with the following format:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hydro_equity
DB_USER=postgres
DB_PASSWORD=admin1234
SECRET_KEY=09d25e094faa6RS24f6f0f4caa6cf63b88e8d
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_HOURS=24
```

---

## 5. Data Generation — One Time or When Source Data Changes
Run the analytics engine pipeline to generate simulation data and insights. Outputs are saved to the `outputs/` folder (outputs already exist in the repo, but re-run if source data changes).

**Note:** `simulation_engine.py` requires `wntr`.

```bash
cd scripts
python v1_data_foundation.py
python load_data.py
python simulation_engine.py all
python v4_equity_minimal.py
python v5_clps.py
python v6_pss.py
python v7_recommendations.py
cd ..
```

---

## 6. Every Session Startup

**1. Start Backend:**
```bash
cd solapur_water_project
uvicorn backend.app:app --reload --port 8000
```

**2. Run Database Migrations:**
**Note:** Do NOT run `db_migrate.py` before the integration test.
```bash
python scripts/db_migrate.py
```

**3. Start Frontend:**
```bash
cd solapur_water_project/frontend
python -m http.server 3000
```

---

## 7. Demo Credentials
All registered roles use the password: `demo123`

| Role | Username | Password |
|---|---|---|
| **Engineer** | `engineer1` | `demo123` |
| **Ward Officer** | `ward_z1` | `demo123` |
| **Commissioner** | `commissioner1` | `demo123` |
| **Field Operator** | `field_op_z1` | `demo123` |
| **Citizen** | *(no login)* | — |

---

## 8. Frontend Pages
Access via `http://localhost:3000/` once the frontend server is running.
- **`login.html`**: Gateway for auth.
- **`engineer_dashboard.html`** (Engineer): Verify the HEI heatmap, alert markers, and the Acknowledge/Resolve alert workflow visually.
- **`ward_dashboard.html`** (Ward Officer): Verify zone-scoped metrics and filtered alerts for a specific zone (e.g., Zone 1).
- **`commissioner_dashboard.html`** (Commissioner): Verify the CWEI gauge, zone rankings, and ensure the "Download Alert Log (CSV)" and "Download Weekly Report (PDF)" buttons generate files.
- **`field_operator_app.html`** (Field Operator): Verify the mobile layout, HEI badge, and alert cards with "Start Work" and "Mark Resolved" buttons visually.
- **`index.html`** (Citizen): Verify the 8-zone public supply status, distinct zone advisories, and the complaint submission form visually.

---

## 9. Integration Test
Tests the full alert lifecycle across roles (Engineer & Field Operator). 
**Note:** The script has a requirement to reset Alert 65's ID status in the database prior to executing the test sequence.

Run from Git Bash:
```bash
bash scripts/integration_test.sh
```
**Expected Output:** `10/10 PASS` (Total 10 passed, 0 failed).

---

## 10. API Endpoints

### Public (No Auth Required)
| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check JSON |
| GET | `/pipeline` | Pipeline GeoJSON (Solapur network) |
| GET | `/infrastructure` | ESR / tank / source markers |
| GET | `/recommendations/citizen` | V7 citizen supply advisories |
| POST| `/citizen/complaint` | Submit water supply complaint |
| GET | `/citizen/zones` | Zone supply status summary |

### Protected (Bearer JWT Required)
| Method | Endpoint | Role | Description |
|---|---|---|---|
| POST| `/auth/login` | All | Returns JWT + role |
| GET | `/auth/me` | All | Current user info |
| GET | `/zones` | All | V4 HEI scores |
| GET | `/alerts/active` | All | V5 CLPS active alerts |
| POST| `/alerts/{id}/acknowledge` | Engineer | Alert lifecycle |
| POST| `/alerts/{id}/resolve` | Engineer | Alert lifecycle directly |
| POST| `/alerts/{id}/accept-resolution` | Engineer | Accept a field operator resolve request |
| GET | `/burst-risk/top10` | All | V6 PSS burst risk |
| GET | `/recommendations/engineer` | Engineer, Field Operator | V7 engineer channel |
| GET | `/recommendations/ward` | Ward Officer | V7 ward channel (zone-filtered) |
| GET | `/recommendations/commissioner` | Commissioner | V7 commissioner channel |
| GET | `/reports/weekly` | Engineer, Commissioner | Download PDF report |

### Mobile Specific (Bearer JWT Required)
| Method | Endpoint | Role | Description |
|---|---|---|---|
| GET | `/mobile/alerts/assigned` | Field Operator | Fetch tasks assigned to the operator's zone |
| POST| `/mobile/alerts/{id}/start` | Field Operator | Mark a task as IN_PROGRESS |
| POST| `/mobile/alerts/{id}/resolve`| Field Operator | Submit resolution report for Engineer approval |
