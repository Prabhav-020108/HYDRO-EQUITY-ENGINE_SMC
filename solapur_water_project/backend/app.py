"""
Hydro-Equity Engine / Dhara — Phase 4b + M1
backend/app.py

M1 CHANGE:
  - Removed POST /alerts/{alert_id}/acknowledge (now in backend/routers/alerts.py)
  - Removed POST /alerts/{alert_id}/resolve     (now in backend/routers/alerts.py)
  These were duplicated in app.py before M1 and would cause route conflicts
  now that the alerts router owns all /alerts/* lifecycle endpoints.

Run from project root:
    AUTH_DEV_MODE=1 uvicorn backend.app:app --reload --port 8000

PUBLIC endpoints (no auth):
    GET  /                               → health check
    GET  /health                         → health check JSON
    GET  /pipeline                       → pipeline GeoJSON
    GET  /infrastructure                 → ESR / tank / source markers
    GET  /recommendations/citizen        → V7 citizen supply advisories
    POST /citizen/complaint              → submit citizen complaint
    GET  /citizen/zones                  → city zone supply status

PROTECTED endpoints (require Bearer token from POST /auth/login):
    POST /auth/login                     → returns JWT
    GET  /auth/me                        → current user info
    GET  /zones                          → V4 HEI scores (ward_officer: zone-filtered)
    GET  /alerts/active?scenario=        → V5 CLPS alerts (+ ?status= filter, M1)
    POST /alerts/{id}/acknowledge        → M1 state machine (engineer only)
    POST /alerts/{id}/request-resolution → M1 state machine (field_operator only)
    POST /alerts/{id}/accept-resolution  → M1 state machine (engineer only)
    POST /alerts/{id}/reject-resolution  → M1 state machine (engineer only)
    POST /alerts/{id}/resolve            → backward-compat alias (engineer only)
    GET  /burst-risk/top10               → V6 PSS top-10 burst risk
    GET  /recommendations/engineer       → V7 engineer channel
    GET  /recommendations/ward           → V7 ward channel (zone-filtered)
    GET  /recommendations/commissioner   → V7 commissioner channel
    GET  /recommendations/updated-at     → last V7 run timestamp
    GET  /reports/weekly                 → download PDF report
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

from backend.auth import get_current_user
from backend.routers import (
    auth_router, zones, alerts, burst,
    pipeline, infrastructure,
    recommendations, reports,
)

# ── App instance ───────────────────────────────────────────────────
app = FastAPI(
    title="Hydro-Equity Engine / Dhara API",
    version="4b.1-M1",
    description=(
        "Smart Water Pressure Management — Solapur Municipal Corporation\n"
        "SAMVED-2026 | Team Devsters | Phase 4b + M1\n\n"
        "M1: Alert state machine (new→acknowledged→resolve_requested→resolved).\n"
        "Protected endpoints require Bearer token from POST /auth/login.\n"
        "Demo: engineer1 / demo@1234  |  AUTH_DEV_MODE=1 for no-Postgres dev mode."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ───────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers (Phase 4a) ────────────────────────────────────────────
app.include_router(auth_router.router)      # /auth/login, /auth/me
app.include_router(zones.router)            # /zones
app.include_router(alerts.router)           # /alerts/active + M1 lifecycle endpoints
app.include_router(burst.router)            # /burst-risk/top10
app.include_router(pipeline.router)         # /pipeline        (public)
app.include_router(infrastructure.router)   # /infrastructure  (public)

# ── Routers (Phase 4b NEW) ────────────────────────────────────────
app.include_router(recommendations.router)  # /recommendations/*
app.include_router(reports.router)          # /reports/weekly (PDF)


# ── NOTE: Alert lifecycle POST endpoints are now in alerts.router ──
# (removed from app.py in M1 to avoid route conflicts)
# See: backend/routers/alerts.py for:
#   POST /alerts/{id}/acknowledge
#   POST /alerts/{id}/request-resolution
#   POST /alerts/{id}/accept-resolution
#   POST /alerts/{id}/reject-resolution
#   POST /alerts/{id}/resolve   ← backward-compat alias


# ── V7 APScheduler ─────────────────────────────────────────────────
# Runs V7 recommendation engine on startup + every 5 minutes.
@app.on_event("startup")
async def startup_event():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from scripts.v7_recommendations import run_v7

        # Run immediately on startup
        try:
            print("[V7] Running initial recommendations pass on startup...")
            run_v7()
        except Exception as e:
            print(f"[V7] Initial run failed (non-fatal): {e}")

        # Schedule every 5 minutes
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            run_v7,
            trigger='interval',
            minutes=5,
            id='v7_engine',
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        print("[V7] ✅ APScheduler running — V7 refreshes every 5 minutes")

    except ImportError:
        print("[V7] ⚠ APScheduler not installed. Run: pip install apscheduler")
    except Exception as e:
        print(f"[V7] Scheduler setup failed (non-fatal): {e}")


# ── Root / Health ──────────────────────────────────────────────────
@app.get("/", tags=["Public"])
def root():
    return {
        "status":   "Hydro-Equity Engine / Dhara API — Phase 4b + M1",
        "team":     "Devsters",
        "event":    "SAMVED-2026",
        "docs":     "/docs",
        "v7":       "Running every 5 minutes via APScheduler",
        "m1_state_machine": [
            "POST /alerts/{id}/acknowledge        (engineer)",
            "POST /alerts/{id}/request-resolution (field_operator)",
            "POST /alerts/{id}/accept-resolution  (engineer)",
            "POST /alerts/{id}/reject-resolution  (engineer)",
            "GET  /alerts/active?status=<state>   (all authed roles)",
        ],
    }


@app.get("/health", tags=["Public"])
def health():
    return {
        "status": "ok",
        "phase":  "4b+M1",
        "auth":   "JWT active",
        "v7":     "APScheduler active",
        "m1":     "Alert state machine active",
    }