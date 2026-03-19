"""
Hydro-Equity Engine — Phase 4b
backend/app.py

Run from project root:
    uvicorn backend.app:app --reload --port 8000

V7 recommendation engine auto-runs on startup, then every 5 minutes.

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
    GET  /alerts/active?scenario=        → V5 CLPS alerts
    POST /alerts/{id}/acknowledge        → alert lifecycle
    POST /alerts/{id}/resolve            → alert lifecycle
    GET  /burst-risk/top10               → V6 PSS top-10 burst risk
    GET  /recommendations/engineer       → V7 engineer channel
    GET  /recommendations/ward           → V7 ward channel (zone-filtered)
    GET  /recommendations/commissioner   → V7 commissioner channel
    GET  /recommendations/updated-at     → last V7 run timestamp
    GET  /reports/weekly                 → download PDF report
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from datetime import datetime

from backend.database import engine
from backend.auth import get_current_user
from backend.routers import (
    auth_router, zones, alerts, burst,
    pipeline, infrastructure,
    recommendations,reports,
)

# ── App instance ───────────────────────────────────────────────────
app = FastAPI(
    title="Hydro-Equity Engine API",
    version="4b.0",
    description=(
        "Smart Water Pressure Management — Solapur Municipal Corporation\n"
        "SAMVED-2026 | Team Devsters | Phase 4b\n\n"
        "Phase 4b: V7 Role-Partitioned Recommendation Engine active.\n"
        "V7 auto-runs every 5 minutes via APScheduler.\n"
        "Protected endpoints require Bearer token from POST /auth/login.\n"
        "Demo: engineer1 / demo@1234"
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
app.include_router(alerts.router)           # /alerts/active
app.include_router(burst.router)            # /burst-risk/top10
app.include_router(pipeline.router)         # /pipeline        (public)
app.include_router(infrastructure.router)   # /infrastructure  (public)

# ── Routers (Phase 4b NEW) ────────────────────────────────────────
app.include_router(recommendations.router)  # /recommendations/*
app.include_router(reports.router)          # /reports/weekly (PDF)


# ── Alert lifecycle endpoints (Phase 4a) ──────────────────────────
@app.post("/alerts/{alert_id}/acknowledge", tags=["Alert Lifecycle"])
def acknowledge_alert(alert_id: int, current_user: dict = Depends(get_current_user)):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    UPDATE alerts
                    SET status='acknowledged', acknowledged_at=:ts
                    WHERE alert_id=:id AND status='fired'
                    RETURNING alert_id
                """),
                {'ts': datetime.utcnow(), 'id': alert_id}
            )
            row = result.fetchone()
            conn.commit()
        if row:
            return {'success': True, 'alert_id': alert_id, 'status': 'acknowledged'}
        return {'success': False, 'error': 'Alert not found or already acknowledged'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/alerts/{alert_id}/resolve", tags=["Alert Lifecycle"])
def resolve_alert(alert_id: int, current_user: dict = Depends(get_current_user)):
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT status FROM alerts WHERE alert_id=:id"),
                {'id': alert_id}
            ).fetchone()
            if not row:
                return {'success': False, 'error': 'Alert not found'}
            if row[0] == 'fired':
                return {'success': False, 'error': 'Alert must be acknowledged before resolving'}
            conn.execute(
                text("UPDATE alerts SET status='resolved', resolved_at=:ts WHERE alert_id=:id"),
                {'ts': datetime.utcnow(), 'id': alert_id}
            )
            conn.commit()
        return {'success': True, 'alert_id': alert_id, 'status': 'resolved'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── V7 APScheduler ─────────────────────────────────────────────────
# Runs V7 recommendation engine on startup + every 5 minutes.
# V7 is non-blocking — uses BackgroundScheduler (separate thread).
@app.on_event("startup")
async def startup_event():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from scripts.v7_recommendations import run_v7

        # Run immediately on startup (populate recs right away)
        try:
            print("[V7] Running initial recommendations pass on startup...")
            run_v7()
        except Exception as e:
            print(f"[V7] Initial run failed (non-fatal): {e}")
            print("[V7] Hint: run 'python scripts/db_setup_phase4b.py' first")

        # Schedule every 5 minutes
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            run_v7,
            trigger='interval',
            minutes=5,
            id='v7_engine',
            max_instances=1,   # prevents overlap if V7 takes > 5 min
            coalesce=True,     # skip missed runs
        )
        scheduler.start()
        print("[V7] ✅ APScheduler running — V7 refreshes every 5 minutes")

    except ImportError:
        print("[V7] ⚠ APScheduler not installed. Run: pip install apscheduler")
        print("[V7] V7 will not auto-schedule. Run manually: python scripts/v7_recommendations.py")
    except Exception as e:
        print(f"[V7] Scheduler setup failed: {e}")
        print("[V7] Server still starting — V7 scheduling failed but all other endpoints work.")


# ── Root / Health ──────────────────────────────────────────────────
@app.get("/", tags=["Public"])
def root():
    return {
        "status":    "Hydro-Equity Engine API v4b — Phase 4b",
        "team":      "Devsters",
        "event":     "SAMVED-2026",
        "docs":      "/docs",
        "v7":        "Running every 5 minutes via APScheduler",
        "new_phase4b": [
            "/recommendations/engineer",
            "/recommendations/ward",
            "/recommendations/commissioner",
            "/recommendations/citizen  (public)",
            "/recommendations/updated-at",
            "/citizen/complaint  (public)",
            "/citizen/zones  (public)",
            "/reports/weekly  (PDF download)",
        ]
    }

@app.get("/health", tags=["Public"])
def health():
    return {"status": "ok", "phase": "4b", "auth": "JWT active", "v7": "APScheduler active"}