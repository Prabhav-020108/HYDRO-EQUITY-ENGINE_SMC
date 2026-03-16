"""
Hydro-Equity Engine — Phase 4a
backend/app.py

Run from project root:
    uvicorn backend.app:app --reload --port 8000

PUBLIC endpoints (no auth):
    GET  /                          → health check
    GET  /health                    → health check JSON
    GET  /pipeline                  → pipeline GeoJSON
    GET  /infrastructure            → ESR / tank / source markers

PROTECTED endpoints (require Bearer token from POST /auth/login):
    POST /auth/login                → returns JWT
    GET  /auth/me                   → current user info
    GET  /zones                     → V4 HEI scores     (ward_officer: zone-filtered)
    GET  /alerts/active?scenario=   → V5 CLPS alerts    (ward_officer: zone-filtered)
    POST /alerts/{id}/acknowledge   → alert lifecycle
    POST /alerts/{id}/resolve       → alert lifecycle
    GET  /burst-risk/top10          → V6 PSS top-10 burst risk
"""

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from datetime import datetime

from backend.database import engine
from backend.auth import get_current_user
from backend.routers import auth_router, zones, alerts, burst, pipeline, infrastructure, recommendations

app = FastAPI(
    title="Hydro-Equity Engine API",
    version="4.0.0",
    description=(
        "Smart Water Pressure Management — Solapur Municipal Corporation\n"
        "SAMVED-2026 | Team Devsters | Phase 4a\n\n"
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

# ── Routers ────────────────────────────────────────────────────────
app.include_router(auth_router.router)     # /auth/login, /auth/me
app.include_router(zones.router)           # /zones
app.include_router(alerts.router)          # /alerts/active
app.include_router(burst.router)           # /burst-risk/top10
app.include_router(pipeline.router)        # /pipeline        (public)
app.include_router(infrastructure.router)  # /infrastructure  (public)
app.include_router(recommendations.router) # /recommendations (V7)


# ── Alert lifecycle endpoints ──────────────────────────────────────
# Used by Acknowledge / Resolve buttons in the dashboard
@app.post("/alerts/{alert_id}/acknowledge", tags=["Alert Lifecycle"])
def acknowledge_alert(
    alert_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Mark an alert as acknowledged. Updates acknowledged_at timestamp."""
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
def resolve_alert(
    alert_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Mark an alert as resolved. Requires it to be acknowledged first."""
    try:
        with engine.connect() as conn:
            # Check current status
            row = conn.execute(
                text("SELECT status FROM alerts WHERE alert_id=:id"),
                {'id': alert_id}
            ).fetchone()
            if not row:
                return {'success': False, 'error': 'Alert not found'}
            if row[0] == 'fired':
                return {'success': False, 'error': 'Alert must be acknowledged before resolving'}
            conn.execute(
                text("""
                    UPDATE alerts
                    SET status='resolved', resolved_at=:ts
                    WHERE alert_id=:id
                """),
                {'ts': datetime.utcnow(), 'id': alert_id}
            )
            conn.commit()
        return {'success': True, 'alert_id': alert_id, 'status': 'resolved'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Root / Health ──────────────────────────────────────────────────
@app.get("/", tags=["Public"])
def root():
    return {
        "status":    "Hydro-Equity Engine API v4.0 — Phase 4a",
        "team":      "Devsters",
        "event":     "SAMVED-2026",
        "docs":      "/docs",
        "login":     "POST /auth/login",
        "public":    ["/pipeline", "/infrastructure"],
        "protected": ["/zones", "/alerts/active", "/burst-risk/top10"]
    }

@app.get("/health", tags=["Public"])
def health():
    return {"status": "ok", "phase": "4a", "auth": "JWT active"}