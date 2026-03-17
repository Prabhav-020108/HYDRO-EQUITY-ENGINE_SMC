"""
Hydro-Equity Engine — Phase 4b
scripts/db_setup_phase4b.py
Creates 6 new tables required by V7 recommendation engine.

Run ONCE from project root:
    python scripts/db_setup_phase4b.py

Safe to re-run (uses CREATE TABLE IF NOT EXISTS).
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import text
from backend.database import engine


TABLES = [

    # ── V7 Channel 1: Engineer recommendations ─────────────────────
    ("engineer_recs", """
        CREATE TABLE IF NOT EXISTS engineer_recs (
            rec_id              SERIAL PRIMARY KEY,
            zone_id             TEXT,
            trigger_type        TEXT,       -- A_equity | B_leak | C_burst
            action_text         TEXT,
            valve_id            TEXT,
            pipe_id             TEXT,
            pressure_delta      FLOAT,
            urgency             TEXT,       -- URGENT | HIGH | MODERATE | LOW
            estimated_hei_gain  FLOAT,
            node_coords         TEXT,       -- JSON {"lat":..,"lon":..}
            created_at          TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── V7 Channel 2: Ward officer recommendations ──────────────────
    ("ward_recs", """
        CREATE TABLE IF NOT EXISTS ward_recs (
            rec_id                      SERIAL PRIMARY KEY,
            zone_id                     TEXT,
            trigger_type                TEXT,
            action_text                 TEXT,
            escalation_flag             BOOLEAN DEFAULT FALSE,
            service_reliability_note    TEXT,
            complaint_count             INTEGER DEFAULT 0,
            created_at                  TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── V7 Channel 3: Commissioner recommendations ──────────────────
    ("commissioner_recs", """
        CREATE TABLE IF NOT EXISTS commissioner_recs (
            rec_id          SERIAL PRIMARY KEY,
            city_summary    TEXT,
            worst_zones     TEXT,   -- JSON array e.g. ["zone_3","zone_7"]
            budget_flag     BOOLEAN DEFAULT FALSE,
            theft_summary   TEXT,
            resolution_rate FLOAT DEFAULT 0,
            trigger_type    TEXT,
            created_at      TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── V7 Channel 4: Citizen recommendations (NO infra data) ───────
    ("citizen_recs", """
        CREATE TABLE IF NOT EXISTS citizen_recs (
            rec_id                  SERIAL PRIMARY KEY,
            zone_id                 TEXT,
            supply_status           TEXT,   -- Normal | Intermittent | Disrupted
            advisory_text           TEXT,
            complaint_guidance      TEXT,
            estimated_restoration   TEXT,
            created_at              TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── Citizen complaints (submitted via citizen_panel.html) ────────
    ("citizen_complaints", """
        CREATE TABLE IF NOT EXISTS citizen_complaints (
            complaint_id    SERIAL PRIMARY KEY,
            zone_id         TEXT,
            problem_type    TEXT,
            landmark        TEXT,
            description     TEXT,
            contact         TEXT,
            status          TEXT DEFAULT 'open',
            created_at      TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── V7 run log (for /recommendations/updated-at endpoint) ────────
    ("v7_run_log", """
        CREATE TABLE IF NOT EXISTS v7_run_log (
            run_id              SERIAL PRIMARY KEY,
            status              TEXT,
            zones_processed     INTEGER DEFAULT 0,
            recs_generated      INTEGER DEFAULT 0,
            engineer_count      INTEGER DEFAULT 0,
            ward_count          INTEGER DEFAULT 0,
            commissioner_count  INTEGER DEFAULT 0,
            citizen_count       INTEGER DEFAULT 0,
            ran_at              TIMESTAMP DEFAULT NOW()
        )
    """),
]


def create_tables():
    print("[db_setup_phase4b] Creating Phase 4b tables...")
    with engine.connect() as conn:
        for name, sql in TABLES:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"  [OK] {name}")
            except Exception as e:
                print(f"  [ERROR] {name}: {e}")
    print(f"[db_setup_phase4b] ✅ Done. {len(TABLES)} tables created (or already exist).")
    print("\n  Next step: python scripts/v7_recommendations.py")


if __name__ == '__main__':
    create_tables()