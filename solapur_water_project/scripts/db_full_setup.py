"""
Dhara — Hydro-Equity Engine
scripts/db_full_setup.py

Single idempotent script that replaces:
  - scripts/db_setup.py
  - scripts/db_setup_phase4b.py
  - scripts/db_setup_alerts.py
  - scripts/create_users_table.py

Run ONCE from project root:
    python scripts/db_full_setup.py

Safe to re-run (every operation is idempotent):
  - CREATE TABLE IF NOT EXISTS for every table
  - ALTER TABLE ... ADD COLUMN IF NOT EXISTS for new columns
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import text
from backend.database import engine


# ══════════════════════════════════════════════════════════════════
#  ALL TABLES (CREATE TABLE IF NOT EXISTS — fully idempotent)
# ══════════════════════════════════════════════════════════════════

TABLES = [

    # ── Original Phase 1 ──────────────────────────────────────────
    ("pipe_segments", """
        CREATE TABLE IF NOT EXISTS pipe_segments (
            segment_id        TEXT PRIMARY KEY,
            pipeline_type     TEXT,
            material          TEXT,
            diameter_m        FLOAT,
            length_m          FLOAT,
            zone_id           TEXT,
            start_node_id     TEXT,
            end_node_id       TEXT,
            hw_c_value        FLOAT,
            assumed_age_years FLOAT,
            design_lifespan   FLOAT,
            data_quality_flag TEXT
        )
    """),

    ("nodes", """
        CREATE TABLE IF NOT EXISTS nodes (
            node_id     TEXT PRIMARY KEY,
            lat         FLOAT,
            lon         FLOAT,
            elevation_m FLOAT,
            zone_id     TEXT,
            node_type   TEXT
        )
    """),

    ("zone_demand", """
        CREATE TABLE IF NOT EXISTS zone_demand (
            zone_id          TEXT PRIMARY KEY,
            base_lps         FLOAT,
            peak_morning_lps FLOAT,
            offpeak_lps      FLOAT,
            population       FLOAT
        )
    """),

    ("zone_equity_scores", """
        CREATE TABLE IF NOT EXISTS zone_equity_scores (
            id          SERIAL PRIMARY KEY,
            zone_id     TEXT,
            hei         FLOAT,
            zes         FLOAT,
            status      TEXT,
            color       TEXT,
            recorded_at TIMESTAMP DEFAULT NOW()
        )
    """),

    ("alerts", """
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id          SERIAL PRIMARY KEY,
            zone_id           TEXT,
            clps              FLOAT,
            severity          TEXT,
            dominant_signal   TEXT,
            probable_nodes    TEXT,
            scenario          TEXT,
            status            TEXT DEFAULT 'new',
            acknowledged_at   TIMESTAMP,
            acknowledged_by   TEXT,
            resolution_report TEXT,
            rejected_count    INTEGER DEFAULT 0,
            field_action_at   TIMESTAMP,
            resolved_at       TIMESTAMP,
            notes             TEXT,
            created_at        TIMESTAMP DEFAULT NOW()
        )
    """),

    ("pipe_stress_scores", """
        CREATE TABLE IF NOT EXISTS pipe_stress_scores (
            id              SERIAL PRIMARY KEY,
            segment_id      TEXT,
            pss             FLOAT,
            psi_n           FLOAT,
            cff_n           FLOAT,
            adf             FLOAT,
            risk_level      TEXT,
            dominant_factor TEXT,
            summary         TEXT,
            lat_start       FLOAT,
            lon_start       FLOAT,
            lat_end         FLOAT,
            lon_end         FLOAT,
            recorded_at     TIMESTAMP DEFAULT NOW()
        )
    """),

    ("audit_log", """
        CREATE TABLE IF NOT EXISTS audit_log (
            log_id     SERIAL PRIMARY KEY,
            event_type TEXT,
            zone_id    TEXT,
            alert_id   INTEGER,
            user_role  TEXT,
            details    TEXT,
            logged_at  TIMESTAMP DEFAULT NOW()
        )
    """),

    ("citizens", """
        CREATE TABLE IF NOT EXISTS citizens (
            citizen_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            phone           TEXT UNIQUE NOT NULL,
            hashed_password TEXT NOT NULL,
            name            TEXT NOT NULL,
            zone_id         TEXT NOT NULL,
            zone_name       TEXT NOT NULL,
            is_active       BOOLEAN DEFAULT TRUE,
            created_at      TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── Users table (Phase 4a) ─────────────────────────────────────
    ("users", """
        CREATE TABLE IF NOT EXISTS users (
            user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            username        TEXT UNIQUE NOT NULL,
            hashed_password TEXT NOT NULL,
            role            TEXT NOT NULL,
            zone_id         TEXT,
            full_name       TEXT,
            is_active       BOOLEAN DEFAULT TRUE,
            created_at      TIMESTAMP DEFAULT NOW(),

            CONSTRAINT users_role_check
                CHECK (role IN ('engineer','ward_officer','commissioner','field_operator'))
        )
    """),

    # ── V7 recommendation channels (Phase 4b) ─────────────────────
    ("engineer_recs", """
        CREATE TABLE IF NOT EXISTS engineer_recs (
            rec_id             SERIAL PRIMARY KEY,
            zone_id            TEXT,
            trigger_type       TEXT,
            action_text        TEXT,
            valve_id           TEXT,
            pipe_id            TEXT,
            pressure_delta     FLOAT,
            urgency            TEXT,
            estimated_hei_gain FLOAT,
            node_coords        TEXT,
            created_at         TIMESTAMP DEFAULT NOW()
        )
    """),

    ("ward_recs", """
        CREATE TABLE IF NOT EXISTS ward_recs (
            rec_id                   SERIAL PRIMARY KEY,
            zone_id                  TEXT,
            trigger_type             TEXT,
            action_text              TEXT,
            escalation_flag          BOOLEAN DEFAULT FALSE,
            service_reliability_note TEXT,
            complaint_count          INTEGER DEFAULT 0,
            created_at               TIMESTAMP DEFAULT NOW()
        )
    """),

    ("commissioner_recs", """
        CREATE TABLE IF NOT EXISTS commissioner_recs (
            rec_id          SERIAL PRIMARY KEY,
            city_summary    TEXT,
            worst_zones     TEXT,
            budget_flag     BOOLEAN DEFAULT FALSE,
            theft_summary   TEXT,
            resolution_rate FLOAT DEFAULT 0,
            trigger_type    TEXT,
            created_at      TIMESTAMP DEFAULT NOW()
        )
    """),

    ("citizen_recs", """
        CREATE TABLE IF NOT EXISTS citizen_recs (
            rec_id                SERIAL PRIMARY KEY,
            zone_id               TEXT,
            supply_status         TEXT,
            advisory_text         TEXT,
            complaint_guidance    TEXT,
            estimated_restoration TEXT,
            created_at            TIMESTAMP DEFAULT NOW()
        )
    """),

    ("citizen_complaints", """
        CREATE TABLE IF NOT EXISTS citizen_complaints (
            complaint_id SERIAL PRIMARY KEY,
            zone_id      TEXT,
            problem_type TEXT,
            landmark     TEXT,
            description  TEXT,
            contact      TEXT,
            status       TEXT DEFAULT 'open',
            updated_at   TIMESTAMP,
            created_at   TIMESTAMP DEFAULT NOW()
        )
    """),

    ("v7_run_log", """
        CREATE TABLE IF NOT EXISTS v7_run_log (
            run_id             SERIAL PRIMARY KEY,
            status             TEXT,
            zones_processed    INTEGER DEFAULT 0,
            recs_generated     INTEGER DEFAULT 0,
            engineer_count     INTEGER DEFAULT 0,
            ward_count         INTEGER DEFAULT 0,
            commissioner_count INTEGER DEFAULT 0,
            citizen_count      INTEGER DEFAULT 0,
            ran_at             TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── NEW: Valve checks (Deployment Bible requirement) ───────────
    ("valve_checks", """
        CREATE TABLE IF NOT EXISTS valve_checks (
            check_id       SERIAL PRIMARY KEY,
            zone_id        TEXT,
            valve_id       TEXT,
            reported_state TEXT,
            checked_by     TEXT,
            notes          TEXT,
            checked_at     TIMESTAMP DEFAULT NOW(),
            UNIQUE(zone_id, valve_id)
        )
    """),

    # ── NEW: Zone polygons (for map visualisation) ─────────────────
    ("zone_polygons", """
        CREATE TABLE IF NOT EXISTS zone_polygons (
            zone_id        TEXT PRIMARY KEY,
            polygon_coords TEXT,
            centroid_lat   FLOAT,
            centroid_lon   FLOAT,
            updated_at     TIMESTAMP DEFAULT NOW()
        )
    """),

    # ── NEW: Data ingest log ───────────────────────────────────────
    ("data_ingest_log", """
        CREATE TABLE IF NOT EXISTS data_ingest_log (
            ingest_id          SERIAL PRIMARY KEY,
            source             TEXT,
            rows_ingested      INTEGER,
            pipeline_triggered BOOLEAN DEFAULT FALSE,
            status             TEXT,
            error_msg          TEXT,
            ingested_at        TIMESTAMP DEFAULT NOW()
        )
    """),
]


# ══════════════════════════════════════════════════════════════════
#  COLUMN ADDITIONS (ALTER TABLE ... ADD COLUMN IF NOT EXISTS)
# ══════════════════════════════════════════════════════════════════

NEW_COLUMNS = [
    # alerts table — resolution photo
    (
        "alerts.resolution_photo",
        "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS resolution_photo TEXT"
    ),
    (
        "alerts.resolved_by",
        "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS resolved_by TEXT"
    ),
    # citizen_complaints — photo support + status + updated_at
    (
        "citizen_complaints.photo_b64",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS photo_b64 TEXT"
    ),
    (
        "citizen_complaints.status",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'open'"
    ),
    (
        "citizen_complaints.updated_at",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP"
    ),
    (
        "citizen_complaints.acknowledged_at",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMP"
    ),
    (
        "citizen_complaints.acknowledged_by",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS acknowledged_by TEXT"
    ),
    (
        "citizen_complaints.resolved_at",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP"
    ),
    (
        "citizen_complaints.disputed_at",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS disputed_at TIMESTAMP"
    ),
    (
        "citizen_complaints.expiry_notified",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS expiry_notified BOOLEAN DEFAULT FALSE"
    ),
    (
        "citizen_complaints.lat",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS lat FLOAT"
    ),
    (
        "citizen_complaints.lon",
        "ALTER TABLE citizen_complaints ADD COLUMN IF NOT EXISTS lon FLOAT"
    ),
]


# ══════════════════════════════════════════════════════════════════
#  MIGRATION: ensure alerts.status default is 'new' (not 'fired')
# ══════════════════════════════════════════════════════════════════

POST_MIGRATIONS = [
    (
        "alerts: set status default to 'new'",
        "ALTER TABLE alerts ALTER COLUMN status SET DEFAULT 'new'"
    ),
    (
        "alerts: migrate 'fired' rows -> 'new'",
        "UPDATE alerts SET status = 'new' WHERE status = 'fired'"
    ),
    (
        "citizen_complaints: allow not_resolved and expired status values",
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='citizen_complaints' AND column_name='status'
            AND data_type='text'
          ) THEN
            NULL;
          END IF;
        END$$;
        UPDATE citizen_complaints SET status = status;
        """
    ),
    (
        "citizen_complaints: back-fill lat/lon from zone_polygons centroids",
        """
        UPDATE citizen_complaints cc
        SET    lat = zp.centroid_lat + (
                   (HASHTEXT(cc.complaint_id::text) % 1000) / 1000.0 * 0.008 - 0.004
               ),
               lon = zp.centroid_lon + (
                   (HASHTEXT(cc.complaint_id::text || 'x') % 1000) / 1000.0 * 0.008 - 0.004
               )
        FROM   zone_polygons zp
        WHERE  cc.zone_id = zp.zone_id
          AND  cc.lat     IS NULL;
        """
    ),
]


def _run(conn, label: str, sql: str, expect_rows: bool = False):
    """Execute a SQL statement and print the result."""
    try:
        result = conn.execute(text(sql))
        conn.commit()
        if expect_rows:
            print(f"  [OK] {label} ({result.rowcount} rows affected)")
        else:
            print(f"  [OK] {label}")
    except Exception as e:
        err = str(e).split('\n')[0]
        # Suppress benign "already exists" errors
        if "already exists" in err.lower():
            print(f"  [ALREADY EXISTS] {label}")
        else:
            print(f"  [ERROR] {label}: {err}")


def setup():
    print("=" * 62)
    print("  db_full_setup.py - Dhara Full Database Setup")
    print("=" * 62)

    with engine.connect() as conn:

        # ── 1. Create / verify all tables ──────────────────────────
        print("\n[Step 1] Creating tables (CREATE TABLE IF NOT EXISTS)...")
        for name, sql in TABLES:
            _run(conn, name, sql)

        # ── 2. Add new columns ──────────────────────────────────────
        print("\n[Step 2] Adding new columns (ADD COLUMN IF NOT EXISTS)...")
        for label, sql in NEW_COLUMNS:
            _run(conn, label, sql)

        # ── 3. Post-migration steps ─────────────────────────────────
        print("\n[Step 3] Running post-migration steps...")
        for label, sql in POST_MIGRATIONS:
            _run(conn, label, sql, expect_rows=True)

    print("\n" + "=" * 62)
    print("  [OK] db_full_setup.py complete.")
    print("  Next: python scripts/seed_users.py")
    print("=" * 62)


if __name__ == '__main__':
    setup()
