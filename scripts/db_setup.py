"""
Hydro-Equity Engine — Database Setup Script (Fixed)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from sqlalchemy import text
from backend.database import engine

def create_tables():
    tables = [
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
                node_id      TEXT PRIMARY KEY,
                lat          FLOAT,
                lon          FLOAT,
                elevation_m  FLOAT,
                zone_id      TEXT,
                node_type    TEXT
            )
        """),
        ("zone_demand", """
            CREATE TABLE IF NOT EXISTS zone_demand (
                zone_id           TEXT PRIMARY KEY,
                base_lps          FLOAT,
                peak_morning_lps  FLOAT,
                offpeak_lps       FLOAT,
                population        FLOAT
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
                alert_id        SERIAL PRIMARY KEY,
                zone_id         TEXT,
                clps            FLOAT,
                severity        TEXT,
                dominant_signal TEXT,
                probable_nodes  TEXT,
                scenario        TEXT,
                status          TEXT DEFAULT 'fired',
                acknowledged_at TIMESTAMP,
                field_action_at TIMESTAMP,
                resolved_at     TIMESTAMP,
                notes           TEXT,
                created_at      TIMESTAMP DEFAULT NOW()
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
                log_id      SERIAL PRIMARY KEY,
                event_type  TEXT,
                zone_id     TEXT,
                alert_id    INTEGER,
                user_role   TEXT,
                details     TEXT,
                logged_at   TIMESTAMP DEFAULT NOW()
            )
        """),
    ]

    print("[db_setup] Creating tables...")
    with engine.connect() as conn:
        for name, sql in tables:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"  [OK] {name}")
            except Exception as e:
                print(f"  [ERROR] {name}: {e}")
    print("[db_setup] ✅ Done.")

if __name__ == '__main__':
    create_tables()