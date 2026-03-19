"""
Dhara — M1 Alert State Machine DB Setup
scripts/db_setup_alerts.py

Extends the existing 'alerts' table with new columns required for the
4-state alert lifecycle: new → acknowledged → resolve_requested → resolved

Bible Reference: Section 3 M1 — "New Database Table — alerts (extended)"

New columns added:
    acknowledged_by   TEXT         — username of engineer who acknowledged
    resolution_report TEXT         — field operator's work notes
    rejected_count    INTEGER DEFAULT 0 — how many times resolution was rejected

Also:
    - Changes status default from 'fired' to 'new'
    - Migrates existing 'fired' rows → 'new' so they enter the state machine

Safe to run multiple times (uses IF NOT EXISTS / checks before altering).

Run: python scripts/db_setup_alerts.py
"""

import sys
import os

# Add project root to path so we can import backend modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import text
from backend.database import engine


def setup_alert_columns():
    print("=" * 62)
    print("  Dhara M1 — Alert State Machine DB Setup")
    print("=" * 62)

    # ── Step 1: Add new columns (IF NOT EXISTS = safe to re-run) ──
    new_columns = [
        (
            "acknowledged_by",
            "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_by TEXT"
        ),
        (
            "resolution_report",
            "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS resolution_report TEXT"
        ),
        (
            "rejected_count",
            "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS rejected_count INTEGER DEFAULT 0"
        ),
    ]

    print("\n[Step 1] Adding new columns...")
    with engine.connect() as conn:
        for col_name, sql in new_columns:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"  [OK] Column '{col_name}' added (or already existed)")
            except Exception as e:
                print(f"  [WARN] Column '{col_name}': {e}")

    # ── Step 2: Update status column default from 'fired' → 'new' ──
    # The original db_setup.py used DEFAULT 'fired'.
    # The M1 state machine uses: new | acknowledged | resolve_requested | resolved
    print("\n[Step 2] Updating status column default to 'new'...")
    with engine.connect() as conn:
        try:
            conn.execute(text(
                "ALTER TABLE alerts ALTER COLUMN status SET DEFAULT 'new'"
            ))
            conn.commit()
            print("  [OK] status DEFAULT changed to 'new'")
        except Exception as e:
            print(f"  [WARN] status default update: {e}")

    # ── Step 3: Migrate existing 'fired' rows → 'new' ──────────────
    # So they appear in the state machine properly
    print("\n[Step 3] Migrating 'fired' rows → 'new'...")
    with engine.connect() as conn:
        try:
            result = conn.execute(text(
                "UPDATE alerts SET status = 'new' WHERE status = 'fired'"
            ))
            conn.commit()
            print(f"  [OK] Updated {result.rowcount} row(s) from 'fired' → 'new'")
        except Exception as e:
            print(f"  [WARN] Migration: {e}")

    # ── Step 4: Verify final schema ─────────────────────────────────
    print("\n[Step 4] Verifying final alerts table schema...")
    with engine.connect() as conn:
        try:
            rows = conn.execute(text("""
                SELECT column_name, data_type, column_default
                FROM information_schema.columns
                WHERE table_name = 'alerts'
                ORDER BY ordinal_position
            """)).fetchall()
            for r in rows:
                print(f"  {r[0]:<25} {r[1]:<20} default={r[2]}")
        except Exception as e:
            print(f"  [WARN] Schema check: {e}")

    print("\n" + "=" * 62)
    print("  ✅  db_setup_alerts.py complete.")
    print("  Next: python -m uvicorn backend.app:app --port 8000 --reload")
    print("=" * 62)


if __name__ == '__main__':
    setup_alert_columns()