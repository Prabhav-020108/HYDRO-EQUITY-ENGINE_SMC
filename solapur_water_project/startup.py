"""
startup.py — Runs before uvicorn on every Render deployment.
Safe to run multiple times (all operations are idempotent).
Does NOT run db_migrate.py — that would wipe alert lifecycle data.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 60)
print("  Dhara Startup — Schema + Seed")
print("=" * 60)

# Step 1: Create all tables (CREATE TABLE IF NOT EXISTS)
print("\n[1/3] Running db_full_setup.py...")
try:
    from scripts.db_full_setup import setup
    setup()
    print("  ✅ Schema ready")
except Exception as e:
    print(f"  ❌ Schema setup failed: {e}")
    sys.exit(1)

# Step 2: Seed users (ON CONFLICT DO UPDATE — idempotent)
print("\n[2/3] Running seed_users.py...")
try:
    from scripts.seed_users import seed
    seed()
    print("  ✅ Users seeded")
except Exception as e:
    print(f"  ⚠  User seeding failed (non-fatal): {e}")
    # Non-fatal: users may already exist

# Step 2b: Seed citizens (ON CONFLICT DO UPDATE — idempotent)
print("\n[2b/3] Running seed_citizens.py...")
try:
    from scripts.seed_citizens import seed as seed_citizens
    seed_citizens()
    print("  ✅ Citizens seeded")
except Exception as e:
    print(f"  ⚠  Citizen seeding failed (non-fatal): {e}")
    # Non-fatal: citizens may already exist

# Step 3: Check if alerts table is empty — if yes, run initial migrate
print("\n[3/3] Checking if initial data migration is needed...")
try:
    from backend.database import engine
    from sqlalchemy import text
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM alerts")).scalar()
    if count == 0:
        print("  Alerts table is empty — running initial db_migrate.py...")
        from scripts.db_migrate import (
            migrate_pipe_segments, migrate_nodes,
            migrate_zone_demand, migrate_equity_scores,
            migrate_alerts, migrate_pipe_stress
        )
        migrate_pipe_segments()
        migrate_nodes()
        migrate_zone_demand()
        migrate_equity_scores()
        migrate_alerts()
        migrate_pipe_stress()
        print("  ✅ Initial data migration complete")
    else:
        print(f"  ✅ Alerts table has {count} rows — skipping migration (data preserved)")
except Exception as e:
    print(f"  ⚠  Initial migration check failed (non-fatal): {e}")

print("\n✅ Startup complete. Starting server...")
