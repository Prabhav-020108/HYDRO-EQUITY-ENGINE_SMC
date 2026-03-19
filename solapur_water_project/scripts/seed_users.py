"""
Hydro-Equity Engine — Phase 4a
scripts/seed_users.py
Seeds 6 demo users into the PostgreSQL 'users' table.

Run AFTER create_users_table.py:
    python scripts/seed_users.py

Safe to re-run (uses ON CONFLICT DO UPDATE — updates existing users).
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import text
from backend.database import engine
from backend.auth import get_password_hash


# ── 6 Demo Users (covers all 4 roles) ─────────────────────────────
DEMO_USERS = [
    {
        "username":  "engineer1",
        "password":  "demo@1234",
        "role":      "engineer",
        "zone_id":   None,               # engineers see all zones
        "full_name": "Prabhav Tiwari — Engineer",
    },
    {
        "username":  "ward_z1",
        "password":  "demo@1234",
        "role":      "ward_officer",
        "zone_id":   "zone_1",           # ward officer for Zone 1
        "full_name": "Ward Officer — Zone 1",
    },
    {
        "username":  "ward_z2",
        "password":  "demo@1234",
        "role":      "ward_officer",
        "zone_id":   "zone_2",           # ward officer for Zone 2
        "full_name": "Ward Officer — Zone 2",
    },
    {
        "username":  "commissioner1",
        "password":  "demo@1234",
        "role":      "commissioner",
        "zone_id":   None,               # commissioners see all zones
        "full_name": "SMC Commissioner",
    },
    
]


def seed():
    print("=" * 60)
    print("  seed_users.py · Phase 4a Demo User Seeding")
    print("=" * 60)
    print(f"  Seeding {len(DEMO_USERS)} demo users...\n")

    success_count = 0

    with engine.connect() as conn:
        for user in DEMO_USERS:
            hashed = get_password_hash(user["password"])
            try:
                conn.execute(
                    text("""
                        INSERT INTO users (username, hashed_password, role, zone_id, full_name)
                        VALUES (:username, :hashed_password, :role, :zone_id, :full_name)
                        ON CONFLICT (username) DO UPDATE SET
                            hashed_password = EXCLUDED.hashed_password,
                            role            = EXCLUDED.role,
                            zone_id         = EXCLUDED.zone_id,
                            full_name       = EXCLUDED.full_name,
                            is_active       = TRUE
                    """),
                    {
                        "username":        user["username"],
                        "hashed_password": hashed,
                        "role":            user["role"],
                        "zone_id":         user.get("zone_id"),
                        "full_name":       user["full_name"],
                    }
                )
                conn.commit()
                zone_str = f"zone={user['zone_id']}" if user.get("zone_id") else "all zones"
                print(f"  ✅  {user['username']:<16} ({user['role']:<16}) [{zone_str}]")
                success_count += 1
            except Exception as e:
                print(f"  ❌  {user['username']}: {e}")

    print(f"\n  {success_count}/{len(DEMO_USERS)} users seeded successfully.")
    print()
    print("  ─" * 30)
    print("  DEMO LOGIN CREDENTIALS (all passwords: demo@1234)")
    print("  ─" * 30)
    print(f"  {'Username':<16} {'Password':<14} {'Role':<18} {'Zone'}")
    print(f"  {'─'*16} {'─'*14} {'─'*18} {'─'*10}")
    for u in DEMO_USERS:
        zone_str = u.get("zone_id") or "all zones"
        print(f"  {u['username']:<16} {u['password']:<14} {u['role']:<18} {zone_str}")

    print()
    print("  ─" * 30)
    print("  Next step: uvicorn backend.app:app --reload --port 8000")
    print("=" * 60)


if __name__ == '__main__':
    seed()