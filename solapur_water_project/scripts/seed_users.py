"""
Hydro-Equity Engine — Phase 4a + M4 + Phase 2
scripts/seed_users.py

Seeds demo users into the PostgreSQL 'users' table.

M4 ADDITION: 8 field_operator users (field_op_z1 through field_op_z8)
             one per zone, password: demo@1234, role: field_operator

Phase 2 ADDITION: ward_z3 through ward_z8 (ward_officer, zone_3..zone_8,
                  password: demo@1234)

WORKS BOTH WAYS:
  - If PostgreSQL is running: seeds all 18 users into the DB
  - If PostgreSQL is unavailable: shows a clear error message with fix steps
  - AUTH_DEV_MODE note: seeding always requires PostgreSQL (users must be stored
    somewhere). AUTH_DEV_MODE bypasses DB *at runtime* for login only.

Run:  python scripts/seed_users.py
Safe to re-run (ON CONFLICT DO UPDATE — updates existing users).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# ── All 18 demo users ──────────────────────────────────────────────────────────
DEMO_USERS = [

    # ── Original 4 users (Phase 4a) ──────────────────────────────────────────
    {
        "username":  "engineer1",
        "password":  "demo123",
        "role":      "engineer",
        "zone_id":   None,
        "full_name": "Prabhav Tiwari — Engineer",
    },
    {
        "username":  "ward_z1",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_1",
        "full_name": "Ward Officer — Zone 1",
    },
    {
        "username":  "ward_z2",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_2",
        "full_name": "Ward Officer — Zone 2",
    },
    {
        "username":  "commissioner1",
        "password":  "demo123",
        "role":      "commissioner",
        "zone_id":   None,
        "full_name": "SMC Commissioner",
    },

    # ── Phase 2 NEW: ward_z3 through ward_z8 ─────────────────────────────────
    {
        "username":  "ward_z3",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_3",
        "full_name": "Ward Officer — Zone 3",
    },
    {
        "username":  "ward_z4",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_4",
        "full_name": "Ward Officer — Zone 4",
    },
    {
        "username":  "ward_z5",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_5",
        "full_name": "Ward Officer — Zone 5",
    },
    {
        "username":  "ward_z6",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_6",
        "full_name": "Ward Officer — Zone 6",
    },
    {
        "username":  "ward_z7",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_7",
        "full_name": "Ward Officer — Zone 7",
    },
    {
        "username":  "ward_z8",
        "password":  "demo123",
        "role":      "ward_officer",
        "zone_id":   "zone_8",
        "full_name": "Ward Officer — Zone 8",
    },

    # ── M4 NEW: 8 field operators, one per zone ───────────────────────────────
    {
        "username":  "field_op_z1",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_1",
        "full_name": "Field Operator — Zone 1",
    },
    {
        "username":  "field_op_z2",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_2",
        "full_name": "Field Operator — Zone 2",
    },
    {
        "username":  "field_op_z3",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_3",
        "full_name": "Field Operator — Zone 3",
    },
    {
        "username":  "field_op_z4",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_4",
        "full_name": "Field Operator — Zone 4",
    },
    {
        "username":  "field_op_z5",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_5",
        "full_name": "Field Operator — Zone 5",
    },
    {
        "username":  "field_op_z6",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_6",
        "full_name": "Field Operator — Zone 6",
    },
    {
        "username":  "field_op_z7",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_7",
        "full_name": "Field Operator — Zone 7",
    },
    {
        "username":  "field_op_z8",
        "password":  "demo123",
        "role":      "field_operator",
        "zone_id":   "zone_8",
        "full_name": "Field Operator — Zone 8",
    },
]

FIELD_OP_COUNT   = sum(1 for u in DEMO_USERS if u["role"] == "field_operator")
WARD_COUNT       = sum(1 for u in DEMO_USERS if u["role"] == "ward_officer")
TOTAL_USERS      = len(DEMO_USERS)


def _check_db_connection(engine):
    """
    Test the DB connection before attempting seeding.
    Returns (True, None) on success or (False, error_message) on failure.
    """
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        return False, str(e)


def seed():
    print("=" * 62)
    print("  seed_users.py · Phase 4a + M4 + Phase 2 User Seeding")
    print(f"  Total users to seed: {TOTAL_USERS} "
          f"(4 original + {WARD_COUNT - 1} ward officers + {FIELD_OP_COUNT} field operators)")
    print("=" * 62)

    # ── Import DB dependencies ────────────────────────────────────────
    try:
        from sqlalchemy import text
        from backend.database import engine
        from backend.auth import get_password_hash
    except Exception as e:
        print(f"\n  ❌  Import error: {e}")
        print("  Make sure you are running from the project root:")
        print("      python scripts/seed_users.py")
        sys.exit(1)

    # ── Test DB connection first ──────────────────────────────────────
    print("\n  Testing PostgreSQL connection...")
    db_ok, db_err = _check_db_connection(engine)

    if not db_ok:
        print(f"\n  ❌  Cannot connect to PostgreSQL: {db_err}")
        print()
        print("  ── HOW TO FIX ──────────────────────────────────────────")
        print("  1. Make sure PostgreSQL is running")
        print("     Windows: Open Services → start 'postgresql-x64-XX'")
        print()
        print("  2. Check your .env file at the project root:")
        print("     DB_HOST=localhost")
        print("     DB_PORT=5432")
        print("     DB_NAME=hydro_equity")
        print("     DB_USER=postgres")
        print("     DB_PASSWORD=<your_password>")
        print()
        print("  3. Make sure the database exists:")
        print("     psql -U postgres -c \"CREATE DATABASE hydro_equity;\"")
        print()
        print("  4. Make sure the users table exists:")
        print("     python scripts/create_users_table.py")
        print()
        print("  NOTE: AUTH_DEV_MODE=1 lets the SERVER run without DB,")
        print("  but seeding always requires PostgreSQL to store users.")
        print("  ────────────────────────────────────────────────────────")
        sys.exit(1)

    print("  ✅  PostgreSQL connection OK\n")

    # ── Seed users ────────────────────────────────────────────────────
    success_count = 0
    fail_count    = 0

    with engine.connect() as conn:
        for user in DEMO_USERS:
            try:
                hashed = get_password_hash(user["password"])
                conn.execute(
                    text("""
                        INSERT INTO users
                            (username, hashed_password, role, zone_id, full_name)
                        VALUES
                            (:username, :hashed_password, :role, :zone_id, :full_name)
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
                if user["role"] == "field_operator":
                    tag = "NEW (M4)"
                elif user["username"] in ("ward_z3","ward_z4","ward_z5","ward_z6","ward_z7","ward_z8"):
                    tag = "NEW (Ph2)"
                else:
                    tag = "original"
                print(f"  ✅  {user['username']:<18} ({user['role']:<16}) "
                      f"[{zone_str}]  [{tag}]")
                success_count += 1
            except Exception as e:
                print(f"  ❌  {user['username']}: {e}")
                fail_count += 1

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n  {success_count}/{TOTAL_USERS} users seeded successfully."
          + (f"  {fail_count} failed." if fail_count else ""))

    if success_count == TOTAL_USERS:
        print("  ✅  All users seeded — M4 seeding complete.")
    elif success_count >= 4:
        print("  ⚠   Original users OK but some M4 field operators may have failed.")
        print("      Check the errors above.")
    else:
        print("  ❌  Seeding had significant failures. Check PostgreSQL and try again.")

    # ── Credentials table ─────────────────────────────────────────────
    print()
    print("  ─" * 33)
    print("  DEMO LOGIN CREDENTIALS (all passwords: demo123)")
    print("  ─" * 33)
    print(f"  {'Username':<18} {'Role':<18} {'Zone'}")
    print(f"  {'─'*18} {'─'*18} {'─'*10}")
    for u in DEMO_USERS:
        zone_str = u.get("zone_id") or "all zones"
        print(f"  {u['username']:<18} {u['role']:<18} {zone_str}")
    print()
    print(f"  M4 field operators : {FIELD_OP_COUNT} users (field_op_z1 through field_op_z8)")
    print(f"  Phase 2 ward officers added: ward_z3 through ward_z8 (demo@1234)")
    print()
    print("  ─" * 33)
    print("  Next step: restart the server")
    print("  $env:AUTH_DEV_MODE = '1'; "
          "python -m uvicorn backend.app:app --reload --port 8000")
    print("=" * 62)


if __name__ == '__main__':
    seed()