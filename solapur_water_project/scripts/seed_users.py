"""
Hydro-Equity Engine — Deployment Fix
scripts/seed_users.py

Seeds all 18 users into the PostgreSQL 'users' table.
Passwords are read exclusively from environment variables:
  ENGINEER_PASSWORD      — password for engineer1
  WARD_PASSWORD          — password for ward_z1 through ward_z8
  COMMISSIONER_PASSWORD  — password for commissioner1
  FIELD_OP_PASSWORD      — password for field_op_z1 through field_op_z8

If any required env var is missing, the script prints a clear error and exits.
Passwords are bcrypt-hashed using get_password_hash() from backend/auth.py.

Run:  python scripts/seed_users.py
Safe to re-run (ON CONFLICT DO UPDATE).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass  # python-dotenv not installed — use shell env vars ($env: in PowerShell)


# ── Read passwords from environment variables ──────────────────────────────────
REQUIRED_VARS = [
    "ENGINEER_PASSWORD",
    "WARD_PASSWORD",
    "COMMISSIONER_PASSWORD",
    "FIELD_OP_PASSWORD",
]

missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if missing:
    print("\n  ❌  MISSING REQUIRED ENVIRONMENT VARIABLES:")
    for v in missing:
        print(f"       {v} — not set or empty")
    print()
    print("  Add the following to your .env file (project root):")
    print("       ENGINEER_PASSWORD=<secure_password>")
    print("       WARD_PASSWORD=<secure_password>")
    print("       COMMISSIONER_PASSWORD=<secure_password>")
    print("       FIELD_OP_PASSWORD=<secure_password>")
    print()
    print("  Do NOT seed with empty passwords — aborting.")
    sys.exit(1)

ENGINEER_PASSWORD     = os.getenv("ENGINEER_PASSWORD")
WARD_PASSWORD         = os.getenv("WARD_PASSWORD")
COMMISSIONER_PASSWORD = os.getenv("COMMISSIONER_PASSWORD")
FIELD_OP_PASSWORD     = os.getenv("FIELD_OP_PASSWORD")


# ── All 18 users ───────────────────────────────────────────────────────────────
# Passwords are assigned by role at seeding time, not stored in this list.
DEMO_USERS = [

    # ── Original engineer ───────────────────────────────────────────────────
    {
        "username":    "engineer1",
        "password_key": "engineer",
        "role":        "engineer",
        "zone_id":     None,
        "full_name":   "Prabhav Tiwari — Engineer",
    },

    # ── Ward officers: zone_1 through zone_8 ────────────────────────────────
    {
        "username":    "ward_z1",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_1",
        "full_name":   "Ward Officer — Zone 1",
    },
    {
        "username":    "ward_z2",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_2",
        "full_name":   "Ward Officer — Zone 2",
    },
    {
        "username":    "ward_z3",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_3",
        "full_name":   "Ward Officer — Zone 3",
    },
    {
        "username":    "ward_z4",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_4",
        "full_name":   "Ward Officer — Zone 4",
    },
    {
        "username":    "ward_z5",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_5",
        "full_name":   "Ward Officer — Zone 5",
    },
    {
        "username":    "ward_z6",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_6",
        "full_name":   "Ward Officer — Zone 6",
    },
    {
        "username":    "ward_z7",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_7",
        "full_name":   "Ward Officer — Zone 7",
    },
    {
        "username":    "ward_z8",
        "password_key": "ward",
        "role":        "ward_officer",
        "zone_id":     "zone_8",
        "full_name":   "Ward Officer — Zone 8",
    },

    # ── Commissioner ────────────────────────────────────────────────────────
    {
        "username":    "commissioner1",
        "password_key": "commissioner",
        "role":        "commissioner",
        "zone_id":     None,
        "full_name":   "SMC Commissioner",
    },

    # ── Field operators: zone_1 through zone_8 ──────────────────────────────
    {
        "username":    "field_op_z1",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_1",
        "full_name":   "Field Operator — Zone 1",
    },
    {
        "username":    "field_op_z2",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_2",
        "full_name":   "Field Operator — Zone 2",
    },
    {
        "username":    "field_op_z3",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_3",
        "full_name":   "Field Operator — Zone 3",
    },
    {
        "username":    "field_op_z4",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_4",
        "full_name":   "Field Operator — Zone 4",
    },
    {
        "username":    "field_op_z5",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_5",
        "full_name":   "Field Operator — Zone 5",
    },
    {
        "username":    "field_op_z6",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_6",
        "full_name":   "Field Operator — Zone 6",
    },
    {
        "username":    "field_op_z7",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_7",
        "full_name":   "Field Operator — Zone 7",
    },
    {
        "username":    "field_op_z8",
        "password_key": "field_op",
        "role":        "field_operator",
        "zone_id":     "zone_8",
        "full_name":   "Field Operator — Zone 8",
    },
]

PASSWORD_MAP = {
    "engineer":     ENGINEER_PASSWORD,
    "ward":         WARD_PASSWORD,
    "commissioner": COMMISSIONER_PASSWORD,
    "field_op":     FIELD_OP_PASSWORD,
}

TOTAL_USERS    = len(DEMO_USERS)
FIELD_OP_COUNT = sum(1 for u in DEMO_USERS if u["role"] == "field_operator")
WARD_COUNT     = sum(1 for u in DEMO_USERS if u["role"] == "ward_officer")


def _check_db_connection(engine):
    """Test the DB connection before attempting seeding."""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        return False, str(e)


def seed():
    print("=" * 62)
    print("  seed_users.py — Dhara Deployment User Seeding")
    print(f"  Total users to seed: {TOTAL_USERS} "
          f"(1 engineer + {WARD_COUNT} ward officers + "
          f"1 commissioner + {FIELD_OP_COUNT} field operators)")
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
        print("  1. Make sure DATABASE_URL in .env points to Neon PostgreSQL")
        print("  2. Check your .env file at the project root")
        print("  3. Make sure the users table exists:")
        print("     python scripts/db_full_setup.py")
        print("  ────────────────────────────────────────────────────────")
        sys.exit(1)

    print("  ✅  PostgreSQL connection OK\n")

    # ── Seed users ────────────────────────────────────────────────────
    success_count = 0
    fail_count    = 0

    with engine.connect() as conn:
        for user in DEMO_USERS:
            try:
                plain_password = PASSWORD_MAP[user["password_key"]]
                hashed = get_password_hash(plain_password)
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
                print(f"  ✅  {user['username']:<18} ({user['role']:<16}) [{zone_str}]")
                success_count += 1
            except Exception as e:
                print(f"  ❌  {user['username']}: {e}")
                fail_count += 1

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n  {success_count}/{TOTAL_USERS} users seeded successfully."
          + (f"  {fail_count} failed." if fail_count else ""))

    if success_count == TOTAL_USERS:
        print("  ✅  All 18 users seeded successfully.")
    elif success_count >= 1:
        print("  ⚠   Some users seeded — check errors above.")
    else:
        print("  ❌  Seeding failed. Check PostgreSQL connection and try again.")

    print()
    print("  ─" * 33)
    print("  SEEDED USERS (passwords from env vars)")
    print("  ─" * 33)
    print(f"  {'Username':<18} {'Role':<18} {'Zone'}")
    print(f"  {'─'*18} {'─'*18} {'─'*10}")
    for u in DEMO_USERS:
        zone_str = u.get("zone_id") or "all zones"
        print(f"  {u['username']:<18} {u['role']:<18} {zone_str}")
    print()
    print("  Password env vars used:")
    print("    engineer1       → ENGINEER_PASSWORD")
    print("    ward_z1..z8     → WARD_PASSWORD")
    print("    commissioner1   → COMMISSIONER_PASSWORD")
    print("    field_op_z1..z8 → FIELD_OP_PASSWORD")
    print("=" * 62)


if __name__ == '__main__':
    seed()