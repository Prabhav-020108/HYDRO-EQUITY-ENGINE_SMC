"""
Hydro-Equity Engine — Phase 4a
scripts/create_users_table.py
Creates the 'users' table in PostgreSQL for JWT authentication.

Run ONCE from project root:
    python scripts/create_users_table.py

Safe to re-run (uses CREATE TABLE IF NOT EXISTS).
"""

import sys
import os

# Add project root to path so we can import backend modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import text
from backend.database import engine


def create_users_table():
    sql = """
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
        );
    """

    print("[create_users_table] Creating users table in PostgreSQL...")
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        print("[create_users_table] ✅ users table created (or already exists).")
        print("\n  Next step: python scripts/seed_users.py")
    except Exception as e:
        print(f"[create_users_table] ❌ Error: {e}")
        print("\n  Troubleshooting:")
        print("  1. Make sure PostgreSQL is running")
        print("  2. Check your .env file has correct DB_PASSWORD")
        print("  3. Check your .env file exists at the project root")


if __name__ == '__main__':
    create_users_table()