"""
scripts/seed_citizens.py
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
CITIZEN_PASSWORD = os.getenv("CITIZEN_PASSWORD")

if not CITIZEN_PASSWORD:
    print("\n  [ERROR] MISSING REQUIRED ENVIRONMENT VARIABLE: CITIZEN_PASSWORD")
    sys.exit(1)

DEMO_CITIZENS = [
    {"name": "Citizen 1", "phone": "1999999999", "zone_id": "zone_1", "zone_name": "Zone 1"},
    {"name": "Citizen 2", "phone": "2999999999", "zone_id": "zone_2", "zone_name": "Zone 2"},
    {"name": "Citizen 3", "phone": "3999999999", "zone_id": "zone_3", "zone_name": "Zone 3"},
    {"name": "Citizen 4", "phone": "4999999999", "zone_id": "zone_4", "zone_name": "Zone 4"},
    {"name": "Citizen 5", "phone": "5999999999", "zone_id": "zone_5", "zone_name": "Zone 5"},
    {"name": "Citizen 6", "phone": "6999999999", "zone_id": "zone_6", "zone_name": "Zone 6"},
    {"name": "Citizen 7", "phone": "7999999999", "zone_id": "zone_7", "zone_name": "Zone 7"},
    {"name": "Citizen 8", "phone": "8999999999", "zone_id": "zone_8", "zone_name": "Zone 8"},
    {"name": "Cit 1", "phone": "1888888888", "zone_id": "zone_1", "zone_name": "Zone 1"},
    {"name": "Cit 2", "phone": "2888888888", "zone_id": "zone_2", "zone_name": "Zone 2"},
    {"name": "Cit 3", "phone": "3888888888", "zone_id": "zone_3", "zone_name": "Zone 3"},
    {"name": "Cit 4", "phone": "4888888888", "zone_id": "zone_4", "zone_name": "Zone 4"},
    {"name": "Cit 5", "phone": "5888888888", "zone_id": "zone_5", "zone_name": "Zone 5"},
    {"name": "Cit 6", "phone": "6888888888", "zone_id": "zone_6", "zone_name": "Zone 6"},
    {"name": "Cit 7", "phone": "7888888888", "zone_id": "zone_7", "zone_name": "Zone 7"},
    {"name": "Cit 8", "phone": "8888888888", "zone_id": "zone_8", "zone_name": "Zone 8"},
]

def seed():
    try:
        from sqlalchemy import text
        from backend.database import engine
        from backend.auth import get_password_hash
    except Exception as e:
        print(f"\n  [ERROR] Import error: {e}")
        sys.exit(1)

    with engine.connect() as conn:
        hashed = get_password_hash(CITIZEN_PASSWORD)
        for c in DEMO_CITIZENS:
            conn.execute(
                text("""
                    INSERT INTO citizens
                        (phone, hashed_password, name, zone_id, zone_name)
                    VALUES
                        (:phone, :hashed_password, :name, :zone_id, :zone_name)
                    ON CONFLICT (phone) DO UPDATE SET
                        hashed_password = EXCLUDED.hashed_password,
                        name            = EXCLUDED.name,
                        zone_id         = EXCLUDED.zone_id,
                        zone_name       = EXCLUDED.zone_name,
                        is_active       = TRUE
                """),
                {
                    "phone": c["phone"],
                    "hashed_password": hashed,
                    "name": c["name"],
                    "zone_id": c["zone_id"],
                    "zone_name": c["zone_name"],
                }
            )
            conn.commit()
            print(f"  [OK] Seeded {c['name']} ({c['phone']}) -> {c['zone_name']}")

if __name__ == '__main__':
    seed()
