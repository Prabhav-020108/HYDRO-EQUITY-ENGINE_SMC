"""
Hydro-Equity Engine — PostgreSQL Connection Module
Shared across all backend scripts and app.py
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env from project root (one level up from backend/)
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_PORT     = os.getenv('DB_PORT', '5432')
DB_NAME     = os.getenv('DB_NAME', 'hydro_equity')
DB_USER     = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def get_connection():
    """Returns a live SQLAlchemy connection."""
    return engine.connect()

def test_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[DB] ✅ PostgreSQL connection successful.")
        return True
    except Exception as e:
        print(f"[DB] ❌ Connection failed: {e}")
        return False

if __name__ == '__main__':
    test_connection()