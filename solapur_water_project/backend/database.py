"""
Hydro-Equity Engine — PostgreSQL Connection Module
Accepts both:
  - Render production: DATABASE_URL env var (single connection string)
  - Local dev: individual DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD vars
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Render provides DATABASE_URL; local dev uses individual vars
_database_url = os.getenv('DATABASE_URL')

if _database_url:
    # Render uses postgres:// but SQLAlchemy needs postgresql://
    if _database_url.startswith('postgres://'):
        _database_url = _database_url.replace('postgres://', 'postgresql://', 1)
    DATABASE_URL = _database_url
else:
    DB_HOST     = os.getenv('DB_HOST', 'localhost')
    DB_PORT     = os.getenv('DB_PORT', '5432')
    DB_NAME     = os.getenv('DB_NAME', 'hydro_equity')
    DB_USER     = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

def get_connection():
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
