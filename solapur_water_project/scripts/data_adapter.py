import os
from typing import Optional

def get_data_mode() -> str:
    return os.environ.get("DATA_MODE", "simulation")

def validate_simulation_outputs() -> bool:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p1 = os.path.join(base_dir, "outputs", "pressure_fullcity_baseline.csv")
    p2 = os.path.join(base_dir, "outputs", "v4_zone_status.json")
    return os.path.exists(p1) and os.path.exists(p2)

def get_latest_ingest() -> Optional[dict]:
    if get_data_mode() != "live":
        return None
        
    try:
        from backend.database import engine
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT ingest_id, source, rows_ingested, pipeline_triggered, status, created_at 
                FROM data_ingest_log 
                WHERE status = 'success' 
                ORDER BY ingest_id DESC 
                LIMIT 1
            """))
            row = result.fetchone()
            if row:
                d = dict(zip(result.keys(), row))
                if 'created_at' in d and d['created_at'] is not None:
                    d['created_at'] = d['created_at'].isoformat()
                return d
            return None
    except Exception:
        return None
