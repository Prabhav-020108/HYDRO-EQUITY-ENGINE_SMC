import os
import csv
import logging
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from sqlalchemy import text
from backend.auth import get_current_user
from backend.database import engine
from backend.routers.recommendations import rebuild_recommendations

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])

@router.post("/ingest")
async def ingest_data(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    role = current_user.get("role", "")
    if role not in ["engineer", "commissioner"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied. Admin role required.")
        
    try:
        content = await file.read()
        decoded = content.decode('utf-8').splitlines()
        
        reader = csv.reader(decoded)
        try:
            headers = next(reader)
        except StopIteration:
            raise HTTPException(status_code=422, detail="Empty file")
            
        data_row_count = sum(1 for _ in reader)
        
        if data_row_count not in (96,97):
            raise HTTPException(status_code=422, detail=f"Expected 96 data rows, found {data_row_count}")
            
        if not any(header.strip().startswith("J") for header in headers):
            raise HTTPException(status_code=422, detail="Missing required 'J' column")
            
        raw_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", "outputs", "pressure_fullcity_baseline.csv")
        out_path = os.path.normpath(raw_path)
        
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        
        with open(out_path, 'wb') as f:
            f.write(content)
            
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO data_ingest_log (source, rows_ingested, pipeline_triggered, status, created_at) 
                        VALUES (:source, :rows, :pt, :status, NOW())
                    """),
                    {
                        "source": file.filename,
                        "rows": data_row_count,
                        "pt": True,
                        "status": "success"
                    }
                )
                conn.commit()
        except Exception as db_e:
            logger.warning(f"Failed to log ingest to DB: {db_e}")
            
        rebuild_recommendations()
        
        return {"success": True, "rows_ingested": data_row_count, "message": "Pipeline rebuild triggered."}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reset-simulation-data")
async def reset_simulation_data(current_user: dict = Depends(get_current_user)):
    """
    Resets all simulation-derived data back to committed defaults.
    Re-imports from outputs/*.json and Data/*.csv files.
    KEEPS: users, citizens, zone_polygons.
    WIPES AND RELOADS: alerts, pipe_segments, nodes, zone_demand,
                       zone_equity_scores, pipe_stress_scores,
                       engineer_recs, ward_recs, commissioner_recs,
                       citizen_recs, citizen_complaints.
    Requires: engineer or commissioner role.
    """
    role = current_user.get("role", "")
    if role not in ("engineer", "commissioner"):
        raise HTTPException(
            status_code=403,
            detail="engineer or commissioner role required."
        )

    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

    results = {}

    try:
        from scripts.db_migrate import (
            migrate_pipe_segments, migrate_nodes, migrate_zone_demand,
            migrate_equity_scores, migrate_alerts, migrate_pipe_stress
        )

        # Also wipe recommendation and complaint tables for clean state
        from backend.database import engine
        from sqlalchemy import text

        with engine.connect() as conn:
            for table in ['engineer_recs', 'ward_recs', 'commissioner_recs',
                          'citizen_recs', 'citizen_complaints', 'v7_run_log',
                          'audit_log']:
                try:
                    conn.execute(text(f"DELETE FROM {table}"))
                except Exception:
                    pass
            conn.commit()

        migrate_pipe_segments()
        results['pipe_segments'] = 'reset'
        migrate_nodes()
        results['nodes'] = 'reset'
        migrate_zone_demand()
        results['zone_demand'] = 'reset'
        migrate_equity_scores()
        results['zone_equity_scores'] = 'reset'
        migrate_alerts()
        results['alerts'] = 'reset'
        migrate_pipe_stress()
        results['pipe_stress_scores'] = 'reset'

        # Re-run V7 recommendations
        try:
            from scripts.v7_recommendations import run_v7
            run_v7()
            results['recommendations'] = 'rebuilt'
        except Exception as e:
            results['recommendations'] = f'skipped: {e}'

        return {
            "success": True,
            "message": "All simulation data reset to defaults from committed outputs.",
            "tables_reset": results,
            "note": "Users, citizens, and zone_polygons were NOT affected."
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Reset failed: {e}"
        )
