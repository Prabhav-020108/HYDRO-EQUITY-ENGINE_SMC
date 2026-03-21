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
