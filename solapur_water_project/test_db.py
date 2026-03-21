import sys
sys.path.insert(0, '.')
from sqlalchemy import text
from backend.database import engine
with engine.connect() as conn:
    print('COUNT:', conn.execute(text('SELECT COUNT(*) FROM alerts;')).scalar())
    print('COUNT ACK:', conn.execute(text("SELECT COUNT(*) FROM alerts WHERE status='acknowledged';")).scalar())
    print('COUNT NEW:', conn.execute(text("SELECT COUNT(*) FROM alerts WHERE status='new';")).scalar())
    print('COUNT RESOLVED:', conn.execute(text("SELECT COUNT(*) FROM alerts WHERE status='resolved';")).scalar())
