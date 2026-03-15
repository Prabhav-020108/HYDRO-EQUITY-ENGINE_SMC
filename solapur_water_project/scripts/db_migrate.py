"""
Hydro-Equity Engine — Data Migration Script
Place in: scripts/db_migrate.py
Migrates V1/V4/V5/V6 CSV and JSON outputs into PostgreSQL.
Run after db_setup.py. Safe to re-run.

Usage: python scripts/db_migrate.py
"""

import sys, os, json, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import text
from backend.database import engine

BASE    = os.path.dirname(os.path.abspath(__file__))
DATA    = os.path.join(BASE, '..', 'Data')
OUTPUTS = os.path.join(BASE, '..', 'outputs')

def dp(f): return os.path.join(DATA, f)
def op(f): return os.path.join(OUTPUTS, f)


def migrate_pipe_segments():
    path = dp('pipe_segments.csv')
    if not os.path.exists(path):
        print("  [SKIP] pipe_segments.csv not found"); return
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append({
                'segment_id':        r.get('segment_id', ''),
                'pipeline_type':     r.get('pipeline_type', r.get('Pipeline T', '')),
                'material':          r.get('material', ''),
                'diameter_m':        float(r.get('diameter_m', r.get('Diameter(m)', 0)) or 0),
                'length_m':          float(r.get('length_m', r.get('Length(m)', 0)) or 0),
                'zone_id':           r.get('zone_id', r.get('Water Zone', '')),
                'start_node_id':     r.get('start_node_id', ''),
                'end_node_id':       r.get('end_node_id', ''),
                'hw_c_value':        float(r.get('hw_c_value', 120) or 120),
                'assumed_age_years': float(r.get('assumed_age_years', 25) or 25),
                'design_lifespan':   float(r.get('design_lifespan', 50) or 50),
                'data_quality_flag': r.get('data_quality_flag', 'ok'),
            })
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM pipe_segments"))
        for r in rows:
            conn.execute(text("""
                INSERT INTO pipe_segments VALUES (
                    :segment_id,:pipeline_type,:material,:diameter_m,:length_m,
                    :zone_id,:start_node_id,:end_node_id,:hw_c_value,
                    :assumed_age_years,:design_lifespan,:data_quality_flag
                ) ON CONFLICT (segment_id) DO NOTHING
            """), r)
        conn.commit()
    print(f"  [OK] pipe_segments — {len(rows)} rows")


def migrate_nodes():
    path = dp('nodes_with_elevation.csv')
    if not os.path.exists(path):
        print("  [SKIP] nodes_with_elevation.csv not found"); return
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        for i, r in enumerate(csv.DictReader(f)):
            rows.append({
                'node_id':     r.get('node_id', f'J{i}'),
                'lat':         float(r.get('lat', 0) or 0),
                'lon':         float(r.get('lon', 0) or 0),
                'elevation_m': float(r.get('elevation', r.get('elevation_m', 450)) or 450),
                'zone_id':     r.get('zone_id', r.get('zone', '')),
                'node_type':   r.get('node_type', 'junction'),
            })
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM nodes"))
        for r in rows:
            conn.execute(text("""
                INSERT INTO nodes VALUES (
                    :node_id,:lat,:lon,:elevation_m,:zone_id,:node_type
                ) ON CONFLICT (node_id) DO NOTHING
            """), r)
        conn.commit()
    print(f"  [OK] nodes — {len(rows)} rows")


def migrate_zone_demand():
    path = dp('zone_demand.csv')
    if not os.path.exists(path):
        print("  [SKIP] zone_demand.csv not found"); return
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append({
                'zone_id':          r.get('zone_id', ''),
                'base_lps':         float(r.get('base_lps', 0) or 0),
                'peak_morning_lps': float(r.get('peak_morning_lps', 0) or 0),
                'offpeak_lps':      float(r.get('offpeak_lps', 0) or 0),
                'population':       float(r.get('population', 0) or 0),
            })
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM zone_demand"))
        for r in rows:
            conn.execute(text("""
                INSERT INTO zone_demand VALUES (
                    :zone_id,:base_lps,:peak_morning_lps,:offpeak_lps,:population
                ) ON CONFLICT (zone_id) DO NOTHING
            """), r)
        conn.commit()
    print(f"  [OK] zone_demand — {len(rows)} rows")


def migrate_equity_scores():
    path = op('v4_zone_status.json')
    if not os.path.exists(path):
        print("  [SKIP] v4_zone_status.json not found"); return
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    zones = data if isinstance(data, list) else data.get('zones', [])
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM zone_equity_scores"))
        for z in zones:
            conn.execute(text("""
                INSERT INTO zone_equity_scores (zone_id, hei, zes, status, color)
                VALUES (:zone_id,:hei,:zes,:status,:color)
            """), {
                'zone_id': z.get('zone_id', z.get('id', '')),
                'hei':     float(z.get('hei', 0) or 0),
                'zes':     float(z.get('zes', z.get('hei', 0)) or 0),
                'status':  z.get('status', ''),
                'color':   z.get('color', '#888888'),
            })
        conn.commit()
    print(f"  [OK] zone_equity_scores — {len(zones)} rows")


def migrate_alerts():
    path = op('v5_alerts.json')
    if not os.path.exists(path):
        print("  [SKIP] v5_alerts.json not found"); return
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        all_alerts = data
    elif isinstance(data, dict):
        all_alerts = []
        for scen, items in data.items():
            if isinstance(items, list):
                for item in items:
                    item['scenario'] = scen
                    all_alerts.append(item)
    else:
        all_alerts = []
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM alerts"))
        for a in all_alerts:
            conn.execute(text("""
                INSERT INTO alerts
                    (zone_id, clps, severity, dominant_signal, probable_nodes, scenario, status)
                VALUES
                    (:zone_id,:clps,:severity,:dominant_signal,:probable_nodes,:scenario,'fired')
            """), {
                'zone_id':         a.get('zone_id', a.get('zone', '')),
                'clps':            float(a.get('clps', 0) or 0),
                'severity':        a.get('severity', 'moderate'),
                'dominant_signal': a.get('dominant_signal', a.get('dominant', '')),
                'probable_nodes':  json.dumps(a.get('probable_node_ids', [])),
                'scenario':        a.get('scenario', 'baseline'),
            })
        conn.commit()
    print(f"  [OK] alerts — {len(all_alerts)} rows")


def migrate_pipe_stress():
    path = op('v6_burst_top10.json')
    if not os.path.exists(path):
        print("  [SKIP] v6_burst_top10.json not found"); return
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    segments = data if isinstance(data, list) else data.get('segments', data.get('top10', []))
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM pipe_stress_scores"))
        for s in segments:
            conn.execute(text("""
                INSERT INTO pipe_stress_scores
                    (segment_id, pss, psi_n, cff_n, adf, risk_level,
                     dominant_factor, summary, lat_start, lon_start, lat_end, lon_end)
                VALUES
                    (:segment_id,:pss,:psi_n,:cff_n,:adf,:risk_level,
                     :dominant_factor,:summary,:lat_start,:lon_start,:lat_end,:lon_end)
            """), {
                'segment_id':     str(s.get('segment_id', '')),
                'pss':            float(s.get('pss', 0) or 0),
                'psi_n':          float(s.get('psi_n', 0) or 0),
                'cff_n':          float(s.get('cff_n', 0) or 0),
                'adf':            float(s.get('adf', 0) or 0),
                'risk_level':     s.get('risk_level', ''),
                'dominant_factor':s.get('dominant_factor', ''),
                'summary':        s.get('summary', s.get('description', '')),
                'lat_start':      float(s.get('start_lat', s.get('lat_start', s.get('lat', 0))) or 0),
                'lon_start':      float(s.get('start_lon', s.get('lon_start', s.get('lon', 0))) or 0),
                'lat_end':        float(s.get('end_lat', s.get('lat_end', 0)) or 0),
                'lon_end':        float(s.get('end_lon', s.get('lon_end', 0)) or 0),
            })
        conn.commit()
    print(f"  [OK] pipe_stress_scores — {len(segments)} rows")


if __name__ == '__main__':
    print("[db_migrate] Starting migration CSV/JSON → PostgreSQL...")
    migrate_pipe_segments()
    migrate_nodes()
    migrate_zone_demand()
    migrate_equity_scores()
    migrate_alerts()
    migrate_pipe_stress()
    print("[db_migrate] ✅ Migration complete.")