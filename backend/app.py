# # """
# # Hydro-Equity Engine — Flask Backend
# # Solapur Municipal Corporation | SAMVED-2026 | Team Devsters
# # """

# # from flask import Flask, jsonify, request
# # from flask_cors import CORS
# # import json, os, math, csv, random

# # app = Flask(__name__)
# # CORS(app)

# # BASE = os.path.dirname(os.path.abspath(__file__))
# # DATA = os.path.join(BASE, '..', 'Data')

# # def dp(fn): return os.path.join(DATA, fn)

# # ESR_SOURCES = [
# #     {'lat':17.6935,'lon':75.8810,'head':72,'name':'Main WTP'},
# #     {'lat':17.7012,'lon':75.9280,'head':68,'name':'North ESR-2'},
# #     {'lat':17.6623,'lon':75.9034,'head':65,'name':'South ESR-3'},
# #     {'lat':17.6878,'lon':75.8654,'head':70,'name':'West ESR-4'},
# #     {'lat':17.6758,'lon':75.9412,'head':60,'name':'East ESR-5'},
# #     {'lat':17.7080,'lon':75.8950,'head':66,'name':'NW ESR-6'},
# #     {'lat':17.6700,'lon':75.8780,'head':63,'name':'SW ESR-7'},
# # ]

# # ZONES = [
# #     {'id':'z1','name':'Zone 1','lat':17.710,'lon':75.882},
# #     {'id':'z2','name':'Zone 2','lat':17.700,'lon':75.910},
# #     {'id':'z3','name':'Zone 3','lat':17.690,'lon':75.935},
# #     {'id':'z4','name':'Zone 4','lat':17.675,'lon':75.898},
# #     {'id':'z5','name':'Zone 5','lat':17.665,'lon':75.920},
# #     {'id':'z6','name':'Zone 6','lat':17.680,'lon':75.870},
# #     {'id':'z7','name':'Zone 7','lat':17.695,'lon':75.958},
# #     {'id':'z8','name':'Zone 8','lat':17.655,'lon':75.940},
# # ]

# # def hdist(la1,lo1,la2,lo2):
# #     dx=(lo1-lo2)*111*math.cos(la1*math.pi/180); dy=(la1-la2)*111
# #     return math.sqrt(dx*dx+dy*dy)

# # def dmult(h):
# #     if 6<=h<8: return 2.5
# #     if 17<=h<20: return 2.0
# #     if 8<=h<17: return 1.0
# #     return 0.05

# # def pAt(lat,lon,scen='baseline',hour=8):
# #     md,sh=1e9,65
# #     for s in ESR_SOURCES:
# #         d=hdist(lat,lon,s['lat'],s['lon'])
# #         if d<md: md=d; sh=s['head']
# #     fl=md*3.8; el=(lat-17.655)*22+(lon-75.960)*15
# #     dl=dmult(hour)*3
# #     ns=((abs(lat*1000)*7+abs(lon*100)*13)%100)/100.0*6-3
# #     p=sh-fl+el*0.4-dl+ns
# #     if scen=='leak':
# #         if lon>75.935: p*=0.52
# #         elif lon>75.920: p*=0.78
# #     elif scen=='valve':
# #         if lat<17.663: p*=0.22
# #         elif lat<17.672: p*=0.55
# #     elif scen=='surge':
# #         p*=(0.64 if lon>75.91 else 0.72)
# #     return round(max(2,min(82,p)),2)

# # def pcol(p):
# #     if p<10: return '#D32F2F'
# #     if p<20: return '#E65100'
# #     if p<30: return '#F9A825'
# #     if p<40: return '#8BC34A'
# #     if p<60: return '#2E7D32'
# #     return '#0D5FA8'

# # def hei_status(h):
# #     if h>=0.85: return 'equitable'
# #     if h>=0.70: return 'moderate'
# #     if h>1.30:  return 'over'
# #     return 'severe'

# # @app.route('/')
# # def index(): return jsonify({'status':'Hydro-Equity Engine · SMC','version':'2.0'})

# # @app.route('/pipeline')
# # def pipeline():
# #     scen=request.args.get('scenario','baseline')
# #     hour=int(request.args.get('hour',8))
# #     path=dp('pipeline.geojson')
# #     if not os.path.exists(path):
# #         return jsonify({'error':f'pipeline.geojson not found at {path}'}),404
# #     with open(path,'r',encoding='utf-8') as f:
# #         geo=json.load(f)
# #     enriched=[]
# #     for feat in geo.get('features',[]):
# #         geom=feat.get('geometry',{})
# #         cs=geom.get('coordinates',[])
# #         if geom.get('type')=='MultiLineString': cs=cs[0] if cs else []
# #         if not cs or len(cs)<2: enriched.append(feat); continue
# #         mid=cs[len(cs)//2]
# #         p=pAt(mid[1],mid[0],scen,hour)
# #         feat['properties']=feat.get('properties') or {}
# #         feat['properties']['sim_pressure']=p
# #         feat['properties']['sim_color']=pcol(p)
# #         enriched.append(feat)
# #     geo['features']=enriched
# #     return jsonify(geo)

# # @app.route('/infrastructure')
# # def infrastructure():
# #     items=[]
# #     path=dp('infrastructure_points.csv')
# #     if os.path.exists(path):
# #         with open(path,newline='',encoding='utf-8') as f:
# #             for row in csv.DictReader(f):
# #                 try:
# #                     items.append({'lat':float(row.get('lat',row.get('Lat',0))),'lon':float(row.get('lon',row.get('Lon',0))),'type':row.get('type',row.get('Type','ESR')),'name':row.get('name',row.get('Name','')),'zone':row.get('zone','')})
# #                 except: pass
# #     if not items:
# #         items=[{'lat':e['lat'],'lon':e['lon'],'type':'ESR','name':e['name'],'zone':''} for e in ESR_SOURCES]
# #     return jsonify(items)

# # @app.route('/tanks')
# # def tanks():
# #     path=dp('storage_tank.geojson')
# #     if os.path.exists(path):
# #         with open(path,'r',encoding='utf-8') as f: return jsonify(json.load(f))
# #     return jsonify({'type':'FeatureCollection','features':[]})

# # @app.route('/sources')
# # def sources():
# #     path=dp('water_source.geojson')
# #     if os.path.exists(path):
# #         with open(path,'r',encoding='utf-8') as f: return jsonify(json.load(f))
# #     return jsonify({'type':'FeatureCollection','features':[]})

# # @app.route('/pressure')
# # def pressure():
# #     scen=request.args.get('scenario','baseline')
# #     hour=int(request.args.get('hour',8))
# #     path=dp('nodes_with_elevation.csv')
# #     nodes=[]
# #     if os.path.exists(path):
# #         with open(path,newline='',encoding='utf-8') as f:
# #             for i,row in enumerate(csv.DictReader(f)):
# #                 if i%5!=0: continue
# #                 try:
# #                     lat,lon=float(row['lat']),float(row['lon'])
# #                     p=pAt(lat,lon,scen,hour)
# #                     nodes.append({'lat':lat,'lon':lon,'pressure':p,'color':pcol(p)})
# #                 except: pass
# #     return jsonify(nodes)

# # @app.route('/equity')
# # def equity():
# #     scen=request.args.get('scenario','baseline')
# #     hour=int(request.args.get('hour',8))
# #     result=[]
# #     for z in ZONES:
# #         pc=pAt(z['lat'],z['lon'],scen,hour)
# #         pt=pAt(z['lat']+.009,z['lon']+.007,scen,hour)*.87
# #         hei=round(min(1.55,pt/pc),3) if pc>0 else 0.0
# #         result.append({'id':z['id'],'name':z['name'],'lat':z['lat'],'lon':z['lon'],'pressure_avg':round(pc,1),'pressure_tailend':round(pt,1),'hei':hei,'status':hei_status(hei)})
# #     cwei=round(sum(r['hei'] for r in result)/len(result),3)
# #     return jsonify({'zones':result,'cwei':cwei,'scenario':scen,'hour':hour})

# # @app.route('/simulate')
# # def simulate():
# #     scen=request.args.get('scenario','baseline')
# #     zone_id=request.args.get('zone','z5')
# #     z=next((x for x in ZONES if x['id']==zone_id),ZONES[4])
# #     tl=[{'hour':h,'pressure':pAt(z['lat'],z['lon'],scen,h)} for h in range(24)]
# #     return jsonify({'zone':zone_id,'timeline':tl,'scenario':scen})

# # @app.route('/alerts')
# # def alerts():
# #     scen=request.args.get('scenario','baseline')
# #     data={
# #         'baseline':[{'level':'moderate','title':'Night Flow Anomaly','body':'Elevated night flow in Zone 7 — possible unauthorized use. NFA=1.38','zone':'Zone 7','clps':0.61,'dominant':'NFA'}],
# #         'leak':[
# #             {'level':'high','title':'Pipe Leak Detected','body':'Sudden pressure decay + flow imbalance in eastern zone.','zone':'Zone 5','clps':0.81,'dominant':'PDR+FPI'},
# #             {'level':'high','title':'Flow-Pressure Imbalance','body':'23% unaccounted flow near (17.682, 75.942).','zone':'Zone 3','clps':0.77,'dominant':'FPI'},
# #             {'level':'moderate','title':'Low Pressure Warning','body':'Tail-end nodes below minimum service pressure.','zone':'Zone 7','clps':0.64,'dominant':'PDR'},
# #         ],
# #         'valve':[
# #             {'level':'high','title':'Zone Isolation Alert','body':'South zone supply cut. Pressure <5m at downstream nodes.','zone':'Zone 8','clps':0.88,'dominant':'FPI'},
# #             {'level':'moderate','title':'Demand Redistribution','body':'Adjacent zones showing elevated demand.','zone':'Zone 4','clps':0.58,'dominant':'DDI'},
# #         ],
# #         'surge':[
# #             {'level':'high','title':'System-wide Pressure Drop','body':'Demand surge (1.5×) causing widespread low pressure.','zone':'Zone 6','clps':0.79,'dominant':'DDI+PDR'},
# #             {'level':'moderate','title':'Tail-End Deficit','body':'High-elevation zones receiving <12m pressure.','zone':'Zone 1','clps':0.65,'dominant':'PDR'},
# #         ]
# #     }
# #     al=data.get(scen,[])
# #     return jsonify({'alerts':al,'count':len(al)})

# # @app.route('/recommendations')
# # def recommendations():
# #     scen=request.args.get('scenario','baseline')
# #     data={
# #         'baseline':[
# #             {'zone':'Zone 7 — ROUTINE','action':'Increase ESR outlet pressure by 10–15%. HEI=0.61 — tail-end deficit.','impact':'HEI: 0.61 → 0.82'},
# #             {'zone':'Zone 3 — MAINTENANCE','action':'Pipe J-3814 HIGH burst risk (PSS: 0.84). Cast Iron, ~35yr. Schedule urgent inspection.','impact':'NRW reduction 2-4%'},
# #             {'zone':'Zone 5 — OPTIMIZATION','action':'Throttle Zone 5 inlet valve 15% — over-pressurized (HEI=1.34). Redirect to Zone 7.','impact':'City CWEI +0.07'},
# #         ],
# #         'leak':[
# #             {'zone':'Zone 3 · Zone 5 — URGENT','action':'Dispatch team to (17.682, 75.942). Isolate pipe east of ESR-5.','impact':'CLPS: 0.81 → 0.28'},
# #             {'zone':'Zone 7 — MODERATE','action':'Inspect for unauthorized extraction 01:00–04:00.','impact':'NRW reduction 4-6%'},
# #         ],
# #         'valve':[
# #             {'zone':'Zone 8 — CRITICAL','action':'Reopen south distribution valve. Activate tanker for Ward 14 & 15.','impact':'HEI: 0.22 → 0.85'},
# #             {'zone':'All Zones','action':'Redistribute demand to Zone 4 cross-connection.','impact':'Stabilizes in 2 supply cycles'},
# #         ],
# #         'surge':[
# #             {'zone':'Zone 1 · Zone 6 — HIGH','action':'Increase ESR pressure 12–15%. Throttle Zone 3 inlet 18%.','impact':'HEI: 0.64 → 0.82'},
# #             {'zone':'System-wide','action':'Stagger supply: Zone 1–4 at 06:00, Zone 5–8 at 07:30.','impact':'Avg pressure +8m, NRW -3%'},
# #         ]
# #     }
# #     return jsonify({'recommendations':data.get(scen,[])})

# # @app.route('/burst-risk')
# # def burst_risk():
# #     random.seed(42)
# #     segments=[]
# #     for i in range(10):
# #         lat=17.655+random.random()*.06; lon=75.875+random.random()*.08
# #         mat=random.choice(['CI','CI','DI','PVC'])
# #         age=35 if mat=='CI' else(15 if mat=='DI' else 10)
# #         psi_n=round(random.uniform(.1,.8),2); cff_n=round(random.uniform(.2,.9),2)
# #         adf=round(min(1.0,age/(50 if mat=='CI' else 60 if mat=='DI' else 25)),2)
# #         pss=round(.40*psi_n+.35*cff_n+.25*adf,3)
# #         dom=max(zip([psi_n,cff_n,adf],['Pressure Surge','Cycle Fatigue','Age Degradation']))[1]
# #         segments.append({'rank':i+1,'segment_id':f'P-{3000+i*127}','material':mat,'assumed_age':age,'lat':round(lat,4),'lon':round(lon,4),'psi_n':psi_n,'cff_n':cff_n,'adf':adf,'pss':pss,'risk_level':'HIGH' if pss>.80 else 'MODERATE' if pss>.55 else 'LOW','dominant_factor':dom,'summary':f'{mat}, ~{age}yr, PSS: {pss}'})
# #     segments.sort(key=lambda x:x['pss'],reverse=True)
# #     for i,s in enumerate(segments): s['rank']=i+1
# #     return jsonify({'segments':segments[:10]})

# # @app.route('/zone-demand')
# # def zone_demand():
# #     path=dp('zone_demand.csv')
# #     if os.path.exists(path):
# #         with open(path,newline='',encoding='utf-8') as f: return jsonify(list(csv.DictReader(f)))
# #     return jsonify([])

# # if __name__=='__main__':
# #     print('='*50)
# #     print('Hydro-Equity Engine Backend · SMC')
# #     print(f'Data: {DATA}')
# #     for ep in ['/pipeline','/infrastructure','/tanks','/sources','/pressure','/equity','/simulate','/alerts','/recommendations','/burst-risk']:
# #         print(f'  GET http://localhost:5000{ep}')
# #     print('='*50)
# #     print('Install deps: pip install flask flask-cors')
# #     app.run(debug=True,port=5000)


# """
# app.py  ·  place in: backend/
# Hydro-Equity Engine — Flask Backend
# Solapur Municipal Corporation | SAMVED-2026 | Team Devsters

# HOW REAL DATA WORKS:
#   When you run simulation_engine.py it writes:
#     outputs/pressure_baseline.csv
#     outputs/flow_baseline.csv
#     outputs/pressure_scenario_A_leak.csv
#     outputs/pressure_scenario_B_valve_close.csv
#     outputs/pressure_scenario_C_demand_surge.csv

#   This backend AUTOMATICALLY uses those real CSV files if they exist.
#   If they don't exist yet, it falls back to the JS formula approximation.
#   You don't need to change anything — just run simulation_engine.py first.
# """

# from flask import Flask, jsonify, request
# from flask_cors import CORS
# import json, os, math, csv, random, time, threading
# import pandas as pd
# import sys
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
# from backend.database import engine
# from sqlalchemy import text

# app = Flask(__name__)
# CORS(app)

# # ── Simulation Clock ────────────────────────────────────────────
# current_timestep = 0

# def run_simulation_clock():
#     global current_timestep
#     while True:
#         time.sleep(60)
#         current_timestep = (current_timestep + 1) % 96

# threading.Thread(target=run_simulation_clock, daemon=True).start()

# @app.route('/health')
# def health():
#     return jsonify({"status": "ok"})

# # ── File paths ──────────────────────────────────────────────────
# BASE    = os.path.dirname(os.path.abspath(__file__))
# ROOT    = os.path.dirname(BASE)
# DATA    = os.path.join(ROOT, "Data")
# OUTPUTS = os.path.join(ROOT, "outputs")

# def dp(fn):  return os.path.join(DATA,    fn)
# def op(fn):  return os.path.join(OUTPUTS, fn)

# # ── Load real simulation CSVs at startup ────────────────────────
# # These are produced by: python scripts/simulation_engine.py
# # If not found, backend falls back to formula approximation.

# SCENARIO_CSV_MAP = {
#     "baseline": "pressure_fullcity_baseline.csv",
#     "leak":     "pressure_fullcity_scenario_A_leak.csv",
#     "valve":    "pressure_fullcity_scenario_B_valve_close.csv",
#     "surge":    "pressure_fullcity_scenario_C_demand_surge.csv",
# }

# # Load nodes CSV to get lat/lon for each node column name
# _nodes_df   = None
# _real_data  = {}   # dict: scenario → pressure_df (rows=timesteps, cols=node names)

# def _load_real_data():
#     """
#     Called once at startup. Loads pressure CSVs and nodes CSV.
#     Silently skips if files don't exist yet.
#     """
#     global _nodes_df, _real_data

#     nodes_path = dp("nodes_with_elevation.csv")
#     if os.path.exists(nodes_path):
#         _nodes_df = pd.read_csv(nodes_path)
#         _nodes_df["lon"] = _nodes_df["lon"].round(6)
#         _nodes_df["lat"] = _nodes_df["lat"].round(6)
#         print(f"[app] Loaded nodes: {len(_nodes_df)} rows")
#     else:
#         print("[app] ⚠  nodes_with_elevation.csv not found — using formula fallback")

#     for scen, filename in SCENARIO_CSV_MAP.items():
#         path = op(filename)
#         if os.path.exists(path):
#             try:
#                 df = pd.read_csv(path, index_col=0)
#                 _real_data[scen] = df
#                 print(f"[app] ✓  Real data loaded: {filename} ({len(df)} timesteps × {len(df.columns)} nodes)")
#             except Exception as e:
#                 print(f"[app] ⚠  Failed to load {filename}: {e}")

#     if _real_data:
#         print(f"[app] ★  REAL WNTR DATA ACTIVE for: {list(_real_data.keys())}")
#     else:
#         print("[app] ℹ  No real data found — using formula approximation")
#         print("[app]    Run: python scripts/simulation_engine.py   to generate real data")

# _load_real_data()


# # ─────────────────────────────────────────────────────────────
# # REAL DATA HELPERS
# # ─────────────────────────────────────────────────────────────
# def _hour_to_timestep(hour):
#     """Convert hour (0-23) to the closest 15-min timestep index (0-95)."""
#     return min(int(hour * 4), 95)

# def _get_real_pressure(lat, lon, scenario, hour):
#     """
#     Look up the real WNTR pressure for the node closest to (lat, lon)
#     at the given hour. Returns None if real data is not available.
#     """
#     if scenario not in _real_data or _nodes_df is None:
#         return None

#     pressure_df = _real_data[scenario]
#     t = _hour_to_timestep(hour)

#     # Find which node names exist in both nodes_df and pressure_df columns
#     # Node columns in CSV are named J0, J1, ... matching nodes_df row index
#     row_at_t = pressure_df.iloc[t]

#     # Find closest node to (lat, lon)
#     dists = ((_nodes_df["lat"] - lat)**2 + (_nodes_df["lon"] - lon)**2)
#     closest_idx = dists.idxmin()
#     node_name   = f"J{closest_idx}"

#     if node_name in row_at_t.index:
#         return float(row_at_t[node_name])

#     # Fallback: try a few neighbours
#     for offset in [1, -1, 2, -2, 5]:
#         alt = f"J{closest_idx + offset}"
#         if alt in row_at_t.index:
#             return float(row_at_t[alt])

#     return None   # column not found — fall back to formula


# # ─────────────────────────────────────────────────────────────
# # FORMULA FALLBACK (used when real data not available)
# # ─────────────────────────────────────────────────────────────
# ESR_SOURCES = [
#     {'lat': 17.6935, 'lon': 75.8810, 'head': 72, 'name': 'Main WTP'},
#     {'lat': 17.7012, 'lon': 75.9280, 'head': 68, 'name': 'North ESR-2'},
#     {'lat': 17.6623, 'lon': 75.9034, 'head': 65, 'name': 'South ESR-3'},
#     {'lat': 17.6878, 'lon': 75.8654, 'head': 70, 'name': 'West ESR-4'},
#     {'lat': 17.6758, 'lon': 75.9412, 'head': 60, 'name': 'East ESR-5'},
#     {'lat': 17.7080, 'lon': 75.8950, 'head': 66, 'name': 'NW ESR-6'},
#     {'lat': 17.6700, 'lon': 75.8780, 'head': 63, 'name': 'SW ESR-7'},
# ]

# ZONES = [
#     {'id': 'z1', 'name': 'Zone 1', 'lat': 17.710, 'lon': 75.882},
#     {'id': 'z2', 'name': 'Zone 2', 'lat': 17.700, 'lon': 75.910},
#     {'id': 'z3', 'name': 'Zone 3', 'lat': 17.690, 'lon': 75.935},
#     {'id': 'z4', 'name': 'Zone 4', 'lat': 17.675, 'lon': 75.898},
#     {'id': 'z5', 'name': 'Zone 5', 'lat': 17.665, 'lon': 75.920},
#     {'id': 'z6', 'name': 'Zone 6', 'lat': 17.680, 'lon': 75.870},
#     {'id': 'z7', 'name': 'Zone 7', 'lat': 17.695, 'lon': 75.958},
#     {'id': 'z8', 'name': 'Zone 8', 'lat': 17.655, 'lon': 75.940},
# ]

# def _hdist(la1, lo1, la2, lo2):
#     dx = (lo1 - lo2) * 111 * math.cos(la1 * math.pi / 180)
#     dy = (la1 - la2) * 111
#     return math.sqrt(dx * dx + dy * dy)

# def _dmult(h):
#     if   6  <= h < 8:  return 2.5
#     if   17 <= h < 20: return 2.0
#     if   8  <= h < 17: return 1.0
#     return 0.05

# def _formula_pressure(lat, lon, scenario='baseline', hour=8):
#     """Formula approximation — used when real WNTR data not available."""
#     md, sh = 1e9, 65
#     for s in ESR_SOURCES:
#         d = _hdist(lat, lon, s['lat'], s['lon'])
#         if d < md:
#             md = d
#             sh = s['head']
#     fl = md * 3.8
#     el = (lat - 17.655) * 22 + (lon - 75.960) * 15
#     dl = _dmult(hour) * 3
#     ns = ((abs(lat * 1000) * 7 + abs(lon * 100) * 13) % 100) / 100.0 * 6 - 3
#     p  = sh - fl + el * 0.4 - dl + ns
#     if scenario == 'leak':
#         if lon > 75.935: p *= 0.52
#         elif lon > 75.920: p *= 0.78
#     elif scenario == 'valve':
#         if lat < 17.663: p *= 0.22
#         elif lat < 17.672: p *= 0.55
#     elif scenario == 'surge':
#         p *= (0.64 if lon > 75.91 else 0.72)
#     return round(max(2, min(82, p)), 2)


# def get_pressure(lat, lon, scenario='baseline', hour=8):
#     """
#     Main pressure function. Uses real WNTR data if available, formula otherwise.
#     """
#     real = _get_real_pressure(lat, lon, scenario, hour)
#     if real is not None:
#         # Clamp to realistic range
#         return round(max(0, min(100, real)), 2)
#     return _formula_pressure(lat, lon, scenario, hour)


# def pcol(p):
#     if p < 10: return '#D32F2F'
#     if p < 20: return '#E65100'
#     if p < 30: return '#F9A825'
#     if p < 40: return '#8BC34A'
#     if p < 60: return '#2E7D32'
#     return '#0D5FA8'

# def hei_status(h):
#     if h > 1.30: return 'over'
#     if h >= 0.85: return 'equitable'
#     if h >= 0.70: return 'moderate'
#     return 'severe'


# # ─────────────────────────────────────────────────────────────
# # ROUTES
# # ─────────────────────────────────────────────────────────────

# @app.route('/')
# def index():
#     return jsonify({
#         'status':    'Hydro-Equity Engine · SMC',
#         'version':   '3.0',
#         'real_data': list(_real_data.keys()),
#         'data_mode': 'REAL_WNTR' if _real_data else 'FORMULA_APPROX'
#     })


# @app.route('/pipeline')
# def pipeline():
#     """
#     Returns pipeline GeoJSON.
#     Phase 3.5: Added zone_id filter and batch response for performance.
#     ?zone=zone_5  → returns only pipes in that zone
#     ?zone=all     → returns all pipes (default, may be slow for large cities)
#     """
#     scen    = request.args.get('scenario', 'baseline')
#     hour    = int(request.args.get('hour', 8))
#     zone_filter = request.args.get('zone', 'all')

#     path = dp('pipeline.geojson')
#     if not os.path.exists(path):
#         return jsonify({'error': f'pipeline.geojson not found at {path}'}), 404

#     with open(path, 'r', encoding='utf-8') as f:
#         geo = json.load(f)

#     enriched = []
#     for feat in geo.get('features', []):
#         props = feat.get('properties') or {}

#         # ── Zone filter (Phase 3.5 performance fix) ──────────
#         if zone_filter != 'all':
#             feat_zone = str(props.get('Water Zone', props.get('zone_id', ''))).strip()
#             # normalise both sides: "Zone 5", "zone_5", "5" → "5"
#             def norm(z): return z.lower().replace('zone','').replace('_','').strip()
#             if norm(feat_zone) != norm(zone_filter):
#                 continue
#         # ─────────────────────────────────────────────────────

#         geom = feat.get('geometry', {})
#         cs   = geom.get('coordinates', [])
#         if geom.get('type') == 'MultiLineString':
#             cs = cs[0] if cs else []
#         if not cs or len(cs) < 2:
#             enriched.append(feat)
#             continue

#         mid = cs[len(cs) // 2]
#         p   = get_pressure(mid[1], mid[0], scen, hour)
#         feat['properties']['sim_pressure'] = p
#         feat['properties']['sim_color']    = pcol(p)
#         enriched.append(feat)

#     geo['features'] = enriched
#     return jsonify(geo)


# @app.route('/infrastructure')
# def infrastructure():
#     items = []
#     path  = dp('infrastructure_points.csv')
#     if os.path.exists(path):
#         with open(path, newline='', encoding='utf-8') as f:
#             for row in csv.DictReader(f):
#                 try:
#                     items.append({
#                         'lat':  float(row.get('lat', row.get('Lat', 0))),
#                         'lon':  float(row.get('lon', row.get('Lon', 0))),
#                         'type': row.get('feature_type', row.get('type', 'ESR')),
#                         'name': row.get('name', row.get('Name', '')),
#                         'zone': row.get('zone', ''),
#                     })
#                 except:
#                     pass
#     if not items:
#         items = [{'lat': e['lat'], 'lon': e['lon'], 'type': 'ESR', 'name': e['name'], 'zone': ''} for e in ESR_SOURCES]
#     return jsonify(items)


# @app.route('/tanks')
# def tanks():
#     path = dp('storage_tank.geojson')
#     if os.path.exists(path):
#         with open(path, 'r', encoding='utf-8') as f:
#             return jsonify(json.load(f))
#     return jsonify({'type': 'FeatureCollection', 'features': []})


# @app.route('/sources')
# def sources():
#     path = dp('water_source.geojson')
#     if os.path.exists(path):
#         with open(path, 'r', encoding='utf-8') as f:
#             return jsonify(json.load(f))
#     return jsonify({'type': 'FeatureCollection', 'features': []})


# @app.route('/pressure')
# def pressure():
#     """Returns sampled node pressures — used for node pressure overlay on map."""
#     scen  = request.args.get('scenario', 'baseline')
#     hour  = int(request.args.get('hour', 8))
#     nodes = []

#     # If real data available: sample every 5th node from nodes_with_elevation.csv
#     if _nodes_df is not None and scen in _real_data:
#         t = _hour_to_timestep(hour)
#         row_at_t = _real_data[scen].iloc[t]
#         for idx in range(0, len(_nodes_df), 5):   # every 5th node to keep response small
#             node_name = f"J{idx}"
#             if node_name in row_at_t.index:
#                 p = float(row_at_t[node_name])
#                 p = max(0, min(100, p))
#                 nodes.append({
#                     'lat':      float(_nodes_df.loc[idx, 'lat']),
#                     'lon':      float(_nodes_df.loc[idx, 'lon']),
#                     'pressure': round(p, 2),
#                     'color':    pcol(p)
#                 })
#     else:
#         # Fallback: formula on nodes CSV
#         path = dp('nodes_with_elevation.csv')
#         if os.path.exists(path):
#             with open(path, newline='', encoding='utf-8') as f:
#                 for i, row in enumerate(csv.DictReader(f)):
#                     if i % 5 != 0:
#                         continue
#                     try:
#                         lat = float(row['lat'])
#                         lon = float(row['lon'])
#                         p   = get_pressure(lat, lon, scen, hour)
#                         nodes.append({'lat': lat, 'lon': lon, 'pressure': p, 'color': pcol(p)})
#                     except:
#                         pass

#     return jsonify(nodes)


# @app.route('/equity')
# def equity():
#     """Returns zone equity data from V4 output."""
#     v4_path = op('v4_equity_minimal.json')
#     if os.path.exists(v4_path):
#         with open(v4_path, 'r', encoding='utf-8') as f:
#             try:
#                 data = json.load(f)
#                 # v4_equity_minimal.json has: {"zones": [...], "cwei_daily": X, ...}
#                 zones = data.get('zones', [])
#                 cwei  = data.get('cwei_daily', 0)
#                 return jsonify({
#                     'zones':       zones,
#                     'cwei':        cwei,
#                     'scenario':    'baseline',
#                     'data_source': 'v4_engine'
#                 })
#             except Exception as e:
#                 print(f"[app] Error parsing V4 JSON: {e}")
#     return jsonify({'zones': [], 'cwei': 0.0, 'error': 'Run V4 first'})


# @app.route('/zones')
# def zones_endpoint():
#     path = op('v4_zone_status.json')
#     if os.path.exists(path):
#         with open(path, 'r', encoding='utf-8') as f:
#             try:
#                 data = json.load(f)
#                 # Frontend expects a plain list
#                 if isinstance(data, list):
#                     return jsonify(data)
#                 return jsonify(data)
#             except Exception as e:
#                 print(f"[app] Error parsing v4_zone_status JSON: {e}")
#     return jsonify({'error': 'Run V4 first'})


# @app.route('/simulate')
# def simulate():
#     """
#     Returns 24-hour pressure timeline for a zone.
#     Uses real WNTR data when available (much more realistic curve shape).
#     """
#     scen    = request.args.get('scenario', 'baseline')
#     zone_id = request.args.get('zone', 'z5')
#     z       = next((x for x in ZONES if x['id'] == zone_id), ZONES[4])

#     if scen in _real_data and _nodes_df is not None:
#         # Find the closest node to zone center
#         pressure_df = _real_data[scen]
#         dists = ((_nodes_df["lat"] - z['lat'])**2 + (_nodes_df["lon"] - z['lon'])**2)
#         closest_idx  = dists.idxmin()
#         node_name    = f"J{closest_idx}"

#         tl = []
#         for h in range(24):
#             t = _hour_to_timestep(h)
#             if node_name in pressure_df.columns:
#                 p = float(pressure_df.iloc[t][node_name])
#                 p = round(max(0, min(100, p)), 2)
#             else:
#                 p = get_pressure(z['lat'], z['lon'], scen, h)
#             tl.append({'hour': h, 'pressure': p})
#     else:
#         tl = [{'hour': h, 'pressure': get_pressure(z['lat'], z['lon'], scen, h)} for h in range(24)]

#     return jsonify({
#         'zone':        zone_id,
#         'timeline':    tl,
#         'scenario':    scen,
#         'data_source': 'wntr' if scen in _real_data else 'formula',
#     })


# @app.route('/data-status')
# def data_status():
#     """Shows what data is loaded — useful for debugging."""
#     files = {}
#     for scen, filename in SCENARIO_CSV_MAP.items():
#         path = op(filename)
#         files[filename] = {
#             'exists': os.path.exists(path),
#             'loaded': scen in _real_data,
#             'rows':   len(_real_data[scen]) if scen in _real_data else 0,
#             'cols':   len(_real_data[scen].columns) if scen in _real_data else 0,
#         }
#     return jsonify({
#         'real_data_scenarios': list(_real_data.keys()),
#         'nodes_loaded':        _nodes_df is not None,
#         'node_count':          len(_nodes_df) if _nodes_df is not None else 0,
#         'files':               files,
#         'data_mode':           'REAL_WNTR' if _real_data else 'FORMULA_APPROX',
#     })


# @app.route('/alerts')
# def alerts():
#     """Reads the actual V5 analytics output from outputs/v5_alerts.json."""
#     scen = request.args.get('scenario', 'baseline')
#     v5_path = op('v5_alerts.json')

#     if os.path.exists(v5_path):
#         with open(v5_path, 'r', encoding='utf-8') as f:
#             try:
#                 all_alerts = json.load(f)
#                 al = all_alerts.get(scen, [])
#                 return jsonify({'alerts': al, 'count': len(al), 'data_source': 'v5_engine'})
#             except Exception as e:
#                 print(f"[app] Error parsing V5 JSON: {e}")

#     # Fallback if V5 engine hasn't been run or file is missing
#     return jsonify({'alerts': [], 'count': 0, 'data_source': 'none', 'error': 'Run V5 first'})

# @app.route('/alerts/active')
# def alerts_active():
#     """Returns top alerts for the requested scenario from V5 output."""
#     scen = request.args.get('scenario', 'baseline')
#     v5_path = op('v5_alerts.json')
#     if os.path.exists(v5_path):
#         with open(v5_path, 'r', encoding='utf-8') as f:
#             try:
#                 data = json.load(f)
#             except Exception as e:
#                 return jsonify({'alerts': [], 'count': 0, 'error': str(e)})

#         # Handle both old flat list and new dict-by-scenario format
#         if isinstance(data, dict):
#             al = data.get(scen, data.get('baseline', []))
#         elif isinstance(data, list):
#             al = data
#         else:
#             al = []

#         # Deduplicate: keep highest CLPS per zone
#         seen = {}
#         for a in al:
#             z = a.get('zone_id', '')
#             if z not in seen or a.get('clps', 0) > seen[z].get('clps', 0):
#                 seen[z] = a

#         top = sorted(seen.values(), key=lambda x: x.get('clps', 0), reverse=True)[:10]

#         # Normalize field names for frontend
#         for a in top:
#             raw_zone = a.get('zone_id', '')
#             a['zone']  = raw_zone.replace('zone_', 'Zone ')
#             a['level'] = 'high' if a.get('clps', 0) > 0.6 else 'moderate'
#             a['title'] = f"{a.get('dominant_signal', 'Alert')} Detected"
#             a['body']  = (f"Zone {raw_zone.replace('zone_','')} under stress · "
#                          f"Signal: {a.get('dominant_signal','')} · "
#                          f"Nodes: {', '.join(str(n) for n in a.get('probable_node_ids',[])[:2])}")

#         return jsonify({'alerts': top, 'count': len(top), 'data_source': 'v5_engine'})

#     return jsonify({'alerts': [], 'count': 0, 'error': 'Run V5 first'})


# @app.route('/recommendations')
# def recommendations():
#     scen = request.args.get('scenario', 'baseline')
#     data = {
#         'baseline': [
#             {'zone': 'Zone 7 — ROUTINE',      'action': 'Increase ESR outlet pressure by 10–15%. HEI=0.61 — tail-end deficit.',           'impact': 'HEI: 0.61 → 0.82'},
#             {'zone': 'Zone 3 — MAINTENANCE',  'action': 'Pipe J-3814 HIGH burst risk (PSS: 0.84). Cast Iron, ~35yr. Schedule urgent inspection.', 'impact': 'NRW reduction 2-4%'},
#             {'zone': 'Zone 5 — OPTIMIZATION', 'action': 'Throttle Zone 5 inlet valve 15% — over-pressurized (HEI=1.34). Redirect to Zone 7.', 'impact': 'City CWEI +0.07'},
#         ],
#         'leak': [
#             {'zone': 'Zone 3 · Zone 5 — URGENT', 'action': 'Dispatch team to (17.682, 75.942). Isolate pipe east of ESR-5.',              'impact': 'CLPS: 0.81 → 0.28'},
#             {'zone': 'Zone 7 — MODERATE',         'action': 'Inspect for unauthorized extraction 01:00–04:00.',                            'impact': 'NRW reduction 4-6%'},
#         ],
#         'valve': [
#             {'zone': 'Zone 8 — CRITICAL',  'action': 'Reopen south distribution valve. Activate tanker for Ward 14 & 15.',                'impact': 'HEI: 0.22 → 0.85'},
#             {'zone': 'All Zones',           'action': 'Redistribute demand to Zone 4 cross-connection.',                                   'impact': 'Stabilizes in 2 supply cycles'},
#         ],
#         'surge': [
#             {'zone': 'Zone 1 · Zone 6 — HIGH', 'action': 'Increase ESR pressure 12–15%. Throttle Zone 3 inlet 18%.',                     'impact': 'HEI: 0.64 → 0.82'},
#             {'zone': 'System-wide',             'action': 'Stagger supply: Zone 1–4 at 06:00, Zone 5–8 at 07:30.',                         'impact': 'Avg pressure +8m, NRW -3%'},
#         ]
#     }
#     return jsonify({'recommendations': data.get(scen, [])})


# @app.route('/burst-risk')
# def burst_risk():
#     """Reads the actual V6 analytics output from outputs/v6_burst.json."""
#     v6_path = op('v6_burst.json')

#     if os.path.exists(v6_path):
#         with open(v6_path, 'r', encoding='utf-8') as f:
#             try:
#                 data = json.load(f)
#                 data['data_source'] = 'v6_engine'
#                 return jsonify(data)
#             except Exception as e:
#                 print(f"[app] Error parsing V6 JSON: {e}")

#     # Fallback if V6 engine hasn't been run or file is missing
#     return jsonify({'segments': [], 'data_source': 'none', 'error': 'Run V6 first'})

# @app.route('/burst-risk/top10')
# def burst_risk_top10():
#     v6_path = op('v6_burst_top10.json')
#     if os.path.exists(v6_path):
#         with open(v6_path, 'r', encoding='utf-8') as f:
#             try:
#                 data = json.load(f)
#                 if isinstance(data, list):
#                     # Add lat/lon from start coords so frontend markers work
#                     for s in data:
#                         if 'lat' not in s:
#                             s['lat'] = s.get('start_lat', s.get('lat_start', 0))
#                             s['lon'] = s.get('start_lon', s.get('lon_start', 0))
#                         if 'assumed_age' not in s:
#                             s['assumed_age'] = s.get('age', 35)
#                     return jsonify({'segments': data, 'data_source': 'v6_engine'})
#                 if isinstance(data, dict):
#                     segs = data.get('segments', [])
#                     for s in segs:
#                         if 'lat' not in s:
#                             s['lat'] = s.get('start_lat', 0)
#                             s['lon'] = s.get('start_lon', 0)
#                     data['data_source'] = 'v6_engine'
#                     return jsonify(data)
#             except Exception as e:
#                 print(f"[app] Error parsing V6 JSON: {e}")
#     return jsonify({'segments': [], 'error': 'Run V6 first'})



# @app.route('/zone-demand')
# def zone_demand():
#     path = dp('zone_demand.csv')
#     if os.path.exists(path):
#         with open(path, newline='', encoding='utf-8') as f:
#             return jsonify(list(csv.DictReader(f)))
#     return jsonify([])


# # ─────────────────────────────────────────────────────────────
# # PHASE 3.5 — RESOLUTION WORKFLOW ENDPOINTS
# # Alert lifecycle: fired → acknowledged → field_action → resolved
# # ─────────────────────────────────────────────────────────────

# @app.route('/alerts/<int:alert_id>/acknowledge', methods=['POST'])
# def acknowledge_alert(alert_id):
#     """Mark alert as acknowledged by an engineer."""
#     notes = request.json.get('notes', '') if request.is_json else ''
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status='acknowledged', acknowledged_at=NOW(), notes=:notes
#                 WHERE alert_id=:aid AND status='fired'
#                 RETURNING alert_id, zone_id, status
#             """), {'aid': alert_id, 'notes': notes})
#             row = result.fetchone()
#             conn.commit()
#         if row:
#             # Log to audit
#             with engine.connect() as conn:
#                 conn.execute(text("""
#                     INSERT INTO audit_log (event_type, zone_id, alert_id, user_role, details)
#                     VALUES ('alert_acknowledged', :zone_id, :alert_id, 'engineer', :details)
#                 """), {'zone_id': row[1], 'alert_id': alert_id,
#                        'details': f'Alert {alert_id} acknowledged. Notes: {notes}'})
#                 conn.commit()
#             return jsonify({'success': True, 'alert_id': alert_id, 'status': 'acknowledged'})
#         return jsonify({'error': 'Alert not found or already acknowledged'}), 404
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500


# @app.route('/alerts/<int:alert_id>/field-action', methods=['POST'])
# def field_action_alert(alert_id):
#     """Mark alert as having a field action dispatched."""
#     notes = request.json.get('notes', '') if request.is_json else ''
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status='field_action', field_action_at=NOW(), notes=:notes
#                 WHERE alert_id=:aid AND status='acknowledged'
#                 RETURNING alert_id, zone_id, status
#             """), {'aid': alert_id, 'notes': notes})
#             row = result.fetchone()
#             conn.commit()
#         if row:
#             with engine.connect() as conn:
#                 conn.execute(text("""
#                     INSERT INTO audit_log (event_type, zone_id, alert_id, user_role, details)
#                     VALUES ('field_action_dispatched', :zone_id, :alert_id, 'engineer', :details)
#                 """), {'zone_id': row[1], 'alert_id': alert_id,
#                        'details': f'Field team dispatched for alert {alert_id}.'})
#                 conn.commit()
#             return jsonify({'success': True, 'alert_id': alert_id, 'status': 'field_action'})
#         return jsonify({'error': 'Alert must be acknowledged before field action'}), 400
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500


# @app.route('/alerts/<int:alert_id>/resolve', methods=['POST'])
# def resolve_alert(alert_id):
#     """Mark alert as resolved."""
#     notes = request.json.get('notes', '') if request.is_json else ''
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 UPDATE alerts
#                 SET status='resolved', resolved_at=NOW(), notes=:notes
#                 WHERE alert_id=:aid AND status IN ('acknowledged','field_action')
#                 RETURNING alert_id, zone_id, status
#             """), {'aid': alert_id, 'notes': notes})
#             row = result.fetchone()
#             conn.commit()
#         if row:
#             with engine.connect() as conn:
#                 conn.execute(text("""
#                     INSERT INTO audit_log (event_type, zone_id, alert_id, user_role, details)
#                     VALUES ('alert_resolved', :zone_id, :alert_id, 'engineer', :details)
#                 """), {'zone_id': row[1], 'alert_id': alert_id,
#                        'details': f'Alert {alert_id} resolved. Notes: {notes}'})
#                 conn.commit()
#             return jsonify({'success': True, 'alert_id': alert_id, 'status': 'resolved'})
#         return jsonify({'error': 'Alert not found or already resolved'}), 404
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500


# @app.route('/alerts/lifecycle', methods=['GET'])
# def alerts_lifecycle():
#     """Returns all alerts with their current lifecycle status from PostgreSQL."""
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 SELECT alert_id, zone_id, clps, severity, dominant_signal,
#                        scenario, status, created_at, acknowledged_at,
#                        field_action_at, resolved_at, notes
#                 FROM alerts
#                 ORDER BY created_at DESC
#                 LIMIT 100
#             """))
#             rows = [dict(r._mapping) for r in result]
#         # Convert datetimes to strings
#         for r in rows:
#             for k in ['created_at','acknowledged_at','field_action_at','resolved_at']:
#                 if r[k]: r[k] = r[k].isoformat()
#         return jsonify({'alerts': rows, 'count': len(rows)})
#     except Exception as e:
#         return jsonify({'error': str(e), 'hint': 'Run db_setup.py and db_migrate.py first'}), 500


# @app.route('/audit-log', methods=['GET'])
# def audit_log():
#     """Returns recent audit log entries."""
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("""
#                 SELECT log_id, event_type, zone_id, alert_id,
#                        user_role, details, logged_at
#                 FROM audit_log
#                 ORDER BY logged_at DESC
#                 LIMIT 50
#             """))
#             rows = [dict(r._mapping) for r in result]
#         for r in rows:
#             if r['logged_at']: r['logged_at'] = r['logged_at'].isoformat()
#         return jsonify({'log': rows, 'count': len(rows)})
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500


# # ─────────────────────────────────────────────────────────────
# if __name__ == '__main__':
#     print('=' * 55)
#     print('  Hydro-Equity Engine Backend v3.0 · SMC')
#     print(f'  Data dir:    {DATA}')
#     print(f'  Outputs dir: {OUTPUTS}')
#     print(f'  Data mode:   {"★ REAL WNTR DATA" if _real_data else "⚠ Formula Approx (run simulation_engine.py first)"}')
#     print()
#     for ep in ['/pipeline', '/infrastructure', '/tanks', '/sources',
#                '/pressure', '/equity', '/simulate', '/alerts',
#                '/recommendations', '/burst-risk', '/data-status']:
#         print(f'  GET http://localhost:5000{ep}')
#     print('=' * 55)
#     app.run(debug=True, port=5000)

"""
app.py  ·  place in: backend/
Hydro-Equity Engine — Flask Backend v3.5 FINAL
Solapur Municipal Corporation | SAMVED-2026 | Team Devsters

Phase 3.5 complete:
  - CORS open for all origins
  - Fullcity WNTR CSV files used
  - /zones returns plain list (zone_1..zone_8 format)
  - /alerts/active filters by scenario, deduplicates by zone
  - /burst-risk/top10 normalises lat/lon for map markers
  - Resolution workflow: acknowledge → field-action → resolve
  - Pipeline zone-filter for performance
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json, os, math, csv, time, threading
import pandas as pd
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from backend.database import engine
from sqlalchemy import text

app = Flask(__name__)
CORS(app)  # Open to all origins — required for VS Code Live Server (port 5500) → Flask (port 5000)

# ── Simulation Clock ─────────────────────────────────────────
current_timestep = 0

def _clock_thread():
    global current_timestep
    while True:
        time.sleep(60)
        current_timestep = (current_timestep + 1) % 96

threading.Thread(target=_clock_thread, daemon=True).start()

# ── File paths ───────────────────────────────────────────────
BASE    = os.path.dirname(os.path.abspath(__file__))
ROOT    = os.path.dirname(BASE)
DATA    = os.path.join(ROOT, 'Data')
OUTPUTS = os.path.join(ROOT, 'outputs')

def dp(fn): return os.path.join(DATA, fn)
def op(fn): return os.path.join(OUTPUTS, fn)

# ── Scenario → CSV mapping (fullcity files from V3) ─────────
SCENARIO_CSV_MAP = {
    'baseline': 'pressure_fullcity_baseline.csv',
    'leak':     'pressure_fullcity_scenario_A_leak.csv',
    'valve':    'pressure_fullcity_scenario_B_valve_close.csv',
    'surge':    'pressure_fullcity_scenario_C_demand_surge.csv',
}

_nodes_df  = None
_real_data = {}   # scenario → DataFrame

def _load_real_data():
    global _nodes_df, _real_data
    nodes_path = dp('nodes_with_elevation.csv')
    if os.path.exists(nodes_path):
        _nodes_df = pd.read_csv(nodes_path)
        _nodes_df['lon'] = _nodes_df['lon'].round(6)
        _nodes_df['lat'] = _nodes_df['lat'].round(6)
        print(f'[app] Loaded nodes: {len(_nodes_df)} rows')
    else:
        print('[app] ⚠  nodes_with_elevation.csv not found')

    for scen, filename in SCENARIO_CSV_MAP.items():
        path = op(filename)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, index_col=0)
                _real_data[scen] = df
                print(f'[app] ✓  {filename} ({len(df)} timesteps × {len(df.columns)} nodes)')
            except Exception as e:
                print(f'[app] ⚠  Failed to load {filename}: {e}')

    if _real_data:
        print(f'[app] ★  REAL WNTR DATA ACTIVE for: {list(_real_data.keys())}')
    else:
        print('[app] ⚠  No real data — using formula approximation')

_load_real_data()

# ── Formula fallback (used when WNTR data unavailable) ───────
ESR_SOURCES = [
    {'lat': 17.6935, 'lon': 75.8810, 'head': 72, 'name': 'Main WTP'},
    {'lat': 17.7012, 'lon': 75.9280, 'head': 68, 'name': 'North ESR-2'},
    {'lat': 17.6623, 'lon': 75.9034, 'head': 65, 'name': 'South ESR-3'},
    {'lat': 17.6878, 'lon': 75.8654, 'head': 70, 'name': 'West ESR-4'},
    {'lat': 17.6758, 'lon': 75.9412, 'head': 60, 'name': 'East ESR-5'},
    {'lat': 17.7080, 'lon': 75.8950, 'head': 66, 'name': 'NW ESR-6'},
    {'lat': 17.6700, 'lon': 75.8780, 'head': 63, 'name': 'SW ESR-7'},
]

ZONES = [
    {'id': 'z1', 'name': 'Zone 1', 'lat': 17.710, 'lon': 75.882},
    {'id': 'z2', 'name': 'Zone 2', 'lat': 17.700, 'lon': 75.910},
    {'id': 'z3', 'name': 'Zone 3', 'lat': 17.690, 'lon': 75.935},
    {'id': 'z4', 'name': 'Zone 4', 'lat': 17.675, 'lon': 75.898},
    {'id': 'z5', 'name': 'Zone 5', 'lat': 17.665, 'lon': 75.920},
    {'id': 'z6', 'name': 'Zone 6', 'lat': 17.680, 'lon': 75.870},
    {'id': 'z7', 'name': 'Zone 7', 'lat': 17.695, 'lon': 75.958},
    {'id': 'z8', 'name': 'Zone 8', 'lat': 17.655, 'lon': 75.940},
]

def _hdist(la1, lo1, la2, lo2):
    dx = (lo1 - lo2) * 111 * math.cos(la1 * math.pi / 180)
    dy = (la1 - la2) * 111
    return math.sqrt(dx * dx + dy * dy)

def _dmult(h):
    if  6 <= h <  8: return 2.5
    if 17 <= h < 20: return 2.0
    if  8 <= h < 17: return 1.0
    return 0.05

def _formula_pressure(lat, lon, scenario='baseline', hour=8):
    md, sh = 1e9, 65
    for s in ESR_SOURCES:
        d = _hdist(lat, lon, s['lat'], s['lon'])
        if d < md:
            md = d
            sh = s['head']
    fl = md * 3.8
    el = (lat - 17.655) * 22 + (lon - 75.960) * 15
    dl = _dmult(hour) * 3
    ns = ((abs(lat * 1000) * 7 + abs(lon * 100) * 13) % 100) / 100.0 * 6 - 3
    p  = sh - fl + el * 0.4 - dl + ns
    if scenario == 'leak':
        if lon > 75.935:   p *= 0.52
        elif lon > 75.920: p *= 0.78
    elif scenario == 'valve':
        if lat < 17.663:   p *= 0.22
        elif lat < 17.672: p *= 0.55
    elif scenario == 'surge':
        p *= (0.64 if lon > 75.91 else 0.72)
    return round(max(2, min(82, p)), 2)

def _pressure_color(p):
    if p < 10: return '#D32F2F'
    if p < 20: return '#E65100'
    if p < 30: return '#F9A825'
    if p < 40: return '#8BC34A'
    if p < 60: return '#2E7D32'
    return '#0D5FA8'

def _hour_to_timestep(hour):
    return min(int(hour * 4), 95)

def get_pressure(lat, lon, scenario='baseline', hour=8):
    """Real WNTR pressure if available, else formula fallback."""
    if scenario in _real_data and _nodes_df is not None:
        try:
            pressure_df = _real_data[scenario]
            t           = _hour_to_timestep(hour)
            dists       = ((_nodes_df['lat'] - lat)**2 + (_nodes_df['lon'] - lon)**2)
            closest_idx = dists.idxmin()
            node_name   = f'J{closest_idx}'
            row         = pressure_df.iloc[t]
            if node_name in row.index:
                return round(max(0, min(100, float(row[node_name]))), 2)
            for offset in [1, -1, 2, -2, 5, -5]:
                alt = f'J{closest_idx + offset}'
                if alt in row.index:
                    return round(max(0, min(100, float(row[alt]))), 2)
        except Exception:
            pass
    return _formula_pressure(lat, lon, scenario, hour)

# ─────────────────────────────────────────────────────────────
# API ROUTES
# ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return jsonify({'status': 'Hydro-Equity Engine · SMC', 'version': '3.5',
                    'data_mode': 'REAL_WNTR' if _real_data else 'FORMULA'})

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'timestep': current_timestep,
                    'data_mode': 'REAL_WNTR' if _real_data else 'FORMULA'})

# ── Pipeline ─────────────────────────────────────────────────
@app.route('/pipeline')
def pipeline():
    """
    Returns pressure-enriched pipeline GeoJSON.
    ?zone=all     → all 10,160 pipes (initial load, cached by frontend)
    ?zone=zone_5  → only Zone 5 pipes (fast zone filter)
    """
    scen        = request.args.get('scenario', 'baseline')
    hour        = int(request.args.get('hour', 8))
    zone_filter = request.args.get('zone', 'all')

    path = dp('pipeline.geojson')
    if not os.path.exists(path):
        return jsonify({'error': 'pipeline.geojson not found'}), 404

    with open(path, 'r', encoding='utf-8') as f:
        geo = json.load(f)

    def _norm(z):
        return str(z).lower().replace('zone', '').replace('_', '').strip()

    enriched = []
    for feat in geo.get('features', []):
        props = feat.get('properties') or {}

        if zone_filter != 'all':
            fz = str(props.get('Water Zone', props.get('zone_id', ''))).strip()
            if _norm(fz) != _norm(zone_filter):
                continue

        geom = feat.get('geometry', {})
        cs   = geom.get('coordinates', [])
        if geom.get('type') == 'MultiLineString':
            cs = cs[0] if cs else []
        if not cs or len(cs) < 2:
            enriched.append(feat)
            continue

        mid = cs[len(cs) // 2]
        p   = get_pressure(mid[1], mid[0], scen, hour)
        feat['properties']['sim_pressure'] = p
        feat['properties']['sim_color']    = _pressure_color(p)
        enriched.append(feat)

    geo['features'] = enriched
    return jsonify(geo)

# ── Infrastructure / GIS layers ──────────────────────────────
@app.route('/infrastructure')
def infrastructure():
    items = []
    path  = dp('infrastructure_points.csv')
    if os.path.exists(path):
        with open(path, newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                try:
                    items.append({
                        'lat':  float(row.get('lat', row.get('Lat', 0))),
                        'lon':  float(row.get('lon', row.get('Lon', 0))),
                        'type': row.get('type', row.get('Type', 'ESR')),
                        'name': row.get('name', row.get('Name', '')),
                        'zone': row.get('zone', ''),
                    })
                except Exception:
                    pass
    if not items:
        items = [{'lat': e['lat'], 'lon': e['lon'], 'type': 'ESR',
                  'name': e['name'], 'zone': ''}
                 for e in ESR_SOURCES]
    return jsonify(items)

@app.route('/tanks')
def tanks():
    path = dp('storage_tank.geojson')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))
    return jsonify({'type': 'FeatureCollection', 'features': []})

@app.route('/sources')
def sources():
    path = dp('water_source.geojson')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))
    return jsonify({'type': 'FeatureCollection', 'features': []})

# ── Pressure nodes ───────────────────────────────────────────
@app.route('/pressure')
def pressure():
    scen  = request.args.get('scenario', 'baseline')
    hour  = int(request.args.get('hour', 8))
    path  = dp('nodes_with_elevation.csv')
    nodes = []
    if os.path.exists(path):
        with open(path, newline='', encoding='utf-8') as f:
            for i, row in enumerate(csv.DictReader(f)):
                if i % 5 != 0:
                    continue
                try:
                    lat = float(row['lat'])
                    lon = float(row['lon'])
                    p   = get_pressure(lat, lon, scen, hour)
                    nodes.append({'lat': lat, 'lon': lon,
                                  'pressure': p, 'color': _pressure_color(p)})
                except Exception:
                    pass
    return jsonify(nodes)

# ── V4 Equity ────────────────────────────────────────────────
@app.route('/equity')
def equity():
    """Returns V4 equity output with cwei_daily."""
    path = op('v4_equity_minimal.json')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            try:
                data  = json.load(f)
                zones = data.get('zones', [])
                cwei  = data.get('cwei_daily', 0)
                return jsonify({'zones': zones, 'cwei': cwei,
                                'data_source': 'v4_engine'})
            except Exception as e:
                print(f'[app] V4 parse error: {e}')
    return jsonify({'zones': [], 'cwei': 0.0, 'error': 'Run V4 first'})

@app.route('/zones')
def zones_endpoint():
    """Returns v4_zone_status.json as a plain list for the frontend heatmap."""
    path = op('v4_zone_status.json')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                # Always return as plain list
                if isinstance(data, list):
                    return jsonify(data)
                return jsonify(data)
            except Exception as e:
                print(f'[app] v4_zone_status parse error: {e}')
    return jsonify({'error': 'Run V4 first'})

# ── Simulate (24h timeline) ───────────────────────────────────
@app.route('/simulate')
def simulate():
    scen    = request.args.get('scenario', 'baseline')
    zone_id = request.args.get('zone', 'z5')
    z       = next((x for x in ZONES if x['id'] == zone_id), ZONES[4])

    if scen in _real_data and _nodes_df is not None:
        pressure_df = _real_data[scen]
        dists       = ((_nodes_df['lat'] - z['lat'])**2 +
                       (_nodes_df['lon'] - z['lon'])**2)
        closest_idx = dists.idxmin()
        node_name   = f'J{closest_idx}'
        tl = []
        for h in range(24):
            t = _hour_to_timestep(h)
            if node_name in pressure_df.columns:
                p = round(max(0, min(100, float(pressure_df.iloc[t][node_name]))), 2)
            else:
                p = get_pressure(z['lat'], z['lon'], scen, h)
            tl.append({'hour': h, 'pressure': p})
    else:
        tl = [{'hour': h,
               'pressure': get_pressure(z['lat'], z['lon'], scen, h)}
              for h in range(24)]

    return jsonify({'zone': zone_id, 'timeline': tl, 'scenario': scen,
                    'data_source': 'wntr' if scen in _real_data else 'formula'})

@app.route('/data-status')
def data_status():
    files = {}
    for scen, filename in SCENARIO_CSV_MAP.items():
        path = op(filename)
        files[filename] = {
            'exists': os.path.exists(path),
            'loaded': scen in _real_data,
            'rows':   len(_real_data[scen]) if scen in _real_data else 0,
            'cols':   len(_real_data[scen].columns) if scen in _real_data else 0,
        }
    return jsonify({
        'real_data_scenarios': list(_real_data.keys()),
        'nodes_loaded':  _nodes_df is not None,
        'node_count':    len(_nodes_df) if _nodes_df is not None else 0,
        'files':         files,
        'data_mode':     'REAL_WNTR' if _real_data else 'FORMULA_APPROX',
    })

# ── V5 Alerts ────────────────────────────────────────────────
@app.route('/alerts')
def alerts():
    scen    = request.args.get('scenario', 'baseline')
    v5_path = op('v5_alerts.json')
    if os.path.exists(v5_path):
        with open(v5_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if isinstance(data, dict):
                    al = data.get(scen, [])
                elif isinstance(data, list):
                    al = data
                else:
                    al = []
                return jsonify({'alerts': al, 'count': len(al),
                                'data_source': 'v5_engine'})
            except Exception as e:
                print(f'[app] V5 parse error: {e}')
    return jsonify({'alerts': [], 'count': 0, 'error': 'Run V5 first'})

@app.route('/alerts/active')
def alerts_active():
    """
    Returns top deduplicated alerts for the requested scenario.
    One alert per zone (highest CLPS wins).
    Normalises field names for frontend rendering.
    Baseline: alerts included (shown in sidebar only, not on map).
    Leak/Valve/Surge: alerts shown both in sidebar and on map.
    """
    scen    = request.args.get('scenario', 'baseline')
    v5_path = op('v5_alerts.json')

    if not os.path.exists(v5_path):
        return jsonify({'alerts': [], 'count': 0, 'error': 'Run V5 first'})

    with open(v5_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except Exception as e:
            return jsonify({'alerts': [], 'count': 0, 'error': str(e)})

    # Support dict-by-scenario (new V5) and flat list (old V5)
    if isinstance(data, dict):
        al = data.get(scen, data.get('baseline', []))
    elif isinstance(data, list):
        al = data
    else:
        al = []

    # Deduplicate: one alert per zone, keep highest CLPS
    seen = {}
    for a in al:
        z = a.get('zone_id', '')
        if z not in seen or a.get('clps', 0) > seen[z].get('clps', 0):
            seen[z] = a

    top = sorted(seen.values(), key=lambda x: x.get('clps', 0), reverse=True)[:10]

    # Normalise field names for frontend
    for a in top:
        raw_zone = a.get('zone_id', '')
        zone_num = raw_zone.replace('zone_', '')
        a['zone']  = f'Zone {zone_num}'
        a['level'] = 'high' if a.get('clps', 0) > 0.6 else 'moderate'
        a['title'] = f"{a.get('dominant_signal', 'Alert')} Detected"
        a['body']  = (f"Zone {zone_num} under stress · "
                      f"Signal: {a.get('dominant_signal', '')} · "
                      f"Nodes: {', '.join(str(n) for n in a.get('probable_node_ids', [])[:2])}")
        # Add zone_id_short for frontend marker lookup (z1..z8)
        a['zone_id_short'] = 'z' + zone_num if zone_num.isdigit() else ''

    return jsonify({'alerts': top, 'count': len(top),
                    'data_source': 'v5_engine', 'scenario': scen})

# ── Recommendations (illustrative per Architecture Bible) ────
@app.route('/recommendations')
def recommendations():
    scen = request.args.get('scenario', 'baseline')
    data = {
        'baseline': [
            {'zone': 'Zone 7 — ROUTINE',
             'action': 'Increase ESR outlet pressure by 10–15%. HEI=0.61 — tail-end nodes receiving inadequate pressure.',
             'impact': 'HEI improvement est: 0.61 → 0.82'},
            {'zone': 'Zone 3 — MAINTENANCE',
             'action': 'Pipe segment near node J-3814 at HIGH burst risk (PSS: 0.84). Cast Iron, ~35yr. Schedule urgent inspection.',
             'impact': 'Prevents burst, NRW reduction 2-4%'},
            {'zone': 'Zone 5 — OPTIMIZATION',
             'action': 'Throttle Zone 5 inlet valve by 15% to reduce over-pressurization (HEI=1.34). Redirect flow to Zone 7.',
             'impact': 'City CWEI improvement: +0.07'},
        ],
        'leak': [
            {'zone': 'Zone 3 · Zone 5 — URGENT',
             'action': 'Dispatch field team to (17.682, 75.942). Isolate pipe segment east of ESR-5 to prevent further pressure loss.',
             'impact': 'CLPS reduction: 0.81 → 0.28 after isolation'},
            {'zone': 'Zone 7 — MODERATE',
             'action': 'Night flow anomaly — inspect for unauthorized extraction 01:00–04:00.',
             'impact': 'NRW reduction est. 4-6% after investigation'},
        ],
        'valve': [
            {'zone': 'Zone 8 — CRITICAL',
             'action': 'Reopen south distribution valve immediately. Activate tanker supply for Ward 14 & 15 until pressure restored.',
             'impact': 'HEI improvement: 0.22 → 0.85 on valve restore'},
            {'zone': 'All Zones — ADVISORY',
             'action': 'Redistribute demand to Zone 4 cross-connection. Inform field engineers of valve status.',
             'impact': 'Stabilizes pressure within 2 supply cycles'},
        ],
        'surge': [
            {'zone': 'Zone 1 · Zone 6 — HIGH',
             'action': 'Increase ESR outlet pressure by 12–15%. Throttle Zone 3 inlet by 18% to redistribute surplus.',
             'impact': 'HEI improvement: 0.64 → 0.82 est.'},
            {'zone': 'System-wide — ADVISORY',
             'action': 'Stagger supply: Zone 1–4 at 06:00, Zone 5–8 at 07:30 to reduce concurrent peak demand.',
             'impact': 'Average pressure +8m, NRW est. -3%'},
        ],
    }
    return jsonify({'recommendations': data.get(scen, [])})

# ── V6 Burst Risk ────────────────────────────────────────────
@app.route('/burst-risk')
def burst_risk():
    path = op('v6_burst.json')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                data['data_source'] = 'v6_engine'
                return jsonify(data)
            except Exception as e:
                print(f'[app] v6_burst parse error: {e}')
    return jsonify({'segments': [], 'error': 'Run V6 first'})

@app.route('/burst-risk/top10')
def burst_risk_top10():
    """
    Top 10 burst risk pipe segments.
    Normalises lat/lon fields so frontend map markers work correctly.
    V6 outputs start_lat/start_lon — frontend needs lat/lon.
    """
    path = op('v6_burst_top10.json')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                segs = data if isinstance(data, list) else data.get('segments', [])
                for s in segs:
                    # Normalise coordinate fields
                    if 'lat' not in s or s.get('lat', 0) == 0:
                        s['lat'] = s.get('start_lat', s.get('lat_start', 0))
                        s['lon'] = s.get('start_lon', s.get('lon_start', 0))
                    # Normalise age field
                    if 'assumed_age' not in s:
                        s['assumed_age'] = s.get('age', 35)
                return jsonify({'segments': segs, 'count': len(segs),
                                'data_source': 'v6_engine'})
            except Exception as e:
                print(f'[app] v6_burst_top10 parse error: {e}')
    return jsonify({'segments': [], 'error': 'Run V6 first'})

@app.route('/zone-demand')
def zone_demand():
    path = dp('zone_demand.csv')
    if os.path.exists(path):
        with open(path, newline='', encoding='utf-8') as f:
            return jsonify(list(csv.DictReader(f)))
    return jsonify([])

# ─────────────────────────────────────────────────────────────
# PHASE 3.5 — RESOLUTION WORKFLOW (PostgreSQL-backed)
# Alert lifecycle: fired → acknowledged → field_action → resolved
# ─────────────────────────────────────────────────────────────

@app.route('/alerts/<int:alert_id>/acknowledge', methods=['POST'])
def acknowledge_alert(alert_id):
    notes = request.json.get('notes', '') if request.is_json else ''
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status='acknowledged', acknowledged_at=NOW(), notes=:notes
                WHERE alert_id=:aid AND status='fired'
                RETURNING alert_id, zone_id, status
            """), {'aid': alert_id, 'notes': notes})
            row = result.fetchone()
            conn.commit()
        if row:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO audit_log
                        (event_type, zone_id, alert_id, user_role, details)
                    VALUES ('alert_acknowledged', :zone_id, :alert_id, 'engineer', :details)
                """), {'zone_id': row[1], 'alert_id': alert_id,
                       'details': f'Alert {alert_id} acknowledged. Notes: {notes}'})
                conn.commit()
            return jsonify({'success': True, 'alert_id': alert_id,
                            'status': 'acknowledged'})
        return jsonify({'error': 'Alert not found or already acknowledged'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/alerts/<int:alert_id>/field-action', methods=['POST'])
def field_action_alert(alert_id):
    notes = request.json.get('notes', '') if request.is_json else ''
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status='field_action', field_action_at=NOW(), notes=:notes
                WHERE alert_id=:aid AND status='acknowledged'
                RETURNING alert_id, zone_id, status
            """), {'aid': alert_id, 'notes': notes})
            row = result.fetchone()
            conn.commit()
        if row:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO audit_log
                        (event_type, zone_id, alert_id, user_role, details)
                    VALUES ('field_action_dispatched', :zone_id, :alert_id, 'engineer', :details)
                """), {'zone_id': row[1], 'alert_id': alert_id,
                       'details': f'Field team dispatched for alert {alert_id}.'})
                conn.commit()
            return jsonify({'success': True, 'alert_id': alert_id,
                            'status': 'field_action'})
        return jsonify({'error': 'Alert must be acknowledged before field action'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/alerts/<int:alert_id>/resolve', methods=['POST'])
def resolve_alert(alert_id):
    notes = request.json.get('notes', '') if request.is_json else ''
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                UPDATE alerts
                SET status='resolved', resolved_at=NOW(), notes=:notes
                WHERE alert_id=:aid AND status IN ('acknowledged','field_action')
                RETURNING alert_id, zone_id, status
            """), {'aid': alert_id, 'notes': notes})
            row = result.fetchone()
            conn.commit()
        if row:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO audit_log
                        (event_type, zone_id, alert_id, user_role, details)
                    VALUES ('alert_resolved', :zone_id, :alert_id, 'engineer', :details)
                """), {'zone_id': row[1], 'alert_id': alert_id,
                       'details': f'Alert {alert_id} resolved. Notes: {notes}'})
                conn.commit()
            return jsonify({'success': True, 'alert_id': alert_id,
                            'status': 'resolved'})
        return jsonify({'error': 'Alert not found or already resolved'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/alerts/lifecycle', methods=['GET'])
def alerts_lifecycle():
    """Returns full alert lifecycle history from PostgreSQL."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT alert_id, zone_id, clps, severity, dominant_signal,
                       scenario, status, created_at, acknowledged_at,
                       field_action_at, resolved_at, notes
                FROM alerts ORDER BY created_at DESC LIMIT 100
            """))
            rows = [dict(r._mapping) for r in result]
        for r in rows:
            for k in ['created_at', 'acknowledged_at', 'field_action_at', 'resolved_at']:
                if r[k]:
                    r[k] = r[k].isoformat()
        return jsonify({'alerts': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e),
                        'hint': 'Run db_setup.py and db_migrate.py first'}), 500

@app.route('/audit-log', methods=['GET'])
def audit_log():
    """Returns recent audit log entries from PostgreSQL."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT log_id, event_type, zone_id, alert_id,
                       user_role, details, logged_at
                FROM audit_log ORDER BY logged_at DESC LIMIT 50
            """))
            rows = [dict(r._mapping) for r in result]
        for r in rows:
            if r['logged_at']:
                r['logged_at'] = r['logged_at'].isoformat()
        return jsonify({'log': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('=' * 55)
    print('  Hydro-Equity Engine Backend v3.5 FINAL · SMC')
    print(f'  Data dir:    {DATA}')
    print(f'  Outputs dir: {OUTPUTS}')
    print(f'  Data mode:   {"★ REAL WNTR DATA" if _real_data else "⚠ Formula Approx"}')
    print('=' * 55)
    app.run(debug=True, port=5000)