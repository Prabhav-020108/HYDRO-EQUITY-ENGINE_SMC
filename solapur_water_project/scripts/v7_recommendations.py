# """
# Hydro-Equity Engine — Phase 4b
# scripts/v7_recommendations.py
# V7 — Role-Partitioned Recommendation Engine

# Reads:
#   outputs/v4_zone_status.json   → zone HEI / status
#   outputs/v5_alerts.json        → CLPS alerts per scenario
#   outputs/v6_burst_top10.json   → top burst-risk pipe segments
#   Data/pipe_segments.csv        → for influence map (optional)
#   Data/nodes_with_elevation.csv → for influence map (optional)
#   Data/infrastructure_points.csv → for ESR locations (optional)

# Writes (PostgreSQL):
#   engineer_recs       → valve_id, pipe_id, urgency, HEI gain (Trigger A,B,C)
#   ward_recs           → plain-language escalation notes  (Trigger A)
#   commissioner_recs   → city summary, budget flag        (Trigger A,C)
#   citizen_recs        → supply status, advisory          (Trigger D)
#   v7_run_log          → run timestamp and counts

# Run manually:   python scripts/v7_recommendations.py
# Auto-scheduled: every 5 minutes via APScheduler in backend/app.py
# """

# import os, sys, json
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# import pandas as pd
# from datetime import datetime
# from sqlalchemy import text

# ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DATA    = os.path.join(ROOT, 'Data')
# OUTPUTS = os.path.join(ROOT, 'outputs')

# # ── Zone display name mapping ──────────────────────────────────────
# ZONE_NAMES = {
#     'zone_1': 'Zone 1', 'zone_2': 'Zone 2', 'zone_3': 'Zone 3',
#     'zone_4': 'Zone 4', 'zone_5': 'Zone 5', 'zone_6': 'Zone 6',
#     'zone_7': 'Zone 7', 'zone_8': 'Zone 8',
# }

# # Fallback ESR positions (used if infrastructure_points.csv missing)
# FALLBACK_ESRS = [
#     {'lat': 17.6935, 'lon': 75.8810, 'zone': 'zone_1'},
#     {'lat': 17.7012, 'lon': 75.9280, 'zone': 'zone_2'},
#     {'lat': 17.6623, 'lon': 75.9034, 'zone': 'zone_3'},
#     {'lat': 17.6878, 'lon': 75.8654, 'zone': 'zone_4'},
#     {'lat': 17.6758, 'lon': 75.9412, 'zone': 'zone_5'},
#     {'lat': 17.7080, 'lon': 75.8950, 'zone': 'zone_6'},
#     {'lat': 17.6700, 'lon': 75.8780, 'zone': 'zone_7'},
#     {'lat': 17.6550, 'lon': 75.9100, 'zone': 'zone_8'},
# ]

# # Zone centroid positions (for node_coords in recommendations)
# ZONE_COORDS = {
#     'zone_1': {'lat': 17.710, 'lon': 75.882},
#     'zone_2': {'lat': 17.700, 'lon': 75.910},
#     'zone_3': {'lat': 17.690, 'lon': 75.935},
#     'zone_4': {'lat': 17.675, 'lon': 75.898},
#     'zone_5': {'lat': 17.665, 'lon': 75.920},
#     'zone_6': {'lat': 17.680, 'lon': 75.870},
#     'zone_7': {'lat': 17.695, 'lon': 75.958},
#     'zone_8': {'lat': 17.655, 'lon': 75.940},
# }


# # ══════════════════════════════════════════════════════════════════════
# #  DATA LOADING
# # ══════════════════════════════════════════════════════════════════════

# def load_analytics_data():
#     """Load V4, V5, V6 outputs. Returns (zones, alerts, burst_segs)."""
#     zones = []
#     path = os.path.join(OUTPUTS, 'v4_zone_status.json')
#     if os.path.exists(path):
#         with open(path, encoding='utf-8') as f:
#             zones = json.load(f)
#     else:
#         print('  [WARN] v4_zone_status.json not found — run V4 first')

#     alerts_all = {}
#     path = os.path.join(OUTPUTS, 'v5_alerts.json')
#     if os.path.exists(path):
#         with open(path, encoding='utf-8') as f:
#             alerts_all = json.load(f)
#     else:
#         print('  [WARN] v5_alerts.json not found — run V5 first')

#     burst_segs = []
#     path = os.path.join(OUTPUTS, 'v6_burst_top10.json')
#     if os.path.exists(path):
#         with open(path, encoding='utf-8') as f:
#             burst_segs = json.load(f)
#     else:
#         print('  [WARN] v6_burst_top10.json not found — run V6 first')

#     return zones, alerts_all, burst_segs


# def load_network_data():
#     """Load pipe segments and nodes for influence map building."""
#     pipes_df = None
#     nodes_df = None
#     infra_df = None

#     p = os.path.join(DATA, 'pipe_segments.csv')
#     if os.path.exists(p):
#         pipes_df = pd.read_csv(p)

#     p = os.path.join(DATA, 'nodes_with_elevation.csv')
#     if os.path.exists(p):
#         nodes_df = pd.read_csv(p)

#     p = os.path.join(DATA, 'infrastructure_points.csv')
#     if os.path.exists(p):
#         infra_df = pd.read_csv(p)

#     return pipes_df, nodes_df, infra_df


# # ══════════════════════════════════════════════════════════════════════
# #  INFLUENCE MAP
# #  For each zone → find control pipe (largest-diameter cross-zone pipe)
# #  and nearest ESR node. Used to build specific valve/pipe recommendations.
# # ══════════════════════════════════════════════════════════════════════

# def build_influence_map(pipes_df, nodes_df, infra_df):
#     """
#     Returns dict: {zone_id: {control_pipe_id, esr_lat, esr_lon}}
#     Falls back gracefully if CSV files are unavailable.
#     """
#     influence = {}

#     # Initialize all known zones with defaults
#     for zone, coords in ZONE_COORDS.items():
#         influence[zone] = {
#             'control_pipe_id': 'V-' + zone.upper().replace('_', ''),
#             'esr_lat': coords['lat'],
#             'esr_lon': coords['lon'],
#         }

#     if pipes_df is None or nodes_df is None:
#         # No network data — use defaults
#         return influence

#     try:
#         # Build zone lookup: node_id → zone_id
#         node_zone = {}
#         for _, r in nodes_df.iterrows():
#             nid = str(int(r['node_id'])) if pd.notna(r['node_id']) else None
#             if nid:
#                 node_zone[nid] = str(r.get('zone_id', ''))

#         # Find cross-zone pipes (inlet pipes per zone)
#         zone_inlets = {}
#         valid = pipes_df.dropna(subset=['start_node_id', 'end_node_id']).copy()
#         for _, row in valid.iterrows():
#             try:
#                 zu = node_zone.get(str(int(row['start_node_id'])), '')
#                 zv = node_zone.get(str(int(row['end_node_id'])), '')
#                 if zu != zv and zv:
#                     if zv not in zone_inlets:
#                         zone_inlets[zv] = []
#                     zone_inlets[zv].append({
#                         'pipe_id':  str(int(row['segment_id'])),
#                         'diameter': float(row.get('diameter_m', 0.1) or 0.1),
#                     })
#             except (ValueError, TypeError):
#                 continue

#         # Control pipe = largest-diameter inlet pipe for each zone
#         for zone, inlets in zone_inlets.items():
#             if inlets:
#                 best = max(inlets, key=lambda x: x['diameter'])
#                 influence[zone] = {**influence.get(zone, {}), 'control_pipe_id': best['pipe_id']}

#         # ESR locations from infrastructure_points.csv
#         if infra_df is not None and len(infra_df) > 0:
#             esrs = infra_df[infra_df['feature_type'].isin(['storage_tank', 'water_source'])]
#             zone_nodes = nodes_df.copy()

#             for zone in influence:
#                 zn = zone_nodes[zone_nodes['zone_id'] == zone]
#                 if len(zn) == 0:
#                     continue
#                 clat = zn['lat'].mean()
#                 clon = zn['lon'].mean()
#                 if len(esrs) > 0:
#                     dists = ((esrs['lat'] - clat)**2 + (esrs['lon'] - clon)**2)**0.5
#                     nearest = esrs.iloc[dists.values.argmin()]
#                     influence[zone]['esr_lat'] = float(nearest['lat'])
#                     influence[zone]['esr_lon'] = float(nearest['lon'])

#     except Exception as e:
#         print(f'  [WARN] Influence map build error: {e} — using defaults')

#     return influence


# # ══════════════════════════════════════════════════════════════════════
# #  DATABASE HELPERS
# # ══════════════════════════════════════════════════════════════════════

# def clear_old_recs(conn):
#     """Remove recommendations older than 24 hours to keep tables clean."""
#     for tbl in ['engineer_recs', 'ward_recs', 'commissioner_recs', 'citizen_recs']:
#         try:
#             conn.execute(text(
#                 f"DELETE FROM {tbl} WHERE created_at < NOW() - INTERVAL '24 hours'"
#             ))
#         except Exception:
#             pass  # Table might not exist yet — safe to ignore


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER A — Equity Rules (→ ALL 4 channels)
# #  Fires for every zone, every run.
# #  Severity: severe < 0.70, moderate 0.70-0.85, equitable 0.85-1.30, over > 1.30
# # ══════════════════════════════════════════════════════════════════════

# def trigger_a_equity(zones, influence, conn):
#     """
#     Writes to engineer_recs, ward_recs, commissioner_recs, citizen_recs
#     based on zone HEI status.
#     Returns (eng_count, ward_count) written.
#     """
#     eng_count = ward_count = 0
#     severe_zones  = [z for z in zones if z.get('status') == 'severe']
#     moderate_zones = [z for z in zones if z.get('status') == 'moderate']
#     over_zones    = [z for z in zones if z.get('status') == 'over']

#     for z in zones:
#         zone_id  = z.get('zone_id', '')
#         hei      = float(z.get('hei', 0) or 0)
#         status   = z.get('status', 'equitable')
#         zm       = ZONE_COORDS.get(zone_id, {'lat': 17.68, 'lon': 75.91})
#         infl     = influence.get(zone_id, {})
#         pipe_id  = infl.get('control_pipe_id', 'unknown')
#         nm       = ZONE_NAMES.get(zone_id, zone_id)

#         # ── engineer_recs (Trigger A) ─────────────────────────────
#         if status == 'severe':
#             est_gain = round(min(0.85, hei + 0.22) - hei, 3)
#             action = (
#                 f"SEVERE inequity detected in {nm} (HEI = {hei:.3f}). "
#                 f"Increase ESR outlet pressure by 12–15% on Control Pipe #{pipe_id}. "
#                 f"Tail-end nodes receiving < 70% of zone average pressure. "
#                 f"Estimated HEI improvement: {hei:.3f} → {hei + est_gain:.3f} after adjustment."
#             )
#             urgency = 'URGENT'
#             delta = 8.0
#         elif status == 'moderate':
#             est_gain = round(min(0.85, hei + 0.08) - hei, 3)
#             action = (
#                 f"Moderate imbalance in {nm} (HEI = {hei:.3f}). "
#                 f"Review valve settings on Control Pipe #{pipe_id}. "
#                 f"Consider 5–8% pressure increase at ESR outlet. "
#                 f"Estimated improvement: {hei:.3f} → {hei + est_gain:.3f}."
#             )
#             urgency = 'MODERATE'
#             delta = 4.0
#             est_gain = 0.08
#         elif status == 'over':
#             est_gain = 0.0
#             action = (
#                 f"Over-pressurization in {nm} (HEI = {hei:.3f}). "
#                 f"Throttle Control Pipe #{pipe_id} by 10–15% to reduce excess pressure. "
#                 f"Risk of pipe burst — immediate valve adjustment recommended."
#             )
#             urgency = 'HIGH'
#             delta = -6.0
#         else:
#             est_gain = 0.0
#             action = (
#                 f"{nm} is within equitable range (HEI = {hei:.3f}). "
#                 f"No pressure adjustment required. Continue routine monitoring."
#             )
#             urgency = 'LOW'
#             delta = 0.0

#         conn.execute(text("""
#             INSERT INTO engineer_recs
#               (zone_id, trigger_type, action_text, valve_id, pipe_id,
#                pressure_delta, urgency, estimated_hei_gain, node_coords)
#             VALUES
#               (:z, 'A_equity', :act, :vid, :pid, :dp, :urg, :gain, :coords)
#         """), {
#             'z':      zone_id,
#             'act':    action,
#             'vid':    f"V-{zone_id.upper().replace('_','')}",
#             'pid':    pipe_id,
#             'dp':     delta,
#             'urg':    urgency,
#             'gain':   est_gain,
#             'coords': json.dumps(zm),
#         })
#         eng_count += 1

#         # ── ward_recs (Trigger A) ─────────────────────────────────
#         if status == 'severe':
#             escalation = True
#             ward_action = (
#                 f"{nm} water supply is critically below acceptable levels (HEI = {hei:.3f}). "
#                 f"Escalate to engineering team immediately for valve adjustment."
#             )
#             reliability_note = "Critical — pressure below minimum service threshold"
#         elif status == 'moderate':
#             escalation = True
#             ward_action = (
#                 f"{nm} supply is experiencing moderate pressure imbalance (HEI = {hei:.3f}). "
#                 f"Notify engineering team for review during next maintenance window."
#             )
#             reliability_note = "Moderate — some tail-end households may have reduced supply"
#         elif status == 'over':
#             escalation = False
#             ward_action = (
#                 f"{nm} supply pressure is above normal levels (HEI = {hei:.3f}). "
#                 f"Engineering team is managing pressure reduction. Monitor for complaints."
#             )
#             reliability_note = "High pressure — watch for burst pipe complaints"
#         else:
#             escalation = False
#             ward_action = (
#                 f"{nm} supply is operating normally (HEI = {hei:.3f}). "
#                 f"No action required. Continue routine complaint monitoring."
#             )
#             reliability_note = "Good — pressure within acceptable range"

#         # Count open complaints for this zone from DB
#         complaint_count = 0
#         try:
#             res = conn.execute(
#                 text("SELECT COUNT(*) FROM citizen_complaints WHERE zone_id=:z AND status='open'"),
#                 {'z': zone_id}
#             ).scalar()
#             complaint_count = int(res or 0)
#         except Exception:
#             pass

#         conn.execute(text("""
#             INSERT INTO ward_recs
#               (zone_id, trigger_type, action_text, escalation_flag,
#                service_reliability_note, complaint_count)
#             VALUES (:z, 'A_equity', :act, :esc, :note, :cc)
#         """), {
#             'z':    zone_id,
#             'act':  ward_action,
#             'esc':  escalation,
#             'note': reliability_note,
#             'cc':   complaint_count,
#         })
#         ward_count += 1

#     return eng_count, ward_count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER B — Leak Rules (→ engineer_recs + ward_recs)
# #  Uses 'baseline' alerts from V5 (most reliable scenario).
# #  Engineer gets technical dispatch instructions.
# #  Ward officer gets plain-language action linked to the same alert.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_b_leak(alerts_all, influence, conn):
#     """Writes to engineer_recs (technical) AND ward_recs (plain language) per alert."""
#     # Use baseline alerts — these are always present and represent real anomalies
#     if isinstance(alerts_all, dict):
#         alerts = alerts_all.get('baseline', alerts_all.get('leak', []))
#     else:
#         alerts = alerts_all if isinstance(alerts_all, list) else []

#     eng_count  = 0
#     ward_count = 0

#     for a in alerts:
#         zone_id = a.get('zone_id', '')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)
#         clps    = float(a.get('clps', 0) or 0)
#         sig     = a.get('dominant_signal', 'PDR_n')
#         infl    = influence.get(zone_id, {})
#         pipe_id = infl.get('control_pipe_id', 'unknown')

#         if clps < 0.05:
#             continue  # Skip trivial alerts

#         # ── Engineer rec: technical, signal-specific ──────────────────
#         if sig == 'PDR_n':
#             eng_action = (
#                 f"SUDDEN PRESSURE DROP in {nm} (CLPS={clps:.3f}). "
#                 f"PDR_n signal dominant — rapid pressure decay detected. "
#                 f"Dispatch field team to inspect Pipe #{pipe_id} immediately. "
#                 f"Probable burst or major leakage event."
#             )
#             urgency      = 'URGENT' if clps > 0.5 else 'HIGH'
#             ward_action  = (
#                 f"A sudden pressure drop has been detected in {nm} (anomaly score: {clps:.3f}). "
#                 f"This may indicate a pipe burst or major leak. "
#                 f"SMC engineers have been dispatched. "
#                 f"Log any citizen reports of no water, flooding, or wet roads immediately."
#             )
#             ward_note    = f"Active pressure drop — CLPS: {clps:.3f}. Escalate if burst confirmed."
#             ward_escalate = True
#         elif sig == 'FPI':
#             eng_action = (
#                 f"FLOW-PRESSURE IMBALANCE in {nm} (CLPS={clps:.3f}). "
#                 f"FPI signal dominant — {int(clps * 15 + 10)}% unaccounted flow detected. "
#                 f"Probable pipe leakage near Pipe #{pipe_id}. "
#                 f"Dispatch inspection team to isolate and locate leak."
#             )
#             urgency      = 'HIGH'
#             ward_action  = (
#                 f"An abnormal flow imbalance has been detected in {nm} (anomaly score: {clps:.3f}). "
#                 f"More water is entering the zone than expected — possible pipe leakage. "
#                 f"Monitor citizen pressure complaints. "
#                 f"Escalate to engineering if multiple households report low pressure."
#             )
#             ward_note    = f"Flow-pressure imbalance — CLPS: {clps:.3f}. Monitor complaint volume."
#             ward_escalate = clps > 0.3
#         elif sig == 'NFA':
#             eng_action = (
#                 f"NIGHT FLOW ANOMALY in {nm} (CLPS={clps:.3f}). "
#                 f"NFA signal dominant — elevated flow detected between 01:00–04:00. "
#                 f"Inspect Pipe #{pipe_id} for unauthorized extraction or continuous leak. "
#                 f"Night patrol recommended 01:00–04:00."
#             )
#             urgency      = 'HIGH'
#             ward_action  = (
#                 f"Unusual water flow was detected in {nm} during off-peak hours (1–4 AM), "
#                 f"anomaly score: {clps:.3f}. "
#                 f"This may indicate unauthorized water extraction or a slow nighttime leak. "
#                 f"Escalate to engineering and log any complaints about low morning pressure."
#             )
#             ward_note    = f"Night flow anomaly — CLPS: {clps:.3f}. Possible unauthorized extraction."
#             ward_escalate = True
#         else:  # DDI — demand deviation
#             eng_action = (
#                 f"DEMAND DEVIATION in {nm} (CLPS={clps:.3f}). "
#                 f"DDI signal dominant — actual consumption deviates {int(clps*20+5)}% "
#                 f"from expected pattern. Check valve status on Pipe #{pipe_id} "
#                 f"and verify meter readings."
#             )
#             urgency      = 'MODERATE'
#             ward_action  = (
#                 f"A demand deviation anomaly has been detected in {nm} "
#                 f"(anomaly score: {clps:.3f}). "
#                 f"Actual water consumption is significantly different from the expected pattern. "
#                 f"Check for valve misalignment, meter issues, or unusual consumption in the ward. "
#                 f"If citizens are reporting low or no water, escalate to the engineering control room."
#             )
#             ward_note    = f"Demand deviation — CLPS: {clps:.3f}. Check valve status and meters."
#             ward_escalate = False

#         # ── Write engineer rec ────────────────────────────────────────
#         conn.execute(text("""
#             INSERT INTO engineer_recs
#               (zone_id, trigger_type, action_text, valve_id, pipe_id,
#                pressure_delta, urgency, estimated_hei_gain, node_coords)
#             VALUES
#               (:z, 'B_leak', :act, :vid, :pid, :dp, :urg, 0.0, :coords)
#         """), {
#             'z':      zone_id,
#             'act':    eng_action,
#             'vid':    f"V-{zone_id.upper().replace('_','')}",
#             'pid':    pipe_id,
#             'dp':     round(-clps * 12, 2),
#             'urg':    urgency,
#             'coords': json.dumps(ZONE_COORDS.get(zone_id, {'lat': 17.68, 'lon': 75.91})),
#         })
#         eng_count += 1

#         # ── Write ward rec — plain language, linked to the same alert ─
#         complaint_count = 0
#         try:
#             res = conn.execute(
#                 text("SELECT COUNT(*) FROM citizen_complaints WHERE zone_id=:z AND status='open'"),
#                 {'z': zone_id}
#             ).scalar()
#             complaint_count = int(res or 0)
#         except Exception:
#             pass

#         conn.execute(text("""
#             INSERT INTO ward_recs
#               (zone_id, trigger_type, action_text, escalation_flag,
#                service_reliability_note, complaint_count)
#             VALUES (:z, 'B_leak', :act, :esc, :note, :cc)
#         """), {
#             'z':    zone_id,
#             'act':  ward_action,
#             'esc':  ward_escalate,
#             'note': ward_note,
#             'cc':   complaint_count,
#         })
#         ward_count += 1

#     return eng_count, ward_count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER C — Burst Rules (→ engineer_recs + commissioner_recs)
# #  Uses V6 top-10 burst-risk segments.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_c_burst(burst_segs, conn):
#     """Writes high/moderate burst risk to engineer_recs and one summary to commissioner_recs."""
#     eng_count = 0
#     high_count = 0
#     moderate_count = 0
#     high_segments = []

#     for s in burst_segs:
#         pss        = float(s.get('pss', 0) or 0)
#         seg_id     = str(s.get('segment_id', '?'))
#         material   = s.get('material', 'Unknown')
#         age        = s.get('age', s.get('assumed_age', '?'))
#         risk_level = s.get('risk_level', 'MODERATE')
#         dom_factor = s.get('dominant_factor', 'unknown')
#         lat        = float(s.get('start_lat', s.get('lat_start', 17.68)) or 17.68)
#         lon        = float(s.get('start_lon', s.get('lon_start', 75.91)) or 75.91)

#         if pss < 0.40:
#             continue  # LOW risk — skip

#         if risk_level == 'HIGH' or pss >= 0.75:
#             action = (
#                 f"HIGH BURST RISK: Pipe Segment #{seg_id} (PSS = {pss:.3f}). "
#                 f"Material: {material}, Age: ~{age} years. "
#                 f"Dominant stress factor: {dom_factor}. "
#                 f"URGENT: Schedule inspection and consider pre-emptive replacement. "
#                 f"Failure probability elevated — do not delay beyond 30 days."
#             )
#             urgency = 'URGENT'
#             high_count += 1
#             high_segments.append(f"#{seg_id} ({material}, PSS={pss:.2f})")
#         else:
#             action = (
#                 f"MODERATE BURST RISK: Pipe Segment #{seg_id} (PSS = {pss:.3f}). "
#                 f"Material: {material}, Age: ~{age} years. "
#                 f"Dominant factor: {dom_factor}. "
#                 f"Schedule maintenance inspection within 60–90 days."
#             )
#             urgency = 'MODERATE'
#             moderate_count += 1

#         conn.execute(text("""
#             INSERT INTO engineer_recs
#               (zone_id, trigger_type, action_text, valve_id, pipe_id,
#                pressure_delta, urgency, estimated_hei_gain, node_coords)
#             VALUES
#               (:z, 'C_burst', :act, :vid, :pid, 0.0, :urg, 0.0, :coords)
#         """), {
#             'z':      '',   # burst risk is network-wide, not zone-specific
#             'act':    action,
#             'vid':    '',
#             'pid':    seg_id,
#             'urg':    urgency,
#             'coords': json.dumps({'lat': lat, 'lon': lon}),
#         })
#         eng_count += 1

#     # ── Commissioner rec: budget flag ─────────────────────────────
#     if burst_segs:
#         total_risk = high_count + moderate_count
#         budget_flag = high_count > 0
#         city_summary = (
#             f"Infrastructure risk assessment complete. "
#             f"{high_count} pipe segment(s) at HIGH burst risk. "
#             f"{moderate_count} at MODERATE risk. "
#             f"Total segments requiring attention: {total_risk}."
#         )
#         worst_segs_str = ', '.join(high_segments[:3]) if high_segments else 'None'
#         comm_action = (
#             f"Pipe inspection and replacement budget allocation required for "
#             f"{high_count} HIGH-risk segment(s): {worst_segs_str}. "
#             f"Estimated NRW reduction if addressed: 4–8%. "
#             f"Risk of emergency burst events if deferred."
#         )
#         conn.execute(text("""
#             INSERT INTO commissioner_recs
#               (city_summary, worst_zones, budget_flag, theft_summary,
#                resolution_rate, trigger_type)
#             VALUES (:cs, :wz, :bf, :ts, 0.0, 'C_burst')
#         """), {
#             'cs': city_summary,
#             'wz': json.dumps(high_segments[:5]),
#             'bf': budget_flag,
#             'ts': 'No theft data — V13 (Phase 4c)',
#         })

#     return eng_count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER D — Citizen Advisory (→ citizen_recs ONLY)
# #  Runs for every zone, every cycle.
# #  CRITICAL: NO infrastructure coords, valve IDs, or pipe segment data.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_d_citizen(zones, conn):
#     """
#     Writes to citizen_recs — plain language, no technical infrastructure data.
#     """
#     count = 0
#     for z in zones:
#         zone_id = z.get('zone_id', '')
#         hei     = float(z.get('hei', 0) or 0)
#         status  = z.get('status', 'equitable')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)

#         if status == 'severe':
#             supply_status = 'Intermittent'
#             advisory = (
#                 f"Water supply in {nm} may be reduced at some households. "
#                 f"The municipal team is actively working to restore normal pressure. "
#                 f"Store water if possible. Expected restoration: within 2-4 supply cycles."
#             )
#             guidance = (
#                 f"If you are experiencing no water or very low pressure, "
#                 f"please file a complaint below. Include your landmark and contact number."
#             )
#             restoration = "Within 2-4 supply cycles (6-24 hours)"
#         elif status == 'moderate':
#             supply_status = 'Normal'
#             advisory = (
#                 f"Water supply in {nm} is operating at near-normal levels. "
#                 f"Some households may experience slightly reduced pressure during peak hours. "
#                 f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM."
#             )
#             guidance = (
#                 f"If pressure seems lower than usual, wait for the next supply cycle. "
#                 f"Persistent issues? File a complaint below."
#             )
#             restoration = "Currently operational — monitoring in progress"
#         elif status == 'over':
#             supply_status = 'Normal'
#             advisory = (
#                 f"Water supply in {nm} is operating normally. "
#                 f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM."
#             )
#             guidance = (
#                 f"If you notice unusually high water flow or pipe vibration, "
#                 f"report it below. The municipal team is monitoring pressure levels."
#             )
#             restoration = "No disruption expected"
#         else:
#             supply_status = 'Normal'
#             advisory = (
#                 f"Water supply in {nm} is operating normally (HEI = {hei:.2f}). "
#                 f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM. "
#                 f"Ensure your overhead tank is filled during supply hours."
#             )
#             guidance = (
#                 f"For complaints about water quality, quantity, or billing, "
#                 f"use the form below. The municipal team responds within 24 hours."
#             )
#             restoration = "No disruption — normal operation"

#         conn.execute(text("""
#             INSERT INTO citizen_recs
#               (zone_id, supply_status, advisory_text,
#                complaint_guidance, estimated_restoration)
#             VALUES (:z, :ss, :adv, :guide, :rest)
#         """), {
#             'z':     zone_id,
#             'ss':    supply_status,
#             'adv':   advisory,
#             'guide': guidance,
#             'rest':  restoration,
#         })
#         count += 1

#     return count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER A (COMMISSIONER SUMMARY)
# #  Separate from per-zone equity — city-wide summary for commissioner.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_a_commissioner(zones, conn):
#     """Writes city-wide equity summary to commissioner_recs."""
#     if not zones:
#         return 0

#     heis = [float(z.get('hei', 0) or 0) for z in zones]
#     cwei = sum(heis) / len(heis) if heis else 0
#     severe_zones  = [z['zone_id'] for z in zones if z.get('status') == 'severe']
#     moderate_zones = [z['zone_id'] for z in zones if z.get('status') == 'moderate']
#     over_zones    = [z['zone_id'] for z in zones if z.get('status') == 'over']

#     worst = sorted(zones, key=lambda z: float(z.get('hei', 0) or 0))[:3]
#     worst_ids = [z['zone_id'] for z in worst]

#     if cwei >= 0.85:
#         status_label = 'EQUITABLE'
#     elif cwei >= 0.70:
#         status_label = 'MODERATE IMBALANCE'
#     else:
#         status_label = 'SEVERE INEQUITY'

#     city_summary = (
#         f"City-Wide Equity Index (CWEI): {cwei:.3f} — {status_label}. "
#         f"Monitoring {len(zones)} zones. "
#         f"{len(severe_zones)} zone(s) in SEVERE inequity, "
#         f"{len(moderate_zones)} in MODERATE imbalance, "
#         f"{len(over_zones)} over-pressurized. "
#         f"Estimated NRW: 18% (baseline)."
#     )

#     worst_detail = ', '.join([
#         f"{ZONE_NAMES.get(z,'?')} (HEI={float(next((x.get('hei',0) for x in zones if x['zone_id']==z), 0)):.3f})"
#         for z in worst_ids
#     ])

#     action = (
#         f"City equity status requires {'immediate attention' if severe_zones else 'routine monitoring'}. "
#         f"Priority zones: {worst_detail if worst_detail else 'None'}. "
#         f"{'URGENT: allocate field teams to ' + str(len(severe_zones)) + ' severe zone(s).' if severe_zones else 'Continue current operations.'}"
#     )

#     conn.execute(text("""
#         INSERT INTO commissioner_recs
#           (city_summary, worst_zones, budget_flag, theft_summary,
#            resolution_rate, trigger_type)
#         VALUES (:cs, :wz, :bf, :ts, :rr, 'A_equity')
#     """), {
#         'cs': city_summary + ' ' + action,
#         'wz': json.dumps(worst_ids),
#         'bf': len(severe_zones) > 0,
#         'ts': 'Theft detection (V13) coming in Phase 4c.',
#         'rr': 0.0,
#     })

#     return 1


# # ══════════════════════════════════════════════════════════════════════
# #  MAIN ENTRY POINT
# # ══════════════════════════════════════════════════════════════════════

# def run_v7():
#     from backend.database import engine
#     """
#     Main V7 function. Can be called:
#     - Directly: python scripts/v7_recommendations.py
#     - By APScheduler: every 5 minutes from backend/app.py
#     """
#     print('=' * 62)
#     print('  V7 · Role-Partitioned Recommendation Engine')
#     print('=' * 62)

#     # ── 1. Load analytics data ────────────────────────────────────
#     zones, alerts_all, burst_segs = load_analytics_data()
#     pipes_df, nodes_df, infra_df = load_network_data()

#     print(f'  Loaded: {len(zones)} zones, '
#           f'{sum(len(v) for v in alerts_all.values() if isinstance(v, list))} alerts, '
#           f'{len(burst_segs)} burst segments')

#     if not zones:
#         print('  [ABORT] No zone data. Run V4 first.')
#         return

#     # ── 2. Build influence map ────────────────────────────────────
#     print('  Building influence map...', end=' ')
#     influence = build_influence_map(pipes_df, nodes_df, infra_df)
#     print(f'{len(influence)} zones mapped')

#     # ── 3. Write to DB ────────────────────────────────────────────
#     print('  Running 5 triggers...')
#     eng_total = ward_total = comm_total = cit_total = 0

#     try:
#         with engine.connect() as conn:
#             # Clear records older than 24h
#             clear_old_recs(conn)

#             # Trigger A — Equity (engineer + ward + commissioner)
#             ec, wc = trigger_a_equity(zones, influence, conn)
#             eng_total  += ec
#             ward_total += wc
#             comm_total += trigger_a_commissioner(zones, conn)
#             # Citizen advisories (Trigger D runs alongside A)
#             cit_total  += trigger_d_citizen(zones, conn)
#             print(f'  [A] Equity  → {ec} engineer, {wc} ward, 1 commissioner, {cit_total} citizen')

#             # Trigger B — Leak (engineer + ward)  ← NOW WRITES WARD RECS TOO
#             bc, bwc = trigger_b_leak(alerts_all, influence, conn)
#             eng_total  += bc
#             ward_total += bwc
#             print(f'  [B] Leak    → {bc} engineer_recs, {bwc} ward_recs')

#             # Trigger C — Burst (engineer + commissioner)
#             cc = trigger_c_burst(burst_segs, conn)
#             eng_total  += cc
#             comm_total += 1 if burst_segs else 0
#             print(f'  [C] Burst   → {cc} engineer, 1 commissioner_recs')

#             print(f'  [D] Citizen → already written above ({cit_total} rows)')

#             # Log the run
#             conn.execute(text("""
#                 INSERT INTO v7_run_log
#                   (status, zones_processed, recs_generated,
#                    engineer_count, ward_count, commissioner_count, citizen_count)
#                 VALUES ('success', :zp, :rg, :ec, :wc, :cc, :cit)
#             """), {
#                 'zp':  len(zones),
#                 'rg':  eng_total + ward_total + comm_total + cit_total,
#                 'ec':  eng_total,
#                 'wc':  ward_total,
#                 'cc':  comm_total,
#                 'cit': cit_total,
#             })
#             conn.commit()
#             print('  [DB] Committed to PostgreSQL.')

#             # ── 4. Also write JSON cache so file-based router can read it ──
#             # Read back what we just wrote and save to outputs/v7_recommendations.json
#             _write_json_cache(conn, zones, alerts_all, burst_segs, influence)

#     except Exception as e:
#         print(f'  [WARN] DB write failed: {e}')
#         print('  Falling back to JSON-only mode (no PostgreSQL required).')
#         # Write JSON directly from in-memory data (no DB needed)
#         _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence)

#     total = eng_total + ward_total + comm_total + cit_total
#     print(f'\n  ✅ V7 complete. Total recs generated: {total}')
#     print(f'     engineer_recs    : {eng_total}')
#     print(f'     ward_recs        : {ward_total}')
#     print(f'     commissioner_recs: {comm_total}')
#     print(f'     citizen_recs     : {cit_total}')
#     print('=' * 62)


# # ══════════════════════════════════════════════════════════════════════
# #  JSON CACHE HELPERS
# #  Write outputs/v7_recommendations.json so the fast router endpoint
# #  can serve recs without hitting the DB on every request.
# # ══════════════════════════════════════════════════════════════════════

# def _write_json_cache(conn, zones, alerts_all, burst_segs, influence):
#     """
#     Read what was just written to DB and dump it all to the JSON cache.
#     Called after a successful DB commit inside run_v7.
#     """
#     try:
#         eng_rows  = conn.execute(text(
#             "SELECT zone_id, trigger_type, action_text, valve_id, pipe_id, "
#             "urgency, estimated_hei_gain FROM engineer_recs ORDER BY created_at DESC LIMIT 100"
#         )).fetchall()
#         ward_rows = conn.execute(text(
#             "SELECT zone_id, trigger_type, action_text, escalation_flag, "
#             "service_reliability_note, complaint_count FROM ward_recs ORDER BY created_at DESC LIMIT 100"
#         )).fetchall()
#         comm_rows = conn.execute(text(
#             "SELECT city_summary, worst_zones, budget_flag, theft_summary, "
#             "resolution_rate, trigger_type FROM commissioner_recs ORDER BY created_at DESC LIMIT 20"
#         )).fetchall()
#         cit_rows  = conn.execute(text(
#             "SELECT zone_id, supply_status, advisory_text, complaint_guidance, "
#             "estimated_restoration FROM citizen_recs ORDER BY zone_id, created_at DESC LIMIT 50"
#         )).fetchall()

#         data = {
#             "engineer_recs": [
#                 {"zone_id": r[0] or '', "trigger_type": r[1] or '', "action_text": r[2] or '',
#                  "valve_id": r[3] or '', "pipe_id": r[4] or '', "urgency": r[5] or 'LOW',
#                  "estimated_hei_gain": float(r[6] or 0)}
#                 for r in eng_rows
#             ],
#             "ward_recs": [
#                 {"zone_id": r[0] or '', "trigger_type": r[1] or '', "action_text": r[2] or '',
#                  "escalation_flag": bool(r[3]), "service_reliability_note": r[4] or '',
#                  "complaint_count": int(r[5] or 0)}
#                 for r in ward_rows
#             ],
#             "commissioner_recs": [
#                 {"city_summary": r[0] or '', "worst_zones": _safe_json_loads(r[1]),
#                  "budget_flag": bool(r[2]), "theft_summary": r[3] or '',
#                  "resolution_rate": float(r[4] or 0), "trigger_type": r[5] or ''}
#                 for r in comm_rows
#             ],
#             "citizen_recs": [
#                 {"zone_id": r[0] or '',
#                  "zone_name": "Zone {}".format((r[0] or '').replace('zone_', '')),
#                  "supply_status": r[1] or 'Normal', "advisory_text": r[2] or '',
#                  "complaint_guidance": r[3] or '', "estimated_restoration": r[4] or 'N/A'}
#                 for r in cit_rows
#             ],
#             "updated_at": datetime.now().isoformat(),
#         }
#         _save_json(data)
#         print(f'  [JSON] Cache written: {len(data["engineer_recs"])} eng, '
#               f'{len(data["ward_recs"])} ward, '
#               f'{len(data["commissioner_recs"])} comm, '
#               f'{len(data["citizen_recs"])} citizen')
#     except Exception as e:
#         print(f'  [WARN] JSON cache write (from DB) failed: {e}')
#         # Fall back to generating from memory
#         _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence)


# def _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence):
#     """
#     Build and write the JSON cache entirely from in-memory analytics data.
#     Used when PostgreSQL is unavailable (dev/file mode).
#     This is the same logic as the triggers but writes to a dict instead of DB.
#     """
#     engineer_recs    = []
#     ward_recs        = []
#     commissioner_recs = []
#     citizen_recs     = []

#     # ── Trigger A: Equity ─────────────────────────────────────────
#     for z in zones:
#         zone_id = z.get('zone_id', '')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)
#         hei     = float(z.get('hei', 0) or 0)
#         status  = z.get('status', 'equitable')
#         infl    = influence.get(zone_id, {})
#         pipe_id = infl.get('control_pipe_id', f'V-{zone_id.upper().replace("_","")}')

#         if status == 'severe':
#             gain = round(0.85 - hei, 3)
#             engineer_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity", "urgency": "URGENT",
#                 "valve_id": pipe_id, "pipe_id": "", "estimated_hei_gain": gain,
#                 "action_text": (
#                     f"[{nm}] HEI critically low at {hei:.3f} (target ≥ 0.85). "
#                     f"Increase ESR outlet pressure 10–15% via control pipe {pipe_id}. "
#                     f"Estimated HEI gain: +{gain:.3f}. Dispatch field team for verification."
#                 ),
#             })
#             ward_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity",
#                 "escalation_flag": True, "complaint_count": 0,
#                 "action_text": (
#                     f"{nm} supply is severely inequitable (HEI = {hei:.3f}). "
#                     f"Citizens in tail-end areas may receive little or no water. "
#                     f"Escalate to engineering control room immediately."
#                 ),
#                 "service_reliability_note": f"HEI: {hei:.3f} — Severe. Tail-end households affected.",
#             })
#         elif status == 'moderate':
#             engineer_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity", "urgency": "MODERATE",
#                 "valve_id": pipe_id, "pipe_id": "", "estimated_hei_gain": round(0.85 - hei, 3),
#                 "action_text": (
#                     f"[{nm}] HEI is {hei:.3f} — moderate imbalance. "
#                     f"Review valve settings on distribution mains."
#                 ),
#             })
#             ward_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity",
#                 "escalation_flag": False, "complaint_count": 0,
#                 "action_text": (
#                     f"{nm} shows moderate pressure imbalance (HEI = {hei:.3f}). "
#                     f"Some tail-end households may face reduced pressure during peak hours. "
#                     f"Monitor complaints and report persistent issues to engineering."
#                 ),
#                 "service_reliability_note": f"HEI: {hei:.3f} — Moderate. Monitor peak-hour supply.",
#             })

#         # Citizen advisory per zone
#         if status == 'severe':
#             cit_status, cit_adv = 'Intermittent', (
#                 f"Water supply in {nm} may be limited. SMC engineers are working to restore pressure. "
#                 f"Please store water during supply hours.")
#             cit_guid = "If you have no water, submit a complaint using the form below."
#             cit_rest = "Engineers are working on it. Check back in 2–4 hours."
#         elif status == 'moderate':
#             cit_status, cit_adv = 'Normal', (
#                 f"Water supply in {nm} is near-normal. Some households may see slightly reduced "
#                 f"pressure during peak hours (6–8 AM, 5–8 PM).")
#             cit_guid = "Persistent low pressure? File a complaint below."
#             cit_rest = "Currently operational — monitoring in progress"
#         else:
#             cit_status, cit_adv = 'Normal', (
#                 f"Water supply in {nm} is operating normally (HEI = {hei:.2f}). "
#                 f"Supply window: 6–8 AM and 5–8 PM. Fill overhead tanks during supply hours.")
#             cit_guid = "For any supply issues, submit a complaint using the form below."
#             cit_rest = "No disruption — normal operation"

#         citizen_recs.append({
#             "zone_id": zone_id,
#             "zone_name": "Zone {}".format(zone_id.replace('zone_', '')),
#             "supply_status": cit_status, "advisory_text": cit_adv,
#             "complaint_guidance": cit_guid, "estimated_restoration": cit_rest,
#         })

#     # ── Trigger B: Leak/Anomaly alerts ────────────────────────────
#     if isinstance(alerts_all, dict):
#         alerts = alerts_all.get('baseline', alerts_all.get('leak', []))
#     else:
#         alerts = alerts_all if isinstance(alerts_all, list) else []

#     for a in alerts:
#         zone_id = a.get('zone_id', '')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)
#         clps    = float(a.get('clps', 0) or 0)
#         sig     = a.get('dominant_signal', 'DDI')
#         infl    = influence.get(zone_id, {})
#         pipe_id = infl.get('control_pipe_id', f'V-{zone_id.upper().replace("_","")}')

#         if clps < 0.05:
#             continue

#         sig_map = {
#             'PDR_n': ('URGENT' if clps > 0.5 else 'HIGH',
#                       f"SUDDEN PRESSURE DROP in {nm} (CLPS={clps:.3f}). Dispatch team to Pipe #{pipe_id}.",
#                       f"Sudden pressure drop detected — possible burst. Log flooding reports immediately.", True),
#             'FPI':   ('HIGH',
#                       f"FLOW-PRESSURE IMBALANCE in {nm} (CLPS={clps:.3f}). Probable leakage near Pipe #{pipe_id}.",
#                       f"Flow imbalance detected — possible pipe leakage. Monitor complaint volume.", clps > 0.3),
#             'NFA':   ('HIGH',
#                       f"NIGHT FLOW ANOMALY in {nm} (CLPS={clps:.3f}). Inspect Pipe #{pipe_id} for unauthorized extraction.",
#                       f"Night flow anomaly — possible unauthorized extraction. Escalate to engineering.", True),
#             'DDI':   ('MODERATE',
#                       f"DEMAND DEVIATION in {nm} (CLPS={clps:.3f}). Check valve status on Pipe #{pipe_id}.",
#                       f"Demand deviation detected in {nm} (CLPS={clps:.3f}). "
#                       f"Actual consumption differs from expected pattern. "
#                       f"Check for valve misalignment or meter issues. "
#                       f"If citizens report low water, escalate to engineering.", False),
#         }
#         urg, eng_txt, ward_txt, escalate = sig_map.get(sig, sig_map['DDI'])

#         engineer_recs.append({
#             "zone_id": zone_id, "trigger_type": "B_leak", "urgency": urg,
#             "valve_id": f"V-{zone_id.upper().replace('_','')}", "pipe_id": pipe_id,
#             "estimated_hei_gain": 0.0, "action_text": eng_txt,
#         })
#         ward_recs.append({
#             "zone_id": zone_id, "trigger_type": "B_leak",
#             "escalation_flag": escalate, "complaint_count": 0,
#             "action_text": ward_txt,
#             "service_reliability_note": f"Active anomaly — CLPS: {clps:.3f} · Signal: {sig}.",
#         })

#     # ── Trigger C: Burst risk → engineer + commissioner ───────────
#     high_burst = [s for s in burst_segs if s.get('risk_level') == 'HIGH']
#     mod_burst  = [s for s in burst_segs if s.get('risk_level') == 'MODERATE']

#     for s in high_burst:
#         seg = s.get('segment_id', '?')
#         pss = float(s.get('pss', 0))
#         mat = s.get('material', 'Unknown')
#         age = s.get('age', '?')
#         dom = s.get('dominant_factor', 'PSI_n')
#         engineer_recs.append({
#             "zone_id": "", "trigger_type": "C_burst",
#             "urgency": "URGENT" if pss > 0.85 else "HIGH",
#             "valve_id": "", "pipe_id": str(seg), "estimated_hei_gain": 0.0,
#             "action_text": (
#                 f"HIGH BURST RISK: Pipe #{seg} (PSS={pss:.3f}). "
#                 f"Material: {mat}, Age: ~{age}yr. Factor: {dom}. "
#                 f"Schedule urgent inspection. Pre-emptive replacement if PSS > 0.90."
#             ),
#         })

#     if high_burst or mod_burst:
#         commissioner_recs.append({
#             "trigger_type": "C_burst",
#             "city_summary": (
#                 f"{len(high_burst)} pipe segment(s) at HIGH burst risk, {len(mod_burst)} MODERATE. "
#                 f"Capital expenditure for emergency pipe replacement should be considered."
#             ),
#             "worst_zones": [str(s.get('segment_id', '?')) for s in high_burst[:3]],
#             "budget_flag": len(high_burst) > 0,
#             "theft_summary": "Theft detection (V13) coming in Phase 4c.",
#             "resolution_rate": 0.0,
#         })

#     # ── Trigger A Commissioner summary ────────────────────────────
#     if zones:
#         heis  = [float(z.get('hei', 0) or 0) for z in zones]
#         cwei  = sum(heis) / len(heis)
#         worst = sorted(zones, key=lambda z: float(z.get('hei', 0) or 0))[:3]
#         sev   = [z for z in zones if z.get('status') == 'severe']
#         commissioner_recs.insert(0, {
#             "trigger_type": "A_equity",
#             "city_summary": (
#                 f"CWEI: {cwei:.3f} — "
#                 f"{'SEVERE INEQUITY' if cwei < 0.70 else 'MODERATE IMBALANCE' if cwei < 0.85 else 'EQUITABLE'}. "
#                 f"{len(sev)} zone(s) severe. "
#                 f"Priority: {', '.join(ZONE_NAMES.get(z['zone_id'], z['zone_id']) for z in worst[:2])}."
#             ),
#             "worst_zones": [z['zone_id'] for z in worst],
#             "budget_flag": len(sev) > 0,
#             "theft_summary": "Theft detection (V13) coming in Phase 4c.",
#             "resolution_rate": 0.0,
#         })

#     # Sort engineer recs by urgency
#     _order = {'URGENT': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}
#     engineer_recs.sort(key=lambda r: _order.get(r.get('urgency', 'LOW'), 3))

#     data = {
#         "engineer_recs":     engineer_recs,
#         "ward_recs":         ward_recs,
#         "commissioner_recs": commissioner_recs,
#         "citizen_recs":      citizen_recs,
#         "updated_at":        datetime.now().isoformat(),
#         "source":            "v7_memory",
#     }
#     _save_json(data)
#     print(f'  [JSON] Memory cache written: {len(engineer_recs)} eng, '
#           f'{len(ward_recs)} ward, '
#           f'{len(commissioner_recs)} comm, '
#           f'{len(citizen_recs)} citizen')


# def _save_json(data):
#     """Write data to outputs/v7_recommendations.json."""
#     out_path = os.path.join(OUTPUTS, 'v7_recommendations.json')
#     os.makedirs(OUTPUTS, exist_ok=True)
#     with open(out_path, 'w', encoding='utf-8') as f:
#         json.dump(data, f, indent=2)


# def _safe_json_loads(val):
#     if not val:
#         return []
#     try:
#         return json.loads(val)
#     except Exception:
#         return [val] if val else []


# if __name__ == '__main__':
#     run_v7()


# # Hydro-Equity Engine V7 — Recommendation Generator
# # Generates logical, data-driven recommendations based on live HEI/zone metrics.
# # NOT hardcoded — all recommendations derive from actual zone data.
# # """

# # from __future__ import annotations
# # import logging
# # from dataclasses import dataclass, field, asdict
# # from datetime import datetime, timezone
# # from typing import Optional
# # import uuid

# # logger = logging.getLogger(__name__)

# # # ── Thresholds (tunable) ──────────────────────────────────────────
# # HEI_EQUITABLE  = 0.85
# # HEI_MODERATE   = 0.70
# # CLPS_ANOMALY   = 0.12
# # PRESSURE_LOW   = 20.0   # m head — below this is critical
# # PRESSURE_HIGH  = 60.0   # m head — above this risks pipe burst
# # NRW_HIGH       = 0.20   # 20% NRW is high
# # COMPLAINT_HIGH = 10     # complaints/zone/day


# # @dataclass
# # class Recommendation:
# #     rec_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
# #     title: str = ""
# #     description: str = ""
# #     priority: str = "medium"         # critical | high | medium | low
# #     action_type: str = ""            # valve_adjust | pressure_boost | maintenance | investigation | policy
# #     zone_id: Optional[str] = None
# #     scope: str = "zone"              # zone | city | strategic
# #     is_strategic: bool = False
# #     estimated_hei_gain: Optional[float] = None
# #     estimated_impact: Optional[str] = None
# #     budget_estimate: Optional[str] = None
# #     generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

# #     def to_dict(self) -> dict:
# #         return {k: v for k, v in asdict(self).items() if v is not None}


# # def generate_recommendations(zones: list[dict], city_summary: dict | None = None) -> list[Recommendation]:
# #     """
# #     Generate logical recommendations from live zone data.
    
# #     zones: list of zone dicts with keys:
# #         zone_id, hei_score, status, avg_pressure_mh, open_complaints,
# #         clps_score, nrw_pct, zes_daily, demand_deviation, pipe_age_avg
    
# #     city_summary: optional dict with city-level aggregates
# #     """
# #     recs: list[Recommendation] = []

# #     if not zones:
# #         logger.warning("V7: No zone data provided — skipping recommendation generation")
# #         return recs

# #     for zone in zones:
# #         zone_id      = zone.get("zone_id", "unknown")
# #         hei          = float(zone.get("hei_score", 1.0))
# #         status       = zone.get("status", "equitable")
# #         pressure     = zone.get("avg_pressure_mh") or zone.get("pressure_mh")
# #         complaints   = zone.get("open_complaints", 0) or 0
# #         clps         = zone.get("clps_score") or zone.get("demand_deviation", 0.0)
# #         nrw          = zone.get("nrw_pct", 0.0) or 0.0
# #         pipe_age     = zone.get("pipe_age_avg")
# #         supply_status = zone.get("supply_status", "normal")

# #         # ── 1. Severe HEI — critical action needed ──
# #         if hei < HEI_MODERATE:
# #             gain = round(HEI_MODERATE - hei + 0.05, 3)
# #             recs.append(Recommendation(
# #                 title=f"Critical Equity Intervention — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"{zone_id.replace('_',' ').title()} has a critically low HEI of {hei:.3f} (threshold: {HEI_MODERATE}). "
# #                     f"Immediate review of inlet valve settings and distribution scheduling is required. "
# #                     f"Consider emergency pressure boost and reallocation from adjacent high-equity zones."
# #                 ),
# #                 priority="critical",
# #                 action_type="valve_adjust",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_hei_gain=gain,
# #                 estimated_impact=f"HEI improvement of +{gain:.3f} expected within 48h"
# #             ))

# #         # ── 2. Moderate HEI — scheduled intervention ──
# #         elif hei < HEI_EQUITABLE:
# #             gain = round(HEI_EQUITABLE - hei + 0.02, 3)
# #             recs.append(Recommendation(
# #                 title=f"Pressure Rebalancing — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"HEI of {hei:.3f} in {zone_id} is below equitable threshold ({HEI_EQUITABLE}). "
# #                     f"Scheduled pressure rebalancing recommended. Check valve positions on main inlet "
# #                     f"and review ZES scheduling to improve supply uniformity."
# #                 ),
# #                 priority="high",
# #                 action_type="pressure_boost",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_hei_gain=gain,
# #                 estimated_impact=f"Projected HEI gain: +{gain:.3f}"
# #             ))

# #         # ── 3. Low pressure ──
# #         if pressure is not None:
# #             pressure = float(pressure)
# #             if pressure < PRESSURE_LOW:
# #                 recs.append(Recommendation(
# #                     title=f"Low Pressure Alert — {zone_id.replace('_',' ').title()}",
# #                     description=(
# #                         f"Average pressure in {zone_id} is {pressure:.1f}m head, below the minimum threshold "
# #                         f"of {PRESSURE_LOW}m. This causes supply inadequacy at tail-end connections. "
# #                         f"Check for blockages, partially-closed valves, or ESR level issues."
# #                     ),
# #                     priority="high" if pressure < 15 else "medium",
# #                     action_type="investigation",
# #                     zone_id=zone_id,
# #                     scope="zone",
# #                     estimated_impact=f"Restoring to {PRESSURE_LOW+5:.0f}m will improve {zone_id} coverage"
# #                 ))

# #             elif pressure > PRESSURE_HIGH:
# #                 recs.append(Recommendation(
# #                     title=f"High Pressure Risk — {zone_id.replace('_',' ').title()}",
# #                     description=(
# #                         f"Pressure in {zone_id} is {pressure:.1f}m head, exceeding safe limit of {PRESSURE_HIGH}m. "
# #                         f"Risk of pipe stress and burst events. Recommend installing or adjusting pressure "
# #                         f"reducing valve (PRV) at zone inlet."
# #                     ),
# #                     priority="high",
# #                     action_type="valve_adjust",
# #                     zone_id=zone_id,
# #                     scope="zone",
# #                     estimated_impact="Reduces burst risk and NRW"
# #                 ))

# #         # ── 4. CLPS anomaly (Demand Deviation Index) ──
# #         if clps is not None and abs(float(clps)) > CLPS_ANOMALY:
# #             clps_val = float(clps)
# #             direction = "over-consumption" if clps_val > 0 else "under-supply"
# #             recs.append(Recommendation(
# #                 title=f"DDI Anomaly Investigation — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"Demand Deviation Index (CLPS) of {clps_val:.3f} in {zone_id} exceeds anomaly threshold "
# #                     f"(±{CLPS_ANOMALY}). This indicates potential {direction}. "
# #                     f"Verify valve status, check for unauthorised connections, and audit meter readings."
# #                 ),
# #                 priority="medium",
# #                 action_type="investigation",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_impact="Resolving DDI reduces NRW and improves equity"
# #             ))

# #         # ── 5. High complaints ──
# #         if int(complaints) >= COMPLAINT_HIGH:
# #             recs.append(Recommendation(
# #                 title=f"High Complaint Volume — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"{zone_id} has {complaints} open complaints — above the threshold of {COMPLAINT_HIGH}. "
# #                     f"Prioritise field inspection to identify common issues. "
# #                     f"Common causes: intermittent supply, low pressure, or localised pipe deterioration."
# #                 ),
# #                 priority="medium",
# #                 action_type="maintenance",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_impact=f"Resolving {complaints} complaints improves citizen satisfaction"
# #             ))

# #         # ── 6. High NRW per zone ──
# #         if float(nrw) > NRW_HIGH:
# #             recs.append(Recommendation(
# #                 title=f"Elevated NRW — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"Non-Revenue Water (NRW) in {zone_id} is {nrw*100:.1f}%, exceeding the {NRW_HIGH*100:.0f}% "
# #                     f"threshold. Conduct distribution loss audit: check meter accuracy, identify illegal "
# #                     f"connections, and inspect high-age pipes for leakage."
# #                 ),
# #                 priority="medium",
# #                 action_type="maintenance",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_impact=f"Reducing NRW to {NRW_HIGH*100:.0f}% recovers significant water volume"
# #             ))

# #         # ── 7. Pipe age (if available) ──
# #         if pipe_age is not None and float(pipe_age) > 25:
# #             recs.append(Recommendation(
# #                 title=f"Aging Infrastructure — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"Average pipe age in {zone_id} is {float(pipe_age):.0f} years, exceeding the "
# #                     f"recommended 25-year service life. Schedule condition assessment and phased replacement "
# #                     f"to prevent burst events and reduce NRW."
# #                 ),
# #                 priority="low",
# #                 action_type="maintenance",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 budget_estimate="To be determined by condition survey",
# #                 estimated_impact="Long-term NRW reduction and reliability improvement"
# #             ))

# #     # ── City-level strategic recommendations ──
# #     if city_summary:
# #         recs += _generate_strategic(zones, city_summary)

# #     # Sort: critical → high → medium → low, then by zone
# #     priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
# #     recs.sort(key=lambda r: (priority_order.get(r.priority, 4), r.zone_id or ""))
# #     logger.info(f"V7: Generated {len(recs)} recommendations for {len(zones)} zones")
# #     return recs


# # def _generate_strategic(zones: list[dict], city: dict) -> list[Recommendation]:
# #     """Generate city-level strategic recommendations."""
# #     recs = []
# #     city_hei     = float(city.get("city_equity_index", 1.0))
# #     severe_zones = [z for z in zones if float(z.get("hei_score", 1.0)) < HEI_MODERATE]
# #     moderate_zones = [z for z in zones if HEI_MODERATE <= float(z.get("hei_score", 1.0)) < HEI_EQUITABLE]
# #     city_nrw     = float(city.get("nrw_pct", 0.0) or 0.0)

# #     # Strategic: multiple severe zones
# #     if len(severe_zones) >= 2:
# #         zone_names = ", ".join(z["zone_id"].replace("_"," ").title() for z in severe_zones[:4])
# #         recs.append(Recommendation(
# #             title="City-Wide Equity Emergency — Multi-Zone Intervention",
# #             description=(
# #                 f"{len(severe_zones)} zones ({zone_names}) have HEI below {HEI_MODERATE}. "
# #                 f"This requires a coordinated city-wide intervention: emergency redistribution plan, "
# #                 f"temporary tanker supply to worst-affected areas, and expedited infrastructure audit. "
# #                 f"Commissioner approval required for resource reallocation."
# #             ),
# #             priority="critical",
# #             action_type="policy",
# #             scope="strategic",
# #             is_strategic=True,
# #             estimated_hei_gain=round((HEI_EQUITABLE - city_hei) * 0.6, 3),
# #             estimated_impact=f"Could bring {len(severe_zones)} zones to moderate status within 2 weeks"
# #         ))

# #     # Strategic: city NRW high
# #     if city_nrw > NRW_HIGH:
# #         recs.append(Recommendation(
# #             title="City-Wide NRW Reduction Programme",
# #             description=(
# #                 f"City-wide NRW of {city_nrw*100:.1f}% represents significant water loss. "
# #                 f"Recommend initiating a structured NRW reduction programme: district metered areas (DMA) "
# #                 f"setup, automated leak detection, and meter replacement on aged connections. "
# #                 f"Target: reduce NRW to below {NRW_HIGH*100:.0f}% within 12 months."
# #             ),
# #             priority="high",
# #             action_type="policy",
# #             scope="strategic",
# #             is_strategic=True,
# #             estimated_impact=f"Recovering {city_nrw*100 - NRW_HIGH*100:.1f}% NRW improves supply for all zones"
# #         ))

# #     # Strategic: equity gap between best and worst zone
# #     if len(zones) >= 2:
# #         sorted_by_hei = sorted(zones, key=lambda z: float(z.get("hei_score", 1.0)))
# #         worst = sorted_by_hei[0]
# #         best  = sorted_by_hei[-1]
# #         hei_gap = float(best.get("hei_score", 1.0)) - float(worst.get("hei_score", 1.0))
# #         if hei_gap > 0.30:
# #             recs.append(Recommendation(
# #                 title="Equity Gap Reduction — Zone Rebalancing Plan",
# #                 description=(
# #                     f"HEI gap of {hei_gap:.3f} between best zone ({best['zone_id']}, HEI {float(best.get('hei_score',1)):.3f}) "
# #                     f"and worst zone ({worst['zone_id']}, HEI {float(worst.get('hei_score',1)):.3f}) is significant. "
# #                     f"Recommend a zone rebalancing study: identify surplus capacity in high-HEI zones and "
# #                     f"establish systematic reallocation schedule to reduce gap to below 0.20."
# #                 ),
# #                 priority="high",
# #                 action_type="policy",
# #                 scope="strategic",
# #                 is_strategic=True,
# #                 estimated_hei_gain=round(hei_gap * 0.4, 3),
# #                 estimated_impact="Reduces inter-zone inequity, improves overall CWEI"
# #             ))

# #     return recs


# # def format_for_api(recs: list[Recommendation], zone_id: str | None = None) -> dict:
# #     """Format recommendations for API response, optionally filtered by zone."""
# #     filtered = recs
# #     if zone_id:
# #         # Include zone-specific + strategic recs
# #         filtered = [r for r in recs if r.zone_id == zone_id or r.is_strategic or r.scope in ('strategic','city')]

# #     return {
# #         "recommendations": [r.to_dict() for r in filtered],
# #         "total":     len(filtered),
# #         "generated_at": datetime.now(timezone.utc).isoformat(),
# #         "engine":    "V7"
# #     }


# """
# Hydro-Equity Engine — Phase 4b
# scripts/v7_recommendations.py
# V7 — Role-Partitioned Recommendation Engine

# Reads:
#   outputs/v4_zone_status.json   → zone HEI / status
#   outputs/v5_alerts.json        → CLPS alerts per scenario
#   outputs/v6_burst_top10.json   → top burst-risk pipe segments
#   Data/pipe_segments.csv        → for influence map (optional)
#   Data/nodes_with_elevation.csv → for influence map (optional)
#   Data/infrastructure_points.csv → for ESR locations (optional)

# Writes (PostgreSQL):
#   engineer_recs       → valve_id, pipe_id, urgency, HEI gain (Trigger A,B,C)
#   ward_recs           → plain-language escalation notes  (Trigger A)
#   commissioner_recs   → city summary, budget flag        (Trigger A,C)
#   citizen_recs        → supply status, advisory          (Trigger D)
#   v7_run_log          → run timestamp and counts

# Run manually:   python scripts/v7_recommendations.py
# Auto-scheduled: every 5 minutes via APScheduler in backend/app.py
# """

# import os, sys, json
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# import pandas as pd
# from datetime import datetime
# from sqlalchemy import text
# from backend.database import engine

# ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DATA    = os.path.join(ROOT, 'Data')
# OUTPUTS = os.path.join(ROOT, 'outputs')

# # ── Zone display name mapping ──────────────────────────────────────
# ZONE_NAMES = {
#     'zone_1': 'Zone 1', 'zone_2': 'Zone 2', 'zone_3': 'Zone 3',
#     'zone_4': 'Zone 4', 'zone_5': 'Zone 5', 'zone_6': 'Zone 6',
#     'zone_7': 'Zone 7', 'zone_8': 'Zone 8',
# }

# # Fallback ESR positions (used if infrastructure_points.csv missing)
# FALLBACK_ESRS = [
#     {'lat': 17.715, 'lon': 75.870, 'zone': 'zone_1'},  # NW — Khed/Sawaleshwar
#     {'lat': 17.709, 'lon': 75.928, 'zone': 'zone_2'},  # NE — Hipparge/NH65
#     {'lat': 17.688, 'lon': 75.948, 'zone': 'zone_3'},  # E  — Mulagaon/Sangali
#     {'lat': 17.692, 'lon': 75.890, 'zone': 'zone_4'},  # N-Central
#     {'lat': 17.673, 'lon': 75.910, 'zone': 'zone_5'},  # Central Solapur
#     {'lat': 17.669, 'lon': 75.858, 'zone': 'zone_6'},  # W  — Hiraj
#     {'lat': 17.652, 'lon': 75.895, 'zone': 'zone_7'},  # S-Central — Hotagi
#     {'lat': 17.641, 'lon': 75.932, 'zone': 'zone_8'},  # SE — Kumbhari
# ]

# # Zone centroid positions (for node_coords in recommendations)
# ZONE_COORDS = {
#     'zone_1': {'lat': 17.718, 'lon': 75.873},  # NW — Khed/Sawaleshwar
#     'zone_2': {'lat': 17.712, 'lon': 75.932},  # NE — Hipparge/NH65
#     'zone_3': {'lat': 17.690, 'lon': 75.952},  # E  — Mulagaon/Sangali
#     'zone_4': {'lat': 17.695, 'lon': 75.893},  # N-Central
#     'zone_5': {'lat': 17.675, 'lon': 75.913},  # Central Solapur
#     'zone_6': {'lat': 17.672, 'lon': 75.862},  # W  — Hiraj
#     'zone_7': {'lat': 17.655, 'lon': 75.898},  # S-Central — Hotagi
#     'zone_8': {'lat': 17.644, 'lon': 75.936},  # SE — Kumbhari
# }


# # ══════════════════════════════════════════════════════════════════════
# #  DATA LOADING
# # ══════════════════════════════════════════════════════════════════════

# def load_analytics_data():
#     """Load V4, V5, V6 outputs. Returns (zones, alerts, burst_segs)."""
#     zones = []
#     path = os.path.join(OUTPUTS, 'v4_zone_status.json')
#     if os.path.exists(path):
#         with open(path, encoding='utf-8') as f:
#             zones = json.load(f)
#     else:
#         print('  [WARN] v4_zone_status.json not found — run V4 first')

#     alerts_all = {}
#     path = os.path.join(OUTPUTS, 'v5_alerts.json')
#     if os.path.exists(path):
#         with open(path, encoding='utf-8') as f:
#             alerts_all = json.load(f)
#     else:
#         print('  [WARN] v5_alerts.json not found — run V5 first')

#     burst_segs = []
#     path = os.path.join(OUTPUTS, 'v6_burst_top10.json')
#     if os.path.exists(path):
#         with open(path, encoding='utf-8') as f:
#             burst_segs = json.load(f)
#     else:
#         print('  [WARN] v6_burst_top10.json not found — run V6 first')

#     return zones, alerts_all, burst_segs


# def load_network_data():
#     """Load pipe segments and nodes for influence map building."""
#     pipes_df = None
#     nodes_df = None
#     infra_df = None

#     p = os.path.join(DATA, 'pipe_segments.csv')
#     if os.path.exists(p):
#         pipes_df = pd.read_csv(p)

#     p = os.path.join(DATA, 'nodes_with_elevation.csv')
#     if os.path.exists(p):
#         nodes_df = pd.read_csv(p)

#     p = os.path.join(DATA, 'infrastructure_points.csv')
#     if os.path.exists(p):
#         infra_df = pd.read_csv(p)

#     return pipes_df, nodes_df, infra_df


# # ══════════════════════════════════════════════════════════════════════
# #  INFLUENCE MAP
# #  For each zone → find control pipe (largest-diameter cross-zone pipe)
# #  and nearest ESR node. Used to build specific valve/pipe recommendations.
# # ══════════════════════════════════════════════════════════════════════

# def build_influence_map(pipes_df, nodes_df, infra_df):
#     """
#     Returns dict: {zone_id: {control_pipe_id, esr_lat, esr_lon}}
#     Falls back gracefully if CSV files are unavailable.
#     """
#     influence = {}

#     # Initialize all known zones with defaults
#     for zone, coords in ZONE_COORDS.items():
#         influence[zone] = {
#             'control_pipe_id': 'V-' + zone.upper().replace('_', ''),
#             'esr_lat': coords['lat'],
#             'esr_lon': coords['lon'],
#         }

#     if pipes_df is None or nodes_df is None:
#         # No network data — use defaults
#         return influence

#     try:
#         # Build zone lookup: node_id → zone_id
#         node_zone = {}
#         for _, r in nodes_df.iterrows():
#             nid = str(int(r['node_id'])) if pd.notna(r['node_id']) else None
#             if nid:
#                 node_zone[nid] = str(r.get('zone_id', ''))

#         # Find cross-zone pipes (inlet pipes per zone)
#         zone_inlets = {}
#         valid = pipes_df.dropna(subset=['start_node_id', 'end_node_id']).copy()
#         for _, row in valid.iterrows():
#             try:
#                 zu = node_zone.get(str(int(row['start_node_id'])), '')
#                 zv = node_zone.get(str(int(row['end_node_id'])), '')
#                 if zu != zv and zv:
#                     if zv not in zone_inlets:
#                         zone_inlets[zv] = []
#                     zone_inlets[zv].append({
#                         'pipe_id':  str(int(row['segment_id'])),
#                         'diameter': float(row.get('diameter_m', 0.1) or 0.1),
#                     })
#             except (ValueError, TypeError):
#                 continue

#         # Control pipe = largest-diameter inlet pipe for each zone
#         for zone, inlets in zone_inlets.items():
#             if inlets:
#                 best = max(inlets, key=lambda x: x['diameter'])
#                 influence[zone] = {**influence.get(zone, {}), 'control_pipe_id': best['pipe_id']}

#         # ESR locations from infrastructure_points.csv
#         if infra_df is not None and len(infra_df) > 0:
#             esrs = infra_df[infra_df['feature_type'].isin(['storage_tank', 'water_source'])]
#             zone_nodes = nodes_df.copy()

#             for zone in influence:
#                 zn = zone_nodes[zone_nodes['zone_id'] == zone]
#                 if len(zn) == 0:
#                     continue
#                 clat = zn['lat'].mean()
#                 clon = zn['lon'].mean()
#                 if len(esrs) > 0:
#                     dists = ((esrs['lat'] - clat)**2 + (esrs['lon'] - clon)**2)**0.5
#                     nearest = esrs.iloc[dists.values.argmin()]
#                     influence[zone]['esr_lat'] = float(nearest['lat'])
#                     influence[zone]['esr_lon'] = float(nearest['lon'])

#     except Exception as e:
#         print(f'  [WARN] Influence map build error: {e} — using defaults')

#     return influence


# # ══════════════════════════════════════════════════════════════════════
# #  DATABASE HELPERS
# # ══════════════════════════════════════════════════════════════════════

# def clear_old_recs(conn):
#     """Remove recommendations older than 24 hours to keep tables clean."""
#     for tbl in ['engineer_recs', 'ward_recs', 'commissioner_recs', 'citizen_recs']:
#         try:
#             conn.execute(text(
#                 f"DELETE FROM {tbl} WHERE created_at < NOW() - INTERVAL '24 hours'"
#             ))
#         except Exception:
#             pass  # Table might not exist yet — safe to ignore


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER A — Equity Rules (→ ALL 4 channels)
# #  Fires for every zone, every run.
# #  Severity: severe < 0.70, moderate 0.70-0.85, equitable 0.85-1.30, over > 1.30
# # ══════════════════════════════════════════════════════════════════════

# def trigger_a_equity(zones, influence, conn):
#     """
#     Writes to engineer_recs, ward_recs, commissioner_recs, citizen_recs
#     based on zone HEI status.
#     Returns (eng_count, ward_count) written.
#     """
#     eng_count = ward_count = 0
#     severe_zones  = [z for z in zones if z.get('status') == 'severe']
#     moderate_zones = [z for z in zones if z.get('status') == 'moderate']
#     over_zones    = [z for z in zones if z.get('status') == 'over']

#     for z in zones:
#         zone_id  = z.get('zone_id', '')
#         hei      = float(z.get('hei', 0) or 0)
#         status   = z.get('status', 'equitable')
#         zm       = ZONE_COORDS.get(zone_id, {'lat': 17.675, 'lon': 75.913})
#         infl     = influence.get(zone_id, {})
#         pipe_id  = infl.get('control_pipe_id', 'unknown')
#         nm       = ZONE_NAMES.get(zone_id, zone_id)

#         # ── engineer_recs (Trigger A) ─────────────────────────────
#         if status == 'severe':
#             est_gain = round(min(0.85, hei + 0.22) - hei, 3)
#             action = (
#                 f"SEVERE inequity detected in {nm} (HEI = {hei:.3f}). "
#                 f"Increase ESR outlet pressure by 12–15% on Control Pipe #{pipe_id}. "
#                 f"Tail-end nodes receiving < 70% of zone average pressure. "
#                 f"Estimated HEI improvement: {hei:.3f} → {hei + est_gain:.3f} after adjustment."
#             )
#             urgency = 'URGENT'
#             delta = 8.0
#         elif status == 'moderate':
#             est_gain = round(min(0.85, hei + 0.08) - hei, 3)
#             action = (
#                 f"Moderate imbalance in {nm} (HEI = {hei:.3f}). "
#                 f"Review valve settings on Control Pipe #{pipe_id}. "
#                 f"Consider 5–8% pressure increase at ESR outlet. "
#                 f"Estimated improvement: {hei:.3f} → {hei + est_gain:.3f}."
#             )
#             urgency = 'MODERATE'
#             delta = 4.0
#             est_gain = 0.08
#         elif status == 'over':
#             est_gain = 0.0
#             action = (
#                 f"Over-pressurization in {nm} (HEI = {hei:.3f}). "
#                 f"Throttle Control Pipe #{pipe_id} by 10–15% to reduce excess pressure. "
#                 f"Risk of pipe burst — immediate valve adjustment recommended."
#             )
#             urgency = 'HIGH'
#             delta = -6.0
#         else:
#             est_gain = 0.0
#             action = (
#                 f"{nm} is within equitable range (HEI = {hei:.3f}). "
#                 f"No pressure adjustment required. Continue routine monitoring."
#             )
#             urgency = 'LOW'
#             delta = 0.0

#         conn.execute(text("""
#             INSERT INTO engineer_recs
#               (zone_id, trigger_type, action_text, valve_id, pipe_id,
#                pressure_delta, urgency, estimated_hei_gain, node_coords)
#             VALUES
#               (:z, 'A_equity', :act, :vid, :pid, :dp, :urg, :gain, :coords)
#         """), {
#             'z':      zone_id,
#             'act':    action,
#             'vid':    f"V-{zone_id.upper().replace('_','')}",
#             'pid':    pipe_id,
#             'dp':     delta,
#             'urg':    urgency,
#             'gain':   est_gain,
#             'coords': json.dumps(zm),
#         })
#         eng_count += 1

#         # ── ward_recs (Trigger A) ─────────────────────────────────
#         if status == 'severe':
#             escalation = True
#             ward_action = (
#                 f"{nm} water supply is critically below acceptable levels (HEI = {hei:.3f}). "
#                 f"Escalate to engineering team immediately for valve adjustment."
#             )
#             reliability_note = "Critical — pressure below minimum service threshold"
#         elif status == 'moderate':
#             escalation = True
#             ward_action = (
#                 f"{nm} supply is experiencing moderate pressure imbalance (HEI = {hei:.3f}). "
#                 f"Notify engineering team for review during next maintenance window."
#             )
#             reliability_note = "Moderate — some tail-end households may have reduced supply"
#         elif status == 'over':
#             escalation = False
#             ward_action = (
#                 f"{nm} supply pressure is above normal levels (HEI = {hei:.3f}). "
#                 f"Engineering team is managing pressure reduction. Monitor for complaints."
#             )
#             reliability_note = "High pressure — watch for burst pipe complaints"
#         else:
#             escalation = False
#             ward_action = (
#                 f"{nm} supply is operating normally (HEI = {hei:.3f}). "
#                 f"No action required. Continue routine complaint monitoring."
#             )
#             reliability_note = "Good — pressure within acceptable range"

#         # Count open complaints for this zone from DB
#         complaint_count = 0
#         try:
#             res = conn.execute(
#                 text("SELECT COUNT(*) FROM citizen_complaints WHERE zone_id=:z AND status='open'"),
#                 {'z': zone_id}
#             ).scalar()
#             complaint_count = int(res or 0)
#         except Exception:
#             pass

#         conn.execute(text("""
#             INSERT INTO ward_recs
#               (zone_id, trigger_type, action_text, escalation_flag,
#                service_reliability_note, complaint_count)
#             VALUES (:z, 'A_equity', :act, :esc, :note, :cc)
#         """), {
#             'z':    zone_id,
#             'act':  ward_action,
#             'esc':  escalation,
#             'note': reliability_note,
#             'cc':   complaint_count,
#         })
#         ward_count += 1

#     return eng_count, ward_count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER B — Leak Rules (→ engineer_recs + ward_recs)
# #  Uses 'baseline' alerts from V5 (most reliable scenario).
# #  Engineer gets technical dispatch instructions.
# #  Ward officer gets plain-language action linked to the same alert.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_b_leak(alerts_all, influence, conn):
#     """Writes to engineer_recs (technical) AND ward_recs (plain language) per alert."""
#     # Use baseline alerts — these are always present and represent real anomalies
#     if isinstance(alerts_all, dict):
#         alerts = alerts_all.get('baseline', alerts_all.get('leak', []))
#     else:
#         alerts = alerts_all if isinstance(alerts_all, list) else []

#     eng_count  = 0
#     ward_count = 0

#     for a in alerts:
#         zone_id = a.get('zone_id', '')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)
#         clps    = float(a.get('clps', 0) or 0)
#         sig     = a.get('dominant_signal', 'PDR_n')
#         infl    = influence.get(zone_id, {})
#         pipe_id = infl.get('control_pipe_id', 'unknown')

#         if clps < 0.05:
#             continue  # Skip trivial alerts

#         # ── Engineer rec: technical, signal-specific ──────────────────
#         if sig == 'PDR_n':
#             eng_action = (
#                 f"SUDDEN PRESSURE DROP in {nm} (CLPS={clps:.3f}). "
#                 f"PDR_n signal dominant — rapid pressure decay detected. "
#                 f"Dispatch field team to inspect Pipe #{pipe_id} immediately. "
#                 f"Probable burst or major leakage event."
#             )
#             urgency      = 'URGENT' if clps > 0.5 else 'HIGH'
#             ward_action  = (
#                 f"A sudden pressure drop has been detected in {nm} (anomaly score: {clps:.3f}). "
#                 f"This may indicate a pipe burst or major leak. "
#                 f"SMC engineers have been dispatched. "
#                 f"Log any citizen reports of no water, flooding, or wet roads immediately."
#             )
#             ward_note    = f"Active pressure drop — CLPS: {clps:.3f}. Escalate if burst confirmed."
#             ward_escalate = True
#         elif sig == 'FPI':
#             eng_action = (
#                 f"FLOW-PRESSURE IMBALANCE in {nm} (CLPS={clps:.3f}). "
#                 f"FPI signal dominant — {int(clps * 15 + 10)}% unaccounted flow detected. "
#                 f"Probable pipe leakage near Pipe #{pipe_id}. "
#                 f"Dispatch inspection team to isolate and locate leak."
#             )
#             urgency      = 'HIGH'
#             ward_action  = (
#                 f"An abnormal flow imbalance has been detected in {nm} (anomaly score: {clps:.3f}). "
#                 f"More water is entering the zone than expected — possible pipe leakage. "
#                 f"Monitor citizen pressure complaints. "
#                 f"Escalate to engineering if multiple households report low pressure."
#             )
#             ward_note    = f"Flow-pressure imbalance — CLPS: {clps:.3f}. Monitor complaint volume."
#             ward_escalate = clps > 0.3
#         elif sig == 'NFA':
#             eng_action = (
#                 f"NIGHT FLOW ANOMALY in {nm} (CLPS={clps:.3f}). "
#                 f"NFA signal dominant — elevated flow detected between 01:00–04:00. "
#                 f"Inspect Pipe #{pipe_id} for unauthorized extraction or continuous leak. "
#                 f"Night patrol recommended 01:00–04:00."
#             )
#             urgency      = 'HIGH'
#             ward_action  = (
#                 f"Unusual water flow was detected in {nm} during off-peak hours (1–4 AM), "
#                 f"anomaly score: {clps:.3f}. "
#                 f"This may indicate unauthorized water extraction or a slow nighttime leak. "
#                 f"Escalate to engineering and log any complaints about low morning pressure."
#             )
#             ward_note    = f"Night flow anomaly — CLPS: {clps:.3f}. Possible unauthorized extraction."
#             ward_escalate = True
#         else:  # DDI — demand deviation
#             eng_action = (
#                 f"DEMAND DEVIATION in {nm} (CLPS={clps:.3f}). "
#                 f"DDI signal dominant — actual consumption deviates {int(clps*20+5)}% "
#                 f"from expected pattern. Check valve status on Pipe #{pipe_id} "
#                 f"and verify meter readings."
#             )
#             urgency      = 'MODERATE'
#             ward_action  = (
#                 f"A demand deviation anomaly has been detected in {nm} "
#                 f"(anomaly score: {clps:.3f}). "
#                 f"Actual water consumption is significantly different from the expected pattern. "
#                 f"Check for valve misalignment, meter issues, or unusual consumption in the ward. "
#                 f"If citizens are reporting low or no water, escalate to the engineering control room."
#             )
#             ward_note    = f"Demand deviation — CLPS: {clps:.3f}. Check valve status and meters."
#             ward_escalate = False

#         # ── Write engineer rec ────────────────────────────────────────
#         conn.execute(text("""
#             INSERT INTO engineer_recs
#               (zone_id, trigger_type, action_text, valve_id, pipe_id,
#                pressure_delta, urgency, estimated_hei_gain, node_coords)
#             VALUES
#               (:z, 'B_leak', :act, :vid, :pid, :dp, :urg, 0.0, :coords)
#         """), {
#             'z':      zone_id,
#             'act':    eng_action,
#             'vid':    f"V-{zone_id.upper().replace('_','')}",
#             'pid':    pipe_id,
#             'dp':     round(-clps * 12, 2),
#             'urg':    urgency,
#             'coords': json.dumps(ZONE_COORDS.get(zone_id, {'lat': 17.675, 'lon': 75.913})),
#         })
#         eng_count += 1

#         # ── Write ward rec — plain language, linked to the same alert ─
#         complaint_count = 0
#         try:
#             res = conn.execute(
#                 text("SELECT COUNT(*) FROM citizen_complaints WHERE zone_id=:z AND status='open'"),
#                 {'z': zone_id}
#             ).scalar()
#             complaint_count = int(res or 0)
#         except Exception:
#             pass

#         conn.execute(text("""
#             INSERT INTO ward_recs
#               (zone_id, trigger_type, action_text, escalation_flag,
#                service_reliability_note, complaint_count)
#             VALUES (:z, 'B_leak', :act, :esc, :note, :cc)
#         """), {
#             'z':    zone_id,
#             'act':  ward_action,
#             'esc':  ward_escalate,
#             'note': ward_note,
#             'cc':   complaint_count,
#         })
#         ward_count += 1

#     return eng_count, ward_count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER C — Burst Rules (→ engineer_recs + commissioner_recs)
# #  Uses V6 top-10 burst-risk segments.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_c_burst(burst_segs, conn):
#     """Writes high/moderate burst risk to engineer_recs and one summary to commissioner_recs."""
#     eng_count = 0
#     high_count = 0
#     moderate_count = 0
#     high_segments = []

#     for s in burst_segs:
#         pss        = float(s.get('pss', 0) or 0)
#         seg_id     = str(s.get('segment_id', '?'))
#         material   = s.get('material', 'Unknown')
#         age        = s.get('age', s.get('assumed_age', '?'))
#         risk_level = s.get('risk_level', 'MODERATE')
#         dom_factor = s.get('dominant_factor', 'unknown')
#         lat        = float(s.get('start_lat', s.get('lat_start', 17.68)) or 17.68)
#         lon        = float(s.get('start_lon', s.get('lon_start', 75.91)) or 75.91)

#         if pss < 0.40:
#             continue  # LOW risk — skip

#         if risk_level == 'HIGH' or pss >= 0.75:
#             action = (
#                 f"HIGH BURST RISK: Pipe Segment #{seg_id} (PSS = {pss:.3f}). "
#                 f"Material: {material}, Age: ~{age} years. "
#                 f"Dominant stress factor: {dom_factor}. "
#                 f"URGENT: Schedule inspection and consider pre-emptive replacement. "
#                 f"Failure probability elevated — do not delay beyond 30 days."
#             )
#             urgency = 'URGENT'
#             high_count += 1
#             high_segments.append(f"#{seg_id} ({material}, PSS={pss:.2f})")
#         else:
#             action = (
#                 f"MODERATE BURST RISK: Pipe Segment #{seg_id} (PSS = {pss:.3f}). "
#                 f"Material: {material}, Age: ~{age} years. "
#                 f"Dominant factor: {dom_factor}. "
#                 f"Schedule maintenance inspection within 60–90 days."
#             )
#             urgency = 'MODERATE'
#             moderate_count += 1

#         conn.execute(text("""
#             INSERT INTO engineer_recs
#               (zone_id, trigger_type, action_text, valve_id, pipe_id,
#                pressure_delta, urgency, estimated_hei_gain, node_coords)
#             VALUES
#               (:z, 'C_burst', :act, :vid, :pid, 0.0, :urg, 0.0, :coords)
#         """), {
#             'z':      '',   # burst risk is network-wide, not zone-specific
#             'act':    action,
#             'vid':    '',
#             'pid':    seg_id,
#             'urg':    urgency,
#             'coords': json.dumps({'lat': lat, 'lon': lon}),
#         })
#         eng_count += 1

#     # ── Commissioner rec: budget flag ─────────────────────────────
#     if burst_segs:
#         total_risk = high_count + moderate_count
#         budget_flag = high_count > 0
#         city_summary = (
#             f"Infrastructure risk assessment complete. "
#             f"{high_count} pipe segment(s) at HIGH burst risk. "
#             f"{moderate_count} at MODERATE risk. "
#             f"Total segments requiring attention: {total_risk}."
#         )
#         worst_segs_str = ', '.join(high_segments[:3]) if high_segments else 'None'
#         comm_action = (
#             f"Pipe inspection and replacement budget allocation required for "
#             f"{high_count} HIGH-risk segment(s): {worst_segs_str}. "
#             f"Estimated NRW reduction if addressed: 4–8%. "
#             f"Risk of emergency burst events if deferred."
#         )
#         conn.execute(text("""
#             INSERT INTO commissioner_recs
#               (city_summary, worst_zones, budget_flag, theft_summary,
#                resolution_rate, trigger_type)
#             VALUES (:cs, :wz, :bf, :ts, 0.0, 'C_burst')
#         """), {
#             'cs': city_summary,
#             'wz': json.dumps(high_segments[:5]),
#             'bf': budget_flag,
#             'ts': 'No theft data — V13 (Phase 4c)',
#         })

#     return eng_count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER D — Citizen Advisory (→ citizen_recs ONLY)
# #  Runs for every zone, every cycle.
# #  CRITICAL: NO infrastructure coords, valve IDs, or pipe segment data.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_d_citizen(zones, conn):
#     """
#     Writes to citizen_recs — plain language, no technical infrastructure data.
#     """
#     count = 0
#     for z in zones:
#         zone_id = z.get('zone_id', '')
#         hei     = float(z.get('hei', 0) or 0)
#         status  = z.get('status', 'equitable')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)

#         if status == 'severe':
#             supply_status = 'Intermittent'
#             advisory = (
#                 f"Water supply in {nm} may be reduced at some households. "
#                 f"The municipal team is actively working to restore normal pressure. "
#                 f"Store water if possible. Expected restoration: within 2-4 supply cycles."
#             )
#             guidance = (
#                 f"If you are experiencing no water or very low pressure, "
#                 f"please file a complaint below. Include your landmark and contact number."
#             )
#             restoration = "Within 2-4 supply cycles (6-24 hours)"
#         elif status == 'moderate':
#             supply_status = 'Normal'
#             advisory = (
#                 f"Water supply in {nm} is operating at near-normal levels. "
#                 f"Some households may experience slightly reduced pressure during peak hours. "
#                 f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM."
#             )
#             guidance = (
#                 f"If pressure seems lower than usual, wait for the next supply cycle. "
#                 f"Persistent issues? File a complaint below."
#             )
#             restoration = "Currently operational — monitoring in progress"
#         elif status == 'over':
#             supply_status = 'Normal'
#             advisory = (
#                 f"Water supply in {nm} is operating normally. "
#                 f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM."
#             )
#             guidance = (
#                 f"If you notice unusually high water flow or pipe vibration, "
#                 f"report it below. The municipal team is monitoring pressure levels."
#             )
#             restoration = "No disruption expected"
#         else:
#             supply_status = 'Normal'
#             advisory = (
#                 f"Water supply in {nm} is operating normally (HEI = {hei:.2f}). "
#                 f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM. "
#                 f"Ensure your overhead tank is filled during supply hours."
#             )
#             guidance = (
#                 f"For complaints about water quality, quantity, or billing, "
#                 f"use the form below. The municipal team responds within 24 hours."
#             )
#             restoration = "No disruption — normal operation"

#         conn.execute(text("""
#             INSERT INTO citizen_recs
#               (zone_id, supply_status, advisory_text,
#                complaint_guidance, estimated_restoration)
#             VALUES (:z, :ss, :adv, :guide, :rest)
#         """), {
#             'z':     zone_id,
#             'ss':    supply_status,
#             'adv':   advisory,
#             'guide': guidance,
#             'rest':  restoration,
#         })
#         count += 1

#     return count


# # ══════════════════════════════════════════════════════════════════════
# #  TRIGGER A (COMMISSIONER SUMMARY)
# #  Separate from per-zone equity — city-wide summary for commissioner.
# # ══════════════════════════════════════════════════════════════════════

# def trigger_a_commissioner(zones, conn):
#     """Writes city-wide equity summary to commissioner_recs."""
#     if not zones:
#         return 0

#     heis = [float(z.get('hei', 0) or 0) for z in zones]
#     cwei = sum(heis) / len(heis) if heis else 0
#     severe_zones  = [z['zone_id'] for z in zones if z.get('status') == 'severe']
#     moderate_zones = [z['zone_id'] for z in zones if z.get('status') == 'moderate']
#     over_zones    = [z['zone_id'] for z in zones if z.get('status') == 'over']

#     worst = sorted(zones, key=lambda z: float(z.get('hei', 0) or 0))[:3]
#     worst_ids = [z['zone_id'] for z in worst]

#     if cwei >= 0.85:
#         status_label = 'EQUITABLE'
#     elif cwei >= 0.70:
#         status_label = 'MODERATE IMBALANCE'
#     else:
#         status_label = 'SEVERE INEQUITY'

#     city_summary = (
#         f"City-Wide Equity Index (CWEI): {cwei:.3f} — {status_label}. "
#         f"Monitoring {len(zones)} zones. "
#         f"{len(severe_zones)} zone(s) in SEVERE inequity, "
#         f"{len(moderate_zones)} in MODERATE imbalance, "
#         f"{len(over_zones)} over-pressurized. "
#         f"Estimated NRW: 18% (baseline)."
#     )

#     worst_detail = ', '.join([
#         f"{ZONE_NAMES.get(z,'?')} (HEI={float(next((x.get('hei',0) for x in zones if x['zone_id']==z), 0)):.3f})"
#         for z in worst_ids
#     ])

#     action = (
#         f"City equity status requires {'immediate attention' if severe_zones else 'routine monitoring'}. "
#         f"Priority zones: {worst_detail if worst_detail else 'None'}. "
#         f"{'URGENT: allocate field teams to ' + str(len(severe_zones)) + ' severe zone(s).' if severe_zones else 'Continue current operations.'}"
#     )

#     conn.execute(text("""
#         INSERT INTO commissioner_recs
#           (city_summary, worst_zones, budget_flag, theft_summary,
#            resolution_rate, trigger_type)
#         VALUES (:cs, :wz, :bf, :ts, :rr, 'A_equity')
#     """), {
#         'cs': city_summary + ' ' + action,
#         'wz': json.dumps(worst_ids),
#         'bf': len(severe_zones) > 0,
#         'ts': 'Theft detection (V13) coming in Phase 4c.',
#         'rr': 0.0,
#     })

#     return 1


# # ══════════════════════════════════════════════════════════════════════
# #  MAIN ENTRY POINT
# # ══════════════════════════════════════════════════════════════════════

# def run_v7():
#     """
#     Main V7 function. Can be called:
#     - Directly: python scripts/v7_recommendations.py
#     - By APScheduler: every 5 minutes from backend/app.py
#     """
#     print('=' * 62)
#     print('  V7 · Role-Partitioned Recommendation Engine')
#     print('=' * 62)

#     # ── 1. Load analytics data ────────────────────────────────────
#     zones, alerts_all, burst_segs = load_analytics_data()
#     pipes_df, nodes_df, infra_df = load_network_data()

#     print(f'  Loaded: {len(zones)} zones, '
#           f'{sum(len(v) for v in alerts_all.values() if isinstance(v, list))} alerts, '
#           f'{len(burst_segs)} burst segments')

#     if not zones:
#         print('  [ABORT] No zone data. Run V4 first.')
#         return

#     # ── 2. Build influence map ────────────────────────────────────
#     print('  Building influence map...', end=' ')
#     influence = build_influence_map(pipes_df, nodes_df, infra_df)
#     print(f'{len(influence)} zones mapped')

#     # ── 3. Write to DB ────────────────────────────────────────────
#     print('  Running 5 triggers...')
#     eng_total = ward_total = comm_total = cit_total = 0

#     try:
#         with engine.connect() as conn:
#             # Clear records older than 24h
#             clear_old_recs(conn)

#             # Trigger A — Equity (engineer + ward + commissioner)
#             ec, wc = trigger_a_equity(zones, influence, conn)
#             eng_total  += ec
#             ward_total += wc
#             comm_total += trigger_a_commissioner(zones, conn)
#             # Citizen advisories (Trigger D runs alongside A)
#             cit_total  += trigger_d_citizen(zones, conn)
#             print(f'  [A] Equity  → {ec} engineer, {wc} ward, 1 commissioner, {cit_total} citizen')

#             # Trigger B — Leak (engineer + ward)  ← NOW WRITES WARD RECS TOO
#             bc, bwc = trigger_b_leak(alerts_all, influence, conn)
#             eng_total  += bc
#             ward_total += bwc
#             print(f'  [B] Leak    → {bc} engineer_recs, {bwc} ward_recs')

#             # Trigger C — Burst (engineer + commissioner)
#             cc = trigger_c_burst(burst_segs, conn)
#             eng_total  += cc
#             comm_total += 1 if burst_segs else 0
#             print(f'  [C] Burst   → {cc} engineer, 1 commissioner_recs')

#             print(f'  [D] Citizen → already written above ({cit_total} rows)')

#             # Log the run
#             conn.execute(text("""
#                 INSERT INTO v7_run_log
#                   (status, zones_processed, recs_generated,
#                    engineer_count, ward_count, commissioner_count, citizen_count)
#                 VALUES ('success', :zp, :rg, :ec, :wc, :cc, :cit)
#             """), {
#                 'zp':  len(zones),
#                 'rg':  eng_total + ward_total + comm_total + cit_total,
#                 'ec':  eng_total,
#                 'wc':  ward_total,
#                 'cc':  comm_total,
#                 'cit': cit_total,
#             })
#             conn.commit()
#             print('  [DB] Committed to PostgreSQL.')

#             # ── 4. Also write JSON cache so file-based router can read it ──
#             # Read back what we just wrote and save to outputs/v7_recommendations.json
#             _write_json_cache(conn, zones, alerts_all, burst_segs, influence)

#     except Exception as e:
#         print(f'  [WARN] DB write failed: {e}')
#         print('  Falling back to JSON-only mode (no PostgreSQL required).')
#         # Write JSON directly from in-memory data (no DB needed)
#         _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence)

#     total = eng_total + ward_total + comm_total + cit_total
#     print(f'\n  ✅ V7 complete. Total recs generated: {total}')
#     print(f'     engineer_recs    : {eng_total}')
#     print(f'     ward_recs        : {ward_total}')
#     print(f'     commissioner_recs: {comm_total}')
#     print(f'     citizen_recs     : {cit_total}')
#     print('=' * 62)


# # ══════════════════════════════════════════════════════════════════════
# #  JSON CACHE HELPERS
# #  Write outputs/v7_recommendations.json so the fast router endpoint
# #  can serve recs without hitting the DB on every request.
# # ══════════════════════════════════════════════════════════════════════

# def _write_json_cache(conn, zones, alerts_all, burst_segs, influence):
#     """
#     Read what was just written to DB and dump it all to the JSON cache.
#     Called after a successful DB commit inside run_v7.
#     """
#     try:
#         eng_rows  = conn.execute(text(
#             "SELECT zone_id, trigger_type, action_text, valve_id, pipe_id, "
#             "urgency, estimated_hei_gain FROM engineer_recs ORDER BY created_at DESC LIMIT 100"
#         )).fetchall()
#         ward_rows = conn.execute(text(
#             "SELECT zone_id, trigger_type, action_text, escalation_flag, "
#             "service_reliability_note, complaint_count FROM ward_recs ORDER BY created_at DESC LIMIT 100"
#         )).fetchall()
#         comm_rows = conn.execute(text(
#             "SELECT city_summary, worst_zones, budget_flag, theft_summary, "
#             "resolution_rate, trigger_type FROM commissioner_recs ORDER BY created_at DESC LIMIT 20"
#         )).fetchall()
#         cit_rows  = conn.execute(text(
#             "SELECT zone_id, supply_status, advisory_text, complaint_guidance, "
#             "estimated_restoration FROM citizen_recs ORDER BY zone_id, created_at DESC LIMIT 50"
#         )).fetchall()

#         data = {
#             "engineer_recs": [
#                 {"zone_id": r[0] or '', "trigger_type": r[1] or '', "action_text": r[2] or '',
#                  "valve_id": r[3] or '', "pipe_id": r[4] or '', "urgency": r[5] or 'LOW',
#                  "estimated_hei_gain": float(r[6] or 0)}
#                 for r in eng_rows
#             ],
#             "ward_recs": [
#                 {"zone_id": r[0] or '', "trigger_type": r[1] or '', "action_text": r[2] or '',
#                  "escalation_flag": bool(r[3]), "service_reliability_note": r[4] or '',
#                  "complaint_count": int(r[5] or 0)}
#                 for r in ward_rows
#             ],
#             "commissioner_recs": [
#                 {"city_summary": r[0] or '', "worst_zones": _safe_json_loads(r[1]),
#                  "budget_flag": bool(r[2]), "theft_summary": r[3] or '',
#                  "resolution_rate": float(r[4] or 0), "trigger_type": r[5] or ''}
#                 for r in comm_rows
#             ],
#             "citizen_recs": [
#                 {"zone_id": r[0] or '',
#                  "zone_name": "Zone {}".format((r[0] or '').replace('zone_', '')),
#                  "supply_status": r[1] or 'Normal', "advisory_text": r[2] or '',
#                  "complaint_guidance": r[3] or '', "estimated_restoration": r[4] or 'N/A'}
#                 for r in cit_rows
#             ],
#             "updated_at": datetime.now().isoformat(),
#         }
#         _save_json(data)
#         print(f'  [JSON] Cache written: {len(data["engineer_recs"])} eng, '
#               f'{len(data["ward_recs"])} ward, '
#               f'{len(data["commissioner_recs"])} comm, '
#               f'{len(data["citizen_recs"])} citizen')
#     except Exception as e:
#         print(f'  [WARN] JSON cache write (from DB) failed: {e}')
#         # Fall back to generating from memory
#         _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence)


# def _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence):
#     """
#     Build and write the JSON cache entirely from in-memory analytics data.
#     Used when PostgreSQL is unavailable (dev/file mode).
#     This is the same logic as the triggers but writes to a dict instead of DB.
#     """
#     engineer_recs    = []
#     ward_recs        = []
#     commissioner_recs = []
#     citizen_recs     = []

#     # ── Trigger A: Equity ─────────────────────────────────────────
#     for z in zones:
#         zone_id = z.get('zone_id', '')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)
#         hei     = float(z.get('hei', 0) or 0)
#         status  = z.get('status', 'equitable')
#         infl    = influence.get(zone_id, {})
#         pipe_id = infl.get('control_pipe_id', f'V-{zone_id.upper().replace("_","")}')

#         if status == 'severe':
#             gain = round(0.85 - hei, 3)
#             engineer_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity", "urgency": "URGENT",
#                 "valve_id": pipe_id, "pipe_id": "", "estimated_hei_gain": gain,
#                 "action_text": (
#                     f"[{nm}] HEI critically low at {hei:.3f} (target ≥ 0.85). "
#                     f"Increase ESR outlet pressure 10–15% via control pipe {pipe_id}. "
#                     f"Estimated HEI gain: +{gain:.3f}. Dispatch field team for verification."
#                 ),
#             })
#             ward_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity",
#                 "escalation_flag": True, "complaint_count": 0,
#                 "action_text": (
#                     f"{nm} supply is severely inequitable (HEI = {hei:.3f}). "
#                     f"Citizens in tail-end areas may receive little or no water. "
#                     f"Escalate to engineering control room immediately."
#                 ),
#                 "service_reliability_note": f"HEI: {hei:.3f} — Severe. Tail-end households affected.",
#             })
#         elif status == 'moderate':
#             engineer_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity", "urgency": "MODERATE",
#                 "valve_id": pipe_id, "pipe_id": "", "estimated_hei_gain": round(0.85 - hei, 3),
#                 "action_text": (
#                     f"[{nm}] HEI is {hei:.3f} — moderate imbalance. "
#                     f"Review valve settings on distribution mains."
#                 ),
#             })
#             ward_recs.append({
#                 "zone_id": zone_id, "trigger_type": "A_equity",
#                 "escalation_flag": False, "complaint_count": 0,
#                 "action_text": (
#                     f"{nm} shows moderate pressure imbalance (HEI = {hei:.3f}). "
#                     f"Some tail-end households may face reduced pressure during peak hours. "
#                     f"Monitor complaints and report persistent issues to engineering."
#                 ),
#                 "service_reliability_note": f"HEI: {hei:.3f} — Moderate. Monitor peak-hour supply.",
#             })

#         # Citizen advisory per zone
#         if status == 'severe':
#             cit_status, cit_adv = 'Intermittent', (
#                 f"Water supply in {nm} may be limited. SMC engineers are working to restore pressure. "
#                 f"Please store water during supply hours.")
#             cit_guid = "If you have no water, submit a complaint using the form below."
#             cit_rest = "Engineers are working on it. Check back in 2–4 hours."
#         elif status == 'moderate':
#             cit_status, cit_adv = 'Normal', (
#                 f"Water supply in {nm} is near-normal. Some households may see slightly reduced "
#                 f"pressure during peak hours (6–8 AM, 5–8 PM).")
#             cit_guid = "Persistent low pressure? File a complaint below."
#             cit_rest = "Currently operational — monitoring in progress"
#         else:
#             cit_status, cit_adv = 'Normal', (
#                 f"Water supply in {nm} is operating normally (HEI = {hei:.2f}). "
#                 f"Supply window: 6–8 AM and 5–8 PM. Fill overhead tanks during supply hours.")
#             cit_guid = "For any supply issues, submit a complaint using the form below."
#             cit_rest = "No disruption — normal operation"

#         citizen_recs.append({
#             "zone_id": zone_id,
#             "zone_name": "Zone {}".format(zone_id.replace('zone_', '')),
#             "supply_status": cit_status, "advisory_text": cit_adv,
#             "complaint_guidance": cit_guid, "estimated_restoration": cit_rest,
#         })

#     # ── Trigger B: Leak/Anomaly alerts ────────────────────────────
#     if isinstance(alerts_all, dict):
#         alerts = alerts_all.get('baseline', alerts_all.get('leak', []))
#     else:
#         alerts = alerts_all if isinstance(alerts_all, list) else []

#     for a in alerts:
#         zone_id = a.get('zone_id', '')
#         nm      = ZONE_NAMES.get(zone_id, zone_id)
#         clps    = float(a.get('clps', 0) or 0)
#         sig     = a.get('dominant_signal', 'DDI')
#         infl    = influence.get(zone_id, {})
#         pipe_id = infl.get('control_pipe_id', f'V-{zone_id.upper().replace("_","")}')

#         if clps < 0.05:
#             continue

#         sig_map = {
#             'PDR_n': ('URGENT' if clps > 0.5 else 'HIGH',
#                       f"SUDDEN PRESSURE DROP in {nm} (CLPS={clps:.3f}). Dispatch team to Pipe #{pipe_id}.",
#                       f"Sudden pressure drop detected — possible burst. Log flooding reports immediately.", True),
#             'FPI':   ('HIGH',
#                       f"FLOW-PRESSURE IMBALANCE in {nm} (CLPS={clps:.3f}). Probable leakage near Pipe #{pipe_id}.",
#                       f"Flow imbalance detected — possible pipe leakage. Monitor complaint volume.", clps > 0.3),
#             'NFA':   ('HIGH',
#                       f"NIGHT FLOW ANOMALY in {nm} (CLPS={clps:.3f}). Inspect Pipe #{pipe_id} for unauthorized extraction.",
#                       f"Night flow anomaly — possible unauthorized extraction. Escalate to engineering.", True),
#             'DDI':   ('MODERATE',
#                       f"DEMAND DEVIATION in {nm} (CLPS={clps:.3f}). Check valve status on Pipe #{pipe_id}.",
#                       f"Demand deviation detected in {nm} (CLPS={clps:.3f}). "
#                       f"Actual consumption differs from expected pattern. "
#                       f"Check for valve misalignment or meter issues. "
#                       f"If citizens report low water, escalate to engineering.", False),
#         }
#         urg, eng_txt, ward_txt, escalate = sig_map.get(sig, sig_map['DDI'])

#         engineer_recs.append({
#             "zone_id": zone_id, "trigger_type": "B_leak", "urgency": urg,
#             "valve_id": f"V-{zone_id.upper().replace('_','')}", "pipe_id": pipe_id,
#             "estimated_hei_gain": 0.0, "action_text": eng_txt,
#         })
#         ward_recs.append({
#             "zone_id": zone_id, "trigger_type": "B_leak",
#             "escalation_flag": escalate, "complaint_count": 0,
#             "action_text": ward_txt,
#             "service_reliability_note": f"Active anomaly — CLPS: {clps:.3f} · Signal: {sig}.",
#         })

#     # ── Trigger C: Burst risk → engineer + commissioner ───────────
#     high_burst = [s for s in burst_segs if s.get('risk_level') == 'HIGH']
#     mod_burst  = [s for s in burst_segs if s.get('risk_level') == 'MODERATE']

#     for s in high_burst:
#         seg = s.get('segment_id', '?')
#         pss = float(s.get('pss', 0))
#         mat = s.get('material', 'Unknown')
#         age = s.get('age', '?')
#         dom = s.get('dominant_factor', 'PSI_n')
#         engineer_recs.append({
#             "zone_id": "", "trigger_type": "C_burst",
#             "urgency": "URGENT" if pss > 0.85 else "HIGH",
#             "valve_id": "", "pipe_id": str(seg), "estimated_hei_gain": 0.0,
#             "action_text": (
#                 f"HIGH BURST RISK: Pipe #{seg} (PSS={pss:.3f}). "
#                 f"Material: {mat}, Age: ~{age}yr. Factor: {dom}. "
#                 f"Schedule urgent inspection. Pre-emptive replacement if PSS > 0.90."
#             ),
#         })

#     if high_burst or mod_burst:
#         commissioner_recs.append({
#             "trigger_type": "C_burst",
#             "city_summary": (
#                 f"{len(high_burst)} pipe segment(s) at HIGH burst risk, {len(mod_burst)} MODERATE. "
#                 f"Capital expenditure for emergency pipe replacement should be considered."
#             ),
#             "worst_zones": [str(s.get('segment_id', '?')) for s in high_burst[:3]],
#             "budget_flag": len(high_burst) > 0,
#             "theft_summary": "Theft detection (V13) coming in Phase 4c.",
#             "resolution_rate": 0.0,
#         })

#     # ── Trigger A Commissioner summary ────────────────────────────
#     if zones:
#         heis  = [float(z.get('hei', 0) or 0) for z in zones]
#         cwei  = sum(heis) / len(heis)
#         worst = sorted(zones, key=lambda z: float(z.get('hei', 0) or 0))[:3]
#         sev   = [z for z in zones if z.get('status') == 'severe']
#         commissioner_recs.insert(0, {
#             "trigger_type": "A_equity",
#             "city_summary": (
#                 f"CWEI: {cwei:.3f} — "
#                 f"{'SEVERE INEQUITY' if cwei < 0.70 else 'MODERATE IMBALANCE' if cwei < 0.85 else 'EQUITABLE'}. "
#                 f"{len(sev)} zone(s) severe. "
#                 f"Priority: {', '.join(ZONE_NAMES.get(z['zone_id'], z['zone_id']) for z in worst[:2])}."
#             ),
#             "worst_zones": [z['zone_id'] for z in worst],
#             "budget_flag": len(sev) > 0,
#             "theft_summary": "Theft detection (V13) coming in Phase 4c.",
#             "resolution_rate": 0.0,
#         })

#     # Sort engineer recs by urgency
#     _order = {'URGENT': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}
#     engineer_recs.sort(key=lambda r: _order.get(r.get('urgency', 'LOW'), 3))

#     data = {
#         "engineer_recs":     engineer_recs,
#         "ward_recs":         ward_recs,
#         "commissioner_recs": commissioner_recs,
#         "citizen_recs":      citizen_recs,
#         "updated_at":        datetime.now().isoformat(),
#         "source":            "v7_memory",
#     }
#     _save_json(data)
#     print(f'  [JSON] Memory cache written: {len(engineer_recs)} eng, '
#           f'{len(ward_recs)} ward, '
#           f'{len(commissioner_recs)} comm, '
#           f'{len(citizen_recs)} citizen')


# def _save_json(data):
#     """Write data to outputs/v7_recommendations.json."""
#     out_path = os.path.join(OUTPUTS, 'v7_recommendations.json')
#     os.makedirs(OUTPUTS, exist_ok=True)
#     with open(out_path, 'w', encoding='utf-8') as f:
#         json.dump(data, f, indent=2)


# def _safe_json_loads(val):
#     if not val:
#         return []
#     try:
#         return json.loads(val)
#     except Exception:
#         return [val] if val else []


# if __name__ == '__main__':
#     run_v7()


# # Hydro-Equity Engine V7 — Recommendation Generator
# # Generates logical, data-driven recommendations based on live HEI/zone metrics.
# # NOT hardcoded — all recommendations derive from actual zone data.
# # """

# # from __future__ import annotations
# # import logging
# # from dataclasses import dataclass, field, asdict
# # from datetime import datetime, timezone
# # from typing import Optional
# # import uuid

# # logger = logging.getLogger(__name__)

# # # ── Thresholds (tunable) ──────────────────────────────────────────
# # HEI_EQUITABLE  = 0.85
# # HEI_MODERATE   = 0.70
# # CLPS_ANOMALY   = 0.12
# # PRESSURE_LOW   = 20.0   # m head — below this is critical
# # PRESSURE_HIGH  = 60.0   # m head — above this risks pipe burst
# # NRW_HIGH       = 0.20   # 20% NRW is high
# # COMPLAINT_HIGH = 10     # complaints/zone/day


# # @dataclass
# # class Recommendation:
# #     rec_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
# #     title: str = ""
# #     description: str = ""
# #     priority: str = "medium"         # critical | high | medium | low
# #     action_type: str = ""            # valve_adjust | pressure_boost | maintenance | investigation | policy
# #     zone_id: Optional[str] = None
# #     scope: str = "zone"              # zone | city | strategic
# #     is_strategic: bool = False
# #     estimated_hei_gain: Optional[float] = None
# #     estimated_impact: Optional[str] = None
# #     budget_estimate: Optional[str] = None
# #     generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

# #     def to_dict(self) -> dict:
# #         return {k: v for k, v in asdict(self).items() if v is not None}


# # def generate_recommendations(zones: list[dict], city_summary: dict | None = None) -> list[Recommendation]:
# #     """
# #     Generate logical recommendations from live zone data.
    
# #     zones: list of zone dicts with keys:
# #         zone_id, hei_score, status, avg_pressure_mh, open_complaints,
# #         clps_score, nrw_pct, zes_daily, demand_deviation, pipe_age_avg
    
# #     city_summary: optional dict with city-level aggregates
# #     """
# #     recs: list[Recommendation] = []

# #     if not zones:
# #         logger.warning("V7: No zone data provided — skipping recommendation generation")
# #         return recs

# #     for zone in zones:
# #         zone_id      = zone.get("zone_id", "unknown")
# #         hei          = float(zone.get("hei_score", 1.0))
# #         status       = zone.get("status", "equitable")
# #         pressure     = zone.get("avg_pressure_mh") or zone.get("pressure_mh")
# #         complaints   = zone.get("open_complaints", 0) or 0
# #         clps         = zone.get("clps_score") or zone.get("demand_deviation", 0.0)
# #         nrw          = zone.get("nrw_pct", 0.0) or 0.0
# #         pipe_age     = zone.get("pipe_age_avg")
# #         supply_status = zone.get("supply_status", "normal")

# #         # ── 1. Severe HEI — critical action needed ──
# #         if hei < HEI_MODERATE:
# #             gain = round(HEI_MODERATE - hei + 0.05, 3)
# #             recs.append(Recommendation(
# #                 title=f"Critical Equity Intervention — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"{zone_id.replace('_',' ').title()} has a critically low HEI of {hei:.3f} (threshold: {HEI_MODERATE}). "
# #                     f"Immediate review of inlet valve settings and distribution scheduling is required. "
# #                     f"Consider emergency pressure boost and reallocation from adjacent high-equity zones."
# #                 ),
# #                 priority="critical",
# #                 action_type="valve_adjust",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_hei_gain=gain,
# #                 estimated_impact=f"HEI improvement of +{gain:.3f} expected within 48h"
# #             ))

# #         # ── 2. Moderate HEI — scheduled intervention ──
# #         elif hei < HEI_EQUITABLE:
# #             gain = round(HEI_EQUITABLE - hei + 0.02, 3)
# #             recs.append(Recommendation(
# #                 title=f"Pressure Rebalancing — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"HEI of {hei:.3f} in {zone_id} is below equitable threshold ({HEI_EQUITABLE}). "
# #                     f"Scheduled pressure rebalancing recommended. Check valve positions on main inlet "
# #                     f"and review ZES scheduling to improve supply uniformity."
# #                 ),
# #                 priority="high",
# #                 action_type="pressure_boost",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_hei_gain=gain,
# #                 estimated_impact=f"Projected HEI gain: +{gain:.3f}"
# #             ))

# #         # ── 3. Low pressure ──
# #         if pressure is not None:
# #             pressure = float(pressure)
# #             if pressure < PRESSURE_LOW:
# #                 recs.append(Recommendation(
# #                     title=f"Low Pressure Alert — {zone_id.replace('_',' ').title()}",
# #                     description=(
# #                         f"Average pressure in {zone_id} is {pressure:.1f}m head, below the minimum threshold "
# #                         f"of {PRESSURE_LOW}m. This causes supply inadequacy at tail-end connections. "
# #                         f"Check for blockages, partially-closed valves, or ESR level issues."
# #                     ),
# #                     priority="high" if pressure < 15 else "medium",
# #                     action_type="investigation",
# #                     zone_id=zone_id,
# #                     scope="zone",
# #                     estimated_impact=f"Restoring to {PRESSURE_LOW+5:.0f}m will improve {zone_id} coverage"
# #                 ))

# #             elif pressure > PRESSURE_HIGH:
# #                 recs.append(Recommendation(
# #                     title=f"High Pressure Risk — {zone_id.replace('_',' ').title()}",
# #                     description=(
# #                         f"Pressure in {zone_id} is {pressure:.1f}m head, exceeding safe limit of {PRESSURE_HIGH}m. "
# #                         f"Risk of pipe stress and burst events. Recommend installing or adjusting pressure "
# #                         f"reducing valve (PRV) at zone inlet."
# #                     ),
# #                     priority="high",
# #                     action_type="valve_adjust",
# #                     zone_id=zone_id,
# #                     scope="zone",
# #                     estimated_impact="Reduces burst risk and NRW"
# #                 ))

# #         # ── 4. CLPS anomaly (Demand Deviation Index) ──
# #         if clps is not None and abs(float(clps)) > CLPS_ANOMALY:
# #             clps_val = float(clps)
# #             direction = "over-consumption" if clps_val > 0 else "under-supply"
# #             recs.append(Recommendation(
# #                 title=f"DDI Anomaly Investigation — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"Demand Deviation Index (CLPS) of {clps_val:.3f} in {zone_id} exceeds anomaly threshold "
# #                     f"(±{CLPS_ANOMALY}). This indicates potential {direction}. "
# #                     f"Verify valve status, check for unauthorised connections, and audit meter readings."
# #                 ),
# #                 priority="medium",
# #                 action_type="investigation",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_impact="Resolving DDI reduces NRW and improves equity"
# #             ))

# #         # ── 5. High complaints ──
# #         if int(complaints) >= COMPLAINT_HIGH:
# #             recs.append(Recommendation(
# #                 title=f"High Complaint Volume — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"{zone_id} has {complaints} open complaints — above the threshold of {COMPLAINT_HIGH}. "
# #                     f"Prioritise field inspection to identify common issues. "
# #                     f"Common causes: intermittent supply, low pressure, or localised pipe deterioration."
# #                 ),
# #                 priority="medium",
# #                 action_type="maintenance",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_impact=f"Resolving {complaints} complaints improves citizen satisfaction"
# #             ))

# #         # ── 6. High NRW per zone ──
# #         if float(nrw) > NRW_HIGH:
# #             recs.append(Recommendation(
# #                 title=f"Elevated NRW — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"Non-Revenue Water (NRW) in {zone_id} is {nrw*100:.1f}%, exceeding the {NRW_HIGH*100:.0f}% "
# #                     f"threshold. Conduct distribution loss audit: check meter accuracy, identify illegal "
# #                     f"connections, and inspect high-age pipes for leakage."
# #                 ),
# #                 priority="medium",
# #                 action_type="maintenance",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 estimated_impact=f"Reducing NRW to {NRW_HIGH*100:.0f}% recovers significant water volume"
# #             ))

# #         # ── 7. Pipe age (if available) ──
# #         if pipe_age is not None and float(pipe_age) > 25:
# #             recs.append(Recommendation(
# #                 title=f"Aging Infrastructure — {zone_id.replace('_',' ').title()}",
# #                 description=(
# #                     f"Average pipe age in {zone_id} is {float(pipe_age):.0f} years, exceeding the "
# #                     f"recommended 25-year service life. Schedule condition assessment and phased replacement "
# #                     f"to prevent burst events and reduce NRW."
# #                 ),
# #                 priority="low",
# #                 action_type="maintenance",
# #                 zone_id=zone_id,
# #                 scope="zone",
# #                 budget_estimate="To be determined by condition survey",
# #                 estimated_impact="Long-term NRW reduction and reliability improvement"
# #             ))

# #     # ── City-level strategic recommendations ──
# #     if city_summary:
# #         recs += _generate_strategic(zones, city_summary)

# #     # Sort: critical → high → medium → low, then by zone
# #     priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
# #     recs.sort(key=lambda r: (priority_order.get(r.priority, 4), r.zone_id or ""))
# #     logger.info(f"V7: Generated {len(recs)} recommendations for {len(zones)} zones")
# #     return recs


# # def _generate_strategic(zones: list[dict], city: dict) -> list[Recommendation]:
# #     """Generate city-level strategic recommendations."""
# #     recs = []
# #     city_hei     = float(city.get("city_equity_index", 1.0))
# #     severe_zones = [z for z in zones if float(z.get("hei_score", 1.0)) < HEI_MODERATE]
# #     moderate_zones = [z for z in zones if HEI_MODERATE <= float(z.get("hei_score", 1.0)) < HEI_EQUITABLE]
# #     city_nrw     = float(city.get("nrw_pct", 0.0) or 0.0)

# #     # Strategic: multiple severe zones
# #     if len(severe_zones) >= 2:
# #         zone_names = ", ".join(z["zone_id"].replace("_"," ").title() for z in severe_zones[:4])
# #         recs.append(Recommendation(
# #             title="City-Wide Equity Emergency — Multi-Zone Intervention",
# #             description=(
# #                 f"{len(severe_zones)} zones ({zone_names}) have HEI below {HEI_MODERATE}. "
# #                 f"This requires a coordinated city-wide intervention: emergency redistribution plan, "
# #                 f"temporary tanker supply to worst-affected areas, and expedited infrastructure audit. "
# #                 f"Commissioner approval required for resource reallocation."
# #             ),
# #             priority="critical",
# #             action_type="policy",
# #             scope="strategic",
# #             is_strategic=True,
# #             estimated_hei_gain=round((HEI_EQUITABLE - city_hei) * 0.6, 3),
# #             estimated_impact=f"Could bring {len(severe_zones)} zones to moderate status within 2 weeks"
# #         ))

# #     # Strategic: city NRW high
# #     if city_nrw > NRW_HIGH:
# #         recs.append(Recommendation(
# #             title="City-Wide NRW Reduction Programme",
# #             description=(
# #                 f"City-wide NRW of {city_nrw*100:.1f}% represents significant water loss. "
# #                 f"Recommend initiating a structured NRW reduction programme: district metered areas (DMA) "
# #                 f"setup, automated leak detection, and meter replacement on aged connections. "
# #                 f"Target: reduce NRW to below {NRW_HIGH*100:.0f}% within 12 months."
# #             ),
# #             priority="high",
# #             action_type="policy",
# #             scope="strategic",
# #             is_strategic=True,
# #             estimated_impact=f"Recovering {city_nrw*100 - NRW_HIGH*100:.1f}% NRW improves supply for all zones"
# #         ))

# #     # Strategic: equity gap between best and worst zone
# #     if len(zones) >= 2:
# #         sorted_by_hei = sorted(zones, key=lambda z: float(z.get("hei_score", 1.0)))
# #         worst = sorted_by_hei[0]
# #         best  = sorted_by_hei[-1]
# #         hei_gap = float(best.get("hei_score", 1.0)) - float(worst.get("hei_score", 1.0))
# #         if hei_gap > 0.30:
# #             recs.append(Recommendation(
# #                 title="Equity Gap Reduction — Zone Rebalancing Plan",
# #                 description=(
# #                     f"HEI gap of {hei_gap:.3f} between best zone ({best['zone_id']}, HEI {float(best.get('hei_score',1)):.3f}) "
# #                     f"and worst zone ({worst['zone_id']}, HEI {float(worst.get('hei_score',1)):.3f}) is significant. "
# #                     f"Recommend a zone rebalancing study: identify surplus capacity in high-HEI zones and "
# #                     f"establish systematic reallocation schedule to reduce gap to below 0.20."
# #                 ),
# #                 priority="high",
# #                 action_type="policy",
# #                 scope="strategic",
# #                 is_strategic=True,
# #                 estimated_hei_gain=round(hei_gap * 0.4, 3),
# #                 estimated_impact="Reduces inter-zone inequity, improves overall CWEI"
# #             ))

# #     return recs


# # def format_for_api(recs: list[Recommendation], zone_id: str | None = None) -> dict:
# #     """Format recommendations for API response, optionally filtered by zone."""
# #     filtered = recs
# #     if zone_id:
# #         # Include zone-specific + strategic recs
# #         filtered = [r for r in recs if r.zone_id == zone_id or r.is_strategic or r.scope in ('strategic','city')]

# #     return {
# #         "recommendations": [r.to_dict() for r in filtered],
# #         "total":     len(filtered),
# #         "generated_at": datetime.now(timezone.utc).isoformat(),
# #         "engine":    "V7"
# #     }





"""
Hydro-Equity Engine — Phase 4b
scripts/v7_recommendations.py
V7 — Role-Partitioned Recommendation Engine

Reads:
  outputs/v4_zone_status.json   → zone HEI / status
  outputs/v5_alerts.json        → CLPS alerts per scenario
  outputs/v6_burst_top10.json   → top burst-risk pipe segments
  Data/pipe_segments.csv        → for influence map (optional)
  Data/nodes_with_elevation.csv → for influence map (optional)
  Data/infrastructure_points.csv → for ESR locations (optional)

Writes (PostgreSQL):
  engineer_recs       → valve_id, pipe_id, urgency, HEI gain (Trigger A,B,C)
  ward_recs           → plain-language escalation notes  (Trigger A)
  commissioner_recs   → city summary, budget flag        (Trigger A,C)
  citizen_recs        → supply status, advisory          (Trigger D)
  v7_run_log          → run timestamp and counts

Run manually:   python scripts/v7_recommendations.py
Auto-scheduled: every 5 minutes via APScheduler in backend/app.py
"""

import os, sys, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from datetime import datetime
from sqlalchemy import text


ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA    = os.path.join(ROOT, 'Data')
OUTPUTS = os.path.join(ROOT, 'outputs')

# ── Zone display name mapping ──────────────────────────────────────
ZONE_NAMES = {
    'zone_1': 'Zone 1', 'zone_2': 'Zone 2', 'zone_3': 'Zone 3',
    'zone_4': 'Zone 4', 'zone_5': 'Zone 5', 'zone_6': 'Zone 6',
    'zone_7': 'Zone 7', 'zone_8': 'Zone 8',
}

# Fallback ESR positions (used if infrastructure_points.csv missing)
FALLBACK_ESRS = [
    {'lat': 17.7038, 'lon': 75.9065, 'zone': 'zone_1'},  # N-Center
    {'lat': 17.7038, 'lon': 75.9430, 'zone': 'zone_2'},  # N-East
    {'lat': 17.6690, 'lon': 75.8700, 'zone': 'zone_3'},  # M-West
    {'lat': 17.6690, 'lon': 75.9065, 'zone': 'zone_4'},  # M-Center
    {'lat': 17.6690, 'lon': 75.9430, 'zone': 'zone_5'},  # M-East
    {'lat': 17.6342, 'lon': 75.8700, 'zone': 'zone_6'},  # S-West
    {'lat': 17.6342, 'lon': 75.9065, 'zone': 'zone_7'},  # S-Center
    {'lat': 17.6342, 'lon': 75.9430, 'zone': 'zone_8'},  # S-East
]

# Zone centroid positions (for node_coords in recommendations)
ZONE_COORDS = {
    'zone_1': {'lat': 17.7038, 'lon': 75.9065},  # N-Center (Hipparge/NH52)
    'zone_2': {'lat': 17.7038, 'lon': 75.9430},  # N-East   (Mulagaon/SH76)
    'zone_3': {'lat': 17.6690, 'lon': 75.8700},  # M-West   (Hiraj/NH166)
    'zone_4': {'lat': 17.6690, 'lon': 75.9065},  # M-Center (Central Solapur)
    'zone_5': {'lat': 17.6690, 'lon': 75.9430},  # M-East   (Sangali/Rajiv Nagar)
    'zone_6': {'lat': 17.6342, 'lon': 75.8700},  # S-West   (Siddheshwar/South)
    'zone_7': {'lat': 17.6342, 'lon': 75.9065},  # S-Center (Bharatmata Nagar)
    'zone_8': {'lat': 17.6342, 'lon': 75.9430},  # S-East   (Kumbhari/Dindoor)
}


# ══════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ══════════════════════════════════════════════════════════════════════

def load_analytics_data():
    """Load V4, V5, V6 outputs. Returns (zones, alerts, burst_segs)."""
    zones = []
    path = os.path.join(OUTPUTS, 'v4_zone_status.json')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            zones = json.load(f)
    else:
        print('  [WARN] v4_zone_status.json not found — run V4 first')

    alerts_all = {}
    path = os.path.join(OUTPUTS, 'v5_alerts.json')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            alerts_all = json.load(f)
    else:
        print('  [WARN] v5_alerts.json not found — run V5 first')

    burst_segs = []
    path = os.path.join(OUTPUTS, 'v6_burst_top10.json')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            burst_segs = json.load(f)
    else:
        print('  [WARN] v6_burst_top10.json not found — run V6 first')

    return zones, alerts_all, burst_segs


def load_network_data():
    """Load pipe segments and nodes for influence map building."""
    pipes_df = None
    nodes_df = None
    infra_df = None

    p = os.path.join(DATA, 'pipe_segments.csv')
    if os.path.exists(p):
        pipes_df = pd.read_csv(p)

    p = os.path.join(DATA, 'nodes_with_elevation.csv')
    if os.path.exists(p):
        nodes_df = pd.read_csv(p)

    p = os.path.join(DATA, 'infrastructure_points.csv')
    if os.path.exists(p):
        infra_df = pd.read_csv(p)

    return pipes_df, nodes_df, infra_df


# ══════════════════════════════════════════════════════════════════════
#  INFLUENCE MAP
#  For each zone → find control pipe (largest-diameter cross-zone pipe)
#  and nearest ESR node. Used to build specific valve/pipe recommendations.
# ══════════════════════════════════════════════════════════════════════

def build_influence_map(pipes_df, nodes_df, infra_df):
    """
    Returns dict: {zone_id: {control_pipe_id, esr_lat, esr_lon}}
    Falls back gracefully if CSV files are unavailable.
    """
    influence = {}

    # Initialize all known zones with defaults
    for zone, coords in ZONE_COORDS.items():
        influence[zone] = {
            'control_pipe_id': 'V-' + zone.upper().replace('_', ''),
            'esr_lat': coords['lat'],
            'esr_lon': coords['lon'],
        }

    if pipes_df is None or nodes_df is None:
        # No network data — use defaults
        return influence

    try:
        # Build zone lookup: node_id → zone_id
        node_zone = {}
        for _, r in nodes_df.iterrows():
            nid = str(int(r['node_id'])) if pd.notna(r['node_id']) else None
            if nid:
                node_zone[nid] = str(r.get('zone_id', ''))

        # Find cross-zone pipes (inlet pipes per zone)
        zone_inlets = {}
        valid = pipes_df.dropna(subset=['start_node_id', 'end_node_id']).copy()
        for _, row in valid.iterrows():
            try:
                zu = node_zone.get(str(int(row['start_node_id'])), '')
                zv = node_zone.get(str(int(row['end_node_id'])), '')
                if zu != zv and zv:
                    if zv not in zone_inlets:
                        zone_inlets[zv] = []
                    zone_inlets[zv].append({
                        'pipe_id':  str(int(row['segment_id'])),
                        'diameter': float(row.get('diameter_m', 0.1) or 0.1),
                    })
            except (ValueError, TypeError):
                continue

        # Control pipe = largest-diameter inlet pipe for each zone
        for zone, inlets in zone_inlets.items():
            if inlets:
                best = max(inlets, key=lambda x: x['diameter'])
                influence[zone] = {**influence.get(zone, {}), 'control_pipe_id': best['pipe_id']}

        # ESR locations from infrastructure_points.csv
        if infra_df is not None and len(infra_df) > 0:
            esrs = infra_df[infra_df['feature_type'].isin(['storage_tank', 'water_source'])]
            zone_nodes = nodes_df.copy()

            for zone in influence:
                zn = zone_nodes[zone_nodes['zone_id'] == zone]
                if len(zn) == 0:
                    continue
                clat = zn['lat'].mean()
                clon = zn['lon'].mean()
                if len(esrs) > 0:
                    dists = ((esrs['lat'] - clat)**2 + (esrs['lon'] - clon)**2)**0.5
                    nearest = esrs.iloc[dists.values.argmin()]
                    influence[zone]['esr_lat'] = float(nearest['lat'])
                    influence[zone]['esr_lon'] = float(nearest['lon'])

    except Exception as e:
        print(f'  [WARN] Influence map build error: {e} — using defaults')

    return influence


# ══════════════════════════════════════════════════════════════════════
#  DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════

def clear_old_recs(conn):
    """Remove recommendations older than 24 hours to keep tables clean."""
    for tbl in ['engineer_recs', 'ward_recs', 'commissioner_recs', 'citizen_recs']:
        try:
            conn.execute(text(
                f"DELETE FROM {tbl} WHERE created_at < NOW() - INTERVAL '24 hours'"
            ))
        except Exception:
            pass  # Table might not exist yet — safe to ignore


# ══════════════════════════════════════════════════════════════════════
#  TRIGGER A — Equity Rules (→ ALL 4 channels)
#  Fires for every zone, every run.
#  Severity: severe < 0.70, moderate 0.70-0.85, equitable 0.85-1.30, over > 1.30
# ══════════════════════════════════════════════════════════════════════

def trigger_a_equity(zones, influence, conn):
    """
    Writes to engineer_recs, ward_recs, commissioner_recs, citizen_recs
    based on zone HEI status.
    Returns (eng_count, ward_count) written.
    """
    eng_count = ward_count = 0
    severe_zones  = [z for z in zones if z.get('status') == 'severe']
    moderate_zones = [z for z in zones if z.get('status') == 'moderate']
    over_zones    = [z for z in zones if z.get('status') == 'over']

    for z in zones:
        zone_id  = z.get('zone_id', '')
        hei      = float(z.get('hei', 0) or 0)
        status   = z.get('status', 'equitable')
        zm       = ZONE_COORDS.get(zone_id, {'lat': 17.6690, 'lon': 75.9065})
        infl     = influence.get(zone_id, {})
        pipe_id  = infl.get('control_pipe_id', 'unknown')
        nm       = ZONE_NAMES.get(zone_id, zone_id)

        # ── engineer_recs (Trigger A) ─────────────────────────────
        if status == 'severe':
            est_gain = round(min(0.85, hei + 0.22) - hei, 3)
            action = (
                f"SEVERE inequity detected in {nm} (HEI = {hei:.3f}). "
                f"Increase ESR outlet pressure by 12–15% on Control Pipe #{pipe_id}. "
                f"Tail-end nodes receiving < 70% of zone average pressure. "
                f"Estimated HEI improvement: {hei:.3f} → {hei + est_gain:.3f} after adjustment."
            )
            urgency = 'URGENT'
            delta = 8.0
        elif status == 'moderate':
            est_gain = round(min(0.85, hei + 0.08) - hei, 3)
            action = (
                f"Moderate imbalance in {nm} (HEI = {hei:.3f}). "
                f"Review valve settings on Control Pipe #{pipe_id}. "
                f"Consider 5–8% pressure increase at ESR outlet. "
                f"Estimated improvement: {hei:.3f} → {hei + est_gain:.3f}."
            )
            urgency = 'MODERATE'
            delta = 4.0
            est_gain = 0.08
        elif status == 'over':
            est_gain = 0.0
            action = (
                f"Over-pressurization in {nm} (HEI = {hei:.3f}). "
                f"Throttle Control Pipe #{pipe_id} by 10–15% to reduce excess pressure. "
                f"Risk of pipe burst — immediate valve adjustment recommended."
            )
            urgency = 'HIGH'
            delta = -6.0
        else:
            est_gain = 0.0
            action = (
                f"{nm} is within equitable range (HEI = {hei:.3f}). "
                f"No pressure adjustment required. Continue routine monitoring."
            )
            urgency = 'LOW'
            delta = 0.0

        conn.execute(text("""
            INSERT INTO engineer_recs
              (zone_id, trigger_type, action_text, valve_id, pipe_id,
               pressure_delta, urgency, estimated_hei_gain, node_coords)
            VALUES
              (:z, 'A_equity', :act, :vid, :pid, :dp, :urg, :gain, :coords)
        """), {
            'z':      zone_id,
            'act':    action,
            'vid':    f"V-{zone_id.upper().replace('_','')}",
            'pid':    pipe_id,
            'dp':     delta,
            'urg':    urgency,
            'gain':   est_gain,
            'coords': json.dumps(zm),
        })
        eng_count += 1

        # ── ward_recs (Trigger A) ─────────────────────────────────
        if status == 'severe':
            escalation = True
            ward_action = (
                f"{nm} water supply is critically below acceptable levels (HEI = {hei:.3f}). "
                f"Escalate to engineering team immediately for valve adjustment."
            )
            reliability_note = "Critical — pressure below minimum service threshold"
        elif status == 'moderate':
            escalation = True
            ward_action = (
                f"{nm} supply is experiencing moderate pressure imbalance (HEI = {hei:.3f}). "
                f"Notify engineering team for review during next maintenance window."
            )
            reliability_note = "Moderate — some tail-end households may have reduced supply"
        elif status == 'over':
            escalation = False
            ward_action = (
                f"{nm} supply pressure is above normal levels (HEI = {hei:.3f}). "
                f"Engineering team is managing pressure reduction. Monitor for complaints."
            )
            reliability_note = "High pressure — watch for burst pipe complaints"
        else:
            escalation = False
            ward_action = (
                f"{nm} supply is operating normally (HEI = {hei:.3f}). "
                f"No action required. Continue routine complaint monitoring."
            )
            reliability_note = "Good — pressure within acceptable range"

        # Count open complaints for this zone from DB
        complaint_count = 0
        try:
            res = conn.execute(
                text("SELECT COUNT(*) FROM citizen_complaints WHERE zone_id=:z AND status='open'"),
                {'z': zone_id}
            ).scalar()
            complaint_count = int(res or 0)
        except Exception:
            pass

        conn.execute(text("""
            INSERT INTO ward_recs
              (zone_id, trigger_type, action_text, escalation_flag,
               service_reliability_note, complaint_count)
            VALUES (:z, 'A_equity', :act, :esc, :note, :cc)
        """), {
            'z':    zone_id,
            'act':  ward_action,
            'esc':  escalation,
            'note': reliability_note,
            'cc':   complaint_count,
        })
        ward_count += 1

    return eng_count, ward_count


# ══════════════════════════════════════════════════════════════════════
#  TRIGGER B — Leak Rules (→ engineer_recs + ward_recs)
#  Uses 'baseline' alerts from V5 (most reliable scenario).
#  Engineer gets technical dispatch instructions.
#  Ward officer gets plain-language action linked to the same alert.
# ══════════════════════════════════════════════════════════════════════

def trigger_b_leak(alerts_all, influence, conn):
    """Writes to engineer_recs (technical) AND ward_recs (plain language) per alert."""
    # Use baseline alerts — these are always present and represent real anomalies
    if isinstance(alerts_all, dict):
        alerts = alerts_all.get('baseline', alerts_all.get('leak', []))
    else:
        alerts = alerts_all if isinstance(alerts_all, list) else []

    eng_count  = 0
    ward_count = 0

    for a in alerts:
        zone_id = a.get('zone_id', '')
        nm      = ZONE_NAMES.get(zone_id, zone_id)
        clps    = float(a.get('clps', 0) or 0)
        sig     = a.get('dominant_signal', 'PDR_n')
        infl    = influence.get(zone_id, {})
        pipe_id = infl.get('control_pipe_id', 'unknown')

        if clps < 0.05:
            continue  # Skip trivial alerts

        # ── Engineer rec: technical, signal-specific ──────────────────
        if sig == 'PDR_n':
            eng_action = (
                f"SUDDEN PRESSURE DROP in {nm} (CLPS={clps:.3f}). "
                f"PDR_n signal dominant — rapid pressure decay detected. "
                f"Dispatch field team to inspect Pipe #{pipe_id} immediately. "
                f"Probable burst or major leakage event."
            )
            urgency      = 'URGENT' if clps > 0.5 else 'HIGH'
            ward_action  = (
                f"A sudden pressure drop has been detected in {nm} (anomaly score: {clps:.3f}). "
                f"This may indicate a pipe burst or major leak. "
                f"SMC engineers have been dispatched. "
                f"Log any citizen reports of no water, flooding, or wet roads immediately."
            )
            ward_note    = f"Active pressure drop — CLPS: {clps:.3f}. Escalate if burst confirmed."
            ward_escalate = True
        elif sig == 'FPI':
            eng_action = (
                f"FLOW-PRESSURE IMBALANCE in {nm} (CLPS={clps:.3f}). "
                f"FPI signal dominant — {int(clps * 15 + 10)}% unaccounted flow detected. "
                f"Probable pipe leakage near Pipe #{pipe_id}. "
                f"Dispatch inspection team to isolate and locate leak."
            )
            urgency      = 'HIGH'
            ward_action  = (
                f"An abnormal flow imbalance has been detected in {nm} (anomaly score: {clps:.3f}). "
                f"More water is entering the zone than expected — possible pipe leakage. "
                f"Monitor citizen pressure complaints. "
                f"Escalate to engineering if multiple households report low pressure."
            )
            ward_note    = f"Flow-pressure imbalance — CLPS: {clps:.3f}. Monitor complaint volume."
            ward_escalate = clps > 0.3
        elif sig == 'NFA':
            eng_action = (
                f"NIGHT FLOW ANOMALY in {nm} (CLPS={clps:.3f}). "
                f"NFA signal dominant — elevated flow detected between 01:00–04:00. "
                f"Inspect Pipe #{pipe_id} for unauthorized extraction or continuous leak. "
                f"Night patrol recommended 01:00–04:00."
            )
            urgency      = 'HIGH'
            ward_action  = (
                f"Unusual water flow was detected in {nm} during off-peak hours (1–4 AM), "
                f"anomaly score: {clps:.3f}. "
                f"This may indicate unauthorized water extraction or a slow nighttime leak. "
                f"Escalate to engineering and log any complaints about low morning pressure."
            )
            ward_note    = f"Night flow anomaly — CLPS: {clps:.3f}. Possible unauthorized extraction."
            ward_escalate = True
        else:  # DDI — demand deviation
            eng_action = (
                f"DEMAND DEVIATION in {nm} (CLPS={clps:.3f}). "
                f"DDI signal dominant — actual consumption deviates {int(clps*20+5)}% "
                f"from expected pattern. Check valve status on Pipe #{pipe_id} "
                f"and verify meter readings."
            )
            urgency      = 'MODERATE'
            ward_action  = (
                f"A demand deviation anomaly has been detected in {nm} "
                f"(anomaly score: {clps:.3f}). "
                f"Actual water consumption is significantly different from the expected pattern. "
                f"Check for valve misalignment, meter issues, or unusual consumption in the ward. "
                f"If citizens are reporting low or no water, escalate to the engineering control room."
            )
            ward_note    = f"Demand deviation — CLPS: {clps:.3f}. Check valve status and meters."
            ward_escalate = False

        # ── Write engineer rec ────────────────────────────────────────
        conn.execute(text("""
            INSERT INTO engineer_recs
              (zone_id, trigger_type, action_text, valve_id, pipe_id,
               pressure_delta, urgency, estimated_hei_gain, node_coords)
            VALUES
              (:z, 'B_leak', :act, :vid, :pid, :dp, :urg, 0.0, :coords)
        """), {
            'z':      zone_id,
            'act':    eng_action,
            'vid':    f"V-{zone_id.upper().replace('_','')}",
            'pid':    pipe_id,
            'dp':     round(-clps * 12, 2),
            'urg':    urgency,
            'coords': json.dumps(ZONE_COORDS.get(zone_id, {'lat': 17.6690, 'lon': 75.9065})),
        })
        eng_count += 1

        # ── Write ward rec — plain language, linked to the same alert ─
        complaint_count = 0
        try:
            res = conn.execute(
                text("SELECT COUNT(*) FROM citizen_complaints WHERE zone_id=:z AND status='open'"),
                {'z': zone_id}
            ).scalar()
            complaint_count = int(res or 0)
        except Exception:
            pass

        conn.execute(text("""
            INSERT INTO ward_recs
              (zone_id, trigger_type, action_text, escalation_flag,
               service_reliability_note, complaint_count)
            VALUES (:z, 'B_leak', :act, :esc, :note, :cc)
        """), {
            'z':    zone_id,
            'act':  ward_action,
            'esc':  ward_escalate,
            'note': ward_note,
            'cc':   complaint_count,
        })
        ward_count += 1

    return eng_count, ward_count


# ══════════════════════════════════════════════════════════════════════
#  TRIGGER C — Burst Rules (→ engineer_recs + commissioner_recs)
#  Uses V6 top-10 burst-risk segments.
# ══════════════════════════════════════════════════════════════════════

def trigger_c_burst(burst_segs, conn):
    """Writes high/moderate burst risk to engineer_recs and one summary to commissioner_recs."""
    eng_count = 0
    high_count = 0
    moderate_count = 0
    high_segments = []

    for s in burst_segs:
        pss        = float(s.get('pss', 0) or 0)
        seg_id     = str(s.get('segment_id', '?'))
        material   = s.get('material', 'Unknown')
        age        = s.get('age', s.get('assumed_age', '?'))
        risk_level = s.get('risk_level', 'MODERATE')
        dom_factor = s.get('dominant_factor', 'unknown')
        lat        = float(s.get('start_lat', s.get('lat_start', 17.68)) or 17.68)
        lon        = float(s.get('start_lon', s.get('lon_start', 75.91)) or 75.91)

        if pss < 0.40:
            continue  # LOW risk — skip

        if risk_level == 'HIGH' or pss >= 0.75:
            action = (
                f"HIGH BURST RISK: Pipe Segment #{seg_id} (PSS = {pss:.3f}). "
                f"Material: {material}, Age: ~{age} years. "
                f"Dominant stress factor: {dom_factor}. "
                f"URGENT: Schedule inspection and consider pre-emptive replacement. "
                f"Failure probability elevated — do not delay beyond 30 days."
            )
            urgency = 'URGENT'
            high_count += 1
            high_segments.append(f"#{seg_id} ({material}, PSS={pss:.2f})")
        else:
            action = (
                f"MODERATE BURST RISK: Pipe Segment #{seg_id} (PSS = {pss:.3f}). "
                f"Material: {material}, Age: ~{age} years. "
                f"Dominant factor: {dom_factor}. "
                f"Schedule maintenance inspection within 60–90 days."
            )
            urgency = 'MODERATE'
            moderate_count += 1

        conn.execute(text("""
            INSERT INTO engineer_recs
              (zone_id, trigger_type, action_text, valve_id, pipe_id,
               pressure_delta, urgency, estimated_hei_gain, node_coords)
            VALUES
              (:z, 'C_burst', :act, :vid, :pid, 0.0, :urg, 0.0, :coords)
        """), {
            'z':      '',   # burst risk is network-wide, not zone-specific
            'act':    action,
            'vid':    '',
            'pid':    seg_id,
            'urg':    urgency,
            'coords': json.dumps({'lat': lat, 'lon': lon}),
        })
        eng_count += 1

    # ── Commissioner rec: budget flag ─────────────────────────────
    if burst_segs:
        total_risk = high_count + moderate_count
        budget_flag = high_count > 0
        city_summary = (
            f"Infrastructure risk assessment complete. "
            f"{high_count} pipe segment(s) at HIGH burst risk. "
            f"{moderate_count} at MODERATE risk. "
            f"Total segments requiring attention: {total_risk}."
        )
        worst_segs_str = ', '.join(high_segments[:3]) if high_segments else 'None'
        comm_action = (
            f"Pipe inspection and replacement budget allocation required for "
            f"{high_count} HIGH-risk segment(s): {worst_segs_str}. "
            f"Estimated NRW reduction if addressed: 4–8%. "
            f"Risk of emergency burst events if deferred."
        )
        conn.execute(text("""
            INSERT INTO commissioner_recs
              (city_summary, worst_zones, budget_flag, theft_summary,
               resolution_rate, trigger_type)
            VALUES (:cs, :wz, :bf, :ts, 0.0, 'C_burst')
        """), {
            'cs': city_summary,
            'wz': json.dumps(high_segments[:5]),
            'bf': budget_flag,
            'ts': 'No theft data — V13 (Phase 4c)',
        })

    return eng_count


# ══════════════════════════════════════════════════════════════════════
#  TRIGGER D — Citizen Advisory (→ citizen_recs ONLY)
#  Runs for every zone, every cycle.
#  CRITICAL: NO infrastructure coords, valve IDs, or pipe segment data.
# ══════════════════════════════════════════════════════════════════════

def trigger_d_citizen(zones, conn):
    """
    Writes to citizen_recs — plain language, no technical infrastructure data.
    """
    count = 0
    for z in zones:
        zone_id = z.get('zone_id', '')
        hei     = float(z.get('hei', 0) or 0)
        status  = z.get('status', 'equitable')
        nm      = ZONE_NAMES.get(zone_id, zone_id)

        if status == 'severe':
            supply_status = 'Intermittent'
            advisory = (
                f"Water supply in {nm} may be reduced at some households. "
                f"The municipal team is actively working to restore normal pressure. "
                f"Store water if possible. Expected restoration: within 2-4 supply cycles."
            )
            guidance = (
                f"If you are experiencing no water or very low pressure, "
                f"please file a complaint below. Include your landmark and contact number."
            )
            restoration = "Within 2-4 supply cycles (6-24 hours)"
        elif status == 'moderate':
            supply_status = 'Normal'
            advisory = (
                f"Water supply in {nm} is operating at near-normal levels. "
                f"Some households may experience slightly reduced pressure during peak hours. "
                f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM."
            )
            guidance = (
                f"If pressure seems lower than usual, wait for the next supply cycle. "
                f"Persistent issues? File a complaint below."
            )
            restoration = "Currently operational — monitoring in progress"
        elif status == 'over':
            supply_status = 'Normal'
            advisory = (
                f"Water supply in {nm} is operating normally. "
                f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM."
            )
            guidance = (
                f"If you notice unusually high water flow or pipe vibration, "
                f"report it below. The municipal team is monitoring pressure levels."
            )
            restoration = "No disruption expected"
        else:
            supply_status = 'Normal'
            advisory = (
                f"Water supply in {nm} is operating normally (HEI = {hei:.2f}). "
                f"Supply window: 6:00 AM – 8:00 AM and 5:00 PM – 7:00 PM. "
                f"Ensure your overhead tank is filled during supply hours."
            )
            guidance = (
                f"For complaints about water quality, quantity, or billing, "
                f"use the form below. The municipal team responds within 24 hours."
            )
            restoration = "No disruption — normal operation"

        conn.execute(text("""
            INSERT INTO citizen_recs
              (zone_id, supply_status, advisory_text,
               complaint_guidance, estimated_restoration)
            VALUES (:z, :ss, :adv, :guide, :rest)
        """), {
            'z':     zone_id,
            'ss':    supply_status,
            'adv':   advisory,
            'guide': guidance,
            'rest':  restoration,
        })
        count += 1

    return count


# ══════════════════════════════════════════════════════════════════════
#  TRIGGER A (COMMISSIONER SUMMARY)
#  Separate from per-zone equity — city-wide summary for commissioner.
# ══════════════════════════════════════════════════════════════════════

def trigger_a_commissioner(zones, conn):
    """Writes city-wide equity summary to commissioner_recs."""
    if not zones:
        return 0

    heis = [float(z.get('hei', 0) or 0) for z in zones]
    cwei = sum(heis) / len(heis) if heis else 0
    severe_zones  = [z['zone_id'] for z in zones if z.get('status') == 'severe']
    moderate_zones = [z['zone_id'] for z in zones if z.get('status') == 'moderate']
    over_zones    = [z['zone_id'] for z in zones if z.get('status') == 'over']

    worst = sorted(zones, key=lambda z: float(z.get('hei', 0) or 0))[:3]
    worst_ids = [z['zone_id'] for z in worst]

    if cwei >= 0.85:
        status_label = 'EQUITABLE'
    elif cwei >= 0.70:
        status_label = 'MODERATE IMBALANCE'
    else:
        status_label = 'SEVERE INEQUITY'

    city_summary = (
        f"City-Wide Equity Index (CWEI): {cwei:.3f} — {status_label}. "
        f"Monitoring {len(zones)} zones. "
        f"{len(severe_zones)} zone(s) in SEVERE inequity, "
        f"{len(moderate_zones)} in MODERATE imbalance, "
        f"{len(over_zones)} over-pressurized. "
        f"Estimated NRW: 18% (baseline)."
    )

    worst_detail = ', '.join([
        f"{ZONE_NAMES.get(z,'?')} (HEI={float(next((x.get('hei',0) for x in zones if x['zone_id']==z), 0)):.3f})"
        for z in worst_ids
    ])

    action = (
        f"City equity status requires {'immediate attention' if severe_zones else 'routine monitoring'}. "
        f"Priority zones: {worst_detail if worst_detail else 'None'}. "
        f"{'URGENT: allocate field teams to ' + str(len(severe_zones)) + ' severe zone(s).' if severe_zones else 'Continue current operations.'}"
    )

    conn.execute(text("""
        INSERT INTO commissioner_recs
          (city_summary, worst_zones, budget_flag, theft_summary,
           resolution_rate, trigger_type)
        VALUES (:cs, :wz, :bf, :ts, :rr, 'A_equity')
    """), {
        'cs': city_summary + ' ' + action,
        'wz': json.dumps(worst_ids),
        'bf': len(severe_zones) > 0,
        'ts': 'Theft detection (V13) coming in Phase 4c.',
        'rr': 0.0,
    })

    return 1


# ══════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def run_v7():
    """
    Main V7 function. Can be called:
    - Directly: python scripts/v7_recommendations.py
    - By APScheduler: every 5 minutes from backend/app.py
    """
    print('=' * 62)
    print('  V7 · Role-Partitioned Recommendation Engine')
    print('=' * 62)

    # ── 1. Load analytics data ────────────────────────────────────
    zones, alerts_all, burst_segs = load_analytics_data()
    pipes_df, nodes_df, infra_df = load_network_data()

    print(f'  Loaded: {len(zones)} zones, '
          f'{sum(len(v) for v in alerts_all.values() if isinstance(v, list))} alerts, '
          f'{len(burst_segs)} burst segments')

    if not zones:
        print('  [ABORT] No zone data. Run V4 first.')
        return

    # ── 2. Build influence map ────────────────────────────────────
    print('  Building influence map...', end=' ')
    influence = build_influence_map(pipes_df, nodes_df, infra_df)
    print(f'{len(influence)} zones mapped')

    # ── 3. Write to DB ────────────────────────────────────────────
    print('  Running 5 triggers...')
    eng_total = ward_total = comm_total = cit_total = 0

    try:
        # Import engine lazily so module import doesn't crash if DB is unavailable
        from backend.database import engine
        with engine.connect() as conn:
            # Clear records older than 24h
            clear_old_recs(conn)

            # Trigger A — Equity (engineer + ward + commissioner)
            ec, wc = trigger_a_equity(zones, influence, conn)
            eng_total  += ec
            ward_total += wc
            comm_total += trigger_a_commissioner(zones, conn)
            # Citizen advisories (Trigger D runs alongside A)
            cit_total  += trigger_d_citizen(zones, conn)
            print(f'  [A] Equity  → {ec} engineer, {wc} ward, 1 commissioner, {cit_total} citizen')

            # Trigger B — Leak (engineer + ward)  ← NOW WRITES WARD RECS TOO
            bc, bwc = trigger_b_leak(alerts_all, influence, conn)
            eng_total  += bc
            ward_total += bwc
            print(f'  [B] Leak    → {bc} engineer_recs, {bwc} ward_recs')

            # Trigger C — Burst (engineer + commissioner)
            cc = trigger_c_burst(burst_segs, conn)
            eng_total  += cc
            comm_total += 1 if burst_segs else 0
            print(f'  [C] Burst   → {cc} engineer, 1 commissioner_recs')

            print(f'  [D] Citizen → already written above ({cit_total} rows)')

            # Log the run
            conn.execute(text("""
                INSERT INTO v7_run_log
                  (status, zones_processed, recs_generated,
                   engineer_count, ward_count, commissioner_count, citizen_count)
                VALUES ('success', :zp, :rg, :ec, :wc, :cc, :cit)
            """), {
                'zp':  len(zones),
                'rg':  eng_total + ward_total + comm_total + cit_total,
                'ec':  eng_total,
                'wc':  ward_total,
                'cc':  comm_total,
                'cit': cit_total,
            })
            conn.commit()
            print('  [DB] Committed to PostgreSQL.')

            # ── 4. Also write JSON cache so file-based router can read it ──
            # Read back what we just wrote and save to outputs/v7_recommendations.json
            _write_json_cache(conn, zones, alerts_all, burst_segs, influence)

    except Exception as e:
        print(f'  [WARN] DB write failed: {e}')
        print('  Falling back to JSON-only mode (no PostgreSQL required).')
        # Write JSON directly from in-memory data (no DB needed)
        _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence)

    total = eng_total + ward_total + comm_total + cit_total
    print(f'\n  ✅ V7 complete. Total recs generated: {total}')
    print(f'     engineer_recs    : {eng_total}')
    print(f'     ward_recs        : {ward_total}')
    print(f'     commissioner_recs: {comm_total}')
    print(f'     citizen_recs     : {cit_total}')
    print('=' * 62)


# ══════════════════════════════════════════════════════════════════════
#  JSON CACHE HELPERS
#  Write outputs/v7_recommendations.json so the fast router endpoint
#  can serve recs without hitting the DB on every request.
# ══════════════════════════════════════════════════════════════════════

def _write_json_cache(conn, zones, alerts_all, burst_segs, influence):
    """
    Read what was just written to DB and dump it all to the JSON cache.
    Called after a successful DB commit inside run_v7.
    """
    try:
        eng_rows  = conn.execute(text(
            "SELECT zone_id, trigger_type, action_text, valve_id, pipe_id, "
            "urgency, estimated_hei_gain FROM engineer_recs ORDER BY created_at DESC LIMIT 100"
        )).fetchall()
        ward_rows = conn.execute(text(
            "SELECT zone_id, trigger_type, action_text, escalation_flag, "
            "service_reliability_note, complaint_count FROM ward_recs ORDER BY created_at DESC LIMIT 100"
        )).fetchall()
        comm_rows = conn.execute(text(
            "SELECT city_summary, worst_zones, budget_flag, theft_summary, "
            "resolution_rate, trigger_type FROM commissioner_recs ORDER BY created_at DESC LIMIT 20"
        )).fetchall()
        cit_rows  = conn.execute(text(
            "SELECT zone_id, supply_status, advisory_text, complaint_guidance, "
            "estimated_restoration FROM citizen_recs ORDER BY zone_id, created_at DESC LIMIT 50"
        )).fetchall()

        data = {
            "engineer_recs": [
                {"zone_id": r[0] or '', "trigger_type": r[1] or '', "action_text": r[2] or '',
                 "valve_id": r[3] or '', "pipe_id": r[4] or '', "urgency": r[5] or 'LOW',
                 "estimated_hei_gain": float(r[6] or 0)}
                for r in eng_rows
            ],
            "ward_recs": [
                {"zone_id": r[0] or '', "trigger_type": r[1] or '', "action_text": r[2] or '',
                 "escalation_flag": bool(r[3]), "service_reliability_note": r[4] or '',
                 "complaint_count": int(r[5] or 0)}
                for r in ward_rows
            ],
            "commissioner_recs": [
                {"city_summary": r[0] or '', "worst_zones": _safe_json_loads(r[1]),
                 "budget_flag": bool(r[2]), "theft_summary": r[3] or '',
                 "resolution_rate": float(r[4] or 0), "trigger_type": r[5] or ''}
                for r in comm_rows
            ],
            "citizen_recs": [
                {"zone_id": r[0] or '',
                 "zone_name": "Zone {}".format((r[0] or '').replace('zone_', '')),
                 "supply_status": r[1] or 'Normal', "advisory_text": r[2] or '',
                 "complaint_guidance": r[3] or '', "estimated_restoration": r[4] or 'N/A'}
                for r in cit_rows
            ],
            "updated_at": datetime.now().isoformat(),
        }
        _save_json(data)
        print(f'  [JSON] Cache written: {len(data["engineer_recs"])} eng, '
              f'{len(data["ward_recs"])} ward, '
              f'{len(data["commissioner_recs"])} comm, '
              f'{len(data["citizen_recs"])} citizen')
    except Exception as e:
        print(f'  [WARN] JSON cache write (from DB) failed: {e}')
        # Fall back to generating from memory
        _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence)


def _write_json_cache_from_memory(zones, alerts_all, burst_segs, influence):
    """
    Build and write the JSON cache entirely from in-memory analytics data.
    Used when PostgreSQL is unavailable (dev/file mode).
    This is the same logic as the triggers but writes to a dict instead of DB.
    """
    engineer_recs    = []
    ward_recs        = []
    commissioner_recs = []
    citizen_recs     = []

    # ── Trigger A: Equity ─────────────────────────────────────────
    for z in zones:
        zone_id = z.get('zone_id', '')
        nm      = ZONE_NAMES.get(zone_id, zone_id)
        hei     = float(z.get('hei', 0) or 0)
        status  = z.get('status', 'equitable')
        infl    = influence.get(zone_id, {})
        pipe_id = infl.get('control_pipe_id', f'V-{zone_id.upper().replace("_","")}')

        if status == 'severe':
            gain = round(0.85 - hei, 3)
            engineer_recs.append({
                "zone_id": zone_id, "trigger_type": "A_equity", "urgency": "URGENT",
                "valve_id": pipe_id, "pipe_id": "", "estimated_hei_gain": gain,
                "action_text": (
                    f"[{nm}] HEI critically low at {hei:.3f} (target ≥ 0.85). "
                    f"Increase ESR outlet pressure 10–15% via control pipe {pipe_id}. "
                    f"Estimated HEI gain: +{gain:.3f}. Dispatch field team for verification."
                ),
            })
            ward_recs.append({
                "zone_id": zone_id, "trigger_type": "A_equity",
                "escalation_flag": True, "complaint_count": 0,
                "action_text": (
                    f"{nm} supply is severely inequitable (HEI = {hei:.3f}). "
                    f"Citizens in tail-end areas may receive little or no water. "
                    f"Escalate to engineering control room immediately."
                ),
                "service_reliability_note": f"HEI: {hei:.3f} — Severe. Tail-end households affected.",
            })
        elif status == 'moderate':
            engineer_recs.append({
                "zone_id": zone_id, "trigger_type": "A_equity", "urgency": "MODERATE",
                "valve_id": pipe_id, "pipe_id": "", "estimated_hei_gain": round(0.85 - hei, 3),
                "action_text": (
                    f"[{nm}] HEI is {hei:.3f} — moderate imbalance. "
                    f"Review valve settings on distribution mains."
                ),
            })
            ward_recs.append({
                "zone_id": zone_id, "trigger_type": "A_equity",
                "escalation_flag": False, "complaint_count": 0,
                "action_text": (
                    f"{nm} shows moderate pressure imbalance (HEI = {hei:.3f}). "
                    f"Some tail-end households may face reduced pressure during peak hours. "
                    f"Monitor complaints and report persistent issues to engineering."
                ),
                "service_reliability_note": f"HEI: {hei:.3f} — Moderate. Monitor peak-hour supply.",
            })

        # Citizen advisory per zone
        if status == 'severe':
            cit_status, cit_adv = 'Intermittent', (
                f"Water supply in {nm} may be limited. SMC engineers are working to restore pressure. "
                f"Please store water during supply hours.")
            cit_guid = "If you have no water, submit a complaint using the form below."
            cit_rest = "Engineers are working on it. Check back in 2–4 hours."
        elif status == 'moderate':
            cit_status, cit_adv = 'Normal', (
                f"Water supply in {nm} is near-normal. Some households may see slightly reduced "
                f"pressure during peak hours (6–8 AM, 5–8 PM).")
            cit_guid = "Persistent low pressure? File a complaint below."
            cit_rest = "Currently operational — monitoring in progress"
        else:
            cit_status, cit_adv = 'Normal', (
                f"Water supply in {nm} is operating normally (HEI = {hei:.2f}). "
                f"Supply window: 6–8 AM and 5–8 PM. Fill overhead tanks during supply hours.")
            cit_guid = "For any supply issues, submit a complaint using the form below."
            cit_rest = "No disruption — normal operation"

        citizen_recs.append({
            "zone_id": zone_id,
            "zone_name": "Zone {}".format(zone_id.replace('zone_', '')),
            "supply_status": cit_status, "advisory_text": cit_adv,
            "complaint_guidance": cit_guid, "estimated_restoration": cit_rest,
        })

    # ── Trigger B: Leak/Anomaly alerts ────────────────────────────
    if isinstance(alerts_all, dict):
        alerts = alerts_all.get('baseline', alerts_all.get('leak', []))
    else:
        alerts = alerts_all if isinstance(alerts_all, list) else []

    for a in alerts:
        zone_id = a.get('zone_id', '')
        nm      = ZONE_NAMES.get(zone_id, zone_id)
        clps    = float(a.get('clps', 0) or 0)
        sig     = a.get('dominant_signal', 'DDI')
        infl    = influence.get(zone_id, {})
        pipe_id = infl.get('control_pipe_id', f'V-{zone_id.upper().replace("_","")}')

        if clps < 0.05:
            continue

        sig_map = {
            'PDR_n': ('URGENT' if clps > 0.5 else 'HIGH',
                      f"SUDDEN PRESSURE DROP in {nm} (CLPS={clps:.3f}). Dispatch team to Pipe #{pipe_id}.",
                      f"Sudden pressure drop detected — possible burst. Log flooding reports immediately.", True),
            'FPI':   ('HIGH',
                      f"FLOW-PRESSURE IMBALANCE in {nm} (CLPS={clps:.3f}). Probable leakage near Pipe #{pipe_id}.",
                      f"Flow imbalance detected — possible pipe leakage. Monitor complaint volume.", clps > 0.3),
            'NFA':   ('HIGH',
                      f"NIGHT FLOW ANOMALY in {nm} (CLPS={clps:.3f}). Inspect Pipe #{pipe_id} for unauthorized extraction.",
                      f"Night flow anomaly — possible unauthorized extraction. Escalate to engineering.", True),
            'DDI':   ('MODERATE',
                      f"DEMAND DEVIATION in {nm} (CLPS={clps:.3f}). Check valve status on Pipe #{pipe_id}.",
                      f"Demand deviation detected in {nm} (CLPS={clps:.3f}). "
                      f"Actual consumption differs from expected pattern. "
                      f"Check for valve misalignment or meter issues. "
                      f"If citizens report low water, escalate to engineering.", False),
        }
        urg, eng_txt, ward_txt, escalate = sig_map.get(sig, sig_map['DDI'])

        engineer_recs.append({
            "zone_id": zone_id, "trigger_type": "B_leak", "urgency": urg,
            "valve_id": f"V-{zone_id.upper().replace('_','')}", "pipe_id": pipe_id,
            "estimated_hei_gain": 0.0, "action_text": eng_txt,
        })
        ward_recs.append({
            "zone_id": zone_id, "trigger_type": "B_leak",
            "escalation_flag": escalate, "complaint_count": 0,
            "action_text": ward_txt,
            "service_reliability_note": f"Active anomaly — CLPS: {clps:.3f} · Signal: {sig}.",
        })

    # ── Trigger C: Burst risk → engineer + commissioner ───────────
    high_burst = [s for s in burst_segs if s.get('risk_level') == 'HIGH']
    mod_burst  = [s for s in burst_segs if s.get('risk_level') == 'MODERATE']

    for s in high_burst:
        seg = s.get('segment_id', '?')
        pss = float(s.get('pss', 0))
        mat = s.get('material', 'Unknown')
        age = s.get('age', '?')
        dom = s.get('dominant_factor', 'PSI_n')
        engineer_recs.append({
            "zone_id": "", "trigger_type": "C_burst",
            "urgency": "URGENT" if pss > 0.85 else "HIGH",
            "valve_id": "", "pipe_id": str(seg), "estimated_hei_gain": 0.0,
            "action_text": (
                f"HIGH BURST RISK: Pipe #{seg} (PSS={pss:.3f}). "
                f"Material: {mat}, Age: ~{age}yr. Factor: {dom}. "
                f"Schedule urgent inspection. Pre-emptive replacement if PSS > 0.90."
            ),
        })

    if high_burst or mod_burst:
        commissioner_recs.append({
            "trigger_type": "C_burst",
            "city_summary": (
                f"{len(high_burst)} pipe segment(s) at HIGH burst risk, {len(mod_burst)} MODERATE. "
                f"Capital expenditure for emergency pipe replacement should be considered."
            ),
            "worst_zones": [str(s.get('segment_id', '?')) for s in high_burst[:3]],
            "budget_flag": len(high_burst) > 0,
            "theft_summary": "Theft detection (V13) coming in Phase 4c.",
            "resolution_rate": 0.0,
        })

    # ── Trigger A Commissioner summary ────────────────────────────
    if zones:
        heis  = [float(z.get('hei', 0) or 0) for z in zones]
        cwei  = sum(heis) / len(heis)
        worst = sorted(zones, key=lambda z: float(z.get('hei', 0) or 0))[:3]
        sev   = [z for z in zones if z.get('status') == 'severe']
        commissioner_recs.insert(0, {
            "trigger_type": "A_equity",
            "city_summary": (
                f"CWEI: {cwei:.3f} — "
                f"{'SEVERE INEQUITY' if cwei < 0.70 else 'MODERATE IMBALANCE' if cwei < 0.85 else 'EQUITABLE'}. "
                f"{len(sev)} zone(s) severe. "
                f"Priority: {', '.join(ZONE_NAMES.get(z['zone_id'], z['zone_id']) for z in worst[:2])}."
            ),
            "worst_zones": [z['zone_id'] for z in worst],
            "budget_flag": len(sev) > 0,
            "theft_summary": "Theft detection (V13) coming in Phase 4c.",
            "resolution_rate": 0.0,
        })

    # Sort engineer recs by urgency
    _order = {'URGENT': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}
    engineer_recs.sort(key=lambda r: _order.get(r.get('urgency', 'LOW'), 3))

    data = {
        "engineer_recs":     engineer_recs,
        "ward_recs":         ward_recs,
        "commissioner_recs": commissioner_recs,
        "citizen_recs":      citizen_recs,
        "updated_at":        datetime.now().isoformat(),
        "source":            "v7_memory",
    }
    _save_json(data)
    print(f'  [JSON] Memory cache written: {len(engineer_recs)} eng, '
          f'{len(ward_recs)} ward, '
          f'{len(commissioner_recs)} comm, '
          f'{len(citizen_recs)} citizen')


def _save_json(data):
    """Write data to outputs/v7_recommendations.json."""
    out_path = os.path.join(OUTPUTS, 'v7_recommendations.json')
    os.makedirs(OUTPUTS, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def _safe_json_loads(val):
    if not val:
        return []
    try:
        return json.loads(val)
    except Exception:
        return [val] if val else []


if __name__ == '__main__':
    run_v7()


# Hydro-Equity Engine V7 — Recommendation Generator
# Generates logical, data-driven recommendations based on live HEI/zone metrics.
# NOT hardcoded — all recommendations derive from actual zone data.
# """

# from __future__ import annotations
# import logging
# from dataclasses import dataclass, field, asdict
# from datetime import datetime, timezone
# from typing import Optional
# import uuid

# logger = logging.getLogger(__name__)

# # ── Thresholds (tunable) ──────────────────────────────────────────
# HEI_EQUITABLE  = 0.85
# HEI_MODERATE   = 0.70
# CLPS_ANOMALY   = 0.12
# PRESSURE_LOW   = 20.0   # m head — below this is critical
# PRESSURE_HIGH  = 60.0   # m head — above this risks pipe burst
# NRW_HIGH       = 0.20   # 20% NRW is high
# COMPLAINT_HIGH = 10     # complaints/zone/day


# @dataclass
# class Recommendation:
#     rec_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
#     title: str = ""
#     description: str = ""
#     priority: str = "medium"         # critical | high | medium | low
#     action_type: str = ""            # valve_adjust | pressure_boost | maintenance | investigation | policy
#     zone_id: Optional[str] = None
#     scope: str = "zone"              # zone | city | strategic
#     is_strategic: bool = False
#     estimated_hei_gain: Optional[float] = None
#     estimated_impact: Optional[str] = None
#     budget_estimate: Optional[str] = None
#     generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

#     def to_dict(self) -> dict:
#         return {k: v for k, v in asdict(self).items() if v is not None}


# def generate_recommendations(zones: list[dict], city_summary: dict | None = None) -> list[Recommendation]:
#     """
#     Generate logical recommendations from live zone data.
    
#     zones: list of zone dicts with keys:
#         zone_id, hei_score, status, avg_pressure_mh, open_complaints,
#         clps_score, nrw_pct, zes_daily, demand_deviation, pipe_age_avg
    
#     city_summary: optional dict with city-level aggregates
#     """
#     recs: list[Recommendation] = []

#     if not zones:
#         logger.warning("V7: No zone data provided — skipping recommendation generation")
#         return recs

#     for zone in zones:
#         zone_id      = zone.get("zone_id", "unknown")
#         hei          = float(zone.get("hei_score", 1.0))
#         status       = zone.get("status", "equitable")
#         pressure     = zone.get("avg_pressure_mh") or zone.get("pressure_mh")
#         complaints   = zone.get("open_complaints", 0) or 0
#         clps         = zone.get("clps_score") or zone.get("demand_deviation", 0.0)
#         nrw          = zone.get("nrw_pct", 0.0) or 0.0
#         pipe_age     = zone.get("pipe_age_avg")
#         supply_status = zone.get("supply_status", "normal")

#         # ── 1. Severe HEI — critical action needed ──
#         if hei < HEI_MODERATE:
#             gain = round(HEI_MODERATE - hei + 0.05, 3)
#             recs.append(Recommendation(
#                 title=f"Critical Equity Intervention — {zone_id.replace('_',' ').title()}",
#                 description=(
#                     f"{zone_id.replace('_',' ').title()} has a critically low HEI of {hei:.3f} (threshold: {HEI_MODERATE}). "
#                     f"Immediate review of inlet valve settings and distribution scheduling is required. "
#                     f"Consider emergency pressure boost and reallocation from adjacent high-equity zones."
#                 ),
#                 priority="critical",
#                 action_type="valve_adjust",
#                 zone_id=zone_id,
#                 scope="zone",
#                 estimated_hei_gain=gain,
#                 estimated_impact=f"HEI improvement of +{gain:.3f} expected within 48h"
#             ))

#         # ── 2. Moderate HEI — scheduled intervention ──
#         elif hei < HEI_EQUITABLE:
#             gain = round(HEI_EQUITABLE - hei + 0.02, 3)
#             recs.append(Recommendation(
#                 title=f"Pressure Rebalancing — {zone_id.replace('_',' ').title()}",
#                 description=(
#                     f"HEI of {hei:.3f} in {zone_id} is below equitable threshold ({HEI_EQUITABLE}). "
#                     f"Scheduled pressure rebalancing recommended. Check valve positions on main inlet "
#                     f"and review ZES scheduling to improve supply uniformity."
#                 ),
#                 priority="high",
#                 action_type="pressure_boost",
#                 zone_id=zone_id,
#                 scope="zone",
#                 estimated_hei_gain=gain,
#                 estimated_impact=f"Projected HEI gain: +{gain:.3f}"
#             ))

#         # ── 3. Low pressure ──
#         if pressure is not None:
#             pressure = float(pressure)
#             if pressure < PRESSURE_LOW:
#                 recs.append(Recommendation(
#                     title=f"Low Pressure Alert — {zone_id.replace('_',' ').title()}",
#                     description=(
#                         f"Average pressure in {zone_id} is {pressure:.1f}m head, below the minimum threshold "
#                         f"of {PRESSURE_LOW}m. This causes supply inadequacy at tail-end connections. "
#                         f"Check for blockages, partially-closed valves, or ESR level issues."
#                     ),
#                     priority="high" if pressure < 15 else "medium",
#                     action_type="investigation",
#                     zone_id=zone_id,
#                     scope="zone",
#                     estimated_impact=f"Restoring to {PRESSURE_LOW+5:.0f}m will improve {zone_id} coverage"
#                 ))

#             elif pressure > PRESSURE_HIGH:
#                 recs.append(Recommendation(
#                     title=f"High Pressure Risk — {zone_id.replace('_',' ').title()}",
#                     description=(
#                         f"Pressure in {zone_id} is {pressure:.1f}m head, exceeding safe limit of {PRESSURE_HIGH}m. "
#                         f"Risk of pipe stress and burst events. Recommend installing or adjusting pressure "
#                         f"reducing valve (PRV) at zone inlet."
#                     ),
#                     priority="high",
#                     action_type="valve_adjust",
#                     zone_id=zone_id,
#                     scope="zone",
#                     estimated_impact="Reduces burst risk and NRW"
#                 ))

#         # ── 4. CLPS anomaly (Demand Deviation Index) ──
#         if clps is not None and abs(float(clps)) > CLPS_ANOMALY:
#             clps_val = float(clps)
#             direction = "over-consumption" if clps_val > 0 else "under-supply"
#             recs.append(Recommendation(
#                 title=f"DDI Anomaly Investigation — {zone_id.replace('_',' ').title()}",
#                 description=(
#                     f"Demand Deviation Index (CLPS) of {clps_val:.3f} in {zone_id} exceeds anomaly threshold "
#                     f"(±{CLPS_ANOMALY}). This indicates potential {direction}. "
#                     f"Verify valve status, check for unauthorised connections, and audit meter readings."
#                 ),
#                 priority="medium",
#                 action_type="investigation",
#                 zone_id=zone_id,
#                 scope="zone",
#                 estimated_impact="Resolving DDI reduces NRW and improves equity"
#             ))

#         # ── 5. High complaints ──
#         if int(complaints) >= COMPLAINT_HIGH:
#             recs.append(Recommendation(
#                 title=f"High Complaint Volume — {zone_id.replace('_',' ').title()}",
#                 description=(
#                     f"{zone_id} has {complaints} open complaints — above the threshold of {COMPLAINT_HIGH}. "
#                     f"Prioritise field inspection to identify common issues. "
#                     f"Common causes: intermittent supply, low pressure, or localised pipe deterioration."
#                 ),
#                 priority="medium",
#                 action_type="maintenance",
#                 zone_id=zone_id,
#                 scope="zone",
#                 estimated_impact=f"Resolving {complaints} complaints improves citizen satisfaction"
#             ))

#         # ── 6. High NRW per zone ──
#         if float(nrw) > NRW_HIGH:
#             recs.append(Recommendation(
#                 title=f"Elevated NRW — {zone_id.replace('_',' ').title()}",
#                 description=(
#                     f"Non-Revenue Water (NRW) in {zone_id} is {nrw*100:.1f}%, exceeding the {NRW_HIGH*100:.0f}% "
#                     f"threshold. Conduct distribution loss audit: check meter accuracy, identify illegal "
#                     f"connections, and inspect high-age pipes for leakage."
#                 ),
#                 priority="medium",
#                 action_type="maintenance",
#                 zone_id=zone_id,
#                 scope="zone",
#                 estimated_impact=f"Reducing NRW to {NRW_HIGH*100:.0f}% recovers significant water volume"
#             ))

#         # ── 7. Pipe age (if available) ──
#         if pipe_age is not None and float(pipe_age) > 25:
#             recs.append(Recommendation(
#                 title=f"Aging Infrastructure — {zone_id.replace('_',' ').title()}",
#                 description=(
#                     f"Average pipe age in {zone_id} is {float(pipe_age):.0f} years, exceeding the "
#                     f"recommended 25-year service life. Schedule condition assessment and phased replacement "
#                     f"to prevent burst events and reduce NRW."
#                 ),
#                 priority="low",
#                 action_type="maintenance",
#                 zone_id=zone_id,
#                 scope="zone",
#                 budget_estimate="To be determined by condition survey",
#                 estimated_impact="Long-term NRW reduction and reliability improvement"
#             ))

#     # ── City-level strategic recommendations ──
#     if city_summary:
#         recs += _generate_strategic(zones, city_summary)

#     # Sort: critical → high → medium → low, then by zone
#     priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
#     recs.sort(key=lambda r: (priority_order.get(r.priority, 4), r.zone_id or ""))
#     logger.info(f"V7: Generated {len(recs)} recommendations for {len(zones)} zones")
#     return recs


# def _generate_strategic(zones: list[dict], city: dict) -> list[Recommendation]:
#     """Generate city-level strategic recommendations."""
#     recs = []
#     city_hei     = float(city.get("city_equity_index", 1.0))
#     severe_zones = [z for z in zones if float(z.get("hei_score", 1.0)) < HEI_MODERATE]
#     moderate_zones = [z for z in zones if HEI_MODERATE <= float(z.get("hei_score", 1.0)) < HEI_EQUITABLE]
#     city_nrw     = float(city.get("nrw_pct", 0.0) or 0.0)

#     # Strategic: multiple severe zones
#     if len(severe_zones) >= 2:
#         zone_names = ", ".join(z["zone_id"].replace("_"," ").title() for z in severe_zones[:4])
#         recs.append(Recommendation(
#             title="City-Wide Equity Emergency — Multi-Zone Intervention",
#             description=(
#                 f"{len(severe_zones)} zones ({zone_names}) have HEI below {HEI_MODERATE}. "
#                 f"This requires a coordinated city-wide intervention: emergency redistribution plan, "
#                 f"temporary tanker supply to worst-affected areas, and expedited infrastructure audit. "
#                 f"Commissioner approval required for resource reallocation."
#             ),
#             priority="critical",
#             action_type="policy",
#             scope="strategic",
#             is_strategic=True,
#             estimated_hei_gain=round((HEI_EQUITABLE - city_hei) * 0.6, 3),
#             estimated_impact=f"Could bring {len(severe_zones)} zones to moderate status within 2 weeks"
#         ))

#     # Strategic: city NRW high
#     if city_nrw > NRW_HIGH:
#         recs.append(Recommendation(
#             title="City-Wide NRW Reduction Programme",
#             description=(
#                 f"City-wide NRW of {city_nrw*100:.1f}% represents significant water loss. "
#                 f"Recommend initiating a structured NRW reduction programme: district metered areas (DMA) "
#                 f"setup, automated leak detection, and meter replacement on aged connections. "
#                 f"Target: reduce NRW to below {NRW_HIGH*100:.0f}% within 12 months."
#             ),
#             priority="high",
#             action_type="policy",
#             scope="strategic",
#             is_strategic=True,
#             estimated_impact=f"Recovering {city_nrw*100 - NRW_HIGH*100:.1f}% NRW improves supply for all zones"
#         ))

#     # Strategic: equity gap between best and worst zone
#     if len(zones) >= 2:
#         sorted_by_hei = sorted(zones, key=lambda z: float(z.get("hei_score", 1.0)))
#         worst = sorted_by_hei[0]
#         best  = sorted_by_hei[-1]
#         hei_gap = float(best.get("hei_score", 1.0)) - float(worst.get("hei_score", 1.0))
#         if hei_gap > 0.30:
#             recs.append(Recommendation(
#                 title="Equity Gap Reduction — Zone Rebalancing Plan",
#                 description=(
#                     f"HEI gap of {hei_gap:.3f} between best zone ({best['zone_id']}, HEI {float(best.get('hei_score',1)):.3f}) "
#                     f"and worst zone ({worst['zone_id']}, HEI {float(worst.get('hei_score',1)):.3f}) is significant. "
#                     f"Recommend a zone rebalancing study: identify surplus capacity in high-HEI zones and "
#                     f"establish systematic reallocation schedule to reduce gap to below 0.20."
#                 ),
#                 priority="high",
#                 action_type="policy",
#                 scope="strategic",
#                 is_strategic=True,
#                 estimated_hei_gain=round(hei_gap * 0.4, 3),
#                 estimated_impact="Reduces inter-zone inequity, improves overall CWEI"
#             ))

#     return recs


# def format_for_api(recs: list[Recommendation], zone_id: str | None = None) -> dict:
#     """Format recommendations for API response, optionally filtered by zone."""
#     filtered = recs
#     if zone_id:
#         # Include zone-specific + strategic recs
#         filtered = [r for r in recs if r.zone_id == zone_id or r.is_strategic or r.scope in ('strategic','city')]

#     return {
#         "recommendations": [r.to_dict() for r in filtered],
#         "total":     len(filtered),
#         "generated_at": datetime.now(timezone.utc).isoformat(),
#         "engine":    "V7"
#     }