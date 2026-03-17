# """
# Hydro-Equity Engine — Phase 4b
# backend/routers/recommendations.py

# V7 Role-Partitioned Recommendation Engine & API Endpoints.

# Strategy:
#   1. Try PostgreSQL first (Phase 4b production mode)
#   2. If DB unavailable, fall back to reading v7_recommendations.json (dev/file mode)
#   3. If JSON missing, generate live from V4/V5/V6 outputs on-the-fly (always works)

# Endpoints:
#   GET  /recommendations/engineer           (engineer + field_operator)
#   GET  /recommendations/ward               (ward_officer, zone-filtered)
#   GET  /recommendations/commissioner       (commissioner)
#   GET  /recommendations/citizen            (PUBLIC — no auth)
#   GET  /recommendations/updated-at         (all authenticated roles)
#   POST /recommendations/rebuild            (engineer — trigger V7 re-run)
# """

# import os
# import json
# from datetime import datetime
# from fastapi import APIRouter, Depends, HTTPException, status
# from sqlalchemy import text

# from backend.auth import get_current_user
# from backend.database import engine

# router = APIRouter(prefix="/recommendations", tags=["Recommendations"])

# OUTPUTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'outputs'))
# V7_JSON     = os.path.join(OUTPUTS_DIR, 'v7_recommendations.json')

# # ── Urgency sort order ────────────────────────────────────────────────
# _URGENCY_ORDER = {'URGENT': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}


# # ══════════════════════════════════════════════════════════════════════
# #  LIVE GENERATION — always available even without DB or JSON file
# # ══════════════════════════════════════════════════════════════════════

# def _generate_live_recs():
#     """
#     Build V7 recommendations live from V4/V5/V6 output files.
#     Returns dict with engineer_recs, ward_recs, commissioner_recs, citizen_recs.
#     This is the universal fallback — works with zero DB, zero pre-run.
#     """
#     engineer_recs    = []
#     ward_recs        = []
#     commissioner_recs = []
#     citizen_recs     = []
#     worst_zone_ids   = []

#     # ── Load V4 zone equity ──────────────────────────────────────────
#     v4_zones = []
#     v4_path  = os.path.join(OUTPUTS_DIR, 'v4_zone_status.json')
#     if os.path.exists(v4_path):
#         with open(v4_path, encoding='utf-8') as f:
#             v4_zones = json.load(f)

#     # Also try equity_minimal for CWEI
#     cwei = None
#     v4m_path = os.path.join(OUTPUTS_DIR, 'v4_equity_minimal.json')
#     if os.path.exists(v4m_path):
#         with open(v4m_path, encoding='utf-8') as f:
#             v4m = json.load(f)
#             cwei = v4m.get('cwei_daily')
#             worst_zone_ids = [v4m.get('worst_zone', '')] if v4m.get('worst_zone') else []

#     # ── Load V5 alerts ───────────────────────────────────────────────
#     v5_alerts_all = {}
#     v5_path = os.path.join(OUTPUTS_DIR, 'v5_alerts.json')
#     if os.path.exists(v5_path):
#         with open(v5_path, encoding='utf-8') as f:
#             v5_alerts_all = json.load(f)
#     baseline_alerts = v5_alerts_all.get('baseline', [])

#     # ── Load V6 burst risk ───────────────────────────────────────────
#     v6_segments = []
#     v6_path = os.path.join(OUTPUTS_DIR, 'v6_burst_top10.json')
#     if os.path.exists(v6_path):
#         with open(v6_path, encoding='utf-8') as f:
#             v6_segments = json.load(f)

#     # ── Trigger A: Equity imbalance (V4 → all channels) ─────────────
#     severe_zones   = [z for z in v4_zones if z.get('status') == 'severe']
#     moderate_zones = [z for z in v4_zones if z.get('status') == 'moderate']
#     over_zones     = [z for z in v4_zones if z.get('status') == 'over']

#     for z in severe_zones:
#         zid  = z.get('zone_id', '')
#         znm  = zid.replace('_', ' ').replace('zone', 'Zone').title()
#         hei  = float(z.get('hei', 0))
#         gain = round(0.85 - hei, 3)  # how much gain to reach equitable

#         engineer_recs.append({
#             "zone_id":            zid,
#             "trigger_type":       "A_equity",
#             "action_text":        (
#                 f"[{znm}] HEI is critically low at {hei:.3f} (target ≥ 0.85). "
#                 f"Increase ESR outlet pressure by 10–15% via upstream valve V-{zid.upper()}-01. "
#                 f"Estimated HEI gain: +{gain:.3f}. Dispatch field team for valve verification."
#             ),
#             "valve_id":           f"V-{zid.upper()}-01",
#             "pipe_id":            "",
#             "urgency":            "URGENT",
#             "estimated_hei_gain": gain,
#         })
#         ward_recs.append({
#             "zone_id":                  zid,
#             "trigger_type":             "A_equity",
#             "action_text":              (
#                 f"Your zone ({znm}) is experiencing severely inequitable water supply. "
#                 f"Citizens in tail-end areas may receive little or no water. "
#                 f"Escalate to the engineering control room immediately and log complaints."
#             ),
#             "escalation_flag":          True,
#             "service_reliability_note": f"HEI: {hei:.3f} — Severe inequity. Tail-end households affected.",
#             "complaint_count":          0,
#         })
#         citizen_recs.append({
#             "zone_id":              zid,
#             "supply_status":        "Intermittent",
#             "advisory_text":        (
#                 f"Water supply in your area ({znm}) is currently limited due to pressure imbalance. "
#                 f"SMC engineers are working to improve distribution. Please store water and use it conservatively."
#             ),
#             "complaint_guidance":   "If you have no water, please submit a complaint using the form below.",
#             "estimated_restoration": "Engineers are working on it. Check back in 2–4 hours.",
#         })

#     for z in moderate_zones:
#         zid = z.get('zone_id', '')
#         znm = zid.replace('_', ' ').replace('zone', 'Zone').title()
#         hei = float(z.get('hei', 0))
#         engineer_recs.append({
#             "zone_id":            zid,
#             "trigger_type":       "A_equity",
#             "action_text":        (
#                 f"[{znm}] HEI is {hei:.3f} — moderate imbalance. "
#                 f"Review valve settings on distribution mains. "
#                 f"Consider redistributing flow from adjacent over-pressurized zones if available."
#             ),
#             "valve_id":           f"V-{zid.upper()}-02",
#             "pipe_id":            "",
#             "urgency":            "MODERATE",
#             "estimated_hei_gain": round(0.85 - hei, 3),
#         })
#         ward_recs.append({
#             "zone_id":                  zid,
#             "trigger_type":             "A_equity",
#             "action_text":              (
#                 f"[{znm}] shows moderate water pressure imbalance. "
#                 f"Some tail-end households may face low pressure during peak hours (6–8 AM and 5–8 PM). "
#                 f"Monitor citizen complaints and report persistent issues to the engineering team."
#             ),
#             "escalation_flag":          False,
#             "service_reliability_note": f"HEI: {hei:.3f} — Monitor closely. Peak-hour supply may be affected.",
#             "complaint_count":          0,
#         })

#     for z in over_zones:
#         zid = z.get('zone_id', '')
#         znm = zid.replace('_', ' ').replace('zone', 'Zone').title()
#         hei = float(z.get('hei', 0))
#         engineer_recs.append({
#             "zone_id":            zid,
#             "trigger_type":       "A_equity",
#             "action_text":        (
#                 f"[{znm}] is over-pressurized — HEI: {hei:.3f} (>1.30). "
#                 f"Reduce upstream ESR outlet pressure by 5–10% to prevent pipe stress and burst risk. "
#                 f"Excess pressure redistributed may improve tail-end zones."
#             ),
#             "valve_id":           f"V-{zid.upper()}-01",
#             "pipe_id":            "",
#             "urgency":            "HIGH",
#             "estimated_hei_gain": 0.0,
#         })

#     # ── Trigger B: Leak anomalies (V5 → engineer_recs) ──────────────
#     for a in baseline_alerts:
#         zid  = a.get('zone_id', '')
#         znm  = zid.replace('_', ' ').replace('zone', 'Zone').title()
#         clps = float(a.get('clps', 0))
#         sig  = a.get('dominant_signal', 'FPI')
#         prob = a.get('probable_node_ids', [])

#         if clps < 0.08:
#             continue  # below threshold, skip

#         urgency = "HIGH" if clps > 0.5 else "MODERATE"

#         signal_actions = {
#             'PDR_n': (
#                 f"Sudden pressure drop detected in {znm} (CLPS: {clps:.3f}). "
#                 f"This indicates a possible active pipe burst or major leak. "
#                 f"Dispatch field team immediately to nodes: {', '.join(prob[:3]) if prob else 'zone perimeter'}."
#             ),
#             'FPI': (
#                 f"Flow-pressure imbalance detected in {znm} (CLPS: {clps:.3f}). "
#                 f"Inlet-to-outlet flow ratio is abnormal — probable pipe leakage or unauthorized offtake. "
#                 f"Inspect distribution mains near nodes: {', '.join(prob[:3]) if prob else 'zone perimeter'}."
#             ),
#             'NFA': (
#                 f"Night flow anomaly detected in {znm} between 01:00–04:00 (CLPS: {clps:.3f}). "
#                 f"Elevated flow outside demand hours indicates unauthorized extraction or slow pipe leak. "
#                 f"Deploy night inspection team. Check night-flow meter at zone inlet."
#             ),
#             'DDI': (
#                 f"Demand deviation anomaly in {znm} (CLPS: {clps:.3f}). "
#                 f"Actual consumption significantly differs from expected profile. "
#                 f"Check for metering issues or unusual consumption in ward. Verify with field team."
#             ),
#         }
#         action = signal_actions.get(sig, signal_actions['FPI'])

#         engineer_recs.append({
#             "zone_id":            zid,
#             "trigger_type":       "B_leak",
#             "action_text":        action,
#             "valve_id":           "",
#             "pipe_id":            prob[0] if prob else "",
#             "urgency":            urgency,
#             "estimated_hei_gain": 0.0,
#         })

#         # Ward also gets a plain-language leak note
#         ward_recs.append({
#             "zone_id":                  zid,
#             "trigger_type":             "B_leak",
#             "action_text":              (
#                 f"A potential water leak or abnormal flow has been detected in {znm}. "
#                 f"Engineering team has been alerted. Residents may notice reduced pressure temporarily. "
#                 f"Please report any visible pipe leaks or water wastage to SMC immediately."
#             ),
#             "escalation_flag":          clps > 0.5,
#             "service_reliability_note": f"CLPS anomaly score: {clps:.3f}. Signal: {sig}.",
#             "complaint_count":          0,
#         })

#     # ── Trigger C: Burst risk (V6 → engineer + commissioner) ────────
#     high_burst = [s for s in v6_segments if s.get('risk_level') == 'HIGH']
#     moderate_burst = [s for s in v6_segments if s.get('risk_level') == 'MODERATE']

#     for s in high_burst:
#         seg  = s.get('segment_id', 'N/A')
#         pss  = float(s.get('pss', 0))
#         mat  = s.get('material', 'Unknown')
#         age  = s.get('age', '?')
#         dom  = s.get('dominant_factor', 'PSI_n')

#         engineer_recs.append({
#             "zone_id":            "",
#             "trigger_type":       "C_burst",
#             "action_text":        (
#                 f"[HIGH BURST RISK] Pipe segment {seg} — PSS: {pss:.3f}. "
#                 f"Material: {mat}, Age: ~{age} years. Dominant stress factor: {dom}. "
#                 f"Schedule urgent physical inspection. Prepare for emergency replacement if PSS exceeds 0.90. "
#                 f"Reduce operating pressure on this segment if possible."
#             ),
#             "valve_id":           "",
#             "pipe_id":            str(seg),
#             "urgency":            "URGENT" if pss > 0.85 else "HIGH",
#             "estimated_hei_gain": 0.0,
#         })

#     for s in moderate_burst[:3]:  # top 3 moderate
#         seg = s.get('segment_id', 'N/A')
#         pss = float(s.get('pss', 0))
#         mat = s.get('material', 'Unknown')
#         age = s.get('age', '?')
#         engineer_recs.append({
#             "zone_id":            "",
#             "trigger_type":       "C_burst",
#             "action_text":        (
#                 f"[MODERATE BURST RISK] Pipe segment {seg} — PSS: {pss:.3f}. "
#                 f"Material: {mat}, Age: ~{age} years. Add to next inspection cycle. "
#                 f"Monitor pressure cycling on this segment."
#             ),
#             "valve_id":           "",
#             "pipe_id":            str(seg),
#             "urgency":            "MODERATE",
#             "estimated_hei_gain": 0.0,
#         })

#     # ── Commissioner recs (Trigger A + C summary) ────────────────────
#     n_sev  = len(severe_zones)
#     n_mod  = len(moderate_zones)
#     n_all  = len(v4_zones)
#     n_high_b = len(high_burst)
#     n_alerts = len(baseline_alerts)
#     city_cwei = cwei if cwei is not None else (
#         sum(float(z.get('hei', 0)) for z in v4_zones) / len(v4_zones) if v4_zones else 0
#     )
#     worst_ids = worst_zone_ids or [
#         z.get('zone_id') for z in sorted(v4_zones, key=lambda x: float(x.get('hei', 1)))[:2]
#     ]
#     worst_display = [z.replace('_',' ').replace('zone','Zone').title() for z in worst_ids if z]

#     if n_sev > 0:
#         commissioner_recs.append({
#             "trigger_type":   "A_equity",
#             "city_summary":   (
#                 f"City-Wide Equity Index (CWEI) is {city_cwei:.3f} — "
#                 f"{'SEVERE' if city_cwei < 0.70 else 'MODERATE' if city_cwei < 0.85 else 'ACCEPTABLE'}. "
#                 f"{n_sev} of {n_all} zones in severe inequity, {n_mod} moderate. "
#                 f"Prioritize valve adjustments in worst-performing zones."
#             ),
#             "worst_zones":   worst_ids,
#             "budget_flag":   n_sev >= 3,
#             "theft_summary": "",
#             "resolution_rate": 0.0,
#         })

#     if n_high_b > 0:
#         commissioner_recs.append({
#             "trigger_type":   "C_burst",
#             "city_summary":   (
#                 f"{n_high_b} pipe segment(s) at HIGH burst risk. "
#                 f"Dominant factors: age degradation and excess pressure cycling. "
#                 f"Capital expenditure for emergency pipe replacement should be considered. "
#                 f"Estimated risk zones: {', '.join(worst_display[:2]) or 'multiple zones'}."
#             ),
#             "worst_zones":   worst_ids,
#             "budget_flag":   True,
#             "theft_summary": "",
#             "resolution_rate": 0.0,
#         })

#     if n_alerts > 0:
#         commissioner_recs.append({
#             "trigger_type":   "B_leak",
#             "city_summary":   (
#                 f"{n_alerts} active anomaly alert(s) in baseline scenario. "
#                 f"CLPS-based leak/anomaly detection is operational. "
#                 f"Engineering team has been notified. NRW reduction potential if leaks resolved: ~5–10%."
#             ),
#             "worst_zones":   worst_ids,
#             "budget_flag":   False,
#             "theft_summary": "",
#             "resolution_rate": 0.0,
#         })

#     if not commissioner_recs:
#         commissioner_recs.append({
#             "trigger_type":   "A_equity",
#             "city_summary":   (
#                 f"All monitored zones are within acceptable equity range (CWEI: {city_cwei:.3f}). "
#                 f"Continue standard monitoring protocols. No immediate capex action required."
#             ),
#             "worst_zones":   [],
#             "budget_flag":   False,
#             "theft_summary": "",
#             "resolution_rate": 1.0,
#         })

#     # ── Trigger D: Citizen advisories — always one per zone ──────────
#     # Build a lookup of zones already covered by severe/moderate recs
#     covered = {r['zone_id'] for r in citizen_recs if r.get('zone_id')}

#     # Fallback advisories for zones with no specific issue
#     all_zone_ids = ['zone_1','zone_2','zone_3','zone_4','zone_5','zone_6','zone_7','zone_8']
#     normal_tips = [
#         "Ensure your overhead tank is filled during supply hours (6–8 AM and 5–8 PM). Use water judiciously.",
#         "Water is available at normal pressure. Store water in covered containers during supply windows.",
#         "Supply is running normally. Avoid wastage — report visible pipe leaks to SMC immediately.",
#         "Pressure within acceptable range. Use water efficiently; high-rise buildings should check pump schedules.",
#         "Supply operating normally. If you notice reduced pressure in the evening, allow a few minutes for the system to pressurize.",
#         "Normal supply conditions. Tanker deliveries are tracked — report unauthorized extraction to SMC.",
#         "Supply is stable. Avoid using water during off-peak hours (midnight–5 AM) to support the network.",
#         "System operating normally. For any billing or metering concerns, contact the SMC Water Works Division.",
#     ]
#     for i, zid in enumerate(all_zone_ids):
#         if zid not in covered:
#             znm = f"Zone {zid.replace('zone_', '')}"
#             citizen_recs.append({
#                 "zone_id":               zid,
#                 "supply_status":         "Normal",
#                 "advisory_text":         f"Water supply in {znm} is currently normal. {normal_tips[i]}",
#                 "complaint_guidance":    "For any supply issues, submit a complaint using the form below.",
#                 "estimated_restoration": "N/A — Supply is normal.",
#             })

#     # Sort engineer recs by urgency
#     engineer_recs.sort(key=lambda r: _URGENCY_ORDER.get(r.get('urgency', 'LOW'), 3))

#     return {
#         "engineer_recs":      engineer_recs,
#         "ward_recs":          ward_recs,
#         "commissioner_recs":  commissioner_recs,
#         "citizen_recs":       citizen_recs,
#         "updated_at":         datetime.utcnow().isoformat(),
#         "source":             "live_v4v5v6",
#     }


# def rebuild_recommendations():
#     """
#     Public function called by rebuild.py and APScheduler.
#     Generates live recs, saves to JSON, and tries to write to DB.
#     """
#     data = _generate_live_recs()
#     # Save JSON (always)
#     os.makedirs(OUTPUTS_DIR, exist_ok=True)
#     with open(V7_JSON, 'w', encoding='utf-8') as f:
#         json.dump(data, f, indent=2)
#     # Try DB write (non-fatal if DB unavailable)
#     try:
#         _write_to_db(data)
#     except Exception as e:
#         print(f"[V7] DB write skipped (non-fatal): {e}")
#     return data


# def _write_to_db(data):
#     """Write generated recs to PostgreSQL tables."""
#     with engine.connect() as conn:
#         # Clear old recs first
#         for tbl in ('engineer_recs', 'ward_recs', 'commissioner_recs', 'citizen_recs'):
#             try:
#                 conn.execute(text(f"DELETE FROM {tbl}"))
#             except Exception:
#                 pass

#         for r in data.get('engineer_recs', []):
#             conn.execute(text("""
#                 INSERT INTO engineer_recs
#                     (zone_id, trigger_type, action_text, valve_id, pipe_id,
#                      urgency, estimated_hei_gain, node_coords)
#                 VALUES (:z, :tt, :at, :vid, :pid, :urg, :hg, :nc)
#             """), {
#                 'z':   r.get('zone_id') or '',
#                 'tt':  r.get('trigger_type') or '',
#                 'at':  r.get('action_text') or '',
#                 'vid': r.get('valve_id') or '',
#                 'pid': r.get('pipe_id') or '',
#                 'urg': r.get('urgency') or 'LOW',
#                 'hg':  r.get('estimated_hei_gain') or 0,
#                 'nc':  json.dumps(r.get('node_coords') or {}),
#             })

#         for r in data.get('ward_recs', []):
#             conn.execute(text("""
#                 INSERT INTO ward_recs
#                     (zone_id, trigger_type, action_text, escalation_flag,
#                      service_reliability_note, complaint_count)
#                 VALUES (:z, :tt, :at, :ef, :srn, :cc)
#             """), {
#                 'z':   r.get('zone_id') or '',
#                 'tt':  r.get('trigger_type') or '',
#                 'at':  r.get('action_text') or '',
#                 'ef':  bool(r.get('escalation_flag')),
#                 'srn': r.get('service_reliability_note') or '',
#                 'cc':  r.get('complaint_count') or 0,
#             })

#         for r in data.get('commissioner_recs', []):
#             conn.execute(text("""
#                 INSERT INTO commissioner_recs
#                     (city_summary, worst_zones, budget_flag, theft_summary,
#                      resolution_rate, trigger_type)
#                 VALUES (:cs, :wz, :bf, :ts, :rr, :tt)
#             """), {
#                 'cs': r.get('city_summary') or '',
#                 'wz': json.dumps(r.get('worst_zones') or []),
#                 'bf': bool(r.get('budget_flag')),
#                 'ts': r.get('theft_summary') or '',
#                 'rr': r.get('resolution_rate') or 0,
#                 'tt': r.get('trigger_type') or '',
#             })

#         for r in data.get('citizen_recs', []):
#             conn.execute(text("""
#                 INSERT INTO citizen_recs
#                     (zone_id, supply_status, advisory_text,
#                      complaint_guidance, estimated_restoration)
#                 VALUES (:z, :ss, :at, :cg, :er)
#             """), {
#                 'z':  r.get('zone_id') or '',
#                 'ss': r.get('supply_status') or 'Normal',
#                 'at': r.get('advisory_text') or '',
#                 'cg': r.get('complaint_guidance') or '',
#                 'er': r.get('estimated_restoration') or '',
#             })

#         # Log the run
#         n_all = (len(data.get('engineer_recs', [])) + len(data.get('ward_recs', [])) +
#                  len(data.get('commissioner_recs', [])) + len(data.get('citizen_recs', [])))
#         conn.execute(text("""
#             INSERT INTO v7_run_log
#                 (status, zones_processed, recs_generated, engineer_count,
#                  ward_count, commissioner_count, citizen_count)
#             VALUES ('success', :zp, :rg, :ec, :wc, :cc, :cit)
#         """), {
#             'zp':  8,
#             'rg':  n_all,
#             'ec':  len(data.get('engineer_recs', [])),
#             'wc':  len(data.get('ward_recs', [])),
#             'cc':  len(data.get('commissioner_recs', [])),
#             'cit': len(data.get('citizen_recs', [])),
#         })
#         conn.commit()


# def _get_last_v7_run():
#     """Get the timestamp of the last V7 run — tries DB first, then JSON."""
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(
#                 text("SELECT MAX(ran_at) FROM v7_run_log WHERE status='success'")
#             ).fetchone()
#             if result and result[0]:
#                 return result[0].isoformat()
#     except Exception:
#         pass
#     # Fallback: read JSON file mod time
#     if os.path.exists(V7_JSON):
#         with open(V7_JSON, encoding='utf-8') as f:
#             d = json.load(f)
#             return d.get('updated_at', datetime.utcnow().isoformat())
#     return datetime.utcnow().isoformat()


# def _load_data_source():
#     """
#     Unified data loader:
#     1. Try PostgreSQL tables
#     2. Fall back to v7_recommendations.json
#     3. Fall back to live generation
#     Returns (engineer_recs, ward_recs, commissioner_recs, citizen_recs, source)
#     """
#     # Try DB
#     try:
#         with engine.connect() as conn:
#             eng_rows = conn.execute(
#                 text("""
#                     SELECT rec_id, zone_id, trigger_type, action_text, valve_id,
#                            pipe_id, pressure_delta, urgency, estimated_hei_gain,
#                            node_coords, created_at
#                     FROM engineer_recs
#                     ORDER BY CASE urgency
#                         WHEN 'URGENT' THEN 1 WHEN 'HIGH' THEN 2
#                         WHEN 'MODERATE' THEN 3 ELSE 4 END, created_at DESC
#                     LIMIT 50
#                 """)
#             ).fetchall()

#             if eng_rows:
#                 # DB has data — use it for all channels
#                 ward_rows = conn.execute(
#                     text("""
#                         SELECT rec_id, zone_id, trigger_type, action_text,
#                                escalation_flag, service_reliability_note,
#                                complaint_count, created_at
#                         FROM ward_recs ORDER BY escalation_flag DESC, created_at DESC LIMIT 50
#                     """)
#                 ).fetchall()
#                 comm_rows = conn.execute(
#                     text("""
#                         SELECT rec_id, city_summary, worst_zones, budget_flag,
#                                theft_summary, resolution_rate, trigger_type, created_at
#                         FROM commissioner_recs ORDER BY budget_flag DESC, created_at DESC LIMIT 20
#                     """)
#                 ).fetchall()
#                 cit_rows = conn.execute(
#                     text("""
#                         SELECT rec_id, zone_id, supply_status, advisory_text,
#                                complaint_guidance, estimated_restoration, created_at
#                         FROM citizen_recs ORDER BY zone_id, created_at DESC
#                     """)
#                 ).fetchall()

#                 def _parse_json(v):
#                     if not v: return []
#                     try: return json.loads(v)
#                     except Exception: return [v] if v else []

#                 e_recs = [{
#                     "rec_id": r[0], "zone_id": r[1] or '', "trigger_type": r[2] or '',
#                     "action_text": r[3] or '', "valve_id": r[4] or '', "pipe_id": r[5] or '',
#                     "pressure_delta": float(r[6] or 0), "urgency": r[7] or 'LOW',
#                     "estimated_hei_gain": float(r[8] or 0),
#                     "node_coords": _parse_json(r[9]),
#                     "created_at": r[10].isoformat() if r[10] else '',
#                 } for r in eng_rows]

#                 w_recs = [{
#                     "rec_id": r[0], "zone_id": r[1] or '', "trigger_type": r[2] or '',
#                     "action_text": r[3] or '', "escalation_flag": bool(r[4]),
#                     "service_reliability_note": r[5] or '', "complaint_count": int(r[6] or 0),
#                     "created_at": r[7].isoformat() if r[7] else '',
#                 } for r in ward_rows]

#                 c_recs = [{
#                     "rec_id": r[0], "city_summary": r[1] or '',
#                     "worst_zones": _parse_json(r[2]), "budget_flag": bool(r[3]),
#                     "theft_summary": r[4] or '', "resolution_rate": float(r[5] or 0),
#                     "trigger_type": r[6] or '',
#                     "created_at": r[7].isoformat() if r[7] else '',
#                 } for r in comm_rows]

#                 cit_recs = [{
#                     "rec_id": r[0], "zone_id": r[1] or '',
#                     "zone_name": f"Zone {(r[1] or '').replace('zone_', '')}",
#                     "supply_status": r[2] or 'Normal', "advisory_text": r[3] or '',
#                     "complaint_guidance": r[4] or '', "estimated_restoration": r[5] or '',
#                     "created_at": r[6].isoformat() if r[6] else '',
#                 } for r in cit_rows]

#                 return e_recs, w_recs, c_recs, cit_recs, "db"
#     except Exception:
#         pass  # DB unavailable — continue to next fallback

#     # Try JSON file
#     if os.path.exists(V7_JSON):
#         try:
#             with open(V7_JSON, encoding='utf-8') as f:
#                 d = json.load(f)
#             e_recs   = d.get('engineer_recs', [])
#             w_recs   = d.get('ward_recs', [])
#             c_recs   = d.get('commissioner_recs', [])
#             cit_recs = d.get('citizen_recs', [])
#             if e_recs or w_recs or c_recs:
#                 # Add zone_name to citizen recs
#                 for r in cit_recs:
#                     if 'zone_name' not in r:
#                         r['zone_name'] = f"Zone {(r.get('zone_id') or '').replace('zone_', '')}"
#                 return e_recs, w_recs, c_recs, cit_recs, "json"
#         except Exception:
#             pass

#     # Live generation fallback
#     live = _generate_live_recs()
#     e_recs   = live.get('engineer_recs', [])
#     w_recs   = live.get('ward_recs', [])
#     c_recs   = live.get('commissioner_recs', [])
#     cit_recs = live.get('citizen_recs', [])
#     for r in cit_recs:
#         if 'zone_name' not in r:
#             r['zone_name'] = f"Zone {(r.get('zone_id') or '').replace('zone_', '')}"
#     # Save JSON for next request
#     try:
#         with open(V7_JSON, 'w', encoding='utf-8') as f:
#             json.dump(live, f, indent=2)
#     except Exception:
#         pass
#     return e_recs, w_recs, c_recs, cit_recs, "live"


# # ══════════════════════════════════════════════════════════════════════
# #  API ENDPOINTS
# # ══════════════════════════════════════════════════════════════════════

# # ── GET /recommendations/engineer ────────────────────────────────────
# @router.get("/engineer", summary="Engineer Recommendations (V7 Triggers A, B, C)")
# def get_engineer_recommendations(current_user: dict = Depends(get_current_user)):
#     role = current_user.get('role', '')
#     if role not in ('engineer', 'field_operator'):
#         raise HTTPException(status_code=403, detail="engineer or field_operator role required.")

#     e_recs, _, _, _, source = _load_data_source()
#     return {
#         "recs":       e_recs,
#         "total":      len(e_recs),
#         "role":       role,
#         "source":     source,
#         "updated_at": _get_last_v7_run(),
#     }


# # ── GET /recommendations/ward ────────────────────────────────────────
# @router.get("/ward", summary="Ward Officer Recommendations (V7 Trigger A)")
# def get_ward_recommendations(current_user: dict = Depends(get_current_user)):
#     role    = current_user.get('role', '')
#     zone_id = current_user.get('zone_id')

#     if role not in ('ward_officer', 'engineer'):
#         raise HTTPException(status_code=403, detail="ward_officer role required.")

#     _, w_recs, _, _, source = _load_data_source()

#     # Zone-filter for ward officers:
#     # Show ONLY this ward's zone recs + any city-wide (no zone_id) recs.
#     # If zone is healthy (no active recs), return empty — frontend shows "zone healthy" message.
#     # NEVER show other zones' recs to a ward officer.
#     if role == 'ward_officer' and zone_id:
#         zone_specific = [r for r in w_recs if r.get('zone_id') == zone_id]
#         city_wide     = [r for r in w_recs if not r.get('zone_id')]
#         w_recs        = zone_specific + city_wide

#     return {
#         "recs":       w_recs,
#         "total":      len(w_recs),
#         "zone_id":    zone_id,
#         "role":       role,
#         "source":     source,
#         "updated_at": _get_last_v7_run(),
#     }


# # ── GET /recommendations/commissioner ────────────────────────────────
# @router.get("/commissioner", summary="Commissioner Strategic Recommendations (V7 Triggers A, C)")
# def get_commissioner_recommendations(current_user: dict = Depends(get_current_user)):
#     role = current_user.get('role', '')
#     if role != 'commissioner':
#         raise HTTPException(status_code=403, detail="commissioner role required.")

#     _, _, c_recs, _, source = _load_data_source()
#     return {
#         "recs":       c_recs,
#         "total":      len(c_recs),
#         "role":       role,
#         "source":     source,
#         "updated_at": _get_last_v7_run(),
#     }


# # ── GET /recommendations/citizen ─────────────────────────────────────
# @router.get("/citizen", summary="Citizen Supply Advisories — PUBLIC (No Auth Required)")
# def get_citizen_recommendations():
#     """Public endpoint — NO auth, NO infrastructure data."""
#     _, _, _, cit_recs, source = _load_data_source()
#     # Safety: strip any infra data
#     safe = []
#     for r in cit_recs:
#         safe.append({
#             "zone_id":               r.get('zone_id', ''),
#             "zone_name":             r.get('zone_name', ''),
#             "supply_status":         r.get('supply_status', 'Normal'),
#             "advisory_text":         r.get('advisory_text', ''),
#             "complaint_guidance":    r.get('complaint_guidance', ''),
#             "estimated_restoration": r.get('estimated_restoration', ''),
#         })
#     return {
#         "advisories":  safe,
#         "count":       len(safe),
#         "updated_at":  _get_last_v7_run(),
#     }


# # ── GET /recommendations/updated-at ──────────────────────────────────
# @router.get("/updated-at", summary="Last V7 Run Timestamp")
# def get_recommendations_updated_at(current_user: dict = Depends(get_current_user)):
#     return {"updated_at": _get_last_v7_run()}


# # ── POST /recommendations/rebuild ────────────────────────────────────
# @router.post("/rebuild", summary="Rebuild V7 Recommendations (Engineer)")
# def post_rebuild(current_user: dict = Depends(get_current_user)):
#     role = current_user.get('role', '')
#     if role not in ('engineer', 'commissioner'):
#         raise HTTPException(status_code=403, detail="engineer or commissioner role required.")
#     data = rebuild_recommendations()
#     return {
#         "success":    True,
#         "message":    "Recommendations rebuilt successfully.",
#         "engineer":   len(data.get('engineer_recs', [])),
#         "ward":       len(data.get('ward_recs', [])),
#         "commissioner": len(data.get('commissioner_recs', [])),
#         "citizen":    len(data.get('citizen_recs', [])),
#         "updated_at": data.get('updated_at'),
#     }



"""
Hydro-Equity Engine — Phase 4b
backend/routers/recommendations.py

Fast V7 recommendation endpoints — all read from v7_recommendations.json cache.
No heavy computation in request path.

Two runtime fixes applied here:
  1. Ward dedup: removes duplicate/low-value recs for same zone,
     prioritises B_leak over A_equity when both exist for same zone.
  2. Citizen sync: cross-checks citizen_recs against v4_zone_status.json
     so zones marked severe/moderate in V4 always show as Intermittent/Normal
     in the citizen panel — even if the JSON cache is stale.
"""

import os
import json
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException

from backend.auth import get_current_user

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])

OUTPUTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')
)
V7_JSON    = os.path.join(OUTPUTS_DIR, 'v7_recommendations.json')
V4_STATUS  = os.path.join(OUTPUTS_DIR, 'v4_zone_status.json')


# ── Read helpers ──────────────────────────────────────────────────────

def _read_cache() -> dict:
    if not os.path.exists(V7_JSON):
        return {"engineer_recs": [], "ward_recs": [], "commissioner_recs": [],
                "citizen_recs": [], "updated_at": None}
    try:
        with open(V7_JSON, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {"engineer_recs": [], "ward_recs": [], "commissioner_recs": [],
                "citizen_recs": [], "updated_at": None}


def _read_v4_status() -> dict:
    """Returns {zone_id: {'hei': float, 'status': str}} from v4_zone_status.json."""
    if not os.path.exists(V4_STATUS):
        return {}
    try:
        with open(V4_STATUS, encoding='utf-8') as f:
            raw = json.load(f)
        if isinstance(raw, list):
            return {z['zone_id']: z for z in raw if z.get('zone_id')}
        return {}
    except Exception:
        return {}


def _updated_at() -> str:
    return _read_cache().get('updated_at') or datetime.utcnow().isoformat()


def rebuild_recommendations():
    try:
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
        from scripts.v7_recommendations import run_v7
        run_v7()
        return _read_cache()
    except Exception as e:
        return {"error": str(e)}


# ── Fix 1: Ward deduplication ─────────────────────────────────────────
# Problem: trigger_a writes a rec for every zone (incl. equitable "no action")
#          trigger_b writes a rec for alerted zones.
#          Result: Zone 1 gets 2 A_equity recs AND 1 B_leak rec.
# Fix:     For each zone, keep only one rec per trigger_type (latest).
#          If zone has a B_leak rec, drop the A_equity "no action" rec for that zone.

def _dedup_ward_recs(recs: list) -> list:
    # Step 1: for each (zone_id, trigger_type), keep only ONE rec
    # Use a dict — later entries overwrite earlier ones (latest wins per slot)
    seen = {}
    for r in recs:
        key = (r.get('zone_id', ''), r.get('trigger_type', ''))
        seen[key] = r  # last one wins (most recent insert)

    deduped = list(seen.values())

    # Step 2: for zones that have B_leak, drop their A_equity "no action" rec
    # (A_equity "no action" = escalation_flag is False AND text contains "No action required")
    b_leak_zones = {r['zone_id'] for r in deduped if r.get('trigger_type') == 'B_leak'}

    result = []
    for r in deduped:
        if (r.get('trigger_type') == 'A_equity'
                and r.get('zone_id') in b_leak_zones
                and not r.get('escalation_flag')
                and 'No action required' in (r.get('action_text') or '')):
            continue  # skip — zone has a real B_leak rec, this A_equity is noise
        result.append(r)

    # Step 3: sort — B_leak first (escalation), then A_equity escalated, then rest
    def _rank(r):
        if r.get('trigger_type') == 'B_leak':
            return 0
        if r.get('escalation_flag'):
            return 1
        return 2

    result.sort(key=_rank)
    return result


# ── Fix 2: Citizen sync with V4 zone status ───────────────────────────
# Problem: citizen_recs in stale JSON all say "Normal" even if V4 shows
#          Zone 3 = severe (HEI 0.56), Zone 8 = severe (HEI 0.48).
# Fix:     Build a map of zone_id → citizen_rec from cache.
#          Then for any zone where V4 says severe/moderate, override status + advisory.
#          Always output exactly 8 zones.

ALL_ZONE_IDS = [
    'zone_1', 'zone_2', 'zone_3', 'zone_4',
    'zone_5', 'zone_6', 'zone_7', 'zone_8',
]

def _build_citizen_advisories(raw_recs: list) -> list:
    v4 = _read_v4_status()

    # Index existing citizen recs by zone_id (take first match per zone)
    cached = {}
    for r in raw_recs:
        zid = r.get('zone_id', '')
        if zid and zid not in cached:
            cached[zid] = r

    result = []
    for zid in ALL_ZONE_IDS:
        znm   = "Zone {}".format(zid.replace('zone_', ''))
        v4z   = v4.get(zid, {})
        v4hei = float(v4z.get('hei', 1.0) or 1.0)
        v4st  = v4z.get('status', 'equitable')  # severe | moderate | equitable | over

        # Start from cached rec if it exists
        base = cached.get(zid, {})

        # Decide supply status from V4 (overrides cache if zone is actually bad)
        if v4st == 'severe':
            supply_status  = 'Intermittent'
            advisory_text  = (
                "Water supply in {} may be limited due to pressure imbalance "
                "(equity score: {:.2f}). SMC engineers are working to restore normal pressure. "
                "Please store water during supply hours and use it conservatively.".format(znm, v4hei)
            )
            complaint_guid = (
                "If you have no water or very low pressure, please submit a complaint "
                "using the form below. Include your area landmark and contact number."
            )
            est_restoration = "Engineers are actively working. Check back in 2–4 hours."
        elif v4st == 'moderate':
            supply_status  = 'Normal'
            advisory_text  = (
                "Water supply in {} is near-normal (equity score: {:.2f}). "
                "Some households may experience slightly reduced pressure during peak hours "
                "(6–8 AM and 5–8 PM). Store water during supply windows.".format(znm, v4hei)
            )
            complaint_guid = (
                "Persistent low pressure? Submit a complaint using the form below."
            )
            est_restoration = "Currently operational — engineering team monitoring."
        elif v4st == 'over':
            supply_status  = 'Normal'
            advisory_text  = (
                "Water supply in {} is operating normally. "
                "Supply window: 6–8 AM and 5–8 PM. "
                "Fill overhead tanks during supply hours.".format(znm)
            )
            complaint_guid = (
                "If you notice unusually high water flow or pipe vibration, report it below."
            )
            est_restoration = "No disruption expected."
        else:
            # Equitable — use cached text if available, else a simple default
            supply_status  = base.get('supply_status', 'Normal')
            advisory_text  = base.get('advisory_text') or (
                "Water supply in {} is operating normally (equity score: {:.2f}). "
                "Supply window: 6–8 AM and 5–8 PM. "
                "Ensure your overhead tank is filled during supply hours.".format(znm, v4hei)
            )
            complaint_guid  = base.get('complaint_guidance') or (
                "For any supply issues, submit a complaint using the form below."
            )
            est_restoration = base.get('estimated_restoration') or 'No disruption — normal operation.'

        result.append({
            "zone_id":               zid,
            "zone_name":             znm,
            "supply_status":         supply_status,
            "advisory_text":         advisory_text,
            "complaint_guidance":    complaint_guid,
            "estimated_restoration": est_restoration,
        })

    return result


# ══════════════════════════════════════════════════════════════════════
#  ENDPOINTS
# ══════════════════════════════════════════════════════════════════════

@router.get("/engineer", summary="Engineer Recommendations (V7 Triggers A, B, C)")
def get_engineer_recommendations(current_user: dict = Depends(get_current_user)):
    role = current_user.get('role', '')
    if role not in ('engineer', 'field_operator'):
        raise HTTPException(status_code=403, detail="engineer or field_operator role required.")

    data  = _read_cache()
    recs  = data.get('engineer_recs', [])
    order = {'URGENT': 0, 'HIGH': 1, 'MODERATE': 2, 'LOW': 3}
    recs  = sorted(recs, key=lambda r: order.get(r.get('urgency', 'LOW'), 3))

    return {
        "recs":       recs,
        "total":      len(recs),
        "role":       role,
        "updated_at": data.get('updated_at') or _updated_at(),
    }


@router.get("/ward", summary="Ward Officer Recommendations (V7 Triggers A, B)")
def get_ward_recommendations(current_user: dict = Depends(get_current_user)):
    role    = current_user.get('role', '')
    zone_id = current_user.get('zone_id')

    if role not in ('ward_officer', 'engineer'):
        raise HTTPException(status_code=403, detail="ward_officer role required.")

    data   = _read_cache()
    w_recs = data.get('ward_recs', [])

    # Zone-filter: only this ward's zone + city-wide (empty zone_id) recs
    if role == 'ward_officer' and zone_id:
        w_recs = [r for r in w_recs
                  if r.get('zone_id') == zone_id or not r.get('zone_id')]

    # Apply dedup fix — removes repetitive "no action" duplicates
    w_recs = _dedup_ward_recs(w_recs)

    return {
        "recs":       w_recs,
        "total":      len(w_recs),
        "zone_id":    zone_id,
        "role":       role,
        "updated_at": data.get('updated_at') or _updated_at(),
    }


@router.get("/commissioner", summary="Commissioner Strategic Recommendations (V7 Triggers A, C)")
def get_commissioner_recommendations(current_user: dict = Depends(get_current_user)):
    role = current_user.get('role', '')
    if role != 'commissioner':
        raise HTTPException(status_code=403, detail="commissioner role required.")

    data = _read_cache()
    recs = data.get('commissioner_recs', [])

    return {
        "recs":       recs,
        "total":      len(recs),
        "role":       role,
        "updated_at": data.get('updated_at') or _updated_at(),
    }


@router.get("/citizen", summary="Citizen Supply Advisories — PUBLIC (No Auth)")
def get_citizen_recommendations():
    """Public endpoint — no auth, no infra data, always 8 zones synced with V4."""
    data = _read_cache()
    raw  = data.get('citizen_recs', [])

    # Always build from V4 ground truth so citizen view is never stale
    advisories = _build_citizen_advisories(raw)

    return {
        "advisories": advisories,
        "count":      len(advisories),
        "updated_at": data.get('updated_at') or _updated_at(),
    }


@router.get("/updated-at", summary="Last V7 Run Timestamp")
def get_recommendations_updated_at(current_user: dict = Depends(get_current_user)):
    return {"updated_at": _updated_at()}


@router.post("/rebuild", summary="Rebuild V7 Recommendations")
def post_rebuild(current_user: dict = Depends(get_current_user)):
    role = current_user.get('role', '')
    if role not in ('engineer', 'commissioner'):
        raise HTTPException(status_code=403, detail="engineer or commissioner role required.")

    result = rebuild_recommendations()
    if 'error' in result:
        raise HTTPException(
            status_code=500,
            detail="V7 rebuild failed: {}. Run: python scripts/v7_recommendations.py".format(
                result['error'])
        )

    return {
        "success":      True,
        "message":      "Recommendations rebuilt.",
        "engineer":     len(result.get('engineer_recs', [])),
        "ward":         len(result.get('ward_recs', [])),
        "commissioner": len(result.get('commissioner_recs', [])),
        "citizen":      len(result.get('citizen_recs', [])),
        "updated_at":   result.get('updated_at'),
    }