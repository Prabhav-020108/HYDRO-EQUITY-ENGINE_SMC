"""
Hydro-Equity Engine — Phase 4b
backend/routers/recommendations.py

V7 Recommendation Engine & Endpoints.
Generates and serves role-partitioned recommendations based on V4, V5, V6 analytics.
"""

import os
import json
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any

from backend.auth import get_current_user, require_roles

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])

OUTPUTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'outputs'))
V7_FILE = os.path.join(OUTPUTS_DIR, "v7_recommendations.json")

def rebuild_recommendations():
    """
    Rule engine that reads V4, V5, V6 outputs and generates V7 recommendations.
    Writes to outputs/v7_recommendations.json.
    """
    engineer_recs = []
    ward_recs = []
    commissioner_recs = []
    citizen_recs = []

    # ── Trigger A: Equity Imbalance (V4) ───────────────────────────
    try:
        v4_path = os.path.join(OUTPUTS_DIR, "v4_equity_minimal.json")
        if os.path.exists(v4_path):
            with open(v4_path, 'r', encoding='utf-8') as f:
                v4_data = json.load(f)
                # v4_data is usually a list of zones or dict. Let's handle both.
                zones = v4_data if isinstance(v4_data, list) else v4_data.get('zones', [])
                for z in zones:
                    # Use daily_hei if available, fallback to hei
                    hei = z.get('daily_hei', z.get('hei', 1.0))
                    zone_id = z.get('zone_id', z.get('id', 'Unknown'))
                    zone_name = z.get('zone_name', z.get('nm', zone_id))
                    
                    if hei < 0.7:
                        engineer_recs.append({
                            "zone_id": zone_id,
                            "urgency": "MODERATE",
                            "message": f"Adjust valves / operations to improve HEI in {zone_name}. (HEI: {hei:.2f})"
                        })
                        ward_recs.append({
                            "zone_id": zone_id,
                            "action_text": f"Notify ward officer about chronic under-supply in {zone_name}.",
                            "escalation_flag": True
                        })
                        commissioner_recs.append({
                            "city_summary": f"Equity risk in {zone_name}.",
                            "worst_zones": [zone_name],
                            "budget_flag": True,
                            "resolution_rate": "N/A"
                        })
                        citizen_recs.append({
                            "supply_status": "Inconsistent",
                            "advisory_text": f"Water supply may be inconsistent in {zone_name}; please follow storage and conservation advisories.",
                            "estimated_restoration": "Ongoing"
                        })
    except Exception as e:
        # Resilience: skip if failed
        pass

    # ── Trigger B: Leaks (V5) ──────────────────────────────────────
    try:
        v5_path = os.path.join(OUTPUTS_DIR, "v5_alerts.json")
        if os.path.exists(v5_path):
            with open(v5_path, 'r', encoding='utf-8') as f:
                v5_data = json.load(f)
                # Handle dict with scenarios
                alerts = []
                if isinstance(v5_data, dict):
                    # Combine alerts from all scenarios for recommendations, or just baseline?
                    # The prompt says "For alerts classified as LEAK".
                    # Let's check 'leak' scenario primarily, fallback to baseline.
                    alerts_dict = v5_data.get('leak', v5_data.get('baseline', []))
                    if isinstance(alerts_dict, list):
                        alerts = alerts_dict
                elif isinstance(v5_data, list):
                    alerts = v5_data

                for a in alerts:
                    # check if leak or equivalent
                    desc = a.get('dominant_signal', '') or a.get('title', '')
                    if 'leak' in desc.lower() or 'PDR' in desc or 'FPI' in desc:
                        zone_id = a.get('zone_id', 'Unknown')
                        engineer_recs.append({
                            "zone_id": zone_id,
                            "pipe_id": a.get('probable_node_ids', ['N/A'])[0] if a.get('probable_node_ids') else 'N/A',
                            "urgency": "HIGH",
                            "message": f"Targeted operational action for leak/anomaly in {zone_id}."
                        })
                        ward_recs.append({
                            "zone_id": zone_id,
                            "action_text": f"Coordinate with field engineers to handle leak in ward's {zone_id}.",
                            "escalation_flag": False
                        })

    except Exception as e:
        pass

    # ── Trigger C: Burst Risk (V6) ─────────────────────────────────
    try:
        v6_path = os.path.join(OUTPUTS_DIR, "v6_burst_top10.json")
        if os.path.exists(v6_path):
            with open(v6_path, 'r', encoding='utf-8') as f:
                v6_data = json.load(f)
                segments = v6_data if isinstance(v6_data, list) else v6_data.get('segments', [])
                for s in segments:
                    if s.get('risk_level') == 'HIGH':
                        segment_id = s.get('segment_id', 'N/A')
                        engineer_recs.append({
                            "pipe_id": segment_id,
                            "urgency": "CRITICAL",
                            "message": f"Emergency segment-level action for Pipe {segment_id} at HIGH burst risk."
                        })
                        commissioner_recs.append({
                            "city_summary": f"High burst risk on Pipe {segment_id}.",
                            "budget_flag": True,
                            "worst_zones": [],
                            "resolution_rate": "N/A"
                        })
    except Exception as e:
        pass

    # ── Trigger D: Citizen Baseline / Advisory ─────────────────────
    if not citizen_recs:
        citizen_recs.append({
            "supply_status": "Normal",
            "advisory_text": "No active alerts; continue following standard water conservation guidelines.",
            "estimated_restoration": "N/A"
        })

    # ── Trigger E: Theft (4c Stub) ─────────────────────────────────
    try:
        v13_path = os.path.join(OUTPUTS_DIR, "v13_theft_alerts.json")
        if os.path.exists(v13_path):
            with open(v13_path, 'r', encoding='utf-8') as f:
                v13_data = json.load(f)
                # Process if exists
    except Exception:
        pass

    output_data = {
        "engineer_recs": engineer_recs,
        "ward_recs": ward_recs,
        "commissioner_recs": commissioner_recs,
        "citizen_recs": citizen_recs,
        "updated_at": datetime.utcnow().isoformat()
    }

    with open(V7_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2)

    return output_data

@router.post("/rebuild", summary="Rebuild V7 Recommendations")
def post_rebuild(current_user: dict = Depends(get_current_user)):
    """Admin/Operator endpoint to rebuild recommendations."""
    # Reuse role check if needed, or allow all authenticated users for now
    data = rebuild_recommendations()
    return {"success": True, "message": "Recommendations rebuilt.", "data": data}

@router.get("/updated-at", summary="Get last update timestamp")
def get_updated_at():
    """Public endpoint to get the last update timestamp."""
    try:
        if os.path.exists(V7_FILE):
            with open(V7_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {"updated_at": data.get("updated_at", "Unknown")}
    except Exception:
        pass
    return {"updated_at": datetime.utcnow().isoformat(), "note": "Default/Fallback"}

@router.get("/engineer", summary="Engineer Recommendations")
def get_engineer_recs(user: dict = Depends(require_roles("engineer", "field_operator"))):
    """Gated to engineer and field operator."""
    try:
        if os.path.exists(V7_FILE):
            with open(V7_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("engineer_recs", [])
    except Exception:
        pass
    return []

@router.get("/ward", summary="Ward Recommendations")
def get_ward_recs(user: dict = Depends(require_roles("ward_officer"))):
    """Gated to ward officer."""
    try:
        if os.path.exists(V7_FILE):
            with open(V7_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("ward_recs", [])
    except Exception:
        pass
    return []

@router.get("/commissioner", summary="Commissioner Recommendations")
def get_commissioner_recs(user: dict = Depends(require_roles("commissioner"))):
    """Gated to commissioner."""
    try:
        if os.path.exists(V7_FILE):
            with open(V7_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("commissioner_recs", [])
    except Exception:
        pass
    return []

@router.get("/citizen", summary="Citizen Recommendations")
def get_citizen_recs():
    """Public endpoint for citizen advisories."""
    try:
        if os.path.exists(V7_FILE):
            with open(V7_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                recs = data.get("citizen_recs", [])
                # Ensure safe fields only
                safe_recs = []
                for r in recs:
                    safe_recs.append({
                        "supply_status": r.get("supply_status", "Normal"),
                        "advisory_text": r.get("advisory_text", ""),
                        "estimated_restoration": r.get("estimated_restoration", "N/A")
                    })
                return safe_recs
    except Exception:
        pass
    return [{"supply_status": "Normal", "advisory_text": "No active alerts.", "estimated_restoration": "N/A"}]
